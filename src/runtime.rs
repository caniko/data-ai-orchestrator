use crate::ids::generated_id;
use crate::{
    AdapterError, ExecutionEvent, InMemoryEventSink, InMemoryRunStore, OrchestratorError, RunId,
    RunManifest, RunProvenance, RunRecord, RunStatus, RunStore, StepId, StepRecord, StepStatus,
    TaskContext, TaskError, TaskRegistry, WorkerProfile, WorkflowError, WorkflowSpec,
};
use futures::future::join_all;
use serde_json::{Map, Value};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::time::{Duration, sleep, timeout};

#[derive(Clone, Debug)]
pub struct ExecutorOptions {
    pub max_parallel_steps: usize,
    pub worker: WorkerProfile,
    pub provenance: RunProvenance,
}

impl Default for ExecutorOptions {
    fn default() -> Self {
        Self {
            max_parallel_steps: 8,
            worker: WorkerProfile::default(),
            provenance: RunProvenance::default(),
        }
    }
}

/// A process-local executor. Production applications can keep the workflow,
/// task, policy, and event contracts while replacing the run store and worker
/// claim loop with durable infrastructure.
pub struct InMemoryExecutor {
    registry: Arc<dyn TaskRegistry>,
    store: Arc<dyn RunStore>,
    events: Arc<dyn crate::EventSink>,
    options: ExecutorOptions,
    sequence: AtomicU64,
}

impl InMemoryExecutor {
    pub fn new(registry: Arc<dyn TaskRegistry>) -> Self {
        Self {
            registry,
            store: Arc::new(InMemoryRunStore::default()),
            events: Arc::new(InMemoryEventSink::default()),
            options: ExecutorOptions::default(),
            sequence: AtomicU64::new(0),
        }
    }

    pub fn with_store(mut self, store: Arc<dyn RunStore>) -> Self {
        self.store = store;
        self
    }

    pub fn with_event_sink(mut self, events: Arc<dyn crate::EventSink>) -> Self {
        self.events = events;
        self
    }

    pub fn with_options(mut self, options: ExecutorOptions) -> Self {
        self.options = ExecutorOptions {
            max_parallel_steps: options.max_parallel_steps.max(1),
            worker: options.worker,
            provenance: options.provenance,
        };
        self
    }

    pub async fn execute(
        &self,
        workflow: WorkflowSpec,
        input: Value,
    ) -> Result<RunRecord, OrchestratorError> {
        self.execute_with_metrics(workflow, input, Vec::new()).await
    }

    pub async fn execute_with_metrics(
        &self,
        workflow: WorkflowSpec,
        input: Value,
        metrics: Vec<crate::Metric>,
    ) -> Result<RunRecord, OrchestratorError> {
        workflow.validate()?;
        self.validate_tasks(&workflow)?;
        let manifest = self.build_manifest(&workflow)?;
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let run_id = match RunId::new(generated_id("run", sequence)) {
            Ok(run_id) => run_id,
            Err(error) => {
                return Err(OrchestratorError::Store(AdapterError::new(
                    "runtime",
                    error.to_string(),
                )));
            }
        };
        let mut run = RunRecord::new(run_id.clone(), workflow.clone(), manifest, input);
        run.status = RunStatus::Running;
        self.emit(
            &mut run,
            ExecutionEvent::RunStarted {
                run_id: run_id.clone(),
                workflow_id: workflow.id.clone(),
            },
        )
        .await?;
        self.store
            .save(&run)
            .await
            .map_err(OrchestratorError::Store)?;

        while run.steps.len() < workflow.steps.len() {
            let ready = ready_steps(&workflow, &run);
            if ready.is_empty() {
                run.status = RunStatus::Failed;
                run.error = Some("workflow could not make progress".to_owned());
                break;
            }
            for chunk in ready.chunks(self.options.max_parallel_steps) {
                let results = join_all(chunk.iter().map(|step| {
                    let dependencies = dependency_outputs(step, &run.steps);
                    self.execute_step(&run, &workflow, step.clone(), dependencies)
                }))
                .await;
                for result in results {
                    let step_id = result.record.id.clone();
                    for event in &result.events {
                        self.emit(&mut run, event.clone()).await?;
                    }
                    run.steps.insert(step_id, result.record);
                }
                self.store
                    .save(&run)
                    .await
                    .map_err(OrchestratorError::Store)?;
                if run
                    .steps
                    .values()
                    .any(|step| step.status == StepStatus::Failed)
                {
                    run.status = RunStatus::Failed;
                    run.error = run.steps.values().find_map(|step| step.error.clone());
                    break;
                }
            }
            if run.status == RunStatus::Failed {
                break;
            }
        }

        run.metrics = metrics;
        run.gate_decisions = crate::evaluate_gates(&run.manifest.evaluation_gates, &run.metrics);
        if run.status != RunStatus::Failed
            && !run.gate_decisions.values().all(|decision| decision.passed)
        {
            run.status = RunStatus::Failed;
            run.error = Some("one or more evaluation gates failed".to_owned());
        }
        if run.status != RunStatus::Failed {
            run.status = RunStatus::Succeeded;
        }
        let final_status = run.status.clone();
        self.emit(
            &mut run,
            ExecutionEvent::RunFinished {
                run_id,
                status: final_status,
            },
        )
        .await?;
        self.store
            .save(&run)
            .await
            .map_err(OrchestratorError::Store)?;
        Ok(run)
    }

    fn validate_tasks(&self, workflow: &WorkflowSpec) -> Result<(), WorkflowError> {
        for step in &workflow.steps {
            if !self.options.worker.supports(&step.policy) {
                return Err(WorkflowError::UnsatisfiedPolicy {
                    workflow: workflow.id.clone(),
                    step: step.id.clone(),
                });
            }
            let task =
                self.registry
                    .get(&step.task)
                    .ok_or_else(|| WorkflowError::UnregisteredTask {
                        task: step.task.clone(),
                    })?;
            if !self.options.worker.supports_task(task.spec(), &step.policy) {
                return Err(WorkflowError::UnsatisfiedPolicy {
                    workflow: workflow.id.clone(),
                    step: step.id.clone(),
                });
            }
            if task.spec().id != step.task {
                return Err(WorkflowError::TaskIdMismatch {
                    workflow: workflow.id.clone(),
                    step: step.id.clone(),
                    task: step.task.clone(),
                });
            }
            if let Some(fallback) = &step.policy.fallback_task {
                let fallback_task =
                    self.registry
                        .get(fallback)
                        .ok_or_else(|| WorkflowError::UnregisteredTask {
                            task: fallback.clone(),
                        })?;
                if !self
                    .options
                    .worker
                    .supports_task(fallback_task.spec(), &step.policy)
                {
                    return Err(WorkflowError::UnsatisfiedPolicy {
                        workflow: workflow.id.clone(),
                        step: step.id.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    fn build_manifest(&self, workflow: &WorkflowSpec) -> Result<RunManifest, WorkflowError> {
        let mut tasks = BTreeMap::new();
        for step in &workflow.steps {
            let task =
                self.registry
                    .get(&step.task)
                    .ok_or_else(|| WorkflowError::UnregisteredTask {
                        task: step.task.clone(),
                    })?;
            tasks.insert(task.spec().id.clone(), task.spec().clone());
            if let Some(fallback) = &step.policy.fallback_task {
                let task =
                    self.registry
                        .get(fallback)
                        .ok_or_else(|| WorkflowError::UnregisteredTask {
                            task: fallback.clone(),
                        })?;
                tasks.insert(task.spec().id.clone(), task.spec().clone());
            }
        }
        Ok(
            RunManifest::new(workflow.id.clone(), workflow.version.clone(), tasks)
                .with_evaluation_gates(workflow.evaluation_gates.clone())
                .with_provenance(self.options.provenance.clone()),
        )
    }

    async fn execute_step(
        &self,
        run: &RunRecord,
        workflow: &WorkflowSpec,
        step: crate::StepSpec,
        dependencies: Map<String, Value>,
    ) -> StepRunResult {
        let mut events = Vec::new();
        let mut selected_task = step.task.clone();
        let mut used_fallback = false;
        let mut fallback_attempted = false;
        let max_attempts = step.policy.retry.max_attempts.max(1);
        let mut attempts = 0;
        let mut last_error = None;
        loop {
            let task = match self.registry.get(&selected_task) {
                Some(task) => task,
                None => {
                    last_error = Some(format!("task {selected_task} is not registered"));
                    break;
                }
            };
            if attempts >= max_attempts && (!used_fallback || fallback_attempted) {
                break;
            }
            attempts += 1;
            if used_fallback {
                fallback_attempted = true;
            }
            let attempt = attempts;
            events.push(ExecutionEvent::StepStarted {
                run_id: run.run_id.clone(),
                step_id: step.id.clone(),
                task: selected_task.clone(),
                attempt,
            });
            let context = TaskContext {
                run_id: run.run_id.clone(),
                workflow_id: workflow.id.clone(),
                step_id: step.id.clone(),
                attempt,
                task: task.spec().clone(),
                worker: Some(self.options.worker.id.clone()),
            };
            let input = build_step_input(&run.input, &dependencies);
            let result = if let Some(timeout_ms) = step.policy.timeout_ms {
                match timeout(
                    Duration::from_millis(timeout_ms),
                    task.execute(input, context),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(TaskError::Timeout { timeout_ms }),
                }
            } else {
                task.execute(input, context).await
            };
            let terminal_failure = match result {
                Ok(output) => {
                    events.push(ExecutionEvent::StepSucceeded {
                        run_id: run.run_id.clone(),
                        step_id: step.id.clone(),
                        attempts: attempt,
                    });
                    return StepRunResult {
                        record: StepRecord {
                            id: step.id,
                            task: selected_task,
                            status: StepStatus::Succeeded,
                            attempts: attempt,
                            output: Some(output),
                            error: None,
                            fallback_used: used_fallback,
                            lease: None,
                            history: Vec::new(),
                            artifacts: Vec::new(),
                            next_attempt_at_ms: None,
                        },
                        events,
                    };
                }
                Err(error) if error.retryable() && attempt < max_attempts => {
                    events.push(ExecutionEvent::StepRetrying {
                        run_id: run.run_id.clone(),
                        step_id: step.id.clone(),
                        attempt,
                        error: error.to_string(),
                    });
                    let delay = step.policy.retry.delay.for_attempt(attempt);
                    if !delay.is_zero() {
                        sleep(delay).await;
                    }
                    last_error = Some(error.to_string());
                    continue;
                }
                Err(error) => {
                    last_error = Some(error.to_string());
                    used_fallback || attempt >= max_attempts
                }
            };
            if !used_fallback {
                if let Some(fallback) = &step.policy.fallback_task {
                    used_fallback = true;
                    selected_task = fallback.clone();
                    events.push(ExecutionEvent::StepFallbackSelected {
                        run_id: run.run_id.clone(),
                        step_id: step.id.clone(),
                        task: selected_task.clone(),
                    });
                    continue;
                }
            }
            if terminal_failure {
                break;
            }
        }
        let error = last_error.unwrap_or_else(|| "task failed without an error".to_owned());
        events.push(ExecutionEvent::StepFailed {
            run_id: run.run_id.clone(),
            step_id: step.id.clone(),
            attempts,
            error: error.clone(),
        });
        StepRunResult {
            record: StepRecord {
                id: step.id,
                task: selected_task,
                status: StepStatus::Failed,
                attempts,
                output: None,
                error: Some(error),
                fallback_used: used_fallback,
                lease: None,
                history: Vec::new(),
                artifacts: Vec::new(),
                next_attempt_at_ms: None,
            },
            events,
        }
    }

    async fn emit(
        &self,
        run: &mut RunRecord,
        event: ExecutionEvent,
    ) -> Result<(), OrchestratorError> {
        run.events.push(event.clone());
        run.revision = run.revision.saturating_add(1);
        self.events
            .emit(event)
            .await
            .map_err(OrchestratorError::EventSink)
    }
}

pub type Executor = InMemoryExecutor;

struct StepRunResult {
    record: StepRecord,
    events: Vec<ExecutionEvent>,
}

fn ready_steps(workflow: &WorkflowSpec, run: &RunRecord) -> Vec<crate::StepSpec> {
    workflow
        .steps
        .iter()
        .filter(|step| {
            !run.steps.contains_key(&step.id)
                && step.depends_on.iter().all(|dependency| {
                    run.steps
                        .get(dependency)
                        .is_some_and(|record| record.status == StepStatus::Succeeded)
                })
        })
        .cloned()
        .collect()
}

fn dependency_outputs(
    step: &crate::StepSpec,
    records: &BTreeMap<StepId, StepRecord>,
) -> Map<String, Value> {
    step.depends_on
        .iter()
        .filter_map(|dependency| {
            records
                .get(dependency)
                .and_then(|record| record.output.clone())
                .map(|output| (dependency.to_string(), output))
        })
        .collect()
}

fn build_step_input(run_input: &Value, dependencies: &Map<String, Value>) -> Value {
    let mut input = Map::new();
    input.insert("run_input".to_owned(), run_input.clone());
    input.insert(
        "dependencies".to_owned(),
        Value::Object(dependencies.clone()),
    );
    Value::Object(input)
}
