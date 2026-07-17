use crate::ids::generated_id;
use crate::{
    AdapterError, ExecutionEvent, InMemoryRunStore, OrchestratorError, RunId, RunManifest,
    RunProvenance, RunRecord, RunRepository, RunRequest, RunStatus, StepClaim, StepCompletion,
    TaskContext, TaskError, TaskRegistry, WorkerProfile, WorkflowError, WorkflowSpec,
};
use futures::future::join_all;
use serde_json::Value;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::time::{Duration, Instant, sleep, timeout};

/// Options for a worker driving a durable repository.
#[derive(Clone, Debug)]
pub struct DurableExecutorOptions {
    pub worker: WorkerProfile,
    pub provenance: RunProvenance,
    pub lease_ms: u64,
    pub max_parallel_steps: usize,
    pub poll_interval_ms: u64,
    /// Optional bound for waiting on another worker or a persisted retry.
    /// `None` keeps resuming until the run reaches a terminal state.
    pub max_idle_time_ms: Option<u64>,
}

impl Default for DurableExecutorOptions {
    fn default() -> Self {
        Self {
            worker: WorkerProfile::default(),
            provenance: RunProvenance::default(),
            lease_ms: 30_000,
            max_parallel_steps: 8,
            poll_interval_ms: 10,
            max_idle_time_ms: None,
        }
    }
}

/// A crash-recoverable executor. The repository owns all durable state
/// transitions; this type only claims work, invokes tasks, and commits results.
pub struct DurableExecutor {
    registry: Arc<dyn TaskRegistry>,
    repository: Arc<dyn RunRepository>,
    options: DurableExecutorOptions,
    sequence: AtomicU64,
}

impl DurableExecutor {
    #[must_use]
    pub fn new(registry: Arc<dyn TaskRegistry>, repository: Arc<dyn RunRepository>) -> Self {
        Self {
            registry,
            repository,
            options: DurableExecutorOptions::default(),
            sequence: AtomicU64::new(0),
        }
    }

    /// Constructs a worker backed by the in-memory repository for local runs.
    #[must_use]
    pub fn in_memory(registry: Arc<dyn TaskRegistry>) -> Self {
        Self::new(registry, Arc::new(InMemoryRunStore::default()))
    }

    #[must_use]
    pub fn with_options(mut self, options: DurableExecutorOptions) -> Self {
        self.options = DurableExecutorOptions {
            lease_ms: options.lease_ms.max(1),
            max_parallel_steps: options.max_parallel_steps.max(1),
            poll_interval_ms: options.poll_interval_ms.max(1),
            max_idle_time_ms: options.max_idle_time_ms,
            worker: options.worker,
            provenance: options.provenance,
        };
        self
    }

    /// Submits a run and returns its durable record without driving it.
    pub async fn submit(
        &self,
        workflow: WorkflowSpec,
        input: Value,
    ) -> Result<RunRecord, OrchestratorError> {
        self.submit_with_idempotency(workflow, input, None).await
    }

    /// Submits a run with an optional idempotency key. Repeating the same key
    /// and payload returns the original run instead of creating a duplicate.
    pub async fn submit_with_idempotency(
        &self,
        workflow: WorkflowSpec,
        input: Value,
        idempotency_key: Option<crate::IdempotencyKey>,
    ) -> Result<RunRecord, OrchestratorError> {
        workflow.validate()?;
        self.validate_workflow(&workflow)?;
        let manifest = self.build_manifest(&workflow)?;
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let run_id = RunId::new(generated_id("run", sequence)).map_err(|error| {
            OrchestratorError::Store(AdapterError::new("runtime", error.to_string()))
        })?;
        self.repository
            .create_run(RunRequest {
                run_id,
                workflow,
                manifest,
                input,
                idempotency_key,
            })
            .await
            .map_err(OrchestratorError::from)
    }

    /// Drives a submitted run until it reaches a terminal state.
    pub async fn execute(
        &self,
        workflow: WorkflowSpec,
        input: Value,
    ) -> Result<RunRecord, OrchestratorError> {
        self.execute_with_metrics(workflow, input, Vec::new()).await
    }

    /// Drives a run and applies its configured evaluation gates to the supplied
    /// metrics before returning the terminal record.
    pub async fn execute_with_metrics(
        &self,
        workflow: WorkflowSpec,
        input: Value,
        metrics: Vec<crate::Metric>,
    ) -> Result<RunRecord, OrchestratorError> {
        let run = self.submit(workflow, input).await?;
        self.resume(&run.run_id, metrics).await
    }

    /// Resumes a previously submitted or recovered run from its durable state.
    pub async fn resume(
        &self,
        run_id: &RunId,
        metrics: Vec<crate::Metric>,
    ) -> Result<RunRecord, OrchestratorError> {
        if self.repository.load_run(run_id).await?.is_none() {
            return Err(OrchestratorError::Repository(
                crate::RepositoryError::RunNotFound {
                    run: run_id.clone(),
                },
            ));
        }
        self.drive(run_id.clone(), metrics).await
    }

    /// Requests cancellation through the repository state machine.
    pub async fn cancel(&self, run_id: &RunId) -> Result<RunRecord, OrchestratorError> {
        self.repository
            .cancel_run(run_id)
            .await
            .map_err(OrchestratorError::from)
    }

    /// Streams the durable event log after the supplied sequence number.
    pub async fn events_since(
        &self,
        run_id: &RunId,
        sequence: u64,
    ) -> Result<Vec<crate::EventEnvelope>, OrchestratorError> {
        self.repository
            .events_since(run_id, sequence)
            .await
            .map_err(OrchestratorError::from)
    }

    async fn drive(
        &self,
        run_id: RunId,
        metrics: Vec<crate::Metric>,
    ) -> Result<RunRecord, OrchestratorError> {
        let mut idle_started = Instant::now();
        loop {
            let mut claims = Vec::new();
            for _ in 0..self.options.max_parallel_steps {
                let Some(claim) = self
                    .repository
                    .claim_next_step(&run_id, &self.options.worker, self.options.lease_ms)
                    .await
                    .map_err(OrchestratorError::from)?
                else {
                    break;
                };
                claims.push(claim);
            }

            if claims.is_empty() {
                let run = self
                    .repository
                    .finish_run(&run_id, metrics.clone())
                    .await
                    .map_err(OrchestratorError::from)?;
                if !matches!(run.status, RunStatus::Running | RunStatus::Pending) {
                    return Ok(run);
                }
                self.repository
                    .recover_expired()
                    .await
                    .map_err(OrchestratorError::from)?;
                if self.options.max_idle_time_ms.is_some_and(|max_idle| {
                    idle_started.elapsed() >= Duration::from_millis(max_idle)
                }) {
                    return Err(OrchestratorError::Repository(
                        crate::RepositoryError::Conflict { run: run_id },
                    ));
                }
                sleep(Duration::from_millis(self.options.poll_interval_ms)).await;
                continue;
            }
            idle_started = Instant::now();

            let results = join_all(claims.iter().map(|claim| self.execute_claim(claim))).await;
            for (claim, completion) in claims.iter().zip(results) {
                self.repository
                    .complete_step(claim, completion?)
                    .await
                    .map_err(OrchestratorError::from)?;
            }
        }
    }

    async fn execute_claim(&self, claim: &StepClaim) -> Result<StepCompletion, OrchestratorError> {
        self.repository
            .record_event(
                &claim.run_id,
                ExecutionEvent::StepStarted {
                    run_id: claim.run_id.clone(),
                    step_id: claim.step.id.clone(),
                    task: claim.task.id.clone(),
                    attempt: claim.attempt,
                },
            )
            .await
            .map_err(OrchestratorError::from)?;
        let task =
            self.registry
                .get(&claim.task.id)
                .ok_or_else(|| WorkflowError::UnregisteredTask {
                    task: claim.task.id.clone(),
                })?;
        let repository = Arc::clone(&self.repository);
        let heartbeat_claim = claim.clone();
        let heartbeat_interval = (self.options.lease_ms / 3).max(1);
        let heartbeat = tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(heartbeat_interval)).await;
                if repository
                    .renew_step(&heartbeat_claim, heartbeat_interval.saturating_mul(3))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        let context = TaskContext {
            run_id: claim.run_id.clone(),
            workflow_id: claim.workflow_id.clone(),
            step_id: claim.step.id.clone(),
            attempt: claim.attempt,
            task: claim.task.clone(),
            worker: Some(claim.worker.clone()),
        };
        let execute = task.execute(claim.input.clone(), context);
        let result = if let Some(timeout_ms) = claim.step.policy.timeout_ms {
            timeout(Duration::from_millis(timeout_ms), execute)
                .await
                .unwrap_or(Err(TaskError::Timeout { timeout_ms }))
        } else {
            execute.await
        };
        heartbeat.abort();
        let _ = heartbeat.await;
        match result {
            Ok(output) => Ok(StepCompletion::succeeded(
                claim.run_id.clone(),
                claim.step.id.clone(),
                output,
                claim.attempt,
            )),
            Err(error)
                if error.retryable()
                    && claim.attempt < claim.step.policy.retry.max_attempts.max(1) =>
            {
                let completion = StepCompletion::retrying(
                    claim.run_id.clone(),
                    claim.step.id.clone(),
                    claim.attempt,
                    error.to_string(),
                );
                let delay_ms = claim
                    .step
                    .policy
                    .retry
                    .delay
                    .for_attempt(claim.attempt)
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64;
                Ok(completion.with_retry_after_ms(delay_ms))
            }
            Err(error) if !claim.fallback_used => match claim.step.policy.fallback_task.clone() {
                Some(fallback) => Ok(StepCompletion::fallback(
                    claim.run_id.clone(),
                    claim.step.id.clone(),
                    fallback,
                    error.to_string(),
                )),
                None => Ok(StepCompletion::failed(
                    claim.run_id.clone(),
                    claim.step.id.clone(),
                    claim.attempt,
                    error.to_string(),
                )),
            },
            Err(error) => Ok(StepCompletion::failed(
                claim.run_id.clone(),
                claim.step.id.clone(),
                claim.attempt,
                error.to_string(),
            )),
        }
    }

    fn validate_workflow(&self, workflow: &WorkflowSpec) -> Result<(), WorkflowError> {
        for step in &workflow.steps {
            let task =
                self.registry
                    .get(&step.task)
                    .ok_or_else(|| WorkflowError::UnregisteredTask {
                        task: step.task.clone(),
                    })?;
            if task.spec().id != step.task {
                return Err(WorkflowError::TaskIdMismatch {
                    workflow: workflow.id.clone(),
                    step: step.id.clone(),
                    task: step.task.clone(),
                });
            }
            if let Some(fallback) = &step.policy.fallback_task {
                self.registry
                    .get(fallback)
                    .ok_or_else(|| WorkflowError::UnregisteredTask {
                        task: fallback.clone(),
                    })?;
            }
        }
        Ok(())
    }

    fn build_manifest(&self, workflow: &WorkflowSpec) -> Result<RunManifest, WorkflowError> {
        let mut tasks = std::collections::BTreeMap::new();
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
}
