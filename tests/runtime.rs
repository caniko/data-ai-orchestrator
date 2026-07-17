use async_trait::async_trait;
use data_ai_orchestrator::*;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

struct CopyTask {
    spec: TaskSpec,
}

#[async_trait]
impl Task for CopyTask {
    fn spec(&self) -> &TaskSpec {
        &self.spec
    }

    async fn execute(
        &self,
        input: Value,
        _context: TaskContext,
    ) -> std::result::Result<Value, TaskError> {
        Ok(json!({ "copied": input }))
    }
}

struct AddTask {
    spec: TaskSpec,
}

#[async_trait]
impl Task for AddTask {
    fn spec(&self) -> &TaskSpec {
        &self.spec
    }

    async fn execute(
        &self,
        input: Value,
        _context: TaskContext,
    ) -> std::result::Result<Value, TaskError> {
        let value = input["run_input"]["value"]
            .as_i64()
            .ok_or_else(|| TaskError::InvalidInput("value must be an integer".into()))?;
        Ok(json!({ "value": value + 1 }))
    }
}

struct FlakyTask {
    spec: TaskSpec,
    attempts: Arc<Mutex<u32>>,
}

struct PermanentFailureTask {
    spec: TaskSpec,
}

struct CountingPermanentFailureTask {
    spec: TaskSpec,
    attempts: Arc<Mutex<u32>>,
}

#[async_trait]
impl Task for PermanentFailureTask {
    fn spec(&self) -> &TaskSpec {
        &self.spec
    }

    async fn execute(
        &self,
        _input: Value,
        _context: TaskContext,
    ) -> std::result::Result<Value, TaskError> {
        Err(TaskError::Permanent("primary unavailable".into()))
    }
}

#[async_trait]
impl Task for CountingPermanentFailureTask {
    fn spec(&self) -> &TaskSpec {
        &self.spec
    }

    async fn execute(
        &self,
        _input: Value,
        _context: TaskContext,
    ) -> std::result::Result<Value, TaskError> {
        *self.attempts.lock().expect("test mutex") += 1;
        Err(TaskError::Permanent("unavailable".into()))
    }
}

#[async_trait]
impl Task for FlakyTask {
    fn spec(&self) -> &TaskSpec {
        &self.spec
    }

    async fn execute(
        &self,
        _input: Value,
        _context: TaskContext,
    ) -> std::result::Result<Value, TaskError> {
        let mut attempts = self.attempts.lock().expect("test mutex");
        *attempts += 1;
        if *attempts < 2 {
            Err(TaskError::Transient("try again".into()))
        } else {
            Ok(json!({ "ready": true }))
        }
    }
}

fn spec(id: &str) -> TaskSpec {
    TaskSpec::new(id, "1", id).expect("valid task spec")
}

#[tokio::test]
async fn runs_dependency_graph_and_persists_events() {
    let mut registry = InMemoryTaskRegistry::default();
    registry
        .register(AddTask { spec: spec("add") })
        .expect("register add");
    registry
        .register(CopyTask { spec: spec("copy") })
        .expect("register copy");
    let events = Arc::new(InMemoryEventSink::default());
    let executor = InMemoryExecutor::new(Arc::new(registry)).with_event_sink(events.clone());

    let mut workflow = WorkflowSpec::new("demo", "1").expect("valid workflow");
    workflow
        .add_step(StepSpec::new("add-step", "add").expect("valid step"))
        .expect("add step");
    workflow
        .add_step(
            StepSpec::new("copy-step", "copy")
                .expect("valid step")
                .depends_on("add-step")
                .expect("dependency"),
        )
        .expect("add step");

    let run = executor
        .execute(workflow, json!({ "value": 4 }))
        .await
        .expect("run succeeds");
    assert_eq!(run.status, RunStatus::Succeeded);
    assert_eq!(run.steps.len(), 2);
    assert_eq!(run.manifest.workflow_id.as_str(), "demo");
    assert_eq!(run.manifest.workflow_version, "1");
    assert_eq!(run.manifest.tasks.len(), 2);
    assert_eq!(
        run.manifest.tasks[&TaskId::new("add").expect("valid id")].version,
        "1"
    );
    assert!(run.events.iter().any(|event| matches!(
        event,
        ExecutionEvent::RunFinished {
            status: RunStatus::Succeeded,
            ..
        }
    )));
    assert_eq!(events.events().await.len(), run.events.len());
}

#[tokio::test]
async fn retries_transient_failures() {
    let attempts = Arc::new(Mutex::new(0));
    let mut registry = InMemoryTaskRegistry::default();
    registry
        .register(FlakyTask {
            spec: spec("flaky"),
            attempts: attempts.clone(),
        })
        .expect("register flaky");
    let executor = InMemoryExecutor::new(Arc::new(registry));
    let mut workflow = WorkflowSpec::new("retry", "1").expect("valid workflow");
    let mut step = StepSpec::new("flaky-step", "flaky").expect("valid step");
    step.policy.retry = RetryPolicy {
        max_attempts: 2,
        delay: RetryDelay::none(),
    };
    workflow.add_step(step).expect("add step");

    let run = executor
        .execute(workflow, json!({}))
        .await
        .expect("run succeeds");
    assert_eq!(run.status, RunStatus::Succeeded);
    assert_eq!(*attempts.lock().expect("test mutex"), 2);
    assert!(
        run.events
            .iter()
            .any(|event| matches!(event, ExecutionEvent::StepRetrying { .. }))
    );
}

#[tokio::test]
async fn retries_primary_before_selecting_fallback() {
    let attempts = Arc::new(Mutex::new(0));
    let mut registry = InMemoryTaskRegistry::default();
    registry
        .register(FlakyTask {
            spec: spec("primary"),
            attempts: attempts.clone(),
        })
        .expect("register primary");
    registry
        .register(CopyTask {
            spec: spec("fallback"),
        })
        .expect("register fallback");
    let executor = InMemoryExecutor::new(Arc::new(registry));
    let mut workflow = WorkflowSpec::new("retry-before-fallback", "1").expect("workflow");
    let mut step = StepSpec::new("step", "primary").expect("step");
    step.policy.retry = RetryPolicy {
        max_attempts: 2,
        delay: RetryDelay::none(),
    };
    step.policy.fallback_task = Some(TaskId::new("fallback").expect("fallback id"));
    workflow.add_step(step).expect("add step");

    let run = executor
        .execute(workflow, json!({}))
        .await
        .expect("primary retry succeeds");
    let record = run.steps.values().next().expect("step record");
    assert_eq!(record.status, StepStatus::Succeeded);
    assert_eq!(record.task, TaskId::new("primary").expect("primary id"));
    assert_eq!(record.attempts, 2);
    assert_eq!(*attempts.lock().expect("test mutex"), 2);
    assert!(
        !run.events
            .iter()
            .any(|event| matches!(event, ExecutionEvent::StepFallbackSelected { .. }))
    );
}

#[tokio::test]
async fn selects_a_registered_fallback_after_primary_failure() {
    let mut registry = InMemoryTaskRegistry::default();
    registry
        .register(PermanentFailureTask {
            spec: spec("primary"),
        })
        .expect("register primary");
    registry
        .register(CopyTask {
            spec: spec("fallback"),
        })
        .expect("register fallback");
    let executor = InMemoryExecutor::new(Arc::new(registry));
    let mut workflow = WorkflowSpec::new("fallback", "1").expect("valid workflow");
    let mut step = StepSpec::new("step", "primary").expect("valid step");
    step.policy.retry = RetryPolicy::never();
    step.policy.fallback_task = Some(TaskId::new("fallback").expect("valid id"));
    workflow.add_step(step).expect("add step");

    let run = executor
        .execute(workflow, json!({ "value": 4 }))
        .await
        .expect("fallback succeeds");
    assert_eq!(run.status, RunStatus::Succeeded);
    assert_eq!(
        run.steps[&StepId::new("step").expect("valid id")].task,
        TaskId::new("fallback").expect("valid id")
    );
    assert!(run.events.iter().any(|event| matches!(
        event,
        ExecutionEvent::StepFallbackSelected { task, .. } if task.as_str() == "fallback"
    )));
}

#[tokio::test]
async fn fallback_shares_the_step_attempt_budget() {
    let primary_attempts = Arc::new(Mutex::new(0));
    let fallback_attempts = Arc::new(Mutex::new(0));
    let mut registry = InMemoryTaskRegistry::default();
    registry
        .register(CountingPermanentFailureTask {
            spec: spec("primary"),
            attempts: primary_attempts.clone(),
        })
        .expect("register primary");
    registry
        .register(CountingPermanentFailureTask {
            spec: spec("fallback"),
            attempts: fallback_attempts.clone(),
        })
        .expect("register fallback");
    let executor = InMemoryExecutor::new(Arc::new(registry));
    let mut workflow = WorkflowSpec::new("shared-budget", "1").expect("workflow");
    let mut step = StepSpec::new("step", "primary").expect("step");
    step.policy.retry = RetryPolicy {
        max_attempts: 2,
        delay: RetryDelay::none(),
    };
    step.policy.fallback_task = Some(TaskId::new("fallback").expect("fallback id"));
    workflow.add_step(step).expect("add step");

    let run = executor
        .execute(workflow, json!({}))
        .await
        .expect("run completes");
    assert_eq!(run.status, RunStatus::Failed);
    assert_eq!(*primary_attempts.lock().expect("test mutex"), 1);
    assert_eq!(*fallback_attempts.lock().expect("test mutex"), 1);
    assert_eq!(run.steps.values().next().unwrap().attempts, 2);
    assert!(
        run.events
            .iter()
            .any(|event| matches!(event, ExecutionEvent::StepFailed { attempts: 2, .. }))
    );
}

#[test]
fn rejects_cycles_and_unknown_dependencies() {
    let mut workflow = WorkflowSpec::new("invalid", "1").expect("valid workflow");
    let a = StepSpec::new("a", "task-a")
        .expect("valid step")
        .depends_on("b")
        .expect("dependency");
    let b = StepSpec::new("b", "task-b")
        .expect("valid step")
        .depends_on("a")
        .expect("dependency");
    workflow.add_step(a).expect("add step");
    workflow.add_step(b).expect("add step");
    assert!(matches!(
        workflow.validate(),
        Err(WorkflowError::DependencyCycle { .. })
    ));

    let mut unknown = WorkflowSpec::new("unknown", "1").expect("valid workflow");
    unknown
        .add_step(
            StepSpec::new("a", "task-a")
                .expect("valid step")
                .depends_on("missing")
                .expect("dependency"),
        )
        .expect("add step");
    assert!(matches!(
        unknown.validate(),
        Err(WorkflowError::UnknownDependency { .. })
    ));
}

#[test]
fn evaluates_quality_gates_without_missing_metric_passes() {
    let gate = EvaluationGate {
        metric: "ndcg@10".into(),
        threshold: 0.8,
        direction: MetricDirection::AtLeast,
    };
    assert!(
        gate.evaluate(&[Metric {
            name: "ndcg@10".into(),
            value: 0.81
        }])
        .passed
    );
    assert!(!gate.evaluate(&[]).passed);
}

#[tokio::test]
async fn executor_applies_quality_gates_to_run_status() {
    let mut registry = InMemoryTaskRegistry::default();
    registry
        .register(CopyTask { spec: spec("copy") })
        .expect("register copy");
    let executor = InMemoryExecutor::new(Arc::new(registry));
    let mut workflow = WorkflowSpec::new("gated-run", "1").expect("workflow");
    workflow
        .add_step(StepSpec::new("copy", "copy").expect("step"))
        .expect("add step");
    workflow.add_gate(EvaluationGate {
        metric: "accuracy".to_owned(),
        threshold: 0.9,
        direction: MetricDirection::AtLeast,
    });
    let run = executor
        .execute_with_metrics(
            workflow,
            json!({}),
            vec![Metric {
                name: "accuracy".to_owned(),
                value: 0.5,
            }],
        )
        .await
        .expect("run is recorded");
    assert_eq!(run.status, RunStatus::Failed);
    assert!(!run.gate_decisions["accuracy"].passed);
}
