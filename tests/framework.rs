use async_trait::async_trait;
use data_ai_orchestrator::{
    ArtifactMetadata, DurableExecutor, ExecutionEvent, IdempotencyKey, InMemoryArtifactStore,
    InMemoryRunStore, InMemoryTaskRegistry, Metric, RunManifest, RunRepository, RunRequest,
    RunStatus, RunStore, StepCompletion, StepSpec, StepStatus, Task, TaskContext, TaskError,
    TaskSpec, TypedTask, TypedTaskAdapter, WorkOutcome, WorkerProfile, WorkflowSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tokio::time::{Duration, sleep, timeout};

struct Echo {
    spec: TaskSpec,
}

#[async_trait]
impl Task for Echo {
    fn spec(&self) -> &TaskSpec {
        &self.spec
    }

    async fn execute(&self, input: Value, _context: TaskContext) -> Result<Value, TaskError> {
        Ok(input)
    }
}

struct PermanentFailure {
    spec: TaskSpec,
}

#[async_trait]
impl Task for PermanentFailure {
    fn spec(&self) -> &TaskSpec {
        &self.spec
    }

    async fn execute(&self, _input: Value, _context: TaskContext) -> Result<Value, TaskError> {
        Err(TaskError::Permanent("primary unavailable".to_owned()))
    }
}

#[derive(Deserialize)]
struct TypedInput {
    value: u32,
}

#[derive(Serialize)]
struct TypedOutput {
    doubled: u32,
}

struct Doubler {
    spec: TaskSpec,
}

#[derive(Deserialize)]
struct EnvelopeInput {
    run_input: Value,
    dependencies: BTreeMap<String, Value>,
}

struct DependencyReader {
    spec: TaskSpec,
}

#[async_trait]
impl TypedTask for DependencyReader {
    type Input = EnvelopeInput;
    type Output = Value;

    fn spec(&self) -> &TaskSpec {
        &self.spec
    }

    async fn execute_typed(
        &self,
        input: Self::Input,
        _context: TaskContext,
    ) -> Result<Self::Output, TaskError> {
        Ok(json!({ "run_input": input.run_input, "dependencies": input.dependencies }))
    }
}

#[async_trait]
impl TypedTask for Doubler {
    type Input = TypedInput;
    type Output = TypedOutput;

    fn spec(&self) -> &TaskSpec {
        &self.spec
    }

    async fn execute_typed(
        &self,
        input: Self::Input,
        _context: TaskContext,
    ) -> Result<Self::Output, TaskError> {
        Ok(TypedOutput {
            doubled: input.value * 2,
        })
    }
}

fn spec(id: &str) -> TaskSpec {
    TaskSpec::new(id, "1", id).expect("valid task spec")
}

#[tokio::test]
async fn durable_executor_persists_claims_and_events() {
    let mut registry = InMemoryTaskRegistry::default();
    registry
        .register(Echo { spec: spec("echo") })
        .expect("register task");
    let executor = DurableExecutor::in_memory(Arc::new(registry));
    let mut workflow = WorkflowSpec::new("durable", "1").expect("valid workflow");
    workflow
        .add_step(StepSpec::new("echo-step", "echo").expect("valid step"))
        .expect("add step");

    let run = executor
        .execute(workflow, json!({ "value": 7 }))
        .await
        .expect("durable run succeeds");
    assert_eq!(run.status, RunStatus::Succeeded);
    assert_eq!(
        run.steps[&data_ai_orchestrator::StepId::new("echo-step").unwrap()].attempts,
        1
    );
    assert!(
        run.event_log
            .windows(2)
            .all(|events| events[0].sequence < events[1].sequence)
    );
    assert!(
        run.event_log
            .iter()
            .any(|event| matches!(event.event, ExecutionEvent::StepStarted { .. }))
    );
}

#[tokio::test]
async fn durable_worker_discovers_and_executes_active_runs() {
    let mut registry = InMemoryTaskRegistry::default();
    registry
        .register(Echo { spec: spec("echo") })
        .expect("register task");
    let executor = DurableExecutor::in_memory(Arc::new(registry));
    let mut workflow = WorkflowSpec::new("worker-discovery", "1").expect("valid workflow");
    workflow
        .add_step(StepSpec::new("echo-step", "echo").expect("valid step"))
        .expect("add step");
    let submitted = executor
        .submit(workflow, json!({ "value": 9 }))
        .await
        .expect("submit run");

    let outcome = executor.work_once(Vec::new()).await.expect("worker tick");
    assert_eq!(
        outcome,
        WorkOutcome::Progressed {
            run_id: submitted.run_id,
            status: RunStatus::Succeeded,
        }
    );
    assert_eq!(
        executor
            .work_once(Vec::new())
            .await
            .expect("idle worker tick"),
        WorkOutcome::Idle
    );
}

#[tokio::test]
async fn durable_executor_uses_fallback_and_idempotency() {
    let mut registry = InMemoryTaskRegistry::default();
    registry
        .register(PermanentFailure {
            spec: spec("primary"),
        })
        .expect("register primary");
    registry
        .register(Echo {
            spec: spec("fallback"),
        })
        .expect("register fallback");
    let executor = DurableExecutor::in_memory(Arc::new(registry));
    let mut workflow = WorkflowSpec::new("fallback", "1").expect("valid workflow");
    let mut step = StepSpec::new("step", "primary").expect("valid step");
    step.policy.retry = data_ai_orchestrator::RetryPolicy::never();
    step.policy.fallback_task = Some(data_ai_orchestrator::TaskId::new("fallback").unwrap());
    workflow.add_step(step).expect("add step");

    let key = IdempotencyKey::new("same-input").unwrap();
    let first = executor
        .submit_with_idempotency(workflow.clone(), json!({ "value": 1 }), Some(key.clone()))
        .await
        .expect("submit");
    let second = executor
        .submit_with_idempotency(workflow, json!({ "value": 1 }), Some(key))
        .await
        .expect("idempotent submit");
    assert_eq!(first.run_id, second.run_id);

    let mut fallback_workflow = WorkflowSpec::new("fallback-run", "1").expect("workflow");
    let mut fallback_step = StepSpec::new("step", "primary").expect("step");
    fallback_step.policy.retry = data_ai_orchestrator::RetryPolicy::never();
    fallback_step.policy.fallback_task =
        Some(data_ai_orchestrator::TaskId::new("fallback").unwrap());
    fallback_workflow.add_step(fallback_step).expect("add step");
    let result = executor
        .execute(fallback_workflow, json!({}))
        .await
        .unwrap();
    assert_eq!(result.status, RunStatus::Succeeded);
}

#[tokio::test]
async fn repository_rejects_stale_leases_and_applies_gates() {
    let store = Arc::new(InMemoryRunStore::default());
    let task = spec("echo");
    let mut workflow = WorkflowSpec::new("repo", "1").unwrap();
    workflow
        .add_step(StepSpec::new("step", "echo").unwrap())
        .unwrap();
    workflow.add_gate(data_ai_orchestrator::EvaluationGate {
        metric: "accuracy".to_owned(),
        threshold: 0.9,
        direction: data_ai_orchestrator::MetricDirection::AtLeast,
    });
    let mut tasks = std::collections::BTreeMap::new();
    tasks.insert(task.id.clone(), task.clone());
    let run_id = data_ai_orchestrator::RunId::new("repo-run").unwrap();
    store
        .create_run(RunRequest {
            run_id: run_id.clone(),
            manifest: RunManifest::new(workflow.id.clone(), "1", tasks)
                .with_evaluation_gates(workflow.evaluation_gates.clone()),
            workflow,
            input: json!({}),
            idempotency_key: None,
        })
        .await
        .unwrap();
    let worker = WorkerProfile::new("worker-a").unwrap();
    let claim = store
        .claim_next_step(&run_id, &worker, 1000)
        .await
        .unwrap()
        .unwrap();
    let stale = data_ai_orchestrator::StepClaim {
        lease_token: claim.lease_token + 1,
        ..claim.clone()
    };
    assert!(matches!(
        store
            .complete_step(
                &stale,
                StepCompletion::succeeded(run_id.clone(), claim.step.id.clone(), json!({}), 1)
            )
            .await,
        Err(data_ai_orchestrator::RepositoryError::LeaseLost { .. })
    ));
    assert!(matches!(
        store
            .complete_step(
                &claim,
                StepCompletion::succeeded(
                    run_id.clone(),
                    claim.step.id.clone(),
                    json!({}),
                    claim.attempt + 1,
                )
            )
            .await,
        Err(data_ai_orchestrator::RepositoryError::InvalidCompletion { .. })
    ));
    store
        .complete_step(
            &claim,
            StepCompletion::succeeded(run_id.clone(), claim.step.id.clone(), json!({}), 1),
        )
        .await
        .unwrap();
    let finished = store
        .finish_run(
            &run_id,
            vec![Metric {
                name: "accuracy".to_owned(),
                value: 0.95,
            }],
        )
        .await
        .unwrap();
    assert_eq!(finished.status, RunStatus::Succeeded);
}

#[tokio::test]
async fn repository_rejects_policy_bypassing_completions() {
    let store = Arc::new(InMemoryRunStore::default());
    let primary = spec("primary");
    let fallback = spec("fallback");
    let mut workflow = WorkflowSpec::new("completion-policy", "1").unwrap();
    let mut step = StepSpec::new("step", "primary").unwrap();
    step.policy.retry = data_ai_orchestrator::RetryPolicy::never();
    step.policy.fallback_task = Some(fallback.id.clone());
    workflow.add_step(step).unwrap();
    let mut tasks = BTreeMap::new();
    tasks.insert(primary.id.clone(), primary);
    tasks.insert(fallback.id.clone(), fallback.clone());
    let run_id = data_ai_orchestrator::RunId::new("completion-policy-run").unwrap();
    store
        .create_run(RunRequest {
            run_id: run_id.clone(),
            workflow: workflow.clone(),
            manifest: RunManifest::new(workflow.id.clone(), "1", tasks),
            input: json!({}),
            idempotency_key: None,
        })
        .await
        .unwrap();
    let worker = WorkerProfile::new("completion-worker").unwrap();
    let claim = store
        .claim_next_step(&run_id, &worker, 1_000)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        store
            .complete_step(
                &claim,
                StepCompletion::retrying(
                    run_id.clone(),
                    claim.step.id.clone(),
                    claim.attempt,
                    "retry after exhaustion".to_owned(),
                ),
            )
            .await,
        Err(data_ai_orchestrator::RepositoryError::InvalidCompletion { .. })
    ));
    assert!(matches!(
        store
            .complete_step(
                &claim,
                StepCompletion::failed(
                    run_id.clone(),
                    claim.step.id.clone(),
                    claim.attempt,
                    "bypass fallback".to_owned(),
                ),
            )
            .await,
        Err(data_ai_orchestrator::RepositoryError::InvalidCompletion { .. })
    ));
    store
        .complete_step(
            &claim,
            StepCompletion::fallback(
                run_id.clone(),
                claim.step.id.clone(),
                fallback.id.clone(),
                "primary failed".to_owned(),
            ),
        )
        .await
        .unwrap();
    let fallback_claim = store
        .claim_next_step(&run_id, &worker, 1_000)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        store
            .complete_step(
                &fallback_claim,
                StepCompletion::fallback(
                    run_id,
                    fallback_claim.step.id.clone(),
                    fallback.id,
                    "fallback again".to_owned(),
                ),
            )
            .await,
        Err(data_ai_orchestrator::RepositoryError::InvalidCompletion { .. })
    ));
}

#[tokio::test]
async fn repository_rejects_inconsistent_event_records() {
    let store = Arc::new(InMemoryRunStore::default());
    let task = spec("event-task");
    let mut workflow = WorkflowSpec::new("event-record", "1").unwrap();
    workflow
        .add_step(StepSpec::new("step", "event-task").unwrap())
        .unwrap();
    let mut tasks = BTreeMap::new();
    tasks.insert(task.id.clone(), task);
    let run_id = data_ai_orchestrator::RunId::new("event-record-run").unwrap();
    store
        .create_run(RunRequest {
            run_id: run_id.clone(),
            workflow: workflow.clone(),
            manifest: RunManifest::new(workflow.id.clone(), "1", tasks),
            input: json!({}),
            idempotency_key: None,
        })
        .await
        .unwrap();
    let worker = WorkerProfile::new("event-worker").unwrap();
    let claim = store
        .claim_next_step(&run_id, &worker, 1_000)
        .await
        .unwrap()
        .unwrap();
    let started = ExecutionEvent::StepStarted {
        run_id: run_id.clone(),
        step_id: claim.step.id.clone(),
        task: claim.task.id.clone(),
        attempt: claim.attempt,
    };
    store.record_event(&run_id, started.clone()).await.unwrap();
    assert!(matches!(
        store.record_event(&run_id, started).await,
        Err(data_ai_orchestrator::RepositoryError::InvalidEvent { .. })
    ));
    store.cancel_run(&run_id).await.unwrap();
    let cancelled = store.load_run(&run_id).await.unwrap().unwrap();
    let record = cancelled.steps.values().next().unwrap();
    assert_eq!(record.status, StepStatus::Cancelled);
    assert_eq!(record.history[0].status, StepStatus::Cancelled);
    assert!(record.history[0].finished_at_ms.is_some());
    assert!(matches!(
        store
            .record_event(
                &run_id,
                ExecutionEvent::RunFinished {
                    run_id: run_id.clone(),
                    status: RunStatus::Succeeded,
                },
            )
            .await,
        Err(data_ai_orchestrator::RepositoryError::InvalidEvent { .. })
    ));
}

#[tokio::test]
async fn repository_rejects_unsupported_snapshot_schema_versions() {
    let store = InMemoryRunStore::default();
    let task = spec("schema-task");
    let mut workflow = WorkflowSpec::new("schema-record", "1").unwrap();
    workflow
        .add_step(StepSpec::new("step", "schema-task").unwrap())
        .unwrap();
    let mut tasks = BTreeMap::new();
    tasks.insert(task.id.clone(), task);
    let run_id = data_ai_orchestrator::RunId::new("schema-record-run").unwrap();
    store
        .create_run(RunRequest {
            run_id: run_id.clone(),
            workflow,
            manifest: RunManifest::new(
                data_ai_orchestrator::WorkflowId::new("schema-record").unwrap(),
                "1",
                tasks,
            ),
            input: json!({}),
            idempotency_key: None,
        })
        .await
        .unwrap();

    let mut run = store.load_run(&run_id).await.unwrap().unwrap();
    run.schema_version += 1;
    let error = store
        .save(&run)
        .await
        .expect_err("future run schema must be rejected");
    assert!(error.message.contains("unsupported run schema version"));

    let mut run = store.load_run(&run_id).await.unwrap().unwrap();
    run.event_log[0].schema_version += 1;
    let error = store
        .save(&run)
        .await
        .expect_err("future event schema must be rejected");
    assert!(error.message.contains("unsupported event schema version"));
}

#[tokio::test]
async fn artifacts_and_typed_tasks_are_reusable() {
    let artifacts = InMemoryArtifactStore::default();
    let reference = data_ai_orchestrator::ArtifactStore::put(
        &artifacts,
        b"dataset".to_vec(),
        ArtifactMetadata {
            media_type: "text/plain".to_owned(),
            ..ArtifactMetadata::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        data_ai_orchestrator::ArtifactStore::get(&artifacts, &reference)
            .await
            .unwrap(),
        Some(b"dataset".to_vec())
    );

    let mut registry = InMemoryTaskRegistry::default();
    registry
        .register(TypedTaskAdapter::new(Doubler {
            spec: spec("double"),
        }))
        .unwrap();
    let executor = DurableExecutor::in_memory(Arc::new(registry));
    let mut workflow = WorkflowSpec::new("typed", "1").unwrap();
    workflow
        .add_step(StepSpec::new("double", "double").unwrap())
        .unwrap();
    let run = executor
        .execute(workflow, json!({ "value": 3 }))
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Succeeded);
    assert_eq!(
        run.steps.values().next().unwrap().output,
        Some(json!({ "doubled": 6 }))
    );
}

#[tokio::test]
async fn typed_envelope_preserves_dependency_outputs() {
    let mut registry = InMemoryTaskRegistry::default();
    registry
        .register(Echo {
            spec: spec("source"),
        })
        .unwrap();
    registry
        .register(
            TypedTaskAdapter::new(DependencyReader {
                spec: spec("reader"),
            })
            .with_envelope(),
        )
        .unwrap();
    let executor = DurableExecutor::in_memory(Arc::new(registry));
    let mut workflow = WorkflowSpec::new("dependencies", "1").unwrap();
    workflow
        .add_step(StepSpec::new("source", "source").unwrap())
        .unwrap();
    let mut reader = StepSpec::new("reader", "reader").unwrap();
    reader
        .depends_on
        .push(data_ai_orchestrator::StepId::new("source").unwrap());
    workflow.add_step(reader).unwrap();
    let run = executor
        .execute(workflow, json!({ "value": 4 }))
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Succeeded);
    assert_eq!(
        run.steps[&data_ai_orchestrator::StepId::new("reader").unwrap()]
            .output
            .as_ref()
            .and_then(|value| value.get("dependencies"))
            .and_then(|value| value.get("source")),
        Some(&json!({ "run_input": { "value": 4 }, "dependencies": {} }))
    );
}

#[tokio::test]
async fn failing_fallback_terminates_instead_of_looping() {
    let mut registry = InMemoryTaskRegistry::default();
    registry
        .register(PermanentFailure {
            spec: spec("primary"),
        })
        .unwrap();
    registry
        .register(PermanentFailure {
            spec: spec("fallback"),
        })
        .unwrap();
    let executor = DurableExecutor::in_memory(Arc::new(registry));
    let mut workflow = WorkflowSpec::new("failed-fallback", "1").unwrap();
    let mut step = StepSpec::new("step", "primary").unwrap();
    step.policy.retry = data_ai_orchestrator::RetryPolicy::never();
    step.policy.fallback_task = Some(data_ai_orchestrator::TaskId::new("fallback").unwrap());
    workflow.add_step(step).unwrap();
    let run = timeout(
        Duration::from_secs(1),
        executor.execute(workflow, json!({})),
    )
    .await
    .expect("fallback failure should terminate")
    .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
}

#[tokio::test]
async fn expired_leases_cannot_commit() {
    let store = Arc::new(InMemoryRunStore::default());
    let task = spec("echo");
    let mut workflow = WorkflowSpec::new("expiry", "1").unwrap();
    workflow
        .add_step(StepSpec::new("step", "echo").unwrap())
        .unwrap();
    let mut tasks = BTreeMap::new();
    tasks.insert(task.id.clone(), task);
    let run_id = data_ai_orchestrator::RunId::new("expiry-run").unwrap();
    store
        .create_run(RunRequest {
            run_id: run_id.clone(),
            workflow: workflow.clone(),
            manifest: RunManifest::new(workflow.id.clone(), "1", tasks),
            input: json!({}),
            idempotency_key: None,
        })
        .await
        .unwrap();
    let worker = WorkerProfile::new("expiry-worker").unwrap();
    let claim = store
        .claim_next_step(&run_id, &worker, 1)
        .await
        .unwrap()
        .unwrap();
    sleep(Duration::from_millis(5)).await;
    assert!(matches!(
        store
            .complete_step(
                &claim,
                StepCompletion::succeeded(run_id, claim.step.id.clone(), json!({}), 1,)
            )
            .await,
        Err(data_ai_orchestrator::RepositoryError::LeaseLost { .. })
    ));
}

#[tokio::test]
async fn expired_primary_final_attempt_selects_fallback() {
    let store = Arc::new(InMemoryRunStore::default());
    let primary = spec("primary");
    let fallback = spec("fallback");
    let mut workflow = WorkflowSpec::new("expired-primary", "1").unwrap();
    let mut step = StepSpec::new("step", "primary").unwrap();
    step.policy.retry = data_ai_orchestrator::RetryPolicy::never();
    step.policy.fallback_task = Some(fallback.id.clone());
    workflow.add_step(step).unwrap();
    let mut tasks = BTreeMap::new();
    tasks.insert(primary.id.clone(), primary);
    tasks.insert(fallback.id.clone(), fallback.clone());
    let run_id = data_ai_orchestrator::RunId::new("expired-primary-run").unwrap();
    store
        .create_run(RunRequest {
            run_id: run_id.clone(),
            workflow: workflow.clone(),
            manifest: RunManifest::new(workflow.id.clone(), "1", tasks),
            input: json!({}),
            idempotency_key: None,
        })
        .await
        .unwrap();
    let worker = WorkerProfile::new("recovery-worker").unwrap();
    store
        .claim_next_step(&run_id, &worker, 1)
        .await
        .unwrap()
        .unwrap();
    sleep(Duration::from_millis(5)).await;
    store.recover_expired().await.unwrap();
    let recovered = store.load_run(&run_id).await.unwrap().unwrap();
    let record = recovered.steps.values().next().unwrap();
    assert_eq!(record.status, data_ai_orchestrator::StepStatus::Retrying);
    assert_eq!(record.task, fallback.id);
    assert!(record.fallback_used);
    assert!(
        recovered
            .event_log
            .iter()
            .any(|event| matches!(event.event, ExecutionEvent::StepFallbackSelected { .. }))
    );

    let fallback_claim = store
        .claim_next_step(&run_id, &worker, 1_000)
        .await
        .unwrap()
        .unwrap();
    store
        .complete_step(
            &fallback_claim,
            StepCompletion::succeeded(
                run_id.clone(),
                fallback_claim.step.id.clone(),
                json!({ "recovered": true }),
                fallback_claim.attempt,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        store.finish_run(&run_id, Vec::new()).await.unwrap().status,
        RunStatus::Succeeded
    );
}

#[tokio::test]
async fn expired_fallback_final_attempt_fails() {
    let store = Arc::new(InMemoryRunStore::default());
    let primary = spec("primary");
    let fallback = spec("fallback");
    let mut workflow = WorkflowSpec::new("expired-fallback", "1").unwrap();
    let mut step = StepSpec::new("step", "primary").unwrap();
    step.policy.retry = data_ai_orchestrator::RetryPolicy::never();
    step.policy.fallback_task = Some(fallback.id.clone());
    workflow.add_step(step).unwrap();
    let mut tasks = BTreeMap::new();
    tasks.insert(primary.id.clone(), primary);
    tasks.insert(fallback.id.clone(), fallback);
    let run_id = data_ai_orchestrator::RunId::new("expired-fallback-run").unwrap();
    store
        .create_run(RunRequest {
            run_id: run_id.clone(),
            workflow: workflow.clone(),
            manifest: RunManifest::new(workflow.id.clone(), "1", tasks),
            input: json!({}),
            idempotency_key: None,
        })
        .await
        .unwrap();
    let worker = WorkerProfile::new("recovery-worker").unwrap();
    let primary_claim = store
        .claim_next_step(&run_id, &worker, 1)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        store
            .complete_step(
                &primary_claim,
                StepCompletion::fallback(
                    run_id.clone(),
                    primary_claim.step.id.clone(),
                    data_ai_orchestrator::TaskId::new("primary").unwrap(),
                    "use the wrong fallback".to_owned(),
                )
            )
            .await,
        Err(data_ai_orchestrator::RepositoryError::InvalidCompletion { .. })
    ));
    let fallback_task = data_ai_orchestrator::TaskId::new("fallback").unwrap();
    store
        .complete_step(
            &primary_claim,
            StepCompletion::fallback(
                run_id.clone(),
                primary_claim.step.id.clone(),
                fallback_task,
                "primary failed".to_owned(),
            ),
        )
        .await
        .unwrap();
    let fallback_claim = store
        .claim_next_step(&run_id, &worker, 1)
        .await
        .unwrap()
        .unwrap();
    sleep(Duration::from_millis(5)).await;
    store.recover_expired().await.unwrap();
    let recovered = store.load_run(&run_id).await.unwrap().unwrap();
    assert_eq!(
        recovered.steps.values().next().unwrap().status,
        data_ai_orchestrator::StepStatus::Failed
    );
    assert_eq!(recovered.error.as_deref(), Some("worker lease expired"));
    assert_eq!(
        store.finish_run(&run_id, Vec::new()).await.unwrap().status,
        RunStatus::Failed
    );
    assert_eq!(fallback_claim.attempt, 2);
}

#[tokio::test]
async fn expired_primary_without_fallback_fails() {
    let store = Arc::new(InMemoryRunStore::default());
    let task = spec("primary");
    let mut workflow = WorkflowSpec::new("expired-no-fallback", "1").unwrap();
    let mut step = StepSpec::new("step", "primary").unwrap();
    step.policy.retry = data_ai_orchestrator::RetryPolicy::never();
    workflow.add_step(step).unwrap();
    let mut tasks = BTreeMap::new();
    tasks.insert(task.id.clone(), task);
    let run_id = data_ai_orchestrator::RunId::new("expired-no-fallback-run").unwrap();
    store
        .create_run(RunRequest {
            run_id: run_id.clone(),
            workflow: workflow.clone(),
            manifest: RunManifest::new(workflow.id.clone(), "1", tasks),
            input: json!({}),
            idempotency_key: None,
        })
        .await
        .unwrap();
    let worker = WorkerProfile::new("recovery-worker").unwrap();
    store
        .claim_next_step(&run_id, &worker, 1)
        .await
        .unwrap()
        .unwrap();
    sleep(Duration::from_millis(5)).await;
    store.recover_expired().await.unwrap();
    let recovered = store.load_run(&run_id).await.unwrap().unwrap();
    assert_eq!(
        recovered.steps.values().next().unwrap().status,
        data_ai_orchestrator::StepStatus::Failed
    );
    assert_eq!(recovered.error.as_deref(), Some("worker lease expired"));
    assert_eq!(
        store.finish_run(&run_id, Vec::new()).await.unwrap().status,
        RunStatus::Failed
    );
}

#[tokio::test]
async fn worker_policy_is_enforced_at_claim_time() {
    let store = Arc::new(InMemoryRunStore::default());
    let task = spec("restricted");
    let mut workflow = WorkflowSpec::new("policy", "1").unwrap();
    workflow
        .add_step(StepSpec::new("step", "restricted").unwrap())
        .unwrap();
    let mut tasks = BTreeMap::new();
    tasks.insert(task.id.clone(), task);
    let run_id = data_ai_orchestrator::RunId::new("policy-run").unwrap();
    store
        .create_run(RunRequest {
            run_id: run_id.clone(),
            workflow: workflow.clone(),
            manifest: RunManifest::new(workflow.id.clone(), "1", tasks),
            input: json!({}),
            idempotency_key: None,
        })
        .await
        .unwrap();
    let mut worker = WorkerProfile::new("restricted-worker").unwrap();
    worker.resources.capabilities.insert("other".to_owned());
    assert!(
        store
            .claim_next_step(&run_id, &worker, 1000)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn idempotency_includes_manifest_metadata() {
    let store = InMemoryRunStore::default();
    let task = spec("echo");
    let mut workflow = WorkflowSpec::new("idempotency", "1").unwrap();
    workflow
        .add_step(StepSpec::new("step", "echo").unwrap())
        .unwrap();
    let mut tasks = BTreeMap::new();
    tasks.insert(task.id.clone(), task);
    let key = IdempotencyKey::new("manifest-key").unwrap();
    let mut first_manifest = RunManifest::new(workflow.id.clone(), "1", tasks.clone());
    first_manifest
        .metadata
        .insert("dataset".to_owned(), "one".to_owned());
    store
        .create_run(RunRequest {
            run_id: data_ai_orchestrator::RunId::new("manifest-one").unwrap(),
            workflow: workflow.clone(),
            manifest: first_manifest,
            input: json!({ "same": true }),
            idempotency_key: Some(key.clone()),
        })
        .await
        .unwrap();
    let mut second_manifest = RunManifest::new(workflow.id.clone(), "1", tasks);
    second_manifest
        .metadata
        .insert("dataset".to_owned(), "two".to_owned());
    assert!(matches!(
        store
            .create_run(RunRequest {
                run_id: data_ai_orchestrator::RunId::new("manifest-two").unwrap(),
                workflow,
                manifest: second_manifest,
                input: json!({ "same": true }),
                idempotency_key: Some(key),
            })
            .await,
        Err(data_ai_orchestrator::RepositoryError::IdempotencyConflict { .. })
    ));
}
