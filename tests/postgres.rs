#![cfg(feature = "postgres")]

use data_ai_orchestrator::{
    IdempotencyKey, PgPool, PostgresRunStore, RetryPolicy, RunId, RunManifest, RunRepository,
    RunRequest, RunStatus, StepCompletion, StepSpec, TaskSpec, WorkerProfile, WorkflowSpec,
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    env,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::time::{Duration, sleep};

fn unique(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
        .as_millis();
    format!("{prefix}-{}-{millis}", std::process::id())
}

fn request(
    run_id: RunId,
    idempotency_key: IdempotencyKey,
    retry: RetryPolicy,
    workflow_id: String,
) -> RunRequest {
    let task = TaskSpec::new("postgres-echo", "1", "echo").expect("valid task");
    let mut workflow = WorkflowSpec::new(workflow_id, "1").expect("valid workflow");
    let mut step = StepSpec::new("step", "postgres-echo").expect("valid step");
    step.policy.retry = retry;
    workflow.add_step(step).expect("add step");
    let mut tasks = BTreeMap::new();
    tasks.insert(task.id.clone(), task);
    RunRequest {
        run_id,
        workflow: workflow.clone(),
        manifest: RunManifest::new(workflow.id, "1", tasks),
        input: json!({"source": "postgres-test"}),
        idempotency_key: Some(idempotency_key),
    }
}

#[tokio::test]
#[ignore = "requires POSTGRES_TEST_DATABASE_URL"]
async fn postgres_repository_smoke_and_concurrent_idempotency() {
    let database_url = env::var("POSTGRES_TEST_DATABASE_URL")
        .expect("set POSTGRES_TEST_DATABASE_URL to run PostgreSQL integration tests");
    let store = PostgresRunStore::new(PgPool::connect(&database_url).await.unwrap());
    store.migrate().await.unwrap();

    let key = IdempotencyKey::new(unique("postgres-key")).unwrap();
    let workflow_id = unique("postgres-workflow");
    let first = request(
        RunId::new(unique("postgres-run-a")).unwrap(),
        key.clone(),
        RetryPolicy::never(),
        workflow_id.clone(),
    );
    let second = request(
        RunId::new(unique("postgres-run-b")).unwrap(),
        key,
        RetryPolicy::never(),
        workflow_id,
    );
    let (first, second) = tokio::join!(store.create_run(first), store.create_run(second));
    let run = first.unwrap();
    assert_eq!(run.run_id, second.unwrap().run_id);

    let worker = WorkerProfile::new(unique("postgres-worker")).unwrap();
    let _claim = store
        .claim_next_step(&run.run_id, &worker, 1)
        .await
        .unwrap()
        .unwrap();
    sleep(Duration::from_millis(5)).await;
    store.recover_expired().await.unwrap();
    let recovered = store.load_run(&run.run_id).await.unwrap().unwrap();
    assert_eq!(recovered.status, RunStatus::Running);
    assert_eq!(recovered.error.as_deref(), Some("worker lease expired"));
    assert_eq!(
        recovered.steps.values().next().unwrap().status,
        data_ai_orchestrator::StepStatus::Failed
    );

    let cancel = request(
        RunId::new(unique("postgres-cancel")).unwrap(),
        IdempotencyKey::new(unique("postgres-cancel-key")).unwrap(),
        RetryPolicy::never(),
        unique("postgres-cancel-workflow"),
    );
    let cancel = store.create_run(cancel).await.unwrap();
    store
        .claim_next_step(&cancel.run_id, &worker, 1_000)
        .await
        .unwrap()
        .unwrap();
    store.cancel_run(&cancel.run_id).await.unwrap();
    let cancelled = store.load_run(&cancel.run_id).await.unwrap().unwrap();
    let cancelled_step = cancelled.steps.values().next().unwrap();
    assert_eq!(cancelled.status, RunStatus::Cancelled);
    assert_eq!(
        cancelled_step.status,
        data_ai_orchestrator::StepStatus::Cancelled
    );
    assert_eq!(
        cancelled_step.history[0].status,
        data_ai_orchestrator::StepStatus::Cancelled
    );
    assert!(cancelled_step.history[0].finished_at_ms.is_some());

    assert_eq!(
        store
            .finish_run(&run.run_id, Vec::new())
            .await
            .unwrap()
            .status,
        RunStatus::Failed
    );

    let success = request(
        RunId::new(unique("postgres-success")).unwrap(),
        IdempotencyKey::new(unique("postgres-success-key")).unwrap(),
        RetryPolicy::never(),
        unique("postgres-success-workflow"),
    );
    let success = store.create_run(success).await.unwrap();
    let claim = store
        .claim_next_step(&success.run_id, &worker, 1_000)
        .await
        .unwrap()
        .unwrap();
    store
        .complete_step(
            &claim,
            StepCompletion::succeeded(
                success.run_id.clone(),
                claim.step.id.clone(),
                json!({"ok": true}),
                claim.attempt,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .finish_run(&success.run_id, Vec::new())
            .await
            .unwrap()
            .status,
        RunStatus::Succeeded
    );
    assert!(
        !store
            .events_since(&success.run_id, 0)
            .await
            .unwrap()
            .is_empty()
    );
}
