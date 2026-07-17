use crate::{
    AdapterError, ArtifactRef, AttemptId, EventId, IdempotencyKey, RepositoryError, RunId,
    RunManifest, StepId, StepSpec, TaskId, TaskSpec, WorkerId, WorkflowId, WorkflowSpec,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum RunStatus {
    Pending,
    Running,
    Paused,
    CancelRequested,
    Cancelled,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum StepStatus {
    Pending,
    Claimed,
    Running,
    Retrying,
    Succeeded,
    Failed,
    Cancelled,
}

/// A single leased attempt of a step.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StepAttempt {
    pub id: AttemptId,
    pub number: u32,
    pub worker: WorkerId,
    pub lease_token: u64,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub status: StepStatus,
    pub error: Option<String>,
}

/// A fencing lease that prevents stale workers from committing results.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StepLease {
    pub worker: WorkerId,
    pub token: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StepRecord {
    pub id: StepId,
    pub task: TaskId,
    pub status: StepStatus,
    pub attempts: u32,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub fallback_used: bool,
    pub lease: Option<StepLease>,
    pub history: Vec<StepAttempt>,
    pub artifacts: Vec<ArtifactRef>,
    #[serde(default)]
    pub next_attempt_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum ExecutionEvent {
    RunStarted {
        run_id: RunId,
        workflow_id: WorkflowId,
    },
    StepStarted {
        run_id: RunId,
        step_id: StepId,
        task: TaskId,
        attempt: u32,
    },
    StepRetrying {
        run_id: RunId,
        step_id: StepId,
        attempt: u32,
        error: String,
    },
    StepFallbackSelected {
        run_id: RunId,
        step_id: StepId,
        task: TaskId,
    },
    StepSucceeded {
        run_id: RunId,
        step_id: StepId,
        attempts: u32,
    },
    StepFailed {
        run_id: RunId,
        step_id: StepId,
        attempts: u32,
        error: String,
    },
    RunFinished {
        run_id: RunId,
        status: RunStatus,
    },
}

/// An ordered, deduplicable event envelope. `ExecutionEvent` remains the
/// ergonomic payload while this envelope is the durable transport contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventEnvelope {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub event_id: EventId,
    pub sequence: u64,
    pub occurred_at_ms: u64,
    pub event: ExecutionEvent,
}

/// Input required to create a durable run.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RunRequest {
    pub run_id: RunId,
    pub workflow: WorkflowSpec,
    pub manifest: RunManifest,
    pub input: Value,
    pub idempotency_key: Option<IdempotencyKey>,
}

/// A worker-owned step claim. The input is snapshotted at claim time so a
/// retry or recovery cannot observe a different dependency graph.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct StepClaim {
    pub run_id: RunId,
    pub workflow_id: WorkflowId,
    pub step: StepSpec,
    pub task: TaskSpec,
    pub input: Value,
    pub worker: WorkerId,
    pub lease_token: u64,
    pub attempt: u32,
    #[serde(default)]
    pub fallback_used: bool,
}

/// The atomic state transition produced by a worker.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct StepCompletion {
    pub status: StepStatus,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub next_task: Option<TaskId>,
    pub fallback_used: bool,
    pub artifacts: Vec<ArtifactRef>,
    pub event: ExecutionEvent,
    #[serde(default)]
    pub retry_after_ms: Option<u64>,
}

impl StepCompletion {
    /// Attaches immutable artifact references to the committed step output.
    #[must_use]
    pub fn with_artifacts(mut self, artifacts: Vec<ArtifactRef>) -> Self {
        self.artifacts = artifacts;
        self
    }

    #[must_use]
    pub fn succeeded(run_id: RunId, step_id: StepId, output: Value, attempts: u32) -> Self {
        Self {
            status: StepStatus::Succeeded,
            output: Some(output),
            error: None,
            next_task: None,
            fallback_used: false,
            artifacts: Vec::new(),
            event: ExecutionEvent::StepSucceeded {
                run_id,
                step_id,
                attempts,
            },
            retry_after_ms: None,
        }
    }

    #[must_use]
    pub fn retrying(run_id: RunId, step_id: StepId, attempt: u32, error: String) -> Self {
        Self {
            status: StepStatus::Retrying,
            output: None,
            error: Some(error.clone()),
            next_task: None,
            fallback_used: false,
            artifacts: Vec::new(),
            event: ExecutionEvent::StepRetrying {
                run_id,
                step_id,
                attempt,
                error,
            },
            retry_after_ms: None,
        }
    }

    #[must_use]
    pub fn fallback(run_id: RunId, step_id: StepId, task: TaskId, error: String) -> Self {
        Self {
            status: StepStatus::Retrying,
            output: None,
            error: Some(error),
            next_task: Some(task.clone()),
            fallback_used: true,
            artifacts: Vec::new(),
            event: ExecutionEvent::StepFallbackSelected {
                run_id,
                step_id,
                task,
            },
            retry_after_ms: None,
        }
    }

    #[must_use]
    pub fn failed(run_id: RunId, step_id: StepId, attempts: u32, error: String) -> Self {
        Self {
            status: StepStatus::Failed,
            output: None,
            error: Some(error.clone()),
            next_task: None,
            fallback_used: false,
            artifacts: Vec::new(),
            event: ExecutionEvent::StepFailed {
                run_id,
                step_id,
                attempts,
                error,
            },
            retry_after_ms: None,
        }
    }

    /// Stores retry backoff in the repository so a worker can release its
    /// lease before waiting for the next eligible attempt.
    #[must_use]
    pub fn with_retry_after_ms(mut self, delay_ms: u64) -> Self {
        self.retry_after_ms = Some(delay_ms);
        self
    }
}

/// Durable storage boundary. An application can implement this with SQLx,
/// object storage, or a queue without changing task code.
#[async_trait]
pub trait RunStore: Send + Sync {
    async fn save(&self, run: &RunRecord) -> Result<(), AdapterError>;
    async fn load(&self, run_id: &RunId) -> Result<Option<RunRecord>, AdapterError>;
}

#[async_trait]
pub trait EventSink: Send + Sync {
    async fn emit(&self, event: ExecutionEvent) -> Result<(), AdapterError>;
}

/// Transactional repository boundary for distributed schedulers and workers.
/// Implementations must make claim, lease validation, state transition, and
/// event publication durable as one operation.
#[async_trait]
pub trait RunRepository: Send + Sync {
    async fn create_run(&self, request: RunRequest) -> Result<RunRecord, RepositoryError>;
    async fn load_run(&self, run_id: &RunId) -> Result<Option<RunRecord>, RepositoryError>;
    /// Finds and claims one runnable step across all active runs.
    ///
    /// Implementations may use an advisory candidate scan, but the returned
    /// claim must be fenced by the same transactional lease checks as
    /// [`Self::claim_next_step`].
    async fn claim_next_runnable_step(
        &self,
        worker: &crate::WorkerProfile,
        lease_ms: u64,
    ) -> Result<Option<StepClaim>, RepositoryError>;
    async fn claim_next_step(
        &self,
        run_id: &RunId,
        worker: &crate::WorkerProfile,
        lease_ms: u64,
    ) -> Result<Option<StepClaim>, RepositoryError>;
    async fn renew_step(&self, claim: &StepClaim, lease_ms: u64) -> Result<(), RepositoryError>;
    async fn complete_step(
        &self,
        claim: &StepClaim,
        completion: StepCompletion,
    ) -> Result<RunRecord, RepositoryError>;
    async fn record_event(
        &self,
        run_id: &RunId,
        event: ExecutionEvent,
    ) -> Result<EventEnvelope, RepositoryError>;
    async fn finish_run(
        &self,
        run_id: &RunId,
        metrics: Vec<crate::Metric>,
    ) -> Result<RunRecord, RepositoryError>;
    async fn cancel_run(&self, run_id: &RunId) -> Result<RunRecord, RepositoryError>;
    async fn recover_expired(&self) -> Result<Vec<(RunId, StepId)>, RepositoryError>;
    async fn events_since(
        &self,
        run_id: &RunId,
        sequence: u64,
    ) -> Result<Vec<EventEnvelope>, RepositoryError>;
}

#[derive(Default)]
struct InMemoryRepositoryState {
    runs: BTreeMap<RunId, RunRecord>,
    idempotency: BTreeMap<IdempotencyKey, RunId>,
    events: Vec<EventEnvelope>,
    next_event_sequence: u64,
    next_lease_token: u64,
}

#[derive(Default)]
pub struct InMemoryRunStore {
    state: RwLock<InMemoryRepositoryState>,
}

#[async_trait]
impl RunStore for InMemoryRunStore {
    async fn save(&self, run: &RunRecord) -> Result<(), AdapterError> {
        validate_run_record_schema(run).map_err(|message| AdapterError::new("memory", message))?;
        self.state
            .write()
            .await
            .runs
            .insert(run.run_id.clone(), run.clone());
        Ok(())
    }

    async fn load(&self, run_id: &RunId) -> Result<Option<RunRecord>, AdapterError> {
        Ok(self.state.read().await.runs.get(run_id).cloned())
    }
}

#[derive(Default)]
pub struct InMemoryEventSink {
    events: RwLock<Vec<ExecutionEvent>>,
}

impl InMemoryEventSink {
    pub async fn events(&self) -> Vec<ExecutionEvent> {
        self.events.read().await.clone()
    }
}

#[async_trait]
impl EventSink for InMemoryEventSink {
    async fn emit(&self, event: ExecutionEvent) -> Result<(), AdapterError> {
        self.events.write().await.push(event);
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RunRecord {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: RunId,
    pub workflow: WorkflowSpec,
    pub manifest: RunManifest,
    pub input: Value,
    pub status: RunStatus,
    pub steps: BTreeMap<StepId, StepRecord>,
    pub events: Vec<ExecutionEvent>,
    pub event_log: Vec<EventEnvelope>,
    pub metrics: Vec<crate::Metric>,
    pub gate_decisions: BTreeMap<String, crate::GateDecision>,
    pub error: Option<String>,
    pub idempotency_key: Option<IdempotencyKey>,
    pub revision: u64,
    pub created_at_ms: u64,
}

impl RunRecord {
    pub(crate) fn new(
        run_id: RunId,
        workflow: WorkflowSpec,
        manifest: RunManifest,
        input: Value,
    ) -> Self {
        Self {
            schema_version: schema_version(),
            run_id,
            workflow,
            manifest,
            input,
            status: RunStatus::Pending,
            steps: BTreeMap::new(),
            events: Vec::new(),
            event_log: Vec::new(),
            metrics: Vec::new(),
            gate_decisions: BTreeMap::new(),
            error: None,
            idempotency_key: None,
            revision: 0,
            created_at_ms: now_ms(),
        }
    }
}

fn schema_version() -> u32 {
    1
}

pub(crate) fn validate_run_record_schema(run: &RunRecord) -> Result<(), String> {
    let expected = schema_version();
    if run.schema_version != expected {
        return Err(format!(
            "unsupported run schema version {}; expected {expected}",
            run.schema_version
        ));
    }
    if run.manifest.schema_version != expected {
        return Err(format!(
            "unsupported manifest schema version {}; expected {expected}",
            run.manifest.schema_version
        ));
    }
    if let Some(event) = run
        .event_log
        .iter()
        .find(|event| event.schema_version != expected)
    {
        return Err(format!(
            "unsupported event schema version {}; expected {expected}",
            event.schema_version
        ));
    }
    Ok(())
}

#[must_use]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().min(u128::from(u64::MAX)) as u64
        })
}

pub(crate) fn build_step_input(run_input: &Value, dependencies: &BTreeMap<String, Value>) -> Value {
    let mut input = Map::new();
    input.insert("run_input".to_owned(), run_input.clone());
    input.insert(
        "dependencies".to_owned(),
        Value::Object(dependencies.clone().into_iter().collect()),
    );
    Value::Object(input)
}

fn event_envelope(run_id: &RunId, sequence: u64, event: ExecutionEvent) -> EventEnvelope {
    // RunId validation plus the fixed non-empty suffix make this identifier
    // construction an internal invariant, rather than deserialized input.
    let event_id = EventId::new(format!("{run_id}-event-{sequence}"))
        .expect("generated event identifiers are non-empty");
    EventEnvelope {
        schema_version: schema_version(),
        event_id,
        sequence,
        occurred_at_ms: now_ms(),
        event,
    }
}

fn append_event(
    state: &mut InMemoryRepositoryState,
    run_id: &RunId,
    event: ExecutionEvent,
) -> Result<EventEnvelope, RepositoryError> {
    state.next_event_sequence = state.next_event_sequence.saturating_add(1);
    let envelope = event_envelope(run_id, state.next_event_sequence, event.clone());
    let run = state
        .runs
        .get_mut(run_id)
        .ok_or_else(|| RepositoryError::RunNotFound {
            run: run_id.clone(),
        })?;
    run.events.push(event);
    run.event_log.push(envelope.clone());
    state.events.push(envelope.clone());
    run.revision = run.revision.saturating_add(1);
    Ok(envelope)
}

#[cfg(feature = "postgres")]
pub(crate) fn append_run_event(
    run: &mut RunRecord,
    event: ExecutionEvent,
) -> Result<EventEnvelope, RepositoryError> {
    if !event_belongs_to_run(&event, &run.run_id) {
        return Err(RepositoryError::InvalidEvent {
            run: run.run_id.clone(),
        });
    }
    let sequence = run
        .event_log
        .last()
        .map_or(0, |envelope| envelope.sequence)
        .saturating_add(1);
    let envelope = event_envelope(&run.run_id, sequence, event.clone());
    run.events.push(event);
    run.event_log.push(envelope.clone());
    run.revision = run.revision.saturating_add(1);
    Ok(envelope)
}

pub(crate) fn completion_matches_claim(
    completion: &StepCompletion,
    claim: &StepClaim,
    manifest: &RunManifest,
) -> bool {
    match &completion.event {
        ExecutionEvent::StepRetrying {
            run_id,
            step_id,
            attempt,
            error,
        } => {
            run_id == &claim.run_id
                && step_id == &claim.step.id
                && *attempt == claim.attempt
                && completion.status == StepStatus::Retrying
                && *attempt < claim.step.policy.retry.max_attempts.max(1)
                && completion.output.is_none()
                && completion.error.as_ref() == Some(error)
                && completion.next_task.is_none()
                && !completion.fallback_used
        }
        ExecutionEvent::StepFallbackSelected {
            run_id,
            step_id,
            task,
        } => {
            run_id == &claim.run_id
                && step_id == &claim.step.id
                && completion.status == StepStatus::Retrying
                && completion.output.is_none()
                && completion.error.is_some()
                && completion.next_task.as_ref() == Some(task)
                && completion.fallback_used
                && !claim.fallback_used
                && claim.step.policy.fallback_task.as_ref() == Some(task)
                && manifest.tasks.contains_key(task)
                && completion.retry_after_ms.is_none()
        }
        ExecutionEvent::StepSucceeded {
            run_id,
            step_id,
            attempts,
        } => {
            run_id == &claim.run_id
                && step_id == &claim.step.id
                && *attempts == claim.attempt
                && completion.status == StepStatus::Succeeded
                && completion.output.is_some()
                && completion.error.is_none()
                && completion.next_task.is_none()
                && !completion.fallback_used
                && completion.retry_after_ms.is_none()
        }
        ExecutionEvent::StepFailed {
            run_id,
            step_id,
            attempts,
            error,
        } => {
            run_id == &claim.run_id
                && step_id == &claim.step.id
                && *attempts == claim.attempt
                && completion.status == StepStatus::Failed
                && completion.output.is_none()
                && completion.error.as_ref() == Some(error)
                && completion.next_task.is_none()
                && !completion.fallback_used
                && (claim.fallback_used || claim.step.policy.fallback_task.is_none())
                && completion.retry_after_ms.is_none()
        }
        _ => false,
    }
}

const LEASE_EXPIRED_ERROR: &str = "worker lease expired";

pub(crate) fn recover_expired_step(
    run_id: &RunId,
    step: &StepSpec,
    record: &mut StepRecord,
    now: u64,
) -> Option<ExecutionEvent> {
    if record
        .lease
        .as_ref()
        .is_none_or(|lease| lease.expires_at_ms > now)
    {
        return None;
    }

    record.lease = None;
    record.error = Some(LEASE_EXPIRED_ERROR.to_owned());
    record.next_attempt_at_ms = None;
    let max_attempts = step.policy.retry.max_attempts.max(1);
    let event = if record.attempts < max_attempts {
        record.status = StepStatus::Retrying;
        ExecutionEvent::StepRetrying {
            run_id: run_id.clone(),
            step_id: record.id.clone(),
            attempt: record.attempts,
            error: LEASE_EXPIRED_ERROR.to_owned(),
        }
    } else if !record.fallback_used {
        if let Some(fallback_task) = &step.policy.fallback_task {
            record.task = fallback_task.clone();
            record.fallback_used = true;
            record.status = StepStatus::Retrying;
            ExecutionEvent::StepFallbackSelected {
                run_id: run_id.clone(),
                step_id: record.id.clone(),
                task: fallback_task.clone(),
            }
        } else {
            record.status = StepStatus::Failed;
            ExecutionEvent::StepFailed {
                run_id: run_id.clone(),
                step_id: record.id.clone(),
                attempts: record.attempts,
                error: LEASE_EXPIRED_ERROR.to_owned(),
            }
        }
    } else {
        record.status = StepStatus::Failed;
        ExecutionEvent::StepFailed {
            run_id: run_id.clone(),
            step_id: record.id.clone(),
            attempts: record.attempts,
            error: LEASE_EXPIRED_ERROR.to_owned(),
        }
    };
    if let Some(attempt) = record.history.last_mut() {
        attempt.finished_at_ms = Some(now);
        attempt.status = record.status.clone();
        attempt.error = record.error.clone();
    }
    Some(event)
}

pub(crate) fn event_belongs_to_run(event: &ExecutionEvent, run_id: &RunId) -> bool {
    match event {
        ExecutionEvent::RunStarted {
            run_id: event_run, ..
        }
        | ExecutionEvent::RunFinished {
            run_id: event_run, ..
        }
        | ExecutionEvent::StepStarted {
            run_id: event_run, ..
        }
        | ExecutionEvent::StepRetrying {
            run_id: event_run, ..
        }
        | ExecutionEvent::StepFallbackSelected {
            run_id: event_run, ..
        }
        | ExecutionEvent::StepSucceeded {
            run_id: event_run, ..
        }
        | ExecutionEvent::StepFailed {
            run_id: event_run, ..
        } => event_run == run_id,
    }
}

pub(crate) fn event_is_valid_for_record(run: &RunRecord, event: &ExecutionEvent) -> bool {
    let event_run_id = match event {
        ExecutionEvent::RunStarted { run_id, .. }
        | ExecutionEvent::StepStarted { run_id, .. }
        | ExecutionEvent::StepRetrying { run_id, .. }
        | ExecutionEvent::StepFallbackSelected { run_id, .. }
        | ExecutionEvent::StepSucceeded { run_id, .. }
        | ExecutionEvent::StepFailed { run_id, .. }
        | ExecutionEvent::RunFinished { run_id, .. } => run_id,
    };
    if event_run_id != &run.run_id {
        return false;
    }

    let already_logged = |same: &dyn Fn(&ExecutionEvent) -> bool| {
        run.event_log.iter().any(|envelope| same(&envelope.event))
    };
    match event {
        ExecutionEvent::RunStarted { workflow_id, .. } => {
            run.status == RunStatus::Running
                && &run.workflow.id == workflow_id
                && !already_logged(&|event| matches!(event, ExecutionEvent::RunStarted { .. }))
        }
        ExecutionEvent::StepStarted {
            step_id,
            task,
            attempt,
            ..
        } => {
            let Some(record) = run.steps.get(step_id) else {
                return false;
            };
            run.status == RunStatus::Running
                && record.status == StepStatus::Running
                && record.task == *task
                && record.attempts == *attempt
                && !already_logged(&|event| {
                    matches!(
                        event,
                        ExecutionEvent::StepStarted {
                            step_id: event_step,
                            attempt: event_attempt,
                            ..
                        } if event_step == step_id && event_attempt == attempt
                    )
                })
        }
        ExecutionEvent::StepRetrying {
            step_id,
            attempt,
            error,
            ..
        } => {
            let Some(record) = run.steps.get(step_id) else {
                return false;
            };
            run.status == RunStatus::Running
                && record.status == StepStatus::Retrying
                && record.attempts == *attempt
                && record.error.as_ref() == Some(error)
                && !already_logged(&|event| {
                    matches!(
                        event,
                        ExecutionEvent::StepRetrying {
                            step_id: event_step,
                            attempt: event_attempt,
                            ..
                        } if event_step == step_id && event_attempt == attempt
                    )
                })
        }
        ExecutionEvent::StepFallbackSelected { step_id, task, .. } => {
            let Some(record) = run.steps.get(step_id) else {
                return false;
            };
            run.status == RunStatus::Running
                && record.status == StepStatus::Retrying
                && record.fallback_used
                && record.task == *task
                && !already_logged(&|event| {
                    matches!(
                        event,
                        ExecutionEvent::StepFallbackSelected {
                            step_id: event_step,
                            task: event_task,
                            ..
                        } if event_step == step_id && event_task == task
                    )
                })
        }
        ExecutionEvent::StepSucceeded {
            step_id, attempts, ..
        } => {
            let Some(record) = run.steps.get(step_id) else {
                return false;
            };
            run.status == RunStatus::Running
                && record.status == StepStatus::Succeeded
                && record.attempts == *attempts
                && !already_logged(&|event| {
                    matches!(
                        event,
                        ExecutionEvent::StepSucceeded {
                            step_id: event_step,
                            attempts: event_attempts,
                            ..
                        } if event_step == step_id && event_attempts == attempts
                    )
                })
        }
        ExecutionEvent::StepFailed {
            step_id,
            attempts,
            error,
            ..
        } => {
            let Some(record) = run.steps.get(step_id) else {
                return false;
            };
            run.status == RunStatus::Running
                && record.status == StepStatus::Failed
                && record.attempts == *attempts
                && record.error.as_ref() == Some(error)
                && !already_logged(&|event| {
                    matches!(
                        event,
                        ExecutionEvent::StepFailed {
                            step_id: event_step,
                            attempts: event_attempts,
                            ..
                        } if event_step == step_id && event_attempts == attempts
                    )
                })
        }
        ExecutionEvent::RunFinished { status, .. } => {
            matches!(
                status,
                RunStatus::Cancelled | RunStatus::Failed | RunStatus::Succeeded
            ) && run.status == *status
                && !already_logged(&|event| matches!(event, ExecutionEvent::RunFinished { .. }))
        }
    }
}

pub(crate) fn validate_run_request(request: &RunRequest) -> Result<(), RepositoryError> {
    request
        .workflow
        .validate()
        .map_err(|error| RepositoryError::InvalidRequest {
            message: error.to_string(),
        })?;
    if request.manifest.schema_version != schema_version() {
        return Err(RepositoryError::InvalidRequest {
            message: format!(
                "unsupported manifest schema version {}",
                request.manifest.schema_version
            ),
        });
    }
    if request.workflow.id != request.manifest.workflow_id
        || request.workflow.version != request.manifest.workflow_version
    {
        return Err(RepositoryError::InvalidRequest {
            message: "workflow and manifest identities do not match".to_owned(),
        });
    }
    for (task_id, task) in &request.manifest.tasks {
        if task_id != &task.id {
            return Err(RepositoryError::InvalidRequest {
                message: format!(
                    "manifest task key {task_id} does not match task id {}",
                    task.id
                ),
            });
        }
    }
    for step in &request.workflow.steps {
        if !request.manifest.tasks.contains_key(&step.task)
            || step
                .policy
                .fallback_task
                .as_ref()
                .is_some_and(|task| !request.manifest.tasks.contains_key(task))
        {
            return Err(RepositoryError::InvalidRequest {
                message: format!("manifest does not contain all tasks for step {}", step.id),
            });
        }
    }
    Ok(())
}

#[async_trait]
impl RunRepository for InMemoryRunStore {
    async fn create_run(&self, request: RunRequest) -> Result<RunRecord, RepositoryError> {
        validate_run_request(&request)?;
        let mut state = self.state.write().await;
        if let Some(key) = &request.idempotency_key {
            if let Some(existing_id) = state.idempotency.get(key) {
                let existing =
                    state
                        .runs
                        .get(existing_id)
                        .ok_or_else(|| RepositoryError::Conflict {
                            run: existing_id.clone(),
                        })?;
                if existing.workflow == request.workflow
                    && existing.manifest == request.manifest
                    && existing.input == request.input
                {
                    return Ok(existing.clone());
                }
                return Err(RepositoryError::IdempotencyConflict {
                    run: existing_id.clone(),
                });
            }
        }
        if state.runs.contains_key(&request.run_id) {
            return Err(RepositoryError::Conflict {
                run: request.run_id,
            });
        }
        let mut run = RunRecord::new(
            request.run_id.clone(),
            request.workflow,
            request.manifest,
            request.input,
        );
        run.status = RunStatus::Running;
        run.idempotency_key = request.idempotency_key.clone();
        let workflow_id = run.workflow.id.clone();
        let created_run_id = run.run_id.clone();
        if let Some(key) = request.idempotency_key {
            state.idempotency.insert(key, run.run_id.clone());
        }
        state.runs.insert(run.run_id.clone(), run.clone());
        append_event(
            &mut state,
            &created_run_id,
            ExecutionEvent::RunStarted {
                run_id: created_run_id.clone(),
                workflow_id,
            },
        )?;
        state
            .runs
            .get(&created_run_id)
            .cloned()
            .ok_or(RepositoryError::Conflict {
                run: created_run_id,
            })
    }

    async fn load_run(&self, run_id: &RunId) -> Result<Option<RunRecord>, RepositoryError> {
        Ok(self.state.read().await.runs.get(run_id).cloned())
    }

    async fn claim_next_runnable_step(
        &self,
        worker: &crate::WorkerProfile,
        lease_ms: u64,
    ) -> Result<Option<StepClaim>, RepositoryError> {
        let candidates = self
            .state
            .read()
            .await
            .runs
            .values()
            .filter(|run| run.status == RunStatus::Running)
            .map(|run| run.run_id.clone())
            .collect::<Vec<_>>();
        for run_id in candidates {
            if let Some(claim) = self.claim_next_step(&run_id, worker, lease_ms).await? {
                return Ok(Some(claim));
            }
        }
        Ok(None)
    }

    async fn claim_next_step(
        &self,
        run_id: &RunId,
        worker: &crate::WorkerProfile,
        lease_ms: u64,
    ) -> Result<Option<StepClaim>, RepositoryError> {
        let mut state = self.state.write().await;
        let next = {
            let run = state
                .runs
                .get(run_id)
                .ok_or_else(|| RepositoryError::RunNotFound {
                    run: run_id.clone(),
                })?;
            if run.status != RunStatus::Running {
                return Ok(None);
            }
            run.workflow.steps.iter().find_map(|step| {
                let record = run.steps.get(&step.id);
                let task_id =
                    record.map_or_else(|| step.task.clone(), |record| record.task.clone());
                let task = run.manifest.tasks.get(&task_id)?;
                if !worker.supports_task(task, &step.policy) {
                    return None;
                }
                let ready = step.depends_on.iter().all(|dependency| {
                    run.steps
                        .get(dependency)
                        .is_some_and(|record| record.status == StepStatus::Succeeded)
                });
                let available = record.is_none_or(|record| {
                    record.lease.is_none()
                        && matches!(record.status, StepStatus::Pending | StepStatus::Retrying)
                        && record
                            .next_attempt_at_ms
                            .is_none_or(|next| next <= now_ms())
                        && (record.attempts < step.policy.retry.max_attempts.max(1)
                            || (record.fallback_used
                                && record.status == StepStatus::Retrying
                                && record.attempts == step.policy.retry.max_attempts.max(1)))
                });
                (ready && available).then(|| {
                    let dependencies = step
                        .depends_on
                        .iter()
                        .filter_map(|dependency| {
                            run.steps
                                .get(dependency)
                                .and_then(|record| record.output.clone())
                                .map(|output| (dependency.to_string(), output))
                        })
                        .collect::<BTreeMap<_, _>>();
                    (step.clone(), task_id, dependencies)
                })
            })
        };
        let Some((step, task_id, dependencies)) = next else {
            return Ok(None);
        };
        let task = state
            .runs
            .get(run_id)
            .and_then(|run| run.manifest.tasks.get(&task_id))
            .cloned()
            .ok_or_else(|| RepositoryError::Conflict {
                run: run_id.clone(),
            })?;
        state.next_lease_token = state.next_lease_token.saturating_add(1);
        let lease_token = state.next_lease_token;
        let now = now_ms();
        let run = state
            .runs
            .get_mut(run_id)
            .ok_or_else(|| RepositoryError::RunNotFound {
                run: run_id.clone(),
            })?;
        let record = run
            .steps
            .entry(step.id.clone())
            .or_insert_with(|| StepRecord {
                id: step.id.clone(),
                task: task_id.clone(),
                status: StepStatus::Pending,
                attempts: 0,
                output: None,
                error: None,
                fallback_used: false,
                lease: None,
                history: Vec::new(),
                artifacts: Vec::new(),
                next_attempt_at_ms: None,
            });
        record.task = task_id.clone();
        record.status = StepStatus::Running;
        record.attempts = record.attempts.saturating_add(1);
        record.lease = Some(StepLease {
            worker: worker.id.clone(),
            token: lease_token,
            expires_at_ms: now.saturating_add(lease_ms.max(1)),
        });
        record.history.push(StepAttempt {
            // The run id and lease token are generated by this repository, so
            // this identifier cannot be empty after formatting.
            id: AttemptId::new(format!("{run_id}-attempt-{lease_token}"))
                .expect("generated attempt identifiers are non-empty"),
            number: record.attempts,
            worker: worker.id.clone(),
            lease_token,
            started_at_ms: now,
            finished_at_ms: None,
            status: StepStatus::Running,
            error: None,
        });
        run.revision = run.revision.saturating_add(1);
        let input = build_step_input(&run.input, &dependencies);
        Ok(Some(StepClaim {
            run_id: run_id.clone(),
            workflow_id: run.workflow.id.clone(),
            step,
            task,
            input,
            worker: worker.id.clone(),
            lease_token,
            attempt: record.attempts,
            fallback_used: record.fallback_used,
        }))
    }

    async fn renew_step(&self, claim: &StepClaim, lease_ms: u64) -> Result<(), RepositoryError> {
        let mut state = self.state.write().await;
        let run =
            state
                .runs
                .get_mut(&claim.run_id)
                .ok_or_else(|| RepositoryError::RunNotFound {
                    run: claim.run_id.clone(),
                })?;
        let record =
            run.steps
                .get_mut(&claim.step.id)
                .ok_or_else(|| RepositoryError::LeaseLost {
                    run: claim.run_id.clone(),
                    step: claim.step.id.clone(),
                })?;
        let Some(lease) = &mut record.lease else {
            return Err(RepositoryError::LeaseLost {
                run: claim.run_id.clone(),
                step: claim.step.id.clone(),
            });
        };
        if lease.worker != claim.worker
            || lease.token != claim.lease_token
            || lease.expires_at_ms <= now_ms()
        {
            return Err(RepositoryError::LeaseLost {
                run: claim.run_id.clone(),
                step: claim.step.id.clone(),
            });
        }
        lease.expires_at_ms = now_ms().saturating_add(lease_ms.max(1));
        run.revision = run.revision.saturating_add(1);
        Ok(())
    }

    async fn complete_step(
        &self,
        claim: &StepClaim,
        completion: StepCompletion,
    ) -> Result<RunRecord, RepositoryError> {
        let mut state = self.state.write().await;
        let event = completion.event.clone();
        let run =
            state
                .runs
                .get_mut(&claim.run_id)
                .ok_or_else(|| RepositoryError::RunNotFound {
                    run: claim.run_id.clone(),
                })?;
        let record =
            run.steps
                .get_mut(&claim.step.id)
                .ok_or_else(|| RepositoryError::LeaseLost {
                    run: claim.run_id.clone(),
                    step: claim.step.id.clone(),
                })?;
        let lease_valid = record.lease.as_ref().is_some_and(|lease| {
            lease.worker == claim.worker
                && lease.token == claim.lease_token
                && lease.expires_at_ms > now_ms()
        });
        if !lease_valid {
            return Err(RepositoryError::LeaseLost {
                run: claim.run_id.clone(),
                step: claim.step.id.clone(),
            });
        }
        if !completion_matches_claim(&completion, claim, &run.manifest) {
            return Err(RepositoryError::InvalidCompletion {
                run: claim.run_id.clone(),
                step: claim.step.id.clone(),
            });
        }
        record.status = completion.status;
        record.output = completion.output;
        record.error = completion.error.clone();
        record.fallback_used |= completion.fallback_used;
        record.artifacts = completion.artifacts;
        if let Some(next_task) = completion.next_task {
            record.task = next_task;
        }
        record.lease = None;
        record.next_attempt_at_ms = completion
            .retry_after_ms
            .map(|delay| now_ms().saturating_add(delay));
        if let Some(attempt) = record.history.last_mut() {
            attempt.finished_at_ms = Some(now_ms());
            attempt.status = record.status.clone();
            attempt.error = record.error.clone();
        }
        run.revision = run.revision.saturating_add(1);
        if matches!(record.status, StepStatus::Failed) {
            run.error = record.error.clone();
        }
        // The event payload is supplied by the worker and committed under the
        // same repository lock as the step transition.
        if let ExecutionEvent::StepFallbackSelected { .. } = event {
            // A fallback selection is followed by a retrying state, so keep the
            // event unchanged and let the next claim run the selected task.
        }
        let run_id = run.run_id.clone();
        let snapshot = run.clone();
        let _ = run;
        append_event(&mut state, &run_id, event)?;
        state
            .runs
            .get(&run_id)
            .cloned()
            .or(Some(snapshot))
            .ok_or(RepositoryError::Conflict { run: run_id })
    }

    async fn record_event(
        &self,
        run_id: &RunId,
        event: ExecutionEvent,
    ) -> Result<EventEnvelope, RepositoryError> {
        let mut state = self.state.write().await;
        let Some(run) = state.runs.get(run_id) else {
            return Err(RepositoryError::RunNotFound {
                run: run_id.clone(),
            });
        };
        if !event_belongs_to_run(&event, run_id) || !event_is_valid_for_record(run, &event) {
            return Err(RepositoryError::InvalidEvent {
                run: run_id.clone(),
            });
        }
        append_event(&mut state, run_id, event)?;
        state
            .runs
            .get(run_id)
            .and_then(|run| run.event_log.last().cloned())
            .ok_or_else(|| RepositoryError::Conflict {
                run: run_id.clone(),
            })
    }

    async fn finish_run(
        &self,
        run_id: &RunId,
        metrics: Vec<crate::Metric>,
    ) -> Result<RunRecord, RepositoryError> {
        let mut state = self.state.write().await;
        let run = state
            .runs
            .get_mut(run_id)
            .ok_or_else(|| RepositoryError::RunNotFound {
                run: run_id.clone(),
            })?;
        run.metrics = metrics;
        run.gate_decisions = crate::evaluate_gates(&run.manifest.evaluation_gates, &run.metrics);
        let previous = run.status.clone();
        let all_succeeded = run.steps.len() == run.workflow.steps.len()
            && run
                .steps
                .values()
                .all(|step| step.status == StepStatus::Succeeded);
        let has_failed = run
            .steps
            .values()
            .any(|step| step.status == StepStatus::Failed);
        if has_failed {
            run.status = RunStatus::Failed;
        } else if all_succeeded {
            run.status = if run.gate_decisions.values().all(|decision| decision.passed) {
                RunStatus::Succeeded
            } else {
                run.error = Some("one or more evaluation gates failed".to_owned());
                RunStatus::Failed
            };
        } else {
            return Ok(run.clone());
        }
        if previous != run.status {
            let status = run.status.clone();
            let _ = run;
            append_event(
                &mut state,
                run_id,
                ExecutionEvent::RunFinished {
                    run_id: run_id.clone(),
                    status,
                },
            )?;
        }
        state
            .runs
            .get(run_id)
            .cloned()
            .ok_or_else(|| RepositoryError::RunNotFound {
                run: run_id.clone(),
            })
    }

    async fn cancel_run(&self, run_id: &RunId) -> Result<RunRecord, RepositoryError> {
        let mut state = self.state.write().await;
        let run = state
            .runs
            .get_mut(run_id)
            .ok_or_else(|| RepositoryError::RunNotFound {
                run: run_id.clone(),
            })?;
        if matches!(
            run.status,
            RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
        ) {
            return Ok(run.clone());
        }
        run.status = RunStatus::Cancelled;
        for record in run.steps.values_mut() {
            if !matches!(record.status, StepStatus::Succeeded | StepStatus::Failed) {
                if let Some(attempt) = record.history.last_mut() {
                    if attempt.status == StepStatus::Running {
                        attempt.finished_at_ms = Some(now_ms());
                        attempt.status = StepStatus::Cancelled;
                    }
                }
                record.status = StepStatus::Cancelled;
                record.lease = None;
            }
        }
        let _ = run;
        append_event(
            &mut state,
            run_id,
            ExecutionEvent::RunFinished {
                run_id: run_id.clone(),
                status: RunStatus::Cancelled,
            },
        )?;
        state
            .runs
            .get(run_id)
            .cloned()
            .ok_or_else(|| RepositoryError::RunNotFound {
                run: run_id.clone(),
            })
    }

    async fn recover_expired(&self) -> Result<Vec<(RunId, StepId)>, RepositoryError> {
        let mut state = self.state.write().await;
        let now = now_ms();
        let mut expired = Vec::new();
        let run_ids = state.runs.keys().cloned().collect::<Vec<_>>();
        for run_id in run_ids {
            let step_specs = state
                .runs
                .get(&run_id)
                .map(|run| run.workflow.steps.clone())
                .ok_or_else(|| RepositoryError::RunNotFound {
                    run: run_id.clone(),
                })?;
            for step in step_specs {
                let event = state
                    .runs
                    .get_mut(&run_id)
                    .and_then(|run| run.steps.get_mut(&step.id))
                    .and_then(|record| recover_expired_step(&run_id, &step, record, now));
                if let Some(event) = event {
                    let failed_error = state
                        .runs
                        .get(&run_id)
                        .and_then(|run| run.steps.get(&step.id))
                        .filter(|record| record.status == StepStatus::Failed)
                        .and_then(|record| record.error.clone());
                    if failed_error.is_some() {
                        if let Some(run) = state.runs.get_mut(&run_id) {
                            run.error = failed_error;
                        }
                    }
                    expired.push((run_id.clone(), step.id));
                    append_event(&mut state, &run_id, event)?;
                }
            }
        }
        Ok(expired)
    }

    async fn events_since(
        &self,
        run_id: &RunId,
        sequence: u64,
    ) -> Result<Vec<EventEnvelope>, RepositoryError> {
        let state = self.state.read().await;
        let run = state
            .runs
            .get(run_id)
            .ok_or_else(|| RepositoryError::RunNotFound {
                run: run_id.clone(),
            })?;
        Ok(run
            .event_log
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect())
    }
}
