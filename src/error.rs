use crate::{StepId, TaskId, WorkflowId};
use thiserror::Error;

/// An identifier was rejected before it entered a workflow definition.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{kind} cannot be empty")]
pub struct IdentifierError {
    pub kind: &'static str,
}

/// A process-local registry rejected a task registration.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RegistryError {
    #[error("task {task} was already registered")]
    DuplicateTask { task: TaskId },
}

/// An application-provided persistence or event adapter failed.
#[derive(Debug, Error)]
#[error("{adapter} adapter failed: {message}")]
pub struct AdapterError {
    pub adapter: &'static str,
    pub message: String,
}

impl AdapterError {
    #[must_use]
    pub fn new(adapter: &'static str, message: impl Into<String>) -> Self {
        Self {
            adapter,
            message: message.into(),
        }
    }
}

/// Errors returned by a task implementation.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TaskError {
    #[error("task input is invalid: {0}")]
    InvalidInput(String),
    #[error("task failed transiently: {0}")]
    Transient(String),
    #[error("task failed permanently: {0}")]
    Permanent(String),
    #[error("task timed out after {timeout_ms} ms")]
    Timeout { timeout_ms: u64 },
    #[error("task serialization failed: {0}")]
    Serialization(String),
}

impl TaskError {
    #[must_use]
    pub fn retryable(&self) -> bool {
        matches!(self, Self::Transient(_) | Self::Timeout { .. })
    }
}

/// Invalid workflow topology or task registration.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkflowError {
    #[error("workflow {workflow} contains duplicate step {step}")]
    DuplicateStep { workflow: WorkflowId, step: StepId },
    #[error("workflow {workflow} step {step} depends on unknown step {dependency}")]
    UnknownDependency {
        workflow: WorkflowId,
        step: StepId,
        dependency: StepId,
    },
    #[error("workflow {workflow} contains a dependency cycle")]
    DependencyCycle { workflow: WorkflowId },
    #[error("workflow {workflow} has no steps")]
    Empty { workflow: WorkflowId },
    #[error("task {task} is not registered")]
    UnregisteredTask { task: TaskId },
    #[error(
        "workflow {workflow} references task {task} from step {step}, but the registered task has a different id"
    )]
    TaskIdMismatch {
        workflow: WorkflowId,
        step: StepId,
        task: TaskId,
    },
    #[error("workflow {workflow} step {step} cannot run on the selected worker")]
    UnsatisfiedPolicy { workflow: WorkflowId, step: StepId },
}

/// A durable repository rejected a state transition or could not find the
/// requested execution record.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RepositoryError {
    #[error("run {run} was not found")]
    RunNotFound { run: crate::RunId },
    #[error("step {step} in run {run} is leased by another worker")]
    LeaseLost {
        run: crate::RunId,
        step: crate::StepId,
    },
    #[error("run {run} has an incompatible idempotency key")]
    IdempotencyConflict { run: crate::RunId },
    #[error("run {run} cannot transition from {from} to {to}")]
    InvalidTransition {
        run: crate::RunId,
        from: String,
        to: String,
    },
    #[error("run {run} changed while it was being updated")]
    Conflict { run: crate::RunId },
    #[error("completion for step {step} in run {run} does not match its claim")]
    InvalidCompletion {
        run: crate::RunId,
        step: crate::StepId,
    },
    #[error("event does not belong to run {run}")]
    InvalidEvent { run: crate::RunId },
    #[error("invalid run request: {message}")]
    InvalidRequest { message: String },
    #[error("repository storage failed: {message}")]
    Storage { message: String },
}

/// Errors raised by the orchestration runtime or its adapters.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OrchestratorError {
    #[error(transparent)]
    Workflow(#[from] WorkflowError),
    #[error(transparent)]
    Store(AdapterError),
    #[error(transparent)]
    EventSink(AdapterError),
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    #[error(transparent)]
    Artifact(AdapterError),
}
