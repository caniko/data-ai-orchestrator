//! A small, domain-free orchestration core for data and AI workflows.
//!
//! The crate deliberately separates workflow policy and execution from the
//! application that owns data, persistence, and model clients. Applications
//! provide [`Task`] implementations and adapters for [`RunStore`] and
//! [`EventSink`].

mod artifact;
mod control;
mod durable;
mod error;
mod evaluation;
mod ids;
mod manifest;
mod policy;
#[cfg(feature = "postgres")]
mod postgres;
mod runtime;
mod store;
mod task;
mod workflow;

pub use artifact::{
    ArtifactMetadata, ArtifactRef, ArtifactStore, InMemoryArtifactStore, InMemoryTaskCache,
    TaskCache, TaskCacheKey,
};
pub use control::RunController;
pub use durable::{DurableExecutor, DurableExecutorOptions, WorkOutcome};
pub use error::{
    AdapterError, IdentifierError, OrchestratorError, RegistryError, RepositoryError, TaskError,
    WorkflowError,
};
pub use evaluation::{EvaluationGate, GateDecision, Metric, MetricDirection, evaluate_gates};
pub use ids::{
    ArtifactId, AttemptId, CacheKey, EventId, IdempotencyKey, RunId, StepId, TaskId, WorkerId,
    WorkflowId,
};
pub use manifest::{RunManifest, RunProvenance};
pub use policy::{
    ExecutionPolicy, Residency, ResourceRequirements, RetryDelay, RetryPolicy, WorkerProfile,
};
#[cfg(feature = "postgres")]
pub use postgres::PostgresRunStore;
pub use runtime::{Executor, ExecutorOptions, InMemoryExecutor};
#[cfg(feature = "postgres")]
pub use sqlx_postgres::PgPool;
pub use store::{
    EventEnvelope, EventSink, ExecutionEvent, InMemoryEventSink, InMemoryRunStore, RunRecord,
    RunRepository, RunRequest, RunStatus, RunStore, StepAttempt, StepClaim, StepCompletion,
    StepLease, StepRecord, StepStatus,
};
pub use task::{
    InMemoryTaskRegistry, Task, TaskContext, TaskRegistry, TaskSpec, TypedTask, TypedTaskAdapter,
};
pub use workflow::{StepSpec, WorkflowSpec};

/// A convenient result type for orchestration APIs.
pub type Result<T> = std::result::Result<T, OrchestratorError>;
