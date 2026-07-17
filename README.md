# Data AI Orchestrator

<!-- simit:badges:start -->

[![CI](https://img.shields.io/badge/CI-managed+extra-2088ff)](.forgejo/workflows/ci.yaml) [![docs](https://img.shields.io/badge/docs-enabled-6f42c1)](https://docs.rs/data-ai-orchestrator) [![crates.io](https://img.shields.io/badge/crates.io-ready-f46623)](https://crates.io/crates/data-ai-orchestrator)

<!-- simit:badges:end -->

`data-ai-orchestrator` is a domain-free Rust core for durable, policy-driven
data and AI workflows.

It separates four concerns:

- **Workflow data**: serializable steps, dependencies, versions, and policies.
- **Capabilities**: application-provided tasks behind a small async `Task`
  trait. Tasks can wrap a model, an embedding service, a retriever, a data
  transform, or a human-review operation.
- **Runtime**: dependency-aware execution, bounded parallelism, retries,
  timeouts, fallback tasks, event emission, and run persistence.
- **Quality**: reusable metric gates for promoting or rejecting a run.

The initial runtime is intentionally process-local. Applications can replace
the `RunStore` and `EventSink` implementations with SQLx, queues, object
storage, or another durable system without changing task implementations.

The framework also includes a durable worker path. [`DurableExecutor`] uses a
[`RunRepository`] to claim steps with fencing leases, renew long-running work,
commit results and event envelopes atomically, recover expired workers, and
apply evaluation gates before finalizing a run. `InMemoryRunStore` implements
both the snapshot and repository contracts for local development and adapter
tests.

## Example

```rust
use async_trait::async_trait;
use data_ai_orchestrator::{InMemoryExecutor, InMemoryTaskRegistry, Task, TaskContext,
    TaskError, TaskSpec, StepSpec, WorkflowSpec};
use serde_json::{json, Value};
use std::sync::Arc;

struct Normalize {
    spec: TaskSpec,
}

#[async_trait]
impl Task for Normalize {
    fn spec(&self) -> &TaskSpec { &self.spec }

    async fn execute(&self, input: Value, _context: TaskContext) -> Result<Value, TaskError> {
        Ok(json!({ "normalized": input }))
    }
}

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut registry = InMemoryTaskRegistry::default();
registry.register(Normalize {
    spec: TaskSpec::new("normalize", "1", "data-normalization")?,
})?;

let mut workflow = WorkflowSpec::new("ingest", "1")?;
workflow.add_step(StepSpec::new("normalize", "normalize")?)?;

let run = InMemoryExecutor::new(Arc::new(registry))
    .execute(workflow, json!({ "value": "input" }))
    .await?;
assert!(matches!(run.status, data_ai_orchestrator::RunStatus::Succeeded));
# Ok(())
# }
```

## Design boundary

The core does not know about application entities, HTTP clients, SQL schemas,
model vendors, or UI. Those belong in adapters. The next production layer
should provide a repository implementation with database-native claiming and
resumption. The optional `postgres` feature provides a JSONB-backed
`RunRepository`; each claim, lease transition, completion, event, and recovery
operation uses a row-level transaction.

Large data should use [`ArtifactStore`] and [`ArtifactRef`] rather than being
embedded in a run record. The manifest already accepts model, prompt, dataset,
input-hash, calibration, code-revision, and environment provenance without
making those concepts dependencies of the core.

## Execution semantics

- Workflow definitions are validated as DAGs before submission.
- Task implementations are object-safe and JSON-facing; `TypedTaskAdapter`
  provides strongly typed Serde inputs and outputs at that edge. Use
  `TypedTaskAdapter::with_envelope()` when a typed task needs dependency output
  as well as the original run input.
- Retry and fallback decisions are represented in the step state machine, with
  retry backoff persisted so workers do not hold leases while sleeping. A
  terminal fallback failure cannot select the fallback again.
- Repository claims carry a worker id, attempt number, and fencing token.
- Event envelopes have monotonic sequence numbers and can be read incrementally
  by a control plane or projection.
- Completion is at-least-once at the worker boundary; adapters must make their
  side effects idempotent when a lease expires during task execution.
- Residency and resource requirements are checked against a `WorkerProfile`
  before execution.

## Optional PostgreSQL adapter

Enable the adapter with `--features postgres`, provide a compatible PostgreSQL
pool to `PostgresRunStore::new`, and call `PostgresRunStore::migrate()` before
using it as a snapshot store or `RunRepository`. The migration adds the
idempotency uniqueness index. The pool type is re-exported as `PgPool` when the
feature is enabled. The core does not require SQLx, PostgreSQL, or a particular
queue implementation.

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

The PostgreSQL integration smoke test is opt-in because it needs a disposable
database. Run it with `POSTGRES_TEST_DATABASE_URL` set:

```sh
POSTGRES_TEST_DATABASE_URL=postgres://... \
  cargo test --all-features --test postgres -- --ignored
```
