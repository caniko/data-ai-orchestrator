# Data AI Orchestrator vision

`data-ai-orchestrator` is a domain-free Rust library for running durable,
policy-driven data and AI workflows. Applications embed it and supply their own
capabilities — a model call, an embedding service, a retriever, a data
transform, or a human-review step — behind one async `Task` trait, while the
crate owns step scheduling, persistence, and the quality gates that decide
whether a run is accepted.

## Problem

Data and AI pipelines need dependency-aware execution, bounded parallelism,
retries, timeouts, fallbacks, run persistence, and promotion gates. Framework
code that hard-codes a model, a provider, or a storage backend makes those
concerns hard to reuse and hard to test, and a process-local-only runtime loses
the work when the process dies.

## Principles

- Domain-free core. No model or vendor is part of the crate; capabilities are
  application-provided `Task` implementations.
- Replaceable infrastructure. `RunStore`, `EventSink`, and `RunRepository` are
  traits; the in-memory implementations exist for local development and adapter
  tests, and applications substitute SQLx, queues, or object storage.
- Durability is explicit, not implied. `DurableExecutor` claims steps with
  fencing leases, renews long-running work, commits results and event envelopes
  atomically, recovers expired workers, and applies evaluation gates before
  finalizing a run.
- Identity survives restore. Restoring a snapshot also restores its idempotency
  index and advances the lease/event counters past retained history, so later
  transitions keep their fencing and event identities.
- Quality is part of the runtime: reusable metric gates promote or reject a
  run rather than leaving that decision entirely to the caller.

## Scope

Workflow, step, version, and policy data; the task registry and task context;
the in-memory and durable executors; artifact, control, and manifest types; run
events; and two stores — the in-memory store and an optional PostgreSQL backend
behind the `postgres` feature.

## Non-goals

The crate ships no model, provider client, hosted service, or domain pipeline;
it is a library that applications build those on. The default runtime is
deliberately process-local and does not manage cluster placement.

## Direction

Work has concentrated on the durable path and its PostgreSQL backend: the most
recent change repaired snapshot restore semantics for durable stores. No
roadmap is documented beyond that.
