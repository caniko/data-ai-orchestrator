//! PostgreSQL persistence for the snapshot and transactional repository APIs.
//!
//! Each repository transition locks one JSONB run row with `FOR UPDATE`,
//! applies the same state-machine checks as the in-memory implementation, and
//! commits the new snapshot and event log in one transaction.

use crate::store::{
    append_run_event, build_step_input, completion_matches_claim, event_belongs_to_run,
    event_is_valid_for_record, recover_expired_step, validate_run_record_schema,
    validate_run_request,
};
use crate::{
    AdapterError, AttemptId, EventEnvelope, ExecutionEvent, IdempotencyKey, RepositoryError, RunId,
    RunRecord, RunRepository, RunRequest, RunStatus, RunStore, StepAttempt, StepClaim,
    StepCompletion, StepLease, StepRecord, StepStatus, WorkerProfile,
};
use async_trait::async_trait;
use sqlx_core::{query::query, row::Row};
use sqlx_postgres::{PgPool, PgTransaction};
use std::collections::BTreeMap;

/// A PostgreSQL-backed implementation of both snapshot and transactional
/// repository contracts.
#[derive(Clone)]
pub struct PostgresRunStore {
    pool: PgPool,
}

impl PostgresRunStore {
    /// Creates an adapter around an existing connection pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Returns the pool used by this adapter.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Creates or upgrades the schema required by this adapter.
    pub async fn migrate(&self) -> Result<(), AdapterError> {
        query(
            "CREATE TABLE IF NOT EXISTS orchestrator_runs (
                run_id TEXT PRIMARY KEY,
                revision BIGINT NOT NULL,
                payload JSONB NOT NULL,
                idempotency_key TEXT,
                lease_expires_at_ms BIGINT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(|error| AdapterError::new("postgres", error.to_string()))?;
        query(
            "ALTER TABLE orchestrator_runs
             ADD COLUMN IF NOT EXISTS idempotency_key TEXT",
        )
        .execute(&self.pool)
        .await
        .map_err(|error| AdapterError::new("postgres", error.to_string()))?;
        query(
            "ALTER TABLE orchestrator_runs
             ADD COLUMN IF NOT EXISTS lease_expires_at_ms BIGINT",
        )
        .execute(&self.pool)
        .await
        .map_err(|error| AdapterError::new("postgres", error.to_string()))?;
        query(
            "UPDATE orchestrator_runs
             SET lease_expires_at_ms = (
                 SELECT MIN((step.value->'lease'->>'expires_at_ms')::BIGINT)
                 FROM jsonb_each(payload->'steps') AS step
                 WHERE step.value->>'lease' IS NOT NULL
             )
             WHERE lease_expires_at_ms IS NULL",
        )
        .execute(&self.pool)
        .await
        .map_err(|error| AdapterError::new("postgres", error.to_string()))?;
        query(
            "CREATE UNIQUE INDEX IF NOT EXISTS orchestrator_runs_idempotency_idx
             ON orchestrator_runs (idempotency_key)
             WHERE idempotency_key IS NOT NULL",
        )
        .execute(&self.pool)
        .await
        .map_err(|error| AdapterError::new("postgres", error.to_string()))?;
        query(
            "CREATE INDEX IF NOT EXISTS orchestrator_runs_updated_at_idx
             ON orchestrator_runs (updated_at)",
        )
        .execute(&self.pool)
        .await
        .map_err(|error| AdapterError::new("postgres", error.to_string()))?;
        query(
            "CREATE INDEX IF NOT EXISTS orchestrator_runs_lease_expiry_idx
             ON orchestrator_runs (lease_expires_at_ms)
             WHERE lease_expires_at_ms IS NOT NULL",
        )
        .execute(&self.pool)
        .await
        .map_err(|error| AdapterError::new("postgres", error.to_string()))?;
        Ok(())
    }
}

fn storage(error: impl ToString) -> RepositoryError {
    RepositoryError::Storage {
        message: error.to_string(),
    }
}

const RECOVERY_BATCH_SIZE: i64 = 100;

async fn database_now_ms(tx: &mut PgTransaction<'_>) -> Result<u64, RepositoryError> {
    let row = query("SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT AS now_ms")
        .fetch_one(&mut **tx)
        .await
        .map_err(storage)?;
    let now_ms: i64 = row.try_get("now_ms").map_err(storage)?;
    u64::try_from(now_ms).map_err(storage)
}

fn decode_payload(payload: serde_json::Value) -> Result<RunRecord, RepositoryError> {
    let run = serde_json::from_value(payload).map_err(storage)?;
    validate_run_record_schema(&run).map_err(storage)?;
    Ok(run)
}

fn decode_payload_adapter(payload: serde_json::Value) -> Result<RunRecord, AdapterError> {
    let run = serde_json::from_value(payload)
        .map_err(|error| AdapterError::new("postgres", format!("invalid run JSON: {error}")))?;
    validate_run_record_schema(&run).map_err(|message| AdapterError::new("postgres", message))?;
    Ok(run)
}

fn lease_expiry_ms(run: &RunRecord) -> Result<Option<i64>, std::num::TryFromIntError> {
    run.steps
        .values()
        .filter_map(|record| record.lease.as_ref().map(|lease| lease.expires_at_ms))
        .min()
        .map(i64::try_from)
        .transpose()
}

async fn load_locked(
    tx: &mut PgTransaction<'_>,
    run_id: &RunId,
) -> Result<Option<RunRecord>, RepositoryError> {
    let row = query("SELECT payload FROM orchestrator_runs WHERE run_id = $1 FOR UPDATE")
        .bind(run_id.as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(storage)?;
    row.map(|row| {
        let payload: serde_json::Value = row.try_get("payload").map_err(storage)?;
        decode_payload(payload)
    })
    .transpose()
}

async fn persist_locked(
    tx: &mut PgTransaction<'_>,
    run: &RunRecord,
) -> Result<(), RepositoryError> {
    let payload = serde_json::to_value(run).map_err(storage)?;
    let revision = i64::try_from(run.revision).map_err(storage)?;
    let lease_expires_at_ms = lease_expiry_ms(run).map_err(storage)?;
    let result = query(
        "UPDATE orchestrator_runs
         SET revision = $2, payload = $3, idempotency_key = $4,
             lease_expires_at_ms = $5, updated_at = now()
         WHERE run_id = $1 AND revision < $2",
    )
    .bind(run.run_id.as_str())
    .bind(revision)
    .bind(payload)
    .bind(run.idempotency_key.as_ref().map(IdempotencyKey::as_str))
    .bind(lease_expires_at_ms)
    .execute(&mut **tx)
    .await
    .map_err(storage)?;
    if result.rows_affected() == 0 {
        return Err(RepositoryError::Conflict {
            run: run.run_id.clone(),
        });
    }
    Ok(())
}

fn next_lease_token(run: &RunRecord) -> u64 {
    run.steps
        .values()
        .flat_map(|record| record.history.iter().map(|attempt| attempt.lease_token))
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

fn event_envelope(
    run: &mut RunRecord,
    event: ExecutionEvent,
) -> Result<EventEnvelope, RepositoryError> {
    append_run_event(run, event)
}

#[async_trait]
impl RunStore for PostgresRunStore {
    async fn save(&self, run: &RunRecord) -> Result<(), AdapterError> {
        validate_run_record_schema(run)
            .map_err(|message| AdapterError::new("postgres", message))?;
        let payload = serde_json::to_value(run)
            .map_err(|error| AdapterError::new("postgres", error.to_string()))?;
        let revision = i64::try_from(run.revision).map_err(|error| {
            AdapterError::new("postgres", format!("run revision is too large: {error}"))
        })?;
        let lease_expires_at_ms = lease_expiry_ms(run).map_err(|error| {
            AdapterError::new("postgres", format!("lease expiry is too large: {error}"))
        })?;
        let result = query(
            "INSERT INTO orchestrator_runs
                (run_id, revision, payload, idempotency_key, lease_expires_at_ms)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (run_id) DO UPDATE SET
                revision = EXCLUDED.revision,
                payload = EXCLUDED.payload,
                idempotency_key = EXCLUDED.idempotency_key,
                lease_expires_at_ms = EXCLUDED.lease_expires_at_ms,
                updated_at = now()
             WHERE orchestrator_runs.revision < EXCLUDED.revision",
        )
        .bind(run.run_id.as_str())
        .bind(revision)
        .bind(payload)
        .bind(run.idempotency_key.as_ref().map(IdempotencyKey::as_str))
        .bind(lease_expires_at_ms)
        .execute(&self.pool)
        .await
        .map_err(|error| AdapterError::new("postgres", error.to_string()))?;
        if result.rows_affected() == 0 {
            return Err(AdapterError::new("postgres", "stale run revision rejected"));
        }
        Ok(())
    }

    async fn load(&self, run_id: &RunId) -> Result<Option<RunRecord>, AdapterError> {
        let row = query("SELECT payload FROM orchestrator_runs WHERE run_id = $1")
            .bind(run_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|error| AdapterError::new("postgres", error.to_string()))?;
        row.map(|row| {
            let payload: serde_json::Value = row.try_get("payload").map_err(|error| {
                AdapterError::new("postgres", format!("invalid run payload: {error}"))
            })?;
            decode_payload_adapter(payload)
        })
        .transpose()
    }
}

#[async_trait]
impl RunRepository for PostgresRunStore {
    async fn create_run(&self, request: RunRequest) -> Result<RunRecord, RepositoryError> {
        validate_run_request(&request)?;
        let mut tx = self.pool.begin().await.map_err(storage)?;
        if let Some(key) = &request.idempotency_key {
            let row = query(
                "SELECT payload FROM orchestrator_runs
                 WHERE idempotency_key = $1 FOR UPDATE",
            )
            .bind(key.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?;
            if let Some(row) = row {
                let existing = decode_payload(row.try_get("payload").map_err(storage)?)?;
                if existing.workflow == request.workflow
                    && existing.manifest == request.manifest
                    && existing.input == request.input
                {
                    tx.commit().await.map_err(storage)?;
                    return Ok(existing);
                }
                return Err(RepositoryError::IdempotencyConflict {
                    run: existing.run_id,
                });
            }
        }
        if load_locked(&mut tx, &request.run_id).await?.is_some() {
            return Err(RepositoryError::Conflict {
                run: request.run_id,
            });
        }
        let mut run = RunRecord::new(
            request.run_id,
            request.workflow,
            request.manifest,
            request.input,
        );
        run.status = RunStatus::Running;
        run.idempotency_key = request.idempotency_key;
        let workflow_id = run.workflow.id.clone();
        let created_run_id = run.run_id.clone();
        event_envelope(
            &mut run,
            ExecutionEvent::RunStarted {
                run_id: created_run_id,
                workflow_id,
            },
        )?;
        let payload = serde_json::to_value(&run).map_err(storage)?;
        let lease_expires_at_ms = lease_expiry_ms(&run).map_err(storage)?;
        let inserted = query(
            "INSERT INTO orchestrator_runs
                (run_id, revision, payload, idempotency_key, lease_expires_at_ms)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT DO NOTHING",
        )
        .bind(run.run_id.as_str())
        .bind(i64::try_from(run.revision).map_err(storage)?)
        .bind(payload)
        .bind(run.idempotency_key.as_ref().map(IdempotencyKey::as_str))
        .bind(lease_expires_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        if inserted.rows_affected() == 0 {
            if let Some(key) = &run.idempotency_key {
                if let Some(row) = query(
                    "SELECT payload FROM orchestrator_runs
                     WHERE idempotency_key = $1 FOR UPDATE",
                )
                .bind(key.as_str())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?
                {
                    let existing = decode_payload(row.try_get("payload").map_err(storage)?)?;
                    if existing.workflow == run.workflow
                        && existing.manifest == run.manifest
                        && existing.input == run.input
                    {
                        tx.commit().await.map_err(storage)?;
                        return Ok(existing);
                    }
                    return Err(RepositoryError::IdempotencyConflict {
                        run: existing.run_id,
                    });
                }
            }
            return Err(RepositoryError::Conflict { run: run.run_id });
        }
        tx.commit().await.map_err(storage)?;
        Ok(run)
    }

    async fn load_run(&self, run_id: &RunId) -> Result<Option<RunRecord>, RepositoryError> {
        self.load(run_id)
            .await
            .map_err(|error| storage(error.message))
    }

    async fn claim_next_runnable_step(
        &self,
        worker: &WorkerProfile,
        lease_ms: u64,
    ) -> Result<Option<StepClaim>, RepositoryError> {
        // The candidate scan is advisory. claim_next_step reacquires and
        // validates the run lock, so concurrent workers cannot receive an
        // unfenced claim.
        let candidates = query(
            "SELECT run_id
             FROM orchestrator_runs
             WHERE payload->>'status' = 'Running'
             ORDER BY updated_at, run_id
             LIMIT $1",
        )
        .bind(RECOVERY_BATCH_SIZE)
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        for row in candidates {
            let run_id = RunId::new(row.try_get::<String, _>("run_id").map_err(storage)?).map_err(
                |error| RepositoryError::InvalidRequest {
                    message: error.to_string(),
                },
            )?;
            if let Some(claim) = self.claim_next_step(&run_id, worker, lease_ms).await? {
                return Ok(Some(claim));
            }
        }
        Ok(None)
    }

    async fn claim_next_step(
        &self,
        run_id: &RunId,
        worker: &WorkerProfile,
        lease_ms: u64,
    ) -> Result<Option<StepClaim>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let Some(mut run) = load_locked(&mut tx, run_id).await? else {
            return Err(RepositoryError::RunNotFound {
                run: run_id.clone(),
            });
        };
        if run.status != RunStatus::Running {
            tx.commit().await.map_err(storage)?;
            return Ok(None);
        }
        let now = database_now_ms(&mut tx).await?;
        let next = run.workflow.steps.iter().find_map(|step| {
            let record = run.steps.get(&step.id);
            let task_id = record.map_or_else(|| step.task.clone(), |record| record.task.clone());
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
                    && record.next_attempt_at_ms.is_none_or(|next| next <= now)
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
        });
        let Some((step, task_id, dependencies)) = next else {
            tx.commit().await.map_err(storage)?;
            return Ok(None);
        };
        let task =
            run.manifest
                .tasks
                .get(&task_id)
                .cloned()
                .ok_or_else(|| RepositoryError::Conflict {
                    run: run_id.clone(),
                })?;
        let token = next_lease_token(&run);
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
        record.task = task_id;
        record.status = StepStatus::Running;
        record.attempts = record.attempts.saturating_add(1);
        record.next_attempt_at_ms = None;
        record.lease = Some(StepLease {
            worker: worker.id.clone(),
            token,
            expires_at_ms: now.saturating_add(lease_ms.max(1)),
        });
        record.history.push(StepAttempt {
            id: AttemptId::new(format!("{run_id}-attempt-{token}")).map_err(|error| {
                RepositoryError::InvalidRequest {
                    message: error.to_string(),
                }
            })?,
            number: record.attempts,
            worker: worker.id.clone(),
            lease_token: token,
            started_at_ms: now,
            finished_at_ms: None,
            status: StepStatus::Running,
            error: None,
        });
        run.revision = run.revision.saturating_add(1);
        let claim = StepClaim {
            run_id: run_id.clone(),
            workflow_id: run.workflow.id.clone(),
            step,
            task,
            input: build_step_input(&run.input, &dependencies),
            worker: worker.id.clone(),
            lease_token: token,
            attempt: record.attempts,
            fallback_used: record.fallback_used,
        };
        persist_locked(&mut tx, &run).await?;
        tx.commit().await.map_err(storage)?;
        Ok(Some(claim))
    }

    async fn renew_step(&self, claim: &StepClaim, lease_ms: u64) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let Some(mut run) = load_locked(&mut tx, &claim.run_id).await? else {
            return Err(RepositoryError::RunNotFound {
                run: claim.run_id.clone(),
            });
        };
        let now = database_now_ms(&mut tx).await?;
        let record =
            run.steps
                .get_mut(&claim.step.id)
                .ok_or_else(|| RepositoryError::LeaseLost {
                    run: claim.run_id.clone(),
                    step: claim.step.id.clone(),
                })?;
        let lease = record
            .lease
            .as_mut()
            .ok_or_else(|| RepositoryError::LeaseLost {
                run: claim.run_id.clone(),
                step: claim.step.id.clone(),
            })?;
        if lease.worker != claim.worker
            || lease.token != claim.lease_token
            || lease.expires_at_ms <= now
        {
            return Err(RepositoryError::LeaseLost {
                run: claim.run_id.clone(),
                step: claim.step.id.clone(),
            });
        }
        lease.expires_at_ms = now.saturating_add(lease_ms.max(1));
        run.revision = run.revision.saturating_add(1);
        persist_locked(&mut tx, &run).await?;
        tx.commit().await.map_err(storage)
    }

    async fn complete_step(
        &self,
        claim: &StepClaim,
        completion: StepCompletion,
    ) -> Result<RunRecord, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let Some(mut run) = load_locked(&mut tx, &claim.run_id).await? else {
            return Err(RepositoryError::RunNotFound {
                run: claim.run_id.clone(),
            });
        };
        let now = database_now_ms(&mut tx).await?;
        let completion_is_valid = completion_matches_claim(&completion, claim, &run.manifest);
        let record =
            run.steps
                .get_mut(&claim.step.id)
                .ok_or_else(|| RepositoryError::LeaseLost {
                    run: claim.run_id.clone(),
                    step: claim.step.id.clone(),
                })?;
        if !record.lease.as_ref().is_some_and(|lease| {
            lease.worker == claim.worker
                && lease.token == claim.lease_token
                && lease.expires_at_ms > now
        }) {
            return Err(RepositoryError::LeaseLost {
                run: claim.run_id.clone(),
                step: claim.step.id.clone(),
            });
        }
        if !completion_is_valid {
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
        if let Some(task) = completion.next_task {
            record.task = task;
        }
        record.lease = None;
        record.next_attempt_at_ms = completion
            .retry_after_ms
            .map(|delay| now.saturating_add(delay));
        if let Some(attempt) = record.history.last_mut() {
            attempt.finished_at_ms = Some(now);
            attempt.status = record.status.clone();
            attempt.error = record.error.clone();
        }
        if record.status == StepStatus::Failed {
            run.error = record.error.clone();
        }
        run.revision = run.revision.saturating_add(1);
        append_run_event(&mut run, completion.event)?;
        persist_locked(&mut tx, &run).await?;
        tx.commit().await.map_err(storage)?;
        Ok(run)
    }

    async fn record_event(
        &self,
        run_id: &RunId,
        event: ExecutionEvent,
    ) -> Result<EventEnvelope, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let Some(mut run) = load_locked(&mut tx, run_id).await? else {
            return Err(RepositoryError::RunNotFound {
                run: run_id.clone(),
            });
        };
        if !event_belongs_to_run(&event, run_id) || !event_is_valid_for_record(&run, &event) {
            return Err(RepositoryError::InvalidEvent {
                run: run_id.clone(),
            });
        }
        let envelope = append_run_event(&mut run, event)?;
        persist_locked(&mut tx, &run).await?;
        tx.commit().await.map_err(storage)?;
        Ok(envelope)
    }

    async fn finish_run(
        &self,
        run_id: &RunId,
        metrics: Vec<crate::Metric>,
    ) -> Result<RunRecord, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let Some(mut run) = load_locked(&mut tx, run_id).await? else {
            return Err(RepositoryError::RunNotFound {
                run: run_id.clone(),
            });
        };
        let gate_decisions = crate::evaluate_gates(&run.manifest.evaluation_gates, &metrics);
        let metrics_changed = run.metrics != metrics || run.gate_decisions != gate_decisions;
        run.metrics = metrics;
        run.gate_decisions = gate_decisions;
        let previous = run.status.clone();
        let all_succeeded = run.steps.len() == run.workflow.steps.len()
            && run
                .steps
                .values()
                .all(|step| step.status == StepStatus::Succeeded);
        if run
            .steps
            .values()
            .any(|step| step.status == StepStatus::Failed)
        {
            run.status = RunStatus::Failed;
        } else if all_succeeded {
            run.status = if run.gate_decisions.values().all(|decision| decision.passed) {
                RunStatus::Succeeded
            } else {
                run.error = Some("one or more evaluation gates failed".to_owned());
                RunStatus::Failed
            };
        }
        if previous == run.status && !metrics_changed {
            tx.commit().await.map_err(storage)?;
            return Ok(run);
        }
        run.revision = run.revision.saturating_add(1);
        if previous != run.status {
            let status = run.status.clone();
            append_run_event(
                &mut run,
                ExecutionEvent::RunFinished {
                    run_id: run_id.clone(),
                    status,
                },
            )?;
        }
        persist_locked(&mut tx, &run).await?;
        tx.commit().await.map_err(storage)?;
        Ok(run)
    }

    async fn cancel_run(&self, run_id: &RunId) -> Result<RunRecord, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let Some(mut run) = load_locked(&mut tx, run_id).await? else {
            return Err(RepositoryError::RunNotFound {
                run: run_id.clone(),
            });
        };
        let now = database_now_ms(&mut tx).await?;
        if matches!(
            run.status,
            RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled
        ) {
            tx.commit().await.map_err(storage)?;
            return Ok(run);
        }
        run.status = RunStatus::Cancelled;
        for record in run.steps.values_mut() {
            if !matches!(record.status, StepStatus::Succeeded | StepStatus::Failed) {
                if let Some(attempt) = record.history.last_mut() {
                    if attempt.status == StepStatus::Running {
                        attempt.finished_at_ms = Some(now);
                        attempt.status = StepStatus::Cancelled;
                    }
                }
                record.status = StepStatus::Cancelled;
                record.lease = None;
            }
        }
        run.revision = run.revision.saturating_add(1);
        append_run_event(
            &mut run,
            ExecutionEvent::RunFinished {
                run_id: run_id.clone(),
                status: RunStatus::Cancelled,
            },
        )?;
        persist_locked(&mut tx, &run).await?;
        tx.commit().await.map_err(storage)?;
        Ok(run)
    }

    async fn recover_expired(&self) -> Result<Vec<(RunId, crate::StepId)>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let now_ms = database_now_ms(&mut tx).await?;
        let rows = query(
            "SELECT payload FROM orchestrator_runs
             WHERE lease_expires_at_ms IS NOT NULL
               AND lease_expires_at_ms <= $1
             ORDER BY lease_expires_at_ms, run_id
             LIMIT $2
             FOR UPDATE SKIP LOCKED",
        )
        .bind(i64::try_from(now_ms).map_err(storage)?)
        .bind(RECOVERY_BATCH_SIZE)
        .fetch_all(&mut *tx)
        .await
        .map_err(storage)?;
        let mut expired = Vec::new();
        for row in rows {
            let payload: serde_json::Value = row.try_get("payload").map_err(storage)?;
            let mut run = decode_payload(payload)?;
            let mut run_changed = false;
            for step in run.workflow.steps.clone() {
                let run_id = run.run_id.clone();
                let event = run
                    .steps
                    .get_mut(&step.id)
                    .and_then(|record| recover_expired_step(&run_id, &step, record, now_ms));
                if let Some(event) = event {
                    if let Some(error) = run
                        .steps
                        .get(&step.id)
                        .filter(|record| record.status == StepStatus::Failed)
                        .and_then(|record| record.error.clone())
                    {
                        run.error = Some(error);
                    }
                    let expired_run_id = run.run_id.clone();
                    append_run_event(&mut run, event)?;
                    expired.push((expired_run_id, step.id));
                    run_changed = true;
                }
            }
            if run_changed {
                run.revision = run.revision.saturating_add(1);
                persist_locked(&mut tx, &run).await?;
            }
        }
        tx.commit().await.map_err(storage)?;
        Ok(expired)
    }

    async fn events_since(
        &self,
        run_id: &RunId,
        sequence: u64,
    ) -> Result<Vec<EventEnvelope>, RepositoryError> {
        let Some(run) = self.load_run(run_id).await? else {
            return Err(RepositoryError::RunNotFound {
                run: run_id.clone(),
            });
        };
        Ok(run
            .event_log
            .into_iter()
            .filter(|event| event.sequence > sequence)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RunManifest, TaskSpec, WorkflowSpec};
    use serde_json::json;

    #[test]
    fn decode_rejects_unsupported_run_schema() {
        let task = TaskSpec::new("task", "1", "capability").expect("valid task");
        let mut tasks = BTreeMap::new();
        tasks.insert(task.id.clone(), task);
        let workflow = WorkflowSpec::new("workflow", "1").expect("valid workflow");
        let manifest = RunManifest::new(workflow.id.clone(), "1", tasks);
        let mut run = RunRecord::new(
            RunId::new("run").expect("valid run id"),
            workflow,
            manifest,
            json!({}),
        );
        run.schema_version = 2;

        let payload = serde_json::to_value(run).expect("serialize run");
        assert!(decode_payload(payload).is_err());
    }

    #[test]
    fn decode_rejects_unsupported_event_schema() {
        let task = TaskSpec::new("task", "1", "capability").expect("valid task");
        let mut tasks = BTreeMap::new();
        tasks.insert(task.id.clone(), task);
        let workflow = WorkflowSpec::new("workflow", "1").expect("valid workflow");
        let manifest = RunManifest::new(workflow.id.clone(), "1", tasks);
        let run_id = RunId::new("run").expect("valid run id");
        let mut run = RunRecord::new(run_id.clone(), workflow, manifest, json!({}));
        let workflow_id = run.workflow.id.clone();
        append_run_event(
            &mut run,
            ExecutionEvent::RunStarted {
                run_id,
                workflow_id,
            },
        )
        .expect("append event");
        run.event_log[0].schema_version = 2;

        let payload = serde_json::to_value(run).expect("serialize run");
        assert!(decode_payload(payload).is_err());
    }
}
