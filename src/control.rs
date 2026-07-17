use crate::{EventEnvelope, OrchestratorError, RunId, RunRecord, RunRepository};
use std::sync::Arc;

/// Minimal control-plane facade for HTTP, CLI, or queue adapters.
pub struct RunController {
    repository: Arc<dyn RunRepository>,
}

impl RunController {
    #[must_use]
    pub fn new(repository: Arc<dyn RunRepository>) -> Self {
        Self { repository }
    }

    pub async fn inspect(&self, run_id: &RunId) -> Result<Option<RunRecord>, OrchestratorError> {
        self.repository
            .load_run(run_id)
            .await
            .map_err(OrchestratorError::from)
    }

    pub async fn cancel(&self, run_id: &RunId) -> Result<RunRecord, OrchestratorError> {
        self.repository
            .cancel_run(run_id)
            .await
            .map_err(OrchestratorError::from)
    }

    pub async fn events_since(
        &self,
        run_id: &RunId,
        sequence: u64,
    ) -> Result<Vec<EventEnvelope>, OrchestratorError> {
        self.repository
            .events_since(run_id, sequence)
            .await
            .map_err(OrchestratorError::from)
    }

    pub async fn recover_expired(&self) -> Result<Vec<(RunId, crate::StepId)>, OrchestratorError> {
        self.repository
            .recover_expired()
            .await
            .map_err(OrchestratorError::from)
    }
}
