use crate::{EvaluationGate, TaskId, TaskSpec, WorkflowId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Reproducibility and audit information supplied by the application.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct RunProvenance {
    pub model: Option<String>,
    pub prompt: Option<String>,
    pub dataset: Option<String>,
    pub input_hash: Option<String>,
    pub calibration: Option<String>,
    pub code_revision: Option<String>,
    pub environment: BTreeMap<String, String>,
}

/// Provenance captured when a workflow starts. Task metadata is intentionally
/// generic: applications can record model, prompt, dataset, code revision, or
/// residency information without making those concepts framework dependencies.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RunManifest {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub workflow_id: WorkflowId,
    pub workflow_version: String,
    pub tasks: BTreeMap<TaskId, TaskSpec>,
    pub evaluation_gates: Vec<EvaluationGate>,
    pub provenance: RunProvenance,
    pub metadata: BTreeMap<String, String>,
}

fn schema_version() -> u32 {
    1
}

impl RunManifest {
    #[must_use]
    pub fn new(
        workflow_id: WorkflowId,
        workflow_version: impl Into<String>,
        tasks: BTreeMap<TaskId, TaskSpec>,
    ) -> Self {
        Self {
            schema_version: 1,
            workflow_id,
            workflow_version: workflow_version.into(),
            tasks,
            evaluation_gates: Vec::new(),
            provenance: RunProvenance::default(),
            metadata: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_evaluation_gates(mut self, gates: Vec<EvaluationGate>) -> Self {
        self.evaluation_gates = gates;
        self
    }

    #[must_use]
    pub fn with_provenance(mut self, provenance: RunProvenance) -> Self {
        self.provenance = provenance;
        self
    }
}
