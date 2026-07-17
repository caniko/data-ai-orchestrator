use crate::{
    EvaluationGate, ExecutionPolicy, IdentifierError, StepId, TaskId, WorkflowError, WorkflowId,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// One executable step in a workflow graph.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StepSpec {
    pub id: StepId,
    pub task: TaskId,
    pub depends_on: Vec<StepId>,
    pub policy: ExecutionPolicy,
}

impl StepSpec {
    pub fn new(id: impl Into<String>, task: impl Into<String>) -> Result<Self, IdentifierError> {
        Ok(Self {
            id: StepId::new(id.into())?,
            task: TaskId::new(task.into())?,
            depends_on: Vec::new(),
            policy: ExecutionPolicy::default(),
        })
    }

    pub fn depends_on(mut self, step: impl Into<String>) -> Result<Self, IdentifierError> {
        self.depends_on.push(StepId::new(step.into())?);
        Ok(self)
    }
}

/// A serializable workflow definition. Workflows are data, while task
/// implementations are registered capabilities supplied by the application.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowSpec {
    pub id: WorkflowId,
    pub version: String,
    pub steps: Vec<StepSpec>,
    pub evaluation_gates: Vec<EvaluationGate>,
}

impl WorkflowSpec {
    pub fn new(id: impl Into<String>, version: impl Into<String>) -> Result<Self, IdentifierError> {
        Ok(Self {
            id: WorkflowId::new(id.into())?,
            version: version.into(),
            steps: Vec::new(),
            evaluation_gates: Vec::new(),
        })
    }

    pub fn add_gate(&mut self, gate: EvaluationGate) {
        self.evaluation_gates.push(gate);
    }

    pub fn add_step(&mut self, step: StepSpec) -> Result<(), WorkflowError> {
        if self.steps.iter().any(|existing| existing.id == step.id) {
            return Err(WorkflowError::DuplicateStep {
                workflow: self.id.clone(),
                step: step.id,
            });
        }
        self.steps.push(step);
        Ok(())
    }

    pub fn validate(&self) -> Result<(), WorkflowError> {
        if self.steps.is_empty() {
            return Err(WorkflowError::Empty {
                workflow: self.id.clone(),
            });
        }

        let ids: BTreeSet<_> = self.steps.iter().map(|step| step.id.clone()).collect();
        for step in &self.steps {
            for dependency in &step.depends_on {
                if !ids.contains(dependency) {
                    return Err(WorkflowError::UnknownDependency {
                        workflow: self.id.clone(),
                        step: step.id.clone(),
                        dependency: dependency.clone(),
                    });
                }
            }
        }

        let mut indegree: BTreeMap<StepId, usize> = self
            .steps
            .iter()
            .map(|step| (step.id.clone(), step.depends_on.len()))
            .collect();
        let mut dependents: BTreeMap<StepId, Vec<StepId>> = BTreeMap::new();
        for step in &self.steps {
            for dependency in &step.depends_on {
                dependents
                    .entry(dependency.clone())
                    .or_default()
                    .push(step.id.clone());
            }
        }
        let mut ready: VecDeque<_> = indegree
            .iter()
            .filter_map(|(id, degree)| (*degree == 0).then_some(id.clone()))
            .collect();
        let mut visited = 0;
        while let Some(id) = ready.pop_front() {
            visited += 1;
            if let Some(children) = dependents.get(&id) {
                for child in children {
                    let Some(degree) = indegree.get_mut(child) else {
                        return Err(WorkflowError::UnknownDependency {
                            workflow: self.id.clone(),
                            step: child.clone(),
                            dependency: id.clone(),
                        });
                    };
                    *degree -= 1;
                    if *degree == 0 {
                        ready.push_back(child.clone());
                    }
                }
            }
        }
        if visited != self.steps.len() {
            return Err(WorkflowError::DependencyCycle {
                workflow: self.id.clone(),
            });
        }
        Ok(())
    }
}
