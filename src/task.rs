use crate::{IdentifierError, RegistryError, TaskError, TaskId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

/// Stable metadata describing an executable capability.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskSpec {
    pub id: TaskId,
    pub version: String,
    pub capability: String,
    pub metadata: BTreeMap<String, String>,
}

impl TaskSpec {
    pub fn new(
        id: impl Into<String>,
        version: impl Into<String>,
        capability: impl Into<String>,
    ) -> Result<Self, IdentifierError> {
        Ok(Self {
            id: TaskId::new(id.into())?,
            version: version.into(),
            capability: capability.into(),
            metadata: BTreeMap::new(),
        })
    }
}

/// Context supplied to every task invocation.
#[derive(Clone, Debug)]
pub struct TaskContext {
    pub run_id: crate::RunId,
    pub workflow_id: crate::WorkflowId,
    pub step_id: crate::StepId,
    pub attempt: u32,
    pub task: TaskSpec,
    pub worker: Option<crate::WorkerId>,
}

/// A dynamically registered task boundary.
///
/// JSON is used only at the orchestration boundary. Applications can keep
/// strongly typed inputs and outputs inside a task implementation and perform
/// conversion at the edge.
#[async_trait]
pub trait Task: Send + Sync {
    fn spec(&self) -> &TaskSpec;

    async fn execute(&self, input: Value, context: TaskContext) -> Result<Value, TaskError>;
}

/// A strongly typed task implementation that can be registered through the
/// object-safe [`Task`] boundary.
#[async_trait]
pub trait TypedTask: Send + Sync {
    type Input: DeserializeOwned + Send;
    type Output: Serialize + Send;

    fn spec(&self) -> &TaskSpec;

    async fn execute_typed(
        &self,
        input: Self::Input,
        context: TaskContext,
    ) -> Result<Self::Output, TaskError>;
}

/// Adapts a [`TypedTask`] to the dynamic registry contract.
pub struct TypedTaskAdapter<T> {
    task: T,
    use_envelope: bool,
}

impl<T> TypedTaskAdapter<T> {
    #[must_use]
    pub fn new(task: T) -> Self {
        Self {
            task,
            use_envelope: false,
        }
    }

    /// Preserves the orchestration envelope (`run_input` and `dependencies`)
    /// for typed tasks whose input models the complete step context.
    #[must_use]
    pub fn with_envelope(mut self) -> Self {
        self.use_envelope = true;
        self
    }
}

#[async_trait]
impl<T> Task for TypedTaskAdapter<T>
where
    T: TypedTask + 'static,
{
    fn spec(&self) -> &TaskSpec {
        self.task.spec()
    }

    async fn execute(&self, input: Value, context: TaskContext) -> Result<Value, TaskError> {
        let input = if self.use_envelope {
            input
        } else {
            input.get("run_input").cloned().unwrap_or(input)
        };
        let input = serde_json::from_value(input)
            .map_err(|error| TaskError::Serialization(error.to_string()))?;
        let output = self.task.execute_typed(input, context).await?;
        serde_json::to_value(output).map_err(|error| TaskError::Serialization(error.to_string()))
    }
}

#[async_trait]
pub trait TaskRegistry: Send + Sync {
    fn get(&self, task: &TaskId) -> Option<Arc<dyn Task>>;
}

/// A simple registry suitable for process-local execution and tests.
#[derive(Default)]
pub struct InMemoryTaskRegistry {
    tasks: BTreeMap<TaskId, Arc<dyn Task>>,
}

impl InMemoryTaskRegistry {
    pub fn register<T>(&mut self, task: T) -> Result<(), RegistryError>
    where
        T: Task + 'static,
    {
        let id = task.spec().id.clone();
        if self.tasks.insert(id.clone(), Arc::new(task)).is_some() {
            return Err(RegistryError::DuplicateTask { task: id });
        }
        Ok(())
    }
}

#[async_trait]
impl TaskRegistry for InMemoryTaskRegistry {
    fn get(&self, task: &TaskId) -> Option<Arc<dyn Task>> {
        self.tasks.get(task).cloned()
    }
}
