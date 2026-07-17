use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};

use crate::{IdentifierError, WorkerId};

/// A serializable delay used by retry policy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetryDelay {
    pub initial_ms: u64,
    pub multiplier: u32,
    pub max_ms: u64,
}

impl RetryDelay {
    #[must_use]
    pub const fn none() -> Self {
        Self {
            initial_ms: 0,
            multiplier: 1,
            max_ms: 0,
        }
    }

    #[must_use]
    pub fn for_attempt(self, attempt: u32) -> Duration {
        if attempt == 0 || self.initial_ms == 0 {
            return Duration::ZERO;
        }
        let exponent = attempt.saturating_sub(1).min(20);
        let factor = self.multiplier.saturating_pow(exponent);
        let delay = self.initial_ms.saturating_mul(u64::from(factor));
        Duration::from_millis(delay.min(self.max_ms.max(self.initial_ms)))
    }
}

impl Default for RetryDelay {
    fn default() -> Self {
        Self {
            initial_ms: 25,
            multiplier: 2,
            max_ms: 1_000,
        }
    }
}

/// Retry policy for transient task failures and timeouts.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetryPolicy {
    /// Total attempts for the step, including the first attempt. Primary and
    /// fallback execution share this budget; a fallback selected after the
    /// primary exhausts the budget receives one final attempt.
    pub max_attempts: u32,
    pub delay: RetryDelay,
}

impl RetryPolicy {
    #[must_use]
    pub const fn never() -> Self {
        Self {
            max_attempts: 1,
            delay: RetryDelay::none(),
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            delay: RetryDelay::default(),
        }
    }
}

/// Where a task is permitted to execute.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum Residency {
    #[default]
    Any,
    Region(String),
}

impl Residency {
    /// Returns whether a worker residency satisfies this requirement.
    #[must_use]
    pub fn allows(&self, worker: &Self) -> bool {
        match (self, worker) {
            (Self::Any, _) => true,
            (Self::Region(required), Self::Region(actual)) => required == actual,
            (Self::Region(_), Self::Any) => false,
        }
    }
}

/// Resource and capability requirements for a step.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResourceRequirements {
    pub cpu_millis: u32,
    pub memory_mb: u32,
    pub gpu_count: u16,
    pub capabilities: BTreeSet<String>,
}

/// A worker's advertised placement and capacity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerProfile {
    pub id: WorkerId,
    pub residency: Residency,
    pub resources: ResourceRequirements,
}

impl WorkerProfile {
    pub fn new(id: impl Into<String>) -> Result<Self, IdentifierError> {
        Ok(Self {
            id: WorkerId::new(id.into())?,
            residency: Residency::Any,
            resources: ResourceRequirements::default(),
        })
    }

    /// Returns whether this worker can satisfy a step's resource policy.
    #[must_use]
    pub fn supports(&self, policy: &ExecutionPolicy) -> bool {
        policy.residency.allows(&self.residency)
            && self.resources.cpu_millis >= policy.resources.cpu_millis
            && self.resources.memory_mb >= policy.resources.memory_mb
            && self.resources.gpu_count >= policy.resources.gpu_count
            && policy
                .resources
                .capabilities
                .is_subset(&self.resources.capabilities)
    }

    /// Returns whether this worker can run a concrete task as well as its
    /// step policy. An empty worker capability set means local/unrestricted.
    #[must_use]
    pub fn supports_task(&self, task: &crate::TaskSpec, policy: &ExecutionPolicy) -> bool {
        self.supports(policy)
            && (self.resources.capabilities.is_empty()
                || self.resources.capabilities.contains(&task.capability))
    }
}

impl Default for WorkerProfile {
    fn default() -> Self {
        Self {
            // The literal is part of the framework's local-worker invariant.
            id: WorkerId::new("local-worker").expect("static worker id is valid"),
            residency: Residency::Any,
            resources: ResourceRequirements::default(),
        }
    }
}

/// Per-step execution behavior. This is intentionally data-only so a workflow
/// can be persisted, reviewed, and replayed by another runtime.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecutionPolicy {
    pub retry: RetryPolicy,
    pub timeout_ms: Option<u64>,
    pub fallback_task: Option<crate::TaskId>,
    pub residency: Residency,
    pub resources: ResourceRequirements,
}

impl Default for ExecutionPolicy {
    fn default() -> Self {
        Self {
            retry: RetryPolicy::default(),
            timeout_ms: Some(30_000),
            fallback_task: None,
            residency: Residency::Any,
            resources: ResourceRequirements::default(),
        }
    }
}
