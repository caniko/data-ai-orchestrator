use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum MetricDirection {
    AtLeast,
    AtMost,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Metric {
    pub name: String,
    pub value: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EvaluationGate {
    pub metric: String,
    pub threshold: f64,
    pub direction: MetricDirection,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GateDecision {
    pub passed: bool,
    pub metric: String,
    pub observed: Option<f64>,
    pub threshold: f64,
    pub direction: MetricDirection,
}

impl EvaluationGate {
    #[must_use]
    pub fn evaluate(&self, metrics: &[Metric]) -> GateDecision {
        let observed = metrics
            .iter()
            .find(|metric| metric.name == self.metric)
            .map(|metric| metric.value);
        let passed = observed.is_some_and(|value| match self.direction {
            MetricDirection::AtLeast => value >= self.threshold,
            MetricDirection::AtMost => value <= self.threshold,
        });
        GateDecision {
            passed,
            metric: self.metric.clone(),
            observed,
            threshold: self.threshold,
            direction: self.direction,
        }
    }
}

/// Evaluates every gate and returns a decision per metric name.
#[must_use]
pub fn evaluate_gates(
    gates: &[EvaluationGate],
    metrics: &[Metric],
) -> BTreeMap<String, GateDecision> {
    gates
        .iter()
        .map(|gate| (gate.metric.clone(), gate.evaluate(metrics)))
        .collect()
}
