use crate::IdentifierError;
use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Creates an identifier after rejecting empty or whitespace-only values.
            pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(IdentifierError {
                        kind: stringify!($name),
                    });
                }
                Ok(Self(value))
            }

            /// Returns the identifier as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<&str> for $name {
            type Error = IdentifierError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id_type!(RunId);
id_type!(StepId);
id_type!(TaskId);
id_type!(WorkflowId);
id_type!(AttemptId);
id_type!(ArtifactId);
id_type!(EventId);
id_type!(IdempotencyKey);
id_type!(WorkerId);
id_type!(CacheKey);

/// Generates a process-local identifier suitable for an execution record.
pub(crate) fn generated_id(prefix: &str, sequence: u64) -> String {
    format!("{prefix}-{}-{sequence}", uuid::Uuid::new_v4().simple())
}
