use crate::{AdapterError, ArtifactId, CacheKey, TaskSpec};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::RwLock;

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for &byte in bytes {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

/// Metadata attached to an immutable artifact.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactMetadata {
    pub media_type: String,
    pub metadata: BTreeMap<String, String>,
}

/// A content-addressed reference suitable for workflow inputs and outputs.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactRef {
    pub id: ArtifactId,
    pub sha256: String,
    pub size_bytes: u64,
    pub metadata: ArtifactMetadata,
}

/// A deterministic cache key for a task invocation.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct TaskCacheKey {
    pub key: CacheKey,
    pub task: crate::TaskId,
    pub task_version: String,
}

impl TaskCacheKey {
    /// Derives a key from the task identity and its JSON input.
    pub fn for_input(task: &TaskSpec, input: &serde_json::Value) -> Result<Self, AdapterError> {
        let bytes = serde_json::to_vec(input)
            .map_err(|error| AdapterError::new("cache-key", error.to_string()))?;
        let digest = Sha256::digest(bytes);
        let input_hash = hex_digest(&digest);
        let key = CacheKey::new(format!("{}@{}:{input_hash}", task.id, task.version))
            .map_err(|error| AdapterError::new("cache-key", error.to_string()))?;
        Ok(Self {
            key,
            task: task.id.clone(),
            task_version: task.version.clone(),
        })
    }
}

/// Storage boundary for datasets, model outputs, and other large payloads.
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    async fn put(
        &self,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<ArtifactRef, AdapterError>;

    async fn get(&self, artifact: &ArtifactRef) -> Result<Option<Vec<u8>>, AdapterError>;
}

/// Optional memoization boundary for deterministic task invocations.
#[async_trait]
pub trait TaskCache: Send + Sync {
    async fn get(&self, key: &TaskCacheKey) -> Result<Option<serde_json::Value>, AdapterError>;
    async fn put(&self, key: TaskCacheKey, output: serde_json::Value) -> Result<(), AdapterError>;
}

#[derive(Clone)]
struct StoredArtifact {
    reference: ArtifactRef,
    bytes: Arc<[u8]>,
}

/// Deterministic process-local artifact storage used by tests and local runs.
#[derive(Default)]
pub struct InMemoryArtifactStore {
    artifacts: RwLock<BTreeMap<ArtifactId, StoredArtifact>>,
}

/// Process-local cache implementation for deterministic task outputs.
#[derive(Default)]
pub struct InMemoryTaskCache {
    values: RwLock<BTreeMap<CacheKey, serde_json::Value>>,
}

#[async_trait]
impl TaskCache for InMemoryTaskCache {
    async fn get(&self, key: &TaskCacheKey) -> Result<Option<serde_json::Value>, AdapterError> {
        Ok(self.values.read().await.get(&key.key).cloned())
    }

    async fn put(&self, key: TaskCacheKey, output: serde_json::Value) -> Result<(), AdapterError> {
        self.values.write().await.insert(key.key, output);
        Ok(())
    }
}

#[async_trait]
impl ArtifactStore for InMemoryArtifactStore {
    async fn put(
        &self,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<ArtifactRef, AdapterError> {
        let digest = Sha256::digest(&bytes);
        let sha256 = hex_digest(&digest);
        let id = ArtifactId::new(format!("sha256-{sha256}"))
            .map_err(|error| AdapterError::new("memory-artifact", error.to_string()))?;
        let reference = ArtifactRef {
            id: id.clone(),
            sha256,
            size_bytes: bytes.len() as u64,
            metadata,
        };
        self.artifacts.write().await.insert(
            id,
            StoredArtifact {
                reference: reference.clone(),
                bytes: Arc::from(bytes),
            },
        );
        Ok(reference)
    }

    async fn get(&self, artifact: &ArtifactRef) -> Result<Option<Vec<u8>>, AdapterError> {
        Ok(self
            .artifacts
            .read()
            .await
            .get(&artifact.id)
            .filter(|stored| stored.reference.sha256 == artifact.sha256)
            .map(|stored| stored.bytes.to_vec()))
    }
}
