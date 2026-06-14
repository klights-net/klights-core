use std::sync::Arc;

use crate::datastore::DatastoreBackend;
use crate::kubelet::pod_runtime::service::PodRuntimeKey;

/// Node-local runtime persistence port for sandbox rows, pod network rows,
/// and pod slot admission.
#[async_trait::async_trait]
pub trait PodRuntimeStore: Send + Sync {
    /// Record a sandbox row keyed by (namespace, pod_name, pod_uid).
    async fn record_sandbox(&self, key: &PodRuntimeKey, sandbox_id: &str) -> anyhow::Result<()>;

    /// Look up sandbox id by UID-qualified key.
    async fn get_sandbox_id(&self, key: &PodRuntimeKey) -> anyhow::Result<Option<String>>;

    /// Delete a sandbox row by UID-qualified key.
    async fn delete_sandbox(&self, key: &PodRuntimeKey) -> anyhow::Result<()>;

    /// Look up sandbox id by namespace/name only (used only at API admission
    /// before UID verification). Callers must validate UID before mutating.
    async fn get_sandbox_id_by_name(
        &self,
        namespace: &str,
        pod_name: &str,
    ) -> anyhow::Result<Option<String>>;
}

/// Pod slot admission operations.
#[async_trait::async_trait]
pub trait PodSlotAdmission: Send + Sync {
    /// Subscribe to pod slot admission events.
    /// Returns a broadcast receiver for slot changes.
    fn subscribe(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::datastore::PodSlotAdmissionEvent>;

    /// Try to admit a pod into a slot.
    async fn try_admit(
        &self,
        key: &PodRuntimeKey,
        node_name: &str,
    ) -> anyhow::Result<crate::datastore::PodSlotAdmissionResult>;

    /// Clear a pod's slot by UID-qualified key.
    async fn clear_slot(&self, key: &PodRuntimeKey) -> anyhow::Result<()>;
}

// --- Production adapters ---

/// Production runtime store adapter over the datastore backend.
pub struct RealPodRuntimeStore {
    db: Arc<dyn DatastoreBackend>,
}

impl RealPodRuntimeStore {
    pub fn new(db: Arc<dyn DatastoreBackend>) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl PodRuntimeStore for RealPodRuntimeStore {
    async fn record_sandbox(&self, key: &PodRuntimeKey, sandbox_id: &str) -> anyhow::Result<()> {
        self.db
            .record_sandbox(&key.namespace, &key.name, &key.uid, sandbox_id)
            .await?;
        Ok(())
    }

    async fn get_sandbox_id(&self, key: &PodRuntimeKey) -> anyhow::Result<Option<String>> {
        self.db
            .get_sandbox_for_uid(&key.namespace, &key.name, &key.uid)
            .await
            .map_err(|e| anyhow::anyhow!("{:#}", e))
    }

    async fn delete_sandbox(&self, key: &PodRuntimeKey) -> anyhow::Result<()> {
        let sandbox_id = match self.get_sandbox_id(key).await? {
            Some(id) => id,
            None => return Ok(()), // already gone
        };
        self.db
            .delete_sandbox_for_uid(&key.namespace, &key.name, &key.uid, &sandbox_id)
            .await?;
        Ok(())
    }

    async fn get_sandbox_id_by_name(
        &self,
        namespace: &str,
        pod_name: &str,
    ) -> anyhow::Result<Option<String>> {
        self.db
            .get_sandbox(namespace, pod_name)
            .await
            .map_err(|e| anyhow::anyhow!("{:#}", e))
    }
}

/// Production slot admission adapter over the datastore backend.
pub struct RealPodSlotAdmission {
    db: Arc<dyn DatastoreBackend>,
    node_name: String,
}

impl RealPodSlotAdmission {
    pub fn new(db: Arc<dyn DatastoreBackend>, node_name: String) -> Self {
        Self { db, node_name }
    }
}

#[async_trait::async_trait]
impl PodSlotAdmission for RealPodSlotAdmission {
    fn subscribe(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::datastore::PodSlotAdmissionEvent> {
        self.db.subscribe_pod_slot_admissions()
    }

    async fn try_admit(
        &self,
        key: &PodRuntimeKey,
        node_name: &str,
    ) -> anyhow::Result<crate::datastore::PodSlotAdmissionResult> {
        self.db
            .pod_slot_try_admit(&key.namespace, &key.name, &key.uid, node_name)
            .await
            .map_err(|e| anyhow::anyhow!("{:#}", e))
    }

    async fn clear_slot(&self, key: &PodRuntimeKey) -> anyhow::Result<()> {
        self.db
            .pod_slot_clear_if_uid(&key.namespace, &key.name, &key.uid, &self.node_name)
            .await?;
        Ok(())
    }
}
