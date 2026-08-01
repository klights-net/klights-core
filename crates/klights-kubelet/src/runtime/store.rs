use std::sync::Arc;

use crate::runtime_clock::RuntimeClock;
use crate::runtime_types::PodRuntimeKey;

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
}

/// Pod slot admission operations.
#[async_trait::async_trait]
pub trait PodSlotAdmission: Send + Sync {
    /// Subscribe to pod slot admission events.
    /// Returns a broadcast receiver for slot changes.
    fn subscribe(&self) -> Box<dyn klights_node_store::PodSlotEventSubscription>;

    /// Try to admit a pod into a slot.
    async fn try_admit(
        &self,
        key: &PodRuntimeKey,
        node_name: &str,
    ) -> anyhow::Result<klights_node_store::PodSlotAdmissionResult>;

    /// Clear a pod's slot by UID-qualified key.
    async fn clear_slot(&self, key: &PodRuntimeKey) -> anyhow::Result<()>;
}

// --- Production adapters ---

/// Production runtime store adapter over the datastore backend.
pub struct RealPodRuntimeStore {
    store: Arc<dyn klights_node_store::PodRuntimeStore>,
    node_name: String,
    clock: Arc<dyn RuntimeClock>,
}

impl RealPodRuntimeStore {
    pub fn new(
        store: Arc<dyn klights_node_store::PodRuntimeStore>,
        node_name: impl Into<String>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        Self {
            store,
            node_name: node_name.into(),
            clock,
        }
    }
}

#[async_trait::async_trait]
impl PodRuntimeStore for RealPodRuntimeStore {
    async fn record_sandbox(&self, key: &PodRuntimeKey, sandbox_id: &str) -> anyhow::Result<()> {
        // This adapter owns the node-local runtime row lifecycle. Persist
        // identity, node ownership, and sandbox in one node-store command so a
        // successful startup can always be recovered and reconciled. The UID
        // primary key prevents same-name replacement aliasing.
        let sandbox = klights_node_store::OwnedPodSandbox::try_new(
            klights_types::PodIdentity::new(&key.namespace, &key.name, &key.uid),
            self.node_name.clone(),
            sandbox_id,
            self.clock.now_ms(),
        )?;
        self.store
            .record_owned_sandbox(sandbox)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn get_sandbox_id(&self, key: &PodRuntimeKey) -> anyhow::Result<Option<String>> {
        let pod_uid = klights_node_store::RuntimePodUid::try_new(&key.uid)?;
        self.store
            .get_pod_runtime(pod_uid)
            .await
            .map_err(anyhow::Error::from)
            .map(|record| {
                record
                    .filter(|record| {
                        record.pod().namespace == key.namespace && record.pod().name == key.name
                    })
                    .and_then(|record| record.sandbox_id().map(ToString::to_string))
            })
    }

    async fn delete_sandbox(&self, key: &PodRuntimeKey) -> anyhow::Result<()> {
        let pod_uid = klights_node_store::RuntimePodUid::try_new(&key.uid)?;
        self.store
            .delete_pod_runtime_for_uid(pod_uid)
            .await
            .map_err(anyhow::Error::from)
    }
}

/// Production slot admission adapter over the datastore backend.
pub struct RealPodSlotAdmission {
    store: Arc<dyn klights_node_store::PodSlotAdmissionStore>,
    events: Arc<dyn klights_node_store::PodSlotAdmissionEventSource>,
    node_name: String,
}

impl RealPodSlotAdmission {
    pub fn new(
        store: Arc<dyn klights_node_store::PodSlotAdmissionStore>,
        events: Arc<dyn klights_node_store::PodSlotAdmissionEventSource>,
        node_name: String,
    ) -> Self {
        Self {
            store,
            events,
            node_name,
        }
    }
}

#[async_trait::async_trait]
impl PodSlotAdmission for RealPodSlotAdmission {
    fn subscribe(&self) -> Box<dyn klights_node_store::PodSlotEventSubscription> {
        self.events.subscribe()
    }

    async fn try_admit(
        &self,
        key: &PodRuntimeKey,
        node_name: &str,
    ) -> anyhow::Result<klights_node_store::PodSlotAdmissionResult> {
        let request = klights_node_store::PodSlotAdmissionRequest::try_new(
            klights_types::PodIdentity::new(&key.namespace, &key.name, &key.uid),
            node_name,
        )?;
        self.store
            .try_admit(request)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn clear_slot(&self, key: &PodRuntimeKey) -> anyhow::Result<()> {
        let request = klights_node_store::PodSlotAdmissionRequest::try_new(
            klights_types::PodIdentity::new(&key.namespace, &key.name, &key.uid),
            &self.node_name,
        )?;
        self.store
            .clear_if_uid(request)
            .await
            .map_err(anyhow::Error::from)?;
        Ok(())
    }
}
