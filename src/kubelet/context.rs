use std::sync::Arc;

use crate::KlightsConfig;
use crate::bootstrap::{NodeMode, NodeRole};
use crate::control_plane::client::LeaderApiClient;
use crate::datastore::node_local::NodeLocalHandle;
use crate::kubelet::outbox::Outbox;
use crate::kubelet::pod_creation_state::PodStartRetryTracker;
use crate::kubelet::pod_lifecycle_router::PodLifecycleRouter;
use crate::kubelet::pod_repository::PodRepository;
use crate::networking::Network;
use crate::task_supervisor::TaskSupervisor;

/// Runtime context for kubelet-owned components.
///
/// Control-plane storage is intentionally absent. Cluster reads go through
/// `cluster_api`; cluster writes are durable `outbox` rows drained by the
/// dispatcher.
#[derive(Clone)]
pub struct KubeletContext {
    pub cluster_api: Arc<dyn LeaderApiClient>,
    pub node_local: NodeLocalHandle,
    pub outbox: Arc<Outbox>,
    pub task_supervisor: Arc<TaskSupervisor>,
    pub config: Arc<KlightsConfig>,
    pub node_mode: NodeMode,
    pub role: NodeRole,
    pub network: Arc<Network>,
    pub pod_repository: Arc<PodRepository>,
    pub pod_lifecycle_router: Arc<PodLifecycleRouter>,
    pub pod_probe_manager: Arc<crate::kubelet::ProbeManager>,
    pub pod_lifecycle_rx: Arc<
        tokio::sync::Mutex<
            Option<tokio::sync::mpsc::Receiver<crate::kubelet::lifecycle::LifecycleCommand>>,
        >,
    >,
    pub pod_start_retry_state: PodStartRetryTracker,
}
