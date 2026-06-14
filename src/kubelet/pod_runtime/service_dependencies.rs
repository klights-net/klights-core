use std::sync::Arc;

use crate::kubelet::pod_cluster_runtime::{ClusterRuntimeView, NodeRuntimeView};
use crate::kubelet::pod_env::EnvSourceReader;
use crate::kubelet::pod_runtime::cri::{ContainerRuntimeControl, CriRuntime};
use crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer;
use crate::kubelet::pod_runtime::events::PodEventSink;
use crate::kubelet::pod_runtime::filesystem::PodFilesystem;
use crate::kubelet::pod_runtime::hooks::PodHookRuntime;
use crate::kubelet::pod_runtime::hostports::HostPortRuntime;
use crate::kubelet::pod_runtime::network::PodNetworkRuntime;
use crate::kubelet::pod_runtime::probes::ProbeRuntime;
use crate::kubelet::pod_runtime::repository::PodRuntimeRepository;
use crate::kubelet::pod_runtime::service::RuntimeConfig;
use crate::kubelet::pod_runtime::store::{PodRuntimeStore, PodSlotAdmission};
use crate::kubelet::pod_runtime::volumes::PodVolumeRuntime;
use crate::task_supervisor::TaskSupervisor;

pub struct RealPodRuntimeServiceDependencies {
    pub cri: Arc<dyn CriRuntime>,
    pub container_control: Arc<dyn ContainerRuntimeControl>,
    pub network: Arc<dyn PodNetworkRuntime>,
    pub store: Arc<dyn PodRuntimeStore>,
    pub slot_admission: Arc<dyn PodSlotAdmission>,
    pub repository: Arc<dyn PodRuntimeRepository>,
    pub filesystem: Arc<dyn PodFilesystem>,
    pub volumes: Arc<dyn PodVolumeRuntime>,
    pub probes: Arc<dyn ProbeRuntime>,
    pub hostports: Arc<dyn HostPortRuntime>,
    pub events: Arc<dyn PodEventSink>,
    pub hooks: Arc<dyn PodHookRuntime>,
    pub env_source: Arc<dyn EnvSourceReader>,
    pub finalizer: Arc<dyn PodDeletionFinalizer>,
    pub supervisor: Arc<TaskSupervisor>,
    pub config: RuntimeConfig,
    pub node_view: Arc<dyn NodeRuntimeView>,
    pub cluster_view: Arc<dyn ClusterRuntimeView>,
}
