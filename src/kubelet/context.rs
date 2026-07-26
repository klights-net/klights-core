use std::fmt;
use std::sync::Arc;

use crate::kubelet::pod_creation_state::PodStartRetryTracker;
use crate::kubelet::pod_lifecycle_router::PodLifecycleRouter;
use crate::kubelet::pod_repository::PodRepository;
use crate::node_outbox::Outbox;
use klights_supervisor::{FileProcessExecutor, TaskSupervisor};

pub(crate) type PodLifecycleReceiver = Arc<
    tokio::sync::Mutex<
        Option<tokio::sync::mpsc::Receiver<crate::kubelet::lifecycle::LifecycleCommand>>,
    >,
>;

#[derive(Clone, Default)]
pub(crate) struct HostIpState {
    value: Arc<std::sync::RwLock<Option<Arc<str>>>>,
}

impl HostIpState {
    pub(crate) fn publish(&self, value: String) {
        *self
            .value
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::from(value));
    }

    pub(crate) fn current(&self) -> Arc<str> {
        self.value
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or_else(|| Arc::from("127.0.0.1"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KubeletConfig {
    service_cidr: String,
    node_name: String,
    containerd_namespace: String,
    log_rotation: crate::kubelet::log_rotation::LogRotationPolicy,
    node_capacity: crate::kubelet::node::NodeCapacity,
    paths: crate::kubelet::runtime_paths::KubeletRuntimePaths,
}

impl KubeletConfig {
    pub(crate) fn try_new(
        service_cidr: String,
        node_name: String,
        containerd_namespace: String,
        log_rotation: crate::kubelet::log_rotation::LogRotationPolicy,
        node_capacity: crate::kubelet::node::NodeCapacity,
        paths: crate::kubelet::runtime_paths::KubeletRuntimePaths,
    ) -> Result<Self, KubeletConfigError> {
        validate_service_cidr(&service_cidr)?;
        validate_nonempty("node_name", &node_name)?;
        validate_nonempty("containerd_namespace", &containerd_namespace)?;
        Ok(Self {
            service_cidr,
            node_name,
            containerd_namespace,
            log_rotation,
            node_capacity,
            paths,
        })
    }

    pub(crate) fn service_cidr(&self) -> &str {
        &self.service_cidr
    }

    pub(crate) fn node_name(&self) -> &str {
        &self.node_name
    }

    pub(crate) fn containerd_namespace(&self) -> &str {
        &self.containerd_namespace
    }

    pub(crate) fn log_rotation(&self) -> crate::kubelet::log_rotation::LogRotationPolicy {
        self.log_rotation
    }

    pub(crate) fn node_capacity(&self) -> crate::kubelet::node::NodeCapacity {
        self.node_capacity
    }

    pub(crate) fn paths(&self) -> &crate::kubelet::runtime_paths::KubeletRuntimePaths {
        &self.paths
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum KubeletConfigError {
    Empty { field: &'static str },
    InvalidServiceCidr(String),
}

impl fmt::Display for KubeletConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { field } => write!(formatter, "{field} must not be empty"),
            Self::InvalidServiceCidr(value) => {
                write!(formatter, "service_cidr must be a valid IP CIDR: {value}")
            }
        }
    }
}

impl std::error::Error for KubeletConfigError {}

fn validate_nonempty(field: &'static str, value: &str) -> Result<(), KubeletConfigError> {
    if value.is_empty() {
        return Err(KubeletConfigError::Empty { field });
    }
    Ok(())
}

fn validate_service_cidr(value: &str) -> Result<(), KubeletConfigError> {
    let Some((address, prefix)) = value.split_once('/') else {
        return Err(KubeletConfigError::InvalidServiceCidr(value.to_string()));
    };
    let address = address
        .parse::<std::net::IpAddr>()
        .map_err(|_| KubeletConfigError::InvalidServiceCidr(value.to_string()))?;
    let prefix = prefix
        .parse::<u8>()
        .map_err(|_| KubeletConfigError::InvalidServiceCidr(value.to_string()))?;
    let max_prefix = if address.is_ipv4() { 32 } else { 128 };
    if prefix > max_prefix {
        return Err(KubeletConfigError::InvalidServiceCidr(value.to_string()));
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct KubeletLifecycleServices {
    pub(crate) pod_repository: Arc<PodRepository>,
    pub(crate) pod_lifecycle_router: Arc<PodLifecycleRouter>,
    pub(crate) pod_lifecycle_rx: PodLifecycleReceiver,
    pub(crate) pod_start_retry_state: PodStartRetryTracker,
}

impl KubeletLifecycleServices {
    pub(crate) fn new(
        pod_repository: Arc<PodRepository>,
        pod_lifecycle_router: Arc<PodLifecycleRouter>,
        pod_lifecycle_rx: PodLifecycleReceiver,
        pod_start_retry_state: PodStartRetryTracker,
    ) -> Self {
        Self {
            pod_repository,
            pod_lifecycle_router,
            pod_lifecycle_rx,
            pod_start_retry_state,
        }
    }
}

#[derive(Clone)]
pub(crate) struct KubeletRuntimeNetworkServices {
    pub(crate) datapath: Arc<dyn klights_network_api::Datapath>,
    pub(crate) peering: Arc<dyn klights_network_api::PeerRouter>,
    pub(crate) services: Arc<dyn klights_network_api::ServiceRouter>,
}

impl KubeletRuntimeNetworkServices {
    pub(crate) fn new(
        datapath: Arc<dyn klights_network_api::Datapath>,
        peering: Arc<dyn klights_network_api::PeerRouter>,
        services: Arc<dyn klights_network_api::ServiceRouter>,
    ) -> Self {
        Self {
            datapath,
            peering,
            services,
        }
    }
}

#[derive(Clone)]
pub(crate) struct KubeletStatusDeliveryServices {
    pub(crate) resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    pub(crate) cache_readiness: Arc<dyn klights_leader_api::LeaderCacheReadiness>,
    pub(crate) pod_cleanup_intents: Arc<dyn klights_leader_api::LeaderPodCleanupIntents>,
    pub(crate) projected_tokens: Arc<dyn klights_leader_api::LeaderProjectedServiceAccountToken>,
    pub(crate) outbox: Arc<Outbox>,
}

impl KubeletStatusDeliveryServices {
    pub(crate) fn new(
        resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
        cache_readiness: Arc<dyn klights_leader_api::LeaderCacheReadiness>,
        pod_cleanup_intents: Arc<dyn klights_leader_api::LeaderPodCleanupIntents>,
        projected_tokens: Arc<dyn klights_leader_api::LeaderProjectedServiceAccountToken>,
        outbox: Arc<Outbox>,
    ) -> Self {
        Self {
            resource_query,
            cache_readiness,
            pod_cleanup_intents,
            projected_tokens,
            outbox,
        }
    }
}

#[derive(Clone)]
pub(crate) struct KubeletLocalExecutionServices {
    pub(crate) pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
    pub(crate) pod_endpoint_store: Arc<dyn klights_node_store::PodEndpointStore>,
    pub(crate) task_supervisor: Arc<TaskSupervisor>,
    pub(crate) file_process: FileProcessExecutor,
    pub(crate) config: KubeletConfig,
}

impl KubeletLocalExecutionServices {
    pub(crate) fn new(
        pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
        pod_endpoint_store: Arc<dyn klights_node_store::PodEndpointStore>,
        task_supervisor: Arc<TaskSupervisor>,
        file_process: FileProcessExecutor,
        config: KubeletConfig,
    ) -> Self {
        Self {
            pod_runtime_store,
            pod_endpoint_store,
            task_supervisor,
            file_process,
            config,
        }
    }
}

/// Kubelet-owned composition aggregate.
///
/// Root constructs this object once. Consumers receive a narrow projection,
/// never this complete aggregate or root configuration types.
#[derive(Clone)]
pub(crate) struct KubeletServices {
    lifecycle: KubeletLifecycleServices,
    runtime_network: KubeletRuntimeNetworkServices,
    status_delivery: KubeletStatusDeliveryServices,
    local_execution: KubeletLocalExecutionServices,
}

impl KubeletServices {
    pub(crate) fn new(
        lifecycle: KubeletLifecycleServices,
        runtime_network: KubeletRuntimeNetworkServices,
        status_delivery: KubeletStatusDeliveryServices,
        local_execution: KubeletLocalExecutionServices,
    ) -> Self {
        Self {
            lifecycle,
            runtime_network,
            status_delivery,
            local_execution,
        }
    }

    pub(crate) fn lifecycle(&self) -> KubeletLifecycleServices {
        self.lifecycle.clone()
    }

    pub(crate) fn runtime_network(&self) -> KubeletRuntimeNetworkServices {
        self.runtime_network.clone()
    }

    pub(crate) fn status_delivery(&self) -> KubeletStatusDeliveryServices {
        self.status_delivery.clone()
    }

    pub(crate) fn local_execution(&self) -> KubeletLocalExecutionServices {
        self.local_execution.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_paths() -> crate::kubelet::runtime_paths::KubeletRuntimePaths {
        crate::kubelet::runtime_paths::KubeletRuntimePaths::new(std::path::PathBuf::from(
            "/tmp/klights-context-test",
        ))
        .unwrap()
    }

    #[test]
    fn kubelet_config_accepts_root_validated_facts() {
        let config = KubeletConfig::try_new(
            "10.43.128.0/17".to_string(),
            "worker-a".to_string(),
            "klights".to_string(),
            crate::kubelet::log_rotation::LogRotationPolicy::default(),
            crate::kubelet::node::NodeCapacity::default(),
            test_paths(),
        )
        .unwrap();

        assert_eq!(config.service_cidr(), "10.43.128.0/17");
        assert_eq!(config.node_name(), "worker-a");
        assert_eq!(config.containerd_namespace(), "klights");
    }

    #[test]
    fn kubelet_config_rejects_invalid_facts() {
        assert!(matches!(
            KubeletConfig::try_new(
                "not-a-cidr".to_string(),
                "worker-a".to_string(),
                "klights".to_string(),
                crate::kubelet::log_rotation::LogRotationPolicy::default(),
                crate::kubelet::node::NodeCapacity::default(),
                test_paths(),
            ),
            Err(KubeletConfigError::InvalidServiceCidr(_))
        ));
        assert_eq!(
            KubeletConfig::try_new(
                "10.43.128.0/17".to_string(),
                String::new(),
                "klights".to_string(),
                crate::kubelet::log_rotation::LogRotationPolicy::default(),
                crate::kubelet::node::NodeCapacity::default(),
                test_paths(),
            ),
            Err(KubeletConfigError::Empty { field: "node_name" })
        );
    }

    #[test]
    fn host_ip_state_is_instance_owned_and_defaults_to_loopback() {
        let first = HostIpState::default();
        let first_clone = first.clone();
        let second = HostIpState::default();

        assert_eq!(&*first.current(), "127.0.0.1");
        first.publish("192.0.2.10".to_string());

        assert_eq!(&*first.current(), "192.0.2.10");
        assert_eq!(
            &*first_clone.current(),
            "192.0.2.10",
            "clones of one injected instance must observe the same publication"
        );
        assert_eq!(&*second.current(), "127.0.0.1");
    }
}
