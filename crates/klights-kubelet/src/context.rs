//! Kubelet-owned composition and validated runtime facts.
//!
//! The application root supplies focused capabilities. This module groups
//! those capabilities by kubelet concern and retains the one app-owned
//! supervisor. It deliberately does not select or construct concrete
//! datastore, replication, RPC, API, controller, or networking
//! implementations.

use std::fmt;
use std::sync::Arc;

/// Single-consumer lifecycle command receiver shared by kubelet composition.
pub type PodLifecycleReceiver = Arc<
    tokio::sync::Mutex<Option<tokio::sync::mpsc::Receiver<crate::lifecycle::LifecycleCommand>>>,
>;

/// Instance-owned publication of the host IP used by Pod status projection.
#[derive(Clone, Default)]
pub struct HostIpState {
    value: Arc<std::sync::RwLock<Option<Arc<str>>>>,
}

impl HostIpState {
    pub fn publish(&self, value: String) {
        *self
            .value
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::from(value));
    }

    pub fn current(&self) -> Arc<str> {
        self.value
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or_else(|| Arc::from("127.0.0.1"))
    }
}

/// Runtime facts validated once before any kubelet task starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KubeletConfig<LogRotation, NodeCapacity, RuntimePaths> {
    service_cidr: String,
    node_name: String,
    containerd_namespace: String,
    log_rotation: LogRotation,
    node_capacity: NodeCapacity,
    paths: RuntimePaths,
}

impl<LogRotation, NodeCapacity, RuntimePaths>
    KubeletConfig<LogRotation, NodeCapacity, RuntimePaths>
{
    pub fn try_new(
        service_cidr: String,
        node_name: String,
        containerd_namespace: String,
        log_rotation: LogRotation,
        node_capacity: NodeCapacity,
        paths: RuntimePaths,
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

    pub fn service_cidr(&self) -> &str {
        &self.service_cidr
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn containerd_namespace(&self) -> &str {
        &self.containerd_namespace
    }

    pub fn log_rotation(&self) -> LogRotation
    where
        LogRotation: Copy,
    {
        self.log_rotation
    }

    pub fn node_capacity(&self) -> NodeCapacity
    where
        NodeCapacity: Copy,
    {
        self.node_capacity
    }

    pub fn paths(&self) -> &RuntimePaths {
        &self.paths
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KubeletConfigError {
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

/// Focused Pod lifecycle capabilities supplied by the application root.
pub struct LifecycleServices<Repository, Router, Receiver, RetryTracker> {
    pub pod_repository: Arc<Repository>,
    pub pod_lifecycle_router: Arc<Router>,
    pub pod_lifecycle_rx: Receiver,
    pub pod_start_retry_state: RetryTracker,
}

impl<Repository, Router, Receiver, RetryTracker> Clone
    for LifecycleServices<Repository, Router, Receiver, RetryTracker>
where
    Receiver: Clone,
    RetryTracker: Clone,
{
    fn clone(&self) -> Self {
        Self {
            pod_repository: self.pod_repository.clone(),
            pod_lifecycle_router: self.pod_lifecycle_router.clone(),
            pod_lifecycle_rx: self.pod_lifecycle_rx.clone(),
            pod_start_retry_state: self.pod_start_retry_state.clone(),
        }
    }
}

impl<Repository, Router, Receiver, RetryTracker>
    LifecycleServices<Repository, Router, Receiver, RetryTracker>
{
    pub fn new(
        pod_repository: Arc<Repository>,
        pod_lifecycle_router: Arc<Router>,
        pod_lifecycle_rx: Receiver,
        pod_start_retry_state: RetryTracker,
    ) -> Self {
        Self {
            pod_repository,
            pod_lifecycle_router,
            pod_lifecycle_rx,
            pod_start_retry_state,
        }
    }
}

/// Focused network capabilities used by kubelet runtime work.
#[derive(Clone)]
pub struct RuntimeNetworkServices {
    pub datapath: Arc<dyn klights_network_api::Datapath>,
    pub peering: Arc<dyn klights_network_api::PeerRouter>,
    pub services: Arc<dyn klights_network_api::ServiceRouter>,
}

impl RuntimeNetworkServices {
    pub fn new(
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

/// Focused leader capabilities and durable node-output producer.
pub struct StatusDeliveryServices<Outbox: ?Sized> {
    pub resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    pub cache_readiness: Arc<dyn klights_leader_api::LeaderCacheReadiness>,
    pub pod_cleanup_intents: Arc<dyn klights_leader_api::LeaderPodCleanupIntents>,
    pub projected_tokens: Arc<dyn klights_leader_api::LeaderProjectedServiceAccountToken>,
    pub outbox: Arc<Outbox>,
}

impl<Outbox: ?Sized> Clone for StatusDeliveryServices<Outbox> {
    fn clone(&self) -> Self {
        Self {
            resource_query: self.resource_query.clone(),
            cache_readiness: self.cache_readiness.clone(),
            pod_cleanup_intents: self.pod_cleanup_intents.clone(),
            projected_tokens: self.projected_tokens.clone(),
            outbox: self.outbox.clone(),
        }
    }
}

impl<Outbox: ?Sized> StatusDeliveryServices<Outbox> {
    pub fn new(
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

/// Node-local execution capabilities retained by kubelet composition.
pub struct LocalExecutionServices<Clock: ?Sized, Config> {
    pub pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
    pub pod_endpoint_store: Arc<dyn klights_node_store::PodEndpointStore>,
    pub wall_clock: Arc<Clock>,
    pub task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    pub file_process: klights_supervisor::FileProcessExecutor,
    pub config: Config,
}

impl<Clock: ?Sized, Config: Clone> Clone for LocalExecutionServices<Clock, Config> {
    fn clone(&self) -> Self {
        Self {
            pod_runtime_store: self.pod_runtime_store.clone(),
            pod_endpoint_store: self.pod_endpoint_store.clone(),
            wall_clock: self.wall_clock.clone(),
            task_supervisor: self.task_supervisor.clone(),
            file_process: self.file_process.clone(),
            config: self.config.clone(),
        }
    }
}

impl<Clock: ?Sized, Config> LocalExecutionServices<Clock, Config> {
    pub fn new(
        pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
        pod_endpoint_store: Arc<dyn klights_node_store::PodEndpointStore>,
        wall_clock: Arc<Clock>,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
        file_process: klights_supervisor::FileProcessExecutor,
        config: Config,
    ) -> Self {
        Self {
            pod_runtime_store,
            pod_endpoint_store,
            wall_clock,
            task_supervisor,
            file_process,
            config,
        }
    }
}

/// The complete kubelet capability aggregate is intentionally crate-private.
#[derive(Clone)]
struct KubeletServices<Lifecycle, RuntimeNetwork, StatusDelivery, LocalExecution> {
    lifecycle: Lifecycle,
    runtime_network: RuntimeNetwork,
    status_delivery: StatusDelivery,
    local_execution: LocalExecution,
}

/// Public handle exposing only focused capability projections.
#[derive(Clone)]
pub struct KubeletContext<Lifecycle, RuntimeNetwork, StatusDelivery, LocalExecution> {
    services: KubeletServices<Lifecycle, RuntimeNetwork, StatusDelivery, LocalExecution>,
}

impl<Lifecycle, RuntimeNetwork, StatusDelivery, LocalExecution>
    KubeletContext<Lifecycle, RuntimeNetwork, StatusDelivery, LocalExecution>
{
    pub fn new(
        lifecycle: Lifecycle,
        runtime_network: RuntimeNetwork,
        status_delivery: StatusDelivery,
        local_execution: LocalExecution,
    ) -> Self {
        Self {
            services: KubeletServices {
                lifecycle,
                runtime_network,
                status_delivery,
                local_execution,
            },
        }
    }
}

impl<Lifecycle, RuntimeNetwork, StatusDelivery, LocalExecution>
    KubeletContext<Lifecycle, RuntimeNetwork, StatusDelivery, LocalExecution>
where
    Lifecycle: Clone,
    RuntimeNetwork: Clone,
    StatusDelivery: Clone,
    LocalExecution: Clone,
{
    pub fn lifecycle(&self) -> Lifecycle {
        self.services.lifecycle.clone()
    }

    pub fn runtime_network(&self) -> RuntimeNetwork {
        self.services.runtime_network.clone()
    }

    pub fn status_delivery(&self) -> StatusDelivery {
        self.services.status_delivery.clone()
    }

    pub fn local_execution(&self) -> LocalExecution {
        self.services.local_execution.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::KubeletContext;

    #[test]
    fn private_aggregate_exposes_only_focused_projections() {
        let context = KubeletContext::new(1_u8, 2_u8, 3_u8, 4_u8);

        assert_eq!(context.lifecycle(), 1);
        assert_eq!(context.runtime_network(), 2);
        assert_eq!(context.status_delivery(), 3);
        assert_eq!(context.local_execution(), 4);
    }
}
