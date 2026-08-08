//! Phase 6: Network boot, nftables, CNI, containerd, and CRI.
//! One function returns all handles needed downstream.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

use crate::KlightsConfig;
use crate::bootstrap::NodeMode;
use crate::bootstrap::networking::{self, NetworkCleanup};
use klights_supervisor::{SupervisedJoinHandle, TaskSupervisor};

pub struct NetworkPhase {
    pub network: Arc<networking::Network>,
    pub services: Arc<dyn klights_network_api::ServiceRouter>,
    pub _local_pod_subnet: String,
    pub cni_rpc_token: CancellationToken,
    pub cni_rpc_handle: SupervisedJoinHandle<()>,
    pub _containerd_manager: Option<klights_kubelet::containerd_manager::ContainerdManager>,
    pub cri_for_pod_watcher: Option<klights_kubelet::cri::CriClient>,
    pub cri_for_api: Option<Arc<tokio::sync::Mutex<klights_kubelet::cri::CriClient>>>,
    pub cni_readiness: klights_kubelet::cni_readiness::CniReadiness,
    pub dataplane_health: klights_networking::dataplane_health::DataplaneHealth,
    pub pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pub pod_runtime_store: Arc<dyn klights_node_store::PodRuntimeStore>,
    pub pod_endpoint_store: Arc<dyn klights_node_store::PodEndpointStore>,
    pub assignment_waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
}

pub struct NetworkBootArgs<'a> {
    pub config: &'a Arc<KlightsConfig>,
    pub node_mode: &'a NodeMode,
    pub node_ip: &'a str,
    pub resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    pub watch: Arc<dyn klights_leader_api::LeaderWatch>,
    pub subnet_allocation: Arc<dyn klights_leader_api::LeaderNodeSubnetAllocation>,
    pub network_topology: Arc<dyn klights_leader_api::LeaderNetworkTopologyQuery>,
    pub pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pub pod_ipam: Arc<dyn klights_node_store::PodIpamStore>,
    pub pod_runtime: Arc<dyn klights_node_store::PodRuntimeStore>,
    pub pod_endpoints: Arc<dyn klights_node_store::PodEndpointStore>,
    pub pod_endpoint_events: Arc<dyn klights_node_store::PodEndpointStoreEventSource>,
    pub network_cleanup: &'a NetworkCleanup,
    pub runtime_paths: &'a klights_kubelet::runtime_paths::KubeletRuntimePaths,
    pub runtime_inputs: crate::bootstrap::runtime_inputs::NetworkRuntimeInputs,
    pub supervisor: Arc<TaskSupervisor>,
    pub grpc_transport_policy: klights_leader_rpc::transport_policy::SharedGrpcTransportPolicy,
    pub shutdown_token: CancellationToken,
}

fn assignment_bus_views() -> (
    Arc<dyn klights_network_api::PodNetworkAssignmentPublisher>,
    Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
) {
    let bus = Arc::new(klights_networking::PodNetworkAssignmentBus::new());
    (bus.clone(), bus)
}

pub async fn boot(args: NetworkBootArgs<'_>) -> Result<NetworkPhase> {
    let NetworkBootArgs {
        config,
        node_mode,
        node_ip,
        resource_query,
        watch,
        subnet_allocation,
        network_topology,
        pod_network_cache,
        pod_ipam,
        pod_runtime,
        pod_endpoints,
        pod_endpoint_events,
        network_cleanup,
        runtime_paths,
        runtime_inputs,
        supervisor,
        grpc_transport_policy,
        shutdown_token,
    } = args;
    let (cni_readiness_publisher, cni_readiness) =
        klights_kubelet::cni_readiness::CniReadiness::channel();
    let (assignment_publisher, assignment_waiter) = assignment_bus_views();
    let mode = match node_mode {
        NodeMode::Root => networking::NetworkMode::Root,
        NodeMode::Rootless { .. } => networking::NetworkMode::Rootless,
    };
    let network_config = networking::NetworkBootConfig::try_new(
        mode,
        &config.bridge_name,
        &config.node_name,
        &config.cluster_cidr,
        node_ip,
        config.dataplane_encryption,
        config.wireguard_device.clone(),
        crate::paths::etc_dir_path(&config.containerd_namespace).join("wireguard-private.key"),
        config.wireguard_port,
    )
    .map_err(anyhow::Error::msg)
    .context("invalid focused network boot configuration")?;
    let network_boot = match networking::NetworkBoot::boot(
        &network_config,
        networking::boot::NetworkBootStores::new(
            subnet_allocation,
            network_topology.clone(),
            pod_network_cache.clone(),
            pod_ipam,
            pod_runtime.clone(),
            assignment_publisher,
        ),
        shutdown_token.clone(),
        supervisor.clone(),
    )
    .await
    {
        Ok(boot) => boot,
        Err(err) => {
            network_cleanup.cleanup_runtime_network_best_effort().await;
            return Err(err.context("failed to boot network plane"));
        }
    };

    let boot_peering = network_boot.peer_router();
    {
        let mut applied = std::collections::HashMap::new();
        if let Err(e) = klights_controllers::node_subnet::sync_peer_routes_with_ports(
            network_topology.as_ref(),
            resource_query.as_ref(),
            &config.node_name,
            boot_peering.as_ref(),
            &mut applied,
        )
        .await
        {
            tracing::warn!("peer route setup failed: {}", e);
        }
    }

    let local_pod_subnet = network_boot.local_pod_subnet().to_string();
    let cluster_cidr = klights_types::ClusterCidr::parse(&config.cluster_cidr)
        .map_err(|e| anyhow::anyhow!("bad cluster_cidr '{}': {}", config.cluster_cidr, e))?;
    let service_cidr = klights_types::ClusterCidr::parse(&config.service_cidr)
        .map_err(|e| anyhow::anyhow!("bad service_cidr '{}': {}", config.service_cidr, e))?;

    let endpoint_adapter = Arc::new(klights_networking::StorePodEndpointResolver::new(
        pod_endpoints.clone(),
        pod_endpoint_events,
        network_topology.clone(),
    ));
    let endpoint_source: Arc<dyn klights_network_api::PodEndpointEventSource> =
        endpoint_adapter.clone();
    let resolver: Arc<dyn klights_network_api::PodEndpointResolver> = endpoint_adapter;

    if let networking::NetworkBoot::Rootless(plane) = &network_boot {
        plane
            .prepare_service_routing_bridge()
            .await
            .context("prepare rootless bridge before service-router sysctls")?;
    }

    let srm = klights_networking::service_routing::ServiceRoutingMode::new();
    let services: Arc<dyn klights_network_api::ServiceRouter> =
        klights_networking::service_routing::NftServiceRouter::boot_with_defaults(
            klights_networking::service_routing::NftServiceRouterDefaultBoot::new(
                klights_networking::service_routing::NftServiceRouterStores::new(
                    Arc::new(
                        crate::bootstrap::composition_adapters::networking_state_adapter::LeaderRoutingStateAdapter::new(
                            resource_query,
                        ),
                    ),
                    watch,
                    endpoint_source,
                ),
                klights_networking::service_routing::NftServiceRouterTableConfig::new(
                    &config.node_name,
                    &config.containerd_namespace,
                    &config.bridge_name,
                ),
                klights_networking::service_routing::NftServiceRouterNetworkConfig::new(
                    network_boot.local_pod_subnet(),
                    cluster_cidr,
                    service_cidr,
                    srm,
                ),
                shutdown_token.clone(),
                supervisor.clone(),
            ),
        )
        .await
        .context("klights service routing requires br_netfilter")?;

    let (datapath, peering): (
        Arc<dyn klights_network_api::Datapath>,
        Arc<dyn klights_network_api::PeerRouter>,
    ) = match (&network_boot, node_mode) {
        (networking::NetworkBoot::Root(p), _) => (p.clone(), network_boot.peer_router()),
        (networking::NetworkBoot::Rootless(p), _) => (p.clone(), network_boot.peer_router()),
    };

    let cni_datapath = datapath.clone();
    let network = Arc::new(networking::Network::new(
        datapath,
        peering,
        services.clone(),
        resolver,
    ));

    // CNI RPC
    let cni_rpc_token = CancellationToken::new();
    let cni_rpc_handle = {
        let state = Arc::new(klights_networking::cni_plugin::CniRpcState {
            socket_path: klights_networking::cni_plugin::CniSocketPath::try_new(
                crate::paths::cni_rpc_socket_path(&config.containerd_namespace)
                    .to_string_lossy()
                    .into_owned(),
            )?,
            socket_filesystem: crate::bootstrap::composition_adapters::cni_socket_adapter::RootCniSocketFilesystem::shared(
                klights_supervisor::FileProcessExecutor::new(supervisor.clone()),
            ),
            datapath: cni_datapath,
            task_supervisor: supervisor.clone(),
        });
        let cancel = cni_rpc_token.clone();
        supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "runtime_cni_rpc_server",
                async move {
                    if let Err(e) =
                        klights_networking::cni_plugin::run_rpc_server(state, cancel).await
                    {
                        tracing::warn!("CNI RPC error: {}", e);
                    }
                },
            )
            .await
            .context("failed to spawn CNI RPC")?
    };
    tracing::info!("CNI RPC server started");

    // Containerd
    let cri_transport_policy = klights_node_api::CriTransportPolicy::new(
        grpc_transport_policy.connect_timeout,
        grpc_transport_policy.max_message_bytes,
    );
    config
        .registry_proxy
        .verify_ready(&supervisor)
        .await
        .context("registry proxy preflight")?;
    let executable_path = std::env::current_exe().context("resolve klights executable path")?;
    let containerd_manager = if let Some(ref sock) = config.containerd_socket {
        tracing::info!("Using external containerd at {}", sock);
        None
    } else {
        let is_rootless = matches!(node_mode, NodeMode::Rootless { .. });
        let mgr = klights_kubelet::containerd_manager::ContainerdManager::start(
            klights_kubelet::containerd_manager::ContainerdStartConfig {
                namespace: &config.containerd_namespace,
                bridge_name: &config.bridge_name,
                pod_subnet: &local_pod_subnet,
                pod_link_mtu: networking::pod_link_mtu_for_encryption(config.dataplane_encryption),
                rootless: is_rootless,
                executable_path: &executable_path,
                image_pull_response_timeout: runtime_inputs.image_pull_response_timeout,
                cri_request_timeout: runtime_inputs.cri_request_timeout,
                paths: runtime_paths,
                task_supervisor: supervisor.clone(),
                cri_transport_policy,
                registry_proxy: &config.registry_proxy,
            },
        )
        .await
        .context("failed to start containerd")?;
        tracing::info!("Started containerd at {}", mgr.socket_path());
        Some(mgr)
    };

    let socket = if let Some(ref mgr) = containerd_manager {
        mgr.socket_path()
    } else if let Some(ref s) = config.containerd_socket {
        s.as_str()
    } else {
        unreachable!("containerd socket required")
    };

    let cri_for_pod_watcher = match klights_kubelet::cri::CriClient::connect_with_policy(
        socket,
        &config.containerd_namespace,
        &cri_transport_policy,
        runtime_inputs.image_pull_response_timeout,
        runtime_inputs.cri_request_timeout,
        supervisor.as_ref().clone(),
    )
    .await
    {
        Ok(client) => client,
        Err(err) => {
            cni_readiness_publisher.publish_failed("CRI connection unavailable after network boot");
            return Err(err).context("CRI connection unavailable after network boot");
        }
    };
    let cri_for_api = match klights_kubelet::cri::CriClient::connect_with_policy(
        socket,
        &config.containerd_namespace,
        &cri_transport_policy,
        runtime_inputs.image_pull_response_timeout,
        runtime_inputs.cri_request_timeout,
        supervisor.as_ref().clone(),
    )
    .await
    {
        Ok(client) => {
            tracing::info!("Connected to containerd (2 connections)");
            Some(Arc::new(tokio::sync::Mutex::new(client)))
        }
        Err(err) => {
            tracing::warn!("Second CRI connect failed: {}", err);
            None
        }
    };

    cni_readiness_publisher.publish_ready();
    tracing::info!("CNI readiness published after network boot and CRI connection");

    Ok(NetworkPhase {
        network,
        services,
        _local_pod_subnet: local_pod_subnet,
        cni_rpc_token,
        cni_rpc_handle,
        _containerd_manager: containerd_manager,
        cri_for_pod_watcher: Some(cri_for_pod_watcher),
        cri_for_api,
        cni_readiness,
        dataplane_health: network_boot.health().clone(),
        pod_network_cache,
        pod_runtime_store: pod_runtime,
        pod_endpoint_store: pod_endpoints,
        assignment_waiter,
    })
}

#[cfg(test)]
mod assignment_composition_tests {
    use klights_network_api::PodNetworkAssignmentKey;

    #[tokio::test]
    async fn publisher_and_waiter_are_views_of_the_same_instance_bus() {
        let (publisher, waiter) = super::assignment_bus_views();
        let key =
            PodNetworkAssignmentKey::try_new("sandbox-a", "default", "pod-a", "uid-a").unwrap();
        let mut subscription = waiter.subscribe(key.clone()).unwrap();

        publisher.publish_assignment(&key);

        subscription.wait().await.unwrap();
    }
}
