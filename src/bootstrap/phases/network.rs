//! Phase 6: Network boot, nftables, CNI, containerd, and CRI.
//! One function returns all handles needed downstream.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

use crate::KlightsConfig;
use crate::bootstrap::NodeMode;
use crate::networking::{self, NetworkCleanup};
use klights_supervisor::{SupervisedJoinHandle, TaskSupervisor};

pub struct NetworkPhase {
    pub network: Arc<networking::Network>,
    pub services: Arc<dyn klights_network_api::ServiceRouter>,
    pub _local_pod_subnet: String,
    pub cni_rpc_token: CancellationToken,
    pub cni_rpc_handle: SupervisedJoinHandle<()>,
    pub _containerd_manager: Option<crate::kubelet::ContainerdManager>,
    pub cri_for_pod_watcher: Option<crate::kubelet::CriClient>,
    pub cri_for_api: Option<Arc<tokio::sync::Mutex<crate::kubelet::CriClient>>>,
    pub cni_readiness: crate::kubelet::cni_readiness::CniReadiness,
    pub dataplane_health: networking::dataplane_health::DataplaneHealth,
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
    pub node_local: crate::datastore::node_local::handle::NodeLocalHandle,
    pub network_cleanup: &'a NetworkCleanup,
    pub runtime_paths: &'a crate::kubelet::runtime_paths::KubeletRuntimePaths,
    pub runtime_inputs: crate::bootstrap::runtime_inputs::NetworkRuntimeInputs,
    pub supervisor: Arc<TaskSupervisor>,
    pub grpc_transport_policy:
        crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy,
    pub shutdown_token: CancellationToken,
}

fn assignment_bus_views() -> (
    Arc<dyn klights_network_api::PodNetworkAssignmentPublisher>,
    Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
) {
    let bus = Arc::new(crate::networking::pod_network_events::PodNetworkAssignmentBus::new());
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
        node_local,
        network_cleanup,
        runtime_paths,
        runtime_inputs,
        supervisor,
        grpc_transport_policy,
        shutdown_token,
    } = args;
    let (cni_readiness_publisher, cni_readiness) =
        crate::kubelet::cni_readiness::CniReadiness::channel();
    let node_network = crate::datastore::node_local::network_adapter::NodeLocalNetworkAdapter::new(
        node_local.clone(),
    );
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
            node_network.clone(),
            node_network.clone(),
            node_network.clone(),
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

    let boot_peering: Arc<dyn klights_network_api::PeerRouter> = match &network_boot {
        networking::NetworkBoot::Root(p) => p.clone(),
        networking::NetworkBoot::Rootless(p) => p.clone(),
    };
    {
        let mut applied = std::collections::HashMap::new();
        if let Err(e) = crate::controllers::node_subnet::sync_peer_routes_with_ports(
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

    let endpoint_adapter = Arc::new(networking::SqlitePodEndpointResolver::new(
        node_network.clone(),
        node_network.clone(),
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

    let srm = networking::service_routing::ServiceRoutingMode::new();
    let services: Arc<dyn klights_network_api::ServiceRouter> =
        networking::service_routing::NftServiceRouter::boot_with_defaults(
            networking::service_routing::NftServiceRouterDefaultBoot::new(
                networking::service_routing::NftServiceRouterStores::new(
                    resource_query,
                    watch,
                    endpoint_source,
                ),
                networking::service_routing::NftServiceRouterTableConfig::new(
                    &config.node_name,
                    &config.containerd_namespace,
                    &config.bridge_name,
                ),
                networking::service_routing::NftServiceRouterNetworkConfig::new(
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
        (networking::NetworkBoot::Root(p), _) => (p.clone(), p.clone()),
        (networking::NetworkBoot::Rootless(p), _) => (p.clone(), p.clone()),
    };

    let network = Arc::new(networking::Network::new(
        datapath,
        peering,
        services.clone(),
        resolver,
    ));

    // CNI RPC
    let cni_rpc_token = CancellationToken::new();
    let cni_rpc_handle = {
        let state = Arc::new(crate::cni_plugin::CniRpcState {
            containerd_namespace: config.containerd_namespace.clone(),
            network: network.clone(),
            task_supervisor: supervisor.clone(),
        });
        let cancel = cni_rpc_token.clone();
        supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "runtime_cni_rpc_server",
                async move {
                    if let Err(e) = crate::cni_plugin::run_rpc_server(state, cancel).await {
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
    let executable_path = std::env::current_exe().context("resolve klights executable path")?;
    let containerd_manager = if let Some(ref sock) = config.containerd_socket {
        tracing::info!("Using external containerd at {}", sock);
        None
    } else {
        let is_rootless = matches!(node_mode, NodeMode::Rootless { .. });
        let mgr = crate::kubelet::ContainerdManager::start(
            crate::kubelet::containerd_manager::ContainerdStartConfig {
                namespace: &config.containerd_namespace,
                bridge_name: &config.bridge_name,
                pod_subnet: &local_pod_subnet,
                pod_link_mtu: networking::pod_link_mtu_for_encryption(config.dataplane_encryption),
                rootless: is_rootless,
                executable_path: &executable_path,
                image_pull_response_timeout: runtime_inputs.image_pull_response_timeout,
                paths: runtime_paths,
                task_supervisor: supervisor.clone(),
                cri_transport_policy,
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

    let cri_for_pod_watcher = match crate::kubelet::CriClient::connect_with_policy(
        socket,
        &config.containerd_namespace,
        &cri_transport_policy,
        runtime_inputs.image_pull_response_timeout,
    )
    .await
    {
        Ok(client) => client,
        Err(err) => {
            cni_readiness_publisher.publish_failed("CRI connection unavailable after network boot");
            return Err(err).context("CRI connection unavailable after network boot");
        }
    };
    let cri_for_api = match crate::kubelet::CriClient::connect_with_policy(
        socket,
        &config.containerd_namespace,
        &cri_transport_policy,
        runtime_inputs.image_pull_response_timeout,
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
        pod_network_cache: node_network.clone(),
        pod_runtime_store: node_network.clone(),
        pod_endpoint_store: node_network,
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
