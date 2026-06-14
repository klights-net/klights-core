//! Phase 6: Network boot, nftables, CNI, containerd, and CRI.
//! One function returns all handles needed downstream.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

use crate::KlightsConfig;
use crate::bootstrap::NodeMode;
use crate::networking::{self, NetworkCleanup};
use crate::task_supervisor::{SupervisedJoinHandle, TaskSupervisor};

pub struct NetworkPhase {
    pub network: Arc<networking::Network>,
    pub services: Arc<dyn networking::ServiceRouter>,
    pub _local_pod_subnet: String,
    pub cni_rpc_token: CancellationToken,
    pub cni_rpc_handle: SupervisedJoinHandle<()>,
    pub _containerd_manager: Option<crate::kubelet::ContainerdManager>,
    pub cri_for_pod_watcher: Option<crate::kubelet::CriClient>,
    pub cri_for_api: Option<Arc<tokio::sync::Mutex<crate::kubelet::CriClient>>>,
    pub dataplane_health: networking::dataplane_health::DataplaneHealth,
}

pub struct NetworkBootArgs<'a> {
    pub config: &'a Arc<KlightsConfig>,
    pub node_mode: &'a NodeMode,
    pub node_ip: &'a str,
    pub cluster_api: Arc<dyn crate::control_plane::client::LeaderApiClient>,
    pub node_local: crate::datastore::node_local::handle::NodeLocalHandle,
    pub db: &'a dyn crate::datastore::DatastoreBackend,
    pub network_cleanup: &'a NetworkCleanup,
    pub containerd_data_dir: &'a str,
    pub containerd_state_dir: &'a str,
    pub supervisor: Arc<TaskSupervisor>,
    pub grpc_transport_policy:
        crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy,
    pub shutdown_token: CancellationToken,
}

pub async fn boot(args: NetworkBootArgs<'_>) -> Result<NetworkPhase> {
    let NetworkBootArgs {
        config,
        node_mode,
        node_ip,
        cluster_api,
        node_local,
        db,
        network_cleanup,
        containerd_data_dir,
        containerd_state_dir,
        supervisor,
        grpc_transport_policy,
        shutdown_token,
    } = args;
    let network_boot = match networking::NetworkBoot::boot(
        node_mode,
        config,
        cluster_api.clone(),
        node_local.clone(),
        node_ip,
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

    let boot_peering: Arc<dyn networking::PeerRouter> = match &network_boot {
        networking::NetworkBoot::Root(p) => p.clone(),
        networking::NetworkBoot::Rootless(p) => p.clone(),
    };
    {
        let mut applied = std::collections::HashMap::new();
        if let Err(e) = crate::controllers::node_subnet::sync_peer_routes(
            db,
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
    let cluster_cidr = networking::ClusterCidr::parse(&config.cluster_cidr)
        .map_err(|e| anyhow::anyhow!("bad cluster_cidr '{}': {}", config.cluster_cidr, e))?;
    let service_cidr = networking::ClusterCidr::parse(&config.service_cidr)
        .map_err(|e| anyhow::anyhow!("bad service_cidr '{}': {}", config.service_cidr, e))?;

    let srm = networking::service_routing::ServiceRoutingMode::new(
        node_mode.clone(),
        config.vxlan_device.clone(),
    );
    let services: Arc<dyn networking::ServiceRouter> =
        networking::service_routing::NftServiceRouter::boot_with_defaults(
            networking::service_routing::NftServiceRouterDefaultBoot::new(
                networking::service_routing::NftServiceRouterStores::new(
                    cluster_api.clone(),
                    node_local.clone(),
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

    let resolver: Arc<dyn networking::PodEndpointResolver> = Arc::new(
        networking::SqlitePodEndpointResolver::new(node_local.clone(), cluster_api),
    );

    let (datapath, peering): (
        Arc<dyn networking::Datapath>,
        Arc<dyn networking::PeerRouter>,
    ) = match (&network_boot, node_mode) {
        (networking::NetworkBoot::Root(p), _) => (p.clone(), p.clone()),
        (networking::NetworkBoot::Rootless(p), _) => (p.clone(), p.clone()),
    };

    let network = Arc::new(networking::Network {
        datapath,
        peering,
        services: services.clone(),
        resolver,
    });

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
                crate::task_supervisor::TaskCategory::Background,
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
                data_dir: containerd_data_dir,
                state_dir: containerd_state_dir,
                rootless: is_rootless,
                task_supervisor: supervisor.clone(),
                grpc_transport_policy: grpc_transport_policy.clone(),
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

    // CRI connect
    let (mut cri_for_pod_watcher, cri_for_api) = {
        let mut c1 = None;
        let mut c2 = None;
        for attempt in 1..=30 {
            match crate::kubelet::CriClient::connect_with_policy(
                socket,
                &config.containerd_namespace,
                grpc_transport_policy.as_ref(),
            )
            .await
            {
                Ok(c) => {
                    c1 = Some(c);
                    break;
                }
                Err(e) => {
                    if attempt == 30 {
                        tracing::warn!("CRI connect failed after 30 attempts: {}", e);
                    } else {
                        let _ = supervisor
                            .sleep("cri_retry", std::time::Duration::from_millis(200))
                            .await;
                    }
                }
            }
        }
        if let Some(c1v) = c1 {
            match crate::kubelet::CriClient::connect_with_policy(
                socket,
                &config.containerd_namespace,
                grpc_transport_policy.as_ref(),
            )
            .await
            {
                Ok(c2v) => {
                    tracing::info!("Connected to containerd (2 connections)");
                    c2 = Some(Arc::new(tokio::sync::Mutex::new(c2v)));
                    c1 = Some(c1v);
                }
                Err(e) => {
                    tracing::warn!("Second CRI connect failed: {}", e);
                    c1 = Some(c1v);
                }
            }
        }
        (c1, c2)
    };

    // CRI health check
    {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(60);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let ready = if let Some(ref mut c) = cri_for_pod_watcher {
                c.list_pod_sandboxes(None).await.is_ok()
            } else {
                true
            };
            if ready {
                tracing::info!(
                    "CRI health check passed ({} attempts, {:?})",
                    attempt,
                    start.elapsed()
                );
                break;
            }
            if start.elapsed() >= timeout {
                tracing::warn!("CRI health check timed out after {}s", timeout.as_secs());
                break;
            }
            if attempt == 1 {
                tracing::info!("Waiting for CRI...");
            }
            let _ = supervisor
                .sleep("cri_hc", std::time::Duration::from_secs(1))
                .await;
        }
    }

    Ok(NetworkPhase {
        network,
        services,
        _local_pod_subnet: local_pod_subnet,
        cni_rpc_token,
        cni_rpc_handle,
        _containerd_manager: containerd_manager,
        cri_for_pod_watcher,
        cri_for_api,
        dataplane_health: network_boot.health().clone(),
    })
}
