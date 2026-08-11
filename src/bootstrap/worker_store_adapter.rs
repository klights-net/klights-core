use anyhow::Context;

pub(crate) async fn start_worker_store_adapter(
    remote_api_client: std::sync::Arc<klights_leader_rpc::client::RemoteApiClient>,
    node_name: String,
    supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    shutdown_token: tokio_util::sync::CancellationToken,
    discovery_client: Option<std::sync::Arc<klights_leader_rpc::client::ReplicationGrpcClient>>,
    initial_leader_endpoints: Vec<String>,
) -> anyhow::Result<std::sync::Arc<klights_kubelet::worker_store::WorkerStoreAdapter>> {
    remote_api_client
        .start_required_worker_informers(shutdown_token.clone())
        .await
        .context("worker informers")?;

    let worker_store = std::sync::Arc::new(
        klights_kubelet::worker_store::WorkerStoreAdapter::from_focused_ports(
            klights_kubelet::worker_store::WorkerStorePorts {
                resource_query: remote_api_client.clone(),
                leader_watch: remote_api_client.clone(),
                subnet_allocation: remote_api_client.clone(),
                network_topology: remote_api_client.clone(),
                cleanup_intents: remote_api_client,
                watch_events: std::sync::Arc::new(
                    klights_kubelet::worker_store::WorkerWatchBus::new(),
                ),
            },
            node_name,
        ),
    );
    let discovery_rx = worker_store.watch_signals(klights_watch::WatchTopic::new("v1", "Node"));
    worker_store
        .start_watch_mirrors(supervisor.clone(), shutdown_token.clone())
        .await
        .context("worker watch mirrors")?;

    if let Some(discovery_client) = discovery_client {
        use klights_leader_api::{
            ControlplaneDiscoveryEvent, WatchEventType, extract_controlplane_endpoint,
        };
        use std::collections::HashMap;
        let mut discovery_rx = discovery_rx;
        let discovery_store = worker_store.clone();
        let cancel = shutdown_token.clone();
        supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "controlplane_endpoint_discovery",
                async move {
                    loop {
                        match discovery_rx.recv().await {
                            Ok(_) => {}
                            Err(klights_watch::WatchSignalReceiveError::Lagged(_)) => continue,
                            Err(_) => {
                                if cancel.is_cancelled() {
                                    return;
                                }
                                continue;
                            }
                        };
                        let nodes = match discovery_store
                            .list_resources(
                                "v1",
                                "Node",
                                None,
                                None,
                                None,
                                klights_kubelet::worker_store::WorkerListPage::unbounded(),
                            )
                            .await
                        {
                            Ok(nodes) => nodes,
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    "controlplane endpoint discovery Node relist failed"
                                );
                                continue;
                            }
                        };
                        let mut next_discovered: HashMap<String, String> = HashMap::new();
                        let mut leader_endpoint = None;
                        for node in nodes.items {
                            match extract_controlplane_endpoint(
                                WatchEventType::Added,
                                node.data.as_ref(),
                                klights_network_api::GRPC_PORT_ANNOTATION,
                                crate::bootstrap::config::DEFAULT_TLS_PORT,
                            ) {
                                ControlplaneDiscoveryEvent::Upsert {
                                    node_name,
                                    endpoint,
                                    is_leader,
                                } => {
                                    if is_leader {
                                        leader_endpoint = Some(endpoint.clone());
                                    }
                                    next_discovered.insert(node_name, endpoint);
                                }
                                ControlplaneDiscoveryEvent::Remove { .. }
                                | ControlplaneDiscoveryEvent::Ignore => {}
                            }
                        }
                        if let Some(endpoint) = leader_endpoint {
                            discovery_client.set_current_leader_endpoint(Some(endpoint));
                        }
                        let discovered = next_discovered;
                        let mut merged = initial_leader_endpoints.clone();
                        for ep in discovered.values() {
                            if !merged.contains(ep) {
                                merged.push(ep.clone());
                            }
                        }
                        discovery_client.set_all_leader_endpoints(merged);
                    }
                },
            )
            .await
            .context("controlplane endpoint discovery")?;
    }

    Ok(worker_store)
}
