//! Leader self ExternalIP discovery from peer-observed transport endpoints.

use std::sync::Arc;

use anyhow::{Context, Result};
use klights_cluster_store::{
    ClusterResourceRead, ResourceCollectionScope, ResourceGetRequest, ResourceListQuery,
    ResourceListRead, ResourceListRequest,
};
use tokio_util::sync::CancellationToken;

use crate::bootstrap::NodeMode;
use klights_leader_api::{JoinRole, PeerEndpoint, node_external_ip, peer_endpoint_from_node};
use klights_leader_rpc::client::{GrpcClientConfig, JoinDataplaneMetadata, ReplicationGrpcClient};
use klights_network_api::GRPC_PORT_ANNOTATION;
use klights_supervisor::{SupervisedJoinHandle, TaskCategory, TaskSupervisor};

pub(crate) struct LeaderPeerEndpointObserverDeps {
    query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    node_status: Arc<dyn klights_leader_api::LeaderNodeSelfStatus>,
    network_command: Arc<dyn klights_leader_api::LeaderNetworkTopologyCommand>,
    config: Arc<crate::KlightsConfig>,
    node_mode: NodeMode,
}

impl LeaderPeerEndpointObserverDeps {
    pub(crate) fn new(
        query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
        node_status: Arc<dyn klights_leader_api::LeaderNodeSelfStatus>,
        network_command: Arc<dyn klights_leader_api::LeaderNetworkTopologyCommand>,
        config: Arc<crate::KlightsConfig>,
        node_mode: NodeMode,
    ) -> Self {
        Self {
            query,
            node_status,
            network_command,
            config,
            node_mode,
        }
    }
}

pub(crate) async fn start_leader_peer_endpoint_observer(
    resource_reads: Arc<dyn ClusterResourceRead>,
    watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    deps: LeaderPeerEndpointObserverDeps,
    supervisor: Arc<TaskSupervisor>,
    grpc_transport_policy: klights_leader_rpc::transport_policy::SharedGrpcTransportPolicy,
    shutdown_token: CancellationToken,
) -> Result<SupervisedJoinHandle<()>> {
    let client_identity = load_local_node_client_identity(
        &deps.config.containerd_namespace,
        &deps.config.node_name,
        supervisor.clone(),
    )
    .await?;
    let supervisor_for_task = supervisor.clone();
    supervisor
        .spawn_async(
            TaskCategory::Background,
            "leader_peer_observed_endpoint_watcher",
            async move {
                run_leader_peer_endpoint_observer(
                    resource_reads,
                    watch_signals,
                    deps,
                    client_identity,
                    supervisor_for_task,
                    grpc_transport_policy,
                    shutdown_token,
                )
                .await;
            },
        )
        .await
        .map_err(Into::into)
}

async fn run_leader_peer_endpoint_observer(
    resource_reads: Arc<dyn ClusterResourceRead>,
    watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
    deps: LeaderPeerEndpointObserverDeps,
    client_identity: ClientIdentity,
    supervisor: Arc<TaskSupervisor>,
    grpc_transport_policy: klights_leader_rpc::transport_policy::SharedGrpcTransportPolicy,
    shutdown_token: CancellationToken,
) {
    // If the leader already has an ExternalIP (e.g. from registration or a
    // prior run), back-fill its dataplane metadata if missing and exit — there
    // is nothing left to observe. The previous early-return skipped the publish
    // entirely, leaving node_dataplane empty and the WireGuard tunnel unformed.
    if ensure_published_if_local_has_external_ip(
        resource_reads.as_ref(),
        deps.network_command.as_ref(),
        &deps.config,
        &deps.node_mode,
        supervisor.as_ref(),
    )
    .await
    {
        return;
    }

    if let Err(err) = observe_from_existing_nodes(
        resource_reads.as_ref(),
        &deps,
        &client_identity,
        supervisor.clone(),
        grpc_transport_policy.clone(),
    )
    .await
    {
        tracing::warn!(
            error = %err,
            "leader peer observed endpoint initial scan failed"
        );
    }

    let mut signal_rx = crate::bootstrap::watch_commit_wiring::subscribe(
        watch_signals.as_ref(),
        klights_watch::WatchTopic::new("v1", "Node"),
    );
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => return,
            signal = signal_rx.recv() => {
                match signal {
                    Ok(_) => {}
                    Err(klights_watch::WatchSignalReceiveError::Lagged(_)) => {}
                    Err(klights_watch::WatchSignalReceiveError::Closed) => return,
                }
                if ensure_published_if_local_has_external_ip(
                    resource_reads.as_ref(),
                    deps.network_command.as_ref(),
                    &deps.config,
                    &deps.node_mode,
                    supervisor.as_ref(),
                )
                .await
                {
                    return;
                }
                if let Err(err) = observe_from_existing_nodes(
                    resource_reads.as_ref(),
                    &deps,
                    &client_identity,
                    supervisor.clone(),
                    grpc_transport_policy.clone(),
                )
                .await
                {
                    tracing::warn!(
                        error = %err,
                        "leader peer observed endpoint probe failed"
                    );
                }
            }
        }
    }
}

async fn observe_from_existing_nodes(
    resource_reads: &dyn ClusterResourceRead,
    deps: &LeaderPeerEndpointObserverDeps,
    client_identity: &ClientIdentity,
    supervisor: Arc<TaskSupervisor>,
    grpc_transport_policy: klights_leader_rpc::transport_policy::SharedGrpcTransportPolicy,
) -> Result<()> {
    let nodes = match resource_reads
        .list_resources(ResourceListRequest::new(
            "v1",
            "Node",
            ResourceCollectionScope::Cluster,
            ResourceListQuery::all(),
        ))
        .await?
    {
        ResourceListRead::Current(page) | ResourceListRead::Historical(page) => page.into_items(),
        ResourceListRead::Expired { .. } => Vec::new(),
    };
    for node in nodes {
        let Some(peer) = peer_endpoint_from_node(
            &node.data,
            &deps.config.node_name,
            GRPC_PORT_ANNOTATION,
            deps.config.tls_port,
        ) else {
            continue;
        };
        if observe_from_peer(
            resource_reads,
            deps,
            client_identity,
            supervisor.clone(),
            grpc_transport_policy.clone(),
            peer,
        )
        .await?
        {
            return Ok(());
        }
    }
    Ok(())
}

async fn observe_from_peer(
    resource_reads: &dyn ClusterResourceRead,
    deps: &LeaderPeerEndpointObserverDeps,
    client_identity: &ClientIdentity,
    supervisor: Arc<TaskSupervisor>,
    grpc_transport_policy: klights_leader_rpc::transport_policy::SharedGrpcTransportPolicy,
    peer: PeerEndpoint,
) -> Result<bool> {
    let client = ReplicationGrpcClient::new(
        GrpcClientConfig {
            leader_endpoint: peer.endpoint().to_owned(),
            token: String::new(),
            node_name: deps.config.node_name.clone(),
            role: JoinRole::Worker,
            dataplane: placeholder_dataplane(&deps.node_mode),
            ca_cert_path: Some(crate::paths::ca_cert_path(
                &deps.config.containerd_namespace,
            )),
            skip_ca: false,
            client_cert_pem: Some(client_identity.client_cert_pem.clone()),
            client_key_pem: Some(client_identity.client_key_pem.clone()),
        },
        supervisor.clone(),
        grpc_transport_policy,
    );
    if let Some(endpoint) = client
        .observe_peer_endpoint_rpc(&deps.config.node_name)
        .await
        .with_context(|| format!("observe peer endpoint from {}", peer.node_name()))?
    {
        let endpoint_ip = endpoint
            .parse::<std::net::IpAddr>()
            .with_context(|| format!("observed endpoint must be an IP address: {endpoint}"))?;
        let endpoint_ip = endpoint_ip.to_string();
        klights_kubelet::node::publish_node_external_ip_if_changed(
            deps.query.as_ref(),
            deps.node_status.as_ref(),
            &deps.config.node_name,
            &endpoint_ip,
        )
        .await?;
        // Now that we know our reachable endpoint, publish dataplane metadata so
        // peers can configure the WireGuard tunnel back to us.
        crate::bootstrap::init::dataplane::publish_local_dataplane_metadata_self_heal_with_resource_reads(
            resource_reads,
            deps.network_command.as_ref(),
            &deps.config,
            &deps.node_mode,
            supervisor.as_ref(),
        )
        .await?;
        return Ok(true);
    }
    Ok(false)
}

async fn local_node_external_ip(
    resource_reads: &dyn ClusterResourceRead,
    node_name: &str,
) -> Result<Option<String>> {
    let Some(node) = resource_reads
        .get_resource(ResourceGetRequest::new("v1", "Node", None, node_name))
        .await?
    else {
        return Ok(None);
    };
    Ok(node_external_ip(&node.data).map(str::to_string))
}

/// When the local node already has an `ExternalIP`, ensure its dataplane
/// metadata row exists (publishing it from that IP if missing) and report
/// `true` so the observer can stop — there is no endpoint left to discover.
/// Returns `false` when no ExternalIP is present yet, signalling the caller to
/// keep observing peers.
async fn ensure_published_if_local_has_external_ip(
    resource_reads: &dyn ClusterResourceRead,
    network_command: &dyn klights_leader_api::LeaderNetworkTopologyCommand,
    config: &crate::KlightsConfig,
    node_mode: &NodeMode,
    supervisor: &TaskSupervisor,
) -> bool {
    match local_node_external_ip(resource_reads, &config.node_name).await {
        Ok(Some(_external_ip)) => {
            if let Err(err) = crate::bootstrap::init::dataplane::publish_local_dataplane_metadata_self_heal_with_resource_reads(
                resource_reads,
                network_command,
                config,
                node_mode,
                supervisor,
            )
            .await
            {
                tracing::warn!(
                    error = %err,
                    "leader self-heal dataplane publish from registered ExternalIP failed"
                );
            }
            true
        }
        Ok(None) => false,
        Err(err) => {
            tracing::warn!(error = %err, "leader external IP lookup failed");
            false
        }
    }
}

fn placeholder_dataplane(node_mode: &NodeMode) -> JoinDataplaneMetadata {
    let mode = match node_mode {
        NodeMode::Root => klights_leader_api::NetworkNodeMode::Root,
        NodeMode::Rootless { .. } => klights_leader_api::NetworkNodeMode::Rootless,
    };
    JoinDataplaneMetadata {
        public_key: None,
        endpoint: String::new(),
        port: None,
        mode,
        encryption: klights_leader_api::DataplaneEncryption::Direct,
    }
}

#[derive(Clone)]
struct ClientIdentity {
    client_cert_pem: String,
    client_key_pem: String,
}

async fn load_local_node_client_identity(
    namespace: &str,
    node_name: &str,
    supervisor: Arc<TaskSupervisor>,
) -> Result<ClientIdentity> {
    use crate::bootstrap::composition_adapters::worker_credential_store_adapter::SupervisedFilesystemWorkerCredentialStore;
    use klights_auth::worker_credential::{WorkerCredentialSource, resolve_worker_credential};

    let store = SupervisedFilesystemWorkerCredentialStore::for_namespace(
        namespace,
        node_name,
        supervisor.clone(),
    );
    let crypto = klights_supervisor::CryptoExecutor::new(supervisor);
    let credential_now = klights_auth::clock::Clock::now(&klights_auth::clock::SystemClock);
    match resolve_worker_credential(&store, &crypto, credential_now).await? {
        WorkerCredentialSource::Existing(cred) => {
            let (client_cert_pem, client_key_pem) = cred.into_tls_parts();
            Ok(ClientIdentity {
                client_cert_pem,
                client_key_pem,
            })
        }
        WorkerCredentialSource::BootstrapRequired => {
            anyhow::bail!("local node client certificate is required for peer endpoint observation")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_dataplane_command(
        db: &klights_cluster_datastore::sqlite::embedded::Datastore,
    ) -> crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderNetwork{
        let canonical = db.clone();
        crate::bootstrap::composition_adapters::leader_topology_cleanup_adapter::ClusterStoreLeaderNetwork::new(
            db.focused_read_store(),
            {
                Arc::new(crate::bootstrap::outbox_apply_adapter::BackendProposalFixture::new(Arc::new(canonical.clone()), Arc::new(canonical.clone()), canonical.focused_read_store()))
            },
            crate::bootstrap::composition_adapters::authority_adapter::always_leader_watch(),
        )
    }

    #[tokio::test]
    async fn ensure_published_self_heals_when_local_node_has_external_ip() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let mut config = crate::KlightsConfig::test_default();
        config.node_name = "leader-a".to_string();
        config.dataplane_encryption = klights_networking::wireguard::DataplaneEncryption::Disabled;
        let supervisor = TaskSupervisor::new(klights_supervisor::TaskCategoryConfig::default());

        db.create_resource(
            "v1",
            "Node",
            None,
            "leader-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "leader-a"},
                "status": {"addresses": [{"type": "ExternalIP", "address": "198.51.100.47"}]}
            }),
        )
        .await
        .unwrap();

        let done = super::ensure_published_if_local_has_external_ip(
            db.focused_read_store().as_ref(),
            &test_dataplane_command(&db),
            &config,
            &NodeMode::Root,
            &supervisor,
        )
        .await;
        assert!(
            done,
            "observer must stop once the local node has an ExternalIP"
        );

        let stored = db
            .get_node_dataplane("leader-a")
            .await
            .unwrap()
            .expect("self-heal must publish dataplane metadata");
        assert_eq!(stored.endpoint.to_string(), "198.51.100.47");
    }

    #[tokio::test]
    async fn ensure_published_keeps_observing_without_external_ip() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
        let mut config = crate::KlightsConfig::test_default();
        config.node_name = "leader-a".to_string();
        config.dataplane_encryption = klights_networking::wireguard::DataplaneEncryption::Disabled;
        let supervisor = TaskSupervisor::new(klights_supervisor::TaskCategoryConfig::default());

        db.create_resource(
            "v1",
            "Node",
            None,
            "leader-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "leader-a"},
                "status": {"addresses": [{"type": "InternalIP", "address": "10.174.0.3"}]}
            }),
        )
        .await
        .unwrap();

        let done = super::ensure_published_if_local_has_external_ip(
            db.focused_read_store().as_ref(),
            &test_dataplane_command(&db),
            &config,
            &NodeMode::Root,
            &supervisor,
        )
        .await;
        assert!(
            !done,
            "without an ExternalIP the observer must keep observing"
        );
        assert!(
            db.get_node_dataplane("leader-a").await.unwrap().is_none(),
            "no dataplane row should be published without an external endpoint"
        );
    }
}
