//! Leader self ExternalIP discovery from peer-observed transport endpoints.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

use crate::bootstrap::NodeMode;
use crate::controllers::annotations::GRPC_PORT_ANNOTATION;
use crate::datastore::{DatastoreHandle, ResourceListQuery};
use klights_leader_api::JoinRole;
use klights_leader_rpc::client::{GrpcClientConfig, JoinDataplaneMetadata, ReplicationGrpcClient};
use klights_supervisor::{SupervisedJoinHandle, TaskCategory, TaskSupervisor};

#[derive(Clone, Debug, PartialEq, Eq)]
struct PeerEndpoint {
    node_name: String,
    endpoint: String,
}

pub(crate) struct LeaderPeerEndpointObserverDeps {
    query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    node_status: Arc<dyn klights_leader_api::LeaderNodeSelfStatus>,
    config: Arc<crate::KlightsConfig>,
    node_mode: NodeMode,
}

impl LeaderPeerEndpointObserverDeps {
    pub(crate) fn new(
        query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
        node_status: Arc<dyn klights_leader_api::LeaderNodeSelfStatus>,
        config: Arc<crate::KlightsConfig>,
        node_mode: NodeMode,
    ) -> Self {
        Self {
            query,
            node_status,
            config,
            node_mode,
        }
    }
}

pub(crate) async fn start_leader_peer_endpoint_observer(
    db: DatastoreHandle,
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
                    db,
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
    db: DatastoreHandle,
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
        db.as_ref(),
        &deps.config,
        &deps.node_mode,
        supervisor.as_ref(),
    )
    .await
    {
        return;
    }

    if let Err(err) = observe_from_existing_nodes(
        db.as_ref(),
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

    let mut signal_rx = crate::watch_commit_observation_adapter::subscribe(
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
                    db.as_ref(),
                    &deps.config,
                    &deps.node_mode,
                    supervisor.as_ref(),
                )
                .await
                {
                    return;
                }
                if let Err(err) = observe_from_existing_nodes(
                    db.as_ref(),
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
    db: &dyn crate::datastore::DatastoreBackend,
    deps: &LeaderPeerEndpointObserverDeps,
    client_identity: &ClientIdentity,
    supervisor: Arc<TaskSupervisor>,
    grpc_transport_policy: klights_leader_rpc::transport_policy::SharedGrpcTransportPolicy,
) -> Result<()> {
    let nodes = db
        .list_resources("v1", "Node", None, ResourceListQuery::all())
        .await?;
    for node in nodes.items {
        let Some(peer) =
            peer_endpoint_from_node(&node.data, &deps.config.node_name, deps.config.tls_port)
        else {
            continue;
        };
        if observe_from_peer(
            db,
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
    db: &dyn crate::datastore::DatastoreBackend,
    deps: &LeaderPeerEndpointObserverDeps,
    client_identity: &ClientIdentity,
    supervisor: Arc<TaskSupervisor>,
    grpc_transport_policy: klights_leader_rpc::transport_policy::SharedGrpcTransportPolicy,
    peer: PeerEndpoint,
) -> Result<bool> {
    let client = ReplicationGrpcClient::new(
        GrpcClientConfig {
            leader_endpoint: peer.endpoint.clone(),
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
        .with_context(|| format!("observe peer endpoint from {}", peer.node_name))?
    {
        let endpoint_ip = endpoint
            .parse::<std::net::IpAddr>()
            .with_context(|| format!("observed endpoint must be an IP address: {endpoint}"))?;
        let endpoint_ip = endpoint_ip.to_string();
        crate::kubelet::node::publish_node_external_ip_if_changed(
            deps.query.as_ref(),
            deps.node_status.as_ref(),
            &deps.config.node_name,
            &endpoint_ip,
        )
        .await?;
        // Now that we know our reachable endpoint, publish dataplane metadata so
        // peers can configure the WireGuard tunnel back to us.
        crate::bootstrap::init::dataplane::ensure_node_dataplane_published(
            db,
            &deps.config,
            &deps.node_mode,
            &endpoint_ip,
            supervisor.as_ref(),
        )
        .await?;
        return Ok(true);
    }
    Ok(false)
}

async fn local_node_external_ip(
    db: &dyn crate::datastore::DatastoreBackend,
    node_name: &str,
) -> Result<Option<String>> {
    let Some(node) = db.get_resource("v1", "Node", None, node_name).await? else {
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
    db: &dyn crate::datastore::DatastoreBackend,
    config: &crate::KlightsConfig,
    node_mode: &NodeMode,
    supervisor: &TaskSupervisor,
) -> bool {
    match local_node_external_ip(db, &config.node_name).await {
        Ok(Some(external_ip)) => {
            if let Err(err) = crate::bootstrap::init::dataplane::ensure_node_dataplane_published(
                db,
                config,
                node_mode,
                &external_ip,
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

fn peer_endpoint_from_node(
    node: &serde_json::Value,
    local_node_name: &str,
    default_port: u16,
) -> Option<PeerEndpoint> {
    if node.get("kind").and_then(|value| value.as_str()) != Some("Node") {
        return None;
    }
    let node_name = node
        .pointer("/metadata/name")
        .and_then(|value| value.as_str())?;
    if node_name == local_node_name {
        return None;
    }
    let external_ip = node_external_ip(node)?;
    let external_ip = external_ip.parse::<std::net::IpAddr>().ok()?;
    let port = node
        .pointer("/metadata/annotations")
        .and_then(|value| value.get(GRPC_PORT_ANNOTATION))
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(default_port);
    Some(PeerEndpoint {
        node_name: node_name.to_string(),
        endpoint: format!("https://{}:{port}", uri_host_for_ip(external_ip)),
    })
}

fn node_external_ip(node: &serde_json::Value) -> Option<&str> {
    node.pointer("/status/addresses")
        .and_then(|value| value.as_array())
        .and_then(|addresses| {
            addresses.iter().find_map(|address| {
                if address.get("type").and_then(|value| value.as_str()) == Some("ExternalIP") {
                    address.get("address").and_then(|value| value.as_str())
                } else {
                    None
                }
            })
        })
}

fn uri_host_for_ip(ip: std::net::IpAddr) -> String {
    match ip {
        std::net::IpAddr::V4(ip) => ip.to_string(),
        std::net::IpAddr::V6(ip) => format!("[{ip}]"),
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
    use crate::bootstrap::worker_identity::{
        CredentialSource, SupervisedFilesystemWorkerCredentialStore, resolve_credential_async,
    };

    let store = SupervisedFilesystemWorkerCredentialStore::for_namespace(
        namespace,
        node_name,
        supervisor.clone(),
    );
    let crypto = klights_supervisor::CryptoExecutor::new(supervisor);
    match resolve_credential_async(&store, &crypto).await? {
        CredentialSource::ExistingCert(cred) => Ok(ClientIdentity {
            client_cert_pem: cred.certificate_pem,
            client_key_pem: cred.private_key_pem,
        }),
        CredentialSource::BootstrapRequired => {
            anyhow::bail!("local node client certificate is required for peer endpoint observation")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn peer_endpoint_from_node_uses_external_ip_only() {
        let node = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "addresses": [
                    {"type": "InternalIP", "address": "172.31.11.2"},
                    {"type": "ExternalIP", "address": "10.99.0.11"}
                ]
            }
        });

        assert_eq!(
            peer_endpoint_from_node(&node, "leader-a", 7679),
            Some(PeerEndpoint {
                node_name: "worker-a".to_string(),
                endpoint: "https://10.99.0.11:7679".to_string(),
            })
        );
    }

    #[test]
    fn peer_endpoint_from_node_ignores_internal_ip_only_peer() {
        let node = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {
                "addresses": [
                    {"type": "InternalIP", "address": "172.31.11.2"}
                ]
            }
        });

        assert_eq!(peer_endpoint_from_node(&node, "leader-a", 7679), None);
    }

    #[tokio::test]
    async fn ensure_published_self_heals_when_local_node_has_external_ip() {
        let db = crate::datastore::test_support::in_memory().await;
        let mut config = crate::KlightsConfig::test_default();
        config.node_name = "leader-a".to_string();
        config.dataplane_encryption = crate::networking::wireguard::DataplaneEncryption::Disabled;
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
            &db,
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
        let db = crate::datastore::test_support::in_memory().await;
        let mut config = crate::KlightsConfig::test_default();
        config.node_name = "leader-a".to_string();
        config.dataplane_encryption = crate::networking::wireguard::DataplaneEncryption::Disabled;
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
            &db,
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

    #[test]
    fn peer_endpoint_from_node_ignores_local_node() {
        let node = json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "leader-a"},
            "status": {
                "addresses": [
                    {"type": "ExternalIP", "address": "10.99.0.10"}
                ]
            }
        });

        assert_eq!(peer_endpoint_from_node(&node, "leader-a", 7679), None);
    }
}
