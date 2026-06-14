//! Leader self ExternalIP discovery from peer-observed transport endpoints.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

use crate::bootstrap::NodeMode;
use crate::controllers::annotations::GRPC_PORT_ANNOTATION;
use crate::datastore::{DatastoreHandle, ResourceListQuery};
use crate::replication::grpc::client::{
    GrpcClientConfig, JoinDataplaneMetadata, ReplicationGrpcClient,
};
use crate::replication::protocol::JoinRole;
use crate::task_supervisor::{SupervisedJoinHandle, TaskCategory, TaskSupervisor};
use crate::watch::{EventType, WatchEvent};

#[derive(Clone, Debug, PartialEq, Eq)]
struct PeerEndpoint {
    node_name: String,
    endpoint: String,
}

pub async fn start_leader_peer_endpoint_observer(
    db: DatastoreHandle,
    config: Arc<crate::KlightsConfig>,
    node_mode: NodeMode,
    supervisor: Arc<TaskSupervisor>,
    grpc_transport_policy: crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy,
    shutdown_token: CancellationToken,
) -> Result<SupervisedJoinHandle<()>> {
    let client_identity = load_local_node_client_identity(
        &config.containerd_namespace,
        &config.node_name,
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
                    config,
                    node_mode,
                    client_identity,
                    supervisor_for_task,
                    grpc_transport_policy,
                    shutdown_token,
                )
                .await;
            },
        )
        .await
}

async fn run_leader_peer_endpoint_observer(
    db: DatastoreHandle,
    config: Arc<crate::KlightsConfig>,
    node_mode: NodeMode,
    client_identity: ClientIdentity,
    supervisor: Arc<TaskSupervisor>,
    grpc_transport_policy: crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy,
    shutdown_token: CancellationToken,
) {
    // If the leader already has an ExternalIP (e.g. from registration or a
    // prior run), back-fill its dataplane metadata if missing and exit — there
    // is nothing left to observe. The previous early-return skipped the publish
    // entirely, leaving node_dataplane empty and the WireGuard tunnel unformed.
    if ensure_published_if_local_has_external_ip(
        db.as_ref(),
        &config,
        &node_mode,
        supervisor.as_ref(),
    )
    .await
    {
        return;
    }

    if let Err(err) = observe_from_existing_nodes(
        db.as_ref(),
        &config,
        &node_mode,
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

    let mut watch_rx = db.subscribe_watch(crate::watch::WatchTopic::new("v1", "Node"));
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => return,
            event = watch_rx.recv() => {
                let event = match event {
                    Ok(event) => event,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                };
                if ensure_published_if_local_has_external_ip(
                    db.as_ref(),
                    &config,
                    &node_mode,
                    supervisor.as_ref(),
                )
                .await
                {
                    return;
                }
                let Some(peer) = peer_endpoint_from_watch_event(&event, &config.node_name, config.tls_port) else {
                    continue;
                };
                if let Err(err) = observe_from_peer(
                    db.as_ref(),
                    &config,
                    &node_mode,
                    &client_identity,
                    supervisor.clone(),
                    grpc_transport_policy.clone(),
                    peer,
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
    config: &crate::KlightsConfig,
    node_mode: &NodeMode,
    client_identity: &ClientIdentity,
    supervisor: Arc<TaskSupervisor>,
    grpc_transport_policy: crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy,
) -> Result<()> {
    let nodes = db
        .list_resources("v1", "Node", None, ResourceListQuery::all())
        .await?;
    for node in nodes.items {
        let Some(peer) = peer_endpoint_from_node(&node.data, &config.node_name, config.tls_port)
        else {
            continue;
        };
        observe_from_peer(
            db,
            config,
            node_mode,
            client_identity,
            supervisor.clone(),
            grpc_transport_policy.clone(),
            peer,
        )
        .await?;
        if local_node_external_ip(db, &config.node_name)
            .await?
            .is_some()
        {
            return Ok(());
        }
    }
    Ok(())
}

async fn observe_from_peer(
    db: &dyn crate::datastore::DatastoreBackend,
    config: &crate::KlightsConfig,
    node_mode: &NodeMode,
    client_identity: &ClientIdentity,
    supervisor: Arc<TaskSupervisor>,
    grpc_transport_policy: crate::replication::grpc::transport_policy::SharedGrpcTransportPolicy,
    peer: PeerEndpoint,
) -> Result<()> {
    let client = ReplicationGrpcClient::new(
        GrpcClientConfig {
            leader_endpoint: peer.endpoint.clone(),
            token: String::new(),
            node_name: config.node_name.clone(),
            role: JoinRole::Worker,
            dataplane: placeholder_dataplane(node_mode),
            ca_cert_path: Some(crate::paths::ca_cert_path(&config.containerd_namespace)),
            skip_ca: false,
            client_cert_pem: Some(client_identity.client_cert_pem.clone()),
            client_key_pem: Some(client_identity.client_key_pem.clone()),
        },
        supervisor.clone(),
        grpc_transport_policy,
    );
    if let Some(endpoint) = client
        .observe_peer_endpoint_rpc(&config.node_name)
        .await
        .with_context(|| format!("observe peer endpoint from {}", peer.node_name))?
    {
        let endpoint_ip = endpoint
            .parse::<std::net::IpAddr>()
            .with_context(|| format!("observed endpoint must be an IP address: {endpoint}"))?;
        let endpoint_ip = endpoint_ip.to_string();
        crate::kubelet::node::update_existing_node_external_ip_if_changed(
            db,
            &config.node_name,
            &endpoint_ip,
        )
        .await?;
        // Now that we know our reachable endpoint, publish dataplane metadata so
        // peers can configure the WireGuard tunnel back to us.
        crate::bootstrap::init::dataplane::ensure_node_dataplane_published(
            db,
            config,
            node_mode,
            &endpoint_ip,
            supervisor.as_ref(),
        )
        .await?;
    }
    Ok(())
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
        NodeMode::Root => crate::networking::wireguard::DataplaneMode::Root,
        NodeMode::Rootless { .. } => crate::networking::wireguard::DataplaneMode::Rootless,
    };
    JoinDataplaneMetadata {
        public_key: None,
        endpoint: String::new(),
        port: None,
        mode,
        encryption: crate::networking::wireguard::DataplaneEncryption::Disabled,
    }
}

fn peer_endpoint_from_watch_event(
    event: &WatchEvent,
    local_node_name: &str,
    default_port: u16,
) -> Option<PeerEndpoint> {
    match event.event_type {
        EventType::Added | EventType::Modified => {
            peer_endpoint_from_node(&event.object, local_node_name, default_port)
        }
        EventType::Deleted | EventType::Bookmark | EventType::Error => None,
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

    let store =
        SupervisedFilesystemWorkerCredentialStore::for_namespace(namespace, node_name, supervisor);
    match resolve_credential_async(&store).await? {
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
        let supervisor = TaskSupervisor::new(crate::task_supervisor::TaskCategoryConfig::default());

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
        let supervisor = TaskSupervisor::new(crate::task_supervisor::TaskCategoryConfig::default());

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
