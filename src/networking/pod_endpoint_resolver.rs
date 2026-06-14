//! `PodEndpointResolver` — cross-mode pod reachability lookup.
//!
//! Cross-mode pod reachability is mediated by a `PodEndpointResolver`. The
//! SQLite-backed implementation can be shared by later dataplane consumers
//! without refactoring root-mode code.

use anyhow::Result;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use crate::control_plane::client::LeaderApiClient;
use crate::datastore::node_local::NodeLocalHandle;
use crate::datastore::{PodEndpointEvent, PodEndpointMode};

/// Resolved endpoint for a pod.
///
/// `Direct` — pod is reachable at its pod IP via the cluster overlay
/// (root mode default).
/// `HostPort` — pod is reachable via `(node_ip, host_port)` per protocol;
/// used in rootless / hybrid clusters where direct overlay reach is
/// unavailable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Endpoint {
    EncryptedDirect {
        pod_ip: Ipv4Addr,
        node_name: String,
    },
    UnencryptedDirect {
        pod_ip: Ipv4Addr,
        node_name: String,
    },
    Direct {
        pod_ip: Ipv4Addr,
    },
    HostPort {
        pod_ip: Ipv4Addr,
        node_name: String,
        node_ip: IpAddr,
        host_port: u16,
        protocol: Protocol,
    },
}

/// L4 protocol for the hostport resolution path. Distinct from
/// `service_routing::Protocol` so this trait surface stays inside the
/// networking root and pulls no service-routing internals.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    Tcp,
    Udp,
}

/// Watch event emitted by the resolver: either a new/updated endpoint
/// for a pod IP, or a deletion keyed by pod IP.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndpointEvent {
    Upsert(Endpoint),
    Delete(Ipv4Addr),
}

#[async_trait]
pub trait PodEndpointResolver: Send + Sync + 'static {
    /// Resolve a pod IP to an `Endpoint`. Returns `None` if no row exists.
    async fn resolve(&self, pod_ip: Ipv4Addr) -> Result<Option<Endpoint>>;

    /// Subscribe to live endpoint events. The returned stream is
    /// reachable across runtime boundaries (`'static`) so consumers can
    /// move it into spawned tasks.
    fn watch(&self) -> BoxStream<'static, EndpointEvent>;
}

/// SQLite-backed resolver — reads from the `pod_endpoints` table
/// (Task 1) and translates the `PodEndpointEvent` broadcast into the
/// trait's `EndpointEvent` shape.
pub struct SqlitePodEndpointResolver {
    node_local: NodeLocalHandle,
    cluster_api: Arc<dyn LeaderApiClient>,
}

impl SqlitePodEndpointResolver {
    pub fn new(node_local: NodeLocalHandle, cluster_api: Arc<dyn LeaderApiClient>) -> Self {
        Self {
            node_local,
            cluster_api,
        }
    }
}

#[async_trait]
impl PodEndpointResolver for SqlitePodEndpointResolver {
    async fn resolve(&self, pod_ip: Ipv4Addr) -> Result<Option<Endpoint>> {
        let Some(row) = self.node_local.get_endpoint_by_pod_ip(pod_ip).await? else {
            return Ok(None);
        };
        Ok(Some(match row.mode {
            PodEndpointMode::Vxlan => {
                let encryption = self
                    .cluster_api
                    .get_node_dataplane(&row.node_name)
                    .await?
                    .map(|metadata| metadata.encryption)
                    .unwrap_or(crate::networking::wireguard::DataplaneEncryption::Enabled);
                match encryption {
                    crate::networking::wireguard::DataplaneEncryption::Enabled => {
                        Endpoint::EncryptedDirect {
                            pod_ip: row.pod_ip,
                            node_name: row.node_name,
                        }
                    }
                    crate::networking::wireguard::DataplaneEncryption::Disabled => {
                        Endpoint::UnencryptedDirect {
                            pod_ip: row.pod_ip,
                            node_name: row.node_name,
                        }
                    }
                }
            }
            PodEndpointMode::Hostport => {
                // Phase 1 SqliteResolver picks the TCP host port if both
                // are present; Phase 2 callers that need protocol-specific
                // resolution will use a wider Endpoint surface.
                let host_port = row.host_port_tcp.or(row.host_port_udp).unwrap_or(0);
                let protocol = if row.host_port_tcp.is_some() {
                    Protocol::Tcp
                } else {
                    Protocol::Udp
                };
                Endpoint::HostPort {
                    pod_ip: row.pod_ip,
                    node_name: row.node_name,
                    node_ip: IpAddr::V4(row.node_ip),
                    host_port,
                    protocol,
                }
            }
        }))
    }

    fn watch(&self) -> BoxStream<'static, EndpointEvent> {
        let rx = self.node_local.subscribe_pod_endpoints();
        let node_local = self.node_local.clone();
        futures::stream::unfold(
            (rx, node_local, Vec::<EndpointEvent>::new()),
            |(mut rx, node_local, mut pending)| async move {
                loop {
                    // Drain any pending re-list events first
                    if let Some(evt) = pending.pop() {
                        return Some((evt, (rx, node_local, pending)));
                    }

                    match rx.recv().await {
                        Ok(event) => {
                            if let Some(evt) = translate_endpoint_event(&event) {
                                return Some((evt, (rx, node_local, pending)));
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(
                                lagged_count = n,
                                "pod_endpoint_resolver: broadcast Lagged; triggering full re-list"
                            );
                            match node_local.list_endpoints_all().await {
                                Ok(rows) => {
                                    pending = rows
                                        .into_iter()
                                        .filter_map(|row| {
                                            translate_endpoint_event(&PodEndpointEvent::Upsert(row))
                                        })
                                        .collect();
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        error = %e,
                                        "pod_endpoint_resolver: re-list after Lagged failed"
                                    );
                                }
                            }
                        }
                    }
                }
            },
        )
        .boxed()
    }
}

/// Translate a raw `PodEndpointEvent` into an `EndpointEvent` for the watch stream.
fn translate_endpoint_event(event: &PodEndpointEvent) -> Option<EndpointEvent> {
    match event {
        PodEndpointEvent::Upsert(row) => match row.mode {
            PodEndpointMode::Vxlan => Some(EndpointEvent::Upsert(Endpoint::EncryptedDirect {
                pod_ip: row.pod_ip,
                node_name: row.node_name.clone(),
            })),
            PodEndpointMode::Hostport => {
                let host_port = row.host_port_tcp.or(row.host_port_udp).unwrap_or(0);
                let protocol = if row.host_port_tcp.is_some() {
                    Protocol::Tcp
                } else {
                    Protocol::Udp
                };
                Some(EndpointEvent::Upsert(Endpoint::HostPort {
                    pod_ip: row.pod_ip,
                    node_name: row.node_name.clone(),
                    node_ip: IpAddr::V4(row.node_ip),
                    host_port,
                    protocol,
                }))
            }
        },
        PodEndpointEvent::Delete { pod_ip, .. } => Some(EndpointEvent::Delete(*pod_ip)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::client::local::LocalApiClient;
    use crate::datastore::PodEndpointRow;
    use crate::datastore::node_local::{NodeLocalHandle, selector};
    use crate::datastore::sqlite::Datastore;
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use std::sync::Arc;
    use tokio::time::Duration;

    fn sample_row(uid: &str, pod_ip: Ipv4Addr, mode: PodEndpointMode) -> PodEndpointRow {
        PodEndpointRow {
            pod_uid: uid.to_string(),
            namespace: "default".to_string(),
            pod_name: format!("pod-{uid}"),
            node_name: "node-a".to_string(),
            mode,
            pod_ip,
            node_ip: pod_ip,
            host_port_tcp: None,
            host_port_udp: None,
            generation: 1,
            updated_at: 1_700_000_000,
        }
    }

    async fn build_resolver() -> (NodeLocalHandle, Datastore, SqlitePodEndpointResolver) {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:pod-endpoint-resolver-test",
        )
        .await
        .expect("open node-local");
        let cluster_db = Datastore::new_in_memory().await.unwrap();
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "node-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let resolver = SqlitePodEndpointResolver::new(node_local.clone(), cluster_api);
        (node_local, cluster_db, resolver)
    }

    #[tokio::test]
    async fn test_resolver_returns_none_for_unknown_pod_ip() {
        let (_node_local, _cluster_db, resolver) = build_resolver().await;
        let result = resolver.resolve(Ipv4Addr::new(10, 0, 0, 99)).await.unwrap();
        assert!(result.is_none(), "unknown pod IP must resolve to None");
    }

    #[tokio::test]
    async fn test_resolver_returns_encrypted_direct_for_vxlan_mode_row_by_default() {
        let (node_local, _cluster_db, resolver) = build_resolver().await;
        let row = sample_row("uid-d", Ipv4Addr::new(10, 42, 1, 5), PodEndpointMode::Vxlan);
        node_local.upsert_endpoint(row).await.unwrap();
        let resolved = resolver
            .resolve(Ipv4Addr::new(10, 42, 1, 5))
            .await
            .unwrap()
            .expect("Vxlan row must resolve");
        match resolved {
            Endpoint::EncryptedDirect { pod_ip, node_name } => {
                assert_eq!(pod_ip, Ipv4Addr::new(10, 42, 1, 5));
                assert_eq!(node_name, "node-a");
            }
            other => panic!("expected EncryptedDirect, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_resolver_returns_unencrypted_direct_only_for_explicit_disabled_dataplane() {
        let (node_local, cluster_db, resolver) = build_resolver().await;
        let row = sample_row("uid-u", Ipv4Addr::new(10, 42, 1, 6), PodEndpointMode::Vxlan);
        node_local.upsert_endpoint(row).await.unwrap();
        cluster_db
            .update_node_dataplane(
                crate::networking::wireguard::DataplanePeerMetadata::try_new(
                    "node-a".to_string(),
                    crate::networking::wireguard::DataplaneMode::Rootless,
                    crate::networking::wireguard::DataplaneEncryption::Disabled,
                    None,
                    Some("192.0.2.10".to_string()),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let resolved = resolver
            .resolve(Ipv4Addr::new(10, 42, 1, 6))
            .await
            .unwrap()
            .expect("Vxlan row must resolve");
        match resolved {
            Endpoint::UnencryptedDirect { pod_ip, node_name } => {
                assert_eq!(pod_ip, Ipv4Addr::new(10, 42, 1, 6));
                assert_eq!(node_name, "node-a");
            }
            other => panic!("expected UnencryptedDirect, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_resolver_returns_hostport_for_hostport_mode_row() {
        let (node_local, _cluster_db, resolver) = build_resolver().await;
        let mut row = sample_row(
            "uid-hp",
            Ipv4Addr::new(10, 42, 9, 1),
            PodEndpointMode::Hostport,
        );
        row.host_port_tcp = Some(31000);
        row.node_ip = Ipv4Addr::new(192, 0, 2, 10);
        node_local.upsert_endpoint(row).await.unwrap();
        let resolved = resolver
            .resolve(Ipv4Addr::new(10, 42, 9, 1))
            .await
            .unwrap()
            .expect("Hostport row must resolve");
        match resolved {
            Endpoint::HostPort {
                pod_ip,
                node_name,
                node_ip,
                host_port,
                protocol,
            } => {
                assert_eq!(pod_ip, Ipv4Addr::new(10, 42, 9, 1));
                assert_eq!(node_name, "node-a");
                assert_eq!(node_ip, IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)));
                assert_eq!(host_port, 31000);
                assert_eq!(protocol, Protocol::Tcp);
            }
            other => panic!("expected HostPort, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_resolver_watch_emits_upsert_then_delete() {
        let (node_local, _cluster_db, resolver) = build_resolver().await;
        let mut stream = resolver.watch();

        let row = sample_row("uid-w", Ipv4Addr::new(10, 42, 7, 9), PodEndpointMode::Vxlan);
        node_local.upsert_endpoint(row).await.unwrap();
        let evt = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timed out waiting for upsert")
            .expect("stream must emit upsert");
        match evt {
            EndpointEvent::Upsert(Endpoint::EncryptedDirect { pod_ip, node_name }) => {
                assert_eq!(pod_ip, Ipv4Addr::new(10, 42, 7, 9));
                assert_eq!(node_name, "node-a");
            }
            other => panic!("expected Upsert(EncryptedDirect), got {other:?}"),
        }

        node_local.delete_endpoint_for_uid("uid-w").await.unwrap();
        let evt = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timed out waiting for delete")
            .expect("stream must emit delete");
        match evt {
            EndpointEvent::Delete(pod_ip) => assert_eq!(pod_ip, Ipv4Addr::new(10, 42, 7, 9)),
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_resolver_watch_preserves_hostport_pod_and_node_identity() {
        let (node_local, _cluster_db, resolver) = build_resolver().await;
        let mut stream = resolver.watch();

        let mut row = sample_row(
            "uid-hpw",
            Ipv4Addr::new(10, 42, 8, 9),
            PodEndpointMode::Hostport,
        );
        row.node_name = "rootless-b".to_string();
        row.node_ip = Ipv4Addr::new(192, 0, 2, 44);
        row.host_port_tcp = Some(31234);
        node_local.upsert_endpoint(row).await.unwrap();

        let evt = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timed out waiting for hostport upsert")
            .expect("stream must emit hostport upsert");
        match evt {
            EndpointEvent::Upsert(Endpoint::HostPort {
                pod_ip,
                node_name,
                node_ip,
                host_port,
                protocol,
            }) => {
                assert_eq!(pod_ip, Ipv4Addr::new(10, 42, 8, 9));
                assert_eq!(node_name, "rootless-b");
                assert_eq!(node_ip, IpAddr::V4(Ipv4Addr::new(192, 0, 2, 44)));
                assert_eq!(host_port, 31234);
                assert_eq!(protocol, Protocol::Tcp);
            }
            other => panic!("expected Upsert(HostPort), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_resolver_handles_empty_table_without_error() {
        // Root-only Phase 1 normal state — table is empty. The resolver
        // must return None for every lookup without error and the watch
        // stream must be live (not closed) even with no events.
        let (_node_local, _cluster_db, resolver) = build_resolver().await;
        for ip in [
            Ipv4Addr::new(0, 0, 0, 0),
            Ipv4Addr::new(10, 1, 2, 3),
            Ipv4Addr::new(255, 255, 255, 255),
        ] {
            let r = resolver
                .resolve(ip)
                .await
                .unwrap_or_else(|e| panic!("empty-table resolve must not error for {ip}: {e}"));
            assert!(r.is_none(), "{ip} must resolve to None on empty table");
        }
        // Stream must still be subscribable (no immediate close).
        let mut stream = resolver.watch();
        let next = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
        assert!(
            next.is_err(),
            "empty-table watch must idle, not emit; got {:?}",
            next
        );
    }

    #[tokio::test]
    async fn test_resolver_watch_survives_high_throughput() {
        // Verify the resolver watch stream doesn't panic or close under
        // high event throughput that may trigger broadcast Lagged.
        let (node_local, _cluster_db, resolver) = build_resolver().await;

        let mut stream = resolver.watch();

        // Produce many upserts rapidly
        let mut inserted_ips: Vec<Ipv4Addr> = Vec::new();
        for i in 0..5000u16 {
            let ip = Ipv4Addr::new(10, 42, (i / 256) as u8, (i % 256) as u8);
            let row = PodEndpointRow {
                pod_uid: format!("uid-{i}"),
                namespace: "default".to_string(),
                pod_name: format!("pod-{i}"),
                node_name: "node-a".to_string(),
                mode: PodEndpointMode::Vxlan,
                pod_ip: ip,
                node_ip: Ipv4Addr::new(10, 0, 0, 1),
                host_port_tcp: None,
                host_port_udp: None,
                generation: 1,
                updated_at: 1_700_000_000,
            };
            node_local.upsert_endpoint(row).await.unwrap();
            inserted_ips.push(ip);
        }

        // The stream should still be alive and emit events (from the re-list
        // if Lagged occurred, or from the live channel)
        let evt = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timed out waiting for event after high throughput")
            .expect("stream must not close after high throughput");
        assert!(
            matches!(evt, EndpointEvent::Upsert(_)),
            "expected Upsert after high-throughput inserts, got {:?}",
            evt
        );
    }
}
