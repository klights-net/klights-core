//! `PodEndpointResolver` — cross-mode pod reachability lookup.
//!
//! Cross-mode pod reachability is mediated by a `PodEndpointResolver`. The
//! SQLite-backed implementation can be shared by later dataplane consumers
//! without refactoring root-mode code.

use futures::stream::{BoxStream, StreamExt};
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::control_plane::client::LeaderApiClient;
use crate::datastore::node_local::NodeLocalHandle;
use crate::datastore::{PodEndpointEvent, PodEndpointMode};
use klights_network_api::{
    DirectPodEndpoint, HostPortPodEndpoint, PodEndpoint, PodEndpointError, PodEndpointEventSource,
    PodEndpointEventStream, PodEndpointEventSubscription, PodEndpointFuture, PodEndpointResolver,
    PodEndpointTopology,
};

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

impl PodEndpointResolver for SqlitePodEndpointResolver {
    fn resolve(&self, pod_ip: Ipv4Addr) -> PodEndpointFuture<'_, Option<PodEndpoint>> {
        Box::pin(async move {
            let Some(row) = self
                .node_local
                .get_endpoint_by_pod_ip(pod_ip)
                .await
                .map_err(|error| PodEndpointError::resolve(error.to_string()))?
            else {
                return Ok(None);
            };
            match row.mode {
                PodEndpointMode::EncryptedDirect => {
                    let query =
                        crate::control_plane::client::NodeDataplaneQuery::try_new(&row.node_name)
                            .map_err(|error| PodEndpointError::resolve(error.to_string()))?;
                    let Some(metadata) = self
                        .cluster_api
                        .get_node_dataplane(query)
                        .await
                        .map_err(|error| PodEndpointError::resolve(error.to_string()))?
                        .into_option()
                    else {
                        return Ok(None);
                    };
                    let endpoint = DirectPodEndpoint::try_new(row.pod_ip, row.node_name)?;
                    Ok(Some(match metadata.encryption() {
                        crate::control_plane::client::DataplaneEncryption::WireGuard => {
                            PodEndpoint::EncryptedDirect(endpoint)
                        }
                        crate::control_plane::client::DataplaneEncryption::Direct => {
                            PodEndpoint::UnencryptedDirect(endpoint)
                        }
                    }))
                }
                PodEndpointMode::Hostport => {
                    if row.host_port_tcp.is_none() && row.host_port_udp.is_none() {
                        return Ok(None);
                    }
                    Ok(Some(PodEndpoint::HostPort(HostPortPodEndpoint::try_new(
                        row.pod_ip,
                        row.node_name,
                        row.node_ip,
                        row.host_port_tcp,
                        row.host_port_udp,
                    )?)))
                }
            }
        })
    }
}

impl PodEndpointEventSource for SqlitePodEndpointResolver {
    fn subscribe(&self) -> PodEndpointFuture<'_, PodEndpointEventStream> {
        Box::pin(async move {
            let (rows, rx) = self
                .node_local
                .subscribe_pod_endpoints_with_snapshot()
                .await
                .map_err(|error| PodEndpointError::event_source(error.to_string()))?;
            let initial = translate_endpoint_snapshot(rows)?;
            let node_local = self.node_local.clone();
            let inner = futures::stream::unfold(
                (Some(initial), Some(rx), node_local),
                |(initial, receiver, node_local)| async move {
                    if let Some(snapshot) = initial {
                        return Some((
                            Ok(klights_network_api::PodEndpointEvent::Resync(snapshot)),
                            (None, receiver, node_local),
                        ));
                    }
                    let mut rx = receiver?;
                    match rx.recv().await {
                            Ok(event) => match translate_endpoint_event(event) {
                                Ok(event) => Some((Ok(event), (None, Some(rx), node_local))),
                                Err(error) => Some((Err(error), (None, None, node_local))),
                            },
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(lagged_count)) => {
                                tracing::warn!(
                                    lagged_count,
                                    "pod_endpoint_resolver: broadcast Lagged; replacing subscription from an atomic snapshot"
                                );
                                match node_local.subscribe_pod_endpoints_with_snapshot().await {
                                    Ok((rows, fresh_rx)) => match translate_endpoint_snapshot(rows) {
                                        Ok(snapshot) => Some((
                                            Ok(klights_network_api::PodEndpointEvent::Resync(
                                                snapshot,
                                            )),
                                            (None, Some(fresh_rx), node_local),
                                        )),
                                        Err(error) => {
                                            Some((Err(error), (None, None, node_local)))
                                        }
                                    },
                                    Err(error) => Some((
                                        Err(PodEndpointError::event_source(error.to_string())),
                                        (None, None, node_local),
                                    )),
                                }
                            }
                    }
                },
            )
            .boxed();
            Ok(Box::pin(ResolverEndpointSubscription { inner }) as PodEndpointEventStream)
        })
    }
}

struct ResolverEndpointSubscription {
    inner: BoxStream<
        'static,
        Result<klights_network_api::PodEndpointEvent, klights_network_api::PodEndpointError>,
    >,
}

impl PodEndpointEventSubscription for ResolverEndpointSubscription {
    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<
        Option<
            Result<klights_network_api::PodEndpointEvent, klights_network_api::PodEndpointError>,
        >,
    > {
        self.get_mut().inner.as_mut().poll_next(context)
    }
}

fn translate_endpoint_snapshot(
    rows: Vec<crate::datastore::PodEndpointRow>,
) -> Result<Vec<PodEndpointTopology>, PodEndpointError> {
    rows.into_iter().map(translate_endpoint_row).collect()
}

fn translate_endpoint_row(
    row: crate::datastore::PodEndpointRow,
) -> Result<PodEndpointTopology, PodEndpointError> {
    match row.mode {
        PodEndpointMode::EncryptedDirect => Ok(PodEndpointTopology::Direct(
            DirectPodEndpoint::try_new(row.pod_ip, row.node_name)?,
        )),
        PodEndpointMode::Hostport => {
            Ok(PodEndpointTopology::HostPort(HostPortPodEndpoint::try_new(
                row.pod_ip,
                row.node_name,
                row.node_ip,
                row.host_port_tcp,
                row.host_port_udp,
            )?))
        }
    }
}

fn translate_endpoint_event(
    event: PodEndpointEvent,
) -> Result<klights_network_api::PodEndpointEvent, PodEndpointError> {
    match event {
        PodEndpointEvent::Upsert(row) => Ok(klights_network_api::PodEndpointEvent::Upsert(
            translate_endpoint_row(row)?,
        )),
        PodEndpointEvent::Delete { pod_ip, .. } => {
            Ok(klights_network_api::PodEndpointEvent::Delete(pod_ip))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::client::local::LocalApiClient;
    use crate::datastore::PodEndpointRow;
    use crate::datastore::node_local::{NodeLocalHandle, selector};
    use crate::datastore::sqlite::Datastore;
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use std::sync::Arc;
    use tokio::time::Duration;

    async fn next_endpoint_event(
        events: &mut PodEndpointEventStream,
    ) -> Option<klights_network_api::PodEndpointEvent> {
        match std::future::poll_fn(|context| events.as_mut().poll_next(context)).await {
            Some(Ok(event)) => Some(event),
            Some(Err(error)) => panic!("endpoint subscription failed: {error}"),
            None => None,
        }
    }

    async fn subscribe_after_initial_resync(
        resolver: &SqlitePodEndpointResolver,
    ) -> PodEndpointEventStream {
        let mut events = resolver.subscribe().await.expect("subscribe endpoints");
        assert!(matches!(
            next_endpoint_event(&mut events).await,
            Some(klights_network_api::PodEndpointEvent::Resync(_))
        ));
        events
    }

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
    async fn test_resolver_returns_none_when_dataplane_metadata_is_missing() {
        let (node_local, _cluster_db, resolver) = build_resolver().await;
        let row = sample_row(
            "uid-d",
            Ipv4Addr::new(10, 42, 1, 5),
            PodEndpointMode::EncryptedDirect,
        );
        node_local.upsert_endpoint(row).await.unwrap();
        let resolved = resolver.resolve(Ipv4Addr::new(10, 42, 1, 5)).await.unwrap();
        assert!(resolved.is_none(), "missing metadata must install no route");
    }

    #[tokio::test]
    async fn test_resolver_returns_encrypted_direct_for_explicit_wireguard_metadata() {
        let (node_local, cluster_db, resolver) = build_resolver().await;
        let row = sample_row(
            "uid-wg",
            Ipv4Addr::new(10, 42, 1, 7),
            PodEndpointMode::EncryptedDirect,
        );
        node_local.upsert_endpoint(row).await.unwrap();
        cluster_db
            .update_node_dataplane(
                crate::networking::wireguard::DataplanePeerMetadata::try_new(
                    "node-a".to_string(),
                    crate::networking::wireguard::DataplaneMode::Root,
                    crate::networking::wireguard::DataplaneEncryption::Enabled,
                    Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
                    Some("192.0.2.10".to_string()),
                    Some(7_679),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let resolved = resolver
            .resolve(Ipv4Addr::new(10, 42, 1, 7))
            .await
            .unwrap()
            .expect("explicit WireGuard metadata must resolve");
        match resolved {
            PodEndpoint::EncryptedDirect(endpoint) => {
                assert_eq!(endpoint.pod_ip(), Ipv4Addr::new(10, 42, 1, 7));
                assert_eq!(endpoint.node_name(), "node-a");
            }
            other => panic!("expected EncryptedDirect, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_resolver_returns_unencrypted_direct_only_for_explicit_disabled_dataplane() {
        let (node_local, cluster_db, resolver) = build_resolver().await;
        let row = sample_row(
            "uid-u",
            Ipv4Addr::new(10, 42, 1, 6),
            PodEndpointMode::EncryptedDirect,
        );
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
            .expect("encrypted-direct row must resolve");
        match resolved {
            PodEndpoint::UnencryptedDirect(endpoint) => {
                assert_eq!(endpoint.pod_ip(), Ipv4Addr::new(10, 42, 1, 6));
                assert_eq!(endpoint.node_name(), "node-a");
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
            PodEndpoint::HostPort(endpoint) => {
                assert_eq!(endpoint.pod_ip(), Ipv4Addr::new(10, 42, 9, 1));
                assert_eq!(endpoint.node_name(), "node-a");
                assert_eq!(endpoint.node_ip(), Ipv4Addr::new(192, 0, 2, 10));
                assert_eq!(endpoint.host_port_tcp(), Some(31000));
                assert_eq!(endpoint.host_port_udp(), None);
            }
            other => panic!("expected HostPort, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_resolver_rejects_hostport_row_without_any_published_port() {
        let (node_local, _cluster_db, resolver) = build_resolver().await;
        let row = sample_row(
            "uid-empty-hp",
            Ipv4Addr::new(10, 42, 9, 2),
            PodEndpointMode::Hostport,
        );
        node_local.upsert_endpoint(row).await.unwrap();

        let resolved = resolver.resolve(Ipv4Addr::new(10, 42, 9, 2)).await.unwrap();
        assert_eq!(
            resolved, None,
            "hostPort topology without TCP or UDP publication is not reachable"
        );
    }

    #[tokio::test]
    async fn test_resolver_watch_emits_upsert_then_delete() {
        let (node_local, _cluster_db, resolver) = build_resolver().await;
        let mut stream = subscribe_after_initial_resync(&resolver).await;

        let row = sample_row(
            "uid-w",
            Ipv4Addr::new(10, 42, 7, 9),
            PodEndpointMode::EncryptedDirect,
        );
        node_local.upsert_endpoint(row).await.unwrap();
        let evt = tokio::time::timeout(Duration::from_secs(2), next_endpoint_event(&mut stream))
            .await
            .expect("timed out waiting for upsert")
            .expect("stream must emit upsert");
        match evt {
            klights_network_api::PodEndpointEvent::Upsert(PodEndpointTopology::Direct(
                endpoint,
            )) => {
                assert_eq!(endpoint.pod_ip(), Ipv4Addr::new(10, 42, 7, 9));
                assert_eq!(endpoint.node_name(), "node-a");
            }
            other => panic!("expected Upsert(Direct), got {other:?}"),
        }

        node_local.delete_endpoint_for_uid("uid-w").await.unwrap();
        let evt = tokio::time::timeout(Duration::from_secs(2), next_endpoint_event(&mut stream))
            .await
            .expect("timed out waiting for delete")
            .expect("stream must emit delete");
        match evt {
            klights_network_api::PodEndpointEvent::Delete(pod_ip) => {
                assert_eq!(pod_ip, Ipv4Addr::new(10, 42, 7, 9));
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_resolver_watch_emits_old_delete_before_address_change_upsert() {
        let (node_local, _cluster_db, resolver) = build_resolver().await;
        let mut events = subscribe_after_initial_resync(&resolver).await;
        node_local
            .upsert_endpoint(sample_row(
                "uid-address-change",
                Ipv4Addr::new(10, 42, 7, 10),
                PodEndpointMode::EncryptedDirect,
            ))
            .await
            .unwrap();
        let _initial =
            tokio::time::timeout(Duration::from_secs(2), next_endpoint_event(&mut events))
                .await
                .expect("timed out waiting for initial upsert")
                .expect("subscription must remain open");

        node_local
            .upsert_endpoint(sample_row(
                "uid-address-change",
                Ipv4Addr::new(10, 42, 7, 11),
                PodEndpointMode::EncryptedDirect,
            ))
            .await
            .unwrap();
        let delete = tokio::time::timeout(Duration::from_secs(2), next_endpoint_event(&mut events))
            .await
            .expect("timed out waiting for old-address delete")
            .expect("subscription must remain open");
        let upsert = tokio::time::timeout(Duration::from_secs(2), next_endpoint_event(&mut events))
            .await
            .expect("timed out waiting for new-address upsert")
            .expect("subscription must remain open");

        assert_eq!(
            delete,
            klights_network_api::PodEndpointEvent::Delete(Ipv4Addr::new(10, 42, 7, 10))
        );
        assert!(
            matches!(
                upsert,
                klights_network_api::PodEndpointEvent::Upsert(PodEndpointTopology::Direct(
                    ref endpoint
                )) if endpoint.pod_ip() == Ipv4Addr::new(10, 42, 7, 11)
            ),
            "new address must follow the old-address delete: {upsert:?}"
        );
    }

    #[tokio::test]
    async fn test_resolver_watch_preserves_hostport_pod_and_node_identity() {
        let (node_local, _cluster_db, resolver) = build_resolver().await;
        let mut stream = subscribe_after_initial_resync(&resolver).await;

        let mut row = sample_row(
            "uid-hpw",
            Ipv4Addr::new(10, 42, 8, 9),
            PodEndpointMode::Hostport,
        );
        row.node_name = "rootless-b".to_string();
        row.node_ip = Ipv4Addr::new(192, 0, 2, 44);
        row.host_port_tcp = Some(31234);
        row.host_port_udp = Some(31235);
        node_local.upsert_endpoint(row).await.unwrap();

        let evt = tokio::time::timeout(Duration::from_secs(2), next_endpoint_event(&mut stream))
            .await
            .expect("timed out waiting for hostport upsert")
            .expect("stream must emit hostport upsert");
        match evt {
            klights_network_api::PodEndpointEvent::Upsert(PodEndpointTopology::HostPort(
                endpoint,
            )) => {
                assert_eq!(endpoint.pod_ip(), Ipv4Addr::new(10, 42, 8, 9));
                assert_eq!(endpoint.node_name(), "rootless-b");
                assert_eq!(endpoint.node_ip(), Ipv4Addr::new(192, 0, 2, 44));
                assert_eq!(endpoint.host_port_tcp(), Some(31234));
                assert_eq!(endpoint.host_port_udp(), Some(31235));
            }
            other => panic!("expected Upsert(HostPort), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_resolver_watch_does_not_misclassify_direct_dataplane_as_encrypted() {
        let (node_local, cluster_db, resolver) = build_resolver().await;
        cluster_db
            .update_node_dataplane(
                crate::networking::wireguard::DataplanePeerMetadata::try_new(
                    "node-a".to_string(),
                    crate::networking::wireguard::DataplaneMode::Root,
                    crate::networking::wireguard::DataplaneEncryption::Disabled,
                    None,
                    Some("192.0.2.10".to_string()),
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let mut stream = subscribe_after_initial_resync(&resolver).await;
        node_local
            .upsert_endpoint(sample_row(
                "uid-direct-watch",
                Ipv4Addr::new(10, 42, 8, 10),
                PodEndpointMode::EncryptedDirect,
            ))
            .await
            .unwrap();

        let event = tokio::time::timeout(Duration::from_secs(2), next_endpoint_event(&mut stream))
            .await
            .expect("timed out waiting for direct topology event")
            .expect("stream must emit direct topology event");
        assert!(
            matches!(
                event,
                klights_network_api::PodEndpointEvent::Upsert(PodEndpointTopology::Direct(
                    ref endpoint
                )) if endpoint.pod_ip() == Ipv4Addr::new(10, 42, 8, 10)
            ),
            "topology events must not claim encryption before resolution: {event:?}"
        );
    }

    #[tokio::test]
    async fn test_resolver_watch_lag_recovery_preserves_persisted_order() {
        let (node_local, _cluster_db, resolver) = build_resolver().await;
        let mut stream = subscribe_after_initial_resync(&resolver).await;

        for i in 0..4_097u16 {
            node_local
                .upsert_endpoint(sample_row(
                    &format!("uid-{i:05}"),
                    Ipv4Addr::new(10, 60, (i / 256) as u8, (i % 256) as u8),
                    PodEndpointMode::EncryptedDirect,
                ))
                .await
                .unwrap();
        }

        let first = tokio::time::timeout(Duration::from_secs(5), next_endpoint_event(&mut stream))
            .await
            .expect("timed out waiting for lag recovery")
            .expect("stream must survive lag recovery");
        let klights_network_api::PodEndpointEvent::Resync(snapshot) = first else {
            panic!("lag recovery must emit an explicit resync snapshot; got {first:?}");
        };
        assert_eq!(snapshot.len(), 4_097);
        assert_eq!(
            snapshot.first().map(PodEndpointTopology::pod_ip),
            Some(Ipv4Addr::new(10, 60, 0, 0)),
            "lag recovery must retain node_name/pod_uid persistence order"
        );
    }

    #[tokio::test]
    async fn test_resolver_lag_resync_discards_retained_stale_events() {
        let (node_local, _cluster_db, resolver) = build_resolver().await;
        let mut stream = subscribe_after_initial_resync(&resolver).await;
        for i in 0..4_097u16 {
            node_local
                .upsert_endpoint(sample_row(
                    &format!("stale-{i:05}"),
                    Ipv4Addr::new(10, 61, (i / 256) as u8, (i % 256) as u8),
                    PodEndpointMode::EncryptedDirect,
                ))
                .await
                .unwrap();
        }

        assert!(matches!(
            next_endpoint_event(&mut stream).await,
            Some(klights_network_api::PodEndpointEvent::Resync(_))
        ));
        let current = sample_row(
            "post-resync",
            Ipv4Addr::new(10, 62, 0, 1),
            PodEndpointMode::EncryptedDirect,
        );
        node_local.upsert_endpoint(current.clone()).await.unwrap();
        let event = next_endpoint_event(&mut stream)
            .await
            .expect("fresh subscription must deliver post-resync mutation");
        assert!(
            matches!(
                event,
                klights_network_api::PodEndpointEvent::Upsert(PodEndpointTopology::Direct(
                    ref endpoint
                )) if endpoint.pod_ip() == current.pod_ip
            ),
            "retained pre-resync backlog must be discarded; got {event:?}"
        );
    }

    #[tokio::test]
    async fn test_resolver_lag_relist_failure_is_observable_and_fresh_retry_is_authoritative() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let executor = crate::datastore::sqlite::DbExecutor::open_with_opts(
            crate::datastore::sqlite::opener::OpenOpts::node_in_memory(),
            supervisor,
            "sqlite:pod-endpoint-resolver-failure-test",
        )
        .await
        .expect("open node-local executor");
        let node_local = crate::datastore::node_local::SqliteNodeLocalDb::from_executor(executor)
            .expect("open node-local backend");
        let cluster_db = Datastore::new_in_memory().await.unwrap();
        let cluster_api = Arc::new(LocalApiClient::new(
            Arc::new(cluster_db),
            "node-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let resolver = SqlitePodEndpointResolver::new(Arc::new(node_local.clone()), cluster_api);
        let mut failed_stream = resolver.subscribe().await.expect("initial subscription");
        assert!(matches!(
            next_endpoint_event(&mut failed_stream).await,
            Some(klights_network_api::PodEndpointEvent::Resync(_))
        ));
        node_local.fail_next_pod_endpoint_snapshot();
        for i in 0..4_097u16 {
            node_local
                .upsert_endpoint(sample_row(
                    &format!("failed-relist-{i:05}"),
                    Ipv4Addr::new(10, 64, (i / 256) as u8, (i % 256) as u8),
                    PodEndpointMode::EncryptedDirect,
                ))
                .await
                .unwrap();
        }
        let error =
            match std::future::poll_fn(|context| failed_stream.as_mut().poll_next(context)).await {
                Some(Err(error)) => error,
                other => panic!("injected lag relist failure must be observable; got {other:?}"),
            };
        assert!(matches!(error, PodEndpointError::EventSource { .. }));

        let current = sample_row(
            "after-failed-snapshot",
            Ipv4Addr::new(10, 63, 0, 1),
            PodEndpointMode::EncryptedDirect,
        );
        node_local.upsert_endpoint(current.clone()).await.unwrap();
        let mut retry = resolver.subscribe().await.expect("fresh retry");
        let Some(klights_network_api::PodEndpointEvent::Resync(snapshot)) =
            next_endpoint_event(&mut retry).await
        else {
            panic!("fresh retry must begin with authoritative Resync");
        };
        assert_eq!(
            snapshot.first().map(PodEndpointTopology::pod_ip),
            Some(current.pod_ip)
        );
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
        let mut stream = subscribe_after_initial_resync(&resolver).await;
        let next =
            tokio::time::timeout(Duration::from_millis(50), next_endpoint_event(&mut stream)).await;
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

        let mut stream = subscribe_after_initial_resync(&resolver).await;

        // Produce many upserts rapidly
        let mut inserted_ips: Vec<Ipv4Addr> = Vec::new();
        for i in 0..5000u16 {
            let ip = Ipv4Addr::new(10, 42, (i / 256) as u8, (i % 256) as u8);
            let row = PodEndpointRow {
                pod_uid: format!("uid-{i}"),
                namespace: "default".to_string(),
                pod_name: format!("pod-{i}"),
                node_name: "node-a".to_string(),
                mode: PodEndpointMode::EncryptedDirect,
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
        let evt = tokio::time::timeout(Duration::from_secs(5), next_endpoint_event(&mut stream))
            .await
            .expect("timed out waiting for event after high throughput")
            .expect("stream must not close after high throughput");
        assert!(
            matches!(
                evt,
                klights_network_api::PodEndpointEvent::Upsert(_)
                    | klights_network_api::PodEndpointEvent::Resync(_)
            ),
            "expected live update or explicit resync after high-throughput inserts, got {:?}",
            evt
        );
    }
}
