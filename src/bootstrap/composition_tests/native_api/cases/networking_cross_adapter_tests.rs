#[cfg(test)]
mod tests {
    use klights_network_api::{
        PodEndpoint, PodEndpointError, PodEndpointEventSource, PodEndpointEventStream,
        PodEndpointResolver, PodEndpointTopology,
    };
    use klights_networking::StorePodEndpointResolver;
    use klights_node_store::{PodEndpointMode, PodEndpointRecord, PodEndpointStore};
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use tokio::time::Duration;

    #[derive(Default)]
    struct FakeNetworkTopology {
        dataplane: Option<klights_leader_api::NetworkDataplane>,
    }

    impl FakeNetworkTopology {
        fn with_dataplane(dataplane: klights_leader_api::NetworkDataplane) -> Self {
            Self {
                dataplane: Some(dataplane),
            }
        }
    }

    impl klights_leader_api::LeaderNetworkTopologyQuery for FakeNetworkTopology {
        fn get_node_subnet(
            &self,
            request: klights_leader_api::NodeSubnetQuery,
        ) -> klights_leader_api::NetworkTopologyFuture<'_, klights_leader_api::NodeSubnetResult>
        {
            Box::pin(async move {
                klights_leader_api::NodeSubnetResult::try_from_wire(
                    request.node_name(),
                    false,
                    None,
                )
            })
        }

        fn list_peer_subnets(
            &self,
            request: klights_leader_api::PeerSubnetsQuery,
        ) -> klights_leader_api::NetworkTopologyFuture<'_, klights_leader_api::PeerSubnetsResult>
        {
            Box::pin(async move {
                klights_leader_api::PeerSubnetsResult::try_new(request.node_name(), Vec::new())
            })
        }

        fn get_node_dataplane(
            &self,
            request: klights_leader_api::NodeDataplaneQuery,
        ) -> klights_leader_api::NetworkTopologyFuture<'_, klights_leader_api::NodeDataplaneResult>
        {
            let dataplane = self.dataplane.clone();
            Box::pin(async move {
                klights_leader_api::NodeDataplaneResult::try_from_wire(
                    request.node_name(),
                    dataplane.is_some(),
                    dataplane,
                )
            })
        }
    }

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
        resolver: &StorePodEndpointResolver,
    ) -> PodEndpointEventStream {
        let mut events = resolver.subscribe().await.expect("subscribe endpoints");
        assert!(matches!(
            next_endpoint_event(&mut events).await,
            Some(klights_network_api::PodEndpointEvent::Resync(_))
        ));
        events
    }

    fn sample_row(uid: &str, pod_ip: Ipv4Addr, mode: PodEndpointMode) -> PodEndpointRecord {
        PodEndpointRecord::try_new(
            klights_types::PodIdentity::new("default", &format!("pod-{uid}"), uid),
            "node-a",
            mode,
            pod_ip,
            pod_ip,
            None,
            None,
            1,
            1_700_000_000,
        )
        .unwrap()
    }

    async fn persist_endpoint(store: &dyn PodEndpointStore, row: PodEndpointRecord) {
        store.upsert_endpoint(row).await.unwrap();
    }

    async fn remove_endpoint(store: &dyn PodEndpointStore, pod_uid: &str) {
        store
            .delete_endpoint_for_uid(klights_node_store::PodUidKey::try_new(pod_uid).unwrap())
            .await
            .unwrap();
    }

    async fn open_endpoint_store(
        connection_key: &'static str,
    ) -> (
        klights_supervisor::DbExecutor,
        Arc<klights_node_datastore::SqliteNodeNetworkStateStore>,
    ) {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let executor = klights_node_datastore::open::open_with_opts(
            klights_node_datastore::open::in_memory_opts(),
            supervisor,
            connection_key,
        )
        .await
        .expect("open node-local executor");
        let node_network = Arc::new(klights_node_datastore::SqliteNodeNetworkStateStore::new(
            executor.clone(),
            Arc::new(klights_supervisor::SystemWallClock),
        ));
        (executor, node_network)
    }

    async fn build_resolver_with_topology(
        topology: FakeNetworkTopology,
    ) -> (
        Arc<klights_node_datastore::SqliteNodeNetworkStateStore>,
        StorePodEndpointResolver,
    ) {
        let (_executor, node_network) =
            open_endpoint_store("sqlite:pod-endpoint-resolver-test").await;
        let resolver = klights_networking::StorePodEndpointResolver::new(
            node_network.clone(),
            node_network.clone(),
            Arc::new(topology),
        );
        (node_network, resolver)
    }

    async fn build_resolver() -> (
        Arc<klights_node_datastore::SqliteNodeNetworkStateStore>,
        StorePodEndpointResolver,
    ) {
        build_resolver_with_topology(FakeNetworkTopology::default()).await
    }

    async fn build_resolver_retaining_executor() -> (
        Arc<klights_node_datastore::SqliteNodeNetworkStateStore>,
        klights_supervisor::DbExecutor,
        StorePodEndpointResolver,
    ) {
        let (executor, node_network) =
            open_endpoint_store("sqlite:pod-endpoint-resolver-failure-test").await;
        let resolver = klights_networking::StorePodEndpointResolver::new(
            node_network.clone(),
            node_network.clone(),
            Arc::new(FakeNetworkTopology::default()),
        );
        (node_network, executor, resolver)
    }

    #[tokio::test]
    async fn test_resolver_returns_none_for_unknown_pod_ip() {
        let (_node_local, resolver) = build_resolver().await;
        let result = resolver.resolve(Ipv4Addr::new(10, 0, 0, 99)).await.unwrap();
        assert!(result.is_none(), "unknown pod IP must resolve to None");
    }

    #[tokio::test]
    async fn test_resolver_returns_none_when_dataplane_metadata_is_missing() {
        let (node_local, resolver) = build_resolver().await;
        let row = sample_row(
            "uid-d",
            Ipv4Addr::new(10, 42, 1, 5),
            PodEndpointMode::EncryptedDirect,
        );
        persist_endpoint(node_local.as_ref(), row).await;
        let resolved = resolver.resolve(Ipv4Addr::new(10, 42, 1, 5)).await.unwrap();
        assert!(resolved.is_none(), "missing metadata must install no route");
    }

    #[tokio::test]
    async fn test_resolver_returns_encrypted_direct_for_explicit_wireguard_metadata() {
        let topology = FakeNetworkTopology::with_dataplane(
            klights_leader_api::NetworkDataplane::try_new(
                "node-a",
                klights_leader_api::NetworkNodeMode::Root,
                klights_leader_api::DataplaneEncryption::WireGuard,
                Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
                IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
                Some(7_679),
            )
            .unwrap(),
        );
        let (node_local, resolver) = build_resolver_with_topology(topology).await;
        let row = sample_row(
            "uid-wg",
            Ipv4Addr::new(10, 42, 1, 7),
            PodEndpointMode::EncryptedDirect,
        );
        persist_endpoint(node_local.as_ref(), row).await;
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
        let topology = FakeNetworkTopology::with_dataplane(
            klights_leader_api::NetworkDataplane::try_new(
                "node-a",
                klights_leader_api::NetworkNodeMode::Rootless,
                klights_leader_api::DataplaneEncryption::Direct,
                None,
                IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
                None,
            )
            .unwrap(),
        );
        let (node_local, resolver) = build_resolver_with_topology(topology).await;
        let row = sample_row(
            "uid-u",
            Ipv4Addr::new(10, 42, 1, 6),
            PodEndpointMode::EncryptedDirect,
        );
        persist_endpoint(node_local.as_ref(), row).await;
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
        let (node_local, resolver) = build_resolver().await;
        let row = PodEndpointRecord::try_new(
            klights_types::PodIdentity::new("default", "pod-uid-hp", "uid-hp"),
            "node-a",
            PodEndpointMode::Hostport,
            Ipv4Addr::new(10, 42, 9, 1),
            Ipv4Addr::new(192, 0, 2, 10),
            Some(31_000),
            None,
            1,
            1_700_000_000,
        )
        .unwrap();
        persist_endpoint(node_local.as_ref(), row).await;
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
        let (node_local, resolver) = build_resolver().await;
        let row = sample_row(
            "uid-empty-hp",
            Ipv4Addr::new(10, 42, 9, 2),
            PodEndpointMode::Hostport,
        );
        persist_endpoint(node_local.as_ref(), row).await;

        let resolved = resolver.resolve(Ipv4Addr::new(10, 42, 9, 2)).await.unwrap();
        assert_eq!(
            resolved, None,
            "hostPort topology without TCP or UDP publication is not reachable"
        );
    }

    #[tokio::test]
    async fn test_resolver_watch_emits_upsert_then_delete() {
        let (node_local, resolver) = build_resolver().await;
        let mut stream = subscribe_after_initial_resync(&resolver).await;

        let row = sample_row(
            "uid-w",
            Ipv4Addr::new(10, 42, 7, 9),
            PodEndpointMode::EncryptedDirect,
        );
        persist_endpoint(node_local.as_ref(), row).await;
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

        remove_endpoint(node_local.as_ref(), "uid-w").await;
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
        let (node_local, resolver) = build_resolver().await;
        let mut events = subscribe_after_initial_resync(&resolver).await;
        persist_endpoint(
            node_local.as_ref(),
            sample_row(
                "uid-address-change",
                Ipv4Addr::new(10, 42, 7, 10),
                PodEndpointMode::EncryptedDirect,
            ),
        )
        .await;
        let _initial =
            tokio::time::timeout(Duration::from_secs(2), next_endpoint_event(&mut events))
                .await
                .expect("timed out waiting for initial upsert")
                .expect("subscription must remain open");

        persist_endpoint(
            node_local.as_ref(),
            sample_row(
                "uid-address-change",
                Ipv4Addr::new(10, 42, 7, 11),
                PodEndpointMode::EncryptedDirect,
            ),
        )
        .await;
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
        let (node_local, resolver) = build_resolver().await;
        let mut stream = subscribe_after_initial_resync(&resolver).await;

        let row = PodEndpointRecord::try_new(
            klights_types::PodIdentity::new("default", "pod-uid-hpw", "uid-hpw"),
            "rootless-b",
            PodEndpointMode::Hostport,
            Ipv4Addr::new(10, 42, 8, 9),
            Ipv4Addr::new(192, 0, 2, 44),
            Some(31_234),
            Some(31_235),
            1,
            1_700_000_000,
        )
        .unwrap();
        persist_endpoint(node_local.as_ref(), row).await;

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
        let topology = FakeNetworkTopology::with_dataplane(
            klights_leader_api::NetworkDataplane::try_new(
                "node-a",
                klights_leader_api::NetworkNodeMode::Root,
                klights_leader_api::DataplaneEncryption::Direct,
                None,
                IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)),
                None,
            )
            .unwrap(),
        );
        let (node_local, resolver) = build_resolver_with_topology(topology).await;
        let mut stream = subscribe_after_initial_resync(&resolver).await;
        persist_endpoint(
            node_local.as_ref(),
            sample_row(
                "uid-direct-watch",
                Ipv4Addr::new(10, 42, 8, 10),
                PodEndpointMode::EncryptedDirect,
            ),
        )
        .await;

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
        let (node_local, resolver) = build_resolver().await;
        let mut stream = subscribe_after_initial_resync(&resolver).await;

        for i in 0..4_097u16 {
            persist_endpoint(
                node_local.as_ref(),
                sample_row(
                    &format!("uid-{i:05}"),
                    Ipv4Addr::new(10, 60, (i / 256) as u8, (i % 256) as u8),
                    PodEndpointMode::EncryptedDirect,
                ),
            )
            .await;
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
        let (node_local, resolver) = build_resolver().await;
        let mut stream = subscribe_after_initial_resync(&resolver).await;
        for i in 0..4_097u16 {
            persist_endpoint(
                node_local.as_ref(),
                sample_row(
                    &format!("stale-{i:05}"),
                    Ipv4Addr::new(10, 61, (i / 256) as u8, (i % 256) as u8),
                    PodEndpointMode::EncryptedDirect,
                ),
            )
            .await;
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
        persist_endpoint(node_local.as_ref(), current.clone()).await;
        let event = next_endpoint_event(&mut stream)
            .await
            .expect("fresh subscription must deliver post-resync mutation");
        assert!(
            matches!(
                event,
                klights_network_api::PodEndpointEvent::Upsert(PodEndpointTopology::Direct(
                    ref endpoint
                )) if endpoint.pod_ip() == current.pod_ip()
            ),
            "retained pre-resync backlog must be discarded; got {event:?}"
        );
    }

    #[tokio::test]
    async fn test_resolver_lag_relist_failure_is_observable_and_fresh_retry_is_authoritative() {
        let (node_local, executor, resolver) = build_resolver_retaining_executor().await;
        let mut failed_stream = resolver.subscribe().await.expect("initial subscription");
        assert!(matches!(
            next_endpoint_event(&mut failed_stream).await,
            Some(klights_network_api::PodEndpointEvent::Resync(_))
        ));
        executor
            .call_raw("test_insert_malformed_endpoint_for_relist", move |conn| {
                conn.execute(
                    "INSERT INTO pod_endpoints \
                     (pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
                      host_port_tcp, host_port_udp, generation, updated_ms) \
                     VALUES ('malformed-relist', 'default', 'malformed-relist', 'node-a', \
                             'hostport', '10.63.255.254', '192.0.2.10', 65536, NULL, 1, 1)",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        for i in 0..4_097u16 {
            persist_endpoint(
                node_local.as_ref(),
                sample_row(
                    &format!("failed-relist-{i:05}"),
                    Ipv4Addr::new(10, 64, (i / 256) as u8, (i % 256) as u8),
                    PodEndpointMode::EncryptedDirect,
                ),
            )
            .await;
        }
        let error =
            match std::future::poll_fn(|context| failed_stream.as_mut().poll_next(context)).await {
                Some(Err(error)) => error,
                other => panic!("injected lag relist failure must be observable; got {other:?}"),
            };
        assert!(matches!(error, PodEndpointError::EventSource { .. }));

        executor
            .call_raw("test_delete_malformed_endpoint_for_retry", move |conn| {
                conn.execute(
                    "DELETE FROM pod_endpoints WHERE pod_uid = 'malformed-relist'",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let current = sample_row(
            "after-failed-snapshot",
            Ipv4Addr::new(10, 63, 0, 1),
            PodEndpointMode::EncryptedDirect,
        );
        persist_endpoint(node_local.as_ref(), current.clone()).await;
        let mut retry = resolver.subscribe().await.expect("fresh retry");
        let Some(klights_network_api::PodEndpointEvent::Resync(snapshot)) =
            next_endpoint_event(&mut retry).await
        else {
            panic!("fresh retry must begin with authoritative Resync");
        };
        assert_eq!(
            snapshot.first().map(PodEndpointTopology::pod_ip),
            Some(current.pod_ip())
        );
    }

    #[tokio::test]
    async fn test_resolver_handles_empty_table_without_error() {
        // Root-only Phase 1 normal state — table is empty. The resolver
        // must return None for every lookup without error and the watch
        // stream must be live (not closed) even with no events.
        let (_node_local, resolver) = build_resolver().await;
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
        let (node_local, resolver) = build_resolver().await;

        let mut stream = subscribe_after_initial_resync(&resolver).await;

        // Produce many upserts rapidly
        let mut inserted_ips: Vec<Ipv4Addr> = Vec::new();
        for i in 0..5000u16 {
            let ip = Ipv4Addr::new(10, 42, (i / 256) as u8, (i % 256) as u8);
            let row = PodEndpointRecord::try_new(
                klights_types::PodIdentity::new(
                    "default",
                    &format!("pod-{i}"),
                    &format!("uid-{i}"),
                ),
                "node-a",
                PodEndpointMode::EncryptedDirect,
                ip,
                Ipv4Addr::new(10, 0, 0, 1),
                None,
                None,
                1,
                1_700_000_000,
            )
            .unwrap();
            persist_endpoint(node_local.as_ref(), row).await;
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

use std::net::Ipv4Addr;
use std::sync::Arc;

use klights_node_store::{PodIpamStore as _, PodNetworkCache as _};

#[tokio::test]
async fn real_adapter_exhaustion_reclaims_stale_row_and_retries_once() {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let executor = klights_node_datastore::open::open_with_opts(
        klights_node_datastore::open::in_memory_opts(),
        supervisor,
        "sqlite:cni-real-adapter-stale-reclaim",
    )
    .await
    .unwrap();
    let wall_clock: Arc<dyn klights_supervisor::WallClock> =
        Arc::new(klights_supervisor::SystemWallClock);
    let adapter = Arc::new(klights_node_datastore::SqliteNodeNetworkStateStore::new(
        executor.clone(),
        wall_clock.clone(),
    ));
    let runtime = Arc::new(klights_node_datastore::SqliteRuntimeWorkStore::new(
        executor, wall_clock,
    ));
    let base = u32::from(Ipv4Addr::new(10, 42, 91, 0));
    let stale_pod = klights_types::PodIdentity::new("default", "stale", "uid-stale");
    let stale = adapter
        .reserve_ip_and_insert_network(
            klights_node_store::PodNetworkAllocationRequest::try_new(
                "sandbox-stale",
                stale_pod,
                base,
                4,
                "veth-stale",
                "/run/netns/stale",
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stale.ip_int(), base + 2);

    let winner_pod = klights_types::PodIdentity::new("default", "winner", "uid-winner");
    let winner = klights_networking::test_support::allocate_ip_with_reclaim(
        adapter.as_ref(),
        adapter.as_ref(),
        runtime.as_ref(),
        "sandbox-winner",
        &winner_pod,
        base,
        4,
        "veth-winner",
        "/run/netns/winner",
    )
    .await
    .expect("typed exhaustion must trigger stale-row reclaim and one retry");
    assert_eq!(winner.1, base + 2);
    assert!(
        adapter
            .get_network_for_sandbox(
                klights_node_store::SandboxKey::try_new("sandbox-stale").unwrap()
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        adapter
            .get_network_for_sandbox(
                klights_node_store::SandboxKey::try_new("sandbox-winner").unwrap()
            )
            .await
            .unwrap()
            .is_some()
    );
}
