use std::sync::Arc;

use klights_node_store::{
    CacheNetworkFuture, EndpointDeleteOutcome, EndpointUpsertOutcome, NodeKey, PodEndpointRecord,
    PodEndpointStore, PodEndpointStoreEventSource, PodEndpointStoreEventStream, PodIpamStore,
    PodNetworkAllocation, PodNetworkAllocationRequest, PodNetworkAssignmentSnapshot,
    PodNetworkCache, PodNetworkEndpoint, PodUidKey, SandboxKey,
};

use super::NodeLocalHandle;

/// Root-owned adapter from the concrete node database to focused node ports.
#[derive(Clone)]
pub(crate) struct NodeLocalNetworkAdapter {
    backend: NodeLocalHandle,
}

impl NodeLocalNetworkAdapter {
    pub(crate) fn new(backend: NodeLocalHandle) -> Arc<Self> {
        Arc::new(Self { backend })
    }
}

impl PodNetworkCache for NodeLocalNetworkAdapter {
    fn get_network_for_uid(
        &self,
        pod_uid: PodUidKey,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        PodNetworkCache::get_network_for_uid(self.backend.as_ref(), pod_uid)
    }

    fn get_network_for_pod(
        &self,
        pod: klights_types::PodIdentity,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        PodNetworkCache::get_network_for_pod(self.backend.as_ref(), pod)
    }

    fn get_network_for_sandbox(
        &self,
        sandbox_id: SandboxKey,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        PodNetworkCache::get_network_for_sandbox(self.backend.as_ref(), sandbox_id)
    }

    fn get_network_for_assignment(
        &self,
        sandbox_id: SandboxKey,
        pod: klights_types::PodIdentity,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        PodNetworkCache::get_network_for_assignment(self.backend.as_ref(), sandbox_id, pod)
    }

    fn delete_network_for_sandbox(&self, sandbox_id: SandboxKey) -> CacheNetworkFuture<'_, ()> {
        PodNetworkCache::delete_network_for_sandbox(self.backend.as_ref(), sandbox_id)
    }

    fn delete_network_if_matches(
        &self,
        request: PodNetworkAllocationRequest,
    ) -> CacheNetworkFuture<'_, bool> {
        PodNetworkCache::delete_network_if_matches(self.backend.as_ref(), request)
    }

    fn list_network_assignments(
        &self,
    ) -> CacheNetworkFuture<'_, Vec<PodNetworkAssignmentSnapshot>> {
        PodNetworkCache::list_network_assignments(self.backend.as_ref())
    }
}

impl PodIpamStore for NodeLocalNetworkAdapter {
    fn reserve_ip_and_insert_network(
        &self,
        request: PodNetworkAllocationRequest,
    ) -> CacheNetworkFuture<'_, PodNetworkAllocation> {
        PodIpamStore::reserve_ip_and_insert_network(self.backend.as_ref(), request)
    }
}

impl PodEndpointStore for NodeLocalNetworkAdapter {
    fn upsert_endpoint(
        &self,
        record: PodEndpointRecord,
    ) -> CacheNetworkFuture<'_, EndpointUpsertOutcome> {
        PodEndpointStore::upsert_endpoint(self.backend.as_ref(), record)
    }

    fn delete_endpoint_for_uid(
        &self,
        pod_uid: PodUidKey,
    ) -> CacheNetworkFuture<'_, EndpointDeleteOutcome> {
        PodEndpointStore::delete_endpoint_for_uid(self.backend.as_ref(), pod_uid)
    }

    fn get_endpoint_by_pod_ip(
        &self,
        pod_ip: std::net::Ipv4Addr,
    ) -> CacheNetworkFuture<'_, Option<PodEndpointRecord>> {
        PodEndpointStore::get_endpoint_by_pod_ip(self.backend.as_ref(), pod_ip)
    }

    fn list_endpoints_all(&self) -> CacheNetworkFuture<'_, Vec<PodEndpointRecord>> {
        PodEndpointStore::list_endpoints_all(self.backend.as_ref())
    }

    fn list_endpoints_for_node(
        &self,
        node_name: NodeKey,
    ) -> CacheNetworkFuture<'_, Vec<PodEndpointRecord>> {
        PodEndpointStore::list_endpoints_for_node(self.backend.as_ref(), node_name)
    }
}

impl PodEndpointStoreEventSource for NodeLocalNetworkAdapter {
    fn subscribe_endpoint_events(&self) -> CacheNetworkFuture<'_, PodEndpointStoreEventStream> {
        self.backend.subscribe_endpoint_events()
    }
}

#[cfg(test)]
impl klights_node_store::PodRuntimeStore for NodeLocalNetworkAdapter {
    fn admit_pod_runtime(
        &self,
        admission: klights_node_store::PodRuntimeAdmission,
    ) -> klights_node_store::RuntimeWorkFuture<'_, ()> {
        self.backend.admit_pod_runtime(admission)
    }

    fn record_owned_sandbox(
        &self,
        sandbox: klights_node_store::OwnedPodSandbox,
    ) -> klights_node_store::RuntimeWorkFuture<'_, ()> {
        self.backend.record_owned_sandbox(sandbox)
    }

    fn record_cgroup(
        &self,
        cgroup: klights_node_store::PodRuntimeCgroup,
    ) -> klights_node_store::RuntimeWorkFuture<'_, ()> {
        self.backend.record_cgroup(cgroup)
    }

    fn delete_pod_runtime_for_uid(
        &self,
        pod_uid: klights_node_store::RuntimePodUid,
    ) -> klights_node_store::RuntimeWorkFuture<'_, ()> {
        self.backend.delete_pod_runtime_for_uid(pod_uid)
    }

    fn get_pod_runtime(
        &self,
        pod_uid: klights_node_store::RuntimePodUid,
    ) -> klights_node_store::RuntimeWorkFuture<'_, Option<klights_node_store::PodRuntimeRecord>>
    {
        self.backend.get_pod_runtime(pod_uid)
    }

    fn list_pod_runtime(
        &self,
    ) -> klights_node_store::RuntimeWorkFuture<'_, Vec<klights_node_store::PodRuntimeRecord>> {
        self.backend.list_pod_runtime()
    }

    fn list_pod_runtime_by_namespace(
        &self,
        namespace: klights_node_store::RuntimeNamespace,
    ) -> klights_node_store::RuntimeWorkFuture<'_, Vec<klights_node_store::PodRuntimeRecord>> {
        self.backend.list_pod_runtime_by_namespace(namespace)
    }
}

#[cfg(test)]
mod tests {
    use klights_node_store::{
        CacheNetworkError, OwnedPodSandbox, PodIpamStore, PodNetworkAllocationRequest,
        PodNetworkCache, PodRuntimeRecord, PodUidKey, RuntimePodUid, RuntimeWorkError, SandboxKey,
    };

    use super::NodeLocalNetworkAdapter;

    #[tokio::test]
    async fn focused_adapter_preserves_sandbox_uid_and_atomic_ipam_values() {
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let backend = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:focused-network-adapter-test",
        )
        .await
        .unwrap();
        let runtime = backend.clone();
        let adapter = NodeLocalNetworkAdapter::new(backend);
        let pod = klights_types::PodIdentity::new("default", "pod-a", "uid-a");
        klights_node_store::PodRuntimeStore::record_owned_sandbox(
            runtime.as_ref(),
            OwnedPodSandbox::try_new(pod.clone(), "node-a", "sandbox-a", 123).unwrap(),
        )
        .await
        .unwrap();

        let allocation = adapter
            .reserve_ip_and_insert_network(
                PodNetworkAllocationRequest::try_new(
                    "sandbox-a",
                    pod,
                    u32::from(std::net::Ipv4Addr::new(10, 42, 7, 0)),
                    256,
                    "veth-a",
                    "/run/netns/a",
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(allocation.ip_addr(), "10.42.7.2");

        let by_sandbox = adapter
            .get_network_for_sandbox(SandboxKey::try_new("sandbox-a").unwrap())
            .await
            .unwrap()
            .unwrap();
        let by_uid = adapter
            .get_network_for_uid(PodUidKey::try_new("uid-a").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_sandbox, by_uid);
        assert_eq!(by_uid.veth_host(), "veth-a");
        let runtime_rows = klights_node_store::PodRuntimeStore::list_pod_runtime(runtime.as_ref())
            .await
            .unwrap();
        assert_eq!(
            runtime_rows.first().and_then(|record| record.sandbox_id()),
            Some("sandbox-a")
        );
        assert_eq!(
            runtime_rows.first().map(PodRuntimeRecord::created_ms),
            Some(123)
        );
    }

    #[tokio::test]
    async fn concurrent_owned_sandbox_records_preserve_one_immutable_winner() {
        let runtime = runtime_store_for_identity_test("sqlite:owned-sandbox-cas-test").await;
        let pod = klights_types::PodIdentity::new("default", "pod-cas", "uid-cas");
        let first = klights_node_store::PodRuntimeStore::record_owned_sandbox(
            runtime.as_ref(),
            OwnedPodSandbox::try_new(pod.clone(), "node-a", "sandbox-a", 101).unwrap(),
        );
        let second = klights_node_store::PodRuntimeStore::record_owned_sandbox(
            runtime.as_ref(),
            OwnedPodSandbox::try_new(pod, "node-a", "sandbox-b", 102).unwrap(),
        );
        let (first, second) = tokio::join!(first, second);

        assert_eq!(
            usize::from(first.is_ok()) + usize::from(second.is_ok()),
            1,
            "exactly one competing sandbox may establish UID ownership"
        );
        let conflict = first.err().or_else(|| second.err()).unwrap();
        assert!(matches!(
            conflict,
            RuntimeWorkError::OwnershipConflict {
                pod_uid,
                existing_sandbox_id: Some(_),
                ..
            } if pod_uid == "uid-cas"
        ));
        let persisted = klights_node_store::PodRuntimeStore::get_pod_runtime(
            runtime.as_ref(),
            RuntimePodUid::try_new("uid-cas").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            persisted.sandbox_id(),
            Some("sandbox-a" | "sandbox-b")
        ));
    }

    async fn runtime_store_for_identity_test(
        diagnostic_name: &'static str,
    ) -> crate::datastore::node_local::NodeLocalHandle {
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            diagnostic_name,
        )
        .await
        .unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn allocation_request(
        sandbox: &str,
        namespace: &str,
        name: &str,
        uid: &str,
        subnet_base: u32,
        subnet_size: u32,
        veth: &str,
        netns: &str,
    ) -> PodNetworkAllocationRequest {
        PodNetworkAllocationRequest::try_new(
            sandbox,
            klights_types::PodIdentity::new(namespace, name, uid),
            subnet_base,
            subnet_size,
            veth,
            netns,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn migrated_legacy_allocation_adopts_exact_requested_subnet_identity() {
        let directory = tempfile::tempdir().expect("create legacy pod-network fixture");
        #[cfg(unix)]
        std::fs::set_permissions(
            directory.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let path = directory.path().join("node.db");
        let base = u32::from(std::net::Ipv4Addr::new(10, 42, 89, 0));
        {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection
                .execute_batch(
                    "
                    CREATE TABLE pod_networks (
                        sandbox_id TEXT PRIMARY KEY,
                        namespace  TEXT NOT NULL,
                        pod_name   TEXT NOT NULL,
                        pod_uid    TEXT NOT NULL,
                        ip_addr    TEXT NOT NULL,
                        ip_int     INTEGER NOT NULL UNIQUE,
                        veth_host  TEXT NOT NULL,
                        netns_path TEXT NOT NULL,
                        created_ms INTEGER NOT NULL
                    );
                    INSERT INTO pod_networks (
                        sandbox_id, namespace, pod_name, pod_uid, ip_addr, ip_int,
                        veth_host, netns_path, created_ms
                    ) VALUES (
                        'sandbox-legacy', 'default', 'pod-legacy', 'uid-legacy',
                        '10.42.89.2', 170547458, 'veth-legacy',
                        '/run/netns/legacy', 1
                    );
                    ",
                )
                .unwrap();
        }

        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let (backend, sqlite) =
            crate::datastore::node_local::selector::open_node_local_with_sqlite(
                crate::datastore::backend_kind::BackendKind::Sqlite,
                Some(&path),
                supervisor,
                None,
                "sqlite:focused-network-legacy-adoption",
            )
            .await
            .unwrap();
        let sqlite = sqlite.expect("SQLite backend");
        let adapter = NodeLocalNetworkAdapter::new(backend);
        let request = allocation_request(
            "sandbox-legacy",
            "default",
            "pod-legacy",
            "uid-legacy",
            base,
            256,
            "veth-legacy",
            "/run/netns/legacy",
        );

        let allocation = adapter
            .reserve_ip_and_insert_network(request)
            .await
            .expect("an exact migrated row must lazily adopt its requested subnet");
        assert_eq!(allocation.ip_addr(), "10.42.89.2");
        assert_eq!(allocation.ip_int(), base + 2);

        let adopted = PodNetworkCache::list_network_assignments(sqlite.as_ref())
            .await
            .unwrap()
            .into_iter()
            .find(|row| row.request().sandbox_id() == "sandbox-legacy")
            .expect("legacy allocation remains present");
        assert_eq!(
            (
                adopted.request().subnet_base_int(),
                adopted.request().subnet_size()
            ),
            (base, 256)
        );
    }

    #[tokio::test]
    async fn real_adapter_preserves_typed_exhaustion_and_exact_allocation_identity() {
        let backend = runtime_store_for_identity_test("sqlite:focused-network-identity").await;
        let adapter = NodeLocalNetworkAdapter::new(backend);
        let base = u32::from(std::net::Ipv4Addr::new(10, 42, 90, 0));
        let original = allocation_request(
            "sandbox-a",
            "namespace-a",
            "pod-a",
            "uid-a",
            base,
            4,
            "veth-a",
            "/run/netns/a",
        );
        let allocation = adapter
            .reserve_ip_and_insert_network(original.clone())
            .await
            .unwrap();
        assert_eq!(
            adapter
                .reserve_ip_and_insert_network(original.clone())
                .await
                .unwrap(),
            allocation,
            "an exact duplicate must reuse the winner"
        );

        let conflicts = [
            allocation_request(
                "sandbox-a",
                "namespace-b",
                "pod-a",
                "uid-a",
                base,
                4,
                "veth-a",
                "/run/netns/a",
            ),
            allocation_request(
                "sandbox-a",
                "namespace-a",
                "pod-b",
                "uid-a",
                base,
                4,
                "veth-a",
                "/run/netns/a",
            ),
            allocation_request(
                "sandbox-a",
                "namespace-a",
                "pod-a",
                "uid-b",
                base,
                4,
                "veth-a",
                "/run/netns/a",
            ),
            allocation_request(
                "sandbox-a",
                "namespace-a",
                "pod-a",
                "uid-a",
                base + 4,
                4,
                "veth-a",
                "/run/netns/a",
            ),
            allocation_request(
                "sandbox-a",
                "namespace-a",
                "pod-a",
                "uid-a",
                base,
                4,
                "veth-b",
                "/run/netns/a",
            ),
            allocation_request(
                "sandbox-a",
                "namespace-a",
                "pod-a",
                "uid-a",
                base,
                4,
                "veth-a",
                "/run/netns/b",
            ),
        ];
        for conflict in conflicts {
            assert!(matches!(
                adapter.reserve_ip_and_insert_network(conflict).await,
                Err(CacheNetworkError::IdentityConflict { ref sandbox_id })
                    if sandbox_id == "sandbox-a"
            ));
        }

        assert!(
            adapter
                .get_network_for_uid(PodUidKey::try_new("uid-b").unwrap())
                .await
                .unwrap()
                .is_none(),
            "UID-A's row must never satisfy a UID-B lookup"
        );
        assert!(matches!(
            adapter
                .reserve_ip_and_insert_network(allocation_request(
                    "sandbox-b",
                    "namespace-b",
                    "pod-b",
                    "uid-b",
                    base,
                    4,
                    "veth-b",
                    "/run/netns/b",
                ))
                .await,
            Err(CacheNetworkError::AddressExhausted {
                subnet_base_int,
                subnet_size: 4,
            }) if subnet_base_int == base
        ));
    }

    #[tokio::test]
    async fn concurrent_conflicting_reservations_cannot_accept_or_delete_the_winner() {
        let backend = runtime_store_for_identity_test("sqlite:focused-network-race").await;
        let adapter = NodeLocalNetworkAdapter::new(backend);
        let base = u32::from(std::net::Ipv4Addr::new(10, 42, 92, 0));
        let request_a = allocation_request(
            "sandbox-race",
            "default",
            "pod-a",
            "uid-a",
            base,
            8,
            "veth-a",
            "/run/netns/a",
        );
        let request_b = allocation_request(
            "sandbox-race",
            "default",
            "pod-b",
            "uid-b",
            base,
            8,
            "veth-b",
            "/run/netns/b",
        );
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let left = {
            let adapter = adapter.clone();
            let request = request_a.clone();
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                (
                    request.clone(),
                    adapter.reserve_ip_and_insert_network(request).await,
                )
            }
        };
        let right = {
            let adapter = adapter.clone();
            let request = request_b.clone();
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                (
                    request.clone(),
                    adapter.reserve_ip_and_insert_network(request).await,
                )
            }
        };
        let (left, right) = tokio::join!(left, right);
        let (winner, loser) = match (&left.1, &right.1) {
            (Ok(_), Err(CacheNetworkError::IdentityConflict { .. })) => (left.0, right.0),
            (Err(CacheNetworkError::IdentityConflict { .. }), Ok(_)) => (right.0, left.0),
            outcomes => panic!("expected exactly one winner and one conflict, got {outcomes:?}"),
        };

        assert!(!adapter.delete_network_if_matches(loser).await.unwrap());
        assert!(
            adapter
                .get_network_for_assignment(
                    SandboxKey::try_new("sandbox-race").unwrap(),
                    winner.pod().clone(),
                )
                .await
                .unwrap()
                .is_some(),
            "loser's conditional cleanup must leave the winning row intact"
        );
    }
}
