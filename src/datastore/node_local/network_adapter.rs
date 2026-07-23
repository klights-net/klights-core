use std::sync::Arc;

use klights_node_store::{
    CacheNetworkError, CacheNetworkFuture, EndpointDeleteOutcome, EndpointUpsertOutcome, NodeKey,
    PodEndpointMode, PodEndpointRecord, PodEndpointStore, PodEndpointStoreEvent,
    PodEndpointStoreEventSource, PodEndpointStoreEventStream, PodEndpointStoreEventSubscription,
    PodIpamStore, PodNetworkAllocation, PodNetworkAllocationRequest, PodNetworkAssignmentSnapshot,
    PodNetworkCache, PodNetworkEndpoint, PodRuntimeAdmission, PodRuntimeCgroup, PodRuntimeRecord,
    PodRuntimeSandbox, PodRuntimeStore, PodUidKey, RuntimeNamespace, RuntimePodUid,
    RuntimeWorkError, RuntimeWorkFuture, SandboxKey,
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

fn cache_error(error: anyhow::Error) -> CacheNetworkError {
    CacheNetworkError::persistence_failed(error.to_string())
}

fn reservation_error(error: super::PodNetworkReservationError) -> CacheNetworkError {
    match error {
        super::PodNetworkReservationError::AddressExhausted {
            subnet_base_int,
            subnet_size,
        } => CacheNetworkError::AddressExhausted {
            subnet_base_int,
            subnet_size,
        },
        super::PodNetworkReservationError::IdentityConflict { sandbox_id } => {
            CacheNetworkError::IdentityConflict { sandbox_id }
        }
        super::PodNetworkReservationError::Persistence { message } => {
            CacheNetworkError::persistence_failed(message)
        }
    }
}

fn runtime_error(error: anyhow::Error) -> RuntimeWorkError {
    RuntimeWorkError::persistence_failed(error.to_string())
}

fn network_endpoint(
    row: crate::datastore::PodNetworkEndpoint,
) -> Result<PodNetworkEndpoint, CacheNetworkError> {
    PodNetworkEndpoint::try_new(row.ip_addr, row.veth_host, row.netns_path)
        .map_err(|error| CacheNetworkError::corrupt_data(error.to_string()))
}

fn assignment_endpoint(
    row: super::PodNetworkAssignmentRow,
) -> Result<PodNetworkEndpoint, CacheNetworkError> {
    PodNetworkEndpoint::try_new(row.ip_addr, row.veth_host, row.netns_path)
        .map_err(|error| CacheNetworkError::corrupt_data(error.to_string()))
}

fn assignment_snapshot(
    row: super::PodNetworkAssignmentRow,
) -> Result<PodNetworkAssignmentSnapshot, CacheNetworkError> {
    let request = PodNetworkAllocationRequest::try_from_persisted(
        row.sandbox_id,
        klights_types::PodIdentity::new(&row.namespace, &row.pod_name, &row.pod_uid),
        row.subnet_base_int,
        row.subnet_size,
        row.veth_host,
        row.netns_path,
    )
    .map_err(|error| CacheNetworkError::corrupt_data(error.to_string()))?;
    let allocation = PodNetworkAllocation::try_new(row.ip_addr, row.ip_int)
        .map_err(|error| CacheNetworkError::corrupt_data(error.to_string()))?;
    PodNetworkAssignmentSnapshot::try_new(request, allocation)
        .map_err(|error| CacheNetworkError::corrupt_data(error.to_string()))
}

fn runtime_record(row: super::PodRuntimeRow) -> Result<PodRuntimeRecord, RuntimeWorkError> {
    PodRuntimeRecord::try_new(
        klights_types::PodIdentity::new(&row.namespace, &row.pod_name, &row.pod_uid),
        row.node_name,
        row.sandbox_id,
        row.cgroup_path,
        row.created_ms,
        row.started_ms,
    )
    .map_err(|error| RuntimeWorkError::corrupt_data(error.to_string()))
}

fn endpoint_record(
    row: crate::datastore::PodEndpointRow,
) -> Result<PodEndpointRecord, CacheNetworkError> {
    PodEndpointRecord::try_from_persisted(
        klights_types::PodIdentity::new(&row.namespace, &row.pod_name, &row.pod_uid),
        row.node_name,
        match row.mode {
            crate::datastore::PodEndpointMode::EncryptedDirect => PodEndpointMode::EncryptedDirect,
            crate::datastore::PodEndpointMode::Hostport => PodEndpointMode::Hostport,
        },
        row.pod_ip,
        row.node_ip,
        row.host_port_tcp.map(i64::from),
        row.host_port_udp.map(i64::from),
        row.generation,
        row.updated_at,
    )
}

fn endpoint_row(record: PodEndpointRecord) -> crate::datastore::PodEndpointRow {
    let (pod, node_name, mode, pod_ip, node_ip, tcp, udp, generation, updated_at_ms) =
        record.into_parts();
    crate::datastore::PodEndpointRow {
        pod_uid: pod.uid,
        namespace: pod.namespace,
        pod_name: pod.name,
        node_name,
        mode: match mode {
            PodEndpointMode::EncryptedDirect => crate::datastore::PodEndpointMode::EncryptedDirect,
            PodEndpointMode::Hostport => crate::datastore::PodEndpointMode::Hostport,
        },
        pod_ip,
        node_ip,
        host_port_tcp: tcp,
        host_port_udp: udp,
        generation,
        updated_at: updated_at_ms,
    }
}

impl PodNetworkCache for NodeLocalNetworkAdapter {
    fn get_network_for_uid(
        &self,
        pod_uid: PodUidKey,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        Box::pin(async move {
            self.backend
                .get_network_for_uid(pod_uid.as_str())
                .await
                .map_err(cache_error)?
                .map(network_endpoint)
                .transpose()
        })
    }

    fn get_network_for_pod(
        &self,
        pod: klights_types::PodIdentity,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        Box::pin(async move {
            self.backend
                .get_network_assignment_for_pod(pod)
                .await
                .map_err(cache_error)?
                .map(assignment_endpoint)
                .transpose()
        })
    }

    fn get_network_for_sandbox(
        &self,
        sandbox_id: SandboxKey,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        Box::pin(async move {
            self.backend
                .get_network_for_sandbox(sandbox_id.as_str())
                .await
                .map_err(cache_error)?
                .map(network_endpoint)
                .transpose()
        })
    }

    fn get_network_for_assignment(
        &self,
        sandbox_id: SandboxKey,
        pod: klights_types::PodIdentity,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        Box::pin(async move {
            let row = self
                .backend
                .get_network_assignment_for_sandbox(sandbox_id.as_str())
                .await
                .map_err(cache_error)?;
            let Some(row) = row else {
                return Ok(None);
            };
            if row.namespace != pod.namespace || row.pod_name != pod.name || row.pod_uid != pod.uid
            {
                return Ok(None);
            }
            assignment_endpoint(row).map(Some)
        })
    }

    fn delete_network_for_sandbox(&self, sandbox_id: SandboxKey) -> CacheNetworkFuture<'_, ()> {
        Box::pin(async move {
            self.backend
                .delete_network_for_sandbox(sandbox_id.as_str())
                .await
                .map_err(cache_error)
        })
    }

    fn delete_network_if_matches(
        &self,
        request: PodNetworkAllocationRequest,
    ) -> CacheNetworkFuture<'_, bool> {
        Box::pin(async move {
            let (sandbox_id, pod, subnet_base, subnet_size, veth_host, netns_path) =
                request.into_parts();
            self.backend
                .delete_network_assignment_if_matches(
                    crate::datastore::PodNetworkAllocationRequest::new(
                        &sandbox_id,
                        crate::datastore::PodNetworkAllocationPod::new(
                            &pod.namespace,
                            &pod.name,
                            &pod.uid,
                        ),
                        crate::datastore::PodNetworkAllocationSubnet::new(subnet_base, subnet_size),
                        crate::datastore::PodNetworkAllocationLink::new(&veth_host, &netns_path),
                    ),
                )
                .await
                .map_err(cache_error)
        })
    }

    fn list_network_assignments(
        &self,
    ) -> CacheNetworkFuture<'_, Vec<PodNetworkAssignmentSnapshot>> {
        Box::pin(async move {
            self.backend
                .list_network_assignments()
                .await
                .map_err(cache_error)?
                .into_iter()
                .map(assignment_snapshot)
                .collect()
        })
    }
}

impl PodIpamStore for NodeLocalNetworkAdapter {
    fn reserve_ip_and_insert_network(
        &self,
        request: PodNetworkAllocationRequest,
    ) -> CacheNetworkFuture<'_, PodNetworkAllocation> {
        Box::pin(async move {
            let (sandbox_id, pod, subnet_base, subnet_size, veth_host, netns_path) =
                request.into_parts();
            let allocation = self
                .backend
                .reserve_network_assignment(crate::datastore::PodNetworkAllocationRequest::new(
                    &sandbox_id,
                    crate::datastore::PodNetworkAllocationPod::new(
                        &pod.namespace,
                        &pod.name,
                        &pod.uid,
                    ),
                    crate::datastore::PodNetworkAllocationSubnet::new(subnet_base, subnet_size),
                    crate::datastore::PodNetworkAllocationLink::new(&veth_host, &netns_path),
                ))
                .await
                .map_err(reservation_error)?;
            PodNetworkAllocation::try_new(allocation.0, allocation.1)
                .map_err(|error| CacheNetworkError::corrupt_data(error.to_string()))
        })
    }
}

impl PodEndpointStore for NodeLocalNetworkAdapter {
    fn upsert_endpoint(
        &self,
        record: PodEndpointRecord,
    ) -> CacheNetworkFuture<'_, EndpointUpsertOutcome> {
        Box::pin(async move {
            let previous = self
                .backend
                .list_endpoints_all()
                .await
                .map_err(cache_error)?
                .into_iter()
                .find(|row| row.pod_uid == record.pod().uid)
                .map(endpoint_record)
                .transpose()?;
            let current = record.clone();
            self.backend
                .upsert_endpoint(endpoint_row(record))
                .await
                .map_err(cache_error)?;
            Ok(EndpointUpsertOutcome::new(previous, current))
        })
    }

    fn delete_endpoint_for_uid(
        &self,
        pod_uid: PodUidKey,
    ) -> CacheNetworkFuture<'_, EndpointDeleteOutcome> {
        Box::pin(async move {
            let removed = self
                .backend
                .list_endpoints_all()
                .await
                .map_err(cache_error)?
                .into_iter()
                .find(|row| row.pod_uid == pod_uid.as_str())
                .map(endpoint_record)
                .transpose()?;
            self.backend
                .delete_endpoint_for_uid(pod_uid.as_str())
                .await
                .map_err(cache_error)?;
            Ok(EndpointDeleteOutcome::new(removed))
        })
    }

    fn get_endpoint_by_pod_ip(
        &self,
        pod_ip: std::net::Ipv4Addr,
    ) -> CacheNetworkFuture<'_, Option<PodEndpointRecord>> {
        Box::pin(async move {
            self.backend
                .get_endpoint_by_pod_ip(pod_ip)
                .await
                .map_err(cache_error)?
                .map(endpoint_record)
                .transpose()
        })
    }

    fn list_endpoints_all(&self) -> CacheNetworkFuture<'_, Vec<PodEndpointRecord>> {
        Box::pin(async move {
            self.backend
                .list_endpoints_all()
                .await
                .map_err(cache_error)?
                .into_iter()
                .map(endpoint_record)
                .collect()
        })
    }

    fn list_endpoints_for_node(
        &self,
        node_name: NodeKey,
    ) -> CacheNetworkFuture<'_, Vec<PodEndpointRecord>> {
        Box::pin(async move {
            self.backend
                .list_endpoints_for_node(node_name.as_str())
                .await
                .map_err(cache_error)?
                .into_iter()
                .map(endpoint_record)
                .collect()
        })
    }
}

struct EndpointStoreEventSubscription {
    inner: futures::stream::BoxStream<'static, Result<PodEndpointStoreEvent, CacheNetworkError>>,
}

impl PodEndpointStoreEventSubscription for EndpointStoreEventSubscription {
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<PodEndpointStoreEvent, CacheNetworkError>>> {
        futures::Stream::poll_next(std::pin::Pin::new(&mut self.get_mut().inner), context)
    }
}

impl PodEndpointStoreEventSource for NodeLocalNetworkAdapter {
    fn subscribe_endpoint_events(&self) -> CacheNetworkFuture<'_, PodEndpointStoreEventStream> {
        Box::pin(async move {
            use futures::StreamExt;

            let (rows, receiver) = self
                .backend
                .subscribe_pod_endpoints_with_snapshot()
                .await
                .map_err(cache_error)?;
            let snapshot = rows
                .into_iter()
                .map(endpoint_record)
                .collect::<Result<Vec<_>, _>>()?;
            let backend = self.backend.clone();
            let stream = futures::stream::unfold(
                (Some(snapshot), Some(receiver), backend),
                |(initial, receiver, backend)| async move {
                    if let Some(initial) = initial {
                        return Some((
                            Ok(PodEndpointStoreEvent::Resync(initial)),
                            (None, receiver, backend),
                        ));
                    }
                    let mut receiver = receiver?;
                    match receiver.recv().await {
                        Ok(crate::datastore::PodEndpointEvent::Upsert(row)) => Some((
                            endpoint_record(row).map(PodEndpointStoreEvent::Upsert),
                            (None, Some(receiver), backend),
                        )),
                        Ok(crate::datastore::PodEndpointEvent::Delete { pod_ip, .. }) => Some((
                            Ok(PodEndpointStoreEvent::Delete { pod_ip }),
                            (None, Some(receiver), backend),
                        )),
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            match backend.subscribe_pod_endpoints_with_snapshot().await {
                                Ok((rows, fresh_receiver)) => {
                                    let snapshot = rows
                                        .into_iter()
                                        .map(endpoint_record)
                                        .collect::<Result<Vec<_>, _>>();
                                    Some((
                                        snapshot.map(PodEndpointStoreEvent::Resync),
                                        (None, Some(fresh_receiver), backend),
                                    ))
                                }
                                Err(error) => {
                                    Some((Err(cache_error(error)), (None, None, backend)))
                                }
                            }
                        }
                    }
                },
            )
            .boxed();
            Ok(Box::pin(EndpointStoreEventSubscription { inner: stream })
                as PodEndpointStoreEventStream)
        })
    }
}

impl PodRuntimeStore for NodeLocalNetworkAdapter {
    fn admit_pod_runtime(&self, admission: PodRuntimeAdmission) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async move {
            let (pod, node_name) = admission.into_parts();
            self.backend
                .admit_pod_runtime(&pod.uid, &pod.namespace, &pod.name, &node_name)
                .await
                .map_err(runtime_error)
        })
    }

    fn record_sandbox(&self, sandbox: PodRuntimeSandbox) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async move {
            let (pod_uid, sandbox_id) = sandbox.into_parts();
            self.backend
                .record_sandbox(&pod_uid, &sandbox_id)
                .await
                .map_err(runtime_error)
        })
    }

    fn record_cgroup(&self, cgroup: PodRuntimeCgroup) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async move {
            let (pod_uid, cgroup_path) = cgroup.into_parts();
            self.backend
                .record_cgroup(&pod_uid, &cgroup_path)
                .await
                .map_err(runtime_error)
        })
    }

    fn delete_pod_runtime_for_uid(&self, pod_uid: RuntimePodUid) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async move {
            self.backend
                .delete_pod_runtime_for_uid(pod_uid.as_str())
                .await
                .map_err(runtime_error)
        })
    }

    fn get_pod_runtime(
        &self,
        pod_uid: RuntimePodUid,
    ) -> RuntimeWorkFuture<'_, Option<PodRuntimeRecord>> {
        Box::pin(async move {
            self.backend
                .get_pod_runtime(pod_uid.as_str())
                .await
                .map_err(runtime_error)?
                .map(runtime_record)
                .transpose()
        })
    }

    fn list_pod_runtime(&self) -> RuntimeWorkFuture<'_, Vec<PodRuntimeRecord>> {
        Box::pin(async move {
            self.backend
                .list_pod_runtime()
                .await
                .map_err(runtime_error)?
                .into_iter()
                .map(runtime_record)
                .collect()
        })
    }

    fn list_pod_runtime_by_namespace(
        &self,
        namespace: RuntimeNamespace,
    ) -> RuntimeWorkFuture<'_, Vec<PodRuntimeRecord>> {
        Box::pin(async move {
            self.backend
                .list_pod_runtime_by_namespace(namespace.as_str())
                .await
                .map_err(runtime_error)?
                .into_iter()
                .map(runtime_record)
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use klights_node_store::{
        CacheNetworkError, PodIpamStore, PodNetworkAllocationRequest, PodNetworkCache,
        PodRuntimeAdmission, PodRuntimeSandbox, PodRuntimeStore, PodUidKey, SandboxKey,
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
        let adapter = NodeLocalNetworkAdapter::new(backend);
        let pod = klights_types::PodIdentity::new("default", "pod-a", "uid-a");
        adapter
            .admit_pod_runtime(PodRuntimeAdmission::try_new(pod.clone(), "node-a").unwrap())
            .await
            .unwrap();
        adapter
            .record_sandbox(PodRuntimeSandbox::try_new("uid-a", "sandbox-a").unwrap())
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
        assert_eq!(
            adapter
                .list_pod_runtime()
                .await
                .unwrap()
                .first()
                .and_then(|record| record.sandbox_id()),
            Some("sandbox-a")
        );
    }

    async fn adapter_for_identity_test(
        diagnostic_name: &'static str,
    ) -> std::sync::Arc<NodeLocalNetworkAdapter> {
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let backend = crate::datastore::node_local::selector::open_node_local(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            diagnostic_name,
        )
        .await
        .unwrap();
        NodeLocalNetworkAdapter::new(backend)
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

        let adopted = sqlite
            .get_network_assignment_for_sandbox("sandbox-legacy")
            .await
            .unwrap()
            .expect("legacy allocation remains present");
        assert_eq!((adopted.subnet_base_int, adopted.subnet_size), (base, 256));
    }

    #[tokio::test]
    async fn real_adapter_preserves_typed_exhaustion_and_exact_allocation_identity() {
        let adapter = adapter_for_identity_test("sqlite:focused-network-identity").await;
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
        let adapter = adapter_for_identity_test("sqlite:focused-network-race").await;
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
