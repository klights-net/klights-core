use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use klights_node_store::{
    CacheNetworkError, CacheNetworkFuture, EndpointDeleteOutcome, EndpointUpsertOutcome, NodeKey,
    PodEndpointMode, PodEndpointRecord, PodEndpointStore, PodIpamStore, PodNetworkAllocation,
    PodNetworkAllocationRequest, PodNetworkAssignmentSnapshot, PodNetworkCache, PodNetworkEndpoint,
    PodUidKey, SandboxKey,
};
use klights_types::PodIdentity;

struct EmptyCacheNetworkStore;

impl PodNetworkCache for EmptyCacheNetworkStore {
    fn get_network_for_uid(
        &self,
        _pod_uid: PodUidKey,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        Box::pin(async { Ok(None) })
    }

    fn get_network_for_pod(
        &self,
        _pod: PodIdentity,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        Box::pin(async { Ok(None) })
    }

    fn get_network_for_sandbox(
        &self,
        _sandbox_id: SandboxKey,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        Box::pin(async { Ok(None) })
    }

    fn get_network_for_assignment(
        &self,
        _sandbox_id: SandboxKey,
        _pod: PodIdentity,
    ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
        Box::pin(async { Ok(None) })
    }

    fn delete_network_for_sandbox(&self, _sandbox_id: SandboxKey) -> CacheNetworkFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn delete_network_if_matches(
        &self,
        _request: PodNetworkAllocationRequest,
    ) -> CacheNetworkFuture<'_, bool> {
        Box::pin(async { Ok(false) })
    }

    fn list_network_assignments(
        &self,
    ) -> CacheNetworkFuture<'_, Vec<PodNetworkAssignmentSnapshot>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

impl PodIpamStore for EmptyCacheNetworkStore {
    fn reserve_ip_and_insert_network(
        &self,
        _request: PodNetworkAllocationRequest,
    ) -> CacheNetworkFuture<'_, PodNetworkAllocation> {
        Box::pin(async {
            PodNetworkAllocation::try_new("10.42.0.2", u32::from(Ipv4Addr::new(10, 42, 0, 2)))
        })
    }
}

impl PodEndpointStore for EmptyCacheNetworkStore {
    fn upsert_endpoint(
        &self,
        record: PodEndpointRecord,
    ) -> CacheNetworkFuture<'_, EndpointUpsertOutcome> {
        Box::pin(async { Ok(EndpointUpsertOutcome::new(None, record)) })
    }

    fn delete_endpoint_for_uid(
        &self,
        _pod_uid: PodUidKey,
    ) -> CacheNetworkFuture<'_, EndpointDeleteOutcome> {
        Box::pin(async { Ok(EndpointDeleteOutcome::new(None)) })
    }

    fn get_endpoint_by_pod_ip(
        &self,
        _pod_ip: Ipv4Addr,
    ) -> CacheNetworkFuture<'_, Option<PodEndpointRecord>> {
        Box::pin(async { Ok(None) })
    }

    fn list_endpoints_all(&self) -> CacheNetworkFuture<'_, Vec<PodEndpointRecord>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_endpoints_for_node(
        &self,
        _node_name: NodeKey,
    ) -> CacheNetworkFuture<'_, Vec<PodEndpointRecord>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

fn assert_network_cache_object_safe(_: &dyn PodNetworkCache) {}
fn assert_ipam_object_safe(_: &dyn PodIpamStore) {}
fn assert_endpoint_store_object_safe(_: &dyn PodEndpointStore) {}

#[test]
fn cache_network_capabilities_are_independently_object_safe() {
    let store = EmptyCacheNetworkStore;
    assert_network_cache_object_safe(&store);
    assert_ipam_object_safe(&store);
    assert_endpoint_store_object_safe(&store);
}

#[test]
fn allocation_request_preserves_identity_subnet_sandbox_and_link_state() {
    let pod = PodIdentity {
        namespace: "tenant/raw".to_string(),
        name: "pod.raw".to_string(),
        uid: "uid/raw".to_string(),
    };
    let subnet_base = u32::from(Ipv4Addr::new(10, 42, 7, 0));
    let request = PodNetworkAllocationRequest::try_new(
        "sandbox/raw",
        pod.clone(),
        subnet_base,
        256,
        "veth.raw",
        "/proc/raw/netns",
    )
    .unwrap();

    assert_eq!(request.sandbox_id(), "sandbox/raw");
    assert_eq!(request.pod(), &pod);
    assert_eq!(request.subnet_base_int(), subnet_base);
    assert_eq!(request.subnet_size(), 256);
    assert_eq!(request.veth_host(), "veth.raw");
    assert_eq!(request.netns_path(), "/proc/raw/netns");
    assert_eq!(
        request.into_parts(),
        (
            "sandbox/raw".to_string(),
            pod,
            subnet_base,
            256,
            "veth.raw".to_string(),
            "/proc/raw/netns".to_string(),
        )
    );
}

#[test]
fn allocation_request_rejects_invalid_identity_link_and_subnet_without_normalizing() {
    let valid_pod = PodIdentity {
        namespace: "default".to_string(),
        name: "pod".to_string(),
        uid: "uid".to_string(),
    };
    let cases = [
        (
            "sandbox_id",
            "",
            valid_pod.clone(),
            100_u32,
            16,
            "veth",
            "/netns",
        ),
        (
            "pod.namespace",
            "sandbox",
            PodIdentity {
                namespace: String::new(),
                ..valid_pod.clone()
            },
            100,
            16,
            "veth",
            "/netns",
        ),
        (
            "veth_host",
            "sandbox",
            valid_pod.clone(),
            100,
            16,
            "",
            "/netns",
        ),
        (
            "netns_path",
            "sandbox",
            valid_pod.clone(),
            100,
            16,
            "veth",
            "",
        ),
        (
            "subnet_size",
            "sandbox",
            valid_pod.clone(),
            100,
            3,
            "veth",
            "/netns",
        ),
        (
            "subnet",
            "sandbox",
            valid_pod,
            u32::MAX - 1,
            8,
            "veth",
            "/netns",
        ),
    ];

    for (field, sandbox, pod, base, size, veth, netns) in cases {
        assert!(matches!(
            PodNetworkAllocationRequest::try_new(sandbox, pod, base, size, veth, netns),
            Err(CacheNetworkError::InvalidInput { field: actual, .. }) if actual == field
        ));
    }
}

#[test]
fn assignment_snapshot_preserves_exact_cas_identity_and_confines_legacy_zero_subnet() {
    let pod = PodIdentity::new("default", "pod-a", "uid-a");
    let base = u32::from(Ipv4Addr::new(10, 42, 7, 0));
    let request = PodNetworkAllocationRequest::try_new(
        "sandbox-a",
        pod.clone(),
        base,
        256,
        "veth-a",
        "/run/netns/a",
    )
    .unwrap();
    let allocation =
        PodNetworkAllocation::try_new("10.42.7.2", base + 2).expect("valid allocation");
    let snapshot =
        PodNetworkAssignmentSnapshot::try_new(request.clone(), allocation.clone()).unwrap();
    assert_eq!(snapshot.request(), &request);
    assert_eq!(snapshot.allocation(), &allocation);

    let legacy = PodNetworkAllocationRequest::try_from_persisted(
        "sandbox-legacy",
        pod,
        0,
        0,
        "veth-legacy",
        "/run/netns/legacy",
    )
    .unwrap();
    let legacy_allocation =
        PodNetworkAllocation::try_new("10.42.7.2", base + 2).expect("valid persisted IP");
    assert!(
        PodNetworkAssignmentSnapshot::try_new(legacy, legacy_allocation).is_ok(),
        "legacy 0/0 rows must remain representable for exact conditional cleanup"
    );

    assert!(matches!(
        PodNetworkAllocationRequest::try_from_persisted(
            "sandbox-invalid",
            PodIdentity::new("default", "pod", "uid"),
            base,
            0,
            "veth",
            "/run/netns/pod",
        ),
        Err(CacheNetworkError::InvalidInput {
            field: "subnet_size",
            ..
        })
    ));
    assert!(matches!(
        PodNetworkAssignmentSnapshot::try_new(
            request,
            PodNetworkAllocation::try_new("10.42.8.2", base + 258).unwrap(),
        ),
        Err(CacheNetworkError::InvalidInput {
            field: "allocation.ip_int",
            ..
        })
    ));
}

#[test]
fn allocation_and_cached_endpoint_preserve_ip_and_link_values_exactly() {
    let ip = Ipv4Addr::new(10, 42, 7, 19);
    let allocation = PodNetworkAllocation::try_new(ip.to_string(), u32::from(ip)).unwrap();
    assert_eq!(allocation.ip_addr(), "10.42.7.19");
    assert_eq!(allocation.ip_int(), u32::from(ip));

    let cached = PodNetworkEndpoint::try_new("10.42.7.19", "veth/raw", "/netns/raw").unwrap();
    assert_eq!(cached.ip_addr(), "10.42.7.19");
    assert_eq!(cached.veth_host(), "veth/raw");
    assert_eq!(cached.netns_path(), "/netns/raw");
    assert_eq!(
        cached.into_parts(),
        (
            "10.42.7.19".to_string(),
            "veth/raw".to_string(),
            "/netns/raw".to_string()
        )
    );

    assert!(matches!(
        PodNetworkAllocation::try_new("not-an-ip", 7),
        Err(CacheNetworkError::InvalidInput {
            field: "ip_addr",
            ..
        })
    ));
    assert!(matches!(
        PodNetworkAllocation::try_new("10.42.7.19", 7),
        Err(CacheNetworkError::InvalidInput {
            field: "ip_int",
            ..
        })
    ));
}

fn endpoint(mode: PodEndpointMode) -> PodEndpointRecord {
    PodEndpointRecord::try_new(
        PodIdentity {
            namespace: "tenant/raw".to_string(),
            name: "pod.raw".to_string(),
            uid: "uid/raw".to_string(),
        },
        "node/raw",
        mode,
        Ipv4Addr::new(10, 42, 7, 19),
        Ipv4Addr::new(192, 0, 2, 10),
        Some(30_001),
        Some(30_002),
        i64::MAX - 7,
        i64::MAX - 3,
    )
    .unwrap()
}

#[test]
fn endpoint_record_preserves_mode_ports_generation_and_identity_exactly() {
    for mode in [PodEndpointMode::EncryptedDirect, PodEndpointMode::Hostport] {
        let record = endpoint(mode);
        assert_eq!(record.pod().namespace, "tenant/raw");
        assert_eq!(record.pod().name, "pod.raw");
        assert_eq!(record.pod().uid, "uid/raw");
        assert_eq!(record.node_name(), "node/raw");
        assert_eq!(record.mode(), mode);
        assert_eq!(record.pod_ip(), Ipv4Addr::new(10, 42, 7, 19));
        assert_eq!(record.node_ip(), Ipv4Addr::new(192, 0, 2, 10));
        assert_eq!(record.host_port_tcp(), Some(30_001));
        assert_eq!(record.host_port_udp(), Some(30_002));
        assert_eq!(record.generation(), i64::MAX - 7);
        assert_eq!(record.updated_at_ms(), i64::MAX - 3);
    }
}

#[test]
fn cache_network_errors_keep_retry_corruption_timeout_and_cancellation_distinct() {
    let errors = [
        CacheNetworkError::persistence_failed("disk/full"),
        CacheNetworkError::corrupt_data("bad/mode"),
        CacheNetworkError::retryable("busy"),
        CacheNetworkError::AddressExhausted {
            subnet_base_int: 100,
            subnet_size: 4,
        },
        CacheNetworkError::IdentityConflict {
            sandbox_id: "sandbox-a".into(),
        },
        CacheNetworkError::Timeout,
        CacheNetworkError::Cancelled,
    ];

    assert!(matches!(
        errors[0],
        CacheNetworkError::PersistenceFailed { .. }
    ));
    assert!(matches!(errors[1], CacheNetworkError::CorruptData { .. }));
    assert!(matches!(errors[2], CacheNetworkError::Retryable { .. }));
    assert_eq!(
        errors[5].to_string(),
        "node cache/network persistence timed out"
    );
    assert_eq!(
        errors[6].to_string(),
        "node cache/network persistence was cancelled"
    );
}

#[test]
fn endpoint_rows_reject_hostile_persisted_values_and_preserve_unspecified_node_ip() {
    let pod = PodIdentity {
        namespace: "ns".into(),
        name: "pod".into(),
        uid: "uid".into(),
    };
    let make = |port, generation, updated| {
        PodEndpointRecord::try_from_persisted(
            pod.clone(),
            "node",
            PodEndpointMode::Hostport,
            Ipv4Addr::new(10, 0, 0, 2),
            Ipv4Addr::UNSPECIFIED,
            port,
            None,
            generation,
            updated,
        )
    };
    assert_eq!(
        make(Some(65_535), 0, 0).unwrap().node_ip(),
        Ipv4Addr::UNSPECIFIED
    );
    for (port, generation, updated, field) in [
        (Some(-1), 0, 0, "host_port_tcp"),
        (Some(65_536), 0, 0, "host_port_tcp"),
        (None, -1, 0, "generation"),
        (None, 0, -1, "updated_at_ms"),
    ] {
        assert!(matches!(
            make(port, generation, updated),
            Err(CacheNetworkError::InvalidInput { field: actual, .. }) if actual == field
        ));
    }
}

#[test]
fn endpoint_mutation_outcomes_preserve_atomic_previous_current_and_removed_facts() {
    let old = endpoint(PodEndpointMode::EncryptedDirect);
    let mut new_parts = old.clone().into_parts();
    new_parts.3 = Ipv4Addr::new(10, 42, 7, 20);
    new_parts.7 += 1;
    let current = PodEndpointRecord::try_new(
        new_parts.0,
        new_parts.1,
        new_parts.2,
        new_parts.3,
        new_parts.4,
        new_parts.5,
        new_parts.6,
        new_parts.7,
        new_parts.8,
    )
    .unwrap();
    let outcome = EndpointUpsertOutcome::new(Some(old.clone()), current.clone());
    assert_eq!(outcome.previous(), Some(&old));
    assert_eq!(outcome.current(), &current);
    let unchanged = EndpointUpsertOutcome::new(Some(current.clone()), current.clone());
    assert_eq!(unchanged.previous(), Some(unchanged.current()));
    assert_eq!(
        EndpointDeleteOutcome::new(Some(current.clone())).into_removed(),
        Some(current)
    );
    assert_eq!(EndpointDeleteOutcome::new(None).into_removed(), None);
}

fn ready<T>(mut future: CacheNetworkFuture<'_, T>) -> Result<T, CacheNetworkError> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match Future::poll(future.as_mut(), &mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("reference persistence future unexpectedly pending"),
    }
}

#[derive(Default)]
struct ModelIpam {
    state: Mutex<ModelIpamState>,
}

#[derive(Default)]
struct ModelIpamState {
    by_sandbox: HashMap<String, (PodNetworkAllocationRequest, PodNetworkAllocation)>,
    used: BTreeSet<u32>,
    next: HashMap<(u32, u32), u32>,
}

impl PodIpamStore for ModelIpam {
    fn reserve_ip_and_insert_network(
        &self,
        request: PodNetworkAllocationRequest,
    ) -> CacheNetworkFuture<'_, PodNetworkAllocation> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            if let Some((existing_request, allocation)) = state.by_sandbox.get(request.sandbox_id())
            {
                return if existing_request == &request {
                    Ok(allocation.clone())
                } else {
                    Err(CacheNetworkError::IdentityConflict {
                        sandbox_id: request.sandbox_id().to_string(),
                    })
                };
            }
            let base = request.subnet_base_int();
            let size = request.subnet_size();
            let first = base + 2;
            let end_exclusive = base + size - 1;
            let key = (base, size);
            let start = state.next.get(&key).copied().unwrap_or(first);
            let candidate = (start..end_exclusive)
                .chain(first..start)
                .find(|candidate| !state.used.contains(candidate))
                .ok_or(CacheNetworkError::AddressExhausted {
                    subnet_base_int: base,
                    subnet_size: size,
                })?;
            state.used.insert(candidate);
            state.next.insert(
                key,
                if candidate + 1 == end_exclusive {
                    first
                } else {
                    candidate + 1
                },
            );
            let allocation =
                PodNetworkAllocation::try_new(Ipv4Addr::from(candidate).to_string(), candidate)?;
            state.by_sandbox.insert(
                request.sandbox_id().to_string(),
                (request, allocation.clone()),
            );
            Ok(allocation)
        })
    }
}

fn request(sandbox: &str, uid: &str, base: u32, size: u32) -> PodNetworkAllocationRequest {
    PodNetworkAllocationRequest::try_new(
        sandbox,
        PodIdentity {
            namespace: "ns".into(),
            name: "pod".into(),
            uid: uid.into(),
        },
        base,
        size,
        format!("veth-{uid}"),
        format!("/netns/{uid}"),
    )
    .unwrap()
}

#[test]
fn reference_ipam_is_atomic_idempotent_rotating_and_typed_on_conflict_or_exhaustion() {
    let store = Arc::new(ModelIpam::default());
    let base = u32::from(Ipv4Addr::new(10, 50, 0, 0));
    let a = ready(store.reserve_ip_and_insert_network(request("a", "a", base, 5))).unwrap();
    assert_eq!(a.ip_int(), base + 2);
    assert_eq!(
        ready(store.reserve_ip_and_insert_network(request("a", "a", base, 5))).unwrap(),
        a
    );
    assert!(matches!(
        ready(store.reserve_ip_and_insert_network(request("a", "other", base, 5))),
        Err(CacheNetworkError::IdentityConflict { .. })
    ));

    let left = Arc::clone(&store);
    let right = Arc::clone(&store);
    let one = std::thread::spawn(move || {
        ready(left.reserve_ip_and_insert_network(request("b", "b", base, 5))).unwrap()
    });
    let two = std::thread::spawn(move || {
        ready(right.reserve_ip_and_insert_network(request("c", "c", base, 5)))
    });
    let b = one.join().unwrap();
    assert_ne!(a, b);
    assert!(matches!(
        two.join().unwrap(),
        Err(CacheNetworkError::AddressExhausted { .. })
    ));
}

#[test]
fn focused_lookup_keys_reject_empty_values() {
    assert!(SandboxKey::try_new("").is_err());
    assert!(PodUidKey::try_new("").is_err());
    assert!(NodeKey::try_new("").is_err());
}
