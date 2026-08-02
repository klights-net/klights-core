//! Shared test utilities for controller wrapper tests.

use std::sync::Arc;

pub(crate) fn controller_store_for_test(
    db: &crate::datastore::sqlite::Datastore,
) -> crate::controller_runtime_adapter::RootControllerLeaderPort {
    crate::controller_runtime_adapter::RootControllerLeaderPort::new(Arc::new(db.clone()))
}

struct TestGcNonPodFinalizationPort;

impl klights_reconcile_api::GcNonPodFinalizationPort for TestGcNonPodFinalizationPort {
    fn finalize_non_pod(
        &self,
        _request: klights_reconcile_api::GcNonPodFinalizationRequest,
    ) -> klights_reconcile_api::GcNonPodFinalizationFuture<'_> {
        Box::pin(async { Ok(klights_reconcile_api::GcNonPodFinalizationOutcome::Gone) })
    }
}

pub fn non_pod_finalization_port_for_test()
-> &'static dyn klights_reconcile_api::GcNonPodFinalizationPort {
    static PORT: TestGcNonPodFinalizationPort = TestGcNonPodFinalizationPort;
    &PORT
}

/// Store a resource in the DB and return it with resourceVersion injected,
/// matching how the API server passes resources to reconcile.
pub async fn store_and_prepare(
    db: &crate::datastore::sqlite::Datastore,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    data: serde_json::Value,
) -> serde_json::Value {
    let created = db
        .create_resource(
            api_version,
            kind,
            namespace.map(String::from).as_deref(),
            name,
            data,
        )
        .await
        .unwrap();
    crate::controllers::inject_resource_version(created.data, created.resource_version)
}

/// Build a `PodRepository` over the supplied in-memory `Datastore` for use
/// in controller unit tests that exercise `reconcile_deployment` /
/// `reconcile_replicaset` without going through the full dispatcher.
///
/// Mirrors the wiring in `api::test_support::build_test_app_state` —
/// returns the same kind of repository the production dispatcher would
/// hand to these controllers.
pub fn pod_repository_for_test(
    db: &crate::datastore::sqlite::Datastore,
) -> Arc<crate::kubelet::pod_repository::PodRepository> {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
    let side_effects = Arc::new(klights_controllers::side_effects::SideEffectRegistry::new());
    let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
    Arc::new(crate::kubelet::pod_repository::PodRepository::new(
        db_handle,
        supervisor,
        side_effects,
        metrics,
    ))
}

fn deterministic_generated_name(prefix: &str, value: u64) -> String {
    const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    const SUFFIX_SPACE: u64 = 36_u64.pow(5);
    // Five Kubernetes name characters have a finite namespace. Exhaustion is
    // the only point at which this deterministic fake wraps.
    let mut remaining = value % SUFFIX_SPACE;
    let mut suffix = [b'0'; 5];
    for slot in suffix.iter_mut().rev() {
        *slot = ALPHABET[(remaining % 36) as usize];
        remaining /= 36;
    }
    format!(
        "{prefix}{}",
        std::str::from_utf8(&suffix).expect("ASCII suffix")
    )
}

fn deterministic_uuid_v4(value: u64) -> String {
    let first = ((value & 0x000f_ffff) << 12) | ((value >> 20) & 0x0fff);
    let second = (value >> 32) & 0xffff;
    let third = 0x4000 | ((value >> 48) & 0x0fff);
    let fourth = 0x8000 | ((value >> 60) & 0x000f);
    format!("{first:08x}-{second:04x}-{third:04x}-{fourth:04x}-000000000000")
}

#[derive(Debug)]
struct DeterministicControllerIdentityGenerator {
    sequence: Arc<std::sync::atomic::AtomicU64>,
}

impl klights_controllers::ControllerIdentityGenerator for DeterministicControllerIdentityGenerator {
    fn generate_name(&self, prefix: &str) -> String {
        let value = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        deterministic_generated_name(prefix, value)
    }

    fn new_uid(&self) -> String {
        let value = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        deterministic_uuid_v4(value)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ControllerIdentityTestGraph {
    sequence: Arc<std::sync::atomic::AtomicU64>,
}

impl ControllerIdentityTestGraph {
    pub(crate) fn with_start(value: u64) -> Self {
        Self {
            sequence: Arc::new(std::sync::atomic::AtomicU64::new(value)),
        }
    }

    pub(crate) fn identity(&self) -> Arc<dyn klights_controllers::ControllerIdentityGenerator> {
        Arc::new(DeterministicControllerIdentityGenerator {
            sequence: self.sequence.clone(),
        })
    }
}

pub(crate) fn deterministic_controller_identity()
-> Arc<dyn klights_controllers::ControllerIdentityGenerator> {
    ControllerIdentityTestGraph::default().identity()
}

pub(crate) struct ScriptedControllerIdentityGenerator {
    uids: std::sync::Mutex<std::collections::VecDeque<String>>,
    uid_calls: std::sync::atomic::AtomicUsize,
}

impl ScriptedControllerIdentityGenerator {
    pub(crate) fn with_uids(uids: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            uids: std::sync::Mutex::new(uids.into_iter().map(str::to_string).collect()),
            uid_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(crate) fn uid_calls(&self) -> usize {
        self.uid_calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl klights_controllers::ControllerIdentityGenerator for ScriptedControllerIdentityGenerator {
    fn generate_name(&self, _prefix: &str) -> String {
        panic!("scripted UID identity must not generate names")
    }

    fn new_uid(&self) -> String {
        self.uid_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.uids
            .lock()
            .expect("scripted identity UID lock")
            .pop_front()
            .expect("scripted identity exhausted")
    }
}

#[test]
fn deterministic_controller_identity_advances_names_and_uids() {
    let graph = ControllerIdentityTestGraph::default();
    let identity = graph.identity();

    assert_eq!(identity.generate_name("pod-"), "pod-00000");
    assert_eq!(identity.generate_name("pod-"), "pod-00001");
    let first_uid = identity.new_uid();
    let second_uid = identity.new_uid();
    assert_eq!(first_uid, "00002000-0000-4000-8000-000000000000");
    assert_eq!(second_uid, "00003000-0000-4000-8000-000000000000");
    assert_ne!(&first_uid[..5], &second_uid[..5]);
    assert_ne!(first_uid.split('-').next(), second_uid.split('-').next(),);
}

#[test]
fn deterministic_controller_identity_graphs_are_independent() {
    let first = ControllerIdentityTestGraph::default();
    let second = ControllerIdentityTestGraph::default();

    assert_eq!(first.identity().generate_name("pod-"), "pod-00000");
    assert_eq!(first.identity().generate_name("pod-"), "pod-00001");
    assert_eq!(second.identity().generate_name("pod-"), "pod-00000");
}

#[test]
fn deterministic_controller_identity_graphs_are_parallel_hermetic() {
    let outputs = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                let graph = ControllerIdentityTestGraph::default();
                let identity = graph.identity();
                (0..64)
                    .map(|_| identity.generate_name("pod-"))
                    .collect::<Vec<_>>()
            })
        })
        .map(|thread| thread.join().expect("identity test graph thread"))
        .collect::<Vec<_>>();

    assert!(outputs.windows(2).all(|pair| pair[0] == pair[1]));
}

#[test]
fn deterministic_controller_identity_remains_valid_at_large_counter_values() {
    let uid_graph = ControllerIdentityTestGraph::with_start(u64::MAX - 1);
    let uid_identity = uid_graph.identity();
    let first = uid_identity.new_uid();
    let second = uid_identity.new_uid();
    assert_ne!(first, second);
    for raw in [first, second] {
        let uid = uuid::Uuid::parse_str(&raw).expect("large deterministic UID");
        assert_eq!(uid.get_version(), Some(uuid::Version::Random));
        assert_eq!(uid.get_variant(), uuid::Variant::RFC4122);
    }

    let name_graph = ControllerIdentityTestGraph::with_start(u64::MAX - 1);
    let names = [
        name_graph.identity().generate_name("pod-"),
        name_graph.identity().generate_name("pod-"),
    ];
    assert_ne!(names[0], names[1]);
    for name in names {
        let suffix = name.strip_prefix("pod-").expect("generated prefix");
        assert_eq!(suffix.len(), 5);
        assert!(
            suffix
                .chars()
                .all(|character| { character.is_ascii_lowercase() || character.is_ascii_digit() })
        );
    }
}

pub(crate) async fn pod_repository_with_node_local_for_test(
    db: &crate::datastore::sqlite::Datastore,
) -> (
    Arc<crate::kubelet::pod_repository::PodRepository>,
    Arc<crate::datastore::node_local::NodeLocalStores>,
) {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let node_local =
        crate::kubelet::pod_repository::test_node_local_store(supervisor.clone()).await;
    let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
    let repository = crate::kubelet::pod_repository::PodRepository::build_parts(
        crate::kubelet::pod_repository::PodRepositoryBuildConfig {
            db: db_handle,
            pod_workqueue_store: Some(node_local.clone()),
            supervisor,
            side_effects: Arc::new(klights_controllers::side_effects::SideEffectRegistry::new()),
            metrics: klights_controllers::side_effects::SideEffectMetrics::new(),
            pod_network_cache: crate::kubelet::pod_repository::test_pod_network_cache(
                node_local.clone(),
            ),
            assignment_waiter: crate::kubelet::pod_repository::test_assignment_bus(),
            scheduling_mode: crate::kubelet::pod_repository::PodSchedulingMode::InlineSingleNode,
            outbox: None,
            cluster_api: None,
            controller_identity: deterministic_controller_identity(),
            scheduler_bind_gate: None,
        },
    )
    .repository;
    (Arc::new(repository), node_local)
}

/// Build the leader/deferred `PodRepository` shape used by multinode controller
/// wiring, where metadata writes go through the outbox before the local store
/// observes them.
pub async fn deferred_outbox_pod_repository_for_test(
    db: &crate::datastore::sqlite::Datastore,
) -> Arc<crate::kubelet::pod_repository::PodRepository> {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let metrics = klights_controllers::side_effects::SideEffectMetrics::new();
    let side_effects = Arc::new(klights_controllers::side_effects::SideEffectRegistry::new());
    let outbox = Arc::new(crate::outbox_test_support::test_outbox().await);
    let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
    Arc::new(
        crate::kubelet::pod_repository::PodRepository::new_with_scheduling_mode_and_outbox(
            db_handle,
            supervisor,
            side_effects,
            metrics,
            crate::pod_repository_composition::PodSchedulingMode::DeferredMultiNodeLeader,
            Some(outbox),
        ),
    )
}
