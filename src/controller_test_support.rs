//! Shared test utilities for controller wrapper tests.

use std::sync::Arc;

pub(crate) fn inject_resource_version(
    data: impl Into<Arc<serde_json::Value>>,
    resource_version: i64,
) -> serde_json::Value {
    let mut data = Arc::unwrap_or_clone(data.into());
    if let Some(metadata) = data
        .get_mut("metadata")
        .and_then(serde_json::Value::as_object_mut)
    {
        metadata.insert(
            "resourceVersion".to_string(),
            serde_json::Value::String(resource_version.to_string()),
        );
    }
    data
}

pub(crate) fn test_reconcile_context<'a>(
    coordination: &'a klights_controllers::ControllerCoordination,
    node_name: &'a str,
) -> klights_controllers::ControllerReconcileContext<'a> {
    klights_controllers::ControllerReconcileContext::at(coordination, node_name, chrono::Utc::now())
}

pub(crate) fn controller_store_for_test(
    db: &crate::datastore::sqlite::Datastore,
) -> crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort {
    crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new(
        Arc::new(db.clone()),
    )
}

pub(crate) fn runtime_dependencies_for_test(
    db: &crate::datastore::sqlite::Datastore,
    node_name: &str,
) -> klights_controllers::ControllerRuntimeDependencies {
    let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
    let repository = pod_repository_for_test(db);
    let leader = Arc::new(
        crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerLeaderPort::new(db_handle.clone()),
    );
    let pods = Arc::new(
        crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerPodPort::new_for_test(repository.clone()),
    );
    let non_pod_finalization: Arc<dyn klights_reconcile_api::GcNonPodFinalizationPort> = Arc::new(
        crate::bootstrap::controller_adapters::gc_delete_adapter::GcNonPodFinalizationAdapter::new(
            db_handle,
        ),
    );
    let services = Arc::new(crate::networking::test_support::MockServiceRouter::default());
    klights_controllers::ControllerRuntimeDependencies {
        wall_time: chrono::Utc::now,
        resource_query: leader.clone(),
        deployment_store: leader.clone(),
        replicaset_store: leader.clone(),
        statefulset_store: leader.clone(),
        daemonset_store: leader.clone(),
        job_store: leader.clone(),
        service_store: leader.clone(),
        pvc_store: leader.clone(),
        pdb_store: leader.clone(),
        replicationcontroller_store: leader.clone(),
        apiservice_store: leader.clone(),
        csr_status_store: leader,
        pod_query: repository.clone(),
        pdb_pod_reader: repository.clone(),
        deployment_pod_reader: repository.clone(),
        deployment_pod_mutation: pods.clone(),
        replicaset_pod_mutation: pods.clone(),
        statefulset_pod_mutation: pods.clone(),
        daemonset_pod_mutation: pods.clone(),
        job_pod_mutation: pods.clone(),
        replicationcontroller_pod_mutation: pods,
        pod_delete_sink: repository,
        reconcile: Arc::new(
            crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerReconcilePort::new(
                non_pod_finalization,
            ),
        ),
        network: Arc::new(
            crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerNetworkPort::new(services),
        ),
        effects: Arc::new(
            crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerEffectPort::new(
                crate::kubelet::file_blocking::test_file_process_executor(),
                crate::KlightsConfig::test_default()
                    .data_root
                    .join("local-path-provisioner"),
            ),
        ),
        coordination: Arc::new(klights_controllers::ControllerCoordination::new()),
        node_name: Arc::from(node_name),
    }
}

struct NoopHpaReconcilePort;

#[async_trait::async_trait]
impl klights_controllers::hpa::HpaReconcilePort for NoopHpaReconcilePort {
    async fn reconcile(
        &self,
        _resource: &serde_json::Value,
        _reconcile_time: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

pub(crate) fn dispatcher_for_test(
    db: &crate::datastore::sqlite::Datastore,
    service_ipam: Arc<klights_controllers::service::ServiceIpam>,
) -> Arc<klights_controllers::ControllerDispatcher> {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    Arc::new(klights_controllers::ControllerDispatcher::new_complete(
        service_ipam,
        Arc::new(klights_controllers::service::NodePortAllocator::new()),
        supervisor,
        None,
        Arc::new(klights_controllers::hpa::HpaController::new(Arc::new(
            NoopHpaReconcilePort,
        ))),
        runtime_dependencies_for_test(db, "test-node"),
        deterministic_controller_identity(),
    ))
}

pub(crate) fn queue_only_dispatcher_for_test(
    service_ipam: Arc<klights_controllers::service::ServiceIpam>,
) -> klights_controllers::ControllerDispatcher {
    klights_controllers::ControllerDispatcher::with_task_supervisor(
        service_ipam,
        Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )),
    )
}

pub(crate) fn default_queue_only_dispatcher_for_test() -> klights_controllers::ControllerDispatcher
{
    queue_only_dispatcher_for_test(Arc::new(klights_controllers::service::ServiceIpam::new(
        "10.43.128.0/17",
    )))
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

/// Build the leader/deferred `PodRepository` shape used by multinode
/// controller tests, where metadata writes go through the outbox before the
/// local store observes them.
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
