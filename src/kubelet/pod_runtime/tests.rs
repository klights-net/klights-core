#![cfg(test)]
#![allow(clippy::items_after_test_module)]

mod cri_recovery;
mod filesystem_volumes;
mod lifecycle_status;
mod network_hostports;
mod probes;
mod slot_retry;
mod worker_outbox;

use k8s_cri::v1::PodSandboxConfig;

use crate::kubelet::pod_cluster_runtime::{FakeCluster, FakeNode};
use crate::kubelet::pod_runtime::events::PodEventSink;
use crate::kubelet::pod_runtime::events::test_support::MockPodEventSink;
use crate::kubelet::pod_runtime::filesystem::PodFilesystem;
use crate::kubelet::pod_runtime::filesystem::test_support::MockPodFilesystem;
use crate::kubelet::pod_runtime::hooks::HookOutcome;
use crate::kubelet::pod_runtime::hostports::HostPortRuntime;
use crate::kubelet::pod_runtime::hostports::test_support::{MockHostPortOp, MockHostPortRuntime};
use crate::kubelet::pod_runtime::network::test_support::{MockNetworkOp, MockPodNetworkRuntime};
use crate::kubelet::pod_runtime::probes::ProbeRuntime;
use crate::kubelet::pod_runtime::probes::test_support::{MockProbeCall, MockProbeRuntime};
use crate::kubelet::pod_runtime::service::{
    PodDeletionFinalizeResult, PodOwnershipError, PodRuntimeKey, PodStartResult,
    RealPodRuntimeServiceDependencies,
};
use crate::kubelet::pod_runtime::service::{PodFinalizeStartupResult, PodRuntimeService};
use crate::kubelet::pod_runtime::store::{PodRuntimeStore, PodSlotAdmission};
use crate::kubelet::pod_runtime::volumes::PodVolumeRuntime;
use crate::kubelet::pod_runtime::volumes::test_support::MockPodVolumeRuntime;
use klights_kubelet::pod_deletion_finalizer::PodDeletionFinalizer;
use klights_kubelet::pod_env::EnvSourceReader;
use klights_kubelet::pod_lifecycle_core::message::PodLifecycleKey;
use klights_kubelet::pod_service_envs::ServiceEnvSource;
use klights_kubelet::runtime::test_support::{
    MockContainerControlOp, MockContainerRuntimeControl, MockCriOperation, MockCriRuntime,
    MockEnvSourceReader, MockPodDeletionFinalizer, MockPodHookRuntime, MockPodRuntimeService,
    MockPodRuntimeStore, MockPodSlotAdmission, MockRuntimeCall,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

fn kubelet_runtime_paths_for_test(
    namespace: &str,
) -> klights_kubelet::runtime_paths::KubeletRuntimePaths {
    klights_kubelet::runtime_paths::KubeletRuntimePaths::new(crate::paths::test_data_root_path(
        namespace,
    ))
    .expect("kubelet test runtime path must be absolute")
}

/// Canonical builder for the concrete root repository backing pod_runtime
/// test fixtures. E6: the 79 `test_create_pod` / `test_get_pod_for_uid` call
/// sites keep their textual shape through focused-port helpers on the parts
/// handoff.
#[async_trait::async_trait]
trait PodTestRepoExt {
    async fn test_create_pod(
        &self,
        namespace: &str,
        name: &str,
        _node_name: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource>;

    async fn test_get_pod_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>>;
}

struct PodRuntimeTestPorts {
    pod_query: Arc<dyn klights_pod_api::PodQuery>,
    pod_status_writer: Arc<dyn klights_kubelet::pod_repository::status::PodStatusWriter>,
    pod_network_assignment: Arc<dyn klights_kubelet::pod_repository::PodNetworkAssignmentQuery>,
    deletion_finalizer: Arc<dyn klights_kubelet::pod_deletion_finalizer::PodDeletionFinalizer>,
    test_api: Option<Arc<dyn klights_pod_api::PodApiMutation>>,
}

#[async_trait::async_trait]
impl PodTestRepoExt for PodRuntimeTestPorts {
    async fn test_create_pod(
        &self,
        namespace: &str,
        name: &str,
        _node_name: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        let created = self
            .test_api
            .as_ref()
            .expect("root Pod API test port")
            .create_pod(klights_pod_api::PodApiCreateRequest {
                namespace: namespace.to_string(),
                body,
                dry_run: false,
            })
            .await?;
        created
            .resource
            .ok_or_else(|| anyhow::anyhow!("test Pod {namespace}/{name} create returned dry-run"))
    }

    async fn test_get_pod_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.pod_query
            .get_pod(klights_pod_api::PodGetRequest::try_by_identity(
                klights_types::PodIdentity::new(namespace, name, uid),
            )?)
            .await
            .map_err(Into::into)
    }
}

/// Canonical builder for the focused root capabilities backing pod_runtime
/// test fixtures. This is the single place in this file that constructs the
/// shared in-memory scheduling config; every caller receives the decomposed
/// capability result instead of repeating this boilerplate.
/// `controller_identity` stays an explicit parameter (each caller still
/// names `deterministic_controller_identity()` itself) so this consolidation
/// does not change the frozen live call-site count tracked by
/// `source_guard_phase18_controller_test_support_ownership.py`.
fn build_test_pod_repository(
    db: crate::datastore::DatastoreHandle,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    node_local: Arc<crate::bootstrap::node_store::NodeLocalStores>,
    controller_identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
) -> PodRuntimeTestPorts {
    let (
        pod_query,
        _pod_snapshot,
        _pod_update,
        pod_status_writer,
        _pod_workqueue,
        pod_network_assignment,
        _pod_host_ip,
        _background,
        deletion_finalizer,
        _dirty_counter,
        _mutation_reconcile,
        _gc_delete,
        _eviction_admission,
        _namespace_bootstrap,
        _namespace_termination_queue,
        _pod_api,
        _pod_subresource,
        _pod_scheduling,
        _watch_source,
        _bound_finalization,
        _deferred_runtime,
        test_api,
        _test_subresource,
    ) = crate::bootstrap::pod_repository_composition::build_pod_repository_parts(
        crate::bootstrap::pod_repository_composition::PodRepositoryBuildConfig {
            db,
            pod_workqueue_store: Some(node_local.pod_workqueue()),
            supervisor,
            side_effects: Arc::new(klights_controllers::side_effects::SideEffectRegistry::new()),
            metrics: klights_controllers::side_effects::SideEffectMetrics::new(),
            pod_network_cache: crate::bootstrap::pod_repository_composition::test_pod_network_cache(
                node_local,
            ),
            assignment_waiter: crate::bootstrap::pod_repository_composition::test_assignment_bus(),
            scheduling_mode:
                crate::bootstrap::pod_repository_composition::PodSchedulingMode::InlineSingleNode,
            outbox: None,
            cluster_api: None,
            remote_delivery_required: false,
            controller_identity,
            scheduler_bind_gate: None,
        },
        None,
    );
    PodRuntimeTestPorts {
        pod_query,
        pod_status_writer,
        pod_network_assignment,
        deletion_finalizer,
        test_api,
    }
}

async fn node_local_runtime_store() -> Arc<crate::bootstrap::node_store::NodeLocalStores> {
    let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let executor = klights_node_datastore::open::open_with_opts(
        klights_node_datastore::open::in_memory_opts(),
        supervisor,
        "sqlite:pod-runtime-test",
    )
    .await
    .expect("open node-local runtime store");
    let backend = crate::bootstrap::node_store::NodeLocalStores::from_executor(executor)
        .expect("create node-local runtime store");
    Arc::new(backend)
}

async fn admit_runtime_key(store: &dyn klights_node_store::PodRuntimeStore, key: &PodRuntimeKey) {
    klights_node_store::PodRuntimeStore::admit_pod_runtime(
        store,
        klights_node_store::PodRuntimeAdmission::try_new(
            klights_types::PodIdentity::new(&key.namespace, &key.name, &key.uid),
            "node-1",
        )
        .expect("valid test runtime admission"),
    )
    .await
    .expect("admit test runtime row");
}

// --- Task 1.2: PodRuntimeKey identity and result types ---

// --- Task 1.3: PodRuntimeService trait ---

// --- Task 2.1: CriRuntime and ContainerRuntimeControl traits ---

// --- Task 2.2: SharedCriRuntime production adapter ---

use super::status_projection;
/// SharedCriRuntime clones the client per-call without wrapping in a Mutex.
/// This is a compile-time test: if Mutex were introduced, the type would
/// not satisfy the structural constraints checked here.
// --- Task 2.3: MockCriRuntime ---
use klights_kubelet::runtime::cri::CriRuntime;

// --- Task 3.1: PodNetworkRuntime trait and mock ---

use crate::kubelet::pod_runtime::network::PodNetworkRuntime;

// --- Task 3.2: PodRuntimeStore trait and mock ---

// --- Task 3.3: Filesystem and Volume runtime traits ---

#[cfg(unix)]
fn alternate_test_group(current_gid: u32) -> Option<u32> {
    unsafe {
        if libc::geteuid() == 0 {
            return Some(current_gid.saturating_add(1));
        }

        let group_count = libc::getgroups(0, std::ptr::null_mut());
        if group_count <= 0 {
            return None;
        }

        let mut groups = vec![0 as libc::gid_t; group_count as usize];
        if libc::getgroups(group_count, groups.as_mut_ptr()) < 0 {
            return None;
        }

        groups.into_iter().find(|gid| *gid != current_gid)
    }
}

// --- Task 3.4: ProbeRuntime trait and mock ---

// --- Task 3.5: HostPortRuntime trait and mock ---

// --- Task 3.6: PodEventSink trait and mock ---

// --- Task 7.1: MockPodRuntimeService tests ---

// --- Task 8.1: RealPodRuntimeService Constructor ---

use crate::kubelet::pod_runtime::service::RuntimeConfig;

async fn fixture_pod_repository() -> PodRuntimeTestPorts {
    let (ds, handle) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
    // These runtime tests place pods in conventional non-system namespaces. The
    // API create path enforces the upstream NamespaceLifecycle rule (target
    // namespace must exist), so seed them as a live cluster would have them.
    seed_runtime_test_namespaces(&handle).await;
    std::mem::forget(ds);
    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let node_local =
        crate::bootstrap::pod_repository_composition::test_node_local_store(supervisor.clone())
            .await;
    build_test_pod_repository(
        handle,
        supervisor,
        node_local,
        crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
    )
}

async fn fixture_env_source(
    _node_name: &str,
) -> std::sync::Arc<dyn klights_kubelet::pod_env::EnvSourceReader> {
    std::sync::Arc::new(MockEnvSourceReader::new())
}

// --- Task 8.2: RealPodRuntimeService::start_pod identity/admission/status ---

/// Focused helper: fetch a Pod by identity through the parts' `pod_query`
/// without forcing every call site to spell out the `PodGetRequest`.
async fn __pod_query_get(
    parts: &PodRuntimeTestPorts,
    ns: &str,
    name: &str,
) -> Option<klights_cluster_core::Resource> {
    parts
        .pod_query
        .get_pod(klights_pod_api::PodGetRequest::try_by_name(ns, name).unwrap())
        .await
        .expect("pod query get_pod")
}

struct SnapshotOnlyPodQuery;

impl klights_pod_api::PodQuery for SnapshotOnlyPodQuery {
    fn get_pod(
        &self,
        _request: klights_pod_api::PodGetRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async { Ok(None) })
    }

    fn list_pods(
        &self,
        _request: klights_pod_api::PodListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodListResult> {
        Box::pin(async { klights_pod_api::PodListResult::try_new(Vec::new(), 0, None, None) })
    }

    fn list_pods_by_owner_uid(
        &self,
        _request: klights_pod_api::PodOwnerListRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

// --- Task 8.3: RealPodRuntimeService::start_pod image pull flow ---

use serde_json::{Value, json};

fn pod_with_pull_policy(ns: &str, name: &str, uid: &str, image: &str, policy: &str) -> Value {
    let mut p = klights_kubelet::runtime::test_support::pod_json(ns, name, uid, image);
    p["spec"]["containers"][0]["imagePullPolicy"] = json!(policy);
    p
}

// --- Task 8.4: RealPodRuntimeService::start_pod sandbox and assignment ---

fn assert_partial_start_rolled_back(
    harness: &PodRuntimeHarness,
    key: &PodRuntimeKey,
    sandbox_id: &str,
) {
    let cri_calls = harness.cri.recorded_calls();
    assert!(
        cri_calls.iter().any(|call| matches!(
            call.operation,
            MockCriOperation::StopPodSandbox(ref id) if id == sandbox_id
        )),
        "partial rollback must stop the sandbox; calls={cri_calls:?}"
    );
    assert!(
        cri_calls.iter().any(|call| matches!(
            call.operation,
            MockCriOperation::RemovePodSandbox(ref id) if id == sandbox_id
        )),
        "partial rollback must remove the sandbox; calls={cri_calls:?}"
    );
    assert!(
        !cri_calls
            .iter()
            .any(|call| matches!(call.operation, MockCriOperation::CreateContainer { .. })),
        "partial rollback must stop before creating containers; calls={cri_calls:?}"
    );

    let net_calls = harness.network.recorded_calls();
    assert!(
        net_calls.iter().any(|call| matches!(
            call,
            MockNetworkOp::ReleaseSandboxNetwork {
                uid,
                sandbox_id: released,
                ..
            } if uid == &key.uid && released == sandbox_id
        )),
        "partial rollback must release the sandbox network; calls={net_calls:?}"
    );

    let store_calls = harness.store.recorded_calls();
    let expected_delete = format!("delete_sandbox:{}/{}/{}", key.namespace, key.name, key.uid);
    assert!(
        store_calls.iter().any(|call| call == &expected_delete),
        "partial rollback must clear the UID-bound sandbox row; calls={store_calls:?}"
    );

    let fs_calls = harness.filesystem.recorded_calls();
    let expected_cleanup = format!("cleanup_fs:{}/{}/{}", key.namespace, key.name, key.uid);
    assert!(
        fs_calls.iter().any(|call| call == &expected_cleanup),
        "partial rollback must remove pod filesystem artifacts; calls={fs_calls:?}"
    );
}

// --- Task 8.5: HostPort, Filesystem, Volume, and Container Flow ---

// --- Task 8.6: Cancellation and Rollback ---

// --- Task 9.1: Stop Pod Slot and Probe Phase ---

// HR #11: actor-owned finalization must confirm runtime cleanup before it
// clears the slot. Under churn the per-UID actor can have already exited, so the
// delete is finalized via the orphan path with NO sandbox hint and NO node-local
// store row. The orphan path must still consult the authoritative runtime (CRI,
// by UID) and stop the running sandbox — not silently clear the slot and leak a
// running sandbox (BUG: pods stayed "active" in the wrapped-volume-race test).

// --- Task 9.2: Stop Pod Sandbox Resolution and Container Cleanup ---

// --- Task 9.3: Stop Pod Sandbox, Cgroup, Store Row, and CNI Cleanup ---

/// P0 StopPod loop: the runtime service must refuse cleanup for a Pod it
/// does not own with a *typed* `PodOwnershipError` (downcastable), so the
/// lifecycle executor can classify it terminal/non-retryable instead of
/// spinning the actor forever on a generic retryable `DispatchFailed`.

// --- Task 9.4: Stop Pod HostPort, Volume Cleanup, CRI Absence, Slot Clear ---

/// The orphan/cold-sandbox stop path has no deleted-Pod snapshot, so it calls
/// `cleanup_pod_local_artifacts(key, None)`. It must STILL unmount and remove
/// the pod's volumes — `cleanup_volumes` needs only the key, not the pod spec.
/// Regression: the artifact helper gated `cleanup_volumes` on a `Some(pod)`
/// snapshot, so the orphan path skipped the unmount and leaked tmpfs/bind
/// mounts (then `remove_dir_all` ran over the live mount).

/// C4/B2 regression: cgroup teardown is UID-keyed and idempotent, so it must run
/// on every stop path via `cleanup_pod_local_artifacts` — even when no sandbox
/// can be resolved (CRI unreachable, store row gone). Previously cgroup cleanup
/// was gated inside the per-sandbox loop, so a no-sandbox stop leaked the pod
/// cgroup tree.
// --- Task 10.1: PodDeletionFinalizer trait and mock ---

// --- Task 11.1: Multi-node runtime traits ---

// --- Task 11.2: FakeNode and FakeCluster test doubles ---
use crate::kubelet::pod_cluster_runtime::{ClusterRuntimeView, NodeRuntimeView};

// --- Task 12.1: Multi-node runtime start respects node ownership ---

/// Build a RealPodRuntimeService with a custom FakeNode for node-ownership tests.
async fn fixture_runtime_with_node(
    node_name: &str,
) -> (
    std::sync::Arc<MockCriRuntime>,
    std::sync::Arc<crate::kubelet::pod_runtime::service::RealPodRuntimeService>,
    PodRuntimeTestPorts,
) {
    let repo = fixture_pod_repository().await;
    let cri = std::sync::Arc::new(MockCriRuntime::new());
    let container_control = std::sync::Arc::new(MockContainerRuntimeControl::new());
    let network = std::sync::Arc::new(MockPodNetworkRuntime::new());
    let store = std::sync::Arc::new(MockPodRuntimeStore::new());
    let slot_admission = std::sync::Arc::new(MockPodSlotAdmission::new());
    let filesystem = std::sync::Arc::new(MockPodFilesystem::new());
    let volumes = std::sync::Arc::new(MockPodVolumeRuntime::new());
    let probes = std::sync::Arc::new(MockProbeRuntime::new());
    let hostports = std::sync::Arc::new(MockHostPortRuntime::new());
    let events = std::sync::Arc::new(MockPodEventSink::new());
    let hooks = std::sync::Arc::new(MockPodHookRuntime::new());
    let env_source = fixture_env_source(node_name).await;
    let finalizer = std::sync::Arc::new(MockPodDeletionFinalizer::new());
    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let config = RuntimeConfig {
        node_name: node_name.to_string(),
        service_cidr: "10.43.128.0/17".into(),
        containerd_namespace: "klights-test".into(),
        sandbox_inputs: klights_kubelet::pod_sandbox_config::SandboxRuntimeInputs::default(),
        node_capacity: klights_kubelet::node_capacity::NodeCapacity::default(),
        paths: klights_kubelet::runtime_paths::KubeletRuntimePaths::new(std::path::PathBuf::from(
            "/tmp/klights-runtime-test",
        ))
        .unwrap(),
    };
    // Every node routes through the same repository-backed cluster-view path.
    let cluster_view: std::sync::Arc<dyn crate::kubelet::pod_cluster_runtime::ClusterRuntimeView> =
        std::sync::Arc::new(
            crate::kubelet::pod_cluster_runtime::RepositoryClusterRuntimeView::new(
                repo.pod_query.clone(),
                repo.pod_status_writer.clone(),
            ),
        );
    let node_view = std::sync::Arc::new(FakeNode::new(node_name));

    let runtime = std::sync::Arc::new(
        crate::kubelet::pod_runtime::service::RealPodRuntimeService::new(
            RealPodRuntimeServiceDependencies {
                cri: cri.clone(),
                container_control,
                network,
                store,
                clock: std::sync::Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
                slot_admission,
                pod_query: repo.pod_query.clone(),
                pod_status_writer: repo.pod_status_writer.clone(),
                filesystem,
                volumes,
                probes,
                hostports,
                events,
                hooks,
                env_source,
                finalizer,
                supervisor,
                config,
                node_view,
                cluster_view,
            },
        ),
    );
    (cri, runtime, repo)
}

use klights_kubelet::runtime::test_support::scheduled_pod_json;

/// F1 companion: the no-snapshot start path (`pod = None`) must ALSO reject a
/// same-name replacement. With no snapshot, start_pod fetches the pod fresh by
/// UID (`get_pod_for_uid`); a replacement carrying a different live UID resolves
/// to None ("not found for uid"), so the stale-UID start fails and never touches
/// CRI. Locks in that the guard does not depend on a snapshot being supplied.

// --- Task 12.2: Multi-node status writes use cluster boundary ---

/// Build a RealPodRuntimeService with a FakeCluster for status-forwarding tests.
async fn fixture_runtime_with_cluster(
    node_name: &str,
    cluster: std::sync::Arc<FakeCluster>,
) -> (
    std::sync::Arc<MockCriRuntime>,
    std::sync::Arc<crate::kubelet::pod_runtime::service::RealPodRuntimeService>,
    PodRuntimeTestPorts,
    std::sync::Arc<FakeCluster>,
    std::sync::Arc<MockHostPortRuntime>,
) {
    let repo = fixture_pod_repository().await;
    let cri = std::sync::Arc::new(MockCriRuntime::new());
    let container_control = std::sync::Arc::new(MockContainerRuntimeControl::new());
    let network = std::sync::Arc::new(MockPodNetworkRuntime::new());
    let store = std::sync::Arc::new(MockPodRuntimeStore::new());
    let slot_admission = std::sync::Arc::new(MockPodSlotAdmission::new());
    let filesystem = std::sync::Arc::new(MockPodFilesystem::new());
    let volumes = std::sync::Arc::new(MockPodVolumeRuntime::new());
    let probes = std::sync::Arc::new(MockProbeRuntime::new());
    let hostports = std::sync::Arc::new(MockHostPortRuntime::new());
    let events = std::sync::Arc::new(MockPodEventSink::new());
    let hooks = std::sync::Arc::new(MockPodHookRuntime::new());
    let env_source = fixture_env_source(node_name).await;
    let finalizer = std::sync::Arc::new(MockPodDeletionFinalizer::new());
    let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
        klights_supervisor::TaskCategoryConfig::default(),
    ));
    let config = RuntimeConfig {
        node_name: node_name.to_string(),
        service_cidr: "10.43.128.0/17".into(),
        containerd_namespace: "klights-test".into(),
        sandbox_inputs: klights_kubelet::pod_sandbox_config::SandboxRuntimeInputs::default(),
        node_capacity: klights_kubelet::node_capacity::NodeCapacity::default(),
        paths: klights_kubelet::runtime_paths::KubeletRuntimePaths::new(std::path::PathBuf::from(
            "/tmp/klights-runtime-test",
        ))
        .unwrap(),
    };
    let node_view = std::sync::Arc::new(FakeNode::new(node_name));

    let runtime = std::sync::Arc::new(
        crate::kubelet::pod_runtime::service::RealPodRuntimeService::new(
            RealPodRuntimeServiceDependencies {
                cri: cri.clone(),
                container_control,
                network,
                store,
                clock: std::sync::Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
                slot_admission,
                pod_query: repo.pod_query.clone(),
                pod_status_writer: repo.pod_status_writer.clone(),
                filesystem,
                volumes,
                probes,
                hostports: hostports.clone(),
                events,
                hooks,
                env_source,
                finalizer,
                supervisor,
                config,
                node_view,
                cluster_view: cluster.clone() as std::sync::Arc<dyn ClusterRuntimeView>,
            },
        ),
    );
    (cri, runtime, repo, cluster, hostports)
}

// --- Task 12.3: Multi-node runtime cleanup node-local ---

// --- Task 17.1: Mock Dependency Coverage Matrix ---

/// CRI: image pull, sandbox run, container stop calls recorded in order.

/// Network: read_assignment and release_sandbox_network carry PodRuntimeKey.

/// Runtime store: sandbox rows isolated by UID; same-name old/new preserved.

/// Repository: MockPodRuntimeStore validates stale UID is rejected.

/// Cluster view: minimal fake verifying forward_pod_status carries UID.

/// Timer: TaskSupervisor spawn_delay fires once per scheduled deadline.

/// Probe: start/stop carry UID.

/// Filesystem: hosts/log/cgroup/fsgroup calls record UID.

/// Volume: process and cleanup recorded.

/// Hostport: add/remove/admission carry UID.

/// Event sink: Scheduled/Pulling/Pulled/Failed events carry UID.

/// Env source: configmap/secret/service lookups are recordable without
/// datastore, leader API, or filesystem.

/// Deletion finalizer: finalize call carries PodRuntimeKey.

/// Fake cluster: separate CRI mocks for leader and worker ensure no cross-talk.

// ── Task 20.1: ContainerRuntimeControl on SharedCriRuntime ──

/// Structural verification that SharedCriRuntime implements
/// ContainerRuntimeControl and the adapter compiles.

// ── Task 20.3-20.4: RealPodSlotAdmission & RealPodRuntimeStore ──

struct FixedRuntimeClock(i64);

impl klights_kubelet::runtime_clock::RuntimeClock for FixedRuntimeClock {
    fn now_ms(&self) -> i64 {
        self.0
    }
}

// ── Task 20.10: LocalNodeRuntimeView ──

use crate::kubelet::pod_cluster_runtime::LocalNodeRuntimeView;

// ── Task 20.11: ClusterRuntimeView ──

// ── Task 21.1: finalize_deletion routes through PodDeletionFinalizer ──

// ── Task 21.2: handle_lifecycle_command ──

// ── Task 21.3: reconcile_ephemeral ──

// ── Task 21.4: reconcile_runtime ──

// ── Task 21.5: finalize_startup ──

// ── Task 22.1: Init Containers ──

// ── Task 22.2: Full Container Config ──

// ── Task 22.3: Restart Policy and Retry ──

// --- Task 22.4: PostStart Hooks ---

// --- Task 22.5: Probe Registration on Start ---

use klights_kubelet::pod_repository::PodStatusUpdate;

// --- Task 23.1: PreStop Hooks ---

struct AdvancingStopClock {
    now_ms: std::sync::atomic::AtomicI64,
    step_ms: i64,
}

impl AdvancingStopClock {
    fn new(now_ms: i64, step_ms: i64) -> Self {
        Self {
            now_ms: std::sync::atomic::AtomicI64::new(now_ms),
            step_ms,
        }
    }
}

impl klights_kubelet::runtime_clock::RuntimeClock for AdvancingStopClock {
    fn now_ms(&self) -> i64 {
        self.now_ms
            .fetch_add(self.step_ms, std::sync::atomic::Ordering::SeqCst)
    }
}

#[allow(clippy::too_many_arguments)]
async fn stop_with_deadline_request(
    harness: &PodRuntimeHarness,
    key: PodRuntimeKey,
    pod: serde_json::Value,
    sandbox_id: &str,
    deadline: chrono::DateTime<chrono::Utc>,
    mode: klights_kubelet::runtime::PodStopMode,
    operation_id: u64,
    cancel: CancellationToken,
) -> anyhow::Result<klights_kubelet::runtime::PodStopResult> {
    <crate::kubelet::pod_runtime::service::RealPodRuntimeService as PodRuntimeService>::stop_pod(
        harness.runtime.as_ref(),
        klights_kubelet::runtime::PodStopRequest {
            key,
            pod: Some(pod),
            sandbox_id: Some(sandbox_id.to_string()),
            deletion_deadline: Some(deadline),
            mode,
            operation_id,
            cancel,
        },
    )
    .await
}

fn deadline_runtime_pod(name: &str, uid: &str, with_pre_stop: bool) -> serde_json::Value {
    let mut container = serde_json::json!({"name": "app", "image": "nginx"});
    if with_pre_stop {
        container["lifecycle"] = serde_json::json!({
            "preStop": {"exec": {"command": ["true"]}}
        });
    }
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"namespace": "ns", "name": name, "uid": uid},
        "spec": {"nodeName": "test-node", "containers": [container]},
        "status": {
            "phase": "Running",
            "podIP": "10.0.0.8",
            "containerStatuses": [{
                "name": "app",
                "containerID": "containerd://ctr-deadline",
                "state": {"running": {"startedAt": "2026-01-01T00:00:00Z"}}
            }]
        }
    })
}

// --- Task 23.2: Termination Grace Period ---

// --- Task 23.3: Sandbox Resolution Full Ladder ---

// --- Task 23.4: Partial-State and Rollback Handling ---

async fn wait_for_pod_status(
    harness: &PodRuntimeHarness,
    key: &PodRuntimeKey,
    predicate: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    for _ in 0..50 {
        let pod = harness.stored_pod(key).await;
        if predicate(&pod) {
            return pod;
        }
        let _ = harness
            .supervisor
            .sleep(
                "pod_runtime_status_wait",
                std::time::Duration::from_millis(10),
            )
            .await;
    }
    let pod = harness.stored_pod(key).await;
    panic!("pod status did not reach expected state: {pod}");
}

// ── Task 1 (fixnow): CRI event fast-exit hint ──
//
// Short-lived pods (ConfigMap-volume / ReplicaSet-adoption) can exit while
// startup finalization is still in flight. The actor defers the CRI stop
// event and later runs a runtime reconcile. If sandbox container listing
// returns empty/stale by then, the reconciler must not synthesize
// Pending/ContainerCreating — it must use the CRI event's container id to
// read the concrete (terminated) status and publish Succeeded.

// Task 4 runtime observation tests moved to cri_recovery.

/// Conventional non-system namespaces these runtime tests place pods in. The
/// API create path enforces the upstream NamespaceLifecycle rule (target
/// namespace must exist), so harnesses seed these as a live cluster would.
/// (System namespaces like `default`/`kube-system` are always considered
/// present, so they need not be listed here.)
const RUNTIME_TEST_NAMESPACES: &[&str] = &[
    "ns",
    "statefulset",
    "sonobuoy",
    "deleted-ns",
    "init-container",
    "container-probe",
    "container-runtime",
    "dns-debug",
    "downward-api",
    "e2e-debug",
    "kubelet-test",
    "logs",
    "pod-network-test",
    "pods",
    "security-context",
    "sysctl",
    "var-expansion",
];

/// Seed every conventional runtime-test namespace into `handle`.
async fn seed_runtime_test_namespaces(handle: &crate::datastore::DatastoreHandle) {
    for ns in RUNTIME_TEST_NAMESPACES {
        crate::datastore::DatastoreBackend::seed_namespace_for_test(handle.as_ref(), ns).await;
    }
}

// --- PodRuntimeHarness ---

/// Wires every mockable port for `RealPodRuntimeService` unit tests.
/// Extended task-by-task; starts with all ports needed by Task 8.1.
struct PodRuntimeHarness {
    cri: std::sync::Arc<MockCriRuntime>,
    container_control: std::sync::Arc<MockContainerRuntimeControl>,
    network: std::sync::Arc<MockPodNetworkRuntime>,
    store: std::sync::Arc<MockPodRuntimeStore>,
    slot_admission: std::sync::Arc<MockPodSlotAdmission>,
    db_handle: crate::datastore::DatastoreHandle,
    repo: PodRuntimeTestPorts,
    filesystem: std::sync::Arc<MockPodFilesystem>,
    volumes: std::sync::Arc<MockPodVolumeRuntime>,
    probes: std::sync::Arc<MockProbeRuntime>,
    hostports: std::sync::Arc<MockHostPortRuntime>,
    events: std::sync::Arc<MockPodEventSink>,
    hooks: std::sync::Arc<MockPodHookRuntime>,
    env_source: std::sync::Arc<MockEnvSourceReader>,
    finalizer: std::sync::Arc<MockPodDeletionFinalizer>,
    supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    node_view: std::sync::Arc<FakeNode>,
    runtime: std::sync::Arc<crate::kubelet::pod_runtime::service::RealPodRuntimeService>,
}

impl PodRuntimeHarness {
    fn default_runtime_config() -> crate::kubelet::pod_runtime::service::RuntimeConfig {
        crate::kubelet::pod_runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.43.128.0/17".into(),
            containerd_namespace: "klights-test".into(),
            sandbox_inputs: klights_kubelet::pod_sandbox_config::SandboxRuntimeInputs::default(),
            node_capacity: klights_kubelet::node_capacity::NodeCapacity::default(),
            paths: klights_kubelet::runtime_paths::KubeletRuntimePaths::new(
                std::path::PathBuf::from("/tmp/klights-runtime-test"),
            )
            .unwrap(),
        }
    }

    /// Construct with all-default mocks and an in-memory repository.
    async fn new() -> Self {
        Self::new_with_runtime_config(Self::default_runtime_config()).await
    }

    async fn new_with_clock(
        clock: std::sync::Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    ) -> Self {
        Self::new_with_runtime_config_and_clock(Self::default_runtime_config(), clock).await
    }

    async fn new_with_runtime_config(
        config: crate::kubelet::pod_runtime::service::RuntimeConfig,
    ) -> Self {
        Self::new_with_runtime_config_and_clock(
            config,
            std::sync::Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        )
        .await
    }

    async fn new_with_runtime_config_and_clock(
        config: crate::kubelet::pod_runtime::service::RuntimeConfig,
        clock: std::sync::Arc<dyn klights_kubelet::runtime_clock::RuntimeClock>,
    ) -> Self {
        let (ds, handle) = crate::datastore::selector::sqlite_in_memory_store_for_test().await;
        // The API create path enforces the upstream NamespaceLifecycle rule
        // (target namespace must exist). Seed the conventional namespaces these
        // runtime tests place pods in, mirroring a live cluster.
        seed_runtime_test_namespaces(&handle).await;
        // Keep ds alive so the handle stays valid.
        std::mem::forget(ds);
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let node_local =
            crate::bootstrap::pod_repository_composition::test_node_local_store(supervisor.clone())
                .await;
        let repo = build_test_pod_repository(
            handle.clone(),
            supervisor.clone(),
            node_local,
            crate::bootstrap::controller_adapters::system_identity_adapter::deterministic_controller_identity(),
        );
        let cri = std::sync::Arc::new(MockCriRuntime::new());
        let container_control = std::sync::Arc::new(MockContainerRuntimeControl::new());
        let network = std::sync::Arc::new(MockPodNetworkRuntime::new());
        let store = std::sync::Arc::new(MockPodRuntimeStore::new());
        let slot_admission = std::sync::Arc::new(MockPodSlotAdmission::new());
        let filesystem = std::sync::Arc::new(MockPodFilesystem::new());
        let volumes = std::sync::Arc::new(MockPodVolumeRuntime::new());
        let probes = std::sync::Arc::new(MockProbeRuntime::new());
        let hostports = std::sync::Arc::new(MockHostPortRuntime::new());
        let events = std::sync::Arc::new(MockPodEventSink::new());
        let hooks = std::sync::Arc::new(MockPodHookRuntime::new());
        let env_source = std::sync::Arc::new(MockEnvSourceReader::new());
        let finalizer = std::sync::Arc::new(MockPodDeletionFinalizer::new());

        let node_view = std::sync::Arc::new(FakeNode::new(&config.node_name));
        let cluster_view = std::sync::Arc::new(
            super::super::pod_cluster_runtime::RepositoryClusterRuntimeView::new(
                repo.pod_query.clone(),
                repo.pod_status_writer.clone(),
            ),
        );

        let runtime = std::sync::Arc::new(
            crate::kubelet::pod_runtime::service::RealPodRuntimeService::new(
                RealPodRuntimeServiceDependencies {
                    cri: cri.clone(),
                    container_control: container_control.clone(),
                    network: network.clone(),
                    store: store.clone(),
                    clock,
                    slot_admission: slot_admission.clone(),
                    pod_query: repo.pod_query.clone(),
                    pod_status_writer: repo.pod_status_writer.clone(),
                    filesystem: filesystem.clone(),
                    volumes: volumes.clone(),
                    probes: probes.clone(),
                    hostports: hostports.clone(),
                    events: events.clone(),
                    hooks: hooks.clone(),
                    env_source: env_source.clone(),
                    finalizer: finalizer.clone(),
                    supervisor: supervisor.clone(),
                    config,
                    node_view: node_view.clone(),
                    cluster_view,
                },
            ),
        );

        Self {
            cri,
            container_control,
            network,
            store,
            slot_admission,
            db_handle: handle,
            repo,
            filesystem,
            volumes,
            probes,
            hostports,
            events,
            hooks,
            env_source,
            finalizer,
            supervisor,
            node_view,
            runtime,
        }
    }

    async fn create_runtime_pod(&self, pod: serde_json::Value) {
        let namespace = pod
            .pointer("/metadata/namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();
        let name = pod
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .expect("test pod must have metadata.name")
            .to_string();
        let node_name = pod
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .unwrap_or("test-node")
            .to_string();

        // The API create path enforces the upstream NamespaceLifecycle rule
        // (target namespace must exist). Ensure the pod's namespace is present,
        // mirroring a live cluster where the namespace always pre-exists.
        self.db_handle.seed_namespace_for_test(&namespace).await;

        self.repo
            .test_create_pod(&namespace, &name, &node_name, pod)
            .await
            .expect("create runtime test pod");
    }

    async fn stored_pod(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
    ) -> serde_json::Value {
        self.repo
            .test_get_pod_for_uid(&key.namespace, &key.name, &key.uid)
            .await
            .expect("read runtime test pod")
            .expect("runtime test pod should exist")
            .data
            .as_ref()
            .clone()
    }

    async fn start_pod_through_runtime(
        &self,
        key: crate::kubelet::pod_runtime::service::PodRuntimeKey,
        pod: serde_json::Value,
    ) -> crate::kubelet::pod_runtime::service::PodStartResult {
        self.runtime
            .start_pod(key, Some(pod), CancellationToken::new())
            .await
            .expect("start pod through runtime")
    }

    fn simulate_running_containers(&self, containers: impl IntoIterator<Item = String>) {
        self.container_control.set_container_states(
            containers
                .into_iter()
                .map(|container_id| {
                    (
                        container_id,
                        klights_kubelet::runtime::cri::ContainerRuntimeState::Running,
                    )
                })
                .collect(),
        );
    }

    async fn reconcile_runtime(&self, key: crate::kubelet::pod_runtime::service::PodRuntimeKey) {
        self.runtime
            .reconcile_runtime(
                key,
                crate::kubelet::pod_runtime::service::RuntimeReconcileHint::none(),
            )
            .await
            .expect("reconcile runtime");
    }
}
