#![cfg(test)]
#![allow(clippy::items_after_test_module)]

use k8s_cri::v1::PodSandboxConfig;

use crate::pod_deletion_finalizer::PodDeletionFinalizer;
use crate::pod_env::EnvSourceReader;
use crate::pod_lifecycle_core::message::PodLifecycleKey;
use crate::pod_service_envs::ServiceEnvSource;
use crate::runtime::events::PodEventSink;
use crate::runtime::events::test_support::MockPodEventSink;
use crate::runtime::filesystem::PodFilesystem;
use crate::runtime::filesystem::test_support::MockPodFilesystem;
use crate::runtime::hooks::HookOutcome;
use crate::runtime::hostports::HostPortRuntime;
use crate::runtime::hostports::test_support::{MockHostPortOp, MockHostPortRuntime};
use crate::runtime::network::test_support::{MockNetworkOp, MockPodNetworkRuntime};
use crate::runtime::probes::ProbeRuntime;
use crate::runtime::probes::test_support::{MockProbeCall, MockProbeRuntime};
use crate::runtime::store::{PodRuntimeStore, PodSlotAdmission};
use crate::runtime::test_repository::{
    InMemoryPodRepository, TestDeletionFinalizer, TestPodStatusWriter,
};
use crate::runtime::test_support::{
    MockContainerControlOp, MockContainerRuntimeControl, MockCriOperation, MockCriRuntime,
    MockEnvSourceReader, MockPodDeletionFinalizer, MockPodHookRuntime, MockPodRuntimeService,
    MockPodRuntimeStore, MockPodSlotAdmission, MockRuntimeCall,
};
use crate::runtime::volumes::PodVolumeRuntime;
use crate::runtime::volumes::test_support::MockPodVolumeRuntime;
use crate::runtime::{
    PodDeletionFinalizeResult, PodFinalizeStartupResult, PodOwnershipError, PodRuntimeKey,
    PodRuntimeService, PodStartResult,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

macro_rules! real_runtime {
    (
        cri: $cri:expr,
        container_control: $container_control:expr,
        network: $network:expr,
        store: $store:expr,
        clock: $clock:expr,
        slot_admission: $slot_admission:expr,
        pod_query: $pod_query:expr,
        pod_status_writer: $pod_status_writer:expr,
        filesystem: $filesystem:expr,
        volumes: $volumes:expr,
        probes: $probes:expr,
        hostports: $hostports:expr,
        events: $events:expr,
        hooks: $hooks:expr,
        env_source: $env_source:expr,
        finalizer: $finalizer:expr,
        supervisor: $supervisor:expr,
        config: $config:expr $(,)?
    ) => {{
        crate::runtime::service::RealPodRuntimeService::new(
            $cri,
            $container_control,
            $network,
            $store,
            $clock,
            $slot_admission,
            $pod_query,
            $pod_status_writer,
            $filesystem,
            $volumes,
            $probes,
            $hostports,
            $events,
            $hooks,
            $env_source,
            $finalizer,
            $supervisor,
            $config,
        )
    }};
}

mod cri_recovery;
mod filesystem_volumes;
mod forwarded_status;
mod lifecycle_status;
mod network_hostports;
mod probes;
mod slot_retry;
mod worker_outbox;

fn kubelet_runtime_paths_for_test(namespace: &str) -> crate::runtime_paths::KubeletRuntimePaths {
    crate::runtime_paths::KubeletRuntimePaths::for_test(namespace)
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
    backend: Arc<InMemoryPodRepository>,
    pod_query: Arc<dyn klights_pod_api::PodQuery>,
    pod_status_writer: Arc<dyn crate::pod_repository::status::PodStatusWriter>,
    deletion_finalizer: Arc<dyn crate::pod_deletion_finalizer::PodDeletionFinalizer>,
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
        let created = self.backend.insert(body)?;
        anyhow::ensure!(created.namespace.as_deref() == Some(namespace));
        anyhow::ensure!(created.name == name);
        Ok(created)
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
fn build_test_pod_repository() -> PodRuntimeTestPorts {
    let backend = Arc::new(InMemoryPodRepository::default());
    let pod_query = backend.clone() as Arc<dyn klights_pod_api::PodQuery>;
    let pod_status_writer = Arc::new(TestPodStatusWriter::new(backend.clone()))
        as Arc<dyn crate::pod_repository::status::PodStatusWriter>;
    let deletion_finalizer = Arc::new(TestDeletionFinalizer::new(backend.clone()))
        as Arc<dyn crate::pod_deletion_finalizer::PodDeletionFinalizer>;
    PodRuntimeTestPorts {
        backend,
        pod_query,
        pod_status_writer,
        deletion_finalizer,
    }
}

async fn node_local_runtime_store() -> Arc<klights_node_datastore::SqliteRuntimeWorkStore> {
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
    Arc::new(klights_node_datastore::SqliteRuntimeWorkStore::new(
        executor,
        Arc::new(klights_supervisor::SystemWallClock),
    ))
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
use crate::runtime::cri::CriRuntime;

// --- Task 3.1: PodNetworkRuntime trait and mock ---

use crate::runtime::network::PodNetworkRuntime;

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

use crate::runtime::service::RuntimeConfig;

async fn fixture_pod_repository() -> PodRuntimeTestPorts {
    build_test_pod_repository()
}

async fn fixture_env_source(
    _node_name: &str,
) -> std::sync::Arc<dyn crate::pod_env::EnvSourceReader> {
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
    let mut p = crate::runtime::test_support::pod_json(ns, name, uid, image);
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

// --- Task 11.2: Focused node/status test doubles ---

struct FakeNode {
    node_name: String,
}

impl FakeNode {
    fn new(node_name: &str) -> Self {
        Self {
            node_name: node_name.to_string(),
        }
    }

    fn node_name(&self) -> &str {
        &self.node_name
    }

    fn owns_pod_runtime(&self, pod: &serde_json::Value) -> bool {
        crate::runtime::cluster_policy::owns_pod_runtime(&self.node_name, pod)
    }
}

type StatusForward = (PodRuntimeKey, serde_json::Value);

#[derive(Default)]
struct FakeCluster {
    status_forwards: std::sync::Mutex<Vec<StatusForward>>,
}

impl FakeCluster {
    fn new() -> Self {
        Self::default()
    }

    async fn get_fresh_pod(
        &self,
        _namespace: &str,
        _name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        Ok(None)
    }

    async fn forward_pod_status(
        &self,
        key: &PodRuntimeKey,
        status: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.status_forwards
            .lock()
            .unwrap()
            .push((key.clone(), status));
        klights_cluster_core::Resource::try_from_data(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": key.namespace,
                "name": key.name,
                "uid": key.uid,
                "resourceVersion": "1"
            }
        })))
        .map_err(Into::into)
    }

    fn recorded_status_forwards(&self) -> Vec<StatusForward> {
        self.status_forwards.lock().unwrap().clone()
    }
}

// --- Task 12.1: Multi-node runtime start respects node ownership ---

/// Build a RealPodRuntimeService with a custom FakeNode for node-ownership tests.
async fn fixture_runtime_with_node(
    node_name: &str,
) -> (
    std::sync::Arc<MockCriRuntime>,
    std::sync::Arc<crate::runtime::service::RealPodRuntimeService>,
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
        sandbox_inputs: crate::pod_sandbox_config::SandboxRuntimeInputs::default(),
        node_capacity: crate::node_capacity::NodeCapacity::default(),
        paths: crate::runtime_paths::KubeletRuntimePaths::new(std::path::PathBuf::from(
            "/tmp/klights/runtime-test",
        ))
        .unwrap(),
    };
    let runtime = std::sync::Arc::new(real_runtime! {
        cri: cri.clone(),
        container_control: container_control,
        network: network,
        store: store,
        clock: std::sync::Arc::new(crate::runtime_clock::SystemRuntimeClock),
        slot_admission: slot_admission,
        pod_query: repo.pod_query.clone(),
        pod_status_writer: repo.pod_status_writer.clone(),
        filesystem: filesystem,
        volumes: volumes,
        probes: probes,
        hostports: hostports,
        events: events,
        hooks: hooks,
        env_source: env_source,
        finalizer: finalizer,
        supervisor: supervisor,
        config: config,
    });
    (cri, runtime, repo)
}

use crate::runtime::test_support::scheduled_pod_json;

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
    std::sync::Arc<crate::runtime::service::RealPodRuntimeService>,
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
        sandbox_inputs: crate::pod_sandbox_config::SandboxRuntimeInputs::default(),
        node_capacity: crate::node_capacity::NodeCapacity::default(),
        paths: crate::runtime_paths::KubeletRuntimePaths::new(std::path::PathBuf::from(
            "/tmp/klights/runtime-test",
        ))
        .unwrap(),
    };
    let runtime = std::sync::Arc::new(real_runtime! {
        cri: cri.clone(),
        container_control: container_control,
        network: network,
        store: store,
        clock: std::sync::Arc::new(crate::runtime_clock::SystemRuntimeClock),
        slot_admission: slot_admission,
        pod_query: repo.pod_query.clone(),
        pod_status_writer: repo.pod_status_writer.clone(),
        filesystem: filesystem,
        volumes: volumes,
        probes: probes,
        hostports: hostports.clone(),
        events: events,
        hooks: hooks,
        env_source: env_source,
        finalizer: finalizer,
        supervisor: supervisor,
        config: config,
    });
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

impl crate::runtime_clock::RuntimeClock for FixedRuntimeClock {
    fn now_ms(&self) -> i64 {
        self.0
    }
}

// ── Task 20.10: LocalNodeRuntimeView ──

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

use crate::pod_repository::PodStatusUpdate;

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

impl crate::runtime_clock::RuntimeClock for AdvancingStopClock {
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
    mode: crate::runtime::PodStopMode,
    operation_id: u64,
    cancel: CancellationToken,
) -> anyhow::Result<crate::runtime::PodStopResult> {
    <crate::runtime::service::RealPodRuntimeService as PodRuntimeService>::stop_pod(
        harness.runtime.as_ref(),
        crate::runtime::PodStopRequest {
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

// --- PodRuntimeHarness ---

/// Wires every mockable port for `RealPodRuntimeService` unit tests.
/// Extended task-by-task; starts with all ports needed by Task 8.1.
struct PodRuntimeHarness {
    cri: std::sync::Arc<MockCriRuntime>,
    container_control: std::sync::Arc<MockContainerRuntimeControl>,
    network: std::sync::Arc<MockPodNetworkRuntime>,
    store: std::sync::Arc<MockPodRuntimeStore>,
    slot_admission: std::sync::Arc<MockPodSlotAdmission>,
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
    runtime: std::sync::Arc<crate::runtime::service::RealPodRuntimeService>,
}

impl PodRuntimeHarness {
    fn default_runtime_config() -> crate::runtime::service::RuntimeConfig {
        crate::runtime::service::RuntimeConfig {
            node_name: "test-node".into(),
            service_cidr: "10.43.128.0/17".into(),
            containerd_namespace: "klights-test".into(),
            sandbox_inputs: crate::pod_sandbox_config::SandboxRuntimeInputs::default(),
            node_capacity: crate::node_capacity::NodeCapacity::default(),
            paths: crate::runtime_paths::KubeletRuntimePaths::new(std::path::PathBuf::from(
                "/tmp/klights/runtime-test",
            ))
            .unwrap(),
        }
    }

    /// Construct with all-default mocks and an in-memory repository.
    async fn new() -> Self {
        Self::new_with_runtime_config(Self::default_runtime_config()).await
    }

    async fn new_with_clock(clock: std::sync::Arc<dyn crate::runtime_clock::RuntimeClock>) -> Self {
        Self::new_with_runtime_config_and_clock(Self::default_runtime_config(), clock).await
    }

    async fn new_with_runtime_config(config: crate::runtime::service::RuntimeConfig) -> Self {
        Self::new_with_runtime_config_and_clock(
            config,
            std::sync::Arc::new(crate::runtime_clock::SystemRuntimeClock),
        )
        .await
    }

    async fn new_with_runtime_config_and_clock(
        config: crate::runtime::service::RuntimeConfig,
        clock: std::sync::Arc<dyn crate::runtime_clock::RuntimeClock>,
    ) -> Self {
        let supervisor = std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let repo = build_test_pod_repository();
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

        let runtime = std::sync::Arc::new(real_runtime! {
            cri: cri.clone(),
            container_control: container_control.clone(),
            network: network.clone(),
            store: store.clone(),
            clock: clock,
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
            config: config,
        });

        Self {
            cri,
            container_control,
            network,
            store,
            slot_admission,
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

        self.repo
            .test_create_pod(&namespace, &name, &node_name, pod)
            .await
            .expect("create runtime test pod");
    }

    async fn stored_pod(&self, key: &crate::runtime_types::PodRuntimeKey) -> serde_json::Value {
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
        key: crate::runtime_types::PodRuntimeKey,
        pod: serde_json::Value,
    ) -> crate::runtime::PodStartResult {
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
                        crate::runtime::cri::ContainerRuntimeState::Running,
                    )
                })
                .collect(),
        );
    }

    async fn reconcile_runtime(&self, key: crate::runtime_types::PodRuntimeKey) {
        self.runtime
            .reconcile_runtime(key, crate::runtime::RuntimeReconcileHint::none())
            .await
            .expect("reconcile runtime");
    }
}
