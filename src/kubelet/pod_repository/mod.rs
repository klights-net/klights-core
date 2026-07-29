//! `PodRepository` — single production boundary for `v1/Pod` persistence.
//!
//! The repository owns kubelet lifecycle, workload-controller, accounting-
//! controller, API pod subresource, AND the main API pod create / update /
//! patch / delete / list paths. `("v1","Pod",...)` does not appear as a
//! `DatastoreBackend` argument outside [`store::PodStore`].
//!
//! Internal services depend on `Arc<PodStore>` rather than
//! `DatastoreHandle`, which localizes the pod-shaped DB boundary to a
//! single file. Network-runtime tables (`pod_network`, `sandbox`) and
//! [`klights_network_api::Datapath::cni_add`] /
//! [`klights_network_api::Datapath::cni_del`] calls remain with their existing owners and
//! are not policed by this boundary.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
#[cfg(test)]
use tokio::sync::broadcast;

#[cfg(test)]
use crate::datastore::DatastoreHandle;
use crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer;
use crate::kubelet::pod_runtime::service::PodDeletionFinalizeResult;
#[cfg(test)]
use crate::side_effects::SideEffectMetrics;
#[cfg(test)]
use crate::side_effects::SideEffectRegistry;
#[cfg(test)]
use crate::watch::WatchEvent;
use klights_cluster_core::Resource;
use klights_leader_api::LeaderResourceQuery;
use klights_leader_api::{ResourceGetRequest, ResourceListRequest, ResourceQueryConsistency};
use klights_pod_api::{PodDeleteOptions, PodRepositoryError};
use klights_reconcile_api::{GcPodDeleteRequest, GcPodDeleteSink};
use klights_supervisor::TaskSupervisor;
use klights_types::{PodIdentity, ResourceKey};

#[derive(Clone, Debug)]
pub struct PodResourceList {
    pub items: Vec<Resource>,
    pub resource_version: i64,
    pub continue_token: Option<String>,
    pub remaining_item_count: Option<i64>,
}

#[cfg(test)]
impl From<crate::datastore::ResourceList> for PodResourceList {
    fn from(list: crate::datastore::ResourceList) -> Self {
        Self {
            items: list.items,
            resource_version: list.resource_version,
            continue_token: list.continue_token,
            remaining_item_count: list.remaining_item_count,
        }
    }
}

fn resource_list_from_leader(result: klights_leader_api::ResourceListResult) -> PodResourceList {
    let (items, resource_version, _position, continue_token, remaining_item_count) =
        result.into_parts();
    PodResourceList {
        items,
        resource_version,
        continue_token,
        remaining_item_count,
    }
}

#[cfg(test)]
struct TestDatastorePodNetworkCache {
    node_local: Option<crate::datastore::node_local::KubeletTestStoreHandle>,
}

#[cfg(test)]
pub(crate) fn test_pod_network_cache(
    node_local: crate::datastore::node_local::KubeletTestStoreHandle,
) -> Arc<dyn klights_node_store::PodNetworkCache> {
    Arc::new(TestDatastorePodNetworkCache {
        node_local: Some(node_local),
    })
}

#[cfg(test)]
pub(crate) fn empty_test_pod_network_cache() -> Arc<dyn klights_node_store::PodNetworkCache> {
    Arc::new(TestDatastorePodNetworkCache { node_local: None })
}

#[cfg(test)]
pub(crate) fn test_assignment_bus()
-> Arc<crate::networking::pod_network_events::PodNetworkAssignmentBus> {
    Arc::new(crate::networking::pod_network_events::PodNetworkAssignmentBus::new())
}

#[cfg(test)]
pub(crate) async fn test_node_local_store(
    supervisor: Arc<TaskSupervisor>,
) -> crate::datastore::node_local::KubeletTestStoreHandle {
    crate::datastore::node_local::selector::open_node_local(
        crate::datastore::backend_kind::BackendKind::Sqlite,
        None,
        supervisor,
        None,
        "sqlite:pod-repository-network-test",
    )
    .await
    .expect("open node-local test store")
}

#[cfg(test)]
impl klights_node_store::PodNetworkCache for TestDatastorePodNetworkCache {
    fn get_network_for_uid(
        &self,
        pod_uid: klights_node_store::PodUidKey,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        Box::pin(async move {
            let Some(node_local) = &self.node_local else {
                return Ok(None);
            };
            node_local
                .get_network_for_uid(pod_uid.as_str())
                .await
                .map_err(|error| {
                    klights_node_store::CacheNetworkError::persistence_failed(error.to_string())
                })?
                .map(test_network_endpoint)
                .transpose()
        })
    }

    fn get_network_for_pod(
        &self,
        pod: klights_types::PodIdentity,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        Box::pin(async move {
            let Some(node_local) = &self.node_local else {
                return Ok(None);
            };
            node_local
                .get_network_assignment_for_pod(pod)
                .await
                .map_err(|error| {
                    klights_node_store::CacheNetworkError::persistence_failed(error.to_string())
                })?
                .map(|row| crate::datastore::node_local::PodNetworkEndpoint {
                    ip_addr: row.ip_addr,
                    veth_host: row.veth_host,
                    netns_path: row.netns_path,
                })
                .map(test_network_endpoint)
                .transpose()
        })
    }

    fn get_network_for_sandbox(
        &self,
        sandbox_id: klights_node_store::SandboxKey,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        Box::pin(async move {
            let Some(node_local) = &self.node_local else {
                return Ok(None);
            };
            node_local
                .get_network_for_sandbox(sandbox_id.as_str())
                .await
                .map_err(|error| {
                    klights_node_store::CacheNetworkError::persistence_failed(error.to_string())
                })?
                .map(test_network_endpoint)
                .transpose()
        })
    }

    fn get_network_for_assignment(
        &self,
        sandbox_id: klights_node_store::SandboxKey,
        _pod: klights_types::PodIdentity,
    ) -> klights_node_store::CacheNetworkFuture<'_, Option<klights_node_store::PodNetworkEndpoint>>
    {
        self.get_network_for_sandbox(sandbox_id)
    }

    fn delete_network_for_sandbox(
        &self,
        sandbox_id: klights_node_store::SandboxKey,
    ) -> klights_node_store::CacheNetworkFuture<'_, ()> {
        Box::pin(async move {
            let Some(node_local) = &self.node_local else {
                return Ok(());
            };
            node_local
                .delete_network_for_sandbox(sandbox_id.as_str())
                .await
                .map_err(|error| {
                    klights_node_store::CacheNetworkError::persistence_failed(error.to_string())
                })
        })
    }

    fn delete_network_if_matches(
        &self,
        request: klights_node_store::PodNetworkAllocationRequest,
    ) -> klights_node_store::CacheNetworkFuture<'_, bool> {
        Box::pin(async move {
            let Some(node_local) = &self.node_local else {
                return Ok(false);
            };
            let legacy = crate::datastore::node_local::PodNetworkAllocationRequest::new(
                request.sandbox_id(),
                crate::datastore::node_local::PodNetworkAllocationPod::new(
                    &request.pod().namespace,
                    &request.pod().name,
                    &request.pod().uid,
                ),
                crate::datastore::node_local::PodNetworkAllocationSubnet::new(
                    request.subnet_base_int(),
                    request.subnet_size(),
                ),
                crate::datastore::node_local::PodNetworkAllocationLink::new(
                    request.veth_host(),
                    request.netns_path(),
                ),
            );
            node_local
                .delete_network_assignment_if_matches(legacy)
                .await
                .map_err(|error| {
                    klights_node_store::CacheNetworkError::persistence_failed(error.to_string())
                })
        })
    }

    fn list_network_assignments(
        &self,
    ) -> klights_node_store::CacheNetworkFuture<
        '_,
        Vec<klights_node_store::PodNetworkAssignmentSnapshot>,
    > {
        Box::pin(async { unreachable!("test-only cache does not drive network cleanup") })
    }
}

#[cfg(test)]
fn test_network_endpoint(
    row: crate::datastore::node_local::PodNetworkEndpoint,
) -> Result<klights_node_store::PodNetworkEndpoint, klights_node_store::CacheNetworkError> {
    klights_node_store::PodNetworkEndpoint::try_new(row.ip_addr, row.veth_host, row.netns_path)
        .map_err(|error| klights_node_store::CacheNetworkError::corrupt_data(error.to_string()))
}

pub mod background;
pub mod delete_coordinator;
pub mod facade;
pub mod network;
pub mod objects;
pub(crate) mod ordinary_access;
pub mod state_only_writer;
pub mod status;
pub(crate) mod store;
pub mod types;
pub mod watch;
pub mod workqueue;

#[cfg(test)]
#[path = "../../pod_repository_integration_tests.rs"]
mod tests;

#[cfg(test)]
pub(crate) use crate::pod_repository_composition::{PodRepositoryBuildConfig, PodSchedulingMode};
pub use types::{
    PodApiCreateRequest, PodApiCreateResult, PodApiDeleteOutcome, PodApiUpdateOutcome,
    PodNetworkAssignment, PodStatusPatchType, PodStatusUpdate, RuntimeReconcileStatus,
    content_type_to_patch_type,
};

use background::PodRepositoryBackground;
use delete_coordinator::PodDeleteCoordinator;
use klights_reconcile_api::{PodEvictionAdmissionSink, PodGcReconcileSink, PodPdbReconcileSink};
use network::PodNetworkService;
use objects::PodObjectService;
use state_only_writer::StateOnlyWriter;
use status::PodStatusService;
use store::PodStore;
use watch::PodWatchService;
use workqueue::PodWorkqueue;

pub(crate) struct PodRepositoryAdapterDependencies {
    pub store: Arc<PodStore>,
    pub status_only: Arc<dyn StateOnlyWriter>,
    pub supervisor: Arc<TaskSupervisor>,
    pub delete_coordinator: Arc<PodDeleteCoordinator>,
}

pub(crate) struct PodRepositoryRuntimeDependencies {
    pub supervisor: Arc<TaskSupervisor>,
    pub metrics: Arc<dyn klights_reconcile_api::ReconcileFailureMetrics>,
    pub wall_clock: Arc<dyn crate::kubelet::pod_runtime::store::RuntimeClock>,
}

pub(crate) struct PodRepositoryCoreDependencies {
    pub store: Arc<PodStore>,
    pub status_only: Arc<dyn StateOnlyWriter>,
    pub workqueue: Arc<PodWorkqueue>,
}

pub(crate) struct PodRepositoryNetworkDependencies {
    pub pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pub assignment_waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
}

pub(crate) struct PodRepositoryDeliveryDependencies {
    pub outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
    pub cluster_api: Option<Arc<dyn LeaderResourceQuery>>,
    pub bound_pod_finalization: Arc<dyn klights_pod_api::BoundPodFinalization>,
}

pub(crate) struct PodRepositoryAdapters {
    pub gc_delete: Arc<dyn GcPodDeleteSink>,
    pub gc_reconcile: Arc<dyn PodGcReconcileSink>,
    pub pdb_reconcile: Arc<dyn PodPdbReconcileSink>,
    pub eviction_admission: Arc<dyn PodEvictionAdmissionSink>,
    pub namespace_bootstrap: Arc<dyn klights_reconcile_api::NamespaceBootstrapSink>,
    pub namespace_termination: Arc<dyn klights_reconcile_api::NamespaceTerminationSink>,
    pub mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    #[cfg(test)]
    pub test_api: Option<Arc<dyn klights_pod_api::PodApiMutation>>,
    #[cfg(test)]
    pub test_subresource: Option<Arc<dyn klights_pod_api::PodSubresourceMutation>>,
    #[cfg(test)]
    pub test_scheduling: Option<Arc<dyn klights_pod_api::PodScheduling>>,
    #[cfg(test)]
    pub test_mark_terminating: Option<Arc<dyn klights_pod_api::PodMarkTerminating>>,
}

#[async_trait]
pub trait PodReader: Send + Sync {
    async fn get_pod(&self, ns: &str, name: &str) -> Result<Option<Resource>>;
    async fn get_pod_for_uid(&self, ns: &str, name: &str, uid: &str) -> Result<Option<Resource>>;
    async fn list_pods(
        &self,
        ns: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> Result<PodResourceList>;
    async fn list_pods_by_owner_uid(&self, ns: &str, owner_uid: &str) -> Result<Vec<Resource>>;
}

#[async_trait]
pub trait PodStatusWriter: Send + Sync {
    /// LEGACY: prefer `set_pod_status_for_uid`. This variant does not
    /// gate the write on pod UID. Production code MUST NOT use this —
    /// stale events for a deleted pod can fold into a same-name
    /// recreated pod and corrupt its status. Retained for legacy test
    /// scaffolding that doesn't have a stable UID at construction time.
    async fn set_pod_status(
        &self,
        ns: &str,
        name: &str,
        update: PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    /// UID-bound status write. All production callers MUST use this.
    async fn set_pod_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    /// LEGACY no-UID variant. Tests call this. Production MUST use
    /// `apply_runtime_reconcile_status_for_uid` to gate stale CRI
    /// events.
    async fn apply_runtime_reconcile_status(
        &self,
        ns: &str,
        name: &str,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    async fn apply_runtime_reconcile_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    /// UID-bound retry-status write for retryable StartPod failures
    /// (image pull, CNI readiness, transient CRI connectivity).
    ///
    /// Writes `containerStatuses[].state.waiting.reason` (escalating
    /// `ErrImagePull` → `ImagePullBackOff` for repeated pull failures) and
    /// `waiting.message` (the underlying error). Phase stays `Pending` so
    /// controller-owned pods are not counted as terminal.
    async fn mark_start_pending_for_retry_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        error_message: &str,
    ) -> Result<Resource>;

    /// LEGACY no-UID variant. Production MUST use
    /// `set_probe_readiness_for_uid`.
    async fn set_probe_readiness(
        &self,
        ns: &str,
        name: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    async fn set_probe_readiness_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    /// LEGACY no-UID variant. Production MUST use
    /// `set_deadline_exceeded_for_uid`.
    async fn set_deadline_exceeded(
        &self,
        ns: &str,
        name: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    async fn set_deadline_exceeded_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    /// Replace `status.ephemeralContainerStatuses` with the given slice
    /// while preserving the rest of `status`. Used by the runtime
    /// reconciler when CRI reports state for `kubectl debug`'s ephemeral
    /// containers. LEGACY no-UID; production uses `_for_uid` variant.
    async fn apply_ephemeral_container_statuses(
        &self,
        ns: &str,
        name: &str,
        statuses: Vec<Value>,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    async fn apply_ephemeral_container_statuses_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        statuses: Vec<Value>,
        expected_rv: Option<i64>,
    ) -> Result<Resource>;

    /// Bump `containerStatuses[name=container].restartCount` by 1 and
    /// stamp `lastState` with the supplied terminated descriptor.
    /// Returns `Ok(None)` if the container is not yet present in
    /// `containerStatuses` (next runtime reconcile will create it).
    /// LEGACY no-UID; production uses `note_container_restart_for_uid`.
    async fn note_container_restart(
        &self,
        ns: &str,
        name: &str,
        container_name: &str,
        terminated: Value,
        expected_rv: Option<i64>,
    ) -> Result<Option<Resource>>;

    async fn note_container_restart_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        terminated: Value,
        expected_rv: Option<i64>,
    ) -> Result<Option<Resource>>;
}

#[async_trait]
pub trait PodMetadataWriter: Send + Sync {
    async fn record_sandbox_id(&self, ns: &str, name: &str, sandbox_id: &str) -> Result<Resource>;

    async fn record_sandbox_id_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<Resource>;
}

#[async_trait]
pub trait PodObjectWriter: Send + Sync {
    /// Controller-driven Pod create. Internally delegates to
    /// `PodApiWriter::api_create_pod` with `run_admission=true,
    /// dry_run=false`.
    async fn create_controller_pod(
        &self,
        ns: &str,
        name: &str,
        _node_name: &str,
        pod: Value,
    ) -> Result<Resource>;

    async fn delete_pod(&self, ns: &str, name: &str) -> Result<()>;

    async fn update_pod_owner_references(
        &self,
        ns: &str,
        name: &str,
        owner_refs: Vec<Value>,
    ) -> Result<Resource>;

    /// UID-gated variant: fails if the live Pod UID does not match
    /// `expected_uid`, protecting same-name replacements.
    async fn update_pod_owner_references_for_uid(
        &self,
        ns: &str,
        name: &str,
        expected_uid: &str,
        owner_refs: Vec<Value>,
    ) -> Result<Resource> {
        let _ = expected_uid;
        self.update_pod_owner_references(ns, name, owner_refs).await
    }

    async fn merge_pod_labels(
        &self,
        ns: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource>;

    /// UID-gated variant: fails if the live Pod UID does not match
    /// `expected_uid`, protecting same-name replacements.
    async fn merge_pod_labels_for_uid(
        &self,
        ns: &str,
        name: &str,
        expected_uid: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource> {
        let _ = expected_uid;
        self.merge_pod_labels(ns, name, labels).await
    }
}

#[async_trait]
pub trait PodSubresourceWriter: Send + Sync {
    /// PUT `/api/v1/.../pods/{name}/status`
    async fn replace_status_from_api(
        &self,
        ns: &str,
        name: &str,
        status: Value,
        expected_rv: i64,
    ) -> Result<Resource>;

    /// UID-gated variant for same-name replacement protection.
    async fn replace_status_from_api_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        status: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        let _ = pod_uid;
        self.replace_status_from_api(ns, name, status, expected_rv)
            .await
    }

    /// PATCH `/api/v1/.../pods/{name}/status` — `patch_type` carries the
    /// request content type.
    async fn patch_status_from_api(
        &self,
        ns: &str,
        name: &str,
        patch: Value,
        patch_type: PodStatusPatchType,
        expected_rv: i64,
    ) -> Result<Resource>;

    /// UID-gated variant for same-name replacement protection.
    async fn patch_status_from_api_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        patch: Value,
        patch_type: PodStatusPatchType,
        expected_rv: i64,
    ) -> Result<Resource> {
        let _ = pod_uid;
        self.patch_status_from_api(ns, name, patch, patch_type, expected_rv)
            .await
    }

    /// PATCH `/api/v1/.../pods/{name}/ephemeralcontainers`
    async fn update_ephemeral_containers(
        &self,
        ns: &str,
        name: &str,
        containers: Vec<Value>,
        expected_rv: i64,
    ) -> Result<Resource>;

    /// UID-gated variant for same-name replacement protection.
    async fn update_ephemeral_containers_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        containers: Vec<Value>,
        expected_rv: i64,
    ) -> Result<Resource> {
        let _ = pod_uid;
        self.update_ephemeral_containers(ns, name, containers, expected_rv)
            .await
    }
}

#[async_trait]
pub trait PodNetworkReader: Send + Sync {
    /// Read the assignment CRI/CNI produced. `host_network=true` returns
    /// the host IP in both fields. Otherwise reads the `pod_network` row
    /// written by the klights CNI shim during containerd `RunPodSandbox`,
    /// waiting on the CNI assignment notification when the row is not visible
    /// on the first read.
    async fn read_pod_network_assignment(
        &self,
        sandbox_id: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        host_network: bool,
    ) -> Result<PodNetworkAssignment>;
}

#[cfg(test)]
pub trait PodWatchSource: Send + Sync {
    fn subscribe_pod_watch(&self) -> broadcast::Receiver<WatchEvent>;
}

#[async_trait]
pub trait PodApiWriter: Send + Sync {
    async fn api_create_pod(
        &self,
        request: PodApiCreateRequest,
    ) -> std::result::Result<PodApiCreateResult, PodRepositoryError>;

    async fn api_update_pod(
        &self,
        ns: &str,
        name: &str,
        body: Value,
        current: Resource,
        dry_run: bool,
    ) -> std::result::Result<PodApiUpdateOutcome, PodRepositoryError>;

    async fn api_patch_pod(
        &self,
        ns: &str,
        name: &str,
        patch: Value,
        patch_type: PodStatusPatchType,
        dry_run: bool,
    ) -> std::result::Result<PodApiUpdateOutcome, PodRepositoryError>;

    async fn api_delete_pod<O>(
        &self,
        ns: &str,
        name: &str,
        options: O,
        dry_run: bool,
    ) -> std::result::Result<PodApiDeleteOutcome, PodRepositoryError>
    where
        O: Into<PodDeleteOptions> + Send;

    async fn api_delete_collection_pods(
        &self,
        ns: &str,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        dry_run: bool,
    ) -> std::result::Result<(), PodRepositoryError>;
}

/// Eight-trait pod persistence repository. Constructed once at process
/// startup by `ApiState`, then shared by every consumer behind narrow
/// trait references.
pub struct PodRepository {
    store: Arc<PodStore>,
    status: PodStatusService,
    objects: PodObjectService,
    #[cfg(test)]
    test_subresource: Option<Arc<dyn klights_pod_api::PodSubresourceMutation>>,
    network_svc: PodNetworkService,
    _watch: PodWatchService,
    #[cfg(test)]
    test_api: Option<Arc<dyn klights_pod_api::PodApiMutation>>,
    #[cfg(test)]
    test_scheduling: Option<Arc<dyn klights_pod_api::PodScheduling>>,
    #[cfg(test)]
    test_mark_terminating: Option<Arc<dyn klights_pod_api::PodMarkTerminating>>,
    gc_delete: Arc<dyn GcPodDeleteSink>,
    eviction_admission: Arc<dyn PodEvictionAdmissionSink>,
    namespace_bootstrap: Arc<dyn klights_reconcile_api::NamespaceBootstrapSink>,
    workqueue: Arc<PodWorkqueue>,
    namespace_termination: Arc<dyn klights_reconcile_api::NamespaceTerminationSink>,
    mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    supervisor: Arc<TaskSupervisor>,
    outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
    cluster_api: Option<Arc<dyn LeaderResourceQuery>>,
    host_ip: crate::kubelet::context::HostIpState,
    #[cfg(test)]
    deletion_finalizer: Arc<dyn PodDeletionFinalizer>,
}

impl PodRepository {
    #[cfg(test)]
    pub(crate) fn test_root_api_services(
        &self,
    ) -> (
        Arc<dyn klights_pod_api::PodApiMutation>,
        Arc<dyn klights_pod_api::PodSubresourceMutation>,
    ) {
        (
            self.test_api
                .clone()
                .expect("test repository requires a root Pod API adapter"),
            self.test_subresource
                .clone()
                .expect("test repository requires a root Pod subresource adapter"),
        )
    }

    pub(crate) fn mutation_reconcile_port(
        &self,
    ) -> Arc<dyn klights_reconcile_api::PodMutationReconcileSink> {
        self.mutation_reconcile.clone()
    }

    pub(crate) fn eviction_admission_port(
        &self,
    ) -> Arc<dyn klights_reconcile_api::PodEvictionAdmissionSink> {
        self.eviction_admission.clone()
    }

    pub(crate) fn namespace_bootstrap_port(
        &self,
    ) -> Arc<dyn klights_reconcile_api::NamespaceBootstrapSink> {
        self.namespace_bootstrap.clone()
    }
}

#[derive(Clone)]
struct PodDeletionFinalizerDependencies {
    store: Arc<PodStore>,
    gc_pod_delete_sink: Arc<dyn GcPodDeleteSink>,
    gc_reconcile: Arc<dyn PodGcReconcileSink>,
    pdb_reconcile: Arc<dyn PodPdbReconcileSink>,
    namespace_termination: Arc<dyn klights_reconcile_api::NamespaceTerminationSink>,
    cluster_api: Option<Arc<dyn LeaderResourceQuery>>,
    outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
    bound_pod_finalization: Arc<dyn klights_pod_api::BoundPodFinalization>,
    mutation_reconcile: Arc<dyn klights_reconcile_api::PodMutationReconcileSink>,
    metrics: Arc<dyn klights_reconcile_api::ReconcileFailureMetrics>,
    supervisor: Arc<TaskSupervisor>,
    deferred_runtime: status::DeferredRuntimeReducerHandle,
}

/// Finalizer decorator that releases repository-private deferred runtime state
/// only after the actor-owned deletion boundary reports a terminal outcome.
/// Pending finalizers and errors retain the observation for the actor retry.
struct DeferredRuntimeCleanupFinalizer {
    inner: Arc<dyn PodDeletionFinalizer>,
    deferred_runtime: status::DeferredRuntimeReducerHandle,
}

impl DeferredRuntimeCleanupFinalizer {
    fn new(
        inner: Arc<dyn PodDeletionFinalizer>,
        deferred_runtime: status::DeferredRuntimeReducerHandle,
    ) -> Self {
        Self {
            inner,
            deferred_runtime,
        }
    }
}

fn compose_pod_deletion_finalizer(
    dependencies: PodDeletionFinalizerDependencies,
) -> Arc<dyn PodDeletionFinalizer> {
    let runtime_deletion_finalizer =
        crate::kubelet::pod_runtime::deletion_finalizer::compose_real_pod_deletion_finalizer(
            crate::kubelet::pod_runtime::deletion_finalizer::RealPodDeletionFinalizerDependencies {
                store: dependencies.store,
                gc_pod_delete_sink: dependencies.gc_pod_delete_sink,
                gc_reconcile: dependencies.gc_reconcile,
                pdb_reconcile: dependencies.pdb_reconcile,
                namespace_termination: dependencies.namespace_termination,
                cluster_api: dependencies.cluster_api,
                outbox: dependencies.outbox,
                bound_pod_finalization: dependencies.bound_pod_finalization,
                mutation_reconcile: dependencies.mutation_reconcile,
                metrics: dependencies.metrics,
                supervisor: dependencies.supervisor,
            },
        );
    Arc::new(DeferredRuntimeCleanupFinalizer::new(
        runtime_deletion_finalizer,
        dependencies.deferred_runtime,
    ))
}

#[async_trait]
impl PodDeletionFinalizer for DeferredRuntimeCleanupFinalizer {
    async fn finalize_after_actor_cleanup(
        &self,
        key: &crate::kubelet::pod_runtime::service::PodRuntimeKey,
    ) -> Result<PodDeletionFinalizeResult> {
        let result = self.inner.finalize_after_actor_cleanup(key).await?;
        if matches!(
            result,
            PodDeletionFinalizeResult::DeletedOrAlreadyGone | PodDeletionFinalizeResult::Queued
        ) {
            self.deferred_runtime.forget(&key.uid);
        }
        Ok(result)
    }
}

#[derive(Debug)]
pub(crate) struct PodUidMismatch {
    pub(crate) expected: String,
    pub(crate) actual: String,
    namespace: String,
    name: String,
}

impl std::fmt::Display for PodUidMismatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Pod {}/{} UID mismatch: expected {}, found {}",
            self.namespace,
            self.name,
            self.expected,
            if self.actual.is_empty() {
                "<empty>"
            } else {
                &self.actual
            }
        )
    }
}

impl std::error::Error for PodUidMismatch {}

pub(crate) fn ensure_pod_uid_matches(
    data: &Value,
    expected_uid: &str,
    ns: &str,
    name: &str,
) -> Result<()> {
    let live_uid = data
        .pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if live_uid == expected_uid {
        return Ok(());
    }

    Err(PodUidMismatch {
        expected: expected_uid.to_string(),
        actual: live_uid.to_string(),
        namespace: ns.to_string(),
        name: name.to_string(),
    }
    .into())
}

impl PodRepository {
    #[cfg(test)]
    pub fn new(
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        side_effects: Arc<SideEffectRegistry>,
        metrics: Arc<SideEffectMetrics>,
    ) -> Self {
        Self::new_with_scheduling_mode(
            db,
            supervisor,
            side_effects,
            metrics,
            PodSchedulingMode::InlineSingleNode,
        )
    }

    pub fn sandbox_gc_dirty_counter(&self) -> Arc<std::sync::atomic::AtomicUsize> {
        self.store.sandbox_gc_dirty.clone()
    }

    pub(crate) fn host_ip_state(&self) -> crate::kubelet::context::HostIpState {
        self.host_ip.clone()
    }

    pub fn outbox(&self) -> Option<&crate::kubelet::outbox::Outbox> {
        self.outbox.as_deref()
    }

    #[cfg(test)]
    pub fn new_with_scheduling_mode(
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        side_effects: Arc<SideEffectRegistry>,
        metrics: Arc<SideEffectMetrics>,
        scheduling_mode: PodSchedulingMode,
    ) -> Self {
        Self::new_with_scheduling_mode_and_outbox(
            db,
            supervisor,
            side_effects,
            metrics,
            scheduling_mode,
            None,
        )
    }

    #[cfg(test)]
    pub fn new_with_scheduling_mode_and_outbox(
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        side_effects: Arc<SideEffectRegistry>,
        metrics: Arc<SideEffectMetrics>,
        scheduling_mode: PodSchedulingMode,
        outbox: Option<Arc<crate::node_outbox::Outbox>>,
    ) -> Self {
        let network_cache = empty_test_pod_network_cache();
        let assignment_bus = test_assignment_bus();
        Self::new_with_network_events(
            db,
            supervisor,
            side_effects,
            metrics,
            network_cache,
            assignment_bus,
            scheduling_mode,
            outbox,
        )
    }

    #[cfg(test)]
    pub fn new_with_scheduling_mode_outbox_and_cluster_api(
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        side_effects: Arc<SideEffectRegistry>,
        metrics: Arc<SideEffectMetrics>,
        scheduling_mode: PodSchedulingMode,
        outbox: Option<Arc<crate::node_outbox::Outbox>>,
        cluster_api: Arc<dyn LeaderResourceQuery>,
    ) -> Self {
        let pod_network_cache = empty_test_pod_network_cache();
        let assignment_waiter = test_assignment_bus();
        Self::new_with_network_events_and_cluster_api(PodRepositoryBuildConfig {
            db,
            node_local: None,
            supervisor,
            side_effects,
            metrics,
            pod_network_cache,
            assignment_waiter,
            scheduling_mode,
            outbox,
            cluster_api: Some(cluster_api),
            scheduler_bind_gate: None,
        })
    }

    #[cfg(test)]
    pub fn new_with_network_events(
        db: DatastoreHandle,
        supervisor: Arc<TaskSupervisor>,
        side_effects: Arc<SideEffectRegistry>,
        metrics: Arc<SideEffectMetrics>,
        pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
        assignment_waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
        scheduling_mode: PodSchedulingMode,
        outbox: Option<Arc<crate::node_outbox::Outbox>>,
    ) -> Self {
        Self::new_with_network_events_and_cluster_api(PodRepositoryBuildConfig {
            db,
            node_local: None,
            supervisor,
            side_effects,
            metrics,
            pod_network_cache,
            assignment_waiter,
            scheduling_mode,
            outbox,
            cluster_api: None,
            scheduler_bind_gate: None,
        })
    }

    #[cfg(test)]
    fn new_with_network_events_and_cluster_api(config: PodRepositoryBuildConfig) -> Self {
        let parts = Self::build_parts(config);
        // Ordinary constructors retain lazy queue startup on first enqueue.
        parts.repository
    }

    /// Build `PodRepository` and its deferred-startup services without
    /// calling `workqueue.start()`. The returned `PodRepositoryBackground`
    /// must be started after lifecycle wiring is complete (Task 4.2).
    #[cfg(test)]
    pub fn build_parts(config: PodRepositoryBuildConfig) -> facade::PodRepositoryParts {
        crate::pod_repository_composition::build_pod_repository_parts(config, None).repository_parts
    }

    pub(crate) fn build_parts_with_adapters(
        core: PodRepositoryCoreDependencies,
        runtime: PodRepositoryRuntimeDependencies,
        network: PodRepositoryNetworkDependencies,
        delivery: PodRepositoryDeliveryDependencies,
        adapters: PodRepositoryAdapters,
    ) -> facade::PodRepositoryParts {
        let PodRepositoryCoreDependencies {
            store,
            status_only,
            workqueue,
        } = core;
        let PodRepositoryRuntimeDependencies {
            supervisor,
            metrics,
            wall_clock,
        } = runtime;
        let PodRepositoryNetworkDependencies {
            pod_network_cache,
            assignment_waiter,
        } = network;
        let PodRepositoryDeliveryDependencies {
            outbox,
            cluster_api,
            bound_pod_finalization,
        } = delivery;
        workqueue.set_namespace_termination_sink(adapters.namespace_termination.clone());
        let gc_reconcile = adapters.gc_reconcile;
        let pdb_reconcile = adapters.pdb_reconcile;
        let eviction_admission = adapters.eviction_admission;
        let namespace_bootstrap = adapters.namespace_bootstrap;
        let namespace_termination = adapters.namespace_termination;
        let mutation_reconcile = adapters.mutation_reconcile;
        let host_ip = crate::kubelet::context::HostIpState::default();
        let status = PodStatusService::new(
            store.clone(),
            status_only.clone(),
            mutation_reconcile.clone(),
            outbox.clone(),
            cluster_api.clone(),
            host_ip.clone(),
            wall_clock.clone(),
        );
        let objects = PodObjectService::new(
            store.clone(),
            mutation_reconcile.clone(),
            outbox.clone(),
            cluster_api.clone(),
            wall_clock,
        );
        let network_svc = PodNetworkService::new(
            pod_network_cache,
            supervisor.clone(),
            assignment_waiter,
            host_ip.clone(),
        );
        let watch = PodWatchService::new(store.clone());
        let gc_pod_delete_sink = adapters.gc_delete.clone();
        workqueue.set_remote_pod_delete_resignal_sink(Arc::downgrade(&gc_pod_delete_sink));

        let deletion_finalizer_dependencies = PodDeletionFinalizerDependencies {
            store: store.clone(),
            gc_pod_delete_sink,
            gc_reconcile,
            pdb_reconcile: pdb_reconcile.clone(),
            namespace_termination: namespace_termination.clone(),
            cluster_api: cluster_api.clone(),
            outbox: outbox.clone(),
            bound_pod_finalization,
            mutation_reconcile: mutation_reconcile.clone(),
            metrics: metrics.clone(),
            supervisor: supervisor.clone(),
            deferred_runtime: status.deferred_runtime_handle(),
        };
        #[cfg(test)]
        let deletion_finalizer =
            compose_pod_deletion_finalizer(deletion_finalizer_dependencies.clone());

        let repository = Self {
            store,
            status,
            objects,
            #[cfg(test)]
            test_subresource: adapters.test_subresource,
            network_svc,
            _watch: watch,
            #[cfg(test)]
            test_api: adapters.test_api,
            #[cfg(test)]
            test_scheduling: adapters.test_scheduling,
            #[cfg(test)]
            test_mark_terminating: adapters.test_mark_terminating,
            gc_delete: adapters.gc_delete,
            eviction_admission,
            namespace_bootstrap,
            workqueue: workqueue.clone(),
            namespace_termination: namespace_termination.clone(),
            mutation_reconcile,
            supervisor,
            outbox,
            cluster_api,
            host_ip,
            #[cfg(test)]
            deletion_finalizer,
        };
        let background = PodRepositoryBackground::new(workqueue);
        facade::PodRepositoryParts::new(repository, background, deletion_finalizer_dependencies)
    }

    #[cfg(test)]
    pub async fn finalize_pod_deletion_after_actor_cleanup(
        &self,
        ns: &str,
        name: &str,
        uid: &str,
    ) -> Result<bool> {
        let key = crate::kubelet::pod_runtime::service::PodRuntimeKey::new(ns, name, uid);
        match self
            .deletion_finalizer
            .finalize_after_actor_cleanup(&key)
            .await?
        {
            PodDeletionFinalizeResult::DeletedOrAlreadyGone => Ok(true),
            PodDeletionFinalizeResult::Queued => Ok(false),
            PodDeletionFinalizeResult::FinalizersPending => Ok(false),
        }
    }

    #[cfg(test)]
    pub fn deletion_finalizer(
        &self,
    ) -> Arc<dyn crate::kubelet::pod_runtime::deletion_finalizer::PodDeletionFinalizer> {
        self.deletion_finalizer.clone()
    }

    pub async fn enqueue_actor_deletes_for_terminating_namespace(
        &self,
        namespace: &str,
    ) -> Result<()> {
        self.workqueue
            .enqueue_actor_deletes_for_terminating_namespace(namespace)
            .await
    }

    pub fn set_pod_lifecycle_router_for_node(
        &self,
        router: Arc<crate::kubelet::pod_lifecycle_router::PodLifecycleRouter>,
        local_node_name: String,
    ) {
        self.workqueue
            .set_lifecycle_router_for_node(router, local_node_name);
    }

    pub async fn enqueue_namespace_termination(
        &self,
        namespace: String,
        uid: String,
    ) -> Result<()> {
        self.workqueue
            .enqueue_namespace_termination(namespace, uid)
            .await
    }

    /// Spawn async namespace-termination reconciliation after a pod status or
    /// metadata write.
    ///
    /// Both operations are derived-state maintenance that must not block
    /// the caller (kubelet status writer, controller pod writer). The
    /// spawned task runs on the TaskSupervisor under `Background` so it
    /// is visible on the admin diagnostics API and participates in
    /// graceful shutdown.
    async fn spawn_post_write_maintenance(&self, namespace: &str) {
        let namespace_termination = self.namespace_termination.clone();
        let ns = namespace.to_string();
        let _ = self
            .supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                format!("post_write_maintenance/{ns}"),
                async move {
                    if let Err(err) = namespace_termination
                        .reconcile_namespace_termination(
                            klights_reconcile_api::NamespaceTerminationRequest {
                                namespace: ns.clone(),
                                expected_uid: None,
                            },
                        )
                        .await
                    {
                        tracing::warn!(
                            namespace = %ns,
                            error = ?err,
                            "post-write namespace termination reconcile failed"
                        );
                    }
                },
            )
            .await;
    }

    async fn finish_status_write(
        &self,
        namespace: &str,
        result: status::PodStatusWriteResult,
        context: &'static str,
    ) -> Resource {
        let resource = result.resource;
        if result.changed {
            if result.endpoint_state_changed {
                let _ = self
                    .mutation_reconcile
                    .reconcile_pod_mutation(
                        klights_reconcile_api::PodMutationReconcileRequest::RunHooks {
                            pod: resource.clone(),
                            named_hook: Some("pdb_reconcile"),
                            context,
                        },
                    )
                    .await;
            }
            self.spawn_post_write_maintenance(namespace).await;
        }
        resource
    }

    #[cfg(test)]
    pub async fn schedule_pending_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.test_scheduling
            .as_deref()
            .expect("test scheduler requires the neutral Pod scheduling port")
            .schedule_pending_pod(namespace.to_string(), name.to_string())
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))
    }

    #[cfg(test)]
    pub async fn bind_pod_from_api(
        &self,
        namespace: &str,
        name: &str,
        binding: serde_json::Value,
        dry_run: bool,
    ) -> Result<()> {
        self.test_api
            .as_deref()
            .expect("test bind requires the neutral Pod API port")
            .bind_pod(klights_pod_api::PodBindingRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                binding,
                dry_run,
            })
            .await
            .map_err(|error| anyhow::anyhow!("{error:?}"))
    }

    #[cfg(test)]
    pub async fn schedule_all_unbound_pods(&self) -> Result<()> {
        self.test_scheduling
            .as_deref()
            .expect("test scheduler requires the neutral Pod scheduling port")
            .schedule_all_unbound_pods()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))
    }

    /// Enqueue the owning Job for asynchronous reconciliation after a pod
    /// reaches a terminal phase or is marked failed.
    ///
    /// This replaces the old synchronous `reconcile_job_for_pod_owner` path
    /// that called `controllers::job::reconcile_job()` inline, blocking the
    /// pod watcher. The async enqueue gives the Job controller exponential
    /// backoff retry and keeps the watcher responsive.
    ///
    /// No-op when the pod has no Job owner or when the controller dispatcher
    /// is not yet bound.
    pub async fn enqueue_job_reconcile_for_pod(&self, pod: &Value) {
        if let Err(err) = self
            .mutation_reconcile
            .reconcile_pod_mutation(
                klights_reconcile_api::PodMutationReconcileRequest::EnqueueJobOwner {
                    pod: Resource::from_data_lossy(Arc::new(pod.clone())),
                },
            )
            .await
        {
            tracing::warn!(error = %err, "failed to enqueue Job reconcile for terminal Pod");
        }
    }
}

impl PodRepository {
    /// Overlay the node-local status checkpoint onto a worker fresh read so the
    /// worker observes its OWN just-written status (read-your-own-write).
    ///
    /// A worker's status writes propagate to the leader asynchronously through
    /// the outbox. Under real inter-node latency a plain leader read-back races
    /// ahead of that write landing and returns a stale phase, which made
    /// `finalize_startup` loop on `Unconfirmed` (and similarly stalled the
    /// deletion confirm path), slowing status convergence and foreground-GC
    /// deletion to the point of conformance-test timeout on a two-VM cluster.
    ///
    /// This is the same merge the status read path already performs
    /// (`PodStatusService::read_current_pod`). The checkpoint only ever reflects
    /// state the worker itself authored and self-clears once the leader catches
    /// up, so it can never surface more than the worker already knows. Only used
    /// on the worker (cluster_api set); the leader reads the cluster store
    /// directly and needs no overlay.
    async fn overlay_local_status_checkpoint(
        &self,
        pod: Option<Resource>,
    ) -> Result<Option<Resource>> {
        match (pod, &self.outbox) {
            (Some(pod), Some(outbox)) => Ok(Some(outbox.merge_pod_status_checkpoint(pod).await?)),
            (other, _) => Ok(other),
        }
    }
}

#[async_trait]
impl PodReader for PodRepository {
    async fn get_pod(&self, ns: &str, name: &str) -> Result<Option<Resource>> {
        if let Some(cluster_api) = &self.cluster_api {
            // Kubelet lifecycle and probe decisions need the current single-pod
            // status. A stale worker informer-cache hit can keep probes behind
            // the startup initialDelay gate after the container is already
            // Running, so use the internal fresh read path here, then overlay
            // the node-local checkpoint so the worker reads its own writes.
            let pod = cluster_api
                .get_resource(ResourceGetRequest::try_new(
                    pod_resource_key(ns, name),
                    ResourceQueryConsistency::LeaderFresh,
                )?)
                .await?;
            return self.overlay_local_status_checkpoint(pod).await;
        }
        self.store.get(ns, name).await
    }

    async fn get_pod_for_uid(&self, ns: &str, name: &str, uid: &str) -> Result<Option<Resource>> {
        if let Some(cluster_api) = &self.cluster_api {
            let pod = cluster_api
                .get_resource(ResourceGetRequest::try_new(
                    pod_resource_key(ns, name),
                    ResourceQueryConsistency::LeaderFresh,
                )?)
                .await?
                .filter(|pod| pod.uid == uid);
            return self.overlay_local_status_checkpoint(pod).await;
        }
        Ok(self.store.get(ns, name).await?.filter(|pod| pod.uid == uid))
    }

    async fn list_pods(
        &self,
        ns: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> Result<PodResourceList> {
        if let Some(cluster_api) = &self.cluster_api {
            let list = cluster_api
                .list_resources(ResourceListRequest::try_new(
                    "v1",
                    "Pod",
                    ns.map(str::to_string),
                    label_selector.map(str::to_string),
                    field_selector.map(str::to_string),
                    limit,
                    continue_token.map(str::to_string),
                    ResourceQueryConsistency::LeaderFresh,
                )?)
                .await?;
            return Ok(resource_list_from_leader(list));
        }
        self.store
            .list(ns, label_selector, field_selector, limit, continue_token)
            .await
    }
    async fn list_pods_by_owner_uid(&self, ns: &str, owner_uid: &str) -> Result<Vec<Resource>> {
        if self.cluster_api.is_some() {
            let pods = self.list_pods(Some(ns), None, None, None, None).await?;
            return Ok(pods
                .items
                .into_iter()
                .filter(|pod| pod_has_owner_uid(&pod.data, owner_uid))
                .collect());
        }
        self.store.list_by_owner(ns, owner_uid).await
    }
}

fn pod_resource_key(ns: &str, name: &str) -> ResourceKey {
    ResourceKey {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some(ns.to_string()),
        name: name.to_string(),
    }
}

fn pod_has_owner_uid(pod: &Value, owner_uid: &str) -> bool {
    pod.pointer("/metadata/ownerReferences")
        .and_then(|owners| owners.as_array())
        .is_some_and(|owners| {
            owners
                .iter()
                .any(|owner| owner.get("uid").and_then(|uid| uid.as_str()) == Some(owner_uid))
        })
}

#[async_trait]
impl PodStatusWriter for PodRepository {
    async fn set_pod_status(
        &self,
        ns: &str,
        name: &str,
        update: PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .set_pod_status(ns, name, &update, expected_rv)
            .await?;
        Ok(self.finish_status_write(ns, result, "pod_status_set").await)
    }

    async fn set_pod_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .set_pod_status_for_uid(ns, name, pod_uid, update, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_status_set_uid")
            .await)
    }

    async fn apply_runtime_reconcile_status(
        &self,
        ns: &str,
        name: &str,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .apply_runtime_reconcile_status(ns, name, update, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_runtime_reconcile_status")
            .await)
    }

    async fn apply_runtime_reconcile_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .apply_runtime_reconcile_status_for_uid(ns, name, pod_uid, update, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_runtime_reconcile_status_uid")
            .await)
    }

    async fn mark_start_pending_for_retry_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        error_message: &str,
    ) -> Result<Resource> {
        let result = self
            .status
            .mark_start_pending_for_retry_for_uid(ns, name, pod_uid, error_message)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_start_pending_retry")
            .await)
    }

    async fn set_probe_readiness(
        &self,
        ns: &str,
        name: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .set_probe_readiness(ns, name, container_name, ready, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_probe_readiness")
            .await)
    }

    async fn set_probe_readiness_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .set_probe_readiness_for_uid(ns, name, pod_uid, container_name, ready, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_probe_readiness_uid")
            .await)
    }
    async fn set_deadline_exceeded(
        &self,
        ns: &str,
        name: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .set_deadline_exceeded(ns, name, message, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_deadline_exceeded")
            .await)
    }

    async fn set_deadline_exceeded_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .set_deadline_exceeded_for_uid(ns, name, pod_uid, message, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_deadline_exceeded_uid")
            .await)
    }
    async fn apply_ephemeral_container_statuses(
        &self,
        ns: &str,
        name: &str,
        statuses: Vec<Value>,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .apply_ephemeral_container_statuses(ns, name, statuses, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_ephemeral_container_statuses")
            .await)
    }

    async fn apply_ephemeral_container_statuses_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        statuses: Vec<Value>,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let result = self
            .status
            .apply_ephemeral_container_statuses_for_uid(ns, name, pod_uid, statuses, expected_rv)
            .await?;
        Ok(self
            .finish_status_write(ns, result, "pod_ephemeral_container_statuses_uid")
            .await)
    }
    async fn note_container_restart(
        &self,
        ns: &str,
        name: &str,
        container_name: &str,
        terminated: Value,
        expected_rv: Option<i64>,
    ) -> Result<Option<Resource>> {
        let updated = self
            .status
            .note_container_restart(ns, name, container_name, terminated, expected_rv)
            .await?;
        if updated.is_some() {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(updated)
    }

    async fn note_container_restart_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        terminated: Value,
        expected_rv: Option<i64>,
    ) -> Result<Option<Resource>> {
        let updated = self
            .status
            .note_container_restart_for_uid(
                ns,
                name,
                pod_uid,
                container_name,
                terminated,
                expected_rv,
            )
            .await?;
        if updated.is_some() {
            self.spawn_post_write_maintenance(ns).await;
        }
        Ok(updated)
    }
}

#[async_trait]
impl PodMetadataWriter for PodRepository {
    async fn record_sandbox_id(&self, ns: &str, name: &str, sandbox_id: &str) -> Result<Resource> {
        let updated = self.objects.record_sandbox_id(ns, name, sandbox_id).await?;
        self.spawn_post_write_maintenance(ns).await;
        Ok(updated)
    }

    async fn record_sandbox_id_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        sandbox_id: &str,
    ) -> Result<Resource> {
        let updated = self
            .objects
            .record_sandbox_id_for_uid(ns, name, pod_uid, sandbox_id)
            .await?;
        self.spawn_post_write_maintenance(ns).await;
        Ok(updated)
    }
}

#[async_trait]
impl PodObjectWriter for PodRepository {
    async fn create_controller_pod(
        &self,
        ns: &str,
        name: &str,
        _node_name: &str,
        pod: Value,
    ) -> Result<Resource> {
        #[cfg(test)]
        {
            let result = self
                .test_api
                .as_deref()
                .expect("test controller create requires the neutral Pod API port")
                .create_pod(klights_pod_api::PodApiCreateRequest {
                    namespace: ns.to_string(),
                    body: pod,
                    dry_run: false,
                })
                .await
                .map_err(|error| anyhow::anyhow!("{error:?}"))?;
            let created = result.resource.ok_or_else(|| {
                anyhow::anyhow!("controller pod {ns}/{name} create returned dry-run")
            })?;
            self.spawn_post_write_maintenance(ns).await;
            return Ok(created);
        }
        #[cfg(not(test))]
        {
            let _ = (ns, name, _node_name, pod);
            Err(anyhow::anyhow!(
                "controller Pod creation is owned by the root Pod API adapter"
            ))
        }
    }
    async fn delete_pod(&self, ns: &str, name: &str) -> Result<()> {
        let Some(current) = self.store.get(ns, name).await? else {
            return Ok(());
        };
        self.gc_delete
            .request_gc_pod_delete(GcPodDeleteRequest::new(PodIdentity::new(
                ns,
                name,
                &current.uid,
            )))
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let _ = self
            .mutation_reconcile
            .reconcile_pod_mutation(
                klights_reconcile_api::PodMutationReconcileRequest::RunHooks {
                    pod: current,
                    named_hook: None,
                    context: "pod_object_mark_terminating",
                },
            )
            .await;
        Ok(())
    }
    async fn update_pod_owner_references(
        &self,
        ns: &str,
        name: &str,
        owner_refs: Vec<Value>,
    ) -> Result<Resource> {
        let updated = self
            .objects
            .update_pod_owner_references(ns, name, owner_refs)
            .await?;
        self.spawn_post_write_maintenance(ns).await;
        Ok(updated)
    }

    async fn update_pod_owner_references_for_uid(
        &self,
        ns: &str,
        name: &str,
        expected_uid: &str,
        owner_refs: Vec<Value>,
    ) -> Result<Resource> {
        let updated = self
            .objects
            .update_pod_owner_references_for_uid(ns, name, expected_uid, owner_refs)
            .await?;
        self.spawn_post_write_maintenance(ns).await;
        Ok(updated)
    }

    async fn merge_pod_labels(
        &self,
        ns: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource> {
        let updated = self.objects.merge_pod_labels(ns, name, labels).await?;
        self.spawn_post_write_maintenance(ns).await;
        Ok(updated)
    }

    async fn merge_pod_labels_for_uid(
        &self,
        ns: &str,
        name: &str,
        expected_uid: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource> {
        let updated = self
            .objects
            .merge_pod_labels_for_uid(ns, name, expected_uid, labels)
            .await?;
        self.spawn_post_write_maintenance(ns).await;
        Ok(updated)
    }
}

#[cfg(test)]
#[async_trait]
impl PodSubresourceWriter for PodRepository {
    async fn replace_status_from_api(
        &self,
        ns: &str,
        name: &str,
        status: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        let updated = self
            .test_subresource
            .as_deref()
            .expect("test status replace requires the neutral Pod subresource port")
            .replace_status(klights_pod_api::PodStatusReplaceRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                expected_uid: None,
                status,
                expected_resource_version: expected_rv,
            })
            .await?;
        let _ = self
            .mutation_reconcile
            .reconcile_pod_mutation(
                klights_reconcile_api::PodMutationReconcileRequest::RunHooks {
                    pod: updated.clone(),
                    named_hook: None,
                    context: "pod_status_subresource_replace",
                },
            )
            .await;
        Ok(updated)
    }
    async fn replace_status_from_api_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        status: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        let updated = self
            .test_subresource
            .as_deref()
            .expect("test status replace requires the neutral Pod subresource port")
            .replace_status(klights_pod_api::PodStatusReplaceRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                expected_uid: Some(pod_uid.to_string()),
                status,
                expected_resource_version: expected_rv,
            })
            .await?;
        let _ = self
            .mutation_reconcile
            .reconcile_pod_mutation(
                klights_reconcile_api::PodMutationReconcileRequest::RunHooks {
                    pod: updated.clone(),
                    named_hook: None,
                    context: "pod_status_subresource_replace",
                },
            )
            .await;
        Ok(updated)
    }
    async fn patch_status_from_api(
        &self,
        ns: &str,
        name: &str,
        patch: Value,
        patch_type: PodStatusPatchType,
        expected_rv: i64,
    ) -> Result<Resource> {
        let updated = self
            .test_subresource
            .as_deref()
            .expect("test status patch requires the neutral Pod subresource port")
            .patch_status(klights_pod_api::PodStatusPatchRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                patch,
                patch_kind: match patch_type {
                    PodStatusPatchType::JsonPatch => klights_pod_api::PodStatusPatchKind::JsonPatch,
                    PodStatusPatchType::MergePatch => {
                        klights_pod_api::PodStatusPatchKind::MergePatch
                    }
                    PodStatusPatchType::StrategicMerge => {
                        klights_pod_api::PodStatusPatchKind::StrategicMerge
                    }
                    PodStatusPatchType::ApplyPatch => {
                        klights_pod_api::PodStatusPatchKind::ApplyPatch
                    }
                },
                expected_resource_version: expected_rv,
            })
            .await?;
        let _ = self
            .mutation_reconcile
            .reconcile_pod_mutation(
                klights_reconcile_api::PodMutationReconcileRequest::RunHooks {
                    pod: updated.clone(),
                    named_hook: None,
                    context: "pod_status_subresource_patch",
                },
            )
            .await;
        Ok(updated)
    }
    async fn update_ephemeral_containers(
        &self,
        ns: &str,
        name: &str,
        containers: Vec<Value>,
        expected_rv: i64,
    ) -> Result<Resource> {
        self.test_subresource
            .as_deref()
            .expect("test ephemeral-container update requires the neutral Pod subresource port")
            .update_ephemeral_containers(klights_pod_api::PodEphemeralContainersRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                containers,
                expected_resource_version: expected_rv,
            })
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
impl klights_pod_api::PodSubresourceMutation for PodRepository {
    fn replace_status(
        &self,
        request: klights_pod_api::PodStatusReplaceRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            let klights_pod_api::PodStatusReplaceRequest {
                namespace,
                name,
                expected_uid,
                status,
                expected_resource_version,
            } = request;
            match expected_uid {
                Some(uid) => {
                    PodSubresourceWriter::replace_status_from_api_for_uid(
                        self,
                        &namespace,
                        &name,
                        &uid,
                        status,
                        expected_resource_version,
                    )
                    .await
                }
                None => {
                    PodSubresourceWriter::replace_status_from_api(
                        self,
                        &namespace,
                        &name,
                        status,
                        expected_resource_version,
                    )
                    .await
                }
            }
            .map_err(|error| ordinary_access::map_repository_error(error, &namespace, &name))
        })
    }

    fn patch_status(
        &self,
        request: klights_pod_api::PodStatusPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        let klights_pod_api::PodStatusPatchRequest {
            namespace,
            name,
            patch,
            patch_kind,
            expected_resource_version,
        } = request;
        let patch_type = match patch_kind {
            klights_pod_api::PodStatusPatchKind::JsonPatch => PodStatusPatchType::JsonPatch,
            klights_pod_api::PodStatusPatchKind::MergePatch => PodStatusPatchType::MergePatch,
            klights_pod_api::PodStatusPatchKind::StrategicMerge => {
                PodStatusPatchType::StrategicMerge
            }
            klights_pod_api::PodStatusPatchKind::ApplyPatch => PodStatusPatchType::ApplyPatch,
        };
        Box::pin(async move {
            PodSubresourceWriter::patch_status_from_api(
                self,
                &namespace,
                &name,
                patch,
                patch_type,
                expected_resource_version,
            )
            .await
            .map_err(|error| ordinary_access::map_repository_error(error, &namespace, &name))
        })
    }

    fn update_ephemeral_containers(
        &self,
        request: klights_pod_api::PodEphemeralContainersRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            let klights_pod_api::PodEphemeralContainersRequest {
                namespace,
                name,
                containers,
                expected_resource_version,
            } = request;
            PodSubresourceWriter::update_ephemeral_containers(
                self,
                &namespace,
                &name,
                containers,
                expected_resource_version,
            )
            .await
            .map_err(|error| ordinary_access::map_repository_error(error, &namespace, &name))
        })
    }
}

#[async_trait]
impl PodNetworkReader for PodRepository {
    async fn read_pod_network_assignment(
        &self,
        sandbox_id: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        host_network: bool,
    ) -> Result<PodNetworkAssignment> {
        self.network_svc
            .read_pod_network_assignment(sandbox_id, namespace, pod_name, pod_uid, host_network)
            .await
    }
}

#[cfg(test)]
impl PodWatchSource for PodRepository {
    fn subscribe_pod_watch(&self) -> broadcast::Receiver<WatchEvent> {
        self.store.subscribe_watch()
    }
}

#[cfg(test)]
#[async_trait]
#[allow(clippy::todo)]
impl PodApiWriter for PodRepository {
    async fn api_create_pod(
        &self,
        request: PodApiCreateRequest,
    ) -> std::result::Result<PodApiCreateResult, PodRepositoryError> {
        let result = self
            .test_api
            .as_deref()
            .expect("test create requires the neutral Pod API port")
            .create_pod(klights_pod_api::PodApiCreateRequest {
                namespace: request.namespace,
                body: request.body,
                dry_run: request.dry_run,
            })
            .await?;
        Ok(PodApiCreateResult {
            resource: result.resource,
            body: result.body,
        })
    }
    async fn api_update_pod(
        &self,
        ns: &str,
        name: &str,
        body: Value,
        current: Resource,
        dry_run: bool,
    ) -> std::result::Result<PodApiUpdateOutcome, PodRepositoryError> {
        match self
            .test_api
            .as_deref()
            .expect("test update requires the neutral Pod API port")
            .update_pod(klights_pod_api::PodApiUpdateRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                body,
                current,
                dry_run,
            })
            .await?
        {
            klights_pod_api::PodApiWriteOutcome::Persisted(resource) => {
                Ok(PodApiUpdateOutcome::Persisted(resource))
            }
            klights_pod_api::PodApiWriteOutcome::DryRun(value) => {
                Ok(PodApiUpdateOutcome::DryRun(value))
            }
        }
    }
    async fn api_patch_pod(
        &self,
        ns: &str,
        name: &str,
        patch: Value,
        patch_type: PodStatusPatchType,
        dry_run: bool,
    ) -> std::result::Result<PodApiUpdateOutcome, PodRepositoryError> {
        let patch_kind = match patch_type {
            PodStatusPatchType::JsonPatch => klights_pod_api::PodStatusPatchKind::JsonPatch,
            PodStatusPatchType::MergePatch => klights_pod_api::PodStatusPatchKind::MergePatch,
            PodStatusPatchType::StrategicMerge => {
                klights_pod_api::PodStatusPatchKind::StrategicMerge
            }
            PodStatusPatchType::ApplyPatch => klights_pod_api::PodStatusPatchKind::ApplyPatch,
        };
        match self
            .test_api
            .as_deref()
            .expect("test patch requires the neutral Pod API port")
            .patch_pod(klights_pod_api::PodApiPatchRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                patch,
                patch_kind,
                dry_run,
            })
            .await?
        {
            klights_pod_api::PodApiWriteOutcome::Persisted(resource) => {
                Ok(PodApiUpdateOutcome::Persisted(resource))
            }
            klights_pod_api::PodApiWriteOutcome::DryRun(value) => {
                Ok(PodApiUpdateOutcome::DryRun(value))
            }
        }
    }
    async fn api_delete_pod<O>(
        &self,
        ns: &str,
        name: &str,
        options: O,
        dry_run: bool,
    ) -> std::result::Result<PodApiDeleteOutcome, PodRepositoryError>
    where
        O: Into<PodDeleteOptions> + Send,
    {
        match self
            .test_api
            .as_deref()
            .expect("test delete requires the neutral Pod API port")
            .delete_pod(klights_pod_api::PodApiDeleteRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                options: options.into(),
                dry_run,
            })
            .await?
        {
            klights_pod_api::PodApiDeleteOutcome::GracefulSet(resource) => {
                Ok(PodApiDeleteOutcome::GracefulSet(resource))
            }
            klights_pod_api::PodApiDeleteOutcome::DryRun(value) => {
                Ok(PodApiDeleteOutcome::DryRun(value))
            }
        }
    }
    async fn api_delete_collection_pods(
        &self,
        ns: &str,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        dry_run: bool,
    ) -> std::result::Result<(), PodRepositoryError> {
        self.test_api
            .as_deref()
            .expect("test collection delete requires the neutral Pod API port")
            .delete_collection_pods(klights_pod_api::PodApiDeleteCollectionRequest {
                namespace: ns.to_string(),
                label_selector: label_selector.map(str::to_string),
                field_selector: field_selector.map(str::to_string),
                dry_run,
            })
            .await
    }
}

impl GcPodDeleteSink for PodRepository {
    fn request_gc_pod_delete(
        &self,
        request: GcPodDeleteRequest,
    ) -> klights_reconcile_api::GcPodDeleteFuture<'_> {
        self.gc_delete.request_gc_pod_delete(request)
    }
}
