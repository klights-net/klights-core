//! Composition-root wiring for Pod repository API and reconcile adapters.

use std::sync::Arc;

impl From<crate::api::pod_repository_ports::ApiPodList> for crate::datastore::ResourceList {
    fn from(list: crate::api::pod_repository_ports::ApiPodList) -> Self {
        Self {
            items: list.items,
            resource_version: list.resource_version,
            watch_replay_position: None,
            continue_token: list.continue_token,
            remaining_item_count: list.remaining_item_count,
        }
    }
}

use crate::datastore::DatastoreHandle;
use crate::kubelet::pod_repository::{
    PodRepository, PodRepositoryAdapters, PodRepositoryDeliveryDependencies,
    PodRepositoryNetworkDependencies, PodRepositoryRuntimeDependencies,
};

impl klights_cluster_store::PodUidPreconditionRead for dyn crate::datastore::DatastoreBackend + '_ {
    fn read_pod_uid_precondition(
        &self,
        request: klights_cluster_store::PodUidPreconditionRequest,
    ) -> klights_cluster_store::PodUidPreconditionFuture<'_> {
        Box::pin(async move {
            let live = self
                .get_resource("v1", "Pod", Some(request.namespace()), request.name())
                .await
                .map_err(|error| {
                    klights_cluster_store::PodUidPreconditionError::retryable(error.to_string())
                })?;
            Ok(match live {
                None => klights_cluster_store::PodUidPreconditionState::Missing,
                Some(pod) if pod.uid == request.expected_uid() => {
                    klights_cluster_store::PodUidPreconditionState::Matches
                }
                Some(pod) => klights_cluster_store::PodUidPreconditionState::Mismatch {
                    actual_uid: pod.uid,
                },
            })
        })
    }
}
use crate::pod_api_service::{PodApiService, PodApiServiceDependencies};
use crate::side_effects::SideEffectMetrics;
use crate::side_effects::SideEffectRegistry;
use klights_leader_api::LeaderResourceQuery;
use klights_supervisor::TaskSupervisor;
use klights_types::PodIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodSchedulingMode {
    InlineSingleNode,
    DeferredMultiNodeLeader,
}

#[derive(Clone)]
pub struct PodRepositoryBuildConfig {
    pub db: DatastoreHandle,
    pub node_local: Option<crate::datastore::node_local::NodeLocalHandle>,
    pub supervisor: Arc<TaskSupervisor>,
    pub side_effects: Arc<SideEffectRegistry>,
    pub metrics: Arc<SideEffectMetrics>,
    pub pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pub assignment_waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
    pub scheduling_mode: PodSchedulingMode,
    pub outbox: Option<Arc<crate::node_outbox::Outbox>>,
    pub cluster_api: Option<Arc<dyn LeaderResourceQuery>>,
}

#[derive(Clone)]
pub struct WorkerPodRepositoryBuildConfig {
    pub resource_query: Arc<dyn LeaderResourceQuery>,
    pub node_local: crate::datastore::node_local::NodeLocalHandle,
    pub supervisor: Arc<TaskSupervisor>,
    pub metrics: Arc<SideEffectMetrics>,
    pub pod_network_cache: Arc<dyn klights_node_store::PodNetworkCache>,
    pub assignment_waiter: Arc<dyn klights_network_api::PodNetworkAssignmentWaiter>,
    pub outbox: Arc<crate::node_outbox::Outbox>,
}

pub(crate) struct RootPodRepositoryParts {
    pub repository_parts: crate::kubelet::pod_repository::facade::PodRepositoryParts,
    pub api: Arc<PodApiService>,
    pub subresource: Arc<crate::pod_subresource_service::PodSubresourceService>,
}

#[derive(Clone)]
struct RootPodWorkqueuePersistence {
    node_local: Option<crate::datastore::node_local::NodeLocalHandle>,
    test_rows: Arc<std::sync::Mutex<Vec<crate::datastore::PodWorkqueueEntry>>>,
    test_next_id: Arc<std::sync::atomic::AtomicI64>,
}

struct RootPodPersistence {
    db: DatastoreHandle,
}

struct WorkerPodPersistence {
    resource_query: Arc<dyn LeaderResourceQuery>,
}

struct WorkerPodAdapters;

fn worker_pod_api_unavailable() -> klights_pod_api::PodRepositoryError {
    klights_pod_api::PodRepositoryError::unavailable(
        "worker kubelet does not own the Kubernetes Pod API",
    )
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodOrdinaryMutation for WorkerPodAdapters {
    async fn create(
        &self,
        _request: crate::kubelet::pod_repository::types::PodApiCreateRequest,
    ) -> Result<
        crate::kubelet::pod_repository::types::PodApiCreateResult,
        klights_pod_api::PodRepositoryError,
    > {
        Err(worker_pod_api_unavailable())
    }

    async fn update(
        &self,
        _namespace: &str,
        _name: &str,
        _body: serde_json::Value,
        _current: klights_cluster_core::Resource,
        _dry_run: bool,
    ) -> Result<
        crate::kubelet::pod_repository::types::PodApiUpdateOutcome,
        klights_pod_api::PodRepositoryError,
    > {
        Err(worker_pod_api_unavailable())
    }

    async fn patch(
        &self,
        _namespace: &str,
        _name: &str,
        _patch: serde_json::Value,
        _patch_type: crate::kubelet::pod_repository::types::PodStatusPatchType,
        _dry_run: bool,
    ) -> Result<
        crate::kubelet::pod_repository::types::PodApiUpdateOutcome,
        klights_pod_api::PodRepositoryError,
    > {
        Err(worker_pod_api_unavailable())
    }

    async fn delete(
        &self,
        _namespace: &str,
        _name: &str,
        _options: klights_pod_api::PodDeleteOptions,
        _dry_run: bool,
    ) -> Result<
        crate::kubelet::pod_repository::types::PodApiDeleteOutcome,
        klights_pod_api::PodRepositoryError,
    > {
        Err(worker_pod_api_unavailable())
    }

    async fn delete_collection(
        &self,
        _namespace: &str,
        _label_selector: Option<&str>,
        _field_selector: Option<&str>,
        _dry_run: bool,
    ) -> Result<(), klights_pod_api::PodRepositoryError> {
        Err(worker_pod_api_unavailable())
    }

    async fn mark_terminating(
        &self,
        _target: &klights_pod_api::PodMutationTarget,
    ) -> Result<klights_cluster_core::Resource, klights_pod_api::PodRepositoryError> {
        Err(worker_pod_api_unavailable())
    }

    async fn schedule_pending(
        &self,
        _namespace: &str,
        _name: &str,
    ) -> Result<Option<klights_cluster_core::Resource>, klights_pod_api::PodRepositoryError> {
        Err(worker_pod_api_unavailable())
    }

    async fn bind(
        &self,
        _namespace: &str,
        _name: &str,
        _binding: serde_json::Value,
        _dry_run: bool,
    ) -> Result<(), klights_pod_api::PodRepositoryError> {
        Err(worker_pod_api_unavailable())
    }

    async fn schedule_all(self: Arc<Self>) -> Result<(), klights_pod_api::PodRepositoryError> {
        Err(worker_pod_api_unavailable())
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::ControllerPodCreate for WorkerPodAdapters {
    async fn create_controller_pod(
        &self,
        _request: crate::kubelet::pod_repository::types::PodApiCreateRequest,
    ) -> Result<
        crate::kubelet::pod_repository::types::PodApiCreateResult,
        klights_pod_api::PodRepositoryError,
    > {
        Err(worker_pod_api_unavailable())
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::PodSubresourcePort for WorkerPodAdapters {
    async fn replace_status(
        &self,
        _namespace: &str,
        _name: &str,
        _pod_uid: Option<&str>,
        _status: serde_json::Value,
        _expected_rv: i64,
    ) -> Result<klights_cluster_core::Resource, klights_pod_api::PodRepositoryError> {
        Err(worker_pod_api_unavailable())
    }

    async fn patch_status(
        &self,
        _namespace: &str,
        _name: &str,
        _patch: serde_json::Value,
        _patch_type: crate::kubelet::pod_repository::types::PodStatusPatchType,
        _expected_rv: i64,
    ) -> Result<klights_cluster_core::Resource, klights_pod_api::PodRepositoryError> {
        Err(worker_pod_api_unavailable())
    }

    async fn update_ephemeral_containers(
        &self,
        _namespace: &str,
        _name: &str,
        _containers: Vec<serde_json::Value>,
        _expected_rv: i64,
    ) -> Result<klights_cluster_core::Resource, klights_pod_api::PodRepositoryError> {
        Err(worker_pod_api_unavailable())
    }
}

fn worker_reconcile_ok() -> klights_reconcile_api::ReconcileSinkFuture<'static> {
    Box::pin(async { Ok(()) })
}

impl klights_reconcile_api::PodMutationReconcileSink for WorkerPodAdapters {
    fn reconcile_pod_mutation(
        &self,
        _request: klights_reconcile_api::PodMutationReconcileRequest,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        worker_reconcile_ok()
    }
}

impl klights_reconcile_api::GcPodDeleteSink for WorkerPodAdapters {
    fn request_gc_pod_delete(
        &self,
        _request: klights_reconcile_api::GcPodDeleteRequest,
    ) -> klights_reconcile_api::GcPodDeleteFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

impl klights_reconcile_api::PodGcReconcileSink for WorkerPodAdapters {
    fn reconcile_owner_references<'a>(
        &'a self,
        _pod: klights_cluster_core::Resource,
        _pod_delete_sink: &'a dyn klights_reconcile_api::GcPodDeleteSink,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'a> {
        worker_reconcile_ok()
    }

    fn cascade_delete_dependents<'a>(
        &'a self,
        _owner: klights_types::PodIdentity,
        _pod_delete_sink: &'a dyn klights_reconcile_api::GcPodDeleteSink,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'a> {
        worker_reconcile_ok()
    }

    fn finalize_foreground_owners<'a>(
        &'a self,
        _deleted_dependent: klights_cluster_core::Resource,
        _pod_delete_sink: &'a dyn klights_reconcile_api::GcPodDeleteSink,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'a> {
        worker_reconcile_ok()
    }
}

impl klights_reconcile_api::PodPdbReconcileSink for WorkerPodAdapters {
    fn reconcile_namespace_pdbs(
        &self,
        _namespace: String,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        worker_reconcile_ok()
    }
}

impl klights_reconcile_api::PodEvictionAdmissionSink for WorkerPodAdapters {
    fn admit_pod_eviction(
        &self,
        _request: klights_reconcile_api::PodEvictionAdmissionRequest,
    ) -> klights_reconcile_api::PodEvictionAdmissionFuture<'_> {
        Box::pin(async { Ok(klights_reconcile_api::PodEvictionAdmissionOutcome::Allowed) })
    }
}

impl klights_reconcile_api::NamespaceTerminationSink for WorkerPodAdapters {
    fn reconcile_namespace_termination(
        &self,
        _request: klights_reconcile_api::NamespaceTerminationRequest,
    ) -> klights_reconcile_api::NamespaceTerminationFuture<'_> {
        Box::pin(async { Ok(klights_reconcile_api::NamespaceTerminationOutcome::Finalized) })
    }
}

impl klights_reconcile_api::NamespaceBootstrapSink for WorkerPodAdapters {
    fn create_default_service_account(
        &self,
        _namespace: String,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        worker_reconcile_ok()
    }

    fn create_root_ca_config_map(
        &self,
        _namespace: String,
        _ca_certificate: String,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        worker_reconcile_ok()
    }
}

fn worker_pod_repository_adapters() -> PodRepositoryAdapters {
    let adapter = Arc::new(WorkerPodAdapters);
    PodRepositoryAdapters {
        ordinary_mutation: adapter.clone(),
        controller_create: adapter.clone(),
        subresource: adapter.clone(),
        gc_delete: adapter.clone(),
        gc_reconcile: adapter.clone(),
        pdb_reconcile: adapter.clone(),
        eviction_admission: adapter.clone(),
        namespace_bootstrap: adapter.clone(),
        namespace_termination: adapter.clone(),
        mutation_reconcile: adapter,
        #[cfg(test)]
        scheduler_test_control: None,
    }
}

fn worker_persistence_unavailable(operation: &str) -> anyhow::Error {
    anyhow::Error::new(klights_pod_api::PodRepositoryError::unavailable(format!(
        "worker Pod persistence cannot perform leader-owned {operation}"
    )))
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::store::PodPersistence for WorkerPodPersistence {
    async fn get(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.resource_query
            .get_resource(klights_leader_api::ResourceGetRequest::try_new(
                klights_types::ResourceKey::new("v1", "Pod", Some(namespace.to_string()), name),
                klights_leader_api::ResourceQueryConsistency::Cached,
            )?)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn list(
        &self,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodResourceList> {
        let result = self
            .resource_query
            .list_resources(klights_leader_api::ResourceListRequest::try_new(
                "v1",
                "Pod",
                namespace.map(str::to_owned),
                label_selector.map(str::to_owned),
                field_selector.map(str::to_owned),
                limit,
                continue_token.map(str::to_owned),
                klights_leader_api::ResourceQueryConsistency::Cached,
            )?)
            .await
            .map_err(anyhow::Error::new)?;
        let (items, resource_version, _, continue_token, remaining_item_count) =
            result.into_parts();
        Ok(crate::kubelet::pod_repository::PodResourceList {
            items,
            resource_version,
            continue_token,
            remaining_item_count,
        })
    }

    async fn snapshot_list(
        &self,
        request: klights_pod_api::PodSnapshotListRequest,
    ) -> anyhow::Result<klights_pod_api::PodSnapshotListOutcome> {
        let list = request.list;
        let result = self
            .list(
                list.namespace(),
                list.label_selector(),
                list.field_selector(),
                list.limit(),
                list.continue_token(),
            )
            .await?;
        if result.resource_version < request.snapshot_resource_version {
            return Ok(klights_pod_api::PodSnapshotListOutcome::Current);
        }
        Ok(klights_pod_api::PodSnapshotListOutcome::List(
            klights_pod_api::PodListResult::try_new(
                result.items,
                result.resource_version,
                result.continue_token,
                result.remaining_item_count,
            )?,
        ))
    }

    async fn list_by_owner(
        &self,
        namespace: &str,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
        Ok(self
            .list(Some(namespace), None, None, None, None)
            .await?
            .items
            .into_iter()
            .filter(|pod| {
                pod.data
                    .pointer("/metadata/ownerReferences")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|owners| {
                        owners.iter().any(|owner| {
                            owner.get("uid").and_then(serde_json::Value::as_str) == Some(owner_uid)
                        })
                    })
            })
            .collect())
    }

    async fn create(
        &self,
        _namespace: &str,
        _name: &str,
        _body: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        Err(worker_persistence_unavailable("create"))
    }

    async fn update(
        &self,
        _namespace: &str,
        _name: &str,
        _body: serde_json::Value,
        _preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        Err(worker_persistence_unavailable("update"))
    }

    async fn patch_latest(
        &self,
        _namespace: &str,
        _name: &str,
        _patch_kind: crate::datastore::PatchKind,
        _patch: serde_json::Value,
        _preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        Err(worker_persistence_unavailable("patch"))
    }

    async fn update_status(
        &self,
        _namespace: &str,
        _name: &str,
        _status: serde_json::Value,
        _preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        Err(worker_persistence_unavailable("status update"))
    }

    async fn delete(
        &self,
        _namespace: &str,
        _name: &str,
        _preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<crate::kubelet::pod_repository::store::PodDeleteCasOutcome> {
        Err(worker_persistence_unavailable("delete"))
    }

    fn log_status_noop(
        &self,
        namespace: &str,
        name: &str,
        resource: &klights_cluster_core::Resource,
    ) {
        tracing::debug!(
            namespace,
            name,
            resource_version = resource.resource_version,
            "worker Pod status write was already current"
        );
    }
}

pub(crate) fn new_pod_store(
    db: DatastoreHandle,
) -> crate::kubelet::pod_repository::store::PodStore {
    crate::kubelet::pod_repository::store::PodStore::from_persistence(Arc::new(
        RootPodPersistence { db },
    ))
}

fn pod_persistence_error(error: anyhow::Error, namespace: &str, name: &str) -> anyhow::Error {
    if let Some(error) = error.downcast_ref::<crate::datastore::errors::DatastoreError>() {
        return match error {
            crate::datastore::errors::DatastoreError::AlreadyExists { message } => {
                anyhow::Error::new(klights_pod_api::PodRepositoryError::already_exists(message))
            }
            crate::datastore::errors::DatastoreError::Conflict { message } => {
                anyhow::Error::new(klights_pod_api::PodRepositoryError::conflict(message))
            }
            crate::datastore::errors::DatastoreError::NotFound { .. } => anyhow::Error::new(
                klights_pod_api::PodRepositoryError::not_found(namespace, name),
            ),
        };
    }
    anyhow::Error::new(klights_pod_api::PodRepositoryError::unavailable(
        error.to_string(),
    ))
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::store::PodPersistence for RootPodPersistence {
    async fn get(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.db
            .get_resource("v1", "Pod", Some(namespace), name)
            .await
            .map_err(|error| pod_persistence_error(error, namespace, name))
    }

    async fn list(
        &self,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> anyhow::Result<crate::kubelet::pod_repository::PodResourceList> {
        let list = self
            .db
            .list_resources(
                "v1",
                "Pod",
                namespace,
                crate::datastore::ResourceListQuery::new(
                    label_selector,
                    field_selector,
                    limit,
                    continue_token,
                ),
            )
            .await
            .map_err(|error| {
                pod_persistence_error(error, namespace.unwrap_or_default(), "Pod list")
            })?;
        Ok(crate::kubelet::pod_repository::PodResourceList {
            items: list.items,
            resource_version: list.resource_version,
            continue_token: list.continue_token,
            remaining_item_count: list.remaining_item_count,
        })
    }

    async fn snapshot_list(
        &self,
        request: klights_pod_api::PodSnapshotListRequest,
    ) -> anyhow::Result<klights_pod_api::PodSnapshotListOutcome> {
        let list = request.list;
        let snapshot = self
            .db
            .snapshot_resources_at_rv(
                "v1",
                "Pod",
                list.namespace(),
                crate::datastore::ResourceListQuery::new(
                    list.label_selector(),
                    list.field_selector(),
                    list.limit(),
                    list.continue_token(),
                ),
                request.snapshot_resource_version,
            )
            .await?;
        Ok(match snapshot {
            crate::datastore::SnapshotAtRv::List(list) => {
                klights_pod_api::PodSnapshotListOutcome::List(
                    klights_pod_api::PodListResult::try_new(
                        list.items,
                        list.resource_version,
                        list.continue_token,
                        list.remaining_item_count,
                    )?,
                )
            }
            crate::datastore::SnapshotAtRv::Current => {
                klights_pod_api::PodSnapshotListOutcome::Current
            }
            crate::datastore::SnapshotAtRv::Expired => {
                klights_pod_api::PodSnapshotListOutcome::Expired
            }
        })
    }

    async fn list_by_owner(
        &self,
        namespace: &str,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
        self.db
            .list_resources_by_owner_uid("v1", "Pod", Some(namespace), owner_uid)
            .await
            .map_err(|error| pod_persistence_error(error, namespace, "Pod owner list"))
    }

    async fn create(
        &self,
        namespace: &str,
        name: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.db
            .create_resource("v1", "Pod", Some(namespace), name, body)
            .await
            .map_err(|error| pod_persistence_error(error, namespace, name))
    }

    async fn update(
        &self,
        namespace: &str,
        name: &str,
        body: serde_json::Value,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.db
            .update_resource_with_preconditions(
                "v1",
                "Pod",
                Some(namespace),
                name,
                body,
                preconditions,
            )
            .await
            .map_err(|error| pod_persistence_error(error, namespace, name))
    }

    async fn patch_latest(
        &self,
        namespace: &str,
        name: &str,
        patch_kind: klights_cluster_core::PatchKind,
        patch: serde_json::Value,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        self.db
            .patch_resource_latest_with_preconditions(
                "v1",
                "Pod",
                Some(namespace),
                name,
                crate::datastore::ResourcePatchRequest::new(patch_kind, patch, preconditions),
            )
            .await
            .map_err(|error| pod_persistence_error(error, namespace, name))
    }

    async fn update_status(
        &self,
        namespace: &str,
        name: &str,
        status: serde_json::Value,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<klights_cluster_core::Resource> {
        self.db
            .update_status_only_with_preconditions(
                "v1",
                "Pod",
                Some(namespace),
                name,
                status,
                preconditions,
            )
            .await
            .map_err(|error| pod_persistence_error(error, namespace, name))
    }

    async fn delete(
        &self,
        namespace: &str,
        name: &str,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> anyhow::Result<crate::kubelet::pod_repository::store::PodDeleteCasOutcome> {
        match self
            .db
            .delete_resource_with_preconditions("v1", "Pod", Some(namespace), name, preconditions)
            .await
        {
            Ok(()) => Ok(crate::kubelet::pod_repository::store::PodDeleteCasOutcome::Removed),
            Err(error) if crate::datastore::errors::is_conflict_error(&error) => {
                Ok(crate::kubelet::pod_repository::store::PodDeleteCasOutcome::Conflict)
            }
            Err(error)
                if error
                    .downcast_ref::<crate::datastore::errors::DatastoreError>()
                    .is_some_and(|error| {
                        matches!(
                            error,
                            crate::datastore::errors::DatastoreError::NotFound { .. }
                        )
                    }) =>
            {
                Ok(crate::kubelet::pod_repository::store::PodDeleteCasOutcome::Gone)
            }
            Err(error) => Err(pod_persistence_error(error, namespace, name)),
        }
    }

    fn log_status_noop(
        &self,
        namespace: &str,
        name: &str,
        resource: &klights_cluster_core::Resource,
    ) {
        crate::datastore::diagnostics::log_noop_resource_write(
            crate::datastore::diagnostics::NoopResourceWrite {
                operation: "pod_store_update_status",
                api_version: "v1",
                kind: "Pod",
                namespace: Some(namespace),
                name,
                uid: &resource.uid,
                resource_version: resource.resource_version,
                reason: "pod status unchanged",
            },
        );
    }

    #[cfg(test)]
    fn subscribe_watch(&self) -> tokio::sync::broadcast::Receiver<crate::watch::WatchEvent> {
        self.db
            .subscribe_watch(klights_watch::WatchTopic::new("v1", "Pod"))
    }

    #[cfg(test)]
    fn legacy_db(&self) -> DatastoreHandle {
        self.db.clone()
    }
}

fn legacy_workqueue_kind(
    kind: crate::kubelet::pod_repository::workqueue::PodWorkqueueKind,
) -> crate::datastore::PodWorkqueueKind {
    match kind {
        crate::kubelet::pod_repository::workqueue::PodWorkqueueKind::Pod => {
            crate::datastore::PodWorkqueueKind::Pod
        }
        crate::kubelet::pod_repository::workqueue::PodWorkqueueKind::Namespace => {
            crate::datastore::PodWorkqueueKind::Namespace
        }
    }
}

fn focused_workqueue_entry(
    row: crate::datastore::PodWorkqueueEntry,
) -> crate::kubelet::pod_repository::workqueue::PodWorkqueueEntry {
    crate::kubelet::pod_repository::workqueue::PodWorkqueueEntry {
        id: row.id,
        kind: match row.kind {
            crate::datastore::PodWorkqueueKind::Pod => {
                crate::kubelet::pod_repository::workqueue::PodWorkqueueKind::Pod
            }
            crate::datastore::PodWorkqueueKind::Namespace => {
                crate::kubelet::pod_repository::workqueue::PodWorkqueueKind::Namespace
            }
        },
        namespace: row.namespace,
        name: row.name,
        uid: row.uid,
        payload: row.payload,
        attempt_count: row.attempt_count,
        next_attempt_at_ms: row.next_attempt_at_ms,
    }
}

fn legacy_workqueue_entry(
    row: crate::kubelet::pod_repository::workqueue::PodWorkqueueEntry,
) -> crate::datastore::PodWorkqueueEntry {
    crate::datastore::PodWorkqueueEntry {
        id: row.id,
        kind: legacy_workqueue_kind(row.kind),
        namespace: row.namespace,
        name: row.name,
        uid: row.uid,
        payload: row.payload,
        attempt_count: row.attempt_count,
        next_attempt_at_ms: row.next_attempt_at_ms,
    }
}

#[async_trait::async_trait]
impl crate::kubelet::pod_repository::workqueue::PodWorkqueuePersistence
    for RootPodWorkqueuePersistence
{
    async fn enqueue(
        &self,
        kind: crate::kubelet::pod_repository::workqueue::PodWorkqueueKind,
        pod: &PodIdentity,
        payload: serde_json::Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> anyhow::Result<()> {
        if let Some(node_local) = &self.node_local {
            return node_local
                .enqueue_workqueue(
                    legacy_workqueue_kind(kind),
                    pod,
                    payload,
                    attempt_count,
                    min_delay_ms,
                    last_error,
                )
                .await;
        }
        let id = self
            .test_next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.test_rows
            .lock()
            .unwrap()
            .push(crate::datastore::PodWorkqueueEntry {
                id,
                kind: legacy_workqueue_kind(kind),
                namespace: pod.namespace.clone(),
                name: pod.name.clone(),
                uid: pod.uid.clone(),
                payload,
                attempt_count,
                next_attempt_at_ms: now_ms.saturating_add(min_delay_ms),
            });
        let _ = last_error;
        Ok(())
    }

    async fn peek_next_due(&self) -> anyhow::Result<Option<i64>> {
        if let Some(node_local) = &self.node_local {
            return node_local.peek_workqueue_next_due().await;
        }
        Ok(self
            .test_rows
            .lock()
            .unwrap()
            .iter()
            .map(|row| row.next_attempt_at_ms)
            .min())
    }

    async fn claim_due(
        &self,
        now_ms: i64,
    ) -> anyhow::Result<Option<crate::kubelet::pod_repository::workqueue::PodWorkqueueEntry>> {
        if let Some(node_local) = &self.node_local {
            return Ok(node_local
                .claim_workqueue_due(now_ms)
                .await?
                .map(focused_workqueue_entry));
        }
        let mut rows = self.test_rows.lock().unwrap();
        let candidate = rows
            .iter()
            .enumerate()
            .filter(|(_, row)| row.next_attempt_at_ms <= now_ms)
            .min_by_key(|(_, row)| (row.next_attempt_at_ms, row.id))
            .map(|(index, _)| index);
        Ok(candidate.map(|index| focused_workqueue_entry(rows.remove(index))))
    }

    async fn complete(&self, id: i64) -> anyhow::Result<()> {
        if let Some(node_local) = &self.node_local {
            return node_local.complete_workqueue(id).await;
        }
        self.test_rows.lock().unwrap().retain(|row| row.id != id);
        Ok(())
    }

    async fn record_failure(
        &self,
        row: crate::kubelet::pod_repository::workqueue::PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> anyhow::Result<()> {
        let row = legacy_workqueue_entry(row);
        let pod = PodIdentity::new(&row.namespace, &row.name, &row.uid);
        self.enqueue(
            match row.kind {
                crate::datastore::PodWorkqueueKind::Pod => {
                    crate::kubelet::pod_repository::workqueue::PodWorkqueueKind::Pod
                }
                crate::datastore::PodWorkqueueKind::Namespace => {
                    crate::kubelet::pod_repository::workqueue::PodWorkqueueKind::Namespace
                }
            },
            &pod,
            row.payload,
            row.attempt_count.saturating_add(1),
            min_delay_ms,
            Some(error),
        )
        .await
    }

    async fn dead_letter(&self, id: i64, error: &str) -> anyhow::Result<()> {
        let _ = error;
        self.complete(id).await
    }
}

fn root_pod_repository_adapters(
    db: DatastoreHandle,
    resource_query: Arc<dyn LeaderResourceQuery>,
    side_effects: Arc<SideEffectRegistry>,
    metrics: Arc<SideEffectMetrics>,
    prepared: &crate::kubelet::pod_repository::PodRepositoryPrepared,
) -> (
    PodRepositoryAdapters,
    Arc<PodApiService>,
    Arc<crate::pod_subresource_service::PodSubresourceService>,
) {
    let pod_reconcile = Arc::new(crate::pod_reconcile_adapter::PodReconcileAdapter::new(
        db.clone(),
        side_effects.controller_dispatcher_slot(),
        metrics.clone(),
        side_effects.clone(),
        prepared.store.clone(),
    ));
    let subresource = Arc::new(crate::pod_subresource_service::PodSubresourceService::new(
        prepared.store.clone(),
        prepared.status_only.clone(),
        db.clone(),
        side_effects.controller_dispatcher_slot(),
    ));
    let api = Arc::new(PodApiService::new(PodApiServiceDependencies {
        store: prepared.store.clone(),
        status_only: prepared.status_only.clone(),
        db: db.clone(),
        resource_query,
        quota_runtime: crate::resource_quota_admission_adapter::ResourceQuotaAdmissionAdapter::new(
            db,
        ),
        supervisor: prepared.supervisor.clone(),
        delete_coordinator: prepared.delete_coordinator.clone(),
        gc_reconcile: pod_reconcile.clone(),
        service_reconcile: pod_reconcile.clone(),
        mutation_effects:
            crate::resource_mutation_effects_adapter::ResourceMutationEffectsAdapter::new(
                side_effects,
                metrics.clone(),
            ),
        metrics,
    }));
    let adapters = PodRepositoryAdapters {
        ordinary_mutation: api.clone(),
        controller_create: api.clone(),
        subresource: subresource.clone(),
        gc_delete: api.clone(),
        gc_reconcile: pod_reconcile.clone(),
        pdb_reconcile: pod_reconcile.clone(),
        eviction_admission: pod_reconcile.clone(),
        namespace_bootstrap: pod_reconcile.clone(),
        namespace_termination: pod_reconcile.clone(),
        mutation_reconcile: pod_reconcile,
        #[cfg(test)]
        scheduler_test_control: Some(api.clone()),
    };
    (adapters, api, subresource)
}

pub(crate) fn build_pod_repository_parts(
    config: PodRepositoryBuildConfig,
    leadership: Option<tokio::sync::watch::Receiver<bool>>,
) -> RootPodRepositoryParts {
    let PodRepositoryBuildConfig {
        db,
        node_local,
        supervisor,
        side_effects,
        metrics,
        pod_network_cache,
        assignment_waiter,
        scheduling_mode,
        outbox,
        cluster_api,
    } = config;
    let _ = scheduling_mode;
    #[cfg(not(test))]
    let resource_query = cluster_api
        .clone()
        .expect("production Pod repository requires a leader resource query");
    #[cfg(test)]
    let resource_query = cluster_api.clone().unwrap_or_else(|| {
        Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            db.clone(),
            "local-node".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ))
    });
    let prepared = PodRepository::prepare(
        Arc::new(RootPodPersistence { db: db.clone() }),
        RootPodWorkqueuePersistence {
            node_local,
            test_rows: Arc::new(std::sync::Mutex::new(Vec::new())),
            test_next_id: Arc::new(std::sync::atomic::AtomicI64::new(1)),
        },
        PodRepositoryRuntimeDependencies {
            supervisor: supervisor.clone(),
            metrics: metrics.clone(),
        },
        leadership,
    );
    let (adapters, api, subresource) =
        root_pod_repository_adapters(db, resource_query, side_effects, metrics.clone(), &prepared);
    let delivery_outbox = outbox.map(|outbox| outbox as Arc<dyn klights_leader_api::NodeOutbox>);
    let bound_pod_adapter = crate::bound_pod_finalization_adapter::RootBoundPodFinalization::new(
        prepared.store.clone(),
        cluster_api.clone(),
        delivery_outbox.clone(),
    );
    let bound_pod_finalization = bound_pod_adapter.clone();
    let actor_delete_mark = adapters.gc_delete.clone();
    let repository_parts = PodRepository::assemble(
        prepared,
        metrics.clone(),
        PodRepositoryNetworkDependencies {
            pod_network_cache,
            assignment_waiter,
        },
        PodRepositoryDeliveryDependencies {
            outbox: delivery_outbox,
            cluster_api,
            bound_pod_finalization,
            actor_delete_mark,
        },
        adapters,
    );
    RootPodRepositoryParts {
        repository_parts,
        api,
        subresource,
    }
}

pub(crate) fn build_worker_pod_repository_parts(
    config: WorkerPodRepositoryBuildConfig,
) -> crate::kubelet::pod_repository::facade::PodRepositoryParts {
    let prepared = PodRepository::prepare(
        Arc::new(WorkerPodPersistence {
            resource_query: config.resource_query.clone(),
        }),
        RootPodWorkqueuePersistence {
            node_local: Some(config.node_local),
            test_rows: Arc::new(std::sync::Mutex::new(Vec::new())),
            test_next_id: Arc::new(std::sync::atomic::AtomicI64::new(1)),
        },
        PodRepositoryRuntimeDependencies {
            supervisor: config.supervisor.clone(),
            metrics: config.metrics.clone(),
        },
        None,
    );
    let delivery_outbox: Arc<dyn klights_leader_api::NodeOutbox> = config.outbox;
    let bound_pod_adapter = crate::bound_pod_finalization_adapter::RootBoundPodFinalization::new(
        prepared.store.clone(),
        Some(config.resource_query.clone()),
        Some(delivery_outbox.clone()),
    );
    let bound_pod_finalization = bound_pod_adapter.clone();
    let actor_delete_mark = bound_pod_adapter;
    PodRepository::assemble(
        prepared,
        config.metrics,
        PodRepositoryNetworkDependencies {
            pod_network_cache: config.pod_network_cache,
            assignment_waiter: config.assignment_waiter,
        },
        PodRepositoryDeliveryDependencies {
            outbox: Some(delivery_outbox),
            cluster_api: Some(config.resource_query),
            bound_pod_finalization,
            actor_delete_mark,
        },
        worker_pod_repository_adapters(),
    )
}
