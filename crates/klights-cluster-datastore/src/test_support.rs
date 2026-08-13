//! Narrow fixtures shared by cluster-datastore consumers.

use klights_cluster_core::{LogApplyCommit, LogApplyMutation, Resource, ResourcePreconditions};
use klights_cluster_store::ResourceListOptions;

/// Exact mutation-pause coordinates used by resource-update race fixtures.
///
/// The pause remains an opt-in datastore test capability; consumers cannot
/// obtain the concrete datastore through this re-export.
pub use crate::sqlite::embedded::{ResourceMutationPause, ResourceMutationPauseOperation};

/// Deterministic control for a durable-history replay failure fixture.
///
/// It intentionally affects only subsequent replay reads. Retention-floor
/// reads continue to delegate, matching a live replay persistence failure
/// without changing positioned list handoff behavior.
#[derive(Clone)]
pub struct WatchHistoryFailureControl {
    fail: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl WatchHistoryFailureControl {
    pub fn new() -> Self {
        Self {
            fail: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn fail_subsequent_reads(&self) {
        self.fail.store(true, std::sync::atomic::Ordering::Release);
    }
}

impl Default for WatchHistoryFailureControl {
    fn default() -> Self {
        Self::new()
    }
}

/// Wrap a durable-history reader with a deterministic replay-failure switch.
///
/// This focused fixture preserves the delegate's positioned replay and floor
/// semantics until the control is activated.
pub fn toggle_failing_watch_history_for_test_support(
    delegate: std::sync::Arc<dyn klights_cluster_store::DurableWatchHistoryRead>,
    control: WatchHistoryFailureControl,
) -> std::sync::Arc<dyn klights_cluster_store::DurableWatchHistoryRead> {
    std::sync::Arc::new(ToggleFailingWatchHistory {
        delegate,
        fail: control.fail,
    })
}

struct ToggleFailingWatchHistory {
    delegate: std::sync::Arc<dyn klights_cluster_store::DurableWatchHistoryRead>,
    fail: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl klights_cluster_store::DurableWatchHistoryRead for ToggleFailingWatchHistory {
    fn replay_watch_history(
        &self,
        request: klights_cluster_store::WatchHistoryRequest,
    ) -> klights_cluster_store::WatchHistoryFuture<'_, klights_cluster_store::WatchHistoryRead>
    {
        if self.fail.load(std::sync::atomic::Ordering::Acquire) {
            return Box::pin(async {
                Err(
                    klights_cluster_store::WatchHistoryError::PersistenceFailed {
                        message: "injected live replay read failure".to_string(),
                    },
                )
            });
        }
        self.delegate.replay_watch_history(request)
    }

    fn list_replay_floors(
        &self,
    ) -> klights_cluster_store::WatchHistoryFuture<'_, Vec<klights_cluster_store::DurableReplayFloor>>
    {
        self.delegate.list_replay_floors()
    }
}

#[derive(Default)]
struct GcCommitSink;

impl klights_cluster_store::CommitObservationSink for GcCommitSink {
    fn observe(&self, _observations: &[klights_cluster_store::StagedPostCommit]) {}

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct GcOutboxCodec;

impl klights_cluster_store::OutboxResponseCodec for GcOutboxCodec {
    fn encode(&self, response: &klights_cluster_core::StorageResponse) -> Result<Vec<u8>, String> {
        serde_json::to_vec(response).map_err(|error| error.to_string())
    }

    fn decode(&self, bytes: &[u8]) -> Result<klights_cluster_core::StorageResponse, String> {
        serde_json::from_slice(bytes).map_err(|error| error.to_string())
    }
}

pub(crate) fn gc_commit_sink() -> std::sync::Arc<dyn klights_cluster_store::CommitObservationSink> {
    std::sync::Arc::new(GcCommitSink)
}

pub(crate) fn gc_outbox_codec() -> std::sync::Arc<dyn klights_cluster_store::OutboxResponseCodec> {
    std::sync::Arc::new(GcOutboxCodec)
}

/// Focused SQLite fixture capability for cross-crate GC conformance tests.
///
/// The concrete datastore stays private so consumers cannot acquire a generic
/// cluster-store escape hatch. Pod removals are exposed only as UID-qualified
/// actor finalization or the terminating-unscheduled UID/RV CAS exception.
#[derive(Clone)]
pub struct GcTestStore {
    datastore: crate::sqlite::embedded::Datastore,
}

/// Focused CRUD/list/watch fixture for canonical service-owner tests.
///
/// Root composition may bind this to its already-open canonical store. Its
/// public surface is deliberately named K8s operations only: it never returns
/// a backend, raw SQLite connection, or broad datastore handle.
#[derive(Clone)]
pub struct ResourceTestStore {
    datastore: crate::sqlite::embedded::Datastore,
}

/// Endpoint/EndpointSlice-only persistence setup and observation for controller
/// tests. The inner resource fixture is intentionally private.
#[derive(Clone)]
pub struct EndpointResourceFixture {
    resources: ResourceTestStore,
}

impl EndpointResourceFixture {
    pub fn new(resources: ResourceTestStore) -> Self {
        Self { resources }
    }

    pub async fn seed_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.resources.create_namespace(name, value).await
    }

    pub async fn seed_pod(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.resources
            .create_resource("v1", "Pod", Some(namespace), name, value)
            .await
    }

    pub async fn seed_service(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.resources
            .create_resource("v1", "Service", Some(namespace), name, value)
            .await
    }

    pub async fn seed_endpoints(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.resources
            .create_resource("v1", "Endpoints", Some(namespace), name, value)
            .await
    }

    pub async fn seed_endpoint_slice(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.resources
            .create_resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                name,
                value,
            )
            .await
    }

    pub async fn endpoints(&self, namespace: &str, name: &str) -> anyhow::Result<Option<Resource>> {
        self.resources
            .get_resource("v1", "Endpoints", Some(namespace), name)
            .await
    }

    pub async fn endpoint_slice(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<Resource>> {
        self.resources
            .get_resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                name,
            )
            .await
    }

    pub async fn endpoint_slices(
        &self,
        namespace: &str,
        label_selector: Option<&str>,
    ) -> anyhow::Result<Vec<Resource>> {
        Ok(self
            .resources
            .list_resources(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                ResourceListOptions::new(label_selector, None, None, None),
            )
            .await?
            .items)
    }

    pub async fn replace_endpoints(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
        expected_rv: i64,
    ) -> anyhow::Result<Resource> {
        self.resources
            .update_resource("v1", "Endpoints", Some(namespace), name, value, expected_rv)
            .await
    }

    pub async fn remove_endpoints(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        self.resources
            .delete_resource("v1", "Endpoints", Some(namespace), name)
            .await
    }

    pub async fn remove_endpoint_slice(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        self.resources
            .delete_resource(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                name,
            )
            .await
    }

    pub async fn current_resource_version(&self) -> anyhow::Result<i64> {
        self.resources.current_resource_version().await
    }

    pub fn value_with_resource_version(
        value: impl Into<std::sync::Arc<serde_json::Value>>,
        resource_version: i64,
    ) -> serde_json::Value {
        let mut value = (*value.into()).clone();
        value["metadata"]["resourceVersion"] =
            serde_json::Value::String(resource_version.to_string());
        value
    }
}

impl ResourceTestStore {
    /// Consumes the canonical embedded store at the composition boundary while
    /// retaining no public path back to its concrete type.
    pub fn from_embedded_for_test_support(datastore: crate::sqlite::embedded::Datastore) -> Self {
        Self { datastore }
    }

    pub async fn create(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .create_resource(api_version, kind, namespace, name, value)
            .await
    }

    pub async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.create(api_version, kind, namespace, name, value).await
    }

    pub async fn create_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.datastore.create_namespace(name, value).await
    }

    pub async fn get(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<Option<Resource>> {
        self.datastore
            .get_resource(api_version, kind, namespace, name)
            .await
    }

    pub async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<Option<Resource>> {
        self.get(api_version, kind, namespace, name).await
    }

    pub async fn list(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        options: ResourceListOptions<'_>,
    ) -> anyhow::Result<klights_cluster_store::ResourceList> {
        self.datastore
            .list_resources(api_version, kind, namespace, options)
            .await
    }

    pub async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        options: ResourceListOptions<'_>,
    ) -> anyhow::Result<klights_cluster_store::ResourceList> {
        self.list(api_version, kind, namespace, options).await
    }

    pub async fn update_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        resource_version: i64,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .update_resource(api_version, kind, namespace, name, data, resource_version)
            .await
    }

    pub async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .update_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                data,
                preconditions,
            )
            .await
    }

    pub async fn update_main_strict(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .update_main_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                data,
                preconditions,
            )
            .await
    }

    pub async fn update_status_strict(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: serde_json::Value,
        resource_version: i64,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .update_status_only(
                api_version,
                kind,
                namespace,
                name,
                status,
                Some(resource_version),
            )
            .await
    }

    pub async fn update_status_only(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: serde_json::Value,
        resource_version: Option<i64>,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .update_status_only(api_version, kind, namespace, name, status, resource_version)
            .await
    }

    pub async fn update_status_only_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .update_status_only_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                status,
                preconditions,
            )
            .await
    }

    pub async fn patch_resource_latest_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: klights_cluster_core::ResourcePatchRequest,
    ) -> anyhow::Result<Option<Resource>> {
        self.datastore
            .patch_resource_latest_with_preconditions(api_version, kind, namespace, name, request)
            .await
    }

    pub async fn delete_non_pod_strict(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            (api_version, kind) != ("v1", "Pod"),
            "generic Pod deletion is forbidden"
        );
        self.datastore
            .delete_resource_with_preconditions(api_version, kind, namespace, name, preconditions)
            .await
    }

    pub async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<()> {
        self.delete_non_pod_strict(api_version, kind, namespace, name, preconditions)
            .await
    }

    pub async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<()> {
        let resource = self
            .get_resource(api_version, kind, namespace, name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("resource not found"))?;
        self.delete_non_pod_strict(
            api_version,
            kind,
            namespace,
            name,
            ResourcePreconditions::from_resource(&resource),
        )
        .await
    }

    pub async fn current_resource_version(&self) -> anyhow::Result<i64> {
        self.datastore.get_current_resource_version().await
    }

    pub async fn get_current_resource_version(&self) -> anyhow::Result<i64> {
        self.current_resource_version().await
    }

    pub async fn watch_replay_position(
        &self,
    ) -> anyhow::Result<klights_cluster_core::WatchReplayPosition> {
        self.datastore.current_watch_replay_position().await
    }

    pub async fn get_namespace(&self, name: &str) -> anyhow::Result<Option<Resource>> {
        self.datastore.get_namespace(name).await
    }

    pub async fn update_namespace(
        &self,
        name: &str,
        data: serde_json::Value,
        expected_rv: i64,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .update_namespace(name, data, expected_rv)
            .await
    }

    pub async fn list_namespace_pods(&self, namespace: &str) -> anyhow::Result<Vec<Resource>> {
        self.datastore
            .list_namespace_resources_of_kind(namespace, "Pod")
            .await
    }

    pub async fn list_namespace_non_pod_resources(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<Resource>> {
        self.datastore
            .list_namespace_resources_excluding_kind(namespace, "Pod")
            .await
    }

    pub async fn count_namespace_resources(&self, namespace: &str) -> anyhow::Result<i64> {
        self.datastore.count_namespace_resources(namespace).await
    }

    pub async fn delete_namespace(&self, namespace: &str) -> anyhow::Result<()> {
        self.datastore.delete_namespace(namespace).await
    }

    pub async fn list_resources_by_owner_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        owner_uid: &str,
    ) -> anyhow::Result<Vec<Resource>> {
        self.datastore
            .list_resources_by_owner_uid(api_version, kind, namespace, owner_uid)
            .await
    }

    pub async fn advance_resource_version_after(&self, min_rv: i64) -> anyhow::Result<i64> {
        self.datastore.advance_resource_version_after(min_rv).await
    }

    pub async fn list_watch_events_since(
        &self,
        targets: &[klights_cluster_store::WatchTarget],
        resource_version: i64,
    ) -> anyhow::Result<Vec<klights_cluster_store::CatchUpResource>> {
        self.datastore
            .list_watch_events_since(targets, resource_version)
            .await
    }

    pub async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> anyhow::Result<usize> {
        self.datastore.gc_watch_events(max_rows, batch_cap).await
    }

    pub async fn apply_resource_batch(
        &self,
        operations: Vec<klights_cluster_core::ResourceBatchOperation>,
    ) -> anyhow::Result<()> {
        self.datastore.apply_resource_batch(operations).await
    }

    pub async fn apply_committed_resource_batch(
        &self,
        commit: klights_cluster_core::LogApplyCommit,
    ) -> anyhow::Result<()> {
        self.datastore.apply_log_apply_commit(commit).await
    }

    pub async fn apply_log_apply_commit(&self, commit: LogApplyCommit) -> anyhow::Result<()> {
        self.datastore.apply_log_apply_commit(commit).await
    }

    pub async fn apply_raft_log_apply_commit(
        &self,
        commit: LogApplyCommit,
    ) -> anyhow::Result<klights_cluster_store::CommittedRaftApplyReceipt> {
        self.datastore
            .apply_raft_log_apply_commit_receipt(commit)
            .await
    }

    pub async fn build_log_apply_commit_for_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::StorageCommand,
        authoring_node: &str,
    ) -> Result<klights_cluster_core::BuildOutboxOutcome, klights_cluster_core::OutboxApplyError>
    {
        self.datastore
            .build_log_apply_commit_for_outbox(idempotency_key, operation, command, authoring_node)
            .await
    }

    pub fn install_resource_mutation_pause(
        &self,
        operation: ResourceMutationPauseOperation,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> std::sync::Arc<ResourceMutationPause> {
        self.datastore.install_resource_mutation_pause(
            operation,
            api_version,
            kind,
            namespace,
            name,
        )
    }

    /// Finds the exact resources owned by a fixture parent without exposing a
    /// generic query backend.
    pub async fn owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<Vec<Resource>> {
        self.datastore
            .find_owned_resources(owner_uid, namespace)
            .await
    }
}

impl GcTestStore {
    pub async fn open(
        supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            datastore: crate::sqlite::embedded::Datastore::new_for_gc_test_support(supervisor)
                .await?,
        })
    }

    pub async fn seed_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.datastore.create_namespace(name, value).await
    }

    pub async fn seed_fixture(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .create_resource(api_version, kind, namespace, name, value)
            .await
    }

    pub async fn create(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        self.seed_fixture(api_version, kind, namespace, name, value)
            .await
    }

    pub async fn get(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<Option<Resource>> {
        self.observe_fixture(api_version, kind, namespace, name)
            .await
    }

    pub async fn list(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<Vec<Resource>> {
        self.list_fixtures(api_version, kind, namespace).await
    }

    pub fn install_resource_mutation_pause(
        &self,
        operation: ResourceMutationPauseOperation,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> std::sync::Arc<ResourceMutationPause> {
        self.datastore.install_resource_mutation_pause(
            operation,
            api_version,
            kind,
            namespace,
            name,
        )
    }

    pub async fn observe_fixture(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<Option<Resource>> {
        self.datastore
            .get_resource(api_version, kind, namespace, name)
            .await
    }

    pub async fn list_fixtures(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<Vec<Resource>> {
        Ok(self
            .datastore
            .list_resources(api_version, kind, namespace, ResourceListOptions::all())
            .await?
            .items)
    }

    pub async fn remove_non_pod_fixture(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<()> {
        assert_ne!(
            (api_version, kind),
            ("v1", "Pod"),
            "generic Pod removal is forbidden"
        );
        self.datastore
            .delete_resource(api_version, kind, namespace, name)
            .await
    }

    pub async fn update_fixture(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .update_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                data,
                preconditions,
            )
            .await
    }

    pub async fn update_main_fixture(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .update_main_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                data,
                preconditions,
            )
            .await
    }

    pub async fn update_fixture_status(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: serde_json::Value,
        resource_version: i64,
    ) -> anyhow::Result<Resource> {
        self.datastore
            .update_status_only(
                api_version,
                kind,
                namespace,
                name,
                status,
                Some(resource_version),
            )
            .await
    }

    pub async fn owned_fixtures(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<Vec<Resource>> {
        self.datastore
            .find_owned_resources(owner_uid, namespace)
            .await
    }

    pub async fn empty_uid_owned_fixtures(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<Vec<Resource>> {
        self.datastore
            .find_owned_by_name_kind_empty_uid(owner_api_version, owner_name, owner_kind, namespace)
            .await
    }

    pub async fn finalize_bound_pod_for_actor(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<()> {
        let live = self
            .datastore
            .get_resource("v1", "Pod", Some(namespace), name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("actor-owned Pod is gone"))?;
        let node_name = live
            .data
            .pointer("/spec/nodeName")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let terminating = live
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.is_empty());
        let finalizers_pending = live
            .data
            .pointer("/metadata/finalizers")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| !items.is_empty());
        if live.uid != uid || node_name.is_empty() || !terminating || finalizers_pending {
            anyhow::bail!("actor finalization preconditions are not satisfied");
        }
        self.datastore
            .delete_resource_with_preconditions(
                "v1",
                "Pod",
                Some(namespace),
                name,
                ResourcePreconditions::uid_and_resource_version(uid, live.resource_version),
            )
            .await
    }

    /// Mark the exact Pod UID terminating before waking its lifecycle actor.
    pub async fn mark_pod_deleting_for_actor(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<bool> {
        let Some(live) = self
            .datastore
            .get_resource("v1", "Pod", Some(namespace), name)
            .await?
        else {
            return Ok(false);
        };
        if live.uid != uid {
            return Ok(false);
        }
        if live
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.is_empty())
        {
            return Ok(true);
        }
        let mut marked = live.data.as_ref().clone();
        marked["metadata"]["deletionTimestamp"] = serde_json::json!("2026-01-01T00:00:00Z");
        match self
            .datastore
            .update_resource_with_preconditions(
                "v1",
                "Pod",
                Some(namespace),
                name,
                marked,
                ResourcePreconditions::uid_and_resource_version(uid, live.resource_version),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if error.to_string().contains("precondition")
                    || error.to_string().contains("conflict") =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn finalize_unscheduled_pod_cas(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        observed_resource_version: i64,
    ) -> anyhow::Result<bool> {
        let Some(live) = self
            .datastore
            .get_resource("v1", "Pod", Some(namespace), name)
            .await?
        else {
            return Ok(false);
        };
        let node_name = live
            .data
            .pointer("/spec/nodeName")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let terminating = live
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.is_empty());
        let finalizers_pending = live
            .data
            .pointer("/metadata/finalizers")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| !items.is_empty());
        if live.uid != uid
            || live.resource_version != observed_resource_version
            || !node_name.is_empty()
            || !terminating
            || finalizers_pending
        {
            return Ok(false);
        }
        match self
            .datastore
            .delete_resource_with_preconditions(
                "v1",
                "Pod",
                Some(namespace),
                name,
                ResourcePreconditions::uid_and_resource_version(uid, observed_resource_version),
            )
            .await
        {
            Ok(()) => Ok(true),
            Err(error)
                if error.to_string().contains("precondition")
                    || error.to_string().contains("conflict") =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }
}

/// Focused cluster-side fixture for node-delivery integration tests.
#[derive(Clone)]
pub struct NodeDeliveryTestCluster {
    datastore: crate::sqlite::embedded::Datastore,
    node_events: tokio::sync::broadcast::Sender<String>,
}

impl NodeDeliveryTestCluster {
    pub async fn open(
        supervisor: std::sync::Arc<klights_supervisor::TaskSupervisor>,
    ) -> anyhow::Result<Self> {
        let (node_events, _) = tokio::sync::broadcast::channel(32);
        Ok(Self {
            datastore: crate::sqlite::embedded::Datastore::new_for_gc_test_support(supervisor)
                .await?,
            node_events,
        })
    }

    pub async fn create(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        let resource = self
            .datastore
            .create_resource(api_version, kind, namespace, name, value)
            .await?;
        if api_version == "v1" && kind == "Node" {
            let _ = self.node_events.send(name.to_string());
        }
        Ok(resource)
    }

    pub async fn get(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<Option<Resource>> {
        self.datastore
            .get_resource(api_version, kind, namespace, name)
            .await
    }

    pub async fn list(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<Vec<Resource>> {
        Ok(self
            .datastore
            .list_resources(api_version, kind, namespace, ResourceListOptions::all())
            .await?
            .items)
    }

    pub async fn update(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> anyhow::Result<Resource> {
        let resource = self
            .datastore
            .update_resource(
                api_version,
                kind,
                namespace,
                name,
                value,
                expected_resource_version,
            )
            .await?;
        if api_version == "v1" && kind == "Node" {
            let _ = self.node_events.send(name.to_string());
        }
        Ok(resource)
    }

    pub async fn replace_if_current(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
        current: &Resource,
    ) -> anyhow::Result<Resource> {
        let resource = self
            .datastore
            .update_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                value,
                ResourcePreconditions::from_resource(current),
            )
            .await?;
        if api_version == "v1" && kind == "Node" {
            let _ = self.node_events.send(name.to_string());
        }
        Ok(resource)
    }

    pub async fn update_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        value: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> anyhow::Result<Resource> {
        let resource = self
            .datastore
            .update_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                value,
                preconditions,
            )
            .await?;
        if api_version == "v1" && kind == "Node" {
            let _ = self.node_events.send(name.to_string());
        }
        Ok(resource)
    }

    pub async fn seed_namespace(&self, name: &str, value: serde_json::Value) -> anyhow::Result<()> {
        self.datastore
            .create_namespace(name, value)
            .await
            .map(|_| ())
    }

    pub async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> anyhow::Result<()> {
        self.datastore
            .allocate_node_subnet(node_name, cluster_cidr, node_ip)
            .await
            .map(|_| ())
    }

    pub async fn current_resource_version(&self) -> anyhow::Result<i64> {
        self.datastore.get_current_resource_version().await
    }

    pub async fn watch_replay_position(
        &self,
    ) -> anyhow::Result<klights_cluster_core::WatchReplayPosition> {
        self.datastore.current_watch_replay_position().await
    }

    pub async fn outbox_stream_watermarks(
        &self,
    ) -> anyhow::Result<Vec<klights_cluster_core::OutboxStreamWatermark>> {
        self.datastore.list_outbox_stream_watermarks().await
    }

    pub fn subscribe_node_events(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.node_events.subscribe()
    }

    pub async fn stamp_node_routing_metadata(
        &self,
        node_name: &str,
        node: &mut serde_json::Value,
    ) -> anyhow::Result<bool> {
        let mut changed = false;
        if let Some(subnet) = self.datastore.get_node_subnet(node_name).await? {
            changed |= klights_cluster_core::set_node_pod_cidr(node, &subnet.subnet.to_string());
        }
        if let Some(metadata) = self.datastore.get_node_dataplane(node_name).await? {
            changed |= klights_types::set_node_dataplane_annotations(
                node,
                &metadata.endpoint.to_string(),
                metadata.mode.as_str(),
                metadata.encryption.as_str(),
                metadata.public_key.as_ref().map(|key| key.as_str()),
                metadata.port,
            );
        }
        Ok(changed)
    }

    pub async fn apply_outbox(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: klights_cluster_core::StorageCommand,
        authoring_node: &str,
        watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
    ) -> Result<klights_cluster_core::OutboxApplyOutcome, klights_cluster_core::OutboxApplyError>
    {
        self.datastore
            .apply_outbox_transactionally_with_watermark(
                idempotency_key,
                operation,
                command,
                authoring_node,
                watermark,
            )
            .await
    }
}

/// Build the RV-zero live-apply template consumed by passive-store tests.
///
/// Public resource versions are allocated by committed apply, so legacy
/// fixture RVs are deliberately erased before validation.
pub fn test_live_commit(
    candidate_resource_version: i64,
    mut mutations: Vec<LogApplyMutation>,
) -> LogApplyCommit {
    fn clear_nested_resource_version(data: &mut serde_json::Value) {
        if let Some(metadata) = data
            .get_mut("metadata")
            .and_then(serde_json::Value::as_object_mut)
        {
            metadata.remove("resourceVersion");
        }
    }

    for mutation in &mut mutations {
        match mutation {
            LogApplyMutation::PutResource(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
            }
            LogApplyMutation::PatchResourceLatest(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.patch);
            }
            LogApplyMutation::PutNamespace(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
            }
            LogApplyMutation::PutWatchEvent(row) => {
                row.resource_version = 0;
                clear_nested_resource_version(&mut row.data);
                if let Some(object) = row.data.get_mut("object") {
                    clear_nested_resource_version(object);
                }
            }
            LogApplyMutation::PutPodCleanupIntent(row) => row.resource_version = 0,
            LogApplyMutation::PutAppliedOutbox(row) => row.applied_rv = None,
            LogApplyMutation::AdvanceResourceVersion { resource_version } => {
                *resource_version = 0;
            }
            _ => {}
        }
    }
    let _ = candidate_resource_version;
    LogApplyCommit::try_new(mutations).expect("test live commit must be an RV-zero template")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use klights_cluster_core::WatchReplayPosition;
    use klights_cluster_store::{
        DurableWatchTarget, WatchHistoryError, WatchHistoryFuture, WatchHistoryRead,
        WatchHistoryRequest,
    };

    use super::{
        EndpointResourceFixture, ResourceMutationPauseOperation, ResourcePreconditions,
        toggle_failing_watch_history_for_test_support,
    };

    #[test]
    fn p12_2b_resource_pause_support_preserves_every_mutation_coordinate() {
        let operations = [
            ResourceMutationPauseOperation::MainUpdate,
            ResourceMutationPauseOperation::PatchLatest,
            ResourceMutationPauseOperation::BuildPatchCommand,
        ];

        assert_eq!(operations.len(), 3);
    }

    #[test]
    fn p12_2d_endpoint_fixture_is_a_narrow_named_resource_owner() {
        fn accepts_endpoint_fixture(_: Option<EndpointResourceFixture>) {}
        accepts_endpoint_fixture(None);
    }

    #[derive(Default)]
    struct RecordingHistory;

    impl klights_cluster_store::DurableWatchHistoryRead for RecordingHistory {
        fn replay_watch_history(
            &self,
            _request: WatchHistoryRequest,
        ) -> WatchHistoryFuture<'_, WatchHistoryRead> {
            Box::pin(async { Ok(WatchHistoryRead::Expired) })
        }

        fn list_replay_floors(
            &self,
        ) -> WatchHistoryFuture<'_, Vec<klights_cluster_store::DurableReplayFloor>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    #[tokio::test]
    async fn p12_2b_watch_history_control_fails_only_replay_after_activation() {
        let control = super::WatchHistoryFailureControl::new();
        let history = toggle_failing_watch_history_for_test_support(
            Arc::new(RecordingHistory),
            control.clone(),
        );
        let request = WatchHistoryRequest::new(
            vec![DurableWatchTarget::cluster("v1", "ConfigMap")],
            WatchReplayPosition::default(),
            1,
        )
        .expect("valid positioned ConfigMap replay request");

        assert!(matches!(
            history.replay_watch_history(request.clone()).await,
            Ok(WatchHistoryRead::Expired)
        ));
        assert_eq!(
            history.list_replay_floors().await.expect("delegate floors"),
            Vec::new()
        );

        control.fail_subsequent_reads();
        assert!(matches!(
            history.replay_watch_history(request).await,
            Err(WatchHistoryError::PersistenceFailed { message })
                if message == "injected live replay read failure"
        ));
        assert_eq!(
            history.list_replay_floors().await.expect("delegate floors"),
            Vec::new()
        );
    }

    #[tokio::test]
    async fn p12_2b_resource_pause_targets_main_update_and_blocks_until_resumed() {
        let store = super::GcTestStore::open(Arc::new(klights_supervisor::TaskSupervisor::new(
            Default::default(),
        )))
        .await
        .expect("open focused resource test store");
        let resource = store
            .create(
                "v1",
                "ConfigMap",
                Some("default"),
                "paused",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": "paused", "namespace": "default"},
                    "data": {"before": "pause"}
                }),
            )
            .await
            .expect("seed ConfigMap");
        let pause = store.install_resource_mutation_pause(
            ResourceMutationPauseOperation::MainUpdate,
            "v1",
            "ConfigMap",
            Some("default"),
            "paused",
        );
        let update_store = store.clone();
        let update = tokio::spawn(async move {
            update_store
                .update_main_fixture(
                    "v1",
                    "ConfigMap",
                    Some("default"),
                    "paused",
                    serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {"name": "paused", "namespace": "default"},
                        "data": {"after": "resume"}
                    }),
                    ResourcePreconditions::from_resource(&resource),
                )
                .await
        });

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            pause.wait_until_reached(),
        )
        .await
        .expect("targeted main update reaches the pause");
        assert!(
            !update.is_finished(),
            "the exact mutation must remain blocked before resume"
        );
        pause.resume();
        let updated = update
            .await
            .expect("update task joins")
            .expect("resumed strict main update succeeds");
        assert_eq!(updated.data["data"]["after"], "resume");
    }
}
