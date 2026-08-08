//! Kubelet-owned ordinary Pod repository service.
//!
//! Query, metadata update, graceful termination marking, and lifecycle wakeup
//! routing are deliberately expressed over focused ports. Neither this module
//! nor its public surface can remove a Pod row: bound-Pod actor finalization
//! and the leader-only unscheduled-Pod CAS remain separate capabilities.

pub mod delete_coordinator;
pub mod delete_deadline;
pub mod status;
pub mod store;
pub mod workqueue;
pub use status::PodStatusWriter;

use std::sync::Arc;
use std::time::Duration;

use klights_cluster_core::{Resource, ResourcePreconditions, StorageCommand};
use klights_network_api::{PodNetworkAssignmentKey, PodNetworkAssignmentWaiter};
use klights_node_store::{PodNetworkCache, SandboxKey};
use klights_pod_api::{
    PodGetRequest, PodLifecycleFuture, PodLifecycleWakeup, PodLifecycleWakeupRequest,
    PodListRequest, PodListResult, PodMarkTerminating, PodMarkTerminatingRequest,
    PodMetadataPatchRequest, PodMutationTarget, PodOwnerListRequest, PodPersistence, PodQuery,
    PodRepositoryError, PodRepositoryFuture, PodUpdate, PodUpdateOperation, PodUpdateRequest,
};
use klights_reconcile_api::{PodMutationReconcileRequest, PodMutationReconcileSink};
use serde_json::{Map, Value};

/// Standard post-sandbox status update authored by the kubelet lifecycle.
#[derive(Debug, Clone)]
pub struct PodStatusUpdate {
    pub phase: String,
    pub pod_ip: String,
    pub host_ip: String,
    pub container_statuses: Vec<Value>,
    pub init_container_statuses: Option<Vec<Value>>,
    pub qos_class: Option<String>,
}

/// Runtime-owned status subset; networking and condition fields are preserved.
#[derive(Debug, Clone)]
pub struct RuntimeReconcileStatus {
    pub phase: String,
    pub container_statuses: Vec<Value>,
}

/// Pod and host IPs resolved from the node-local CNI assignment state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PodNetworkAssignment {
    pub pod_ip: String,
    pub host_ip: String,
}

/// Immutable request for a single CNI-produced Pod network assignment.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PodNetworkAssignmentRequest {
    sandbox_id: String,
    pod: klights_types::PodIdentity,
    host_network: bool,
}

impl PodNetworkAssignmentRequest {
    pub fn try_new(
        sandbox_id: impl Into<String>,
        pod: klights_types::PodIdentity,
        host_network: bool,
    ) -> std::result::Result<Self, PodNetworkAssignmentError> {
        let sandbox_id = sandbox_id.into();
        PodNetworkAssignmentKey::try_new(&sandbox_id, &pod.namespace, &pod.name, &pod.uid)
            .map_err(|error| PodNetworkAssignmentError::InvalidRequest(error.to_string()))?;
        Ok(Self {
            sandbox_id,
            pod,
            host_network,
        })
    }

    pub fn sandbox_id(&self) -> &str {
        &self.sandbox_id
    }

    pub fn pod(&self) -> &klights_types::PodIdentity {
        &self.pod
    }

    pub fn host_network(&self) -> bool {
        self.host_network
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PodNetworkAssignmentError {
    InvalidRequest(String),
    CacheUnavailable(String),
    WaitClosed(String),
    TimedOut(String),
    MissingAssignment(String),
}

impl std::fmt::Display for PodNetworkAssignmentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (category, message) = match self {
            Self::InvalidRequest(message) => ("invalid network assignment request", message),
            Self::CacheUnavailable(message) => ("network assignment cache unavailable", message),
            Self::WaitClosed(message) => ("network assignment wait closed", message),
            Self::TimedOut(message) => ("pod network assignment timed out", message),
            Self::MissingAssignment(message) => ("network assignment row missing", message),
        };
        write!(formatter, "{category}: {message}")
    }
}

impl std::error::Error for PodNetworkAssignmentError {}

pub type PodNetworkAssignmentFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = std::result::Result<PodNetworkAssignment, PodNetworkAssignmentError>,
            > + Send
            + 'a,
    >,
>;

/// Focused read-only query for a Pod's node-local network assignment.
pub trait PodNetworkAssignmentQuery: Send + Sync {
    fn read_pod_network_assignment(
        &self,
        request: PodNetworkAssignmentRequest,
    ) -> PodNetworkAssignmentFuture<'_>;
}

const ASSIGNMENT_WAIT: Duration = Duration::from_secs(30);

/// Event-driven node-local network assignment reader.
pub struct PodNetworkService {
    cache: Arc<dyn PodNetworkCache>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    assignment_waiter: Arc<dyn PodNetworkAssignmentWaiter>,
    host_ip: crate::context::HostIpState,
}

impl PodNetworkService {
    pub fn new(
        cache: Arc<dyn PodNetworkCache>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        assignment_waiter: Arc<dyn PodNetworkAssignmentWaiter>,
        host_ip: crate::context::HostIpState,
    ) -> Self {
        Self {
            cache,
            supervisor,
            assignment_waiter,
            host_ip,
        }
    }

    async fn lookup_assignment(
        &self,
        request: &PodNetworkAssignmentRequest,
        host_ip: &str,
    ) -> std::result::Result<Option<PodNetworkAssignment>, PodNetworkAssignmentError> {
        let sandbox_id = SandboxKey::try_new(request.sandbox_id())
            .map_err(|error| PodNetworkAssignmentError::InvalidRequest(error.to_string()))?;
        if let Some(row) = self
            .cache
            .get_network_for_assignment(sandbox_id, request.pod().clone())
            .await
            .map_err(|error| PodNetworkAssignmentError::CacheUnavailable(error.to_string()))?
        {
            return Ok(Some(PodNetworkAssignment {
                pod_ip: row.ip_addr().to_string(),
                host_ip: host_ip.to_string(),
            }));
        }
        if let Some(row) = self
            .cache
            .get_network_for_pod(request.pod().clone())
            .await
            .map_err(|error| PodNetworkAssignmentError::CacheUnavailable(error.to_string()))?
        {
            return Ok(Some(PodNetworkAssignment {
                pod_ip: row.ip_addr().to_string(),
                host_ip: host_ip.to_string(),
            }));
        }
        Ok(None)
    }
}

pub fn pod_network_assignment_query(
    cache: Arc<dyn PodNetworkCache>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    assignment_waiter: Arc<dyn PodNetworkAssignmentWaiter>,
    host_ip: crate::context::HostIpState,
) -> Arc<dyn PodNetworkAssignmentQuery> {
    Arc::new(PodNetworkService::new(
        cache,
        supervisor,
        assignment_waiter,
        host_ip,
    ))
}

impl PodNetworkAssignmentQuery for PodNetworkService {
    fn read_pod_network_assignment(
        &self,
        request: PodNetworkAssignmentRequest,
    ) -> PodNetworkAssignmentFuture<'_> {
        Box::pin(async move {
            let host_ip = self.host_ip.current().to_string();
            if request.host_network() {
                return Ok(PodNetworkAssignment {
                    pod_ip: host_ip.clone(),
                    host_ip,
                });
            }

            let key = PodNetworkAssignmentKey::try_new(
                request.sandbox_id(),
                &request.pod().namespace,
                &request.pod().name,
                &request.pod().uid,
            )
            .map_err(|error| PodNetworkAssignmentError::InvalidRequest(error.to_string()))?;
            let mut subscription = self
                .assignment_waiter
                .subscribe(key)
                .map_err(|error| PodNetworkAssignmentError::WaitClosed(error.to_string()))?;

            if let Some(assignment) = self.lookup_assignment(&request, &host_ip).await? {
                return Ok(assignment);
            }
            let wait_result = self
                .supervisor
                .timeout(
                    "pod_network_assignment_wait",
                    ASSIGNMENT_WAIT,
                    subscription.wait(),
                )
                .await
                .map_err(|error| PodNetworkAssignmentError::WaitClosed(error.to_string()))?;
            match wait_result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    return Err(PodNetworkAssignmentError::WaitClosed(format!(
                        "pod network assignment bus closed for sandbox {} or pod {}/{} uid {}: {error}",
                        request.sandbox_id(),
                        request.pod().namespace,
                        request.pod().name,
                        request.pod().uid,
                    )));
                }
                Err(_) => {
                    return Err(PodNetworkAssignmentError::TimedOut(format!(
                        "pod network assignment wait timed out for sandbox {} or pod {}/{} uid {}",
                        request.sandbox_id(),
                        request.pod().namespace,
                        request.pod().name,
                        request.pod().uid,
                    )));
                }
            }

            self.lookup_assignment(&request, &host_ip)
                .await?
                .ok_or_else(|| {
                    PodNetworkAssignmentError::MissingAssignment(format!(
                        "pod network assignment notification arrived without row for sandbox {} or pod {}/{} uid {}",
                        request.sandbox_id(),
                        request.pod().namespace,
                        request.pod().name,
                        request.pod().uid,
                    ))
                })
        })
    }
}

/// Focused graceful-termination marker. This intent can update a Pod and
/// enqueue work, but it can never finalize or remove a Pod row.
pub trait PodTerminationPort: Send + Sync {
    fn mark_terminating(&self, target: PodMutationTarget) -> PodRepositoryFuture<'_, Resource>;
}

pub struct PodMetadataDependencies {
    pub persistence: Arc<dyn PodPersistence>,
    pub outbox: Option<Arc<crate::outbox::Outbox>>,
    pub remote_delivery_required: bool,
    pub mutation_reconcile: Arc<dyn PodMutationReconcileSink>,
    pub wall_clock: Arc<dyn crate::runtime_clock::RuntimeClock>,
}

pub struct PodMetadataService {
    dependencies: PodMetadataDependencies,
}

impl PodMetadataService {
    pub fn new(dependencies: PodMetadataDependencies) -> Self {
        Self { dependencies }
    }

    pub fn update_pod_from<'a>(
        &'a self,
        query: &'a dyn PodQuery,
        request: PodUpdateRequest,
    ) -> PodRepositoryFuture<'a, Resource> {
        self.update_metadata(query, request)
    }

    fn update_metadata<'a>(
        &'a self,
        query: &'a dyn PodQuery,
        request: PodUpdateRequest,
    ) -> PodRepositoryFuture<'a, Resource> {
        Box::pin(async move {
            let (target, operation) = request.into_parts();
            let current = query
                .get_pod(PodGetRequest::try_by_name(
                    target.namespace(),
                    target.name(),
                )?)
                .await?
                .ok_or_else(|| PodRepositoryError::not_found(target.namespace(), target.name()))?;
            if let Some(expected_uid) = target.uid()
                && current.uid != expected_uid
            {
                return Err(PodRepositoryError::uid_mismatch(expected_uid, current.uid));
            }

            let previous = current.clone();
            let (patch, body, endpoint_relevant) = match operation {
                PodUpdateOperation::MergeLabels(labels) => {
                    let mut label_patch = Map::new();
                    for label in labels {
                        let (key, value) = label.into_parts();
                        label_patch.insert(key, Value::String(value));
                    }
                    let patch = serde_json::json!({"metadata": {"labels": label_patch}});
                    (patch.clone(), merge_metadata_patch(&current, &patch)?, true)
                }
                PodUpdateOperation::ReplaceOwnerReferences(owner_references) => {
                    let owner_references = owner_references
                        .into_iter()
                        .map(owner_reference_value)
                        .collect::<Vec<_>>();
                    let patch =
                        serde_json::json!({"metadata": {"ownerReferences": owner_references}});
                    (
                        patch.clone(),
                        merge_metadata_patch(&current, &patch)?,
                        false,
                    )
                }
                PodUpdateOperation::RecordSandboxId(sandbox_id) => {
                    let patch = serde_json::json!({
                        "metadata": {"annotations": {"klights.dev/sandbox-id": sandbox_id}}
                    });
                    (
                        patch.clone(),
                        merge_metadata_patch(&current, &patch)?,
                        false,
                    )
                }
            };

            if self.dependencies.remote_delivery_required {
                let outbox = self.dependencies.outbox.as_deref().ok_or_else(|| {
                    PodRepositoryError::unavailable(
                        "outbox is unavailable for node-local Pod metadata delivery",
                    )
                })?;
                let subject_key = format!(
                    "v1/Pod/{}/{}/{}",
                    target.namespace(),
                    target.name(),
                    current.uid
                );
                crate::outbox::OutboxSendPlanner::new(Some(outbox))
                    .route(crate::outbox::OutboxCommand {
                        idempotency_key: format!("{}:{}", subject_key, uuid::Uuid::new_v4()),
                        operation: crate::outbox::OutboxOperation::PodMetadata,
                        subject: crate::outbox::OutboxSubject {
                            key: subject_key,
                            namespace: Some(target.namespace().to_string()),
                            name: target.name().to_string(),
                            uid: Some(current.uid.clone()),
                        },
                        pod_uid: current.uid.clone(),
                        command: StorageCommand::PatchResource {
                            api_version: "v1".to_string(),
                            kind: "Pod".to_string(),
                            namespace: Some(target.namespace().to_string()),
                            name: target.name().to_string(),
                            patch_kind: klights_cluster_core::PatchKind::Merge,
                            patch,
                            preconditions: ResourcePreconditions {
                                uid: Some(current.uid.clone()),
                                resource_version: Some(current.resource_version),
                            },
                            strict_resource_version: true,
                        },
                        now_ms: self.dependencies.wall_clock.now_ms(),
                    })
                    .await
                    .map_err(|error| PodRepositoryError::unavailable(error.to_string()))?;
                return Ok(synthetic_resource(current, body));
            }

            let updated = self
                .dependencies
                .persistence
                .patch_pod_metadata(PodMetadataPatchRequest {
                    namespace: target.namespace().to_string(),
                    name: target.name().to_string(),
                    expected_uid: current.uid.clone(),
                    expected_resource_version: current.resource_version,
                    patch,
                })
                .await?;
            if endpoint_relevant
                && let Err(error) = self
                    .dependencies
                    .mutation_reconcile
                    .reconcile_pod_mutation(PodMutationReconcileRequest::ServicesAfterUpdate {
                        previous,
                        updated: updated.clone(),
                    })
                    .await
            {
                tracing::debug!(
                    target: "klights::kubelet::pod_repository",
                    error = %error,
                    pod = %target.name(),
                    "failed to enqueue Service reconcile after Pod label merge"
                );
            }
            Ok(updated)
        })
    }
}

/// Ordinary Pod service composed from canonical neutral ports.
pub struct PodRepositoryService {
    query: Arc<dyn PodQuery>,
    metadata: PodMetadataService,
    termination: Arc<dyn PodTerminationPort>,
}

impl PodRepositoryService {
    pub fn new(
        query: Arc<dyn PodQuery>,
        metadata: PodMetadataDependencies,
        termination: Arc<dyn PodTerminationPort>,
    ) -> Self {
        Self {
            query,
            metadata: PodMetadataService::new(metadata),
            termination,
        }
    }

    pub fn mark_pod_terminating_from<'a, Termination>(
        termination: &'a Termination,
        request: PodMarkTerminatingRequest,
    ) -> PodRepositoryFuture<'a, Resource>
    where
        Termination: PodTerminationPort + ?Sized,
    {
        termination.mark_terminating(request.into_target())
    }
}

impl PodQuery for PodRepositoryService {
    fn get_pod(&self, request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
        self.query.get_pod(request)
    }

    fn list_pods(&self, request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
        self.query.list_pods(request)
    }

    fn list_pods_by_owner_uid(
        &self,
        request: PodOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<Resource>> {
        self.query.list_pods_by_owner_uid(request)
    }
}

impl PodUpdate for PodRepositoryService {
    fn update_pod(&self, request: PodUpdateRequest) -> PodRepositoryFuture<'_, Resource> {
        self.metadata.update_pod_from(self.query.as_ref(), request)
    }
}

impl PodMarkTerminating for PodRepositoryService {
    fn mark_pod_terminating(
        &self,
        request: PodMarkTerminatingRequest,
    ) -> PodRepositoryFuture<'_, Resource> {
        Self::mark_pod_terminating_from(self.termination.as_ref(), request)
    }
}

fn owner_reference_value(owner: klights_pod_api::PodOwnerReference) -> Value {
    let (api_version, kind, name, uid, controller, block_owner_deletion) = owner.into_parts();
    let mut value = Map::new();
    value.insert("apiVersion".to_string(), Value::String(api_version));
    value.insert("kind".to_string(), Value::String(kind));
    value.insert("name".to_string(), Value::String(name));
    value.insert("uid".to_string(), Value::String(uid));
    if let Some(controller) = controller {
        value.insert("controller".to_string(), Value::Bool(controller));
    }
    if let Some(block_owner_deletion) = block_owner_deletion {
        value.insert(
            "blockOwnerDeletion".to_string(),
            Value::Bool(block_owner_deletion),
        );
    }
    Value::Object(value)
}

fn merge_metadata_patch(current: &Resource, patch: &Value) -> Result<Value, PodRepositoryError> {
    let mut body = Arc::unwrap_or_clone(current.data.clone());
    let body_metadata = body
        .as_object_mut()
        .ok_or_else(|| PodRepositoryError::internal("Pod body is not a JSON object"))?
        .entry("metadata")
        .or_insert_with(|| serde_json::json!({}));
    let body_metadata = body_metadata
        .as_object_mut()
        .ok_or_else(|| PodRepositoryError::internal("Pod metadata is not a JSON object"))?;
    let patch_metadata = patch
        .get("metadata")
        .and_then(Value::as_object)
        .ok_or_else(|| PodRepositoryError::internal("Pod metadata patch is invalid"))?;
    for (key, value) in patch_metadata {
        if matches!(key.as_str(), "labels" | "annotations") {
            let destination = body_metadata
                .entry(key.clone())
                .or_insert_with(|| serde_json::json!({}));
            if key == "labels" && !destination.is_object() {
                *destination = serde_json::json!({});
            }
            let destination = destination.as_object_mut().ok_or_else(|| {
                PodRepositoryError::internal(format!("Pod metadata {key} is not an object"))
            })?;
            for (nested_key, nested_value) in value.as_object().ok_or_else(|| {
                PodRepositoryError::internal(format!("Pod metadata patch {key} is not an object"))
            })? {
                destination.insert(nested_key.clone(), nested_value.clone());
            }
        } else {
            body_metadata.insert(key.clone(), value.clone());
        }
    }
    Ok(body)
}

fn synthetic_resource(mut current: Resource, body: Value) -> Resource {
    current.uid = Resource::uid_from_data(&body);
    current.data = Arc::new(body);
    current
}

/// Transport-neutral UID-qualified route request delivered to the root actor
/// adapter. Root alone translates this value into private lifecycle messages.
#[derive(Clone, Debug)]
pub struct PodLifecycleRouteRequest {
    identity: klights_types::PodIdentity,
    resource_version: i64,
    pod: Resource,
}

impl PodLifecycleRouteRequest {
    pub fn identity(&self) -> &klights_types::PodIdentity {
        &self.identity
    }

    pub fn resource_version(&self) -> i64 {
        self.resource_version
    }

    pub fn pod(&self) -> &Resource {
        &self.pod
    }

    pub fn into_parts(self) -> (klights_types::PodIdentity, i64, Resource) {
        (self.identity, self.resource_version, self.pod)
    }
}

pub trait PodLifecycleRouteSink: Send + Sync {
    fn route_pod_lifecycle(&self, request: PodLifecycleRouteRequest) -> PodLifecycleFuture<'_>;
}

pub struct PodLifecycleWakeupService {
    route: Arc<dyn PodLifecycleRouteSink>,
}

impl PodLifecycleWakeupService {
    pub fn new(route: Arc<dyn PodLifecycleRouteSink>) -> Self {
        Self { route }
    }
}

impl PodLifecycleWakeup for PodLifecycleWakeupService {
    fn wake_pod_lifecycle(&self, request: PodLifecycleWakeupRequest) -> PodLifecycleFuture<'_> {
        let (identity, resource_version, pod) = request.into_parts();
        self.route.route_pod_lifecycle(PodLifecycleRouteRequest {
            identity,
            resource_version,
            pod,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn pod(uid: &str, resource_version: i64) -> Resource {
        Resource::try_from_data(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": uid,
                "resourceVersion": resource_version.to_string(),
                "labels": {"existing": "kept"},
                "annotations": {"existing": "kept"}
            },
            "spec": {"containers": [{"name": "app", "image": "busybox"}]},
            "status": {"phase": "Running", "podIP": "10.0.0.2"}
        })))
        .unwrap()
    }

    struct FixedQuery {
        current: Resource,
        calls: Mutex<Vec<PodGetRequest>>,
    }

    impl PodQuery for FixedQuery {
        fn get_pod(&self, request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
            self.calls.lock().unwrap().push(request);
            let current = self.current.clone();
            Box::pin(async move { Ok(Some(current)) })
        }

        fn list_pods(&self, _request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
            Box::pin(async { PodListResult::try_new(Vec::new(), 0, None, None) })
        }

        fn list_pods_by_owner_uid(
            &self,
            _request: PodOwnerListRequest,
        ) -> PodRepositoryFuture<'_, Vec<Resource>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    #[derive(Default)]
    struct RecordingPersistence {
        patches: Mutex<Vec<PodMetadataPatchRequest>>,
        persisted: AtomicBool,
    }

    impl PodPersistence for RecordingPersistence {
        fn create_pod(
            &self,
            _request: klights_pod_api::PodPersistenceCreateRequest,
        ) -> PodRepositoryFuture<'_, Resource> {
            Box::pin(async { Err(PodRepositoryError::internal("unexpected create")) })
        }

        fn replace_pod(
            &self,
            _request: klights_pod_api::PodPersistenceReplaceRequest,
        ) -> PodRepositoryFuture<'_, Resource> {
            Box::pin(async { Err(PodRepositoryError::internal("unexpected replace")) })
        }

        fn replace_pod_including_status(
            &self,
            _request: klights_pod_api::PodPersistenceReplaceRequest,
        ) -> PodRepositoryFuture<'_, Resource> {
            Box::pin(async { Err(PodRepositoryError::internal("unexpected replace")) })
        }

        fn patch_pod_metadata(
            &self,
            request: PodMetadataPatchRequest,
        ) -> PodRepositoryFuture<'_, Resource> {
            self.patches.lock().unwrap().push(request.clone());
            self.persisted.store(true, Ordering::Release);
            let updated = pod(&request.expected_uid, request.expected_resource_version + 1);
            Box::pin(async move { Ok(updated) })
        }
    }

    struct RecordingEffects {
        calls: Mutex<Vec<&'static str>>,
        persisted: Arc<RecordingPersistence>,
        observed_after_persistence: AtomicBool,
    }

    impl PodMutationReconcileSink for RecordingEffects {
        fn reconcile_pod_mutation(
            &self,
            request: PodMutationReconcileRequest,
        ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
            let kind = match request {
                PodMutationReconcileRequest::ServicesAfterUpdate { .. } => "services",
                PodMutationReconcileRequest::RunHooks { .. } => "hooks",
                PodMutationReconcileRequest::ServicesAfterDelete { .. } => "delete",
                PodMutationReconcileRequest::StatusChanged { .. } => "status",
                PodMutationReconcileRequest::EnqueueJobOwner { .. } => "job",
            };
            self.calls.lock().unwrap().push(kind);
            self.observed_after_persistence.store(
                self.persisted.persisted.load(Ordering::Acquire),
                Ordering::Release,
            );
            Box::pin(async { Ok(()) })
        }
    }

    struct FixedClock;
    impl crate::runtime_clock::RuntimeClock for FixedClock {
        fn now_ms(&self) -> i64 {
            1234
        }
    }

    fn fixture(
        remote_delivery_required: bool,
    ) -> (
        PodMetadataService,
        Arc<FixedQuery>,
        Arc<RecordingPersistence>,
        Arc<RecordingEffects>,
    ) {
        let query = Arc::new(FixedQuery {
            current: pod("uid-live", 41),
            calls: Mutex::new(Vec::new()),
        });
        let persistence = Arc::new(RecordingPersistence::default());
        let effects = Arc::new(RecordingEffects {
            calls: Mutex::new(Vec::new()),
            persisted: persistence.clone(),
            observed_after_persistence: AtomicBool::new(false),
        });
        let service = PodMetadataService::new(PodMetadataDependencies {
            persistence: persistence.clone(),
            outbox: None,
            remote_delivery_required,
            mutation_reconcile: effects.clone(),
            wall_clock: Arc::new(FixedClock),
        });
        (service, query, persistence, effects)
    }

    fn by_name() -> PodMutationTarget {
        PodMutationTarget::try_by_name("default", "web").unwrap()
    }

    fn by_uid(uid: &str) -> PodMutationTarget {
        PodMutationTarget::try_by_identity(klights_types::PodIdentity::new("default", "web", uid))
            .unwrap()
    }

    fn label_request(target: PodMutationTarget) -> PodUpdateRequest {
        PodUpdateRequest::merge_labels(
            target,
            vec![klights_pod_api::PodLabel::try_new("app", "web").unwrap()],
        )
    }

    fn owner_request(target: PodMutationTarget) -> PodUpdateRequest {
        PodUpdateRequest::replace_owner_references(
            target,
            vec![
                klights_pod_api::PodOwnerReference::try_new(
                    "apps/v1",
                    "ReplicaSet",
                    "web-rs",
                    "uid-rs",
                    Some(true),
                    Some(true),
                )
                .unwrap(),
            ],
        )
    }

    fn sandbox_request(target: PodMutationTarget) -> PodUpdateRequest {
        PodUpdateRequest::try_record_sandbox_id(target, "sandbox-1").unwrap()
    }

    async fn assert_patch(request: PodUpdateRequest, pointer: &str, expected: Value) {
        let (service, query, persistence, _) = fixture(false);
        service
            .update_pod_from(query.as_ref(), request)
            .await
            .unwrap();
        let patches = persistence.patches.lock().unwrap();
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0].patch.pointer(pointer), Some(&expected));
    }

    #[tokio::test]
    async fn merge_labels_routes_name_only() {
        assert_patch(
            label_request(by_name()),
            "/metadata/labels/app",
            Value::String("web".into()),
        )
        .await;
    }

    #[tokio::test]
    async fn merge_labels_routes_uid_qualified() {
        assert_patch(
            label_request(by_uid("uid-live")),
            "/metadata/labels/app",
            Value::String("web".into()),
        )
        .await;
    }

    #[tokio::test]
    async fn replace_owner_references_routes_name_only() {
        assert_patch(
            owner_request(by_name()),
            "/metadata/ownerReferences/0/uid",
            Value::String("uid-rs".into()),
        )
        .await;
    }

    #[tokio::test]
    async fn replace_owner_references_routes_uid_qualified() {
        assert_patch(
            owner_request(by_uid("uid-live")),
            "/metadata/ownerReferences/0/uid",
            Value::String("uid-rs".into()),
        )
        .await;
    }

    #[tokio::test]
    async fn record_sandbox_id_routes_name_only() {
        assert_patch(
            sandbox_request(by_name()),
            "/metadata/annotations/klights.dev~1sandbox-id",
            Value::String("sandbox-1".into()),
        )
        .await;
    }

    #[tokio::test]
    async fn record_sandbox_id_routes_uid_qualified() {
        assert_patch(
            sandbox_request(by_uid("uid-live")),
            "/metadata/annotations/klights.dev~1sandbox-id",
            Value::String("sandbox-1".into()),
        )
        .await;
    }

    #[tokio::test]
    async fn metadata_update_rejects_same_name_uid_replacement() {
        let (service, query, persistence, _) = fixture(false);
        let error = service
            .update_pod_from(query.as_ref(), label_request(by_uid("uid-old")))
            .await
            .unwrap_err();
        assert!(
            matches!(error, PodRepositoryError::UidMismatch { expected, actual } if expected == "uid-old" && actual == "uid-live")
        );
        assert!(persistence.patches.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn metadata_update_preserves_resource_version_cas() {
        let (service, query, persistence, _) = fixture(false);
        service
            .update_pod_from(query.as_ref(), label_request(by_uid("uid-live")))
            .await
            .unwrap();
        let patches = persistence.patches.lock().unwrap();
        assert_eq!(patches[0].expected_uid, "uid-live");
        assert_eq!(patches[0].expected_resource_version, 41);
    }

    #[tokio::test]
    async fn metadata_update_fails_closed_without_worker_outbox() {
        let (service, query, persistence, _) = fixture(true);
        let error = service
            .update_pod_from(query.as_ref(), label_request(by_uid("uid-live")))
            .await
            .unwrap_err();
        assert!(matches!(error, PodRepositoryError::Unavailable { .. }));
        assert!(persistence.patches.lock().unwrap().is_empty());
    }

    #[derive(Default)]
    struct RecordingOutbox {
        commands: Mutex<Vec<klights_leader_api::NodeOutboxCommand>>,
    }

    impl klights_leader_api::NodeOutbox for RecordingOutbox {
        fn enqueue(
            &self,
            command: klights_leader_api::NodeOutboxCommand,
        ) -> klights_leader_api::NodeOutboxFuture<'_, klights_leader_api::NodeOutboxRoute> {
            self.commands.lock().unwrap().push(command);
            Box::pin(async { Ok(klights_leader_api::NodeOutboxRoute::Enqueued) })
        }

        fn next_status_stamp(&self) -> klights_leader_api::NodeOutboxFuture<'_, i64> {
            Box::pin(async { Ok(1) })
        }

        fn record_pod_status_checkpoint<'a>(
            &'a self,
            _checkpoint: &'a Resource,
            _updated_ms: i64,
        ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn merge_pod_status_checkpoint(
            &self,
            pod: Resource,
        ) -> klights_leader_api::NodeOutboxFuture<'_, Resource> {
            Box::pin(async move { Ok(pod) })
        }

        fn delete_pod_status_checkpoint<'a>(
            &'a self,
            _pod_uid: &'a str,
        ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn record_runtime_observation_checkpoint<'a>(
            &'a self,
            _pod_uid: &'a str,
            _container_ids: Vec<String>,
            _generation: u64,
            _updated_ms: i64,
        ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn get_runtime_observation_checkpoint<'a>(
            &'a self,
            _pod_uid: &'a str,
        ) -> klights_leader_api::NodeOutboxFuture<
            'a,
            Option<klights_leader_api::NodeRuntimeObservationCheckpoint>,
        > {
            Box::pin(async { Ok(None) })
        }

        fn delete_runtime_observation_checkpoint<'a>(
            &'a self,
            _pod_uid: &'a str,
        ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn metadata_update_persists_locally_when_outbox_exists_but_remote_delivery_is_disabled() {
        let (_, query, persistence, effects) = fixture(false);
        let outbox = Arc::new(RecordingOutbox::default());
        let service = PodMetadataService::new(PodMetadataDependencies {
            persistence: persistence.clone(),
            outbox: Some(outbox.clone()),
            remote_delivery_required: false,
            mutation_reconcile: effects.clone(),
            wall_clock: Arc::new(FixedClock),
        });

        let updated = service
            .update_pod_from(query.as_ref(), owner_request(by_uid("uid-live")))
            .await
            .unwrap();

        let patches = persistence.patches.lock().unwrap();
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0].expected_uid, "uid-live");
        assert_eq!(patches[0].expected_resource_version, 41);
        assert_eq!(updated.resource_version, 42);
        assert!(outbox.commands.lock().unwrap().is_empty());
        assert!(effects.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn metadata_update_routes_worker_outbox_with_exact_cas() {
        let (_, query, persistence, effects) = fixture(false);
        let outbox = Arc::new(RecordingOutbox::default());
        let service = PodMetadataService::new(PodMetadataDependencies {
            persistence: persistence.clone(),
            outbox: Some(outbox.clone()),
            remote_delivery_required: true,
            mutation_reconcile: effects.clone(),
            wall_clock: Arc::new(FixedClock),
        });

        let updated = service
            .update_pod_from(query.as_ref(), label_request(by_uid("uid-live")))
            .await
            .unwrap();

        let commands = outbox.commands.lock().unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].pod_uid, "uid-live");
        assert_eq!(commands[0].now_ms, 1234);
        match &commands[0].command {
            StorageCommand::PatchResource {
                api_version,
                kind,
                namespace,
                name,
                preconditions,
                strict_resource_version,
                ..
            } => {
                assert_eq!(api_version, "v1");
                assert_eq!(kind, "Pod");
                assert_eq!(namespace.as_deref(), Some("default"));
                assert_eq!(name, "web");
                assert_eq!(preconditions.uid.as_deref(), Some("uid-live"));
                assert_eq!(preconditions.resource_version, Some(41));
                assert!(*strict_resource_version);
            }
            other => panic!("expected Pod metadata patch command, got {other:?}"),
        }
        assert_eq!(updated.resource_version, 41);
        assert_eq!(
            updated
                .data
                .pointer("/metadata/labels/existing")
                .and_then(Value::as_str),
            Some("kept")
        );
        assert_eq!(
            updated
                .data
                .pointer("/status/phase")
                .and_then(Value::as_str),
            Some("Running")
        );
        assert!(persistence.patches.lock().unwrap().is_empty());
        assert!(effects.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn label_update_enqueues_services_only_after_local_persistence() {
        let (service, query, _, effects) = fixture(false);
        service
            .update_pod_from(query.as_ref(), label_request(by_name()))
            .await
            .unwrap();
        assert_eq!(*effects.calls.lock().unwrap(), vec!["services"]);
        assert!(effects.observed_after_persistence.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn owner_reference_update_emits_no_service_or_pdb_feedback() {
        let (service, query, _, effects) = fixture(false);
        service
            .update_pod_from(query.as_ref(), owner_request(by_name()))
            .await
            .unwrap();
        assert!(effects.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn sandbox_id_update_emits_no_service_or_pdb_feedback() {
        let (service, query, _, effects) = fixture(false);
        service
            .update_pod_from(query.as_ref(), sandbox_request(by_name()))
            .await
            .unwrap();
        assert!(effects.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn sandbox_id_update_preserves_unrelated_metadata_and_status() {
        let (service, query, _, _) = fixture(false);
        let updated = service
            .update_pod_from(query.as_ref(), sandbox_request(by_name()))
            .await
            .unwrap();
        assert_eq!(
            updated
                .data
                .pointer("/metadata/labels/existing")
                .and_then(Value::as_str),
            Some("kept")
        );
        assert_eq!(
            updated
                .data
                .pointer("/status/phase")
                .and_then(Value::as_str),
            Some("Running")
        );
    }
}

#[cfg(test)]
mod network_tests {
    use super::*;
    use klights_network_api::{
        PodNetworkAssignmentEventError, PodNetworkAssignmentSubscription,
        PodNetworkAssignmentWaitFuture,
    };
    use klights_node_store::{
        CacheNetworkFuture, PodNetworkAllocationRequest, PodNetworkAssignmentSnapshot,
        PodNetworkEndpoint, PodUidKey,
    };
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct TestSubscription {
        receiver: tokio::sync::watch::Receiver<u64>,
    }

    impl PodNetworkAssignmentSubscription for TestSubscription {
        fn wait(&mut self) -> PodNetworkAssignmentWaitFuture<'_> {
            Box::pin(async move {
                self.receiver
                    .changed()
                    .await
                    .map_err(|_| PodNetworkAssignmentEventError::closed())
            })
        }
    }

    struct TestWaiter {
        sender: tokio::sync::watch::Sender<u64>,
        subscribed: AtomicBool,
        fail_subscribe: bool,
    }

    impl TestWaiter {
        fn new() -> Arc<Self> {
            let (sender, _) = tokio::sync::watch::channel(0);
            Arc::new(Self {
                sender,
                subscribed: AtomicBool::new(false),
                fail_subscribe: false,
            })
        }

        fn publish(&self) {
            let next = (*self.sender.borrow()).wrapping_add(1);
            self.sender.send_replace(next);
        }
    }

    impl PodNetworkAssignmentWaiter for TestWaiter {
        fn subscribe(
            &self,
            _key: PodNetworkAssignmentKey,
        ) -> Result<Box<dyn PodNetworkAssignmentSubscription>, PodNetworkAssignmentEventError>
        {
            if self.fail_subscribe {
                return Err(PodNetworkAssignmentEventError::closed());
            }
            self.subscribed.store(true, Ordering::Release);
            Ok(Box::new(TestSubscription {
                receiver: self.sender.subscribe(),
            }))
        }
    }

    struct TestCache {
        assignment: Mutex<Option<PodNetworkEndpoint>>,
        pod: Mutex<Option<PodNetworkEndpoint>>,
        assignment_reads: AtomicUsize,
        publish_on_first_assignment_read: Option<Arc<TestWaiter>>,
        expose_assignment_after_first_read: bool,
        fail: bool,
    }

    impl TestCache {
        fn empty() -> Arc<Self> {
            Arc::new(Self {
                assignment: Mutex::new(None),
                pod: Mutex::new(None),
                assignment_reads: AtomicUsize::new(0),
                publish_on_first_assignment_read: None,
                expose_assignment_after_first_read: false,
                fail: false,
            })
        }

        fn endpoint(ip: &str) -> PodNetworkEndpoint {
            PodNetworkEndpoint::try_new(ip, "veth0", "/run/netns/test").unwrap()
        }
    }

    impl PodNetworkCache for TestCache {
        fn get_network_for_uid(
            &self,
            _pod_uid: PodUidKey,
        ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
            Box::pin(async { Ok(None) })
        }

        fn get_network_for_pod(
            &self,
            _pod: klights_types::PodIdentity,
        ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
            let result = self.pod.lock().unwrap().clone();
            Box::pin(async move { Ok(result) })
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
            _pod: klights_types::PodIdentity,
        ) -> CacheNetworkFuture<'_, Option<PodNetworkEndpoint>> {
            if self.fail {
                return Box::pin(async {
                    Err(klights_node_store::CacheNetworkError::persistence_failed(
                        "cache unavailable",
                    ))
                });
            }
            let read = self.assignment_reads.fetch_add(1, Ordering::SeqCst);
            if read == 0
                && let Some(waiter) = &self.publish_on_first_assignment_read
            {
                waiter.publish();
            }
            let result = if self.expose_assignment_after_first_read && read == 0 {
                None
            } else {
                self.assignment.lock().unwrap().clone()
            };
            Box::pin(async move { Ok(result) })
        }

        fn delete_network_for_sandbox(
            &self,
            _sandbox_id: SandboxKey,
        ) -> CacheNetworkFuture<'_, ()> {
            Box::pin(async { panic!("read-only network query") })
        }

        fn delete_network_if_matches(
            &self,
            _request: PodNetworkAllocationRequest,
        ) -> CacheNetworkFuture<'_, bool> {
            Box::pin(async { panic!("read-only network query") })
        }

        fn list_network_assignments(
            &self,
        ) -> CacheNetworkFuture<'_, Vec<PodNetworkAssignmentSnapshot>> {
            Box::pin(async { panic!("read-only network query") })
        }
    }

    fn request(host_network: bool) -> PodNetworkAssignmentRequest {
        PodNetworkAssignmentRequest::try_new(
            "sandbox-1",
            klights_types::PodIdentity::new("default", "web", "uid-live"),
            host_network,
        )
        .unwrap()
    }

    fn service(cache: Arc<TestCache>, waiter: Arc<TestWaiter>) -> PodNetworkService {
        PodNetworkService::new(
            cache,
            Arc::new(klights_supervisor::TaskSupervisor::new(
                klights_supervisor::TaskCategoryConfig::default(),
            )),
            waiter,
            crate::context::HostIpState::default(),
        )
    }

    #[tokio::test]
    async fn read_pod_network_assignment_returns_assigned_ip() {
        let cache = TestCache::empty();
        *cache.assignment.lock().unwrap() = Some(TestCache::endpoint("10.42.0.2"));
        let result = service(cache, TestWaiter::new())
            .read_pod_network_assignment(request(false))
            .await
            .unwrap();
        assert_eq!(result.pod_ip, "10.42.0.2");
    }

    #[tokio::test]
    async fn read_pod_network_assignment_falls_back_to_pod_identity() {
        let cache = TestCache::empty();
        *cache.pod.lock().unwrap() = Some(TestCache::endpoint("10.42.0.43"));
        let result = service(cache, TestWaiter::new())
            .read_pod_network_assignment(request(false))
            .await
            .unwrap();
        assert_eq!(result.pod_ip, "10.42.0.43");
    }

    #[tokio::test]
    async fn read_pod_network_assignment_host_network_returns_host_ip_twice_without_db() {
        let cache = Arc::new(TestCache {
            fail: true,
            ..Arc::try_unwrap(TestCache::empty()).ok().unwrap()
        });
        let result = service(cache, TestWaiter::new())
            .read_pod_network_assignment(request(true))
            .await
            .unwrap();
        assert_eq!(result.pod_ip, result.host_ip);
    }

    #[tokio::test]
    async fn read_pod_network_assignment_retries_then_succeeds() {
        let cache = TestCache::empty();
        let waiter = TestWaiter::new();
        let reader = service(cache.clone(), waiter.clone());
        let task =
            tokio::spawn(async move { reader.read_pod_network_assignment(request(false)).await });
        while !waiter.subscribed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        *cache.assignment.lock().unwrap() = Some(TestCache::endpoint("10.42.0.44"));
        waiter.publish();
        assert_eq!(task.await.unwrap().unwrap().pod_ip, "10.42.0.44");
    }

    #[tokio::test]
    async fn read_pod_network_assignment_retains_publish_inside_first_lookup_gap() {
        let waiter = TestWaiter::new();
        let cache = Arc::new(TestCache {
            assignment: Mutex::new(Some(TestCache::endpoint("10.42.0.101"))),
            pod: Mutex::new(None),
            assignment_reads: AtomicUsize::new(0),
            publish_on_first_assignment_read: Some(waiter.clone()),
            expose_assignment_after_first_read: true,
            fail: false,
        });
        let result = service(cache.clone(), waiter)
            .read_pod_network_assignment(request(false))
            .await
            .unwrap();
        assert_eq!(result.pod_ip, "10.42.0.101");
        assert_eq!(cache.assignment_reads.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn read_pod_network_assignment_tolerates_cni_db_backlog() {
        let cache = TestCache::empty();
        let waiter = TestWaiter::new();
        let reader = service(cache.clone(), waiter.clone());
        let task =
            tokio::spawn(async move { reader.read_pod_network_assignment(request(false)).await });
        while !waiter.subscribed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        *cache.assignment.lock().unwrap() = Some(TestCache::endpoint("10.42.0.45"));
        waiter.publish();
        assert_eq!(task.await.unwrap().unwrap().pod_ip, "10.42.0.45");
    }

    #[tokio::test]
    async fn read_pod_network_assignment_waits_for_assignment_notification() {
        let cache = TestCache::empty();
        let waiter = TestWaiter::new();
        let reader = service(cache.clone(), waiter.clone());
        let task =
            tokio::spawn(async move { reader.read_pod_network_assignment(request(false)).await });
        while !waiter.subscribed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        assert!(!task.is_finished());
        *cache.assignment.lock().unwrap() = Some(TestCache::endpoint("10.42.0.46"));
        waiter.publish();
        assert!(task.await.unwrap().is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn read_pod_network_assignment_exhausts_retries_returns_error() {
        let reader = service(TestCache::empty(), TestWaiter::new());
        let future = reader.read_pod_network_assignment(request(false));
        tokio::pin!(future);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(31)).await;
        let error = future.await.unwrap_err();
        assert!(matches!(error, PodNetworkAssignmentError::TimedOut(_)));
    }

    #[test]
    fn network_assignment_request_rejects_invalid_identity() {
        let error = PodNetworkAssignmentRequest::try_new(
            "sandbox-1",
            klights_types::PodIdentity::new("", "web", "uid-live"),
            false,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            PodNetworkAssignmentError::InvalidRequest(_)
        ));
    }

    #[tokio::test]
    async fn read_pod_network_assignment_reports_cache_unavailable() {
        let cache = Arc::new(TestCache {
            fail: true,
            ..Arc::try_unwrap(TestCache::empty()).ok().unwrap()
        });
        let error = service(cache, TestWaiter::new())
            .read_pod_network_assignment(request(false))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            PodNetworkAssignmentError::CacheUnavailable(_)
        ));
    }

    #[tokio::test]
    async fn read_pod_network_assignment_reports_closed_wait_subscription() {
        let (sender, _) = tokio::sync::watch::channel(0);
        let waiter = Arc::new(TestWaiter {
            sender,
            subscribed: AtomicBool::new(false),
            fail_subscribe: true,
        });
        let error = service(TestCache::empty(), waiter)
            .read_pod_network_assignment(request(false))
            .await
            .unwrap_err();
        assert!(matches!(error, PodNetworkAssignmentError::WaitClosed(_)));
    }

    #[tokio::test]
    async fn read_pod_network_assignment_reports_missing_row_after_notification() {
        let waiter = TestWaiter::new();
        let reader = service(TestCache::empty(), waiter.clone());
        let task =
            tokio::spawn(async move { reader.read_pod_network_assignment(request(false)).await });
        while !waiter.subscribed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        waiter.publish();
        let error = task.await.unwrap().unwrap_err();
        assert!(matches!(
            error,
            PodNetworkAssignmentError::MissingAssignment(_)
        ));
    }
}
