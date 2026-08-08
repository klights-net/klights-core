//! Focused, transport-neutral access contracts for Pod operations.
//!
//! Ordinary access deliberately does not grant datastore removal or
//! lifecycle-actor control. Bound-Pod row removal and leader-side unscheduled
//! row removal are distinct UID-qualified capabilities whose real
//! implementations are kept private by root composition.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

use klights_cluster_core::Resource;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodLifecycleActorDiagnostic {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub state: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodLifecycleTraceDiagnostic {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub event: String,
    pub resource_version: Option<i64>,
    pub sandbox_id: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PodLifecycleDiagnostics {
    pub actor_states: Vec<PodLifecycleActorDiagnostic>,
    pub recent_trace: Vec<PodLifecycleTraceDiagnostic>,
}

pub type PodLifecycleDiagnosticsFuture<'a> =
    Pin<Box<dyn Future<Output = PodLifecycleDiagnostics> + Send + 'a>>;

pub trait PodLifecycleDiagnosticsQuery: Send + Sync {
    fn pod_lifecycle_diagnostics(&self) -> PodLifecycleDiagnosticsFuture<'_>;
}

pub type PodStartRetryDiagnosticsFuture<'a> =
    Pin<Box<dyn Future<Output = Vec<(String, String)>> + Send + 'a>>;

pub trait PodStartRetryDiagnostics: Send + Sync {
    fn pending_pod_start_retries(&self) -> PodStartRetryDiagnosticsFuture<'_>;
}
use klights_types::PodIdentity;

pub type PodRepositoryFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, PodRepositoryError>> + Send + 'a>>;
pub type PodLifecycleFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), PodRoutingError>> + Send + 'a>>;
pub type BoundPodFinalizationFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<BoundPodFinalizationOutcome, BoundPodFinalizationError>>
            + Send
            + 'a,
    >,
>;
pub type UnscheduledPodDeletionFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<UnscheduledPodDeletionOutcome, UnscheduledPodDeletionError>>
            + Send
            + 'a,
    >,
>;

/// UID- and observed-resourceVersion-qualified capability used only by the
/// leader deferred-delete worker for Pods that have never been bound.
///
/// The real adapter remains private to root composition. Implementations must
/// remove only a terminating, finalizer-free Pod whose `spec.nodeName` was
/// empty at `observed_resource_version`; any intervening write defers to the
/// lifecycle actor path.
pub trait UnscheduledPodDeletion: Send + Sync {
    fn delete_unscheduled_pod(
        &self,
        request: UnscheduledPodDeletionRequest,
    ) -> UnscheduledPodDeletionFuture<'_>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnscheduledPodDeletionRequest {
    identity: PodIdentity,
    observed_resource_version: i64,
}

impl UnscheduledPodDeletionRequest {
    pub fn try_new(
        identity: PodIdentity,
        observed_resource_version: i64,
    ) -> Result<Self, UnscheduledPodDeletionError> {
        validate_unscheduled_required("pod.identity.namespace", &identity.namespace)?;
        validate_unscheduled_required("pod.identity.name", &identity.name)?;
        validate_unscheduled_required("pod.identity.uid", &identity.uid)?;
        if observed_resource_version <= 0 {
            return Err(UnscheduledPodDeletionError::invalid_request(
                "pod.observed_resource_version",
                "must be positive",
            ));
        }
        Ok(Self {
            identity,
            observed_resource_version,
        })
    }

    pub fn identity(&self) -> &PodIdentity {
        &self.identity
    }

    pub fn observed_resource_version(&self) -> i64 {
        self.observed_resource_version
    }

    pub fn into_parts(self) -> (PodIdentity, i64) {
        (self.identity, self.observed_resource_version)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnscheduledPodDeletionOutcome {
    /// The requested UID is gone, including when its row was already absent or
    /// the namespace/name slot now belongs to a replacement UID.
    Removed,
    /// The Pod is bound. Only lifecycle actor finalization may remove the
    /// surviving row.
    DeferToActor,
    /// The Pod still has finalizers and must remain until they clear.
    FinalizersPending,
    /// The row changed after the worker's eligibility observation. Retry from
    /// a fresh read before deciding whether the Pod is still unscheduled or
    /// must be handed to its lifecycle actor.
    Retry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnscheduledPodDeletionError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    Unavailable {
        message: String,
    },
}

impl UnscheduledPodDeletionError {
    pub fn invalid_request(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }
}

impl fmt::Display for UnscheduledPodDeletionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::Unavailable { message } => formatter.write_str(message),
        }
    }
}

impl Error for UnscheduledPodDeletionError {}

/// UID-qualified capability used only after the lifecycle actor has completed
/// runtime cleanup for a bound Pod.
///
/// The contract is public so the lifecycle finalizer can receive an opaque
/// trait object and tests can provide inert fakes. The real deleting adapter is
/// private to root composition and is never exposed through ordinary Pod ports.
pub trait BoundPodFinalization: Send + Sync {
    fn finalize_bound_pod(
        &self,
        request: BoundPodFinalizationRequest,
    ) -> BoundPodFinalizationFuture<'_>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundPodFinalizationRequest {
    identity: PodIdentity,
}

impl BoundPodFinalizationRequest {
    pub fn try_new(identity: PodIdentity) -> Result<Self, BoundPodFinalizationError> {
        validate_bound_required("pod.identity.namespace", &identity.namespace)?;
        validate_bound_required("pod.identity.name", &identity.name)?;
        validate_bound_required("pod.identity.uid", &identity.uid)?;
        Ok(Self { identity })
    }

    pub fn identity(&self) -> &PodIdentity {
        &self.identity
    }

    pub fn into_identity(self) -> PodIdentity {
        self.identity
    }
}

/// Root-adapter disposition needed by the existing lifecycle finalizer to
/// preserve its exact local post-delete maintenance behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BoundPodFinalizationOutcome {
    /// The matching local row was removed.
    Removed,
    /// The UID-qualified operation was accepted by the leader-facing path.
    Accepted,
    /// The namespace/name slot no longer contains the requested UID.
    IdentityChanged,
    /// A finalizer appeared or remained on the matching Pod.
    FinalizersPending,
    /// The matching row is not currently eligible or changed during the
    /// observed-resourceVersion delete CAS. The actor must retry from a fresh
    /// lifecycle observation.
    Retry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BoundPodFinalizationError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    Unavailable {
        message: String,
    },
}

impl BoundPodFinalizationError {
    pub fn invalid_request(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }
}

impl fmt::Display for BoundPodFinalizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::Unavailable { message } => formatter.write_str(message),
        }
    }
}

impl Error for BoundPodFinalizationError {}

pub trait PodQuery: Send + Sync {
    fn get_pod(&self, request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>>;

    fn list_pods(&self, request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult>;

    fn list_pods_by_owner_uid(
        &self,
        request: PodOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<Resource>>;
}

/// Persistence-only reads used by the root Pod repository facade.
///
/// This capability is Pod-shaped and cannot select a datastore, address an
/// arbitrary Kubernetes kind, subscribe to watches, or remove rows.
pub trait PodRepositoryReadPersistence: Send + Sync {
    fn get_persisted_pod(
        &self,
        request: PodRepositoryGetRequest,
    ) -> PodRepositoryFuture<'_, Option<Resource>>;

    fn list_persisted_pods(
        &self,
        request: PodRepositoryListRequest,
    ) -> PodRepositoryFuture<'_, PodListResult>;

    fn snapshot_persisted_pods(
        &self,
        request: PodSnapshotListRequest,
    ) -> PodRepositoryFuture<'_, PodSnapshotListOutcome>;

    fn list_persisted_pods_by_owner(
        &self,
        request: PodRepositoryOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<Resource>>;
}

/// Persistence-only writes used by the root Pod repository facade.
///
/// Hard deletion is intentionally absent. Bound and never-bound Pod deletion
/// use their separate opaque capabilities.
pub trait PodRepositoryWritePersistence: Send + Sync {
    fn create_persisted_pod(
        &self,
        request: PodRepositoryCreateRequest,
    ) -> PodRepositoryFuture<'_, Resource>;

    fn replace_persisted_pod(
        &self,
        request: PodRepositoryReplaceRequest,
    ) -> PodRepositoryFuture<'_, Resource>;

    fn patch_persisted_pod(
        &self,
        request: PodRepositoryPatchRequest,
    ) -> PodRepositoryFuture<'_, Option<Resource>>;

    fn write_persisted_pod_status(
        &self,
        request: PodRepositoryStatusRequest,
    ) -> PodRepositoryFuture<'_, Resource>;

    fn log_persisted_pod_status_noop(&self, request: PodRepositoryStatusNoop<'_>);
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodRepositoryGetRequest {
    pub namespace: String,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodRepositoryListRequest {
    pub namespace: Option<String>,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub limit: Option<i64>,
    pub continue_token: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodRepositoryOwnerListRequest {
    pub namespace: String,
    pub owner_uid: String,
}

#[derive(Clone, Debug)]
pub struct PodRepositoryCreateRequest {
    pub namespace: String,
    pub name: String,
    pub body: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct PodRepositoryReplaceRequest {
    pub namespace: String,
    pub name: String,
    pub body: serde_json::Value,
    pub preconditions: klights_cluster_core::ResourcePreconditions,
}

#[derive(Clone, Debug)]
pub struct PodRepositoryPatchRequest {
    pub namespace: String,
    pub name: String,
    pub patch_kind: klights_cluster_core::PatchKind,
    pub patch: serde_json::Value,
    pub preconditions: klights_cluster_core::ResourcePreconditions,
}

#[derive(Clone, Debug)]
pub struct PodRepositoryStatusRequest {
    pub namespace: String,
    pub name: String,
    pub status: serde_json::Value,
    pub preconditions: klights_cluster_core::ResourcePreconditions,
}

#[derive(Clone, Copy, Debug)]
pub struct PodRepositoryStatusNoop<'a> {
    pub namespace: &'a str,
    pub name: &'a str,
    pub resource: &'a Resource,
}

/// Focused persistence capability consumed by Kubernetes-native Pod
/// orchestration. It deliberately exposes only Pod create/replace operations;
/// datastore selection, watches, generic resources, and row deletion are not
/// reachable through this port.
pub trait PodPersistence: Send + Sync {
    fn create_pod(&self, request: PodPersistenceCreateRequest)
    -> PodRepositoryFuture<'_, Resource>;

    fn replace_pod(
        &self,
        request: PodPersistenceReplaceRequest,
    ) -> PodRepositoryFuture<'_, Resource>;

    fn replace_pod_including_status(
        &self,
        request: PodPersistenceReplaceRequest,
    ) -> PodRepositoryFuture<'_, Resource>;

    /// Apply a metadata-only merge patch under exact Pod UID/RV CAS.
    fn patch_pod_metadata(
        &self,
        request: PodMetadataPatchRequest,
    ) -> PodRepositoryFuture<'_, Resource>;
}

#[derive(Clone, Debug)]
pub struct PodPersistenceCreateRequest {
    pub namespace: String,
    pub name: String,
    pub body: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct PodPersistenceReplaceRequest {
    pub namespace: String,
    pub name: String,
    pub body: serde_json::Value,
    pub expected_resource_version: i64,
}

#[derive(Clone, Debug)]
pub struct PodMetadataPatchRequest {
    pub namespace: String,
    pub name: String,
    pub expected_uid: String,
    pub expected_resource_version: i64,
    pub patch: serde_json::Value,
}

/// Status-only persistence capability. Implementations must discard every
/// non-status field supplied by the caller.
pub trait PodStatusPersistence: Send + Sync {
    fn write_pod_status(&self, request: PodStatusWriteRequest)
    -> PodRepositoryFuture<'_, Resource>;
}

#[derive(Clone, Debug)]
pub struct PodStatusWriteRequest {
    pub namespace: String,
    pub name: String,
    pub status: serde_json::Value,
    pub expected_resource_version: Option<i64>,
}

/// API/controller intent for marking a Pod terminating and waking its
/// UID-qualified lifecycle actor. This port can never hard-delete a Pod row.
pub trait PodDeleteOrchestration: Send + Sync {
    fn preview_delete(
        &self,
        resource: &Resource,
        requested_grace_period_seconds: Option<i64>,
    ) -> Result<serde_json::Value, PodRepositoryError>;

    fn mark_and_queue_delete(
        &self,
        request: PodDeleteMarkRequest,
    ) -> PodRepositoryFuture<'_, PodDeleteMarkOutcome>;

    fn enqueue_actor_finalize_if_ready(
        &self,
        request: PodActorFinalizeRequest,
    ) -> PodRepositoryFuture<'_, ()>;

    fn enqueue_marked_retry(&self, request: PodMarkedRetryRequest) -> PodRepositoryFuture<'_, ()>;
}

#[derive(Clone, Debug)]
pub struct PodDeleteMarkRequest {
    pub namespace: String,
    pub name: String,
    pub requested_grace_period_seconds: Option<i64>,
    pub preconditions: klights_cluster_core::ResourcePreconditions,
    pub initial_resource: Resource,
}

#[derive(Clone, Debug)]
pub struct PodDeleteMarkOutcome {
    pub updated: Resource,
    pub previous: Resource,
    pub uid: String,
    pub changed: bool,
}

#[derive(Clone, Debug)]
pub struct PodActorFinalizeRequest {
    pub namespace: String,
    pub name: String,
    pub resource: Resource,
}

#[derive(Clone, Debug)]
pub struct PodMarkedRetryRequest {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub run_after: std::time::Duration,
    pub pod_data: serde_json::Value,
}

/// Pure Pod-spec validation whose concrete implementation currently lives
/// with kubelet volume semantics.
pub trait PodSpecValidation: Send + Sync {
    fn validate_volume_paths(&self, pod: &serde_json::Value) -> Result<(), PodRepositoryError>;
}

/// Focused effect for a control-plane-authored Kubernetes Event about a Pod.
/// Node-authored outbox routing is intentionally outside this capability.
pub trait PodControlPlaneEventSink: Send + Sync {
    fn emit_pod_event(&self, request: PodControlPlaneEventRequest) -> PodRepositoryFuture<'_, ()>;
}

#[derive(Clone, Debug)]
pub struct PodControlPlaneEventRequest {
    pub pod: std::sync::Arc<serde_json::Value>,
    pub reason: String,
    pub message: String,
    pub event_type: String,
    pub reporting_component: String,
    pub reporting_instance: String,
}

pub fn preserve_pod_status_from_current(current: &serde_json::Value, next: &mut serde_json::Value) {
    let Some(next_object) = next.as_object_mut() else {
        return;
    };
    match current.get("status") {
        Some(status) => {
            next_object.insert("status".to_string(), status.clone());
        }
        None => {
            next_object.remove("status");
        }
    }
}

pub type PodSchedulingFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, PodRepositoryError>> + Send + 'a>>;

/// Focused controller-owned Pod scheduling capability. Kubernetes-native API
/// code may request a bind pass through this contract without owning scheduler
/// placement, preemption, datastore, or kubelet implementation details.
pub trait PodScheduling: Send + Sync {
    fn schedule_all_unbound_pods(&self) -> PodSchedulingFuture<'_, ()>;

    fn schedule_pending_pod(
        &self,
        namespace: String,
        name: String,
    ) -> PodSchedulingFuture<'_, Option<Resource>>;
}

/// Pure scheduler placement engine consumed by the controller-owned Pod
/// scheduling service. Resource discovery and persistence remain outside this
/// port, so the concrete scheduler implementation cannot acquire datastore or
/// lifecycle authority.
pub trait PodPlacement: Send + Sync {
    fn place_pod(
        &self,
        request: PodPlacementRequest,
    ) -> Result<PodPlacementDecision, PodRepositoryError>;
}

#[derive(Clone, Debug)]
pub struct PodPlacementRequest {
    pub nodes: Vec<std::sync::Arc<serde_json::Value>>,
    pub incoming_pod: std::sync::Arc<serde_json::Value>,
    pub existing_pods_by_node: Vec<(String, Vec<std::sync::Arc<serde_json::Value>>)>,
    pub namespaces: Vec<std::sync::Arc<serde_json::Value>>,
    pub disruption_budgets: Vec<std::sync::Arc<serde_json::Value>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PodPlacementDecision {
    pub selected_node: Option<String>,
    pub unschedulable_message: Option<String>,
    pub preemption_victims: Vec<String>,
}

pub trait PodUpdate: Send + Sync {
    fn update_pod(&self, request: PodUpdateRequest) -> PodRepositoryFuture<'_, Resource>;
}

pub trait PodMarkTerminating: Send + Sync {
    fn mark_pod_terminating(
        &self,
        request: PodMarkTerminatingRequest,
    ) -> PodRepositoryFuture<'_, Resource>;
}

#[derive(Clone, Debug)]
pub struct PodApiCreateRequest {
    pub namespace: String,
    pub body: serde_json::Value,
    pub dry_run: bool,
}

#[derive(Clone, Debug)]
pub struct PodApiCreateResult {
    pub resource: Option<Resource>,
    pub body: serde_json::Value,
}

#[derive(Clone, Debug)]
pub enum PodApiWriteOutcome {
    Persisted(Resource),
    DryRun(serde_json::Value),
}

#[derive(Clone, Debug)]
pub enum PodApiDeleteOutcome {
    GracefulSet(Resource),
    DryRun(serde_json::Value),
}

#[derive(Clone, Debug)]
pub struct PodApiUpdateRequest {
    pub namespace: String,
    pub name: String,
    pub body: serde_json::Value,
    pub current: Resource,
    pub dry_run: bool,
}

#[derive(Clone, Debug)]
pub struct PodApiPatchRequest {
    pub namespace: String,
    pub name: String,
    pub patch: serde_json::Value,
    pub patch_kind: PodStatusPatchKind,
    pub dry_run: bool,
}

#[derive(Clone, Debug)]
pub struct PodApiDeleteRequest {
    pub namespace: String,
    pub name: String,
    pub options: PodDeleteOptions,
    pub dry_run: bool,
}

#[derive(Clone, Debug)]
pub struct PodApiDeleteCollectionRequest {
    pub namespace: String,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub dry_run: bool,
}

pub trait PodApiMutation: Send + Sync {
    fn create_pod(
        &self,
        request: PodApiCreateRequest,
    ) -> PodRepositoryFuture<'_, PodApiCreateResult>;

    fn update_pod(
        &self,
        request: PodApiUpdateRequest,
    ) -> PodRepositoryFuture<'_, PodApiWriteOutcome>;

    fn patch_pod(&self, request: PodApiPatchRequest)
    -> PodRepositoryFuture<'_, PodApiWriteOutcome>;

    fn delete_pod(
        &self,
        request: PodApiDeleteRequest,
    ) -> PodRepositoryFuture<'_, PodApiDeleteOutcome>;

    fn delete_collection_pods(
        &self,
        request: PodApiDeleteCollectionRequest,
    ) -> PodRepositoryFuture<'_, ()>;

    fn bind_pod(&self, request: PodBindingRequest) -> PodRepositoryFuture<'_, ()>;
}

/// Canonical Kubernetes Binding object plus its URL target and dry-run mode.
///
/// The complete JSON value is intentional here: Binding admission and
/// annotation propagation consume the Kubernetes object as a whole.
#[derive(Clone, Debug)]
pub struct PodBindingRequest {
    pub namespace: String,
    pub name: String,
    pub binding: serde_json::Value,
    pub dry_run: bool,
}

#[derive(Clone, Debug)]
pub struct PodSnapshotListRequest {
    pub list: PodListRequest,
    pub snapshot_resource_version: i64,
}

#[derive(Clone, Debug)]
pub enum PodSnapshotListOutcome {
    List(PodListResult),
    Current,
    Expired,
}

pub trait PodSnapshotQuery: Send + Sync {
    fn snapshot_pods(
        &self,
        request: PodSnapshotListRequest,
    ) -> PodRepositoryFuture<'_, PodSnapshotListOutcome>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodEvictionDeleteRequest {
    namespace: String,
    name: String,
    options: PodDeleteOptions,
    dry_run: bool,
}

impl PodEvictionDeleteRequest {
    pub fn try_new(
        namespace: impl Into<String>,
        name: impl Into<String>,
        options: PodDeleteOptions,
        dry_run: bool,
    ) -> Result<Self, PodRepositoryError> {
        let target = PodMutationTarget::try_by_name(namespace, name)?;
        Ok(Self {
            namespace: target.namespace,
            name: target.name,
            options,
            dry_run,
        })
    }

    pub fn into_parts(self) -> (String, String, PodDeleteOptions, bool) {
        (self.namespace, self.name, self.options, self.dry_run)
    }
}

#[derive(Clone, Debug)]
pub enum PodEvictionDeleteOutcome {
    Persisted(Resource),
    DryRun,
}

pub trait PodEvictionDelete: Send + Sync {
    fn delete_for_eviction(
        &self,
        request: PodEvictionDeleteRequest,
    ) -> PodRepositoryFuture<'_, PodEvictionDeleteOutcome>;
}

/// HTTP patch semantics for the Pod status subresource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodStatusPatchKind {
    JsonPatch,
    MergePatch,
    StrategicMerge,
    ApplyPatch,
}

impl PodStatusPatchKind {
    pub fn from_content_type(content_type: Option<&str>) -> Self {
        match content_type {
            Some("application/json-patch+json") => Self::JsonPatch,
            Some("application/strategic-merge-patch+json") => Self::StrategicMerge,
            Some("application/apply-patch+yaml") => Self::ApplyPatch,
            _ => Self::MergePatch,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PodStatusReplaceRequest {
    pub namespace: String,
    pub name: String,
    pub expected_uid: Option<String>,
    pub status: serde_json::Value,
    pub expected_resource_version: i64,
}

#[derive(Clone, Debug)]
pub struct PodStatusPatchRequest {
    pub namespace: String,
    pub name: String,
    pub patch: serde_json::Value,
    pub patch_kind: PodStatusPatchKind,
    /// Client-supplied `metadata.resourceVersion`, when present. `None` is an
    /// unconditional Kubernetes PATCH and must not turn an internal status
    /// writer's resourceVersion advance into a client-visible conflict.
    pub expected_resource_version: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct PodEphemeralContainersRequest {
    pub namespace: String,
    pub name: String,
    pub containers: Vec<serde_json::Value>,
    pub expected_resource_version: i64,
}

pub trait PodSubresourceMutation: Send + Sync {
    fn replace_status(&self, request: PodStatusReplaceRequest)
    -> PodRepositoryFuture<'_, Resource>;

    fn patch_status(&self, request: PodStatusPatchRequest) -> PodRepositoryFuture<'_, Resource>;

    fn update_ephemeral_containers(
        &self,
        request: PodEphemeralContainersRequest,
    ) -> PodRepositoryFuture<'_, Resource>;
}

/// Transport-neutral Kubernetes Pod deletion policy.
///
/// HTTP decoding owns the conversion from `metav1.DeleteOptions`; repository
/// contracts carry this value so kubelet code does not depend on API-server
/// request or error types.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PodDeleteOptions {
    propagation_policy: Option<String>,
    orphan_dependents: Option<bool>,
    grace_period_seconds: Option<i64>,
    preconditions: PodDeletePreconditions,
}

impl PodDeleteOptions {
    pub fn new(
        propagation_policy: Option<String>,
        orphan_dependents: Option<bool>,
        grace_period_seconds: Option<i64>,
        preconditions: PodDeletePreconditions,
    ) -> Self {
        Self {
            propagation_policy,
            orphan_dependents,
            grace_period_seconds,
            preconditions,
        }
    }

    pub fn with_uid_precondition(uid: impl Into<String>) -> Self {
        Self {
            preconditions: PodDeletePreconditions::new(Some(uid.into()), None),
            ..Self::default()
        }
    }

    pub fn propagation_policy(&self) -> Option<&str> {
        self.propagation_policy.as_deref()
    }

    pub fn orphan_dependents(&self) -> Option<bool> {
        self.orphan_dependents
    }

    pub fn grace_period_seconds(&self) -> Option<i64> {
        self.grace_period_seconds
    }

    pub fn preconditions(&self) -> &PodDeletePreconditions {
        &self.preconditions
    }

    pub fn into_parts(
        self,
    ) -> (
        Option<String>,
        Option<bool>,
        Option<i64>,
        PodDeletePreconditions,
    ) {
        (
            self.propagation_policy,
            self.orphan_dependents,
            self.grace_period_seconds,
            self.preconditions,
        )
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PodDeletePreconditions {
    uid: Option<String>,
    resource_version: Option<String>,
}

impl PodDeletePreconditions {
    pub fn new(uid: Option<String>, resource_version: Option<String>) -> Self {
        Self {
            uid,
            resource_version,
        }
    }

    pub fn uid(&self) -> Option<&str> {
        self.uid.as_deref()
    }

    pub fn resource_version(&self) -> Option<&str> {
        self.resource_version.as_deref()
    }

    pub fn into_parts(self) -> (Option<String>, Option<String>) {
        (self.uid, self.resource_version)
    }
}

pub trait PodLifecycleWakeup: Send + Sync {
    fn wake_pod_lifecycle(&self, request: PodLifecycleWakeupRequest) -> PodLifecycleFuture<'_>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodGetRequest {
    namespace: String,
    name: String,
    uid: Option<String>,
}

impl PodGetRequest {
    pub fn try_by_name(
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, PodRepositoryError> {
        let namespace = namespace.into();
        let name = name.into();
        validate_required("pod.namespace", &namespace)?;
        validate_required("pod.name", &name)?;
        Ok(Self {
            namespace,
            name,
            uid: None,
        })
    }

    pub fn try_by_identity(identity: PodIdentity) -> Result<Self, PodRepositoryError> {
        validate_required("pod.namespace", &identity.namespace)?;
        validate_required("pod.name", &identity.name)?;
        validate_required("pod.uid", &identity.uid)?;
        Ok(Self {
            namespace: identity.namespace,
            name: identity.name,
            uid: Some(identity.uid),
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn uid(&self) -> Option<&str> {
        self.uid.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodListRequest {
    namespace: Option<String>,
    label_selector: Option<String>,
    field_selector: Option<String>,
    limit: Option<i64>,
    continue_token: Option<String>,
}

impl PodListRequest {
    pub fn try_new(
        namespace: Option<String>,
        label_selector: Option<String>,
        field_selector: Option<String>,
        limit: Option<i64>,
        continue_token: Option<String>,
    ) -> Result<Self, PodRepositoryError> {
        if let Some(namespace) = namespace.as_deref() {
            validate_required("list.namespace", namespace)?;
        }
        if limit.is_some_and(|limit| limit < 0) {
            return Err(PodRepositoryError::invalid_request(
                "list.limit",
                "must be non-negative",
            ));
        }
        Ok(Self {
            namespace,
            label_selector,
            field_selector,
            limit,
            continue_token,
        })
    }

    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub fn label_selector(&self) -> Option<&str> {
        self.label_selector.as_deref()
    }

    pub fn field_selector(&self) -> Option<&str> {
        self.field_selector.as_deref()
    }

    pub fn limit(&self) -> Option<i64> {
        self.limit
    }

    pub fn continue_token(&self) -> Option<&str> {
        self.continue_token.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodOwnerListRequest {
    namespace: String,
    owner_uid: String,
}

impl PodOwnerListRequest {
    pub fn try_new(
        namespace: impl Into<String>,
        owner_uid: impl Into<String>,
    ) -> Result<Self, PodRepositoryError> {
        let namespace = namespace.into();
        let owner_uid = owner_uid.into();
        validate_required("owner_list.namespace", &namespace)?;
        validate_required("owner_list.owner_uid", &owner_uid)?;
        Ok(Self {
            namespace,
            owner_uid,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn owner_uid(&self) -> &str {
        &self.owner_uid
    }
}

#[derive(Clone, Debug)]
pub struct PodListResult {
    items: Vec<Resource>,
    resource_version: i64,
    continue_token: Option<String>,
    remaining_item_count: Option<i64>,
}

impl PodListResult {
    pub fn try_new(
        items: Vec<Resource>,
        resource_version: i64,
        continue_token: Option<String>,
        remaining_item_count: Option<i64>,
    ) -> Result<Self, PodRepositoryError> {
        if resource_version < 0 {
            return Err(PodRepositoryError::invalid_request(
                "list_result.resource_version",
                "must be non-negative",
            ));
        }
        if remaining_item_count.is_some_and(|remaining| remaining < 0) {
            return Err(PodRepositoryError::invalid_request(
                "list_result.remaining_item_count",
                "must be non-negative",
            ));
        }
        Ok(Self {
            items,
            resource_version,
            continue_token,
            remaining_item_count,
        })
    }

    pub fn items(&self) -> &[Resource] {
        &self.items
    }

    pub fn resource_version(&self) -> i64 {
        self.resource_version
    }

    pub fn continue_token(&self) -> Option<&str> {
        self.continue_token.as_deref()
    }

    pub fn remaining_item_count(&self) -> Option<i64> {
        self.remaining_item_count
    }

    pub fn into_parts(self) -> (Vec<Resource>, i64, Option<String>, Option<i64>) {
        (
            self.items,
            self.resource_version,
            self.continue_token,
            self.remaining_item_count,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodMutationTarget {
    namespace: String,
    name: String,
    uid: Option<String>,
}

impl PodMutationTarget {
    pub fn try_by_name(
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, PodRepositoryError> {
        let request = PodGetRequest::try_by_name(namespace, name)?;
        Ok(Self {
            namespace: request.namespace,
            name: request.name,
            uid: None,
        })
    }

    pub fn try_by_identity(identity: PodIdentity) -> Result<Self, PodRepositoryError> {
        let request = PodGetRequest::try_by_identity(identity)?;
        Ok(Self {
            namespace: request.namespace,
            name: request.name,
            uid: request.uid,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn uid(&self) -> Option<&str> {
        self.uid.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodLabel {
    key: String,
    value: String,
}

impl PodLabel {
    pub fn try_new(
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, PodRepositoryError> {
        let key = key.into();
        validate_required("label.key", &key)?;
        Ok(Self {
            key,
            value: value.into(),
        })
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn into_parts(self) -> (String, String) {
        (self.key, self.value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodOwnerReference {
    api_version: String,
    kind: String,
    name: String,
    uid: String,
    controller: Option<bool>,
    block_owner_deletion: Option<bool>,
}

impl PodOwnerReference {
    pub fn try_new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        name: impl Into<String>,
        uid: impl Into<String>,
        controller: Option<bool>,
        block_owner_deletion: Option<bool>,
    ) -> Result<Self, PodRepositoryError> {
        let api_version = api_version.into();
        let kind = kind.into();
        let name = name.into();
        let uid = uid.into();
        validate_required("owner_reference.api_version", &api_version)?;
        validate_required("owner_reference.kind", &kind)?;
        validate_required("owner_reference.name", &name)?;
        validate_required("owner_reference.uid", &uid)?;
        Ok(Self {
            api_version,
            kind,
            name,
            uid,
            controller,
            block_owner_deletion,
        })
    }

    pub fn api_version(&self) -> &str {
        &self.api_version
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn uid(&self) -> &str {
        &self.uid
    }

    pub fn controller(&self) -> Option<bool> {
        self.controller
    }

    pub fn block_owner_deletion(&self) -> Option<bool> {
        self.block_owner_deletion
    }

    pub fn into_parts(self) -> (String, String, String, String, Option<bool>, Option<bool>) {
        (
            self.api_version,
            self.kind,
            self.name,
            self.uid,
            self.controller,
            self.block_owner_deletion,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodUpdateOperation {
    MergeLabels(Vec<PodLabel>),
    ReplaceOwnerReferences(Vec<PodOwnerReference>),
    RecordSandboxId(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodUpdateRequest {
    target: PodMutationTarget,
    operation: PodUpdateOperation,
}

impl PodUpdateRequest {
    pub fn merge_labels(target: PodMutationTarget, labels: Vec<PodLabel>) -> Self {
        Self {
            target,
            operation: PodUpdateOperation::MergeLabels(labels),
        }
    }

    pub fn replace_owner_references(
        target: PodMutationTarget,
        owner_references: Vec<PodOwnerReference>,
    ) -> Self {
        Self {
            target,
            operation: PodUpdateOperation::ReplaceOwnerReferences(owner_references),
        }
    }

    pub fn try_record_sandbox_id(
        target: PodMutationTarget,
        sandbox_id: impl Into<String>,
    ) -> Result<Self, PodRepositoryError> {
        let sandbox_id = sandbox_id.into();
        validate_required("sandbox_id", &sandbox_id)?;
        Ok(Self {
            target,
            operation: PodUpdateOperation::RecordSandboxId(sandbox_id),
        })
    }

    pub fn target(&self) -> &PodMutationTarget {
        &self.target
    }

    pub fn labels(&self) -> Option<&[PodLabel]> {
        match &self.operation {
            PodUpdateOperation::MergeLabels(labels) => Some(labels),
            _ => None,
        }
    }

    pub fn owner_references(&self) -> Option<&[PodOwnerReference]> {
        match &self.operation {
            PodUpdateOperation::ReplaceOwnerReferences(owner_references) => Some(owner_references),
            _ => None,
        }
    }

    pub fn sandbox_id(&self) -> Option<&str> {
        match &self.operation {
            PodUpdateOperation::RecordSandboxId(sandbox_id) => Some(sandbox_id),
            _ => None,
        }
    }

    pub fn into_parts(self) -> (PodMutationTarget, PodUpdateOperation) {
        (self.target, self.operation)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodMarkTerminatingRequest {
    target: PodMutationTarget,
}

impl PodMarkTerminatingRequest {
    pub fn new(target: PodMutationTarget) -> Self {
        Self { target }
    }

    pub fn target(&self) -> &PodMutationTarget {
        &self.target
    }

    pub fn into_target(self) -> PodMutationTarget {
        self.target
    }
}

#[derive(Clone, Debug)]
pub struct PodLifecycleWakeupRequest {
    identity: PodIdentity,
    resource_version: i64,
    pod: Resource,
}

impl PodLifecycleWakeupRequest {
    pub fn try_from_pod(identity: PodIdentity, pod: Resource) -> Result<Self, PodRoutingError> {
        validate_routing_required("pod.identity.namespace", &identity.namespace)?;
        validate_routing_required("pod.identity.name", &identity.name)?;
        validate_routing_required("pod.identity.uid", &identity.uid)?;
        if pod.api_version != "v1" || pod.kind != "Pod" {
            return Err(PodRoutingError::invalid_request(
                "pod",
                "must be a v1 Pod resource",
            ));
        }
        let namespace = pod
            .namespace
            .as_deref()
            .ok_or_else(|| PodRoutingError::invalid_request("pod.namespace", "must be present"))?;
        validate_routing_required("pod.namespace", namespace)?;
        validate_routing_required("pod.name", &pod.name)?;
        validate_routing_required("pod.uid", &pod.uid)?;
        if namespace != identity.namespace || pod.name != identity.name || pod.uid != identity.uid {
            return Err(PodRoutingError::invalid_request(
                "pod.identity",
                format!(
                    "expected {}, found {}/{}/{}",
                    identity, namespace, pod.name, pod.uid
                ),
            ));
        }
        if pod.resource_version < 0 {
            return Err(PodRoutingError::invalid_request(
                "pod.resource_version",
                "must be non-negative",
            ));
        }
        Ok(Self {
            identity,
            resource_version: pod.resource_version,
            pod,
        })
    }

    pub fn identity(&self) -> &PodIdentity {
        &self.identity
    }

    pub fn resource_version(&self) -> i64 {
        self.resource_version
    }

    pub fn pod(&self) -> &Resource {
        &self.pod
    }

    pub fn into_pod(self) -> Resource {
        self.pod
    }

    pub fn into_parts(self) -> (PodIdentity, i64, Resource) {
        (self.identity, self.resource_version, self.pod)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodRepositoryError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    NotFound {
        namespace: String,
        name: String,
    },
    UidMismatch {
        expected: String,
        actual: String,
    },
    AlreadyExists {
        message: String,
    },
    Conflict {
        message: String,
    },
    Forbidden {
        message: String,
    },
    Unprocessable {
        message: String,
    },
    Internal {
        message: String,
    },
    Unavailable {
        message: String,
    },
    CorruptResponse {
        message: String,
    },
    Timeout,
    Cancelled,
}

impl PodRepositoryError {
    pub fn invalid_request(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn not_found(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self::NotFound {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    pub fn uid_mismatch(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        Self::UidMismatch {
            expected: expected.into(),
            actual: actual.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::AlreadyExists {
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::Forbidden {
            message: message.into(),
        }
    }

    pub fn unprocessable(message: impl Into<String>) -> Self {
        Self::Unprocessable {
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    pub fn corrupt_response(message: impl Into<String>) -> Self {
        Self::CorruptResponse {
            message: message.into(),
        }
    }
}

impl fmt::Display for PodRepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::NotFound { namespace, name } => {
                write!(formatter, "Pod {namespace}/{name} not found")
            }
            Self::UidMismatch { expected, actual } => {
                write!(
                    formatter,
                    "Pod UID mismatch: expected {expected}, found {actual}"
                )
            }
            Self::AlreadyExists { message } => {
                write!(formatter, "Pod already exists: {message}")
            }
            Self::Conflict { message } => {
                write!(formatter, "Pod conflict: {message} (409 Conflict)")
            }
            Self::Forbidden { message } => write!(formatter, "Pod operation forbidden: {message}"),
            Self::Unprocessable { message } => write!(formatter, "invalid Pod: {message}"),
            Self::Internal { message } => write!(formatter, "Pod repository failure: {message}"),
            Self::Unavailable { message } => {
                write!(formatter, "Pod repository unavailable: {message}")
            }
            Self::CorruptResponse { message } => {
                write!(formatter, "invalid Pod response: {message}")
            }
            Self::Timeout => formatter.write_str("Pod repository request timed out"),
            Self::Cancelled => formatter.write_str("Pod repository request cancelled"),
        }
    }
}

impl Error for PodRepositoryError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodRoutingError {
    InvalidRequest {
        field: &'static str,
        message: String,
    },
    Unavailable {
        message: String,
    },
    Timeout,
    Cancelled,
}

impl PodRoutingError {
    pub fn invalid_request(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field,
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }
}

impl fmt::Display for PodRoutingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::Unavailable { message } => {
                write!(formatter, "Pod routing unavailable: {message}")
            }
            Self::Timeout => formatter.write_str("Pod routing timed out"),
            Self::Cancelled => formatter.write_str("Pod routing cancelled"),
        }
    }
}

impl Error for PodRoutingError {}

fn validate_required(field: &'static str, value: &str) -> Result<(), PodRepositoryError> {
    if value.trim().is_empty() {
        return Err(PodRepositoryError::invalid_request(
            field,
            "must not be empty",
        ));
    }
    Ok(())
}

fn validate_bound_required(
    field: &'static str,
    value: &str,
) -> Result<(), BoundPodFinalizationError> {
    if value.trim().is_empty() {
        return Err(BoundPodFinalizationError::invalid_request(
            field,
            "must not be empty",
        ));
    }
    Ok(())
}

fn validate_unscheduled_required(
    field: &'static str,
    value: &str,
) -> Result<(), UnscheduledPodDeletionError> {
    if value.trim().is_empty() {
        return Err(UnscheduledPodDeletionError::invalid_request(
            field,
            "must not be empty",
        ));
    }
    Ok(())
}

fn validate_routing_required(field: &'static str, value: &str) -> Result<(), PodRoutingError> {
    if value.trim().is_empty() {
        return Err(PodRoutingError::invalid_request(field, "must not be empty"));
    }
    Ok(())
}
