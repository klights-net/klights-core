//! Focused, transport-neutral mutation and reconciliation contracts for klights.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

use klights_cluster_core::Resource;
use klights_cluster_core::ResourcePreconditions;

pub fn compute_statefulset_update_revision(name: &str, template: &serde_json::Value) -> String {
    let canonical = serde_json::to_string(template).unwrap_or_default();
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    canonical.hash(&mut hasher);
    format!("{}-{:x}", name, hasher.finish())
}

/// Failure counters consumed by Pod deletion and deferred-work paths.
pub trait ReconcileFailureMetrics: Send + Sync {
    fn record_cascade_delete_failure(&self);
    fn record_namespace_delete_failure(&self);
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalizerResourceTarget {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    name: String,
}

impl FinalizerResourceTarget {
    pub fn try_new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<&str>,
        name: impl Into<String>,
    ) -> Result<Self, FinalizerLifecycleError> {
        let api_version = api_version.into();
        let kind = kind.into();
        if api_version == "v1" && kind == "Pod" {
            return Err(FinalizerLifecycleError::PodForbidden(
                "generic finalizer lifecycle operations are forbidden for v1/Pod; use the Pod lifecycle actor"
                    .to_string(),
            ));
        }
        Ok(Self {
            api_version,
            kind,
            namespace: namespace.map(str::to_string),
            name: name.into(),
        })
    }

    pub fn api_version(&self) -> &str {
        &self.api_version
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug)]
pub struct FinalizerUpdateRequest {
    pub target: FinalizerResourceTarget,
    pub data: serde_json::Value,
    pub preconditions: ResourcePreconditions,
}

#[derive(Clone, Debug)]
pub struct FinalizerTombstoneDeleteRequest {
    pub target: FinalizerResourceTarget,
    pub preconditions: ResourcePreconditions,
    pub grace_seconds: i64,
}

#[derive(Clone, Debug)]
pub struct FinalizerOrphanRequest {
    pub target: FinalizerResourceTarget,
    pub owner_uid: String,
}

#[derive(Clone, Debug)]
pub struct FinalizerEffectsRequest {
    pub resource: Resource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinalizerLifecycleError {
    PodForbidden(String),
    NotFound(String),
    Conflict(String),
    Internal(String),
}

impl fmt::Display for FinalizerLifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PodForbidden(message)
            | Self::NotFound(message)
            | Self::Conflict(message)
            | Self::Internal(message) => formatter.write_str(message),
        }
    }
}

impl Error for FinalizerLifecycleError {}

pub type FinalizerLifecycleFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, FinalizerLifecycleError>> + Send + 'a>>;

pub trait FinalizerLifecyclePort: Send + Sync {
    fn get_resource(
        &self,
        target: FinalizerResourceTarget,
    ) -> FinalizerLifecycleFuture<'_, Option<Resource>>;

    fn update_resource(
        &self,
        request: FinalizerUpdateRequest,
    ) -> FinalizerLifecycleFuture<'_, Resource>;

    fn delete_with_tombstone(
        &self,
        request: FinalizerTombstoneDeleteRequest,
    ) -> FinalizerLifecycleFuture<'_, Resource>;

    fn orphan_children(&self, request: FinalizerOrphanRequest) -> FinalizerLifecycleFuture<'_, ()>;

    fn run_finalized_effects(
        &self,
        request: FinalizerEffectsRequest,
    ) -> FinalizerLifecycleFuture<'_, ()>;
}
use klights_types::PodIdentity;

/// Object-safe future used at the coarse component reconciliation boundary.
///
/// The allocation is paid once for a complete mutation batch; classification
/// and no-op gating remain allocation-free.
pub type ReconcileSinkFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), ReconcileSinkError>> + Send + 'a>>;
pub type GcPodDeleteFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), GcPodDeleteError>> + Send + 'a>>;

/// Mutation operation observed at a successfully negotiated API boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationOperation {
    Create,
    Update,
    Patch,
    DeleteMark,
    HardDelete,
}

/// Reconciliation-visible classification of an effective resource mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceChange {
    Created,
    Updated,
    Deleted,
}

pub struct ResourceMutationEffectsRequest<'a> {
    change: ResourceChange,
    resource: &'a serde_json::Value,
    old_resource: Option<&'a serde_json::Value>,
    context: &'static str,
}

impl<'a> ResourceMutationEffectsRequest<'a> {
    pub const fn new(
        change: ResourceChange,
        resource: &'a serde_json::Value,
        old_resource: Option<&'a serde_json::Value>,
        context: &'static str,
    ) -> Self {
        Self {
            change,
            resource,
            old_resource,
            context,
        }
    }

    pub const fn change(&self) -> ResourceChange {
        self.change
    }

    pub const fn resource(&self) -> &serde_json::Value {
        self.resource
    }

    pub const fn old_resource(&self) -> Option<&serde_json::Value> {
        self.old_resource
    }

    pub const fn context(&self) -> &'static str {
        self.context
    }
}

pub type ResourceMutationEffectsFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

pub trait ResourceMutationEffectsPort: Send + Sync {
    fn dispatch_resource_mutation_effects<'a>(
        &'a self,
        request: ResourceMutationEffectsRequest<'a>,
    ) -> ResourceMutationEffectsFuture<'a>;
}

pub use klights_cluster_core::PodEndpointState;

/// Neutral facts used to decide whether a mutation may emit reconciliation work.
///
/// Protocol-specific dry-run parsing stays at the API boundary. The contract
/// records only the result: dry-run and non-persisted operations never become
/// reconciliation changes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MutationFacts {
    operation: MutationOperation,
    persisted: bool,
    dry_run: bool,
}

impl MutationFacts {
    pub const fn new(operation: MutationOperation, persisted: bool, dry_run: bool) -> Self {
        Self {
            operation,
            persisted,
            dry_run,
        }
    }

    pub const fn operation(self) -> MutationOperation {
        self.operation
    }

    pub const fn persisted(self) -> bool {
        self.persisted
    }

    pub const fn dry_run(self) -> bool {
        self.dry_run
    }

    pub const fn change(self) -> Option<ResourceChange> {
        if !self.persisted || self.dry_run {
            return None;
        }
        Some(match self.operation {
            MutationOperation::Create => ResourceChange::Created,
            MutationOperation::Update
            | MutationOperation::Patch
            | MutationOperation::DeleteMark => ResourceChange::Updated,
            MutationOperation::HardDelete => ResourceChange::Deleted,
        })
    }
}

/// Controller reconciliation identity.
///
/// API version and kind remain static because the current dispatcher accepts
/// only its fixed registration set. Namespace and name preserve their current
/// strings exactly; validation remains with the resource/API owner.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ReconcileKey {
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<String>,
    name: String,
}

impl ReconcileKey {
    pub fn namespaced(
        api_version: &'static str,
        kind: &'static str,
        namespace: &str,
        name: &str,
    ) -> Self {
        Self {
            api_version,
            kind,
            namespace: Some(namespace.to_string()),
            name: name.to_string(),
        }
    }

    pub fn cluster(api_version: &'static str, kind: &'static str, name: &str) -> Self {
        Self {
            api_version,
            kind,
            namespace: None,
            name: name.to_string(),
        }
    }

    pub const fn api_version(&self) -> &'static str {
        self.api_version
    }

    pub const fn kind(&self) -> &'static str {
        self.kind
    }

    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn into_parts(self) -> (&'static str, &'static str, Option<String>, String) {
        (self.api_version, self.kind, self.namespace, self.name)
    }
}

impl fmt::Display for ReconcileKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(namespace) = &self.namespace {
            write!(
                formatter,
                "{}/{} {}/{}",
                self.api_version, self.kind, namespace, self.name
            )
        } else {
            write!(
                formatter,
                "{}/{} {}",
                self.api_version, self.kind, self.name
            )
        }
    }
}

/// Narrow namespaced Service reconciliation identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ServiceReconcileKey {
    namespace: String,
    name: String,
}

impl ServiceReconcileKey {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn into_reconcile_key(self) -> ReconcileKey {
        ReconcileKey {
            api_version: "v1",
            kind: "Service",
            namespace: Some(self.namespace),
            name: self.name,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconcileSinkError {
    Closed { message: String },
    Unavailable { message: String },
    UnsupportedKey { message: String },
}

impl ReconcileSinkError {
    pub fn closed(message: impl Into<String>) -> Self {
        Self::Closed {
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    pub fn unsupported_key(message: impl Into<String>) -> Self {
        Self::UnsupportedKey {
            message: message.into(),
        }
    }
}

impl fmt::Display for ReconcileSinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed { message }
            | Self::Unavailable { message }
            | Self::UnsupportedKey { message } => formatter.write_str(message),
        }
    }
}

impl Error for ReconcileSinkError {}

/// General controller-dispatch notification capability.
pub trait ControllerReconcileSink: Send + Sync {
    fn enqueue_reconcile_batch(&self, keys: Vec<ReconcileKey>) -> ReconcileSinkFuture<'_>;
}

pub type ControllerDispatchFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait ControllerDispatcherPort: ServiceReconcileSink + Send + Sync {
    fn enqueue<'a>(&'a self, resource: &'a serde_json::Value) -> ControllerDispatchFuture<'a, ()>;

    fn enqueue_reconcile(&self, key: ReconcileKey) -> ControllerDispatchFuture<'_, ()>;

    fn pending_reconcile_keys(&self) -> ControllerDispatchFuture<'_, Vec<ReconcileKey>>;
}

/// Service-only reconciliation notification capability.
pub trait ServiceReconcileSink: Send + Sync {
    fn enqueue_service_reconcile_batch(
        &self,
        keys: Vec<ServiceReconcileKey>,
    ) -> ReconcileSinkFuture<'_>;
}

pub trait ServiceRoutingSync: Send + Sync {
    fn request_service_routing_sync(&self) -> Result<(), ReconcileSinkError>;
}

pub trait ServiceAllocationReservation: Send {
    fn release(self: Box<Self>);
}

pub type ServiceAllocationFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ReconcileSinkError>> + Send + 'a>>;

pub trait ServiceWriteAllocator: Send + Sync {
    fn is_ready(&self) -> bool;

    fn prepare_create<'a>(
        &'a self,
        service: &'a mut serde_json::Value,
    ) -> ServiceAllocationFuture<'a, Box<dyn ServiceAllocationReservation>>;

    fn allocate_after_write<'a>(
        &'a self,
        service: &'a serde_json::Value,
    ) -> ServiceAllocationFuture<'a, Option<serde_json::Value>>;

    fn release_resource(&self, service: &serde_json::Value);
}

/// Root-owned effects emitted after kubelet Pod persistence.
///
/// The request contains only canonical resources and semantic intent; concrete
/// side-effect registries and controller dispatchers remain above kubelet.
#[derive(Clone, Debug)]
pub enum PodMutationReconcileRequest {
    RunHooks {
        pod: Resource,
        named_hook: Option<&'static str>,
        context: &'static str,
    },
    ServicesAfterUpdate {
        previous: Resource,
        updated: Resource,
    },
    ServicesAfterDelete {
        deleted: Resource,
    },
    StatusChanged {
        previous: Resource,
        updated: Resource,
    },
    EnqueueJobOwner {
        pod: Resource,
    },
}

pub trait PodMutationReconcileSink: Send + Sync {
    fn reconcile_pod_mutation(
        &self,
        request: PodMutationReconcileRequest,
    ) -> ReconcileSinkFuture<'_>;
}

/// UID-qualified Pod deletion request emitted by garbage collection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcPodDeleteRequest {
    identity: PodIdentity,
}

impl GcPodDeleteRequest {
    pub fn new(identity: PodIdentity) -> Self {
        Self { identity }
    }

    pub fn identity(&self) -> &PodIdentity {
        &self.identity
    }

    pub fn into_identity(self) -> PodIdentity {
        self.identity
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GcPodDeleteError {
    NotFound { message: String },
    IdentityChanged { message: String },
    Unavailable { message: String },
}

impl GcPodDeleteError {
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound {
            message: message.into(),
        }
    }

    pub fn identity_changed(message: impl Into<String>) -> Self {
        Self::IdentityChanged {
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable {
            message: message.into(),
        }
    }

    pub const fn is_gone_or_identity_changed(&self) -> bool {
        matches!(self, Self::NotFound { .. } | Self::IdentityChanged { .. })
    }
}

impl fmt::Display for GcPodDeleteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { message }
            | Self::IdentityChanged { message }
            | Self::Unavailable { message } => formatter.write_str(message),
        }
    }
}

impl Error for GcPodDeleteError {}

/// Garbage-collection boundary for actor-owned Pod deletion.
///
/// Implementations may only mark the exact UID terminating and wake its
/// lifecycle actor. This capability does not grant Pod-row hard deletion.
pub trait GcPodDeleteSink: Send + Sync {
    fn request_gc_pod_delete(&self, request: GcPodDeleteRequest) -> GcPodDeleteFuture<'_>;
}

#[derive(Clone, Debug)]
pub struct GcNonPodFinalizationRequest {
    pub resource: Resource,
    pub orphan_children: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GcNonPodFinalizationOutcome {
    HardDeleted,
    MarkedTerminating,
    Gone,
}

pub type GcNonPodFinalizationFuture<'a> = Pin<
    Box<dyn Future<Output = Result<GcNonPodFinalizationOutcome, ReconcileSinkError>> + Send + 'a>,
>;

/// Focused GC capability for finalizer-aware deletion of non-Pod resources.
///
/// Implementations must reject `v1/Pod`; actor-owned Pod deletion is exposed
/// only through [`GcPodDeleteSink`].
pub trait GcNonPodFinalizationPort: Send + Sync {
    fn finalize_non_pod(
        &self,
        request: GcNonPodFinalizationRequest,
    ) -> GcNonPodFinalizationFuture<'_>;
}

/// Complete UID-bound identity of a garbage-collection owner.
///
/// Empty UIDs remain representable for Kubernetes' legacy circular-owner
/// compatibility path; the GC implementation decides how that identity is
/// resolved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcOwnerIdentity {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub uid: String,
}

impl GcOwnerIdentity {
    pub fn new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<String>,
        name: impl Into<String>,
        uid: impl Into<String>,
    ) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace,
            name: name.into(),
            uid: uid.into(),
        }
    }
}

pub type GcOwnerBoolFuture<'a> =
    Pin<Box<dyn Future<Output = Result<bool, ReconcileSinkError>> + Send + 'a>>;

/// Focused owner-reference and dependent-cascade lifecycle capability.
///
/// Implementations must route Pods through [`GcPodDeleteSink`] and must use
/// [`GcNonPodFinalizationPort`] for every non-Pod finalization. The capability
/// preserves Kubernetes foreground, background, orphan, and UID semantics; it
/// does not expose generic controller dispatch or datastore access.
pub trait GcOwnerLifecyclePort: Send + Sync {
    fn reconcile_owner_references(&self, resource: Resource) -> ReconcileSinkFuture<'_>;

    fn cascade_delete(&self, owner: GcOwnerIdentity) -> ReconcileSinkFuture<'_>;

    fn sweep_dependents(&self, owner: GcOwnerIdentity) -> GcOwnerBoolFuture<'_>;

    fn finalize_foreground_owner(&self, owner: Resource) -> GcOwnerBoolFuture<'_>;
}

/// Pod-scoped garbage-collection reconciliation.
///
/// This capability may discover and mark dependent Pods through the supplied
/// UID-qualified delete sink. It never grants direct Pod-row deletion.
pub trait PodGcReconcileSink: Send + Sync {
    fn reconcile_owner_references<'a>(
        &'a self,
        pod: Resource,
        pod_delete_sink: &'a dyn GcPodDeleteSink,
    ) -> ReconcileSinkFuture<'a>;

    fn cascade_delete_dependents<'a>(
        &'a self,
        owner: PodIdentity,
        pod_delete_sink: &'a dyn GcPodDeleteSink,
    ) -> ReconcileSinkFuture<'a>;

    fn finalize_foreground_owners<'a>(
        &'a self,
        deleted_dependent: Resource,
        pod_delete_sink: &'a dyn GcPodDeleteSink,
    ) -> ReconcileSinkFuture<'a>;
}

/// PDB reconciliation triggered after a Pod lifecycle write.
pub trait PodPdbReconcileSink: Send + Sync {
    fn reconcile_namespace_pdbs(&self, namespace: String) -> ReconcileSinkFuture<'_>;
}

#[derive(Clone, Debug)]
pub struct PodEvictionAdmissionRequest {
    pub pod: Resource,
    pub dry_run: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodEvictionAdmissionOutcome {
    Allowed,
    DisruptionBudgetDenied {
        pdb_name: String,
        desired_healthy: i64,
        current_healthy: i64,
    },
    MultipleDisruptionBudgets {
        pdb_names: Vec<String>,
    },
    InvalidDisruptionBudget {
        pdb_name: String,
        message: String,
    },
}

pub type PodEvictionAdmissionFuture<'a> = Pin<
    Box<dyn Future<Output = Result<PodEvictionAdmissionOutcome, ReconcileSinkError>> + Send + 'a>,
>;

/// Atomically admits an eviction against the matching PodDisruptionBudget.
///
/// Live admission records the Pod in `status.disruptedPods` with a
/// resourceVersion CAS. Dry-run performs the same checks without persistence.
pub trait PodEvictionAdmissionSink: Send + Sync {
    fn admit_pod_eviction(
        &self,
        request: PodEvictionAdmissionRequest,
    ) -> PodEvictionAdmissionFuture<'_>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PvcReconcileOutcome {
    pub phase: Option<String>,
    pub volume_name: Option<String>,
}

pub type PvcReconcileFuture<'a> =
    Pin<Box<dyn Future<Output = Result<PvcReconcileOutcome, ReconcileSinkError>> + Send + 'a>>;

/// Root-owned PVC binding/provisioning capability.
pub trait PvcReconcileSink: Send + Sync {
    fn reconcile_pvc(&self, pvc: Resource) -> PvcReconcileFuture<'_>;
}

/// Pod-to-Service reconciliation boundary.
///
/// Implementations preserve selector matching, stale targetRef cleanup, and
/// endpoint-relevant update gating. Endpoint and EndpointSlice mutations must
/// not feed work back through this sink.
pub trait PodServiceReconcileSink: Send + Sync {
    fn enqueue_after_pod_create(&self, pod: Resource) -> ReconcileSinkFuture<'_>;

    fn enqueue_after_pod_update(
        &self,
        previous: Resource,
        updated: Resource,
    ) -> ReconcileSinkFuture<'_>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceTerminationRequest {
    pub namespace: String,
    pub expected_uid: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamespaceTerminationOutcome {
    Finalized,
    StillPending,
}

pub type NamespaceTerminationFuture<'a> = Pin<
    Box<dyn Future<Output = Result<NamespaceTerminationOutcome, ReconcileSinkError>> + Send + 'a>,
>;

/// Root-owned namespace finalization capability used by Pod lifecycle paths.
pub trait NamespaceTerminationSink: Send + Sync {
    fn reconcile_namespace_termination(
        &self,
        request: NamespaceTerminationRequest,
    ) -> NamespaceTerminationFuture<'_>;
}

pub trait NamespaceTerminationQueueSink: Send + Sync {
    fn enqueue_namespace_termination(
        &self,
        namespace: String,
        uid: String,
    ) -> ReconcileSinkFuture<'_>;
}

pub trait NamespaceBootstrapSink: Send + Sync {
    fn create_default_service_account(&self, namespace: String) -> ReconcileSinkFuture<'_>;

    fn create_root_ca_config_map(
        &self,
        namespace: String,
        ca_certificate: String,
    ) -> ReconcileSinkFuture<'_>;
}

pub type QuotaResourceListFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<Resource>, ReconcileSinkError>> + Send + 'a>>;

pub trait ResourceQuotaAdmissionRuntime: Send + Sync {
    fn list_resources<'a>(
        &'a self,
        api_version: &'a str,
        kind: &'a str,
        namespace: &'a str,
    ) -> QuotaResourceListFuture<'a>;

    fn pod_has_deletion_timestamp(&self, pod: &serde_json::Value) -> bool;
    fn pod_matches_resource_quota_scopes(
        &self,
        pod: &serde_json::Value,
        quota: &serde_json::Value,
    ) -> bool;
    fn resource_quota_has_pod_scope_constraints(&self, quota: &serde_json::Value) -> bool;
    fn parse_resource_quantity(&self, resource_key: &str, quantity: &str) -> Option<i64>;
    fn format_resource_quantity(&self, resource_key: &str, value: i64) -> String;
    fn calculate_pod_effective_resource_for_key(
        &self,
        pod: &serde_json::Value,
        bucket: &str,
        resource_key: &str,
    ) -> i64;
}
