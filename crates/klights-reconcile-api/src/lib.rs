//! Focused, transport-neutral mutation and reconciliation contracts for klights.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

use klights_cluster_core::Resource;
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

/// Borrowed, transport-neutral Pod facts that affect Service endpoint output.
///
/// The leaf owns comparison semantics while root adapters retain ownership of
/// protocol-specific extraction. The borrowed representation keeps mutation
/// classification allocation-free.
#[derive(Clone, Copy, Debug)]
pub struct PodEndpointState<'a, T: ?Sized> {
    ready: bool,
    terminal: bool,
    labels: Option<&'a T>,
    pod_ip: Option<&'a T>,
    pod_ips: Option<&'a T>,
    deletion_timestamp: Option<&'a T>,
}

impl<'a, T: ?Sized> PodEndpointState<'a, T> {
    pub const fn new(
        ready: bool,
        terminal: bool,
        labels: Option<&'a T>,
        pod_ip: Option<&'a T>,
        pod_ips: Option<&'a T>,
        deletion_timestamp: Option<&'a T>,
    ) -> Self {
        Self {
            ready,
            terminal,
            labels,
            pod_ip,
            pod_ips,
            deletion_timestamp,
        }
    }

    pub const fn is_ready(&self) -> bool {
        self.ready
    }

    pub const fn is_terminal(&self) -> bool {
        self.terminal
    }
}

impl<T: PartialEq + ?Sized> PodEndpointState<'_, T> {
    pub fn differs_from(&self, updated: &PodEndpointState<'_, T>) -> bool {
        self.ready != updated.ready
            || self.terminal != updated.terminal
            || self.labels != updated.labels
            || self.pod_ip != updated.pod_ip
            || self.pod_ips != updated.pod_ips
            || self.deletion_timestamp != updated.deletion_timestamp
    }
}

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

/// Service-only reconciliation notification capability.
pub trait ServiceReconcileSink: Send + Sync {
    fn enqueue_service_reconcile_batch(
        &self,
        keys: Vec<ServiceReconcileKey>,
    ) -> ReconcileSinkFuture<'_>;
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
