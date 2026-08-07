//! Node-local pod runtime, probe, durable workqueue, and slot persistence ports.
//!
//! This module owns persistence contracts only. CRI behavior, probe scheduling,
//! workqueue retry policy, timers, actors, and volume/filesystem behavior remain
//! with their feature owners. Workqueue payloads are deliberately opaque bytes.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use klights_types::PodIdentity;

/// Failure returned by node-local runtime/work persistence.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeWorkError {
    InvalidInput {
        field: &'static str,
        message: String,
    },
    PersistenceFailed {
        message: String,
    },
    CorruptData {
        message: String,
    },
    Retryable {
        message: String,
    },
    /// A UID-qualified slot mutation observed a different current owner.
    UidConflict {
        expected_uid: String,
        actual_uid: String,
    },
    /// An immutable pod-runtime owner or sandbox already owns the UID row.
    OwnershipConflict {
        pod_uid: String,
        existing_namespace: String,
        existing_pod_name: String,
        existing_node_name: String,
        existing_sandbox_id: Option<String>,
    },
    Timeout,
    Cancelled,
}

impl RuntimeWorkError {
    pub fn invalid_input(field: &'static str, message: impl Into<String>) -> Self {
        Self::invalid(field, message)
    }

    pub fn persistence_failed(message: impl Into<String>) -> Self {
        Self::PersistenceFailed {
            message: message.into(),
        }
    }

    pub fn corrupt_data(message: impl Into<String>) -> Self {
        Self::CorruptData {
            message: message.into(),
        }
    }

    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable {
            message: message.into(),
        }
    }

    pub fn uid_conflict(expected_uid: impl Into<String>, actual_uid: impl Into<String>) -> Self {
        Self::UidConflict {
            expected_uid: expected_uid.into(),
            actual_uid: actual_uid.into(),
        }
    }

    pub fn ownership_conflict(
        pod_uid: impl Into<String>,
        existing_namespace: impl Into<String>,
        existing_pod_name: impl Into<String>,
        existing_node_name: impl Into<String>,
        existing_sandbox_id: Option<String>,
    ) -> Self {
        Self::OwnershipConflict {
            pod_uid: pod_uid.into(),
            existing_namespace: existing_namespace.into(),
            existing_pod_name: existing_pod_name.into(),
            existing_node_name: existing_node_name.into(),
            existing_sandbox_id,
        }
    }

    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidInput {
            field,
            message: message.into(),
        }
    }
}

impl fmt::Display for RuntimeWorkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput { field, message } => {
                write!(formatter, "invalid {field}: {message}")
            }
            Self::PersistenceFailed { message }
            | Self::CorruptData { message }
            | Self::Retryable { message } => formatter.write_str(message),
            Self::UidConflict {
                expected_uid,
                actual_uid,
            } => write!(
                formatter,
                "pod slot UID precondition failed: expected {expected_uid:?}, found {actual_uid:?}"
            ),
            Self::OwnershipConflict {
                pod_uid,
                existing_namespace,
                existing_pod_name,
                existing_node_name,
                existing_sandbox_id,
            } => write!(
                formatter,
                "pod runtime ownership conflict for UID {pod_uid:?}: existing owner \
                 {existing_namespace}/{existing_pod_name} on {existing_node_name:?}, sandbox \
                 {existing_sandbox_id:?}"
            ),
            Self::Timeout => formatter.write_str("node runtime/work persistence timed out"),
            Self::Cancelled => formatter.write_str("node runtime/work persistence was cancelled"),
        }
    }
}

impl std::error::Error for RuntimeWorkError {}

fn require_nonempty(value: &str, field: &'static str) -> Result<(), RuntimeWorkError> {
    if value.is_empty() {
        Err(RuntimeWorkError::invalid(field, "must not be empty"))
    } else {
        Ok(())
    }
}

fn require_nonnegative(value: i64, field: &'static str) -> Result<(), RuntimeWorkError> {
    if value < 0 {
        Err(RuntimeWorkError::invalid(field, "must be non-negative"))
    } else {
        Ok(())
    }
}

fn require_positive(value: i64, field: &'static str) -> Result<(), RuntimeWorkError> {
    if value <= 0 {
        Err(RuntimeWorkError::invalid(field, "must be positive"))
    } else {
        Ok(())
    }
}

fn validate_pod_identity(pod: &PodIdentity) -> Result<(), RuntimeWorkError> {
    require_nonempty(&pod.namespace, "pod.namespace")?;
    require_nonempty(&pod.name, "pod.name")?;
    require_nonempty(&pod.uid, "pod.uid")
}

macro_rules! string_key {
    ($name:ident, $field:literal) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            pub fn try_new(value: impl Into<String>) -> Result<Self, RuntimeWorkError> {
                let value = value.into();
                require_nonempty(&value, $field)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }
    };
}

string_key!(RuntimePodUid, "pod_uid");
string_key!(RuntimeNamespace, "namespace");

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkItemId(i64);

impl WorkItemId {
    pub fn try_new(value: i64) -> Result<Self, RuntimeWorkError> {
        require_positive(value, "id")?;
        Ok(Self(value))
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DueTimeMs(i64);

impl DueTimeMs {
    pub fn try_new(value: i64) -> Result<Self, RuntimeWorkError> {
        require_nonnegative(value, "due_time_ms")?;
        Ok(Self(value))
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

/// One bounded lease claim against the durable workqueue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PodWorkqueueClaimRequest {
    now_ms: DueTimeMs,
    lease_duration_ms: i64,
    lease_deadline_ms: DueTimeMs,
}

impl PodWorkqueueClaimRequest {
    pub fn try_new(now_ms: i64, lease_duration_ms: i64) -> Result<Self, RuntimeWorkError> {
        let now_ms = DueTimeMs::try_new(now_ms)?;
        require_positive(lease_duration_ms, "lease_duration_ms")?;
        let lease_deadline_ms = now_ms
            .get()
            .checked_add(lease_duration_ms)
            .ok_or_else(|| RuntimeWorkError::invalid("lease_deadline_ms", "must not overflow"))?;
        Ok(Self {
            now_ms,
            lease_duration_ms,
            lease_deadline_ms: DueTimeMs::try_new(lease_deadline_ms)?,
        })
    }

    pub const fn now_ms(&self) -> DueTimeMs {
        self.now_ms
    }

    pub const fn lease_duration_ms(&self) -> i64 {
        self.lease_duration_ms
    }

    pub const fn lease_deadline_ms(&self) -> DueTimeMs {
        self.lease_deadline_ms
    }
}

/// Initial UID-bound runtime-row admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodRuntimeAdmission {
    pod: PodIdentity,
    node_name: String,
}

impl PodRuntimeAdmission {
    pub fn try_new(
        pod: PodIdentity,
        node_name: impl Into<String>,
    ) -> Result<Self, RuntimeWorkError> {
        let node_name = node_name.into();
        validate_pod_identity(&pod)?;
        require_nonempty(&node_name, "node_name")?;
        Ok(Self { pod, node_name })
    }

    pub const fn pod(&self) -> &PodIdentity {
        &self.pod
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn into_parts(self) -> (PodIdentity, String) {
        (self.pod, self.node_name)
    }
}

/// UID-qualified runtime ownership and sandbox record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedPodSandbox {
    pod: PodIdentity,
    node_name: String,
    sandbox_id: String,
    created_ms: i64,
}

impl OwnedPodSandbox {
    pub fn try_new(
        pod: PodIdentity,
        node_name: impl Into<String>,
        sandbox_id: impl Into<String>,
        created_ms: i64,
    ) -> Result<Self, RuntimeWorkError> {
        let node_name = node_name.into();
        let sandbox_id = sandbox_id.into();
        validate_pod_identity(&pod)?;
        require_nonempty(&node_name, "node_name")?;
        require_nonempty(&sandbox_id, "sandbox_id")?;
        require_nonnegative(created_ms, "created_ms")?;
        Ok(Self {
            pod,
            node_name,
            sandbox_id,
            created_ms,
        })
    }

    pub const fn pod(&self) -> &PodIdentity {
        &self.pod
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn sandbox_id(&self) -> &str {
        &self.sandbox_id
    }

    pub const fn created_ms(&self) -> i64 {
        self.created_ms
    }

    pub fn into_parts(self) -> (PodIdentity, String, String, i64) {
        (self.pod, self.node_name, self.sandbox_id, self.created_ms)
    }
}

/// UID-qualified cgroup update for an admitted runtime row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodRuntimeCgroup {
    pod_uid: String,
    cgroup_path: String,
}

impl PodRuntimeCgroup {
    pub fn try_new(
        pod_uid: impl Into<String>,
        cgroup_path: impl Into<String>,
    ) -> Result<Self, RuntimeWorkError> {
        let pod_uid = pod_uid.into();
        let cgroup_path = cgroup_path.into();
        require_nonempty(&pod_uid, "pod_uid")?;
        require_nonempty(&cgroup_path, "cgroup_path")?;
        Ok(Self {
            pod_uid,
            cgroup_path,
        })
    }

    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }

    pub fn cgroup_path(&self) -> &str {
        &self.cgroup_path
    }

    pub fn into_parts(self) -> (String, String) {
        (self.pod_uid, self.cgroup_path)
    }
}

/// Complete persisted runtime bookkeeping row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodRuntimeRecord {
    pod: PodIdentity,
    node_name: String,
    sandbox_id: Option<String>,
    cgroup_path: Option<String>,
    created_ms: i64,
    started_ms: Option<i64>,
}

impl PodRuntimeRecord {
    pub fn try_new(
        pod: PodIdentity,
        node_name: impl Into<String>,
        sandbox_id: Option<String>,
        cgroup_path: Option<String>,
        created_ms: i64,
        started_ms: Option<i64>,
    ) -> Result<Self, RuntimeWorkError> {
        let node_name = node_name.into();
        validate_pod_identity(&pod)?;
        require_nonempty(&node_name, "node_name")?;
        if let Some(value) = sandbox_id.as_deref() {
            require_nonempty(value, "sandbox_id")?;
        }
        if let Some(value) = cgroup_path.as_deref() {
            require_nonempty(value, "cgroup_path")?;
        }
        require_nonnegative(created_ms, "created_ms")?;
        if let Some(value) = started_ms {
            require_nonnegative(value, "started_ms")?;
        }
        Ok(Self {
            pod,
            node_name,
            sandbox_id,
            cgroup_path,
            created_ms,
            started_ms,
        })
    }

    pub const fn pod(&self) -> &PodIdentity {
        &self.pod
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn sandbox_id(&self) -> Option<&str> {
        self.sandbox_id.as_deref()
    }

    pub fn cgroup_path(&self) -> Option<&str> {
        self.cgroup_path.as_deref()
    }

    pub const fn created_ms(&self) -> i64 {
        self.created_ms
    }

    pub const fn started_ms(&self) -> Option<i64> {
        self.started_ms
    }

    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        PodIdentity,
        String,
        Option<String>,
        Option<String>,
        i64,
        Option<i64>,
    ) {
        (
            self.pod,
            self.node_name,
            self.sandbox_id,
            self.cgroup_path,
            self.created_ms,
            self.started_ms,
        )
    }
}

/// Exact identity of one persisted probe state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeKey {
    pod_uid: String,
    container_name: String,
    probe_kind: String,
}

impl ProbeKey {
    pub fn try_new(
        pod_uid: impl Into<String>,
        container_name: impl Into<String>,
        probe_kind: impl Into<String>,
    ) -> Result<Self, RuntimeWorkError> {
        let pod_uid = pod_uid.into();
        let container_name = container_name.into();
        let probe_kind = probe_kind.into();
        require_nonempty(&pod_uid, "pod_uid")?;
        require_nonempty(&container_name, "container_name")?;
        require_nonempty(&probe_kind, "probe_kind")?;
        Ok(Self {
            pod_uid,
            container_name,
            probe_kind,
        })
    }

    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }

    pub fn container_name(&self) -> &str {
        &self.container_name
    }

    pub fn probe_kind(&self) -> &str {
        &self.probe_kind
    }

    pub fn into_parts(self) -> (String, String, String) {
        (self.pod_uid, self.container_name, self.probe_kind)
    }
}

/// One probe observation to fold into durable probe state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeResult {
    key: ProbeKey,
    success: bool,
    result_ms: i64,
}

impl ProbeResult {
    pub fn try_new(key: ProbeKey, success: bool, result_ms: i64) -> Result<Self, RuntimeWorkError> {
        require_nonnegative(result_ms, "result_ms")?;
        Ok(Self {
            key,
            success,
            result_ms,
        })
    }

    pub const fn key(&self) -> &ProbeKey {
        &self.key
    }

    pub const fn success(&self) -> bool {
        self.success
    }

    pub const fn result_ms(&self) -> i64 {
        self.result_ms
    }

    pub fn into_parts(self) -> (ProbeKey, bool, i64) {
        (self.key, self.success, self.result_ms)
    }
}

/// Durable probe state after applying zero or more results.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeState {
    key: ProbeKey,
    last_result_ms: Option<i64>,
    last_success: Option<bool>,
    consecutive_failures: i64,
    next_eligible_ms: i64,
}

impl ProbeState {
    pub fn try_new(
        key: ProbeKey,
        last_result_ms: Option<i64>,
        last_success: Option<bool>,
        consecutive_failures: i64,
        next_eligible_ms: i64,
    ) -> Result<Self, RuntimeWorkError> {
        if let Some(value) = last_result_ms {
            require_nonnegative(value, "last_result_ms")?;
        }
        require_nonnegative(consecutive_failures, "consecutive_failures")?;
        require_nonnegative(next_eligible_ms, "next_eligible_ms")?;
        match (last_result_ms, last_success) {
            (None, None) if consecutive_failures == 0 => {}
            (Some(_), Some(true)) if consecutive_failures == 0 => {}
            (Some(_), Some(false)) if consecutive_failures > 0 => {}
            _ => {
                return Err(RuntimeWorkError::invalid(
                    "consecutive_failures",
                    "must match the persisted last-result success derivation",
                ));
            }
        }
        let expected_next = last_result_ms.unwrap_or(0);
        if next_eligible_ms != expected_next {
            return Err(RuntimeWorkError::invalid(
                "next_eligible_ms",
                "must equal the persisted last-result timestamp",
            ));
        }
        Ok(Self {
            key,
            last_result_ms,
            last_success,
            consecutive_failures,
            next_eligible_ms,
        })
    }

    pub const fn key(&self) -> &ProbeKey {
        &self.key
    }

    pub const fn last_result_ms(&self) -> Option<i64> {
        self.last_result_ms
    }

    pub const fn last_success(&self) -> Option<bool> {
        self.last_success
    }

    pub const fn consecutive_failures(&self) -> i64 {
        self.consecutive_failures
    }

    pub const fn next_eligible_ms(&self) -> i64 {
        self.next_eligible_ms
    }

    pub fn into_parts(self) -> (ProbeKey, Option<i64>, Option<bool>, i64, i64) {
        (
            self.key,
            self.last_result_ms,
            self.last_success,
            self.consecutive_failures,
            self.next_eligible_ms,
        )
    }
}

/// Durable Pod workqueue discriminator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodWorkqueueKind {
    Pod,
    Namespace,
}

/// Kind-aware durable work identity.
///
/// Namespace work intentionally persists the legacy shape
/// `PodIdentity("", namespace_name, namespace_uid)` without treating its empty
/// namespace field as a corrupt Pod identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodWorkIdentity {
    Pod(PodIdentity),
    Namespace { name: String, uid: String },
}

impl PodWorkIdentity {
    pub fn try_pod(pod: PodIdentity) -> Result<Self, RuntimeWorkError> {
        validate_pod_identity(&pod)?;
        Ok(Self::Pod(pod))
    }

    pub fn try_namespace(
        name: impl Into<String>,
        uid: impl Into<String>,
    ) -> Result<Self, RuntimeWorkError> {
        let name = name.into();
        let uid = uid.into();
        require_nonempty(&name, "namespace.name")?;
        require_nonempty(&uid, "namespace.uid")?;
        Ok(Self::Namespace { name, uid })
    }

    pub fn try_from_persisted(
        kind: PodWorkqueueKind,
        identity: PodIdentity,
    ) -> Result<Self, RuntimeWorkError> {
        match kind {
            PodWorkqueueKind::Pod => Self::try_pod(identity),
            PodWorkqueueKind::Namespace => {
                if !identity.namespace.is_empty() {
                    return Err(RuntimeWorkError::invalid(
                        "namespace.namespace",
                        "must be empty in the persisted namespace-work shape",
                    ));
                }
                Self::try_namespace(identity.name, identity.uid)
            }
        }
    }

    pub const fn kind(&self) -> PodWorkqueueKind {
        match self {
            Self::Pod(_) => PodWorkqueueKind::Pod,
            Self::Namespace { .. } => PodWorkqueueKind::Namespace,
        }
    }

    pub fn as_pod(&self) -> Option<&PodIdentity> {
        match self {
            Self::Pod(pod) => Some(pod),
            Self::Namespace { .. } => None,
        }
    }

    pub fn namespace_parts(&self) -> Option<(&str, &str)> {
        match self {
            Self::Namespace { name, uid } => Some((name, uid)),
            Self::Pod(_) => None,
        }
    }

    pub fn into_persisted(self) -> (PodWorkqueueKind, PodIdentity) {
        match self {
            Self::Pod(pod) => (PodWorkqueueKind::Pod, pod),
            Self::Namespace { name, uid } => (
                PodWorkqueueKind::Namespace,
                PodIdentity {
                    namespace: String::new(),
                    name,
                    uid,
                },
            ),
        }
    }
}

/// One UID-bound item to enqueue or replace in the durable workqueue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodWorkqueueEnqueue {
    identity: PodWorkIdentity,
    payload: Vec<u8>,
    attempt_count: i64,
    minimum_delay_ms: i64,
    last_error: Option<String>,
}

impl PodWorkqueueEnqueue {
    pub fn try_new(
        identity: PodWorkIdentity,
        payload: Vec<u8>,
        attempt_count: i64,
        minimum_delay_ms: i64,
        last_error: Option<String>,
    ) -> Result<Self, RuntimeWorkError> {
        require_nonnegative(attempt_count, "attempt_count")?;
        require_nonnegative(minimum_delay_ms, "minimum_delay_ms")?;
        Ok(Self {
            identity,
            payload,
            attempt_count,
            minimum_delay_ms,
            last_error,
        })
    }

    pub const fn kind(&self) -> PodWorkqueueKind {
        self.identity.kind()
    }

    pub const fn identity(&self) -> &PodWorkIdentity {
        &self.identity
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub const fn attempt_count(&self) -> i64 {
        self.attempt_count
    }

    pub const fn minimum_delay_ms(&self) -> i64 {
        self.minimum_delay_ms
    }

    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    pub fn into_parts(self) -> (PodWorkIdentity, Vec<u8>, i64, i64, Option<String>) {
        (
            self.identity,
            self.payload,
            self.attempt_count,
            self.minimum_delay_ms,
            self.last_error,
        )
    }
}

/// One atomically claimed workqueue entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodWorkqueueEntry {
    id: WorkItemId,
    identity: PodWorkIdentity,
    payload: Vec<u8>,
    attempt_count: i64,
    next_due_ms: DueTimeMs,
}

/// Exact ownership token for one retained workqueue row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodWorkqueueLeaseToken {
    id: WorkItemId,
    identity: PodWorkIdentity,
    leased_next_due_ms: DueTimeMs,
}

impl PodWorkqueueLeaseToken {
    pub fn try_new(
        id: i64,
        identity: PodWorkIdentity,
        leased_next_due_ms: i64,
    ) -> Result<Self, RuntimeWorkError> {
        Ok(Self {
            id: WorkItemId::try_new(id)?,
            identity,
            leased_next_due_ms: DueTimeMs::try_new(leased_next_due_ms)?,
        })
    }

    pub const fn id(&self) -> WorkItemId {
        self.id
    }

    pub const fn identity(&self) -> &PodWorkIdentity {
        &self.identity
    }

    pub const fn leased_next_due_ms(&self) -> DueTimeMs {
        self.leased_next_due_ms
    }

    pub fn into_parts(self) -> (WorkItemId, PodWorkIdentity, DueTimeMs) {
        (self.id, self.identity, self.leased_next_due_ms)
    }
}

/// A retained workqueue row and the exact token which owns its current lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodWorkqueueLease {
    entry: PodWorkqueueEntry,
    token: PodWorkqueueLeaseToken,
}

impl PodWorkqueueLease {
    pub fn try_new(
        entry: PodWorkqueueEntry,
        token: PodWorkqueueLeaseToken,
    ) -> Result<Self, RuntimeWorkError> {
        if entry.id() != token.id() || entry.identity() != token.identity() {
            return Err(RuntimeWorkError::invalid(
                "lease_token",
                "must match the claimed row identity",
            ));
        }
        Ok(Self { entry, token })
    }

    pub const fn entry(&self) -> &PodWorkqueueEntry {
        &self.entry
    }

    pub const fn token(&self) -> &PodWorkqueueLeaseToken {
        &self.token
    }

    pub fn into_parts(self) -> (PodWorkqueueEntry, PodWorkqueueLeaseToken) {
        (self.entry, self.token)
    }
}

/// Result of an exact lease-token mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodWorkqueueMutationOutcome {
    Applied,
    Stale,
}

/// Exact-token requeue of one retained work item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodWorkqueueRequeue {
    token: PodWorkqueueLeaseToken,
    payload: Vec<u8>,
    attempt_count: i64,
    minimum_delay_ms: i64,
    last_error: Option<String>,
}

impl PodWorkqueueRequeue {
    pub fn try_new(
        token: PodWorkqueueLeaseToken,
        payload: Vec<u8>,
        attempt_count: i64,
        minimum_delay_ms: i64,
        last_error: Option<String>,
    ) -> Result<Self, RuntimeWorkError> {
        require_nonnegative(attempt_count, "attempt_count")?;
        require_nonnegative(minimum_delay_ms, "minimum_delay_ms")?;
        Ok(Self {
            token,
            payload,
            attempt_count,
            minimum_delay_ms,
            last_error,
        })
    }

    pub const fn token(&self) -> &PodWorkqueueLeaseToken {
        &self.token
    }

    pub fn into_parts(self) -> (PodWorkqueueLeaseToken, Vec<u8>, i64, i64, Option<String>) {
        (
            self.token,
            self.payload,
            self.attempt_count,
            self.minimum_delay_ms,
            self.last_error,
        )
    }
}

impl PodWorkqueueEntry {
    pub fn try_new(
        id: i64,
        identity: PodWorkIdentity,
        payload: Vec<u8>,
        attempt_count: i64,
        next_due_ms: i64,
    ) -> Result<Self, RuntimeWorkError> {
        let id = WorkItemId::try_new(id)?;
        require_nonnegative(attempt_count, "attempt_count")?;
        let next_due_ms = DueTimeMs::try_new(next_due_ms)?;
        Ok(Self {
            id,
            identity,
            payload,
            attempt_count,
            next_due_ms,
        })
    }

    pub const fn id(&self) -> WorkItemId {
        self.id
    }

    pub const fn kind(&self) -> PodWorkqueueKind {
        self.identity.kind()
    }

    pub const fn identity(&self) -> &PodWorkIdentity {
        &self.identity
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub const fn attempt_count(&self) -> i64 {
        self.attempt_count
    }

    pub const fn next_due_ms(&self) -> DueTimeMs {
        self.next_due_ms
    }

    pub fn into_parts(self) -> (WorkItemId, PodWorkIdentity, Vec<u8>, i64, DueTimeMs) {
        (
            self.id,
            self.identity,
            self.payload,
            self.attempt_count,
            self.next_due_ms,
        )
    }
}

/// Persisted state of a Pod namespace/name admission slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodSlotAdmissionState {
    Admitted,
    Terminating,
}

/// Positive, opaque Pod ordering observation carried by slot persistence.
///
/// Node-store neither allocates nor interprets this value. Positivity rejects
/// the legacy worker compatibility sentinel `0`.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObservedPodVersion(i64);

impl ObservedPodVersion {
    pub fn try_new(value: i64) -> Result<Self, RuntimeWorkError> {
        require_positive(value, "observed_pod_version")?;
        Ok(Self(value))
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

/// UID-qualified slot operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodSlotAdmissionRequest {
    pod: PodIdentity,
    node_name: String,
}

impl PodSlotAdmissionRequest {
    pub fn try_new(
        pod: PodIdentity,
        node_name: impl Into<String>,
    ) -> Result<Self, RuntimeWorkError> {
        let node_name = node_name.into();
        validate_pod_identity(&pod)?;
        require_nonempty(&node_name, "node_name")?;
        Ok(Self { pod, node_name })
    }

    pub const fn pod(&self) -> &PodIdentity {
        &self.pod
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn into_parts(self) -> (PodIdentity, String) {
        (self.pod, self.node_name)
    }
}

/// Result of atomically trying to acquire a namespace/name Pod slot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodSlotAdmissionResult {
    Admitted {
        /// Opaque Pod ordering observation; node-store never allocates or interprets it.
        observed_pod_version: ObservedPodVersion,
    },
    Blocked {
        blocking_uid: String,
        blocking_node: String,
        state: PodSlotAdmissionState,
        /// Opaque Pod ordering observation; node-store never allocates or interprets it.
        observed_pod_version: ObservedPodVersion,
    },
}

/// Result of an idempotent same-UID state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodSlotMutationResult {
    Changed {
        observed_pod_version: ObservedPodVersion,
    },
    Unchanged {
        observed_pod_version: ObservedPodVersion,
    },
}

/// Result of compare-and-delete by Pod UID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodSlotClearResult {
    Cleared {
        observed_pod_version: ObservedPodVersion,
    },
    NotFound,
    UidMismatch {
        blocking_uid: String,
        blocking_node: String,
        state: PodSlotAdmissionState,
        observed_pod_version: ObservedPodVersion,
    },
}

/// Post-commit slot transition. Idempotent/no-op operations emit no event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodSlotAdmissionEvent {
    Changed {
        pod: PodIdentity,
        state: PodSlotAdmissionState,
        /// Opaque Pod ordering observation passed through from slot persistence.
        observed_pod_version: ObservedPodVersion,
    },
    Cleared {
        pod: PodIdentity,
        /// Opaque Pod ordering observation passed through from slot persistence.
        observed_pod_version: ObservedPodVersion,
    },
}

/// Heap-erased future used by coarse persistence boundaries.
pub type RuntimeWorkFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, RuntimeWorkError>> + Send + 'a>>;

/// UID-bound pod runtime bookkeeping persistence.
pub trait PodRuntimeStore: Send + Sync {
    /// Inserts by Pod UID or confirms the same immutable identity/node owner
    /// without resetting the original creation timestamp.
    fn admit_pod_runtime(&self, admission: PodRuntimeAdmission) -> RuntimeWorkFuture<'_, ()>;
    fn record_owned_sandbox(&self, sandbox: OwnedPodSandbox) -> RuntimeWorkFuture<'_, ()>;
    fn record_cgroup(&self, cgroup: PodRuntimeCgroup) -> RuntimeWorkFuture<'_, ()>;
    fn delete_pod_runtime_for_uid(&self, pod_uid: RuntimePodUid) -> RuntimeWorkFuture<'_, ()>;
    fn get_pod_runtime(
        &self,
        pod_uid: RuntimePodUid,
    ) -> RuntimeWorkFuture<'_, Option<PodRuntimeRecord>>;
    /// Lists rows in stable Pod-UID order.
    fn list_pod_runtime(&self) -> RuntimeWorkFuture<'_, Vec<PodRuntimeRecord>>;
    /// Lists one exact namespace in stable Pod-UID order.
    fn list_pod_runtime_by_namespace(
        &self,
        namespace: RuntimeNamespace,
    ) -> RuntimeWorkFuture<'_, Vec<PodRuntimeRecord>>;
}

/// Persistence of the existing dormant probe-result derivation.
pub trait ProbeStateStore: Send + Sync {
    /// Upserts one result: success resets consecutive failures, failure
    /// increments them, and next-eligible time becomes the result timestamp.
    fn record_probe_result(&self, result: ProbeResult) -> RuntimeWorkFuture<'_, ()>;
    fn get_probe_state(&self, key: ProbeKey) -> RuntimeWorkFuture<'_, Option<ProbeState>>;
}

/// Durable UID-bound Pod workqueue persistence.
pub trait PodWorkqueueStore: Send + Sync {
    /// Stamps enqueue time and atomically inserts or replaces the same
    /// `(kind, namespace, name, UID)` key. Due ordering remains strictly after
    /// the current tail of every other key while honoring the minimum delay.
    fn enqueue_work(&self, entry: PodWorkqueueEnqueue) -> RuntimeWorkFuture<'_, ()>;
    fn peek_next_due_ms(&self) -> RuntimeWorkFuture<'_, Option<i64>>;
    /// Atomically leases and retains the due row ordered by `(next_due_ms, id)`.
    fn claim_due_work_with_lease(
        &self,
        request: PodWorkqueueClaimRequest,
    ) -> RuntimeWorkFuture<'_, Option<PodWorkqueueLease>>;
    /// Removes only the exact currently leased row and reports stale ownership.
    fn acknowledge_work(
        &self,
        token: PodWorkqueueLeaseToken,
    ) -> RuntimeWorkFuture<'_, PodWorkqueueMutationOutcome>;
    /// Requeues only the exact currently leased row and reports stale ownership.
    fn requeue_work(
        &self,
        request: PodWorkqueueRequeue,
    ) -> RuntimeWorkFuture<'_, PodWorkqueueMutationOutcome>;
}

/// Exact node-local Pod slot admission and UID-CAS persistence.
///
/// The Phase 6B compatibility adapter intentionally does not implement this
/// port: the current exact implementation is reachable only through a broad
/// datastore facade, while a worker compatibility path fabricates observed Pod
/// version `0`.
/// Phase 11D implements this port directly over node-local persistence.
pub trait PodSlotAdmissionStore: Send + Sync {
    /// Admits an empty/same-UID slot and blocks without mutation for another UID.
    fn try_admit(
        &self,
        request: PodSlotAdmissionRequest,
    ) -> RuntimeWorkFuture<'_, PodSlotAdmissionResult>;
    /// Inserts or transitions the same UID to terminating; another UID is a
    /// typed precondition conflict.
    fn mark_terminating(
        &self,
        request: PodSlotAdmissionRequest,
    ) -> RuntimeWorkFuture<'_, PodSlotMutationResult>;
    /// Deletes only the matching UID and reports absent/mismatched slots
    /// without freeing a same-name replacement.
    fn clear_if_uid(
        &self,
        request: PodSlotAdmissionRequest,
    ) -> RuntimeWorkFuture<'_, PodSlotClearResult>;
}

/// Transport-neutral subscription to post-commit slot events.
pub trait PodSlotEventSubscription: Send {
    fn next_event(&mut self) -> RuntimeWorkFuture<'_, Option<PodSlotAdmissionEvent>>;
}

/// Separate event capability so mutation-only consumers need no subscription.
pub trait PodSlotAdmissionEventSource: Send + Sync {
    fn subscribe(&self) -> Box<dyn PodSlotEventSubscription>;
}
