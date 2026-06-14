//! Shared lifecycle message types — moved from `pod_lifecycle_actor` to
//! prevent `pod_lifecycle_actor` ↔ `pod_lifecycle_router` dependency cycles.

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PodLifecycleKey {
    pub namespace: String,
    pub name: String,
    pub uid: String,
}

impl PodLifecycleKey {
    pub fn new(namespace: &str, name: &str, uid: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            name: name.to_string(),
            uid: uid.to_string(),
        }
    }

    pub fn display(&self) -> String {
        format!("{}/{} uid={}", self.namespace, self.name, self.uid)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PodSlotKey {
    pub namespace: String,
    pub name: String,
}

impl PodSlotKey {
    pub fn new(namespace: &str, name: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            name: name.to_string(),
        }
    }
}

impl From<&PodLifecycleKey> for PodSlotKey {
    fn from(key: &PodLifecycleKey) -> Self {
        Self {
            namespace: key.namespace.clone(),
            name: key.name.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum LifecycleMessage {
    WatchAdded {
        key: PodLifecycleKey,
        resource_version: Option<i64>,
        pod: serde_json::Value,
    },
    WatchModified {
        key: PodLifecycleKey,
        resource_version: Option<i64>,
        pod: serde_json::Value,
    },
    WatchDeleted {
        key: PodLifecycleKey,
        resource_version: Option<i64>,
        pod: serde_json::Value,
    },
    CriEvent {
        key: PodLifecycleKey,
        container_id: String,
        kind: crate::kubelet::cri_events::KubeletEventKind,
    },
    LifecycleCommand {
        key: PodLifecycleKey,
        command: crate::kubelet::lifecycle::LifecycleCommand,
    },
    NetworkAssigned {
        key: PodLifecycleKey,
        sandbox_id: String,
        pod_ip: String,
    },
    PodWorkCompleted {
        key: PodLifecycleKey,
        operation_id: u64,
        kind: PodLifecycleWorkKind,
        /// Sandbox id for completion correlation (StartPod carries the new sandbox).
        sandbox_id: Option<String>,
    },
    PodWorkFailed {
        key: PodLifecycleKey,
        operation_id: u64,
        kind: PodLifecycleWorkKind,
        /// Whether the failure is transient and should trigger a retry.
        retryable: bool,
        failure: PodLifecycleWorkFailure,
    },
    SlotAdmissionGranted {
        key: PodLifecycleKey,
        operation_id: u64,
        pod: serde_json::Value,
        resource_version: Option<i64>,
        start_after_admit: bool,
    },
    SlotAdmissionBlocked {
        key: PodLifecycleKey,
        operation_id: u64,
        blocking_uid: String,
        blocking_node: String,
        state: crate::datastore::PodSlotAdmissionState,
    },
    SlotAdmissionWake {
        key: PodLifecycleKey,
    },
    ProbeResult {
        key: PodLifecycleKey,
        probe_id: u64,
        container_name: String,
        kind: PodProbeKind,
        result: PodProbeResult,
    },
    RetryDue {
        key: PodLifecycleKey,
    },
    OrphanFinalize {
        key: PodLifecycleKey,
        reason: crate::kubelet::pod_lifecycle_router::OrphanReason,
    },
    ActiveDeadlineDue {
        key: PodLifecycleKey,
    },
    ActorIdleGraceExpired {
        key: PodLifecycleKey,
        generation: u64,
    },
}

impl LifecycleMessage {
    pub fn key(&self) -> &PodLifecycleKey {
        match self {
            Self::WatchAdded { key, .. }
            | Self::WatchModified { key, .. }
            | Self::WatchDeleted { key, .. }
            | Self::CriEvent { key, .. }
            | Self::LifecycleCommand { key, .. }
            | Self::NetworkAssigned { key, .. }
            | Self::PodWorkCompleted { key, .. }
            | Self::PodWorkFailed { key, .. }
            | Self::SlotAdmissionGranted { key, .. }
            | Self::SlotAdmissionBlocked { key, .. }
            | Self::SlotAdmissionWake { key }
            | Self::ProbeResult { key, .. }
            | Self::RetryDue { key }
            | Self::OrphanFinalize { key, .. }
            | Self::ActiveDeadlineDue { key }
            | Self::ActorIdleGraceExpired { key, .. } => key,
        }
    }

    pub fn event_name(&self) -> &'static str {
        match self {
            Self::WatchAdded { .. } => "watch_added",
            Self::WatchModified { .. } => "watch_modified",
            Self::WatchDeleted { .. } => "watch_deleted",
            Self::CriEvent { .. } => "cri_event",
            Self::LifecycleCommand { .. } => "lifecycle_command",
            Self::NetworkAssigned { .. } => "network_assigned",
            Self::PodWorkCompleted { .. } => "pod_work_completed",
            Self::PodWorkFailed { .. } => "pod_work_failed",
            Self::SlotAdmissionGranted { .. } => "slot_admission_granted",
            Self::SlotAdmissionBlocked { .. } => "slot_admission_blocked",
            Self::SlotAdmissionWake { .. } => "slot_admission_wake",
            Self::ProbeResult { .. } => "probe_result",
            Self::RetryDue { .. } => "retry_due",
            Self::OrphanFinalize { .. } => "orphan_finalize",
            Self::ActiveDeadlineDue { .. } => "active_deadline_due",
            Self::ActorIdleGraceExpired { .. } => "actor_idle_grace_expired",
        }
    }

    pub fn resource_version(&self) -> Option<i64> {
        match self {
            Self::WatchAdded {
                resource_version, ..
            }
            | Self::WatchModified {
                resource_version, ..
            }
            | Self::WatchDeleted {
                resource_version, ..
            } => *resource_version,
            Self::CriEvent { .. }
            | Self::LifecycleCommand { .. }
            | Self::NetworkAssigned { .. }
            | Self::PodWorkCompleted { .. }
            | Self::PodWorkFailed { .. }
            | Self::SlotAdmissionBlocked { .. }
            | Self::SlotAdmissionWake { .. }
            | Self::ProbeResult { .. }
            | Self::RetryDue { .. }
            | Self::OrphanFinalize { .. }
            | Self::ActiveDeadlineDue { .. }
            | Self::ActorIdleGraceExpired { .. } => None,
            Self::SlotAdmissionGranted {
                resource_version, ..
            } => *resource_version,
        }
    }

    pub fn sandbox_id_hint(&self) -> Option<&str> {
        match self {
            Self::NetworkAssigned { sandbox_id, .. } => Some(sandbox_id.as_str()),
            Self::PodWorkCompleted {
                sandbox_id: Some(sandbox_id),
                ..
            } => Some(sandbox_id.as_str()),
            Self::WatchAdded { .. }
            | Self::WatchModified { .. }
            | Self::WatchDeleted { .. }
            | Self::CriEvent { .. }
            | Self::LifecycleCommand { .. }
            | Self::PodWorkCompleted {
                sandbox_id: None, ..
            }
            | Self::PodWorkFailed { .. }
            | Self::SlotAdmissionGranted { .. }
            | Self::SlotAdmissionBlocked { .. }
            | Self::SlotAdmissionWake { .. }
            | Self::ProbeResult { .. }
            | Self::RetryDue { .. }
            | Self::OrphanFinalize { .. }
            | Self::ActiveDeadlineDue { .. }
            | Self::ActorIdleGraceExpired { .. } => None,
        }
    }

    pub fn idle_grace_generation(&self) -> Option<u64> {
        match self {
            Self::ActorIdleGraceExpired { generation, .. } => Some(*generation),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodLifecycleWorkKind {
    StartPod,
    StopPod,
    FinalizePodDeletion,
    CheckSlotAdmission,
    FinalizeStartup,
    ReconcileRuntime,
    ReconcileCriLeftovers,
    HandleCommand,
    ReconcileEphemeral,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodLifecycleWorkFailure {
    Cancelled,
    DeadlineExceeded,
    Deleted,
    /// The runtime no longer has the target container.
    ContainerNotFound,
    FinalizersPending,
    Startup(String),
    /// Synthesized by the actor backend or multiplex adapter when dispatch
    /// itself fails (e.g. executor error, spawn rejection).
    DispatchFailed(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodProbeKind {
    Startup,
    Readiness,
    Liveness,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodProbeResult {
    Success,
    Failure(String),
    Timeout,
}
