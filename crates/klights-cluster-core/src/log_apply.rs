//! Pure logical commit envelope and deterministic outbox decisions.
//!
//! These values describe the mutation submitted to committed cluster-state
//! apply. Generated wire messages, durable store DTOs, SQL queries/upserts,
//! public resource-version allocation, and runtime orchestration remain adapter
//! concerns outside this crate.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{PatchKind, Resource};

/// Canonical state-machine result for a committed logical delta. The enum
/// prevents a terminal rejection from simultaneously claiming a visible
/// mutation or a no-op from manufacturing a new public resourceVersion.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum CommittedApplyOutcome {
    Visible {
        resource_version: i64,
        resource: Option<Resource>,
    },
    NoPublicChange {
        resource_version: i64,
        reason: NoPublicChangeReason,
    },
    Rejected(CommittedApplyRejection),
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoPublicChangeReason {
    DuplicateIdempotencyKey,
    DuplicateWatermark,
    StaleStatusStamp,
    EqualStatusStamp,
    LedgerOnly,
}

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommittedApplyRejection {
    NotFound { message: String },
    AlreadyExists { message: String },
    UidConflict { message: String },
    ResourceVersionConflict { message: String },
    InvalidCommit { message: String },
}

impl CommittedApplyRejection {
    pub fn message(&self) -> &str {
        match self {
            Self::NotFound { message }
            | Self::AlreadyExists { message }
            | Self::UidConflict { message }
            | Self::ResourceVersionConflict { message }
            | Self::InvalidCommit { message } => message,
        }
    }
}

/// One logical cluster-state commit in legacy or committed-apply form.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogApplyCommit {
    pub resource_version: i64,
    /// Missing fields from pre-envelope JSON/protobuf payloads decode to the
    /// legacy leader-assigned behavior.
    #[serde(default)]
    pub resource_version_assignment: ResourceVersionAssignment,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbox_watermark: Option<OutboxStreamWatermark>,
    pub mutations: Vec<LogApplyMutation>,
}

/// Wire-stable source of a replicated commit's public resourceVersion.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceVersionAssignment {
    #[default]
    LegacyLeaderAssigned,
    CommittedApplyV1,
}

impl ResourceVersionAssignment {
    pub const fn as_metadata_value(self) -> &'static str {
        match self {
            Self::LegacyLeaderAssigned => "legacy_leader_assigned",
            Self::CommittedApplyV1 => "committed_apply_v1",
        }
    }
}

/// Validation failure for the live/snapshot assignment envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceVersionAssignmentError {
    LegacyLiveRequiresPositive,
    CommittedApplyV1LiveRequiresZero,
    SnapshotRestoreRequiresLegacy,
}

impl fmt::Display for ResourceVersionAssignmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::LegacyLiveRequiresPositive => {
                "legacy leader-assigned live commit requires resourceVersion > 0"
            }
            Self::CommittedApplyV1LiveRequiresZero => {
                "committed-apply-v1 live commit requires resourceVersion == 0"
            }
            Self::SnapshotRestoreRequiresLegacy => {
                "snapshot restore requires legacy leader-assigned resourceVersion envelope"
            }
        })
    }
}

impl std::error::Error for ResourceVersionAssignmentError {}

/// Monotonic identity of one node-outbox stream position.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxStreamWatermark {
    pub client_id: String,
    pub stream_id: i64,
    pub stream_seq: i64,
}

impl LogApplyCommit {
    /// Construct a legacy leader-assigned commit from neutral mutation values.
    pub fn new(resource_version: i64, mutations: Vec<LogApplyMutation>) -> Self {
        Self {
            resource_version,
            resource_version_assignment: ResourceVersionAssignment::LegacyLeaderAssigned,
            outbox_watermark: None,
            mutations,
        }
    }

    pub fn from_cluster_mutations(resource_version: i64, mutations: Vec<ClusterMutation>) -> Self {
        Self {
            resource_version,
            resource_version_assignment: ResourceVersionAssignment::LegacyLeaderAssigned,
            outbox_watermark: None,
            mutations: mutations
                .into_iter()
                .map(ClusterMutation::into_log_apply_mutation)
                .collect(),
        }
    }

    pub fn put_resource(resource: &Resource) -> Self {
        Self::new(
            resource.resource_version,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: resource.api_version.clone(),
                kind: resource.kind.clone(),
                namespace: resource.namespace.clone(),
                name: resource.name.clone(),
                uid: resource.uid.clone(),
                resource_version: resource.resource_version,
                data: (*resource.data).clone(),
                require_absent: false,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            })],
        )
    }

    pub fn delete_resource(
        resource_version: i64,
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<String>,
        name: impl Into<String>,
        uid: impl Into<String>,
    ) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::DeleteResource(LogApplyResourceKey {
                api_version: api_version.into(),
                kind: kind.into(),
                namespace,
                name: name.into(),
                uid: uid.into(),
                precondition_resource_version: None,
            })],
        )
    }

    pub fn put_namespace(resource: &Resource) -> Self {
        Self::new(
            resource.resource_version,
            vec![LogApplyMutation::PutNamespace(LogApplyNamespaceRow {
                name: resource.name.clone(),
                uid: resource.uid.clone(),
                resource_version: resource.resource_version,
                data: (*resource.data).clone(),
            })],
        )
    }

    pub fn delete_namespace(resource_version: i64, name: impl Into<String>) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::DeleteNamespace { name: name.into() }],
        )
    }

    pub fn delete_namespace_contents(resource_version: i64, name: impl Into<String>) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::DeleteNamespaceContents { name: name.into() }],
        )
    }

    pub fn put_node_subnet_row(resource_version: i64, row: LogApplyNodeSubnetRow) -> Self {
        Self::new(resource_version, vec![LogApplyMutation::PutNodeSubnet(row)])
    }

    pub fn delete_node_subnet(resource_version: i64, node_name: impl Into<String>) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::DeleteNodeSubnet {
                node_name: node_name.into(),
            }],
        )
    }

    pub fn put_node_dataplane_row(resource_version: i64, row: LogApplyNodeDataplaneRow) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::PutNodeDataplane(row)],
        )
    }

    pub fn delete_node_dataplane(resource_version: i64, node_name: impl Into<String>) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::DeleteNodeDataplane {
                node_name: node_name.into(),
            }],
        )
    }

    pub fn advance_resource_version(resource_version: i64) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::AdvanceResourceVersion { resource_version }],
        )
    }

    pub fn put_applied_outbox_row(resource_version: i64, row: LogApplyAppliedOutboxRow) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::PutAppliedOutbox(row)],
        )
    }

    pub fn put_watch_event(row: LogApplyWatchEventRow) -> Self {
        Self::new(
            row.resource_version,
            vec![LogApplyMutation::PutWatchEvent(row)],
        )
    }

    pub fn gc_applied_outbox(
        resource_version: i64,
        cutoff_ms: i64,
        operations: Vec<String>,
    ) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::GcAppliedOutbox {
                cutoff_ms,
                operations,
            }],
        )
    }

    pub fn put_pod_cleanup_intent_row(
        resource_version: i64,
        row: LogApplyPodCleanupIntentRow,
    ) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::PutPodCleanupIntent(row)],
        )
    }

    /// Validate a live replicated commit before state-machine apply.
    pub const fn validate_live_resource_version_assignment(
        &self,
    ) -> Result<(), ResourceVersionAssignmentError> {
        match self.resource_version_assignment {
            ResourceVersionAssignment::LegacyLeaderAssigned if self.resource_version > 0 => Ok(()),
            ResourceVersionAssignment::LegacyLeaderAssigned => {
                Err(ResourceVersionAssignmentError::LegacyLiveRequiresPositive)
            }
            ResourceVersionAssignment::CommittedApplyV1 if self.resource_version == 0 => Ok(()),
            ResourceVersionAssignment::CommittedApplyV1 => {
                Err(ResourceVersionAssignmentError::CommittedApplyV1LiveRequiresZero)
            }
        }
    }

    /// Exact snapshot replay preserves historical RVs and never restamps them.
    pub const fn validate_snapshot_restore_resource_version_assignment(
        &self,
    ) -> Result<(), ResourceVersionAssignmentError> {
        if matches!(
            self.resource_version_assignment,
            ResourceVersionAssignment::LegacyLeaderAssigned
        ) {
            Ok(())
        } else {
            Err(ResourceVersionAssignmentError::SnapshotRestoreRequiresLegacy)
        }
    }

    /// Convert a proposal-time command materialization into a V1 template.
    /// Preconditions are deliberately retained; every output RV is assigned
    /// once by the committed persistence transaction.
    pub fn into_committed_apply_v1_template(mut self) -> Self {
        self.resource_version_assignment = ResourceVersionAssignment::CommittedApplyV1;
        self.resource_version = 0;
        for mutation in &mut self.mutations {
            match mutation {
                LogApplyMutation::PutResource(row) => row.resource_version = 0,
                LogApplyMutation::PatchResourceLatest(row) => row.resource_version = 0,
                LogApplyMutation::PutNamespace(row) => row.resource_version = 0,
                LogApplyMutation::PutWatchEvent(row) => row.resource_version = 0,
                LogApplyMutation::PutPodCleanupIntent(row) => row.resource_version = 0,
                LogApplyMutation::PutAppliedOutbox(row) => row.applied_rv = None,
                LogApplyMutation::AdvanceResourceVersion { resource_version } => {
                    *resource_version = 0;
                }
                _ => {}
            }
        }
        self
    }
}

/// One logical mutation inside a replicated cluster commit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LogApplyMutation {
    PutResource(LogApplyResourceRow),
    PatchResourceLatest(LogApplyResourcePatch),
    DeleteResource(LogApplyResourceKey),
    FinalizeBoundPod(LogApplyPodActorFinalization),
    PutNamespace(LogApplyNamespaceRow),
    DeleteNamespace {
        name: String,
    },
    DeleteNamespaceContents {
        name: String,
    },
    PutNodeSubnet(LogApplyNodeSubnetRow),
    AllocateNodeSubnet(LogApplyNodeSubnetAllocation),
    DeleteNodeSubnet {
        node_name: String,
    },
    PutNodeDataplane(LogApplyNodeDataplaneRow),
    DeleteNodeDataplane {
        node_name: String,
    },
    PutAppliedOutbox(LogApplyAppliedOutboxRow),
    DeleteAppliedOutbox {
        idempotency_key: String,
    },
    GcAppliedOutbox {
        cutoff_ms: i64,
        operations: Vec<String>,
    },
    GcWatchEvents {
        max_rows: i64,
        batch_cap: i64,
    },
    PutWatchEvent(LogApplyWatchEventRow),
    AdvanceResourceVersion {
        resource_version: i64,
    },
    PutKlightsMeta {
        key: String,
        value: String,
    },
    PutPodCleanupIntent(LogApplyPodCleanupIntentRow),
    DeletePodCleanupIntent(LogApplyPodCleanupIntentKey),
    DeletePodCleanupIntentsForNode {
        node_name: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogApplyResourceRow {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub uid: String,
    pub resource_version: i64,
    pub data: serde_json::Value,
    #[serde(default)]
    pub require_absent: bool,
    #[serde(default)]
    pub require_existing: bool,
    #[serde(default)]
    pub precondition_uid: Option<String>,
    #[serde(default)]
    pub precondition_resource_version: Option<i64>,
    #[serde(default)]
    pub status_only: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogApplyResourcePatch {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub resource_version: i64,
    pub patch_kind: PatchKind,
    pub patch: serde_json::Value,
    #[serde(default)]
    pub require_existing: bool,
    #[serde(default)]
    pub precondition_uid: Option<String>,
    #[serde(default)]
    pub precondition_resource_version: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminating_pod_unready_timestamp: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogApplyResourceKey {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    /// UID captured by the leader so a stale delete cannot remove a same-name
    /// replacement. Empty remains reserved for snapshot/backfill replay.
    #[serde(default)]
    pub uid: String,
    #[serde(default)]
    pub precondition_resource_version: Option<i64>,
}

/// Stable actor intent resolved against the live Pod row by the committed
/// state-machine transaction. Mutable resourceVersion state is deliberately
/// absent so transport replay cannot carry a stale CAS.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogApplyPodActorFinalization {
    pub namespace: String,
    pub name: String,
    pub pod_uid: String,
    pub node_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogApplyNodeSubnetAllocation {
    pub node_name: String,
    pub cluster_cidr: String,
    pub node_ip: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogApplyNamespaceRow {
    pub name: String,
    pub uid: String,
    pub resource_version: i64,
    pub data: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogApplyNodeSubnetRow {
    pub node_name: String,
    pub subnet: String,
    pub subnet_base_int: u32,
    pub gateway_ip: String,
    pub node_ip: String,
    pub mode: String,
    pub hostport_range: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogApplyNodeDataplaneRow {
    pub node_name: String,
    pub mode: String,
    pub encryption: String,
    pub public_key: Option<String>,
    pub endpoint: String,
    pub port: Option<u16>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogApplyAppliedOutboxRow {
    pub idempotency_key: String,
    pub subject_key: String,
    pub operation: String,
    pub first_seen_ms: i64,
    pub applied_rv: Option<i64>,
    pub result_proto: Vec<u8>,
    #[serde(default)]
    pub status_stamp: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogApplyWatchEventRow {
    #[serde(default)]
    pub event_id: Option<i64>,
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub resource_version: i64,
    pub event_type: String,
    pub data: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogApplyPodCleanupIntentRow {
    pub node_name: String,
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub reason: String,
    pub resource_version: i64,
    pub created_at_ms: i64,
    pub pod_data: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogApplyPodCleanupIntentKey {
    pub node_name: String,
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub reason: String,
}

/// Explicitly versioned family envelope used by the compatible JSON decoder.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VersionedClusterMutation {
    pub version: u32,
    pub mutation: ClusterMutation,
}

impl VersionedClusterMutation {
    pub const CURRENT_VERSION: u32 = 1;

    pub const fn new(mutation: ClusterMutation) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            mutation,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ResourceMutation {
    PutResource(LogApplyResourceRow),
    PatchResourceLatest(LogApplyResourcePatch),
    DeleteResource(LogApplyResourceKey),
    FinalizeBoundPod(LogApplyPodActorFinalization),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum NamespaceMutation {
    PutNamespace(LogApplyNamespaceRow),
    DeleteNamespace { name: String },
    DeleteNamespaceContents { name: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WatchHistoryMutation {
    PutWatchEvent(LogApplyWatchEventRow),
    GcWatchEvents { max_rows: i64, batch_cap: i64 },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum NetworkMutation {
    PutNodeSubnet(LogApplyNodeSubnetRow),
    AllocateNodeSubnet(LogApplyNodeSubnetAllocation),
    DeleteNodeSubnet { node_name: String },
    PutNodeDataplane(LogApplyNodeDataplaneRow),
    DeleteNodeDataplane { node_name: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OutboxLedgerMutation {
    PutAppliedOutbox(LogApplyAppliedOutboxRow),
    DeleteAppliedOutbox {
        idempotency_key: String,
    },
    GcAppliedOutbox {
        cutoff_ms: i64,
        operations: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ClusterMetaMutation {
    AdvanceResourceVersion { resource_version: i64 },
    PutKlightsMeta { key: String, value: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PodCleanupMutation {
    PutPodCleanupIntent(LogApplyPodCleanupIntentRow),
    DeletePodCleanupIntent(LogApplyPodCleanupIntentKey),
    DeletePodCleanupIntentsForNode { node_name: String },
}

/// Tagged logical mutation family. Every flat mutation converts without loss.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "family", content = "mutation")]
pub enum ClusterMutation {
    Resource(ResourceMutation),
    Namespace(NamespaceMutation),
    WatchHistory(WatchHistoryMutation),
    Network(NetworkMutation),
    OutboxLedger(OutboxLedgerMutation),
    ClusterMeta(ClusterMetaMutation),
    PodCleanup(PodCleanupMutation),
}

impl From<LogApplyMutation> for ClusterMutation {
    fn from(mutation: LogApplyMutation) -> Self {
        match mutation {
            LogApplyMutation::PutResource(value) => {
                Self::Resource(ResourceMutation::PutResource(value))
            }
            LogApplyMutation::PatchResourceLatest(value) => {
                Self::Resource(ResourceMutation::PatchResourceLatest(value))
            }
            LogApplyMutation::DeleteResource(value) => {
                Self::Resource(ResourceMutation::DeleteResource(value))
            }
            LogApplyMutation::FinalizeBoundPod(value) => {
                Self::Resource(ResourceMutation::FinalizeBoundPod(value))
            }
            LogApplyMutation::PutNamespace(value) => {
                Self::Namespace(NamespaceMutation::PutNamespace(value))
            }
            LogApplyMutation::DeleteNamespace { name } => {
                Self::Namespace(NamespaceMutation::DeleteNamespace { name })
            }
            LogApplyMutation::DeleteNamespaceContents { name } => {
                Self::Namespace(NamespaceMutation::DeleteNamespaceContents { name })
            }
            LogApplyMutation::PutWatchEvent(value) => {
                Self::WatchHistory(WatchHistoryMutation::PutWatchEvent(value))
            }
            LogApplyMutation::GcWatchEvents {
                max_rows,
                batch_cap,
            } => Self::WatchHistory(WatchHistoryMutation::GcWatchEvents {
                max_rows,
                batch_cap,
            }),
            LogApplyMutation::PutNodeSubnet(value) => {
                Self::Network(NetworkMutation::PutNodeSubnet(value))
            }
            LogApplyMutation::AllocateNodeSubnet(value) => {
                Self::Network(NetworkMutation::AllocateNodeSubnet(value))
            }
            LogApplyMutation::DeleteNodeSubnet { node_name } => {
                Self::Network(NetworkMutation::DeleteNodeSubnet { node_name })
            }
            LogApplyMutation::PutNodeDataplane(value) => {
                Self::Network(NetworkMutation::PutNodeDataplane(value))
            }
            LogApplyMutation::DeleteNodeDataplane { node_name } => {
                Self::Network(NetworkMutation::DeleteNodeDataplane { node_name })
            }
            LogApplyMutation::PutAppliedOutbox(value) => {
                Self::OutboxLedger(OutboxLedgerMutation::PutAppliedOutbox(value))
            }
            LogApplyMutation::DeleteAppliedOutbox { idempotency_key } => {
                Self::OutboxLedger(OutboxLedgerMutation::DeleteAppliedOutbox { idempotency_key })
            }
            LogApplyMutation::GcAppliedOutbox {
                cutoff_ms,
                operations,
            } => Self::OutboxLedger(OutboxLedgerMutation::GcAppliedOutbox {
                cutoff_ms,
                operations,
            }),
            LogApplyMutation::AdvanceResourceVersion { resource_version } => {
                Self::ClusterMeta(ClusterMetaMutation::AdvanceResourceVersion { resource_version })
            }
            LogApplyMutation::PutKlightsMeta { key, value } => {
                Self::ClusterMeta(ClusterMetaMutation::PutKlightsMeta { key, value })
            }
            LogApplyMutation::PutPodCleanupIntent(value) => {
                Self::PodCleanup(PodCleanupMutation::PutPodCleanupIntent(value))
            }
            LogApplyMutation::DeletePodCleanupIntent(value) => {
                Self::PodCleanup(PodCleanupMutation::DeletePodCleanupIntent(value))
            }
            LogApplyMutation::DeletePodCleanupIntentsForNode { node_name } => {
                Self::PodCleanup(PodCleanupMutation::DeletePodCleanupIntentsForNode { node_name })
            }
        }
    }
}

impl ClusterMutation {
    pub fn into_log_apply_mutation(self) -> LogApplyMutation {
        self.into()
    }
}

impl From<ClusterMutation> for LogApplyMutation {
    fn from(mutation: ClusterMutation) -> Self {
        match mutation {
            ClusterMutation::Resource(ResourceMutation::PutResource(value)) => {
                Self::PutResource(value)
            }
            ClusterMutation::Resource(ResourceMutation::PatchResourceLatest(value)) => {
                Self::PatchResourceLatest(value)
            }
            ClusterMutation::Resource(ResourceMutation::DeleteResource(value)) => {
                Self::DeleteResource(value)
            }
            ClusterMutation::Resource(ResourceMutation::FinalizeBoundPod(value)) => {
                Self::FinalizeBoundPod(value)
            }
            ClusterMutation::Namespace(NamespaceMutation::PutNamespace(value)) => {
                Self::PutNamespace(value)
            }
            ClusterMutation::Namespace(NamespaceMutation::DeleteNamespace { name }) => {
                Self::DeleteNamespace { name }
            }
            ClusterMutation::Namespace(NamespaceMutation::DeleteNamespaceContents { name }) => {
                Self::DeleteNamespaceContents { name }
            }
            ClusterMutation::WatchHistory(WatchHistoryMutation::PutWatchEvent(value)) => {
                Self::PutWatchEvent(value)
            }
            ClusterMutation::WatchHistory(WatchHistoryMutation::GcWatchEvents {
                max_rows,
                batch_cap,
            }) => Self::GcWatchEvents {
                max_rows,
                batch_cap,
            },
            ClusterMutation::Network(NetworkMutation::PutNodeSubnet(value)) => {
                Self::PutNodeSubnet(value)
            }
            ClusterMutation::Network(NetworkMutation::AllocateNodeSubnet(value)) => {
                Self::AllocateNodeSubnet(value)
            }
            ClusterMutation::Network(NetworkMutation::DeleteNodeSubnet { node_name }) => {
                Self::DeleteNodeSubnet { node_name }
            }
            ClusterMutation::Network(NetworkMutation::PutNodeDataplane(value)) => {
                Self::PutNodeDataplane(value)
            }
            ClusterMutation::Network(NetworkMutation::DeleteNodeDataplane { node_name }) => {
                Self::DeleteNodeDataplane { node_name }
            }
            ClusterMutation::OutboxLedger(OutboxLedgerMutation::PutAppliedOutbox(value)) => {
                Self::PutAppliedOutbox(value)
            }
            ClusterMutation::OutboxLedger(OutboxLedgerMutation::DeleteAppliedOutbox {
                idempotency_key,
            }) => Self::DeleteAppliedOutbox { idempotency_key },
            ClusterMutation::OutboxLedger(OutboxLedgerMutation::GcAppliedOutbox {
                cutoff_ms,
                operations,
            }) => Self::GcAppliedOutbox {
                cutoff_ms,
                operations,
            },
            ClusterMutation::ClusterMeta(ClusterMetaMutation::AdvanceResourceVersion {
                resource_version,
            }) => Self::AdvanceResourceVersion { resource_version },
            ClusterMutation::ClusterMeta(ClusterMetaMutation::PutKlightsMeta { key, value }) => {
                Self::PutKlightsMeta { key, value }
            }
            ClusterMutation::PodCleanup(PodCleanupMutation::PutPodCleanupIntent(value)) => {
                Self::PutPodCleanupIntent(value)
            }
            ClusterMutation::PodCleanup(PodCleanupMutation::DeletePodCleanupIntent(value)) => {
                Self::DeletePodCleanupIntent(value)
            }
            ClusterMutation::PodCleanup(PodCleanupMutation::DeletePodCleanupIntentsForNode {
                node_name,
            }) => Self::DeletePodCleanupIntentsForNode { node_name },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnsupportedClusterMutationVersion {
    pub version: u32,
    pub current: u32,
}

impl fmt::Display for UnsupportedClusterMutationVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unsupported ClusterMutation version {} (current {})",
            self.version, self.current
        )
    }
}

impl std::error::Error for UnsupportedClusterMutationVersion {}

impl TryFrom<VersionedClusterMutation> for LogApplyMutation {
    type Error = UnsupportedClusterMutationVersion;

    fn try_from(value: VersionedClusterMutation) -> Result<Self, Self::Error> {
        if value.version != VersionedClusterMutation::CURRENT_VERSION {
            return Err(UnsupportedClusterMutationVersion {
                version: value.version,
                current: VersionedClusterMutation::CURRENT_VERSION,
            });
        }
        Ok(value.mutation.into())
    }
}

/// Pure sequencing result after a persistence adapter reads the last sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutboxWatermarkDecision {
    Apply,
    Duplicate,
    Gap { last_seq: i64, next_seq: i64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidOutboxStreamSequence;

impl fmt::Display for InvalidOutboxStreamSequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("outbox stream seq must be positive")
    }
}

impl std::error::Error for InvalidOutboxStreamSequence {}

/// Decide whether a stream position applies, duplicates, or skips a sequence.
pub fn decide_outbox_watermark(
    last_seq: Option<i64>,
    watermark: Option<&OutboxStreamWatermark>,
) -> Result<OutboxWatermarkDecision, InvalidOutboxStreamSequence> {
    let Some(watermark) = watermark else {
        return Ok(OutboxWatermarkDecision::Apply);
    };
    if watermark.stream_seq <= 0 {
        return Err(InvalidOutboxStreamSequence);
    }
    Ok(match last_seq {
        Some(last_seq) if watermark.stream_seq <= last_seq => OutboxWatermarkDecision::Duplicate,
        Some(last_seq) if watermark.stream_seq != last_seq.saturating_add(1) => {
            OutboxWatermarkDecision::Gap {
                last_seq,
                next_seq: watermark.stream_seq,
            }
        }
        Some(_) => OutboxWatermarkDecision::Apply,
        None if watermark.stream_seq == 1 => OutboxWatermarkDecision::Apply,
        None => OutboxWatermarkDecision::Gap {
            last_seq: 0,
            next_seq: watermark.stream_seq,
        },
    })
}

/// Whether an outbox operation carries the monotonic Pod-status stamp contract.
pub fn is_stamped_pod_status_outbox_operation(operation: &str) -> bool {
    matches!(
        operation,
        "PodStatus"
            | "RuntimeReconcile"
            | "ProbeReadiness"
            | "DeadlineExceeded"
            | "ContainerStatusSnapshot"
            | "EphemeralContainerStatuses"
    )
}

/// Borrow the subject and positive status stamp from the first eligible row.
pub fn stamped_pod_status_subject_and_stamp(commit: &LogApplyCommit) -> Option<(&str, i64)> {
    commit.mutations.iter().find_map(|mutation| match mutation {
        LogApplyMutation::PutAppliedOutbox(row)
            if row.status_stamp.is_some_and(|stamp| stamp > 0)
                && is_stamped_pod_status_outbox_operation(&row.operation)
                && !row.subject_key.is_empty() =>
        {
            Some((
                row.subject_key.as_str(),
                row.status_stamp.expect("status_stamp was validated"),
            ))
        }
        _ => None,
    })
}

/// Preserve envelope metadata while retaining only durable outbox ledger puts.
pub fn commit_with_outbox_rows_only(commit: LogApplyCommit) -> LogApplyCommit {
    let mutations = commit
        .mutations
        .into_iter()
        .filter(|mutation| matches!(mutation, LogApplyMutation::PutAppliedOutbox(_)))
        .collect();
    LogApplyCommit {
        mutations,
        ..commit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn outbox_row(
        operation: &str,
        subject_key: &str,
        status_stamp: Option<i64>,
    ) -> LogApplyMutation {
        LogApplyMutation::PutAppliedOutbox(LogApplyAppliedOutboxRow {
            idempotency_key: "key-1".into(),
            subject_key: subject_key.into(),
            operation: operation.into(),
            first_seen_ms: 17,
            applied_rv: Some(19),
            result_proto: vec![1, 2, 3],
            status_stamp,
        })
    }

    #[test]
    fn assignment_validation_and_defaults_are_table_driven() {
        let decoded: LogApplyCommit =
            serde_json::from_str(r#"{"resource_version":9,"mutations":[]}"#).unwrap();
        assert_eq!(
            decoded.resource_version_assignment,
            ResourceVersionAssignment::LegacyLeaderAssigned
        );

        let cases = [
            (
                "legacy-positive",
                ResourceVersionAssignment::LegacyLeaderAssigned,
                1,
                None,
            ),
            (
                "legacy-zero",
                ResourceVersionAssignment::LegacyLeaderAssigned,
                0,
                Some(ResourceVersionAssignmentError::LegacyLiveRequiresPositive),
            ),
            (
                "v1-zero",
                ResourceVersionAssignment::CommittedApplyV1,
                0,
                None,
            ),
            (
                "v1-positive",
                ResourceVersionAssignment::CommittedApplyV1,
                1,
                Some(ResourceVersionAssignmentError::CommittedApplyV1LiveRequiresZero),
            ),
        ];
        for (label, assignment, resource_version, expected_error) in cases {
            let commit = LogApplyCommit {
                resource_version,
                resource_version_assignment: assignment,
                outbox_watermark: None,
                mutations: Vec::new(),
            };
            assert_eq!(
                commit.validate_live_resource_version_assignment().err(),
                expected_error,
                "{label}"
            );
            assert_eq!(
                commit
                    .validate_snapshot_restore_resource_version_assignment()
                    .is_ok(),
                assignment == ResourceVersionAssignment::LegacyLeaderAssigned,
                "{label}"
            );
        }
    }

    #[test]
    fn committed_apply_v1_template_zeroing_is_table_driven() {
        let commit = LogApplyCommit::new(
            73,
            vec![
                LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                    namespace: Some("default".into()),
                    name: "pod-a".into(),
                    uid: "uid-a".into(),
                    resource_version: 73,
                    data: json!({}),
                    require_absent: false,
                    require_existing: true,
                    precondition_uid: Some("uid-a".into()),
                    precondition_resource_version: Some(72),
                    status_only: true,
                }),
                LogApplyMutation::PatchResourceLatest(LogApplyResourcePatch {
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                    namespace: Some("default".into()),
                    name: "pod-a".into(),
                    resource_version: 73,
                    patch_kind: PatchKind::Merge,
                    patch: json!({"status": {}}),
                    require_existing: true,
                    precondition_uid: Some("uid-a".into()),
                    precondition_resource_version: Some(72),
                    terminating_pod_unready_timestamp: Some("timestamp".into()),
                }),
                LogApplyMutation::PutNamespace(LogApplyNamespaceRow {
                    name: "ns".into(),
                    uid: "ns-uid".into(),
                    resource_version: 73,
                    data: json!({}),
                }),
                LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                    event_id: Some(3),
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                    namespace: Some("default".into()),
                    name: "pod-a".into(),
                    resource_version: 73,
                    event_type: "MODIFIED".into(),
                    data: json!({}),
                }),
                LogApplyMutation::PutPodCleanupIntent(LogApplyPodCleanupIntentRow {
                    node_name: "node-a".into(),
                    namespace: "default".into(),
                    pod_name: "pod-a".into(),
                    pod_uid: "uid-a".into(),
                    reason: "NodeLost".into(),
                    resource_version: 73,
                    created_at_ms: 1,
                    pod_data: json!({}),
                }),
                outbox_row("PodStatus", "subject", Some(8)),
                LogApplyMutation::AdvanceResourceVersion {
                    resource_version: 73,
                },
            ],
        )
        .into_committed_apply_v1_template();

        assert_eq!(commit.resource_version, 0);
        assert_eq!(
            commit.resource_version_assignment,
            ResourceVersionAssignment::CommittedApplyV1
        );
        for mutation in &commit.mutations {
            match mutation {
                LogApplyMutation::PutResource(row) => {
                    assert_eq!(row.resource_version, 0);
                    assert_eq!(row.precondition_uid.as_deref(), Some("uid-a"));
                    assert_eq!(row.precondition_resource_version, Some(72));
                }
                LogApplyMutation::PatchResourceLatest(row) => {
                    assert_eq!(row.resource_version, 0);
                    assert_eq!(row.precondition_uid.as_deref(), Some("uid-a"));
                    assert_eq!(row.precondition_resource_version, Some(72));
                }
                LogApplyMutation::PutNamespace(row) => assert_eq!(row.resource_version, 0),
                LogApplyMutation::PutWatchEvent(row) => {
                    assert_eq!(row.resource_version, 0);
                    assert_eq!(row.event_id, Some(3));
                }
                LogApplyMutation::PutPodCleanupIntent(row) => {
                    assert_eq!(row.resource_version, 0)
                }
                LogApplyMutation::PutAppliedOutbox(row) => assert_eq!(row.applied_rv, None),
                LogApplyMutation::AdvanceResourceVersion { resource_version } => {
                    assert_eq!(*resource_version, 0)
                }
                other => panic!("unexpected template mutation: {other:?}"),
            }
        }
    }

    #[test]
    fn outbox_watermark_decision_matrix_is_table_driven() {
        let cases = [
            ("absent", None, None, Ok(OutboxWatermarkDecision::Apply)),
            ("first", None, Some(1), Ok(OutboxWatermarkDecision::Apply)),
            (
                "first-gap",
                None,
                Some(2),
                Ok(OutboxWatermarkDecision::Gap {
                    last_seq: 0,
                    next_seq: 2,
                }),
            ),
            ("next", Some(8), Some(9), Ok(OutboxWatermarkDecision::Apply)),
            (
                "duplicate",
                Some(8),
                Some(8),
                Ok(OutboxWatermarkDecision::Duplicate),
            ),
            (
                "older",
                Some(8),
                Some(7),
                Ok(OutboxWatermarkDecision::Duplicate),
            ),
            (
                "gap",
                Some(8),
                Some(10),
                Ok(OutboxWatermarkDecision::Gap {
                    last_seq: 8,
                    next_seq: 10,
                }),
            ),
            ("zero", Some(8), Some(0), Err(InvalidOutboxStreamSequence)),
            ("negative", None, Some(-1), Err(InvalidOutboxStreamSequence)),
        ];
        for (label, last_seq, stream_seq, expected) in cases {
            let watermark = stream_seq.map(|stream_seq| OutboxStreamWatermark {
                client_id: "worker-a".into(),
                stream_id: 4,
                stream_seq,
            });
            assert_eq!(
                decide_outbox_watermark(last_seq, watermark.as_ref()),
                expected,
                "{label}"
            );
        }
        assert_eq!(
            InvalidOutboxStreamSequence.to_string(),
            "outbox stream seq must be positive"
        );
    }

    #[test]
    fn stamped_status_operation_allowlist_is_exact() {
        let cases = [
            ("PodStatus", true),
            ("RuntimeReconcile", true),
            ("ProbeReadiness", true),
            ("DeadlineExceeded", true),
            ("ContainerStatusSnapshot", true),
            ("EphemeralContainerStatuses", true),
            ("PodMetadata", false),
            ("podstatus", false),
            ("", false),
        ];
        for (operation, expected) in cases {
            assert_eq!(
                is_stamped_pod_status_outbox_operation(operation),
                expected,
                "{operation:?}"
            );
        }
    }

    #[test]
    fn outbox_only_commit_filter_preserves_envelope() {
        let commit = LogApplyCommit {
            resource_version: 0,
            resource_version_assignment: ResourceVersionAssignment::CommittedApplyV1,
            outbox_watermark: Some(OutboxStreamWatermark {
                client_id: "worker-a".into(),
                stream_id: 3,
                stream_seq: 9,
            }),
            mutations: vec![
                LogApplyMutation::AdvanceResourceVersion {
                    resource_version: 0,
                },
                outbox_row("PodStatus", "subject-a", Some(7)),
                outbox_row("PodMetadata", "subject-b", None),
            ],
        };
        assert_eq!(
            stamped_pod_status_subject_and_stamp(&commit),
            Some(("subject-a", 7))
        );
        let filtered = commit_with_outbox_rows_only(commit);
        assert_eq!(filtered.resource_version, 0);
        assert_eq!(
            filtered.resource_version_assignment,
            ResourceVersionAssignment::CommittedApplyV1
        );
        assert_eq!(filtered.outbox_watermark.as_ref().unwrap().stream_seq, 9);
        assert_eq!(filtered.mutations.len(), 2);
        assert!(
            filtered
                .mutations
                .iter()
                .all(|mutation| matches!(mutation, LogApplyMutation::PutAppliedOutbox(_)))
        );

        for invalid in [
            outbox_row("PodStatus", "", Some(7)),
            outbox_row("PodStatus", "subject", Some(0)),
            outbox_row("PodMetadata", "subject", Some(7)),
        ] {
            assert_eq!(
                stamped_pod_status_subject_and_stamp(&LogApplyCommit::new(1, vec![invalid])),
                None
            );
        }
    }

    fn family_samples() -> Vec<(&'static str, LogApplyMutation)> {
        vec![
            (
                "resource",
                LogApplyMutation::DeleteResource(LogApplyResourceKey {
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                    namespace: Some("default".into()),
                    name: "pod-a".into(),
                    uid: "uid-a".into(),
                    precondition_resource_version: Some(7),
                }),
            ),
            (
                "bound-pod-finalization",
                LogApplyMutation::FinalizeBoundPod(LogApplyPodActorFinalization {
                    namespace: "default".into(),
                    name: "pod-a".into(),
                    pod_uid: "uid-a".into(),
                    node_name: "worker-a".into(),
                }),
            ),
            (
                "namespace",
                LogApplyMutation::DeleteNamespace { name: "ns".into() },
            ),
            (
                "watch-history",
                LogApplyMutation::GcWatchEvents {
                    max_rows: 100,
                    batch_cap: 10,
                },
            ),
            (
                "network",
                LogApplyMutation::AllocateNodeSubnet(LogApplyNodeSubnetAllocation {
                    node_name: "node-a".into(),
                    cluster_cidr: "10.42.0.0/16".into(),
                    node_ip: "192.0.2.1".into(),
                }),
            ),
            (
                "outbox-ledger",
                LogApplyMutation::DeleteAppliedOutbox {
                    idempotency_key: "key".into(),
                },
            ),
            (
                "cluster-meta",
                LogApplyMutation::PutKlightsMeta {
                    key: "mode".into(),
                    value: "v1".into(),
                },
            ),
            (
                "pod-cleanup",
                LogApplyMutation::DeletePodCleanupIntent(LogApplyPodCleanupIntentKey {
                    node_name: "node-a".into(),
                    namespace: "default".into(),
                    pod_name: "pod-a".into(),
                    pod_uid: "uid-a".into(),
                    reason: "NodeLost".into(),
                }),
            ),
        ]
    }

    #[test]
    fn versioned_cluster_mutation_families_round_trip() {
        for (label, mutation) in family_samples() {
            let family: ClusterMutation = mutation.clone().into();
            let versioned = VersionedClusterMutation::new(family.clone());
            let json = serde_json::to_vec(&versioned).unwrap();
            let decoded: VersionedClusterMutation = serde_json::from_slice(&json).unwrap();
            assert_eq!(decoded, versioned, "{label}: JSON round trip");
            assert_eq!(
                LogApplyMutation::try_from(decoded).unwrap(),
                mutation,
                "{label}: family conversion"
            );
            assert_eq!(family.into_log_apply_mutation(), mutation, "{label}");
        }

        let family: ClusterMutation = family_samples().remove(0).1.into();
        let error = LogApplyMutation::try_from(VersionedClusterMutation {
            version: 999,
            mutation: family,
        })
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "unsupported ClusterMutation version 999 (current 1)"
        );
    }
}
