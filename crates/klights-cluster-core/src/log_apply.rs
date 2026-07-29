//! Pure logical commit envelope and deterministic outbox decisions.
//!
//! These values describe the mutation submitted to committed cluster-state
//! apply. Generated wire messages, durable store DTOs, SQL queries/upserts,
//! public resource-version allocation, and runtime orchestration remain adapter
//! concerns outside this crate.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{PatchKind, Resource};

/// Stable operation label carried by durable actor-owned Pod finalization.
pub const POD_METADATA_OPERATION: &str = "PodMetadata";

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

/// One logical live cluster-state commit.
///
/// This is always a `CommittedApplyV1` template: every public
/// resourceVersion field is zero until the committed persistence transaction
/// allocates it exactly once.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct LogApplyCommit {
    resource_version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outbox_watermark: Option<OutboxStreamWatermark>,
    mutations: Vec<LogApplyMutation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SerializedLogApplyCommit {
    resource_version: i64,
    #[serde(default)]
    outbox_watermark: Option<OutboxStreamWatermark>,
    mutations: Vec<LogApplyMutation>,
}

impl<'de> Deserialize<'de> for LogApplyCommit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = SerializedLogApplyCommit::deserialize(deserializer)?;
        let commit = Self {
            resource_version: wire.resource_version,
            outbox_watermark: wire.outbox_watermark,
            mutations: wire.mutations,
        };
        commit
            .validate_live_template()
            .map_err(serde::de::Error::custom)?;
        Ok(commit)
    }
}

/// Operation for restoring exact historical state from an authoritative
/// snapshot.
///
/// This value is deliberately not serializable or versioned. Snapshot
/// transport carries its own bytes; the restore adapter constructs this
/// operation only at the typed snapshot-install boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotRestoreOperation {
    resource_version: i64,
    outbox_watermark: Option<OutboxStreamWatermark>,
    mutations: Vec<LogApplyMutation>,
}

/// Build the exact authoritative restore operation for one live resource.
///
/// Both persistent backends use this canonical conversion so Namespace table
/// identity and ordinary resource row fields cannot drift across snapshots.
pub fn resource_snapshot_restore_operation(resource: &Resource) -> SnapshotRestoreOperation {
    let mutation = if resource.api_version == "v1"
        && resource.kind == "Namespace"
        && resource.namespace.is_none()
    {
        ClusterMutation::Namespace(NamespaceMutation::PutNamespace(LogApplyNamespaceRow {
            name: resource.name.clone(),
            uid: resource.uid.clone(),
            resource_version: resource.resource_version,
            data: (*resource.data).clone(),
        }))
    } else {
        ClusterMutation::Resource(ResourceMutation::PutResource(LogApplyResourceRow {
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
        }))
    };
    SnapshotRestoreOperation::new(
        resource.resource_version,
        None,
        vec![mutation.into_log_apply_mutation()],
    )
}

/// A live committed-apply template contains a pre-assigned public RV.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveCommitResourceVersionError {
    pub field: &'static str,
    pub actual: String,
}

impl fmt::Display for LiveCommitResourceVersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "live committed-apply template field {} requires resourceVersion 0, got {}",
            self.field, self.actual
        )
    }
}

impl std::error::Error for LiveCommitResourceVersionError {}

/// Monotonic identity of one node-outbox stream position.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxStreamWatermark {
    pub client_id: String,
    pub stream_id: i64,
    pub stream_seq: i64,
}

impl LogApplyCommit {
    /// Construct one fixed committed-apply template, rejecting any mutation
    /// that already carries a public RV.
    pub fn try_new(
        mutations: Vec<LogApplyMutation>,
    ) -> Result<Self, LiveCommitResourceVersionError> {
        Self::try_new_with_watermark(mutations, None)
    }

    pub fn try_new_with_watermark(
        mutations: Vec<LogApplyMutation>,
        outbox_watermark: Option<OutboxStreamWatermark>,
    ) -> Result<Self, LiveCommitResourceVersionError> {
        let commit = Self {
            resource_version: 0,
            outbox_watermark,
            mutations,
        };
        commit.validate_live_template()?;
        Ok(commit)
    }

    fn authored(mutations: Vec<LogApplyMutation>) -> Self {
        Self::try_new(mutations).expect("live commit builders must author only RV-zero templates")
    }

    pub fn try_from_cluster_mutations(
        mutations: Vec<ClusterMutation>,
    ) -> Result<Self, LiveCommitResourceVersionError> {
        Self::try_new(
            mutations
                .into_iter()
                .map(ClusterMutation::into_log_apply_mutation)
                .collect(),
        )
    }

    pub fn put_resource(resource: &Resource) -> Self {
        let mut data = (*resource.data).clone();
        clear_metadata_resource_version(&mut data);
        Self::authored(vec![LogApplyMutation::PutResource(LogApplyResourceRow {
            api_version: resource.api_version.clone(),
            kind: resource.kind.clone(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
            uid: resource.uid.clone(),
            resource_version: 0,
            data,
            require_absent: false,
            require_existing: false,
            precondition_uid: None,
            precondition_resource_version: None,
            status_only: false,
        })])
    }

    pub fn delete_resource(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<String>,
        name: impl Into<String>,
        uid: impl Into<String>,
    ) -> Self {
        Self::authored(vec![LogApplyMutation::DeleteResource(
            LogApplyResourceKey {
                api_version: api_version.into(),
                kind: kind.into(),
                namespace,
                name: name.into(),
                uid: uid.into(),
                precondition_resource_version: None,
            },
        )])
    }

    pub fn put_namespace(resource: &Resource) -> Self {
        let mut data = (*resource.data).clone();
        clear_metadata_resource_version(&mut data);
        Self::authored(vec![LogApplyMutation::PutNamespace(LogApplyNamespaceRow {
            name: resource.name.clone(),
            uid: resource.uid.clone(),
            resource_version: 0,
            data,
        })])
    }

    pub fn delete_namespace(name: impl Into<String>) -> Self {
        Self::authored(vec![LogApplyMutation::DeleteNamespace {
            name: name.into(),
        }])
    }

    pub fn delete_namespace_contents(name: impl Into<String>) -> Self {
        Self::authored(vec![LogApplyMutation::DeleteNamespaceContents {
            name: name.into(),
        }])
    }

    pub fn put_node_subnet_row(row: LogApplyNodeSubnetRow) -> Self {
        Self::authored(vec![LogApplyMutation::PutNodeSubnet(row)])
    }

    pub fn delete_node_subnet(node_name: impl Into<String>) -> Self {
        Self::authored(vec![LogApplyMutation::DeleteNodeSubnet {
            node_name: node_name.into(),
        }])
    }

    pub fn put_node_dataplane_row(row: LogApplyNodeDataplaneRow) -> Self {
        Self::authored(vec![LogApplyMutation::PutNodeDataplane(row)])
    }

    pub fn delete_node_dataplane(node_name: impl Into<String>) -> Self {
        Self::authored(vec![LogApplyMutation::DeleteNodeDataplane {
            node_name: node_name.into(),
        }])
    }

    pub fn advance_resource_version() -> Self {
        Self::authored(vec![LogApplyMutation::AdvanceResourceVersion {
            resource_version: 0,
        }])
    }

    pub fn put_applied_outbox_row(mut row: LogApplyAppliedOutboxRow) -> Self {
        row.applied_rv = None;
        Self::authored(vec![LogApplyMutation::PutAppliedOutbox(row)])
    }

    pub fn put_watch_event(mut row: LogApplyWatchEventRow) -> Self {
        row.resource_version = 0;
        clear_metadata_resource_version(&mut row.data);
        if let Some(object) = row.data.get_mut("object") {
            clear_metadata_resource_version(object);
        }
        Self::authored(vec![LogApplyMutation::PutWatchEvent(row)])
    }

    pub fn gc_applied_outbox(cutoff_ms: i64, operations: Vec<String>) -> Self {
        Self::authored(vec![LogApplyMutation::GcAppliedOutbox {
            cutoff_ms,
            operations,
        }])
    }

    pub fn put_pod_cleanup_intent_row(mut row: LogApplyPodCleanupIntentRow) -> Self {
        row.resource_version = 0;
        Self::authored(vec![LogApplyMutation::PutPodCleanupIntent(row)])
    }

    /// Validate that no public RV was assigned before committed persistence.
    pub fn validate_live_template(&self) -> Result<(), LiveCommitResourceVersionError> {
        validate_zero("commit.resource_version", self.resource_version)?;
        for mutation in &self.mutations {
            let field_and_value = match mutation {
                LogApplyMutation::PutResource(row) => {
                    validate_json_resource_version(
                        "put_resource.data.metadata.resourceVersion",
                        row.data.pointer("/metadata/resourceVersion"),
                    )?;
                    Some(("put_resource.resource_version", row.resource_version))
                }
                LogApplyMutation::PatchResourceLatest(row) => {
                    validate_json_resource_version(
                        "patch_resource_latest.patch.metadata.resourceVersion",
                        row.patch.pointer("/metadata/resourceVersion"),
                    )?;
                    Some((
                        "patch_resource_latest.resource_version",
                        row.resource_version,
                    ))
                }
                LogApplyMutation::PutNamespace(row) => {
                    validate_json_resource_version(
                        "put_namespace.data.metadata.resourceVersion",
                        row.data.pointer("/metadata/resourceVersion"),
                    )?;
                    Some(("put_namespace.resource_version", row.resource_version))
                }
                LogApplyMutation::PutWatchEvent(row) => {
                    validate_json_resource_version(
                        "put_watch_event.data.metadata.resourceVersion",
                        row.data.pointer("/metadata/resourceVersion"),
                    )?;
                    validate_json_resource_version(
                        "put_watch_event.data.object.metadata.resourceVersion",
                        row.data.pointer("/object/metadata/resourceVersion"),
                    )?;
                    Some(("put_watch_event.resource_version", row.resource_version))
                }
                LogApplyMutation::PutPodCleanupIntent(row) => Some((
                    "put_pod_cleanup_intent.resource_version",
                    row.resource_version,
                )),
                LogApplyMutation::PutAppliedOutbox(row) => row
                    .applied_rv
                    .map(|value| ("put_applied_outbox.applied_rv", value)),
                LogApplyMutation::AdvanceResourceVersion { resource_version } => Some((
                    "advance_resource_version.resource_version",
                    *resource_version,
                )),
                _ => None,
            };
            if let Some((field, value)) = field_and_value {
                validate_zero(field, value)?;
            }
        }
        Ok(())
    }

    pub const fn resource_version(&self) -> i64 {
        self.resource_version
    }

    pub const fn outbox_watermark(&self) -> Option<&OutboxStreamWatermark> {
        self.outbox_watermark.as_ref()
    }

    pub fn mutations(&self) -> &[LogApplyMutation] {
        &self.mutations
    }

    pub fn into_parts(self) -> (i64, Option<OutboxStreamWatermark>, Vec<LogApplyMutation>) {
        (self.resource_version, self.outbox_watermark, self.mutations)
    }
}

fn validate_zero(field: &'static str, actual: i64) -> Result<(), LiveCommitResourceVersionError> {
    if actual == 0 {
        Ok(())
    } else {
        Err(LiveCommitResourceVersionError {
            field,
            actual: actual.to_string(),
        })
    }
}

fn validate_json_resource_version(
    field: &'static str,
    value: Option<&serde_json::Value>,
) -> Result<(), LiveCommitResourceVersionError> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(()),
        Some(serde_json::Value::Number(value)) if value.as_i64() == Some(0) => Ok(()),
        Some(serde_json::Value::String(value)) if value == "0" => Ok(()),
        Some(serde_json::Value::String(value)) => Err(LiveCommitResourceVersionError {
            field,
            actual: value.clone(),
        }),
        Some(value) => Err(LiveCommitResourceVersionError {
            field,
            actual: value.to_string(),
        }),
    }
}

fn clear_metadata_resource_version(data: &mut serde_json::Value) {
    if let Some(metadata) = data
        .get_mut("metadata")
        .and_then(serde_json::Value::as_object_mut)
    {
        metadata.remove("resourceVersion");
    }
}

impl SnapshotRestoreOperation {
    pub fn new(
        resource_version: i64,
        outbox_watermark: Option<OutboxStreamWatermark>,
        mutations: Vec<LogApplyMutation>,
    ) -> Self {
        Self {
            resource_version,
            outbox_watermark,
            mutations,
        }
    }

    pub const fn resource_version(&self) -> i64 {
        self.resource_version
    }

    pub const fn outbox_watermark(&self) -> Option<&OutboxStreamWatermark> {
        self.outbox_watermark.as_ref()
    }

    pub fn mutations(&self) -> &[LogApplyMutation] {
        &self.mutations
    }

    pub fn into_parts(self) -> (i64, Option<OutboxStreamWatermark>, Vec<LogApplyMutation>) {
        (self.resource_version, self.outbox_watermark, self.mutations)
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
    fn live_template_and_snapshot_restore_are_distinct_contracts() {
        let decoded: LogApplyCommit =
            serde_json::from_str(r#"{"resource_version":0,"mutations":[]}"#).unwrap();
        assert!(decoded.validate_live_template().is_ok());
        assert!(
            !serde_json::to_string(&decoded)
                .unwrap()
                .contains("assignment")
        );
        assert!(
            serde_json::from_str::<LogApplyCommit>(
                r#"{"resource_version":0,"resource_version_assignment":"LegacyLeaderAssigned","mutations":[]}"#
            )
            .is_err(),
            "a removed assignment profile must not be silently accepted"
        );

        let invalid_live = LogApplyCommit {
            resource_version: 9,
            outbox_watermark: None,
            mutations: Vec::new(),
        };
        assert_eq!(
            invalid_live.validate_live_template(),
            Err(LiveCommitResourceVersionError {
                field: "commit.resource_version",
                actual: "9".into(),
            })
        );

        let restore = SnapshotRestoreOperation::new(9, None, Vec::new());
        assert_eq!(restore.resource_version(), 9);

        let historical_data = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "historical",
                "namespace": "default",
                "uid": "historical-uid",
                "resourceVersion": "9"
            },
            "data": {"bytes": "must-remain-exact"}
        });
        let historical_mutation = LogApplyMutation::PutResource(LogApplyResourceRow {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "historical".into(),
            uid: "historical-uid".into(),
            resource_version: 9,
            data: historical_data.clone(),
            require_absent: false,
            require_existing: false,
            precondition_uid: None,
            precondition_resource_version: None,
            status_only: false,
        });
        assert!(
            LogApplyCommit::try_new(vec![historical_mutation.clone()]).is_err(),
            "historical public RVs must never enter the live template type"
        );
        let restore = SnapshotRestoreOperation::new(9, None, vec![historical_mutation]);
        let LogApplyMutation::PutResource(restored) = &restore.mutations()[0] else {
            panic!("snapshot operation changed mutation family")
        };
        assert_eq!(restored.resource_version, 9);
        assert_eq!(restored.data, historical_data);
    }

    #[test]
    fn fixed_live_constructor_rejects_every_preassigned_rv_field() {
        let cases = vec![
            (
                "put_resource.resource_version",
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
            ),
            (
                "patch_resource_latest.resource_version",
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
            ),
            (
                "put_namespace.resource_version",
                LogApplyMutation::PutNamespace(LogApplyNamespaceRow {
                    name: "ns".into(),
                    uid: "ns-uid".into(),
                    resource_version: 73,
                    data: json!({}),
                }),
            ),
            (
                "put_watch_event.resource_version",
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
            ),
            (
                "put_pod_cleanup_intent.resource_version",
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
            ),
            (
                "put_applied_outbox.applied_rv",
                outbox_row("PodStatus", "subject", Some(8)),
            ),
            (
                "advance_resource_version.resource_version",
                LogApplyMutation::AdvanceResourceVersion {
                    resource_version: 73,
                },
            ),
        ];
        for (expected_field, mutation) in cases {
            assert_eq!(
                LogApplyCommit::try_new(vec![mutation]),
                Err(LiveCommitResourceVersionError {
                    field: expected_field,
                    actual: if expected_field == "put_applied_outbox.applied_rv" {
                        19
                    } else {
                        73
                    }
                    .to_string(),
                }),
                "{expected_field}"
            );
        }

        let commit = LogApplyCommit::try_new(vec![LogApplyMutation::DeleteNamespace {
            name: "ns".into(),
        }])
        .unwrap();
        assert_eq!(commit.resource_version(), 0);
        assert!(commit.validate_live_template().is_ok());
    }

    #[test]
    fn fixed_live_constructor_rejects_nested_public_resource_versions() {
        let cases = vec![
            (
                "put_resource.data.metadata.resourceVersion",
                41,
                LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".into(),
                    kind: "ConfigMap".into(),
                    namespace: Some("default".into()),
                    name: "item".into(),
                    uid: "item-uid".into(),
                    resource_version: 0,
                    data: json!({"metadata": {"resourceVersion": "41"}}),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                }),
            ),
            (
                "patch_resource_latest.patch.metadata.resourceVersion",
                42,
                LogApplyMutation::PatchResourceLatest(LogApplyResourcePatch {
                    api_version: "v1".into(),
                    kind: "ConfigMap".into(),
                    namespace: Some("default".into()),
                    name: "item".into(),
                    resource_version: 0,
                    patch_kind: PatchKind::Merge,
                    patch: json!({"metadata": {"resourceVersion": 42}}),
                    require_existing: true,
                    precondition_uid: Some("item-uid".into()),
                    precondition_resource_version: Some(40),
                    terminating_pod_unready_timestamp: None,
                }),
            ),
            (
                "put_namespace.data.metadata.resourceVersion",
                43,
                LogApplyMutation::PutNamespace(LogApplyNamespaceRow {
                    name: "ns".into(),
                    uid: "ns-uid".into(),
                    resource_version: 0,
                    data: json!({"metadata": {"resourceVersion": "43"}}),
                }),
            ),
            (
                "put_watch_event.data.metadata.resourceVersion",
                44,
                LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                    event_id: None,
                    api_version: "v1".into(),
                    kind: "ConfigMap".into(),
                    namespace: Some("default".into()),
                    name: "item".into(),
                    resource_version: 0,
                    event_type: "MODIFIED".into(),
                    data: json!({"metadata": {"resourceVersion": "44"}}),
                }),
            ),
            (
                "put_watch_event.data.object.metadata.resourceVersion",
                45,
                LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                    event_id: None,
                    api_version: "v1".into(),
                    kind: "ConfigMap".into(),
                    namespace: Some("default".into()),
                    name: "item".into(),
                    resource_version: 0,
                    event_type: "MODIFIED".into(),
                    data: json!({
                        "type": "MODIFIED",
                        "object": {"metadata": {"resourceVersion": "45"}}
                    }),
                }),
            ),
        ];

        for (expected_field, actual, mutation) in cases {
            assert_eq!(
                LogApplyCommit::try_new(vec![mutation]),
                Err(LiveCommitResourceVersionError {
                    field: expected_field,
                    actual: actual.to_string(),
                }),
                "{expected_field}"
            );
        }

        let zero_nested =
            LogApplyCommit::try_new(vec![LogApplyMutation::PutNamespace(LogApplyNamespaceRow {
                name: "zero".into(),
                uid: "zero-uid".into(),
                resource_version: 0,
                data: json!({"metadata": {"resourceVersion": "0"}}),
            })])
            .expect("an explicit nested zero remains an RV-zero template");
        let mut encoded = serde_json::to_value(zero_nested).unwrap();
        *encoded
            .pointer_mut("/mutations/0/PutNamespace/data/metadata/resourceVersion")
            .expect("serialized namespace RV") = json!("opaque-rv");
        let error = serde_json::from_value::<LogApplyCommit>(encoded)
            .expect_err("JSON decode must validate nested public RVs");
        assert!(
            error
                .to_string()
                .contains("put_namespace.data.metadata.resourceVersion"),
            "{error}"
        );
    }

    #[test]
    fn fixed_live_builders_clear_observed_nested_resource_versions() {
        let resource = Resource {
            id: 1,
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "item".into(),
            uid: "item-uid".into(),
            resource_version: 51,
            data: std::sync::Arc::new(json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "item",
                    "namespace": "default",
                    "uid": "item-uid",
                    "resourceVersion": "51"
                }
            })),
        };
        let resource_commit = LogApplyCommit::put_resource(&resource);
        let LogApplyMutation::PutResource(row) = &resource_commit.mutations()[0] else {
            panic!("resource builder emitted the wrong mutation")
        };
        assert!(row.data.pointer("/metadata/resourceVersion").is_none());
        assert_eq!(
            resource
                .data
                .pointer("/metadata/resourceVersion")
                .and_then(serde_json::Value::as_str),
            Some("51"),
            "live authoring must not mutate the caller's shared resource"
        );

        let namespace_commit = LogApplyCommit::put_namespace(&Resource {
            namespace: None,
            kind: "Namespace".into(),
            name: "ns".into(),
            uid: "ns-uid".into(),
            resource_version: 52,
            data: std::sync::Arc::new(json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": "ns",
                    "uid": "ns-uid",
                    "resourceVersion": "52"
                }
            })),
            ..resource.clone()
        });
        let LogApplyMutation::PutNamespace(row) = &namespace_commit.mutations()[0] else {
            panic!("namespace builder emitted the wrong mutation")
        };
        assert!(row.data.pointer("/metadata/resourceVersion").is_none());

        let watch_commit = LogApplyCommit::put_watch_event(LogApplyWatchEventRow {
            event_id: None,
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "item".into(),
            resource_version: 53,
            event_type: "MODIFIED".into(),
            data: json!({
                "type": "MODIFIED",
                "object": {"metadata": {"resourceVersion": "53"}}
            }),
        });
        let LogApplyMutation::PutWatchEvent(row) = &watch_commit.mutations()[0] else {
            panic!("watch builder emitted the wrong mutation")
        };
        assert!(
            row.data
                .pointer("/object/metadata/resourceVersion")
                .is_none()
        );
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
                stamped_pod_status_subject_and_stamp(&LogApplyCommit {
                    resource_version: 0,
                    outbox_watermark: None,
                    mutations: vec![invalid],
                }),
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
