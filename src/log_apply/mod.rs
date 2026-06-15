//! Ordered log-apply core shared by promotable replicas now and Raft learners later.
//!
//! The core is deliberately port-based: sources provide durable ordered
//! entries, targets install snapshots and apply entries, and the follower
//! orchestration contains no SQLite or gRPC assumptions.

use anyhow::Result;
use prost::Message;
use serde::{Deserialize, Serialize};

use crate::datastore::types::{
    AppliedOutboxRecord, NodeSubnet, PatchKind, PodCleanupIntent, Resource,
};
use crate::networking::wireguard::DataplanePeerMetadata;

// T3: `KEY_LAST_APPLIED_INDEX`, `KEY_LAST_APPLIED_RV` removed —
// the `log_apply_entries` table and its checkpoint are gone.

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogApplyCommit {
    pub resource_version: i64,
    pub mutations: Vec<LogApplyMutation>,
}

impl LogApplyCommit {
    pub fn new(resource_version: i64, mutations: Vec<LogApplyMutation>) -> Self {
        Self {
            resource_version,
            mutations,
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

    pub fn put_node_subnet(resource_version: i64, row: &NodeSubnet) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::PutNodeSubnet(LogApplyNodeSubnetRow {
                node_name: row.node_name.as_str().to_string(),
                subnet: row.subnet.to_string(),
                subnet_base_int: row.subnet_base_int,
                vtep_ip: row.vtep_ip.to_string(),
                vtep_mac: row.vtep_mac.as_ref().map(|mac| mac.to_string()),
                node_ip: row.node_ip.to_string(),
                mode: match row.mode {
                    crate::controllers::annotations::NodePeerMode::Root => "root".to_string(),
                    crate::controllers::annotations::NodePeerMode::Rootless => {
                        "rootless".to_string()
                    }
                },
                hostport_range: row.hostport_range.as_ref().map(|range| range.to_string()),
            })],
        )
    }

    pub fn delete_node_subnet(resource_version: i64, node_name: impl Into<String>) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::DeleteNodeSubnet {
                node_name: node_name.into(),
            }],
        )
    }

    pub fn put_node_dataplane(resource_version: i64, row: &DataplanePeerMetadata) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::PutNodeDataplane(
                LogApplyNodeDataplaneRow {
                    node_name: row.node_name.clone(),
                    mode: row.mode.as_str().to_string(),
                    encryption: row.encryption.as_str().to_string(),
                    public_key: row.public_key.as_ref().map(|key| key.to_string()),
                    endpoint: row.endpoint.to_string(),
                    port: row.port,
                },
            )],
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

    pub fn put_applied_outbox(resource_version: i64, record: AppliedOutboxRecord) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::PutAppliedOutbox(record.into())],
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

    pub fn put_pod_cleanup_intent(resource_version: i64, row: PodCleanupIntent) -> Self {
        Self::new(
            resource_version,
            vec![LogApplyMutation::PutPodCleanupIntent(row.into())],
        )
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum LogApplyMutation {
    PutResource(LogApplyResourceRow),
    PatchResourceLatest(LogApplyResourcePatch),
    DeleteResource(LogApplyResourceKey),
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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogApplyResourceKey {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    /// UID captured by the leader at delete-time inside the same outbox
    /// transaction. When non-empty, `apply_commit_in_tx` enforces
    /// `WHERE api_version/kind/namespace/name/uid = ?` so a stale delete
    /// for an older UID is a no-op against a same-name replacement.
    /// Empty UID is permitted only by snapshot/backfill paths that
    /// reconstruct deletes from watch history without UID context.
    #[serde(default)]
    pub uid: String,
    #[serde(default)]
    pub precondition_resource_version: Option<i64>,
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
    pub vtep_ip: String,
    pub vtep_mac: Option<String>,
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
    /// Worker-observed monotonic stamp of the Pod status snapshot this ledger
    /// row recorded (`None` for non-status operations). Replicated so any raft
    /// member that becomes leader can keep gating stale status snapshots.
    #[serde(default)]
    pub status_stamp: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogApplyWatchEventRow {
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

impl From<PodCleanupIntent> for LogApplyPodCleanupIntentRow {
    fn from(row: PodCleanupIntent) -> Self {
        Self {
            node_name: row.node_name,
            namespace: row.namespace,
            pod_name: row.pod_name,
            pod_uid: row.pod_uid,
            reason: row.reason,
            resource_version: row.resource_version,
            created_at_ms: row.created_at_ms,
            pod_data: row.pod_data,
        }
    }
}

impl From<LogApplyPodCleanupIntentRow> for PodCleanupIntent {
    fn from(row: LogApplyPodCleanupIntentRow) -> Self {
        Self {
            node_name: row.node_name,
            namespace: row.namespace,
            pod_name: row.pod_name,
            pod_uid: row.pod_uid,
            reason: row.reason,
            resource_version: row.resource_version,
            created_at_ms: row.created_at_ms,
            pod_data: row.pod_data,
        }
    }
}

impl From<AppliedOutboxRecord> for LogApplyAppliedOutboxRow {
    fn from(record: AppliedOutboxRecord) -> Self {
        Self {
            idempotency_key: record.idempotency_key,
            subject_key: record.subject_key,
            operation: record.operation,
            first_seen_ms: record.first_seen_ms,
            applied_rv: record.applied_rv,
            result_proto: record.result_proto,
            // AppliedOutboxRecord is the read/snapshot view and does not carry
            // the status stamp; snapshot restore falls back to no gate until
            // fresh status snapshots re-establish stamps.
            status_stamp: None,
        }
    }
}

impl From<LogApplyAppliedOutboxRow> for AppliedOutboxRecord {
    fn from(row: LogApplyAppliedOutboxRow) -> Self {
        Self {
            idempotency_key: row.idempotency_key,
            subject_key: row.subject_key,
            operation: row.operation,
            first_seen_ms: row.first_seen_ms,
            applied_rv: row.applied_rv,
            result_proto: row.result_proto,
        }
    }
}

pub fn encode_commit_json(commit: &LogApplyCommit) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(commit)?)
}

pub fn decode_commit_json(bytes: &[u8]) -> Result<LogApplyCommit> {
    Ok(serde_json::from_slice(bytes)?)
}

pub fn encode_commit_protobuf(commit: &LogApplyCommit) -> Result<Vec<u8>> {
    let proto = ProtoLogApplyCommit::from(commit.clone());
    Ok(proto.encode_to_vec())
}

pub fn decode_commit_protobuf(bytes: &[u8]) -> Result<LogApplyCommit> {
    let proto = ProtoLogApplyCommit::decode(bytes)?;
    proto.try_into()
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyCommit {
    #[prost(int64, tag = "1")]
    resource_version: i64,
    #[prost(message, repeated, tag = "2")]
    mutations: Vec<ProtoLogApplyMutation>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyMutation {
    #[prost(
        oneof = "proto_log_apply_mutation::Mutation",
        tags = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20"
    )]
    mutation: Option<proto_log_apply_mutation::Mutation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, prost::Enumeration)]
enum ProtoLogApplyPatchKind {
    Merge = 0,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyKlightsMeta {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyNodeSubnetAllocation {
    #[prost(string, tag = "1")]
    node_name: String,
    #[prost(string, tag = "2")]
    cluster_cidr: String,
    #[prost(string, tag = "3")]
    node_ip: String,
}

mod proto_log_apply_mutation {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Mutation {
        #[prost(message, tag = "1")]
        PutResource(super::ProtoLogApplyResourceRow),
        #[prost(message, tag = "2")]
        DeleteResource(super::ProtoLogApplyResourceKey),
        #[prost(message, tag = "3")]
        PutNamespace(super::ProtoLogApplyNamespaceRow),
        #[prost(string, tag = "4")]
        DeleteNamespace(String),
        #[prost(string, tag = "5")]
        DeleteNamespaceContents(String),
        #[prost(message, tag = "6")]
        PutNodeSubnet(super::ProtoLogApplyNodeSubnetRow),
        #[prost(string, tag = "7")]
        DeleteNodeSubnet(String),
        #[prost(message, tag = "8")]
        PutNodeDataplane(super::ProtoLogApplyNodeDataplaneRow),
        #[prost(string, tag = "9")]
        DeleteNodeDataplane(String),
        #[prost(message, tag = "10")]
        PutAppliedOutbox(super::ProtoLogApplyAppliedOutboxRow),
        #[prost(string, tag = "11")]
        DeleteAppliedOutbox(String),
        #[prost(int64, tag = "12")]
        AdvanceResourceVersion(i64),
        #[prost(message, tag = "13")]
        GcAppliedOutbox(super::ProtoLogApplyAppliedOutboxGc),
        #[prost(message, tag = "14")]
        PutWatchEvent(super::ProtoLogApplyWatchEventRow),
        #[prost(message, tag = "15")]
        PutKlightsMeta(super::ProtoLogApplyKlightsMeta),
        #[prost(message, tag = "16")]
        PutPodCleanupIntent(super::ProtoLogApplyPodCleanupIntentRow),
        #[prost(message, tag = "17")]
        DeletePodCleanupIntent(super::ProtoLogApplyPodCleanupIntentKey),
        #[prost(string, tag = "18")]
        DeletePodCleanupIntentsForNode(String),
        #[prost(message, tag = "19")]
        AllocateNodeSubnet(super::ProtoLogApplyNodeSubnetAllocation),
        #[prost(message, tag = "20")]
        PatchResourceLatest(super::ProtoLogApplyResourcePatch),
    }
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyResourcePatch {
    #[prost(string, tag = "1")]
    api_version: String,
    #[prost(string, tag = "2")]
    kind: String,
    #[prost(string, optional, tag = "3")]
    namespace: Option<String>,
    #[prost(string, tag = "4")]
    name: String,
    #[prost(int64, tag = "5")]
    resource_version: i64,
    #[prost(enumeration = "ProtoLogApplyPatchKind", tag = "6")]
    patch_kind: i32,
    #[prost(bytes = "vec", tag = "7")]
    patch_json: Vec<u8>,
    #[prost(bool, tag = "8")]
    require_existing: bool,
    #[prost(string, optional, tag = "9")]
    precondition_uid: Option<String>,
    #[prost(int64, optional, tag = "10")]
    precondition_resource_version: Option<i64>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyResourceRow {
    #[prost(string, tag = "1")]
    api_version: String,
    #[prost(string, tag = "2")]
    kind: String,
    #[prost(string, optional, tag = "3")]
    namespace: Option<String>,
    #[prost(string, tag = "4")]
    name: String,
    #[prost(string, tag = "5")]
    uid: String,
    #[prost(int64, tag = "6")]
    resource_version: i64,
    #[prost(bytes = "vec", tag = "7")]
    data_json: Vec<u8>,
    #[prost(bool, tag = "8")]
    require_absent: bool,
    #[prost(bool, tag = "9")]
    require_existing: bool,
    #[prost(string, optional, tag = "10")]
    precondition_uid: Option<String>,
    #[prost(int64, optional, tag = "11")]
    precondition_resource_version: Option<i64>,
    #[prost(bool, tag = "12")]
    status_only: bool,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyResourceKey {
    #[prost(string, tag = "1")]
    api_version: String,
    #[prost(string, tag = "2")]
    kind: String,
    #[prost(string, optional, tag = "3")]
    namespace: Option<String>,
    #[prost(string, tag = "4")]
    name: String,
    #[prost(string, tag = "5")]
    uid: String,
    #[prost(int64, optional, tag = "6")]
    precondition_resource_version: Option<i64>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyNamespaceRow {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    uid: String,
    #[prost(int64, tag = "3")]
    resource_version: i64,
    #[prost(bytes = "vec", tag = "4")]
    data_json: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyNodeSubnetRow {
    #[prost(string, tag = "1")]
    node_name: String,
    #[prost(string, tag = "2")]
    subnet: String,
    #[prost(uint32, tag = "3")]
    subnet_base_int: u32,
    #[prost(string, tag = "4")]
    vtep_ip: String,
    #[prost(string, optional, tag = "5")]
    vtep_mac: Option<String>,
    #[prost(string, tag = "6")]
    node_ip: String,
    #[prost(string, tag = "7")]
    mode: String,
    #[prost(string, optional, tag = "8")]
    hostport_range: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyNodeDataplaneRow {
    #[prost(string, tag = "1")]
    node_name: String,
    #[prost(string, tag = "2")]
    mode: String,
    #[prost(string, tag = "3")]
    encryption: String,
    #[prost(string, optional, tag = "4")]
    public_key: Option<String>,
    #[prost(string, tag = "5")]
    endpoint: String,
    #[prost(uint32, optional, tag = "6")]
    port: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyAppliedOutboxRow {
    #[prost(string, tag = "1")]
    idempotency_key: String,
    #[prost(string, tag = "2")]
    subject_key: String,
    #[prost(string, tag = "3")]
    operation: String,
    #[prost(int64, tag = "4")]
    first_seen_ms: i64,
    #[prost(int64, optional, tag = "5")]
    applied_rv: Option<i64>,
    #[prost(bytes = "vec", tag = "6")]
    result_proto: Vec<u8>,
    #[prost(int64, optional, tag = "7")]
    status_stamp: Option<i64>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyAppliedOutboxGc {
    #[prost(int64, tag = "1")]
    cutoff_ms: i64,
    #[prost(string, repeated, tag = "2")]
    operations: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyWatchEventRow {
    #[prost(string, tag = "1")]
    api_version: String,
    #[prost(string, tag = "2")]
    kind: String,
    #[prost(string, optional, tag = "3")]
    namespace: Option<String>,
    #[prost(string, tag = "4")]
    name: String,
    #[prost(int64, tag = "5")]
    resource_version: i64,
    #[prost(string, tag = "6")]
    event_type: String,
    #[prost(bytes = "vec", tag = "7")]
    data_json: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyPodCleanupIntentRow {
    #[prost(string, tag = "1")]
    node_name: String,
    #[prost(string, tag = "2")]
    namespace: String,
    #[prost(string, tag = "3")]
    pod_name: String,
    #[prost(string, tag = "4")]
    pod_uid: String,
    #[prost(string, tag = "5")]
    reason: String,
    #[prost(int64, tag = "6")]
    resource_version: i64,
    #[prost(int64, tag = "7")]
    created_at_ms: i64,
    #[prost(bytes = "vec", tag = "8")]
    pod_data_json: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoLogApplyPodCleanupIntentKey {
    #[prost(string, tag = "1")]
    node_name: String,
    #[prost(string, tag = "2")]
    namespace: String,
    #[prost(string, tag = "3")]
    pod_name: String,
    #[prost(string, tag = "4")]
    pod_uid: String,
    #[prost(string, tag = "5")]
    reason: String,
}

impl From<LogApplyCommit> for ProtoLogApplyCommit {
    fn from(commit: LogApplyCommit) -> Self {
        Self {
            resource_version: commit.resource_version,
            mutations: commit.mutations.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<ProtoLogApplyCommit> for LogApplyCommit {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoLogApplyCommit) -> Result<Self> {
        Ok(Self {
            resource_version: proto.resource_version,
            mutations: proto
                .mutations
                .into_iter()
                .map(LogApplyMutation::try_from)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl From<LogApplyMutation> for ProtoLogApplyMutation {
    fn from(mutation: LogApplyMutation) -> Self {
        use proto_log_apply_mutation::Mutation;
        let mutation = match mutation {
            LogApplyMutation::PutResource(row) => Mutation::PutResource(row.into()),
            LogApplyMutation::PatchResourceLatest(patch) => {
                Mutation::PatchResourceLatest(patch.into())
            }
            LogApplyMutation::DeleteResource(key) => Mutation::DeleteResource(key.into()),
            LogApplyMutation::PutNamespace(row) => Mutation::PutNamespace(row.into()),
            LogApplyMutation::DeleteNamespace { name } => Mutation::DeleteNamespace(name),
            LogApplyMutation::DeleteNamespaceContents { name } => {
                Mutation::DeleteNamespaceContents(name)
            }
            LogApplyMutation::PutNodeSubnet(row) => Mutation::PutNodeSubnet(row.into()),
            LogApplyMutation::AllocateNodeSubnet(allocation) => {
                Mutation::AllocateNodeSubnet(ProtoLogApplyNodeSubnetAllocation {
                    node_name: allocation.node_name,
                    cluster_cidr: allocation.cluster_cidr,
                    node_ip: allocation.node_ip,
                })
            }
            LogApplyMutation::DeleteNodeSubnet { node_name } => {
                Mutation::DeleteNodeSubnet(node_name)
            }
            LogApplyMutation::PutNodeDataplane(row) => Mutation::PutNodeDataplane(row.into()),
            LogApplyMutation::DeleteNodeDataplane { node_name } => {
                Mutation::DeleteNodeDataplane(node_name)
            }
            LogApplyMutation::PutAppliedOutbox(row) => Mutation::PutAppliedOutbox(row.into()),
            LogApplyMutation::DeleteAppliedOutbox { idempotency_key } => {
                Mutation::DeleteAppliedOutbox(idempotency_key)
            }
            LogApplyMutation::AdvanceResourceVersion { resource_version } => {
                Mutation::AdvanceResourceVersion(resource_version)
            }
            LogApplyMutation::GcAppliedOutbox {
                cutoff_ms,
                operations,
            } => Mutation::GcAppliedOutbox(ProtoLogApplyAppliedOutboxGc {
                cutoff_ms,
                operations,
            }),
            LogApplyMutation::PutWatchEvent(row) => Mutation::PutWatchEvent(row.into()),
            LogApplyMutation::PutKlightsMeta { key, value } => {
                Mutation::PutKlightsMeta(ProtoLogApplyKlightsMeta { key, value })
            }
            LogApplyMutation::PutPodCleanupIntent(row) => Mutation::PutPodCleanupIntent(row.into()),
            LogApplyMutation::DeletePodCleanupIntent(key) => {
                Mutation::DeletePodCleanupIntent(key.into())
            }
            LogApplyMutation::DeletePodCleanupIntentsForNode { node_name } => {
                Mutation::DeletePodCleanupIntentsForNode(node_name)
            }
        };
        Self {
            mutation: Some(mutation),
        }
    }
}

impl TryFrom<ProtoLogApplyMutation> for LogApplyMutation {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoLogApplyMutation) -> Result<Self> {
        use proto_log_apply_mutation::Mutation;
        Ok(
            match proto
                .mutation
                .ok_or_else(|| anyhow::anyhow!("log_apply mutation is missing variant"))?
            {
                Mutation::PutResource(row) => LogApplyMutation::PutResource(row.try_into()?),
                Mutation::PatchResourceLatest(patch) => {
                    LogApplyMutation::PatchResourceLatest(patch.try_into()?)
                }
                Mutation::DeleteResource(key) => LogApplyMutation::DeleteResource(key.into()),
                Mutation::PutNamespace(row) => LogApplyMutation::PutNamespace(row.try_into()?),
                Mutation::DeleteNamespace(name) => LogApplyMutation::DeleteNamespace { name },
                Mutation::DeleteNamespaceContents(name) => {
                    LogApplyMutation::DeleteNamespaceContents { name }
                }
                Mutation::PutNodeSubnet(row) => LogApplyMutation::PutNodeSubnet(row.into()),
                Mutation::AllocateNodeSubnet(allocation) => {
                    LogApplyMutation::AllocateNodeSubnet(LogApplyNodeSubnetAllocation {
                        node_name: allocation.node_name,
                        cluster_cidr: allocation.cluster_cidr,
                        node_ip: allocation.node_ip,
                    })
                }
                Mutation::DeleteNodeSubnet(node_name) => {
                    LogApplyMutation::DeleteNodeSubnet { node_name }
                }
                Mutation::PutNodeDataplane(row) => {
                    LogApplyMutation::PutNodeDataplane(row.try_into()?)
                }
                Mutation::DeleteNodeDataplane(node_name) => {
                    LogApplyMutation::DeleteNodeDataplane { node_name }
                }
                Mutation::PutAppliedOutbox(row) => LogApplyMutation::PutAppliedOutbox(row.into()),
                Mutation::DeleteAppliedOutbox(idempotency_key) => {
                    LogApplyMutation::DeleteAppliedOutbox { idempotency_key }
                }
                Mutation::AdvanceResourceVersion(resource_version) => {
                    LogApplyMutation::AdvanceResourceVersion { resource_version }
                }
                Mutation::GcAppliedOutbox(gc) => LogApplyMutation::GcAppliedOutbox {
                    cutoff_ms: gc.cutoff_ms,
                    operations: gc.operations,
                },
                Mutation::PutWatchEvent(row) => LogApplyMutation::PutWatchEvent(row.try_into()?),
                Mutation::PutKlightsMeta(meta) => LogApplyMutation::PutKlightsMeta {
                    key: meta.key,
                    value: meta.value,
                },
                Mutation::PutPodCleanupIntent(row) => {
                    LogApplyMutation::PutPodCleanupIntent(row.try_into()?)
                }
                Mutation::DeletePodCleanupIntent(key) => {
                    LogApplyMutation::DeletePodCleanupIntent(key.into())
                }
                Mutation::DeletePodCleanupIntentsForNode(node_name) => {
                    LogApplyMutation::DeletePodCleanupIntentsForNode { node_name }
                }
            },
        )
    }
}

impl From<LogApplyResourcePatch> for ProtoLogApplyResourcePatch {
    fn from(patch: LogApplyResourcePatch) -> Self {
        Self {
            api_version: patch.api_version,
            kind: patch.kind,
            namespace: patch.namespace,
            name: patch.name,
            resource_version: patch.resource_version,
            patch_kind: match patch.patch_kind {
                PatchKind::Merge => ProtoLogApplyPatchKind::Merge as i32,
            },
            patch_json: serde_json::to_vec(&patch.patch)
                .expect("serde_json::Value serialization is infallible"),
            require_existing: patch.require_existing,
            precondition_uid: patch.precondition_uid,
            precondition_resource_version: patch.precondition_resource_version,
        }
    }
}

impl TryFrom<ProtoLogApplyResourcePatch> for LogApplyResourcePatch {
    type Error = anyhow::Error;

    fn try_from(patch: ProtoLogApplyResourcePatch) -> Result<Self> {
        Ok(Self {
            api_version: patch.api_version,
            kind: patch.kind,
            namespace: patch.namespace,
            name: patch.name,
            resource_version: patch.resource_version,
            patch_kind: match ProtoLogApplyPatchKind::try_from(patch.patch_kind) {
                Ok(ProtoLogApplyPatchKind::Merge) => PatchKind::Merge,
                Err(_) => {
                    anyhow::bail!("unknown protobuf LogApply PatchKind: {}", patch.patch_kind)
                }
            },
            patch: serde_json::from_slice(&patch.patch_json)?,
            require_existing: patch.require_existing,
            precondition_uid: patch.precondition_uid,
            precondition_resource_version: patch.precondition_resource_version,
        })
    }
}

impl From<LogApplyResourceRow> for ProtoLogApplyResourceRow {
    fn from(row: LogApplyResourceRow) -> Self {
        Self {
            api_version: row.api_version,
            kind: row.kind,
            namespace: row.namespace,
            name: row.name,
            uid: row.uid,
            resource_version: row.resource_version,
            data_json: serde_json::to_vec(&row.data)
                .expect("serde_json::Value serialization is infallible"),
            require_absent: row.require_absent,
            require_existing: row.require_existing,
            precondition_uid: row.precondition_uid,
            precondition_resource_version: row.precondition_resource_version,
            status_only: row.status_only,
        }
    }
}

impl TryFrom<ProtoLogApplyResourceRow> for LogApplyResourceRow {
    type Error = anyhow::Error;

    fn try_from(row: ProtoLogApplyResourceRow) -> Result<Self> {
        Ok(Self {
            api_version: row.api_version,
            kind: row.kind,
            namespace: row.namespace,
            name: row.name,
            uid: row.uid,
            resource_version: row.resource_version,
            data: serde_json::from_slice(&row.data_json)?,
            require_absent: row.require_absent,
            require_existing: row.require_existing,
            precondition_uid: row.precondition_uid,
            precondition_resource_version: row.precondition_resource_version,
            status_only: row.status_only,
        })
    }
}

impl From<LogApplyResourceKey> for ProtoLogApplyResourceKey {
    fn from(key: LogApplyResourceKey) -> Self {
        Self {
            api_version: key.api_version,
            kind: key.kind,
            namespace: key.namespace,
            name: key.name,
            uid: key.uid,
            precondition_resource_version: key.precondition_resource_version,
        }
    }
}

impl From<ProtoLogApplyResourceKey> for LogApplyResourceKey {
    fn from(key: ProtoLogApplyResourceKey) -> Self {
        Self {
            api_version: key.api_version,
            kind: key.kind,
            namespace: key.namespace,
            name: key.name,
            uid: key.uid,
            precondition_resource_version: key.precondition_resource_version,
        }
    }
}

impl From<LogApplyNamespaceRow> for ProtoLogApplyNamespaceRow {
    fn from(row: LogApplyNamespaceRow) -> Self {
        Self {
            name: row.name,
            uid: row.uid,
            resource_version: row.resource_version,
            data_json: serde_json::to_vec(&row.data)
                .expect("serde_json::Value serialization is infallible"),
        }
    }
}

impl TryFrom<ProtoLogApplyNamespaceRow> for LogApplyNamespaceRow {
    type Error = anyhow::Error;

    fn try_from(row: ProtoLogApplyNamespaceRow) -> Result<Self> {
        Ok(Self {
            name: row.name,
            uid: row.uid,
            resource_version: row.resource_version,
            data: serde_json::from_slice(&row.data_json)?,
        })
    }
}

impl From<LogApplyNodeSubnetRow> for ProtoLogApplyNodeSubnetRow {
    fn from(row: LogApplyNodeSubnetRow) -> Self {
        Self {
            node_name: row.node_name,
            subnet: row.subnet,
            subnet_base_int: row.subnet_base_int,
            vtep_ip: row.vtep_ip,
            vtep_mac: row.vtep_mac,
            node_ip: row.node_ip,
            mode: row.mode,
            hostport_range: row.hostport_range,
        }
    }
}

impl From<ProtoLogApplyNodeSubnetRow> for LogApplyNodeSubnetRow {
    fn from(row: ProtoLogApplyNodeSubnetRow) -> Self {
        Self {
            node_name: row.node_name,
            subnet: row.subnet,
            subnet_base_int: row.subnet_base_int,
            vtep_ip: row.vtep_ip,
            vtep_mac: row.vtep_mac,
            node_ip: row.node_ip,
            mode: row.mode,
            hostport_range: row.hostport_range,
        }
    }
}

impl From<LogApplyNodeDataplaneRow> for ProtoLogApplyNodeDataplaneRow {
    fn from(row: LogApplyNodeDataplaneRow) -> Self {
        Self {
            node_name: row.node_name,
            mode: row.mode,
            encryption: row.encryption,
            public_key: row.public_key,
            endpoint: row.endpoint,
            port: row.port.map(u32::from),
        }
    }
}

impl TryFrom<ProtoLogApplyNodeDataplaneRow> for LogApplyNodeDataplaneRow {
    type Error = anyhow::Error;

    fn try_from(row: ProtoLogApplyNodeDataplaneRow) -> Result<Self> {
        Ok(Self {
            node_name: row.node_name,
            mode: row.mode,
            encryption: row.encryption,
            public_key: row.public_key,
            endpoint: row.endpoint,
            port: row.port.map(u16::try_from).transpose()?,
        })
    }
}

impl From<LogApplyAppliedOutboxRow> for ProtoLogApplyAppliedOutboxRow {
    fn from(row: LogApplyAppliedOutboxRow) -> Self {
        Self {
            idempotency_key: row.idempotency_key,
            subject_key: row.subject_key,
            operation: row.operation,
            first_seen_ms: row.first_seen_ms,
            applied_rv: row.applied_rv,
            result_proto: row.result_proto,
            status_stamp: row.status_stamp,
        }
    }
}

impl From<ProtoLogApplyAppliedOutboxRow> for LogApplyAppliedOutboxRow {
    fn from(row: ProtoLogApplyAppliedOutboxRow) -> Self {
        Self {
            idempotency_key: row.idempotency_key,
            subject_key: row.subject_key,
            operation: row.operation,
            first_seen_ms: row.first_seen_ms,
            applied_rv: row.applied_rv,
            result_proto: row.result_proto,
            status_stamp: row.status_stamp,
        }
    }
}

impl From<LogApplyWatchEventRow> for ProtoLogApplyWatchEventRow {
    fn from(row: LogApplyWatchEventRow) -> Self {
        Self {
            api_version: row.api_version,
            kind: row.kind,
            namespace: row.namespace,
            name: row.name,
            resource_version: row.resource_version,
            event_type: row.event_type,
            data_json: serde_json::to_vec(&row.data)
                .expect("serde_json::Value serialization is infallible"),
        }
    }
}

impl TryFrom<ProtoLogApplyWatchEventRow> for LogApplyWatchEventRow {
    type Error = anyhow::Error;

    fn try_from(row: ProtoLogApplyWatchEventRow) -> Result<Self> {
        Ok(Self {
            api_version: row.api_version,
            kind: row.kind,
            namespace: row.namespace,
            name: row.name,
            resource_version: row.resource_version,
            event_type: row.event_type,
            data: serde_json::from_slice(&row.data_json)?,
        })
    }
}

impl From<LogApplyPodCleanupIntentRow> for ProtoLogApplyPodCleanupIntentRow {
    fn from(row: LogApplyPodCleanupIntentRow) -> Self {
        Self {
            node_name: row.node_name,
            namespace: row.namespace,
            pod_name: row.pod_name,
            pod_uid: row.pod_uid,
            reason: row.reason,
            resource_version: row.resource_version,
            created_at_ms: row.created_at_ms,
            pod_data_json: serde_json::to_vec(&row.pod_data)
                .expect("serde_json::Value serialization is infallible"),
        }
    }
}

impl TryFrom<ProtoLogApplyPodCleanupIntentRow> for LogApplyPodCleanupIntentRow {
    type Error = anyhow::Error;

    fn try_from(row: ProtoLogApplyPodCleanupIntentRow) -> Result<Self> {
        Ok(Self {
            node_name: row.node_name,
            namespace: row.namespace,
            pod_name: row.pod_name,
            pod_uid: row.pod_uid,
            reason: row.reason,
            resource_version: row.resource_version,
            created_at_ms: row.created_at_ms,
            pod_data: serde_json::from_slice(&row.pod_data_json)?,
        })
    }
}

impl From<LogApplyPodCleanupIntentKey> for ProtoLogApplyPodCleanupIntentKey {
    fn from(key: LogApplyPodCleanupIntentKey) -> Self {
        Self {
            node_name: key.node_name,
            namespace: key.namespace,
            pod_name: key.pod_name,
            pod_uid: key.pod_uid,
            reason: key.reason,
        }
    }
}

impl From<ProtoLogApplyPodCleanupIntentKey> for LogApplyPodCleanupIntentKey {
    fn from(key: ProtoLogApplyPodCleanupIntentKey) -> Self {
        Self {
            node_name: key.node_name,
            namespace: key.namespace,
            pod_name: key.pod_name,
            pod_uid: key.pod_uid,
            reason: key.reason,
        }
    }
}

// T3: `LogApplyEntry` and `LogApplyCheckpoint` removed —
// the `log_apply_entries` table is gone. Raft AppendEntries
// through `apply_log_apply_commit` is the sole replication path.

#[cfg(test)]
mod parity_tests {
    //! T1.2: every `LogApplyMutation` variant must round-trip through
    //! both wire formats (`encode_commit_protobuf` and
    //! `encode_commit_json`) and survive an encode → decode → re-encode
    //! cycle byte-for-byte. The raft `EntryPayload::Normal` payload uses
    //! the protobuf encoding; the JSON encoding backs debug dumps and
    //! the existing watch-test fixtures.
    //!
    //! This test will fail if a new variant is added to
    //! `LogApplyMutation` without a matching sample below, because the
    //! exhaustive `match` on `variant_name` will not compile.
    use super::*;
    use serde_json::json;

    fn sample(name: &'static str) -> (String, LogApplyMutation) {
        let mutation = match name {
            "PutResource" => LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "cm".to_string(),
                uid: "cm-uid".to_string(),
                resource_version: 7,
                data: json!({"metadata": {"name": "cm", "uid": "cm-uid"}}),
                require_absent: false,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            }),
            "PatchResourceLatest" => LogApplyMutation::PatchResourceLatest(LogApplyResourcePatch {
                api_version: "v1".to_string(),
                kind: "ReplicationController".to_string(),
                namespace: Some("default".to_string()),
                name: "rc".to_string(),
                resource_version: 8,
                patch_kind: PatchKind::Merge,
                patch: json!({"spec": {"replicas": 2}}),
                require_existing: true,
                precondition_uid: Some("rc-uid".to_string()),
                precondition_resource_version: None,
            }),
            "DeleteResource" => LogApplyMutation::DeleteResource(LogApplyResourceKey {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "p1".to_string(),
                uid: "pod-uid-A".to_string(),
                precondition_resource_version: None,
            }),
            "PutNamespace" => LogApplyMutation::PutNamespace(LogApplyNamespaceRow {
                name: "ns".to_string(),
                uid: "ns-uid".to_string(),
                resource_version: 3,
                data: json!({"metadata": {"name": "ns"}}),
            }),
            "DeleteNamespace" => LogApplyMutation::DeleteNamespace {
                name: "ns".to_string(),
            },
            "DeleteNamespaceContents" => LogApplyMutation::DeleteNamespaceContents {
                name: "ns".to_string(),
            },
            "PutNodeSubnet" => LogApplyMutation::PutNodeSubnet(LogApplyNodeSubnetRow {
                node_name: "node-1".to_string(),
                subnet: "10.42.1.0/24".to_string(),
                subnet_base_int: 0x0a2a0100,
                vtep_ip: "10.42.1.0".to_string(),
                vtep_mac: Some("aa:bb:cc:dd:ee:ff".to_string()),
                node_ip: "192.168.0.10".to_string(),
                mode: "root".to_string(),
                hostport_range: Some("30000-32767".to_string()),
            }),
            "AllocateNodeSubnet" => {
                LogApplyMutation::AllocateNodeSubnet(LogApplyNodeSubnetAllocation {
                    node_name: "node-alloc".to_string(),
                    cluster_cidr: "10.42.0.0/16".to_string(),
                    node_ip: "192.168.0.20".to_string(),
                })
            }
            "DeleteNodeSubnet" => LogApplyMutation::DeleteNodeSubnet {
                node_name: "node-1".to_string(),
            },
            "PutNodeDataplane" => LogApplyMutation::PutNodeDataplane(LogApplyNodeDataplaneRow {
                node_name: "node-1".to_string(),
                mode: "root".to_string(),
                encryption: "wireguard".to_string(),
                public_key: Some("pub=".to_string()),
                endpoint: "192.168.0.10".to_string(),
                port: Some(51820),
            }),
            "DeleteNodeDataplane" => LogApplyMutation::DeleteNodeDataplane {
                node_name: "node-1".to_string(),
            },
            "PutAppliedOutbox" => LogApplyMutation::PutAppliedOutbox(LogApplyAppliedOutboxRow {
                idempotency_key: "cmd-1".to_string(),
                subject_key: "v1:Pod:default:p1".to_string(),
                operation: "CreateResource".to_string(),
                first_seen_ms: 1_700_000_000_000,
                applied_rv: Some(42),
                result_proto: vec![0u8, 1, 2, 3, 4],
                status_stamp: Some(7),
            }),
            "DeleteAppliedOutbox" => LogApplyMutation::DeleteAppliedOutbox {
                idempotency_key: "cmd-1".to_string(),
            },
            "GcAppliedOutbox" => LogApplyMutation::GcAppliedOutbox {
                cutoff_ms: 1_700_000_000_000,
                operations: vec!["CreateResource".to_string(), "DeleteResource".to_string()],
            },
            "PutWatchEvent" => LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "cm".to_string(),
                resource_version: 9,
                event_type: "MODIFIED".to_string(),
                data: json!({"data": {"k": "v"}}),
            }),
            "AdvanceResourceVersion" => LogApplyMutation::AdvanceResourceVersion {
                resource_version: 99,
            },
            "PutKlightsMeta" => LogApplyMutation::PutKlightsMeta {
                key: "cluster_id".to_string(),
                value: "test-uuid".to_string(),
            },
            "PutPodCleanupIntent" => {
                LogApplyMutation::PutPodCleanupIntent(LogApplyPodCleanupIntentRow {
                    node_name: "node-1".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "p1".to_string(),
                    pod_uid: "pod-uid-A".to_string(),
                    reason: "NodeLost".to_string(),
                    resource_version: 101,
                    created_at_ms: 1_700_000_000_000,
                    pod_data: json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {"namespace": "default", "name": "p1", "uid": "pod-uid-A"},
                        "spec": {"nodeName": "node-1"}
                    }),
                })
            }
            "DeletePodCleanupIntent" => {
                LogApplyMutation::DeletePodCleanupIntent(LogApplyPodCleanupIntentKey {
                    node_name: "node-1".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "p1".to_string(),
                    pod_uid: "pod-uid-A".to_string(),
                    reason: "NodeLost".to_string(),
                })
            }
            "DeletePodCleanupIntentsForNode" => LogApplyMutation::DeletePodCleanupIntentsForNode {
                node_name: "node-1".to_string(),
            },
            other => panic!("unknown variant {other}"),
        };
        (name.to_string(), mutation)
    }

    /// Compile-time exhaustive enumeration. Adding a new variant to
    /// `LogApplyMutation` without listing it here is a compile error;
    /// the `match` below has no wildcard arm.
    fn all_variant_names() -> Vec<&'static str> {
        let names: Vec<&'static str> = vec![
            "PutResource",
            "PatchResourceLatest",
            "DeleteResource",
            "PutNamespace",
            "DeleteNamespace",
            "DeleteNamespaceContents",
            "PutNodeSubnet",
            "AllocateNodeSubnet",
            "DeleteNodeSubnet",
            "PutNodeDataplane",
            "DeleteNodeDataplane",
            "PutAppliedOutbox",
            "DeleteAppliedOutbox",
            "GcAppliedOutbox",
            "PutWatchEvent",
            "AdvanceResourceVersion",
            "PutKlightsMeta",
            "PutPodCleanupIntent",
            "DeletePodCleanupIntent",
            "DeletePodCleanupIntentsForNode",
        ];
        // The exhaustive match below validates that `names` enumerates
        // every variant — adding a new variant is a compile error here.
        let probe: LogApplyMutation = LogApplyMutation::AdvanceResourceVersion {
            resource_version: 0,
        };
        let _ = match probe {
            LogApplyMutation::PutResource(_) => 0,
            LogApplyMutation::PatchResourceLatest(_) => 1,
            LogApplyMutation::DeleteResource(_) => 2,
            LogApplyMutation::PutNamespace(_) => 3,
            LogApplyMutation::DeleteNamespace { .. } => 4,
            LogApplyMutation::DeleteNamespaceContents { .. } => 5,
            LogApplyMutation::PutNodeSubnet(_) => 6,
            LogApplyMutation::AllocateNodeSubnet(_) => 7,
            LogApplyMutation::DeleteNodeSubnet { .. } => 8,
            LogApplyMutation::PutNodeDataplane(_) => 9,
            LogApplyMutation::DeleteNodeDataplane { .. } => 10,
            LogApplyMutation::PutAppliedOutbox(_) => 11,
            LogApplyMutation::DeleteAppliedOutbox { .. } => 12,
            LogApplyMutation::GcAppliedOutbox { .. } => 13,
            LogApplyMutation::PutWatchEvent(_) => 14,
            LogApplyMutation::AdvanceResourceVersion { .. } => 15,
            LogApplyMutation::PutKlightsMeta { .. } => 16,
            LogApplyMutation::PutPodCleanupIntent(_) => 17,
            LogApplyMutation::DeletePodCleanupIntent(_) => 18,
            LogApplyMutation::DeletePodCleanupIntentsForNode { .. } => 19,
        };
        names
    }

    fn commit_for(mutation: LogApplyMutation) -> LogApplyCommit {
        let rv = match &mutation {
            LogApplyMutation::PutResource(row) => row.resource_version,
            LogApplyMutation::PutNamespace(row) => row.resource_version,
            LogApplyMutation::PutWatchEvent(row) => row.resource_version,
            LogApplyMutation::PutPodCleanupIntent(row) => row.resource_version,
            LogApplyMutation::AdvanceResourceVersion { resource_version } => *resource_version,
            _ => 1,
        };
        LogApplyCommit::new(rv, vec![mutation])
    }

    #[test]
    fn every_mutation_variant_round_trips_protobuf() {
        for name in all_variant_names() {
            let (label, mutation) = sample(name);
            let commit = commit_for(mutation);

            let bytes1 = encode_commit_protobuf(&commit)
                .unwrap_or_else(|err| panic!("{label}: first protobuf encode failed: {err:#}"));
            let decoded: LogApplyCommit = decode_commit_protobuf(&bytes1)
                .unwrap_or_else(|err| panic!("{label}: protobuf decode failed: {err:#}"));
            assert_eq!(
                decoded, commit,
                "{label}: protobuf round-trip changed the value"
            );
            let bytes2 = encode_commit_protobuf(&decoded)
                .unwrap_or_else(|err| panic!("{label}: re-encode failed: {err:#}"));
            assert_eq!(
                bytes1, bytes2,
                "{label}: protobuf re-encode produced different bytes"
            );
        }
    }

    #[test]
    fn every_mutation_variant_round_trips_json() {
        for name in all_variant_names() {
            let (label, mutation) = sample(name);
            let commit = commit_for(mutation);

            let bytes1 = encode_commit_json(&commit)
                .unwrap_or_else(|err| panic!("{label}: first JSON encode failed: {err:#}"));
            let decoded: LogApplyCommit = decode_commit_json(&bytes1)
                .unwrap_or_else(|err| panic!("{label}: JSON decode failed: {err:#}"));
            assert_eq!(
                decoded, commit,
                "{label}: JSON round-trip changed the value"
            );
            let bytes2 = encode_commit_json(&decoded)
                .unwrap_or_else(|err| panic!("{label}: JSON re-encode failed: {err:#}"));
            assert_eq!(
                bytes1, bytes2,
                "{label}: JSON re-encode produced different bytes"
            );
        }
    }

    #[test]
    fn json_and_protobuf_round_trips_agree_on_decoded_value() {
        for name in all_variant_names() {
            let (label, mutation) = sample(name);
            let commit = commit_for(mutation);

            let from_json: LogApplyCommit =
                decode_commit_json(&encode_commit_json(&commit).unwrap()).unwrap();
            let from_proto: LogApplyCommit =
                decode_commit_protobuf(&encode_commit_protobuf(&commit).unwrap()).unwrap();
            assert_eq!(
                from_json, from_proto,
                "{label}: JSON and protobuf decoded into different values"
            );
        }
    }

    #[test]
    fn status_only_resource_row_round_trips_json_and_protobuf() {
        let commit = LogApplyCommit::new(
            11,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "status-only".to_string(),
                uid: "status-only-uid".to_string(),
                resource_version: 11,
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "status-only",
                        "uid": "status-only-uid",
                        "resourceVersion": "11"
                    },
                    "status": {"phase": "Running"}
                }),
                require_absent: false,
                require_existing: true,
                precondition_uid: Some("status-only-uid".to_string()),
                precondition_resource_version: None,
                status_only: true,
            })],
        );

        let from_json: LogApplyCommit =
            decode_commit_json(&encode_commit_json(&commit).unwrap()).unwrap();
        let from_proto: LogApplyCommit =
            decode_commit_protobuf(&encode_commit_protobuf(&commit).unwrap()).unwrap();
        assert_eq!(from_json, commit, "JSON must preserve status_only");
        assert_eq!(from_proto, commit, "protobuf must preserve status_only");
    }
}
