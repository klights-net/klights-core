//! Protobuf/gRPC types for storage commands.
//! Extracted from command.rs (refactor).

use super::*;

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoCommandMeta {
    #[prost(string, tag = "1")]
    pub command_id: String,
    #[prost(uint32, tag = "2")]
    pub codec_version: u32,
    #[prost(int64, tag = "3")]
    pub resource_version: i64,
    #[prost(string, optional, tag = "4")]
    pub uid: Option<String>,
    #[prost(int64, tag = "5")]
    pub timestamp_ms: i64,
    #[prost(string, tag = "6")]
    pub authoring_node: String,
}

impl From<CommandMeta> for ProtoCommandMeta {
    fn from(m: CommandMeta) -> Self {
        Self {
            command_id: m.command_id.0,
            codec_version: m.codec_version,
            resource_version: m.resource_version,
            uid: m.uid,
            timestamp_ms: m.timestamp_ms,
            authoring_node: m.authoring_node,
        }
    }
}

impl From<ProtoCommandMeta> for CommandMeta {
    fn from(p: ProtoCommandMeta) -> Self {
        Self {
            command_id: CommandId(p.command_id),
            codec_version: p.codec_version,
            resource_version: p.resource_version,
            uid: p.uid,
            timestamp_ms: p.timestamp_ms,
            authoring_node: p.authoring_node,
        }
    }
}

/// Protobuf wire type for the `CommandError` enum.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoCommandError {
    #[prost(enumeration = "proto_command_error::ErrorCode", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
}

/// Protobuf wire type for the `StorageResponse` enum.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoStorageResponse {
    #[prost(oneof = "proto_storage_response::Response", tags = "1, 2, 3, 4")]
    pub response: Option<proto_storage_response::Response>,
}

pub mod proto_storage_response {
    use super::*;

    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Response {
        #[prost(message, tag = "1")]
        Resource(ProtoResourceResp),
        #[prost(message, tag = "2")]
        Ack(ProtoAckResp),
        #[prost(message, tag = "3")]
        NodeSubnet(ProtoNodeSubnetResp),
        #[prost(message, tag = "4")]
        Error(ProtoErrorResp),
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoResourceResp {
    #[prost(int64, tag = "1")]
    pub resource_version: i64,
    #[prost(bytes = "vec", tag = "2")]
    pub data: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoAckResp {
    #[prost(int64, tag = "1")]
    pub resource_version: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoNodeSubnetResp {
    #[prost(string, tag = "1")]
    pub node_name: String,
    #[prost(string, tag = "2")]
    pub subnet: String,
    #[prost(uint32, tag = "3")]
    pub subnet_base_int: u32,
    #[prost(string, tag = "4")]
    pub vtep_ip: String,
    #[prost(string, tag = "5")]
    pub node_ip: String,
    #[prost(string, tag = "6")]
    pub mode: String,
    #[prost(string, optional, tag = "7")]
    pub hostport_range: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoErrorResp {
    #[prost(string, tag = "1")]
    pub message: String,
}

/// Protobuf wire type for the `StorageCommand` enum.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoStorageCommand {
    #[prost(
        oneof = "proto_storage_command::Command",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26"
    )]
    pub command: Option<proto_storage_command::Command>,
}

pub mod proto_storage_command {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Command {
        #[prost(message, tag = "1")]
        CreateResource(super::ProtoCreateResource),
        #[prost(message, tag = "2")]
        UpdateResource(super::ProtoUpdateResource),
        #[prost(message, tag = "3")]
        DeleteResource(super::ProtoDeleteResource),
        #[prost(message, tag = "4")]
        PatchResource(super::ProtoPatchResource),
        #[prost(message, tag = "5")]
        UpdateStatus(super::ProtoUpdateStatus),
        #[prost(message, tag = "6")]
        CreateNamespace(super::ProtoCreateNamespace),
        #[prost(message, tag = "7")]
        UpdateNamespace(super::ProtoUpdateNamespace),
        #[prost(message, tag = "8")]
        DeleteNamespace(super::ProtoDeleteNamespace),
        #[prost(message, tag = "9")]
        DeleteNamespaceContents(super::ProtoDeleteNamespaceContents),
        #[prost(message, tag = "10")]
        AllocateNodeSubnet(super::ProtoAllocateNodeSubnet),
        #[prost(message, tag = "11")]
        UpdateNodeVtepMac(super::ProtoUpdateNodeVtepMac),
        #[prost(message, tag = "12")]
        UpdateNodePeerAttributes(super::ProtoUpdateNodePeerAttributes),
        #[prost(message, tag = "13")]
        DeleteNodeSubnet(super::ProtoDeleteNodeSubnet),
        #[prost(message, tag = "14")]
        AdvanceResourceVersion(super::ProtoAdvanceResourceVersion),
        #[prost(message, tag = "15")]
        WatchEventAppend(super::ProtoWatchEventAppend),
        #[prost(message, tag = "16")]
        GcWatchEvents(super::ProtoGcWatchEvents),
        #[prost(message, tag = "17")]
        UpdateNodeDataplane(super::ProtoUpdateNodeDataplane),
        #[prost(message, tag = "18")]
        PodSlotTryAdmit(super::ProtoPodSlotAdmissionCommand),
        #[prost(message, tag = "19")]
        PodSlotMarkTerminating(super::ProtoPodSlotAdmissionCommand),
        #[prost(message, tag = "20")]
        PodSlotClearIfUid(super::ProtoPodSlotAdmissionCommand),
        #[prost(message, tag = "21")]
        EnsureClusterMetadata(super::ProtoEnsureClusterMetadata),
        #[prost(message, tag = "22")]
        SetKlightsMeta(super::ProtoSetKlightsMeta),
        #[prost(message, tag = "23")]
        MovePodToCleanupIntent(super::ProtoPodCleanupIntentCommand),
        #[prost(message, tag = "24")]
        DeletePodCleanupIntent(super::ProtoPodCleanupIntentCommand),
        #[prost(message, tag = "25")]
        DeletePodCleanupIntentsForNode(super::ProtoDeletePodCleanupIntentsForNode),
        #[prost(message, tag = "26")]
        ApplyResourceBatch(super::ProtoApplyResourceBatch),
    }
}

// -- Individual command protobuf messages --

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoCreateResource {
    #[prost(string, tag = "1")]
    pub api_version: String,
    #[prost(string, tag = "2")]
    pub kind: String,
    #[prost(string, optional, tag = "3")]
    pub namespace: Option<String>,
    #[prost(string, tag = "4")]
    pub name: String,
    #[prost(bytes = "vec", tag = "5")]
    pub data: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoUpdateResource {
    #[prost(string, tag = "1")]
    pub api_version: String,
    #[prost(string, tag = "2")]
    pub kind: String,
    #[prost(string, optional, tag = "3")]
    pub namespace: Option<String>,
    #[prost(string, tag = "4")]
    pub name: String,
    #[prost(bytes = "vec", tag = "5")]
    pub data: Vec<u8>,
    #[prost(int64, tag = "6")]
    pub expected_rv: i64,
    #[prost(message, optional, tag = "7")]
    pub preconditions: Option<ProtoResourcePreconditions>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoDeleteResource {
    #[prost(string, tag = "1")]
    pub api_version: String,
    #[prost(string, tag = "2")]
    pub kind: String,
    #[prost(string, optional, tag = "3")]
    pub namespace: Option<String>,
    #[prost(string, tag = "4")]
    pub name: String,
    #[prost(message, optional, tag = "5")]
    pub preconditions: Option<ProtoResourcePreconditions>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoPatchResource {
    #[prost(string, tag = "1")]
    pub api_version: String,
    #[prost(string, tag = "2")]
    pub kind: String,
    #[prost(string, optional, tag = "3")]
    pub namespace: Option<String>,
    #[prost(string, tag = "4")]
    pub name: String,
    #[prost(enumeration = "ProtoPatchKind", tag = "5")]
    pub patch_kind: i32,
    #[prost(bytes = "vec", tag = "6")]
    pub patch: Vec<u8>,
    #[prost(message, optional, tag = "7")]
    pub preconditions: Option<ProtoResourcePreconditions>,
    #[prost(bool, tag = "8")]
    pub strict_resource_version: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoUpdateStatus {
    #[prost(string, tag = "1")]
    pub api_version: String,
    #[prost(string, tag = "2")]
    pub kind: String,
    #[prost(string, optional, tag = "3")]
    pub namespace: Option<String>,
    #[prost(string, tag = "4")]
    pub name: String,
    #[prost(bytes = "vec", tag = "5")]
    pub status: Vec<u8>,
    #[prost(int64, optional, tag = "6")]
    pub expected_rv: Option<i64>,
    #[prost(message, optional, tag = "7")]
    pub preconditions: Option<ProtoResourcePreconditions>,
    #[prost(int64, optional, tag = "8")]
    pub observed_status_stamp: Option<i64>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoResourcePreconditions {
    #[prost(string, optional, tag = "1")]
    pub uid: Option<String>,
    #[prost(int64, optional, tag = "2")]
    pub resource_version: Option<i64>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoApplyResourceBatch {
    #[prost(message, repeated, tag = "1")]
    pub operations: Vec<ProtoResourceBatchOperation>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoResourceBatchOperation {
    #[prost(oneof = "proto_resource_batch_operation::Operation", tags = "1, 2")]
    pub operation: Option<proto_resource_batch_operation::Operation>,
}

pub mod proto_resource_batch_operation {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Operation {
        #[prost(message, tag = "1")]
        Put(super::ProtoResourceBatchPut),
        #[prost(message, tag = "2")]
        Delete(super::ProtoResourceBatchDelete),
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoResourceBatchPut {
    #[prost(string, tag = "1")]
    pub api_version: String,
    #[prost(string, tag = "2")]
    pub kind: String,
    #[prost(string, optional, tag = "3")]
    pub namespace: Option<String>,
    #[prost(string, tag = "4")]
    pub name: String,
    #[prost(bytes = "vec", tag = "5")]
    pub data: Vec<u8>,
    #[prost(enumeration = "ProtoResourceBatchPutMode", tag = "6")]
    pub mode: i32,
    #[prost(message, optional, tag = "7")]
    pub preconditions: Option<ProtoResourcePreconditions>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoResourceBatchDelete {
    #[prost(string, tag = "1")]
    pub api_version: String,
    #[prost(string, tag = "2")]
    pub kind: String,
    #[prost(string, optional, tag = "3")]
    pub namespace: Option<String>,
    #[prost(string, tag = "4")]
    pub name: String,
    #[prost(message, optional, tag = "5")]
    pub preconditions: Option<ProtoResourcePreconditions>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum ProtoResourceBatchPutMode {
    Create = 0,
    Update = 1,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoCreateNamespace {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(bytes = "vec", tag = "2")]
    pub data: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoUpdateNamespace {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(bytes = "vec", tag = "2")]
    pub data: Vec<u8>,
    #[prost(int64, tag = "3")]
    pub expected_rv: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoDeleteNamespace {
    #[prost(string, tag = "1")]
    pub name: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoDeleteNamespaceContents {
    #[prost(string, tag = "1")]
    pub name: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoAllocateNodeSubnet {
    #[prost(string, tag = "1")]
    pub node_name: String,
    #[prost(string, tag = "2")]
    pub subnet: String,
    #[prost(string, tag = "3")]
    pub node_ip: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoUpdateNodeVtepMac {
    #[prost(string, tag = "1")]
    pub node_name: String,
    #[prost(string, tag = "2")]
    pub vtep_mac: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoUpdateNodePeerAttributes {
    #[prost(string, tag = "1")]
    pub node_name: String,
    #[prost(string, tag = "2")]
    pub mode: String,
    #[prost(string, optional, tag = "3")]
    pub hostport_range: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoUpdateNodeDataplane {
    #[prost(string, tag = "1")]
    pub node_name: String,
    #[prost(string, tag = "2")]
    pub mode: String,
    #[prost(string, tag = "3")]
    pub encryption: String,
    #[prost(string, optional, tag = "4")]
    pub public_key: Option<String>,
    #[prost(string, tag = "5")]
    pub endpoint: String,
    #[prost(uint32, optional, tag = "6")]
    pub port: Option<u32>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoDeleteNodeSubnet {
    #[prost(string, tag = "1")]
    pub node_name: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoPodSlotAdmissionCommand {
    #[prost(string, tag = "1")]
    pub namespace: String,
    #[prost(string, tag = "2")]
    pub pod_name: String,
    #[prost(string, tag = "3")]
    pub pod_uid: String,
    #[prost(string, tag = "4")]
    pub node_name: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoPodCleanupIntentCommand {
    #[prost(string, tag = "1")]
    pub node_name: String,
    #[prost(string, tag = "2")]
    pub namespace: String,
    #[prost(string, tag = "3")]
    pub pod_name: String,
    #[prost(string, tag = "4")]
    pub pod_uid: String,
    #[prost(string, tag = "5")]
    pub reason: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoDeletePodCleanupIntentsForNode {
    #[prost(string, tag = "1")]
    pub node_name: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoAdvanceResourceVersion {
    #[prost(int64, tag = "1")]
    pub min_rv: i64,
    #[prost(int64, tag = "2")]
    pub new_rv: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoEnsureClusterMetadata {
    #[prost(string, tag = "1")]
    pub cluster_id: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoSetKlightsMeta {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoWatchEventAppend {
    #[prost(bytes = "vec", tag = "1")]
    pub event_bytes: Vec<u8>,
    #[prost(int64, tag = "2")]
    pub rv: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoGcWatchEvents {
    #[prost(int64, tag = "1")]
    pub max_rows: i64,
    #[prost(int64, tag = "2")]
    pub batch_cap: i64,
}

/// Protobuf enum for `PatchKind`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
pub enum ProtoPatchKind {
    Merge = 0,
}

/// Protobuf enum for `CommandError` codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
pub enum ProtoCommandErrorCode {
    Unknown = 0,
    Conflict = 1,
    NotFound = 2,
    Internal = 3,
}

/// Helper to encode the error code in the protobuf message.
pub mod proto_command_error {
    pub use super::ProtoCommandErrorCode as ErrorCode;
}

// ---------------------------------------------------------------------------
// serde helper: pass-through for serde_json::Value (already native JSON)
// ---------------------------------------------------------------------------

/// Serde helper that serializes `serde_json::Value` as-is.
/// Since `Value` is natively serializable, this is identity.
pub mod serde_bytes_base64 {
    pub fn serialize<S: serde::Serializer>(
        val: &serde_json::Value,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(val, s)
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<serde_json::Value, D::Error> {
        serde::Deserialize::deserialize(d)
    }
}

// ---------------------------------------------------------------------------
// JSON codec
