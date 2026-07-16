//! Stable protobuf wire representations for committed cluster mutations.

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyCommit {
    #[prost(int64, tag = "1")]
    pub resource_version: i64,
    #[prost(message, repeated, tag = "2")]
    pub mutations: Vec<ProtoLogApplyMutation>,
    #[prost(message, optional, tag = "3")]
    pub outbox_watermark: Option<ProtoOutboxStreamWatermark>,
    #[prost(enumeration = "ProtoResourceVersionAssignment", tag = "4")]
    pub resource_version_assignment: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, prost::Enumeration)]
pub enum ProtoResourceVersionAssignment {
    LegacyLeaderAssigned = 0,
    CommittedApplyV1 = 1,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoOutboxStreamWatermark {
    #[prost(string, tag = "1")]
    pub client_id: String,
    #[prost(int64, tag = "2")]
    pub stream_id: i64,
    #[prost(int64, tag = "3")]
    pub stream_seq: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyMutation {
    #[prost(
        oneof = "proto_log_apply_mutation::Mutation",
        tags = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21"
    )]
    pub mutation: Option<proto_log_apply_mutation::Mutation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, prost::Enumeration)]
pub enum ProtoLogApplyPatchKind {
    Merge = 0,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyKlightsMeta {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyWatchEventsGc {
    #[prost(int64, tag = "1")]
    pub max_rows: i64,
    #[prost(int64, tag = "2")]
    pub batch_cap: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyNodeSubnetAllocation {
    #[prost(string, tag = "1")]
    pub node_name: String,
    #[prost(string, tag = "2")]
    pub cluster_cidr: String,
    #[prost(string, tag = "3")]
    pub node_ip: String,
}

pub mod proto_log_apply_mutation {
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
        #[prost(message, tag = "21")]
        GcWatchEvents(super::ProtoLogApplyWatchEventsGc),
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyResourcePatch {
    #[prost(string, tag = "1")]
    pub api_version: String,
    #[prost(string, tag = "2")]
    pub kind: String,
    #[prost(string, optional, tag = "3")]
    pub namespace: Option<String>,
    #[prost(string, tag = "4")]
    pub name: String,
    #[prost(int64, tag = "5")]
    pub resource_version: i64,
    #[prost(enumeration = "ProtoLogApplyPatchKind", tag = "6")]
    pub patch_kind: i32,
    #[prost(bytes = "vec", tag = "7")]
    pub patch_json: Vec<u8>,
    #[prost(bool, tag = "8")]
    pub require_existing: bool,
    #[prost(string, optional, tag = "9")]
    pub precondition_uid: Option<String>,
    #[prost(int64, optional, tag = "10")]
    pub precondition_resource_version: Option<i64>,
    #[prost(string, optional, tag = "11")]
    pub terminating_pod_unready_timestamp: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyResourceRow {
    #[prost(string, tag = "1")]
    pub api_version: String,
    #[prost(string, tag = "2")]
    pub kind: String,
    #[prost(string, optional, tag = "3")]
    pub namespace: Option<String>,
    #[prost(string, tag = "4")]
    pub name: String,
    #[prost(string, tag = "5")]
    pub uid: String,
    #[prost(int64, tag = "6")]
    pub resource_version: i64,
    #[prost(bytes = "vec", tag = "7")]
    pub data_json: Vec<u8>,
    #[prost(bool, tag = "8")]
    pub require_absent: bool,
    #[prost(bool, tag = "9")]
    pub require_existing: bool,
    #[prost(string, optional, tag = "10")]
    pub precondition_uid: Option<String>,
    #[prost(int64, optional, tag = "11")]
    pub precondition_resource_version: Option<i64>,
    #[prost(bool, tag = "12")]
    pub status_only: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyResourceKey {
    #[prost(string, tag = "1")]
    pub api_version: String,
    #[prost(string, tag = "2")]
    pub kind: String,
    #[prost(string, optional, tag = "3")]
    pub namespace: Option<String>,
    #[prost(string, tag = "4")]
    pub name: String,
    #[prost(string, tag = "5")]
    pub uid: String,
    #[prost(int64, optional, tag = "6")]
    pub precondition_resource_version: Option<i64>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyNamespaceRow {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub uid: String,
    #[prost(int64, tag = "3")]
    pub resource_version: i64,
    #[prost(bytes = "vec", tag = "4")]
    pub data_json: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyNodeSubnetRow {
    #[prost(string, tag = "1")]
    pub node_name: String,
    #[prost(string, tag = "2")]
    pub subnet: String,
    #[prost(uint32, tag = "3")]
    pub subnet_base_int: u32,
    #[prost(string, tag = "4")]
    pub gateway_ip: String,
    #[prost(string, tag = "6")]
    pub node_ip: String,
    #[prost(string, tag = "7")]
    pub mode: String,
    #[prost(string, optional, tag = "8")]
    pub hostport_range: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyNodeDataplaneRow {
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
pub struct ProtoLogApplyAppliedOutboxRow {
    #[prost(string, tag = "1")]
    pub idempotency_key: String,
    #[prost(string, tag = "2")]
    pub subject_key: String,
    #[prost(string, tag = "3")]
    pub operation: String,
    #[prost(int64, tag = "4")]
    pub first_seen_ms: i64,
    #[prost(int64, optional, tag = "5")]
    pub applied_rv: Option<i64>,
    #[prost(bytes = "vec", tag = "6")]
    pub result_proto: Vec<u8>,
    #[prost(int64, optional, tag = "7")]
    pub status_stamp: Option<i64>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyAppliedOutboxGc {
    #[prost(int64, tag = "1")]
    pub cutoff_ms: i64,
    #[prost(string, repeated, tag = "2")]
    pub operations: Vec<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyWatchEventRow {
    #[prost(string, tag = "1")]
    pub api_version: String,
    #[prost(string, tag = "2")]
    pub kind: String,
    #[prost(string, optional, tag = "3")]
    pub namespace: Option<String>,
    #[prost(string, tag = "4")]
    pub name: String,
    #[prost(int64, tag = "5")]
    pub resource_version: i64,
    #[prost(string, tag = "6")]
    pub event_type: String,
    #[prost(bytes = "vec", tag = "7")]
    pub data_json: Vec<u8>,
    #[prost(int64, optional, tag = "8")]
    pub event_id: Option<i64>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyPodCleanupIntentRow {
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
    #[prost(int64, tag = "6")]
    pub resource_version: i64,
    #[prost(int64, tag = "7")]
    pub created_at_ms: i64,
    #[prost(bytes = "vec", tag = "8")]
    pub pod_data_json: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtoLogApplyPodCleanupIntentKey {
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
