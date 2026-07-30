//! Reusable authenticated leader RPC transport for klights.
//!
//! This crate owns tonic client/server framing, TLS and peer-authentication
//! adaptation, internal protobuf conversion, remote leader/node forwarding,
//! transport errors, and the opaque byte-oriented Raft handler contract. It
//! deliberately owns no OpenRaft, persistence, or Kubernetes feature
//! implementation.

extern crate self as klights_leader_rpc;

pub mod client;
pub mod protocol;
pub mod raft_rpc;
pub mod semantic_operations;
pub mod server;
pub mod storage_wire_codec;
pub mod tls_policy;
pub mod transport_policy;

pub use transport_policy::{GrpcTransportPolicy, SharedGrpcTransportPolicy};

use bytes::Bytes;
use prost::Message;
use tonic::metadata::{MetadataMap, MetadataValue};

mod ca_files;
pub use ca_files::ReplicationRuntimeFiles;
mod conversions;
pub use conversions::{
    entry_from_proto, entry_to_proto, resource_command_request_from_proto,
    resource_command_request_to_proto, watch_replay_position_from_proto,
    watch_replay_position_to_proto,
};
pub const JOIN_TOKEN_METADATA_KEY: &str = "x-klights-join-token";
pub const WATCH_REPLAY_EXPIRED_REASON_METADATA_KEY: &str = "x-klights-watch-error";
pub const WATCH_REPLAY_EXPIRED_REASON: &str = "watch-replay-expired";
const LEGACY_WATCH_REPLAY_EXPIRED_PREFIX: &str = "WatchResources replay window expired: resume rv ";
const LEGACY_WATCH_REPLAY_EXPIRED_SUFFIX: &str = " requires relist";

pub fn watch_replay_expired_status(
    accepted_resource_version: i64,
    message: impl Into<String>,
) -> tonic::Status {
    let details = klights_internal_protobuf::WatchReplayExpiredDetails {
        reason: WATCH_REPLAY_EXPIRED_REASON.to_string(),
        accepted_resource_version,
    }
    .encode_to_vec();
    let mut metadata = MetadataMap::new();
    metadata.insert(
        WATCH_REPLAY_EXPIRED_REASON_METADATA_KEY,
        MetadataValue::from_static(WATCH_REPLAY_EXPIRED_REASON),
    );
    tonic::Status::with_details_and_metadata(
        tonic::Code::OutOfRange,
        message,
        Bytes::from(details),
        metadata,
    )
}

pub fn is_watch_replay_expired_status(status: &tonic::Status) -> bool {
    watch_replay_expired_resource_version(status).is_some()
}

pub fn watch_replay_expired_resource_version(status: &tonic::Status) -> Option<i64> {
    if status.code() != tonic::Code::OutOfRange {
        return None;
    }
    if let Some(resource_version) = typed_watch_replay_expired_resource_version(status) {
        return Some(resource_version);
    }
    legacy_watch_replay_expired_resource_version(status)
}

fn typed_watch_replay_expired_resource_version(status: &tonic::Status) -> Option<i64> {
    let reason = status
        .metadata()
        .get(WATCH_REPLAY_EXPIRED_REASON_METADATA_KEY)
        .and_then(|value| value.to_str().ok())?;
    if reason != WATCH_REPLAY_EXPIRED_REASON {
        return None;
    }
    let Ok(details) =
        klights_internal_protobuf::WatchReplayExpiredDetails::decode(status.details())
    else {
        return None;
    };
    (details.reason == WATCH_REPLAY_EXPIRED_REASON).then_some(details.accepted_resource_version)
}

fn legacy_watch_replay_expired_resource_version(status: &tonic::Status) -> Option<i64> {
    if !status.metadata().is_empty() || !status.details().is_empty() {
        return None;
    }
    let resume_rv = status
        .message()
        .strip_prefix(LEGACY_WATCH_REPLAY_EXPIRED_PREFIX)
        .and_then(|message| message.strip_suffix(LEGACY_WATCH_REPLAY_EXPIRED_SUFFIX))?;
    resume_rv.parse::<i64>().ok()
}
