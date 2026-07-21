// TEMPORARY(Phase 4.1): root keeps the generated bridge for current consumers.
// REMOVE: Phase 12A removes this bridge after generated API migration.
pub(crate) use klights_internal_protobuf as generated;
pub(crate) use klights_internal_protobuf::FILE_DESCRIPTOR_SET;

pub mod client;
pub mod fanout;
pub mod raft_rpc;
pub mod server;
pub mod snapshot_cache;
pub mod transport_policy;

pub use transport_policy::{GrpcTransportPolicy, SharedGrpcTransportPolicy};

use bytes::Bytes;
use prost::Message;
use tonic::metadata::{MetadataMap, MetadataValue};

mod ca_files;
mod conversions;
pub(crate) use conversions::{
    entry_from_proto, entry_to_proto, log_apply_commit_from_proto, log_apply_commit_to_proto,
    resource_command_request_from_proto, resource_command_request_to_proto,
    watch_replay_position_from_proto, watch_replay_position_to_proto,
};
pub const JOIN_TOKEN_METADATA_KEY: &str = "x-klights-join-token";
pub(crate) const WATCH_REPLAY_EXPIRED_REASON_METADATA_KEY: &str = "x-klights-watch-error";
pub(crate) const WATCH_REPLAY_EXPIRED_REASON: &str = "watch-replay-expired";
const LEGACY_WATCH_REPLAY_EXPIRED_PREFIX: &str = "WatchResources replay window expired: resume rv ";
const LEGACY_WATCH_REPLAY_EXPIRED_SUFFIX: &str = " requires relist";

pub fn watch_replay_expired_status(
    accepted_resource_version: i64,
    message: impl Into<String>,
) -> tonic::Status {
    let details = generated::WatchReplayExpiredDetails {
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

#[cfg(test)]
pub(crate) fn is_watch_replay_expired_status(status: &tonic::Status) -> bool {
    watch_replay_expired_resource_version(status).is_some()
}

pub(crate) fn watch_replay_expired_resource_version(status: &tonic::Status) -> Option<i64> {
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
    let Ok(details) = generated::WatchReplayExpiredDetails::decode(status.details()) else {
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
