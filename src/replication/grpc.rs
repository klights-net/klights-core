pub(crate) use klights_internal_protobuf as generated;
pub(crate) use klights_internal_protobuf::FILE_DESCRIPTOR_SET;

pub mod client;
pub mod fanout;
pub mod raft_rpc;
pub mod server;
pub mod snapshot_cache;
pub mod transport_policy;

pub use transport_policy::{GrpcTransportPolicy, SharedGrpcTransportPolicy};

use anyhow::Result;
use bytes::Bytes;
use prost::Message;
use tonic::metadata::{MetadataMap, MetadataValue};

mod ca_files;
pub const JOIN_TOKEN_METADATA_KEY: &str = "x-klights-join-token";
pub(crate) const WATCH_REPLAY_EXPIRED_REASON_METADATA_KEY: &str = "x-klights-watch-error";
pub(crate) const WATCH_REPLAY_EXPIRED_REASON: &str = "watch-replay-expired";
const LEGACY_WATCH_REPLAY_EXPIRED_PREFIX: &str = "WatchResources replay window expired: resume rv ";
const LEGACY_WATCH_REPLAY_EXPIRED_SUFFIX: &str = " requires relist";

pub(crate) fn watch_replay_position_to_proto(
    position: crate::datastore::WatchReplayPosition,
) -> generated::WatchReplayPosition {
    generated::WatchReplayPosition {
        resource_version: position.resource_version,
        event_id: position.event_id,
        resource_version_filter_through_event_id: position.resource_version_filter_through_event_id,
    }
}

pub(crate) fn watch_replay_position_from_proto(
    position: &generated::WatchReplayPosition,
) -> crate::datastore::WatchReplayPosition {
    crate::datastore::WatchReplayPosition {
        resource_version: position.resource_version.max(0),
        event_id: position.event_id.max(0),
        resource_version_filter_through_event_id: position
            .resource_version_filter_through_event_id
            .max(0),
    }
}

pub(crate) fn watch_replay_expired_status(
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

pub(crate) fn is_watch_replay_expired_status(status: &tonic::Status) -> bool {
    if status.code() != tonic::Code::OutOfRange {
        return false;
    }
    if is_typed_watch_replay_expired_status(status) {
        return true;
    }
    legacy_watch_replay_expired_status(status)
}

fn is_typed_watch_replay_expired_status(status: &tonic::Status) -> bool {
    let Some(reason) = status
        .metadata()
        .get(WATCH_REPLAY_EXPIRED_REASON_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    if reason != WATCH_REPLAY_EXPIRED_REASON {
        return false;
    }
    let Ok(details) = generated::WatchReplayExpiredDetails::decode(status.details()) else {
        return false;
    };
    details.reason == WATCH_REPLAY_EXPIRED_REASON
}

fn legacy_watch_replay_expired_status(status: &tonic::Status) -> bool {
    if !status.metadata().is_empty() || !status.details().is_empty() {
        return false;
    }
    let Some(resume_rv) = status
        .message()
        .strip_prefix(LEGACY_WATCH_REPLAY_EXPIRED_PREFIX)
        .and_then(|message| message.strip_suffix(LEGACY_WATCH_REPLAY_EXPIRED_SUFFIX))
    else {
        return false;
    };
    resume_rv.parse::<i64>().is_ok()
}

use crate::datastore::command::{
    decode_command_protobuf, decode_meta_protobuf, encode_command_protobuf, encode_meta_protobuf,
};
use crate::log_apply::{decode_commit_protobuf, encode_commit_protobuf};

pub fn entry_to_proto(
    entry: &crate::replication::protocol::ReplicationEntry,
) -> Result<generated::ReplicationEntry> {
    Ok(generated::ReplicationEntry {
        command_protobuf: encode_command_protobuf(&entry.command)?,
        meta_protobuf: encode_meta_protobuf(&entry.meta)?,
        log_index: 0,
        term: 0,
        commit_protobuf: Vec::new(),
    })
}

pub fn entry_from_proto(
    entry: generated::ReplicationEntry,
) -> Result<crate::replication::protocol::ReplicationEntry> {
    Ok(crate::replication::protocol::ReplicationEntry {
        command: decode_command_protobuf(&entry.command_protobuf)?,
        meta: decode_meta_protobuf(&entry.meta_protobuf)?,
    })
}

pub fn log_apply_commit_to_proto(
    commit: &crate::log_apply::LogApplyCommit,
) -> Result<generated::ReplicationEntry> {
    Ok(generated::ReplicationEntry {
        command_protobuf: Vec::new(),
        meta_protobuf: Vec::new(),
        log_index: 0,
        term: 0,
        commit_protobuf: encode_commit_protobuf(commit)?,
    })
}

pub fn log_apply_commit_from_proto(
    entry: generated::ReplicationEntry,
) -> Result<crate::log_apply::LogApplyCommit> {
    decode_commit_protobuf(&entry.commit_protobuf)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        datastore::command::{COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand},
        replication::{grpc, protocol},
    };

    #[test]
    fn watch_replay_position_proto_round_trip_preserves_composite_cursor() {
        let position = crate::datastore::WatchReplayPosition {
            resource_version: 41,
            event_id: 92,
            resource_version_filter_through_event_id: 87,
        };
        let encoded = super::watch_replay_position_to_proto(position);
        assert_eq!(super::watch_replay_position_from_proto(&encoded), position);

        let invalid = grpc::generated::WatchReplayPosition {
            resource_version: -1,
            event_id: -2,
            resource_version_filter_through_event_id: -3,
        };
        assert_eq!(
            super::watch_replay_position_from_proto(&invalid),
            crate::datastore::WatchReplayPosition::default(),
            "untrusted wire cursors must be normalized to non-negative values"
        );
    }

    #[test]
    fn replication_entry_proto_wraps_storage_command_and_meta_bytes() {
        let entry = protocol::ReplicationEntry {
            command: StorageCommand::CreateResource {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "cm-a".to_string(),
                data: json!({"metadata": {"name": "cm-a", "namespace": "default"}}),
            },
            meta: CommandMeta {
                command_id: CommandId("cmd-a".to_string()),
                codec_version: COMMAND_CODEC_VERSION,
                resource_version: 42,
                uid: Some("uid-a".to_string()),
                timestamp_ms: 1_700_000_000_000,
                authoring_node: "worker-1".to_string(),
            },
        };

        let proto = grpc::entry_to_proto(&entry).unwrap();
        assert!(!proto.command_protobuf.is_empty());
        assert!(!proto.meta_protobuf.is_empty());
        assert_eq!(grpc::entry_from_proto(proto).unwrap(), entry);
    }
}
