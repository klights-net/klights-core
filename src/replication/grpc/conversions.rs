//! Temporary cluster-domain/internal-wire conversions.
//!
//! Wire values are owned by `klights-internal-protobuf`; domain policy stays
//! in root until Phase 5 moves the domain side and this adapter into the
//! replication package.
//! REMOVE: Phase 5.5 moves the residual gRPC conversion adapter.

use anyhow::Result;

use super::generated;
use crate::datastore::command::{
    decode_command_protobuf, decode_meta_protobuf, encode_command_protobuf, encode_meta_protobuf,
};
use crate::log_apply::{decode_commit_protobuf, encode_commit_protobuf};

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

pub(crate) fn entry_to_proto(
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

pub(crate) fn entry_from_proto(
    entry: generated::ReplicationEntry,
) -> Result<crate::replication::protocol::ReplicationEntry> {
    Ok(crate::replication::protocol::ReplicationEntry {
        command: decode_command_protobuf(&entry.command_protobuf)?,
        meta: decode_meta_protobuf(&entry.meta_protobuf)?,
    })
}

pub(crate) fn log_apply_commit_to_proto(
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

pub(crate) fn log_apply_commit_from_proto(
    entry: generated::ReplicationEntry,
) -> Result<crate::log_apply::LogApplyCommit> {
    decode_commit_protobuf(&entry.commit_protobuf)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::datastore::command::{
        COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand,
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

        let invalid = super::generated::WatchReplayPosition {
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
        let entry = crate::replication::protocol::ReplicationEntry {
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

        let proto = super::entry_to_proto(&entry).unwrap();
        assert!(!proto.command_protobuf.is_empty());
        assert!(!proto.meta_protobuf.is_empty());
        assert_eq!(super::entry_from_proto(proto).unwrap(), entry);
    }

    #[test]
    fn log_apply_commit_proto_round_trip_preserves_assignment_and_watermark() {
        let mut commit = crate::log_apply::LogApplyCommit::new(73, Vec::new());
        commit.resource_version_assignment =
            crate::log_apply::ResourceVersionAssignment::CommittedApplyV1;
        commit.outbox_watermark = Some(crate::log_apply::OutboxStreamWatermark {
            client_id: "worker-a".to_string(),
            stream_id: 8,
            stream_seq: 13,
        });

        let proto = super::log_apply_commit_to_proto(&commit).unwrap();
        assert!(proto.command_protobuf.is_empty());
        assert!(proto.meta_protobuf.is_empty());
        assert!(!proto.commit_protobuf.is_empty());
        assert_eq!(super::log_apply_commit_from_proto(proto).unwrap(), commit);
    }
}
