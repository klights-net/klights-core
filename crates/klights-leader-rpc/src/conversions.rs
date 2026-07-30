//! Cluster-domain/internal-wire conversions for the root-owned gRPC adapter.
//!
//! Wire values are owned by `klights-internal-protobuf`; domain policy stays
//! in focused lower contracts. Phase 12 moves this adapter mechanically with
//! the reusable leader RPC transport; it does not own persistence or Raft.

use anyhow::Result;
use klights_cluster_core::WatchReplayPosition;

use crate::storage_wire_codec::{
    decode_command_protobuf, decode_meta_protobuf, encode_command_protobuf, encode_meta_protobuf,
};
pub fn resource_command_request_to_proto(
    request: &klights_leader_api::ResourceCommandRequest,
) -> Result<klights_internal_protobuf::SubmitResourceCommandRequest> {
    Ok(klights_internal_protobuf::SubmitResourceCommandRequest {
        command_protobuf: encode_command_protobuf(request.command())?,
        codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
    })
}

pub fn resource_command_request_from_proto(
    request: klights_internal_protobuf::SubmitResourceCommandRequest,
) -> std::result::Result<
    klights_leader_api::ResourceCommandRequest,
    klights_leader_api::ResourceCommandError,
> {
    let command = decode_command_protobuf(&request.command_protobuf).map_err(|error| {
        klights_leader_api::ResourceCommandError::invalid_request(
            "command.protobuf",
            error.to_string(),
        )
    })?;
    klights_leader_api::ResourceCommandRequest::try_new(command)
}

pub fn watch_replay_position_to_proto(
    position: WatchReplayPosition,
) -> klights_internal_protobuf::WatchReplayPosition {
    klights_internal_protobuf::WatchReplayPosition {
        resource_version: position.resource_version,
        event_id: position.event_id,
        resource_version_filter_through_event_id: position.resource_version_filter_through_event_id,
    }
}

pub fn watch_replay_position_from_proto(
    position: &klights_internal_protobuf::WatchReplayPosition,
) -> WatchReplayPosition {
    WatchReplayPosition {
        resource_version: position.resource_version,
        event_id: position.event_id,
        resource_version_filter_through_event_id: position.resource_version_filter_through_event_id,
    }
}

pub fn entry_to_proto(
    entry: &crate::protocol::ReplicationEntry,
) -> Result<klights_internal_protobuf::ReplicationEntry> {
    Ok(klights_internal_protobuf::ReplicationEntry {
        command_protobuf: encode_command_protobuf(&entry.command)?,
        meta_protobuf: encode_meta_protobuf(&entry.meta)?,
        log_index: 0,
        term: 0,
        commit_protobuf: Vec::new(),
    })
}

pub fn entry_from_proto(
    entry: klights_internal_protobuf::ReplicationEntry,
) -> Result<crate::protocol::ReplicationEntry> {
    Ok(crate::protocol::ReplicationEntry {
        command: decode_command_protobuf(&entry.command_protobuf)?,
        meta: decode_meta_protobuf(&entry.meta_protobuf)?,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use klights_cluster_core::command::{
        COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand,
    };
    use klights_leader_api::ResourceCommandRequest;

    #[test]
    fn watch_replay_position_proto_round_trip_preserves_composite_cursor() {
        let position = klights_cluster_core::WatchReplayPosition {
            resource_version: 41,
            event_id: 92,
            resource_version_filter_through_event_id: 87,
        };
        let encoded = super::watch_replay_position_to_proto(position);
        assert_eq!(super::watch_replay_position_from_proto(&encoded), position);

        let invalid = klights_internal_protobuf::WatchReplayPosition {
            resource_version: -1,
            event_id: -2,
            resource_version_filter_through_event_id: -3,
        };
        assert_eq!(
            super::watch_replay_position_from_proto(&invalid),
            klights_cluster_core::WatchReplayPosition {
                resource_version: -1,
                event_id: -2,
                resource_version_filter_through_event_id: -3,
            },
            "untrusted wire cursors must remain invalid until typed validation rejects them"
        );
    }

    #[test]
    fn replication_entry_proto_wraps_storage_command_and_meta_bytes() {
        let entry = crate::protocol::ReplicationEntry {
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
    fn resource_command_proto_round_trip_preserves_canonical_command() {
        let command = StorageCommand::PatchResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            patch_kind: klights_cluster_core::PatchKind::Merge,
            patch: json!({"data": {"mode": "strict"}}),
            preconditions: klights_cluster_core::ResourcePreconditions::uid_and_resource_version(
                "uid-a", 41,
            ),
            strict_resource_version: true,
        };
        let request = ResourceCommandRequest::try_new(command.clone()).expect("valid command");
        let proto = super::resource_command_request_to_proto(&request).expect("encode request");
        assert_eq!(
            super::resource_command_request_from_proto(proto).expect("decode request"),
            request
        );
        assert_eq!(request.into_command(), command);
    }
}
