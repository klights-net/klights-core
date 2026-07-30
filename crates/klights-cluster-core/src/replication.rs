//! Transport-neutral cluster replication stream values.

use serde::{Deserialize, Serialize};

use crate::{CommandMeta, StorageCommand};

/// A replication envelope wrapping a command with its metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReplicationEntry {
    pub command: StorageCommand,
    pub meta: CommandMeta,
}

/// Request to subscribe to the command stream from a given resource version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRequest {
    /// Start streaming from this resource version (inclusive).
    pub start_rv: i64,
}

/// A single item in the command stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StreamItem {
    /// A replicated command with metadata.
    Entry(Box<ReplicationEntry>),
    /// A keep-alive / heartbeat when no commands have been produced.
    Heartbeat { current_rv: i64 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{COMMAND_CODEC_VERSION, CommandId};

    fn sample_meta() -> CommandMeta {
        CommandMeta {
            command_id: CommandId("replication-sample-command".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 1,
            uid: None,
            timestamp_ms: 0,
            authoring_node: "test".into(),
        }
    }

    #[test]
    fn replication_entry_round_trips_json() {
        let entry = ReplicationEntry {
            command: StorageCommand::CreateResource {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace: Some("default".into()),
                name: "test".into(),
                data: serde_json::json!({"metadata": {"name": "test"}}),
            },
            meta: sample_meta(),
        };

        let json = serde_json::to_vec(&entry).unwrap();
        let decoded: ReplicationEntry = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded.command, entry.command);
        assert_eq!(decoded.meta.command_id, entry.meta.command_id);
    }

    #[test]
    fn stream_values_round_trip_json() {
        let request = StreamRequest { start_rv: 5 };
        assert_eq!(
            serde_json::from_slice::<StreamRequest>(&serde_json::to_vec(&request).unwrap())
                .unwrap(),
            request
        );

        let entry = StreamItem::Entry(Box::new(ReplicationEntry {
            command: StorageCommand::CreateNamespace {
                name: "test".into(),
                data: serde_json::json!({}),
            },
            meta: sample_meta(),
        }));
        assert_eq!(
            serde_json::from_slice::<StreamItem>(&serde_json::to_vec(&entry).unwrap()).unwrap(),
            entry
        );

        let heartbeat = StreamItem::Heartbeat { current_rv: 42 };
        assert_eq!(
            serde_json::from_slice::<StreamItem>(&serde_json::to_vec(&heartbeat).unwrap()).unwrap(),
            heartbeat
        );
    }
}
