//! Encode/decode boundary for the replication apply path.
//!
//! Currently the apply pipeline calls free `encode_*_protobuf` /
//! `decode_*_protobuf` functions in `crate::datastore::command` directly.
//! This module introduces a `CommandCodec` trait so that:
//!
//! 1. `apply::mod` can take `&dyn CommandCodec` and tests can swap in a
//!    deterministic / failure-injecting fake without touching prost.
//! 2. Future migrations of `replication::grpc` and other call sites can
//!    route through the same trait.
//!
//! The default impl `ProtobufCommandCodec` preserves today's behavior
//! bit-for-bit by delegating to the existing `datastore::command`
//! functions; no on-wire change.

use crate::datastore::command::{
    CommandMeta, StorageCommand, StorageResponse, decode_command_protobuf, decode_meta_protobuf,
    decode_response_protobuf, encode_command_protobuf, encode_meta_protobuf,
    encode_response_protobuf,
};

/// Pluggable wire codec for replication entries.
///
/// Default impl: `ProtobufCommandCodec`. Tests may provide alternative
/// impls (e.g. recording, failure-injecting) without changing the
/// production wire format.
pub trait CommandCodec: Send + Sync {
    fn encode_command(&self, cmd: &StorageCommand) -> anyhow::Result<Vec<u8>>;
    fn decode_command(&self, bytes: &[u8]) -> anyhow::Result<StorageCommand>;
    fn encode_response(&self, resp: &StorageResponse) -> anyhow::Result<Vec<u8>>;
    fn decode_response(&self, bytes: &[u8]) -> anyhow::Result<StorageResponse>;
    fn encode_meta(&self, meta: &CommandMeta) -> anyhow::Result<Vec<u8>>;
    fn decode_meta(&self, bytes: &[u8]) -> anyhow::Result<CommandMeta>;
}

/// Today's wire codec — protobuf via prost. Stateless; cheap to clone.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProtobufCommandCodec;

impl CommandCodec for ProtobufCommandCodec {
    fn encode_command(&self, cmd: &StorageCommand) -> anyhow::Result<Vec<u8>> {
        encode_command_protobuf(cmd)
    }
    fn decode_command(&self, bytes: &[u8]) -> anyhow::Result<StorageCommand> {
        decode_command_protobuf(bytes)
    }
    fn encode_response(&self, resp: &StorageResponse) -> anyhow::Result<Vec<u8>> {
        encode_response_protobuf(resp)
    }
    fn decode_response(&self, bytes: &[u8]) -> anyhow::Result<StorageResponse> {
        decode_response_protobuf(bytes)
    }
    fn encode_meta(&self, meta: &CommandMeta) -> anyhow::Result<Vec<u8>> {
        encode_meta_protobuf(meta)
    }
    fn decode_meta(&self, bytes: &[u8]) -> anyhow::Result<CommandMeta> {
        decode_meta_protobuf(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::command::{COMMAND_CODEC_VERSION, CommandId};
    use crate::datastore::types::ResourcePreconditions;
    use serde_json::json;

    fn codec() -> ProtobufCommandCodec {
        ProtobufCommandCodec
    }

    fn meta() -> CommandMeta {
        CommandMeta {
            command_id: CommandId("k".to_string()),
            codec_version: COMMAND_CODEC_VERSION,
            resource_version: 7,
            uid: Some("u".to_string()),
            timestamp_ms: 42,
            authoring_node: "n".to_string(),
        }
    }

    // ---- response codec ------------------------------------------------

    #[test]
    fn response_resource_round_trip_preserves_data() {
        let c = codec();
        let resp = StorageResponse::Resource {
            resource_version: 99,
            data: json!({"metadata": {"name": "x"}}),
        };
        let bytes = c.encode_response(&resp).unwrap();
        let decoded = c.decode_response(&bytes).unwrap();
        assert_eq!(decoded, resp);
    }

    #[test]
    fn response_node_subnet_round_trip_preserves_fields() {
        let c = codec();
        let resp = StorageResponse::NodeSubnet {
            node_name: "n1".into(),
            subnet: "10.244.0.0/24".into(),
            subnet_base_int: 7,
            vtep_ip: "10.244.0.1".into(),
            node_ip: "192.168.1.1".into(),
            mode: "Root".into(),
            hostport_range: None,
        };
        let bytes = c.encode_response(&resp).unwrap();
        let decoded = c.decode_response(&bytes).unwrap();
        assert_eq!(decoded, resp);
    }

    #[test]
    fn response_ack_round_trip_preserves_rv() {
        let c = codec();
        let resp = StorageResponse::Ack {
            resource_version: 12345,
        };
        let bytes = c.encode_response(&resp).unwrap();
        let decoded = c.decode_response(&bytes).unwrap();
        assert_eq!(decoded, resp);
    }

    #[test]
    fn response_decode_rejects_malformed_bytes() {
        let c = codec();
        let err = c.decode_response(&[0xff, 0xff, 0xff, 0xff]).unwrap_err();
        let msg = format!("{err}");
        assert!(
            !msg.is_empty(),
            "decode error must surface a message, got empty"
        );
    }

    // ---- command codec -------------------------------------------------

    #[test]
    fn command_create_resource_round_trip() {
        let c = codec();
        let cmd = StorageCommand::CreateResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "foo".into(),
            data: json!({"metadata": {"uid": "u-1"}}),
        };
        let bytes = c.encode_command(&cmd).unwrap();
        let decoded = c.decode_command(&bytes).unwrap();
        assert_eq!(decoded, cmd);
    }

    #[test]
    fn command_delete_resource_round_trip_preserves_preconditions() {
        let c = codec();
        let cmd = StorageCommand::DeleteResource {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            namespace: Some("default".into()),
            name: "foo".into(),
            preconditions: ResourcePreconditions {
                uid: Some("u-2".into()),
                resource_version: Some(11),
            },
        };
        let bytes = c.encode_command(&cmd).unwrap();
        let decoded = c.decode_command(&bytes).unwrap();
        assert_eq!(decoded, cmd);
    }

    #[test]
    fn command_namespace_lifecycle_round_trip() {
        let c = codec();
        for cmd in [
            StorageCommand::CreateNamespace {
                name: "ns".into(),
                data: json!({"metadata": {"name": "ns"}}),
            },
            StorageCommand::DeleteNamespace { name: "ns".into() },
            StorageCommand::DeleteNamespaceContents { name: "ns".into() },
        ] {
            let bytes = c.encode_command(&cmd).unwrap();
            assert_eq!(c.decode_command(&bytes).unwrap(), cmd);
        }
    }

    #[test]
    fn command_node_subnet_round_trip() {
        let c = codec();
        let cmd = StorageCommand::AllocateNodeSubnet {
            node_name: "n1".into(),
            subnet: "10.244.0.0/24".into(),
            node_ip: "192.168.1.1".into(),
        };
        let bytes = c.encode_command(&cmd).unwrap();
        assert_eq!(c.decode_command(&bytes).unwrap(), cmd);
    }

    #[test]
    fn command_pod_slot_round_trip_each_variant() {
        let c = codec();
        for cmd in [
            StorageCommand::PodSlotTryAdmit {
                namespace: "ns".into(),
                pod_name: "p".into(),
                pod_uid: "u".into(),
                node_name: "n1".into(),
            },
            StorageCommand::PodSlotMarkTerminating {
                namespace: "ns".into(),
                pod_name: "p".into(),
                pod_uid: "u".into(),
                node_name: "n1".into(),
            },
            StorageCommand::PodSlotClearIfUid {
                namespace: "ns".into(),
                pod_name: "p".into(),
                pod_uid: "u".into(),
                node_name: "n1".into(),
            },
        ] {
            let bytes = c.encode_command(&cmd).unwrap();
            assert_eq!(c.decode_command(&bytes).unwrap(), cmd);
        }
    }

    #[test]
    fn command_decode_rejects_malformed_bytes() {
        let c = codec();
        let err = c.decode_command(&[0xff, 0xff]).unwrap_err();
        assert!(!format!("{err}").is_empty());
    }

    // ---- meta codec ----------------------------------------------------

    #[test]
    fn meta_round_trip_preserves_all_fields() {
        let c = codec();
        let m = meta();
        let bytes = c.encode_meta(&m).unwrap();
        let decoded = c.decode_meta(&bytes).unwrap();
        assert_eq!(decoded.command_id, m.command_id);
        assert_eq!(decoded.codec_version, m.codec_version);
        assert_eq!(decoded.resource_version, m.resource_version);
        assert_eq!(decoded.uid, m.uid);
        assert_eq!(decoded.timestamp_ms, m.timestamp_ms);
        assert_eq!(decoded.authoring_node, m.authoring_node);
    }

    #[test]
    fn meta_round_trip_with_none_uid() {
        let c = codec();
        let mut m = meta();
        m.uid = None;
        let bytes = c.encode_meta(&m).unwrap();
        let decoded = c.decode_meta(&bytes).unwrap();
        assert_eq!(decoded.uid, None);
    }

    #[test]
    fn meta_decode_rejects_malformed_bytes() {
        let c = codec();
        let err = c.decode_meta(&[0xff, 0xff]).unwrap_err();
        assert!(!format!("{err}").is_empty());
    }
}
