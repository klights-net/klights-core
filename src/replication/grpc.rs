pub mod generated {
    tonic::include_proto!("klights.replication");
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("klights_replication_descriptor");

pub mod client;
pub mod fanout;
pub mod raft_rpc;
pub mod server;
pub mod snapshot_cache;
pub mod transport_policy;

pub use transport_policy::{GrpcTransportPolicy, SharedGrpcTransportPolicy};

use anyhow::Result;

mod ca_files;
pub const JOIN_TOKEN_METADATA_KEY: &str = "x-klights-join-token";

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
    use prost::Message;
    use serde_json::json;

    use crate::replication::grpc::generated::{
        DataplanePeer, FollowerMessage, JoinAccepted, JoinRequest, JoinRole, LeaderMessage,
        MetadataRequest, MetadataResponse, NodeExecRequest, NodeExecStreamFrame,
        ObserveLeaderEndpointRequest, ObservedLeaderEndpoint, ReplicationEntry, StreamAck,
        follower_message, leader_message,
    };
    use crate::{
        datastore::command::{COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand},
        replication::{grpc, protocol},
    };

    #[test]
    fn proto_generated_messages_round_trip() {
        let follower_join = FollowerMessage {
            payload: Some(follower_message::Payload::Join(JoinRequest {
                token: "join-token".to_string(),
                node_name: "worker-1".to_string(),
                role: JoinRole::Worker as i32,
                dataplane_public_key: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
                dataplane_endpoint: "192.0.2.10".to_string(),
                dataplane_port: 51_820,
                dataplane_mode: "root".to_string(),
                dataplane_encryption: "enabled".to_string(),
            })),
        };
        let follower_ack = FollowerMessage {
            payload: Some(follower_message::Payload::Ack(StreamAck { applied_rv: 42 })),
        };
        let leader_join = LeaderMessage {
            payload: Some(leader_message::Payload::JoinResponse(
                crate::replication::grpc::generated::JoinResponse {
                    result: Some(
                        crate::replication::grpc::generated::join_response::Result::Accepted(
                            JoinAccepted {
                                cluster_id: "cluster-a".to_string(),
                                leader_epoch: 1,
                                current_rv: 42,
                                peers: vec![DataplanePeer {
                                    node_name: "leader".to_string(),
                                    pod_cidr: "10.42.0.0/24".to_string(),
                                    public_key: "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB="
                                        .to_string(),
                                    endpoint: "192.0.2.1".to_string(),
                                    port: 51_820,
                                    mode: "rootless".to_string(),
                                    encryption: "enabled".to_string(),
                                }],
                            },
                        ),
                    ),
                },
            )),
        };
        let leader_exec_request = LeaderMessage {
            payload: Some(leader_message::Payload::NodeExecRequest(NodeExecRequest {
                request_id: "exec-1".to_string(),
                node_name: "worker-1".to_string(),
                namespace: "default".to_string(),
                pod_name: "remote-exec".to_string(),
                container_id: "container-a".to_string(),
                command: vec!["/bin/sh".to_string()],
                tty: true,
                stdin: true,
                stdout: true,
                stderr: true,
                attach: false,
            })),
        };
        let exec_frame = NodeExecStreamFrame {
            request_id: "exec-1".to_string(),
            channel: "stdin".to_string(),
            data: b"echo ok\n".to_vec(),
            fin: false,
        };
        let leader_exec_frame = LeaderMessage {
            payload: Some(leader_message::Payload::NodeExecStreamFrame(
                exec_frame.clone(),
            )),
        };
        let follower_exec_frame = FollowerMessage {
            payload: Some(follower_message::Payload::NodeExecStreamFrame(exec_frame)),
        };
        let follower_observed_endpoint = FollowerMessage {
            payload: Some(follower_message::Payload::ObservedLeaderEndpoint(
                ObservedLeaderEndpoint {
                    endpoint: "10.99.0.10".to_string(),
                },
            )),
        };
        let leader_observe_endpoint_request = LeaderMessage {
            payload: Some(leader_message::Payload::ObserveLeaderEndpointRequest(
                ObserveLeaderEndpointRequest {},
            )),
        };
        let metadata_request = MetadataRequest {};
        let metadata_response = MetadataResponse {
            cluster_id: "cluster-a".to_string(),
            leader_epoch: 1,
            current_rv: 42,
            current_log_index: 7,
        };
        let entry = ReplicationEntry {
            command_protobuf: vec![1, 2, 3],
            meta_protobuf: vec![4, 5, 6],
            log_index: 9,
            term: 0,
            commit_protobuf: vec![7, 8, 9],
        };

        assert_eq!(
            FollowerMessage::decode(follower_join.encode_to_vec().as_slice()).unwrap(),
            follower_join
        );
        assert_eq!(
            FollowerMessage::decode(follower_ack.encode_to_vec().as_slice()).unwrap(),
            follower_ack
        );
        assert_eq!(
            LeaderMessage::decode(leader_join.encode_to_vec().as_slice()).unwrap(),
            leader_join
        );
        assert_eq!(
            LeaderMessage::decode(leader_exec_request.encode_to_vec().as_slice()).unwrap(),
            leader_exec_request
        );
        assert_eq!(
            LeaderMessage::decode(leader_exec_frame.encode_to_vec().as_slice()).unwrap(),
            leader_exec_frame
        );
        assert_eq!(
            FollowerMessage::decode(follower_exec_frame.encode_to_vec().as_slice()).unwrap(),
            follower_exec_frame
        );
        assert_eq!(
            FollowerMessage::decode(follower_observed_endpoint.encode_to_vec().as_slice()).unwrap(),
            follower_observed_endpoint
        );
        assert_eq!(
            LeaderMessage::decode(leader_observe_endpoint_request.encode_to_vec().as_slice())
                .unwrap(),
            leader_observe_endpoint_request
        );
        assert_eq!(
            MetadataRequest::decode(metadata_request.encode_to_vec().as_slice()).unwrap(),
            metadata_request
        );
        assert_eq!(
            MetadataResponse::decode(metadata_response.encode_to_vec().as_slice()).unwrap(),
            metadata_response
        );
        assert_eq!(
            ReplicationEntry::decode(entry.encode_to_vec().as_slice()).unwrap(),
            entry
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
