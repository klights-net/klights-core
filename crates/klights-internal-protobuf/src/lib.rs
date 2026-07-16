//! Generated internal Raft and RPC wire definitions for klights.

pub mod log_apply;
pub mod storage;

tonic::include_proto!("klights.replication");

/// Encoded descriptor set for the internal replication service.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("klights_replication_descriptor");

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::{
        DataplanePeer, FollowerMessage, JoinAccepted, JoinAsControlplaneRequest, JoinRequest,
        JoinRole, LeaderMessage, MetadataRequest, MetadataResponse, NodeExecRequest,
        NodeExecStreamFrame, ObserveLeaderEndpointRequest, ObservedLeaderEndpoint,
        ReplicationEntry, StreamAck, follower_message, leader_message,
    };

    const COMMITTED_APPLY_RV_V1: u64 = 1 << 0;

    #[test]
    fn metadata_supported_features_defaults_to_zero_and_round_trips_v1() {
        let legacy = MetadataResponse {
            cluster_id: "legacy".to_string(),
            leader_epoch: 1,
            current_rv: 2,
            current_log_index: 3,
            supported_features: 0,
        };
        let legacy_bytes = legacy.encode_to_vec();
        assert_eq!(
            MetadataResponse::decode(legacy_bytes.as_slice())
                .unwrap()
                .supported_features,
            0
        );

        let v1 = MetadataResponse {
            supported_features: COMMITTED_APPLY_RV_V1,
            ..legacy
        };
        assert_eq!(
            MetadataResponse::decode(v1.encode_to_vec().as_slice())
                .unwrap()
                .supported_features,
            COMMITTED_APPLY_RV_V1
        );
    }

    #[test]
    fn join_controlplane_supported_features_round_trip() {
        let request = JoinAsControlplaneRequest {
            supported_features: COMMITTED_APPLY_RV_V1,
            ..Default::default()
        };
        assert_eq!(
            JoinAsControlplaneRequest::decode(request.encode_to_vec().as_slice())
                .unwrap()
                .supported_features,
            COMMITTED_APPLY_RV_V1
        );
    }

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
            payload: Some(leader_message::Payload::JoinResponse(super::JoinResponse {
                result: Some(super::join_response::Result::Accepted(JoinAccepted {
                    cluster_id: "cluster-a".to_string(),
                    leader_epoch: 1,
                    current_rv: 42,
                    peers: vec![DataplanePeer {
                        node_name: "leader".to_string(),
                        pod_cidr: "10.42.0.0/24".to_string(),
                        public_key: "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB=".to_string(),
                        endpoint: "192.0.2.1".to_string(),
                        port: 51_820,
                        mode: "rootless".to_string(),
                        encryption: "enabled".to_string(),
                    }],
                })),
            })),
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
            supported_features: COMMITTED_APPLY_RV_V1,
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
    fn storage_wire_tags_remain_stable() {
        let meta = crate::storage::ProtoCommandMeta {
            command_id: "x".to_string(),
            codec_version: 2,
            resource_version: 3,
            uid: None,
            timestamp_ms: 4,
            authoring_node: "n".to_string(),
        };
        assert_eq!(
            meta.encode_to_vec(),
            vec![
                0x0a, 0x01, b'x', 0x10, 0x02, 0x18, 0x03, 0x28, 0x04, 0x32, 0x01, b'n'
            ]
        );

        let command = crate::storage::ProtoStorageCommand {
            command: Some(
                crate::storage::proto_storage_command::Command::DeleteResourceWithTombstone(
                    crate::storage::ProtoDeleteResourceWithTombstone {
                        api_version: String::new(),
                        kind: String::new(),
                        namespace: None,
                        name: String::new(),
                        preconditions: None,
                        grace_seconds: 0,
                    },
                ),
            ),
        };
        assert_eq!(command.encode_to_vec(), vec![0xe2, 0x01, 0x00]);
    }

    #[test]
    fn log_apply_wire_tags_remain_stable() {
        let commit = crate::log_apply::ProtoLogApplyCommit {
            resource_version: 7,
            mutations: Vec::new(),
            outbox_watermark: None,
            resource_version_assignment:
                crate::log_apply::ProtoResourceVersionAssignment::CommittedApplyV1 as i32,
        };
        assert_eq!(commit.encode_to_vec(), vec![0x08, 0x07, 0x20, 0x01]);

        let mutation = crate::log_apply::ProtoLogApplyMutation {
            mutation: Some(
                crate::log_apply::proto_log_apply_mutation::Mutation::GcWatchEvents(
                    crate::log_apply::ProtoLogApplyWatchEventsGc {
                        max_rows: 0,
                        batch_cap: 0,
                    },
                ),
            ),
        };
        assert_eq!(mutation.encode_to_vec(), vec![0xaa, 0x01, 0x00]);
    }
}
