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

    const EXACT_COMMAND_CODEC_V3: u32 = 3;

    #[test]
    fn private_stream_envelope_tags_preserve_every_preexisting_payload() {
        let follower_cases = [
            (
                "join",
                FollowerMessage {
                    payload: Some(follower_message::Payload::Join(Default::default())),
                },
                0x0a,
            ),
            (
                "ack",
                FollowerMessage {
                    payload: Some(follower_message::Payload::Ack(Default::default())),
                },
                0x1a,
            ),
            (
                "exec_sync",
                FollowerMessage {
                    payload: Some(follower_message::Payload::NodeExecSyncResponse(
                        Default::default(),
                    )),
                },
                0x22,
            ),
            (
                "logs",
                FollowerMessage {
                    payload: Some(follower_message::Payload::PodLogResponse(Default::default())),
                },
                0x2a,
            ),
            (
                "exec_stream",
                FollowerMessage {
                    payload: Some(follower_message::Payload::NodeExecStreamFrame(
                        Default::default(),
                    )),
                },
                0x32,
            ),
            (
                "observed_endpoint",
                FollowerMessage {
                    payload: Some(follower_message::Payload::ObservedLeaderEndpoint(
                        Default::default(),
                    )),
                },
                0x3a,
            ),
            (
                "metrics",
                FollowerMessage {
                    payload: Some(follower_message::Payload::NodeMetricsResponse(
                        Default::default(),
                    )),
                },
                0x42,
            ),
        ];
        for (name, message, expected_key) in follower_cases {
            let encoded = message.encode_to_vec();
            assert_eq!(encoded, [expected_key, 0], "follower {name}");
            assert_eq!(
                FollowerMessage::decode(encoded.as_slice()).unwrap(),
                message
            );
        }

        let leader_cases = [
            (
                "join",
                LeaderMessage {
                    payload: Some(leader_message::Payload::JoinResponse(Default::default())),
                },
                0x0a,
            ),
            (
                "stream_item",
                LeaderMessage {
                    payload: Some(leader_message::Payload::StreamItem(Default::default())),
                },
                0x1a,
            ),
            (
                "exec_sync",
                LeaderMessage {
                    payload: Some(leader_message::Payload::NodeExecSyncRequest(
                        Default::default(),
                    )),
                },
                0x22,
            ),
            (
                "logs",
                LeaderMessage {
                    payload: Some(leader_message::Payload::PodLogRequest(Default::default())),
                },
                0x2a,
            ),
            (
                "exec_stream_request",
                LeaderMessage {
                    payload: Some(leader_message::Payload::NodeExecRequest(Default::default())),
                },
                0x32,
            ),
            (
                "exec_stream_frame",
                LeaderMessage {
                    payload: Some(leader_message::Payload::NodeExecStreamFrame(
                        Default::default(),
                    )),
                },
                0x3a,
            ),
            (
                "observe_endpoint",
                LeaderMessage {
                    payload: Some(leader_message::Payload::ObserveLeaderEndpointRequest(
                        Default::default(),
                    )),
                },
                0x42,
            ),
            (
                "metrics",
                LeaderMessage {
                    payload: Some(leader_message::Payload::NodeMetricsRequest(
                        Default::default(),
                    )),
                },
                0x4a,
            ),
        ];
        for (name, message, expected_key) in leader_cases {
            let encoded = message.encode_to_vec();
            assert_eq!(encoded, [expected_key, 0], "leader {name}");
            assert_eq!(LeaderMessage::decode(encoded.as_slice()).unwrap(), message);
        }
    }

    #[test]
    fn metadata_wire_preserves_missing_zero_for_exact_v3_adapter_rejection() {
        let missing = MetadataResponse {
            cluster_id: "missing-version".to_string(),
            leader_epoch: 1,
            current_rv: 2,
            current_log_index: 3,
            command_codec_version: 0,
        };
        let missing_bytes = missing.encode_to_vec();
        assert_eq!(
            MetadataResponse::decode(missing_bytes.as_slice())
                .unwrap()
                .command_codec_version,
            0
        );

        let exact_v3 = MetadataResponse {
            command_codec_version: EXACT_COMMAND_CODEC_V3,
            ..missing
        };
        assert_eq!(
            MetadataResponse::decode(exact_v3.encode_to_vec().as_slice())
                .unwrap()
                .command_codec_version,
            EXACT_COMMAND_CODEC_V3
        );
    }

    #[test]
    fn join_controlplane_command_codec_version_round_trip() {
        let request = JoinAsControlplaneRequest {
            command_codec_version: EXACT_COMMAND_CODEC_V3,
            ..Default::default()
        };
        assert_eq!(
            JoinAsControlplaneRequest::decode(request.encode_to_vec().as_slice())
                .unwrap()
                .command_codec_version,
            EXACT_COMMAND_CODEC_V3
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
                command_codec_version: 3,
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
            command_codec_version: EXACT_COMMAND_CODEC_V3,
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
    fn outbox_command_envelope_carries_exact_codec_provenance_before_opaque_payload() {
        let raw_v2_payload = vec![0x0a, 0x01, b'x', 0x10, 0x02];
        let envelope = crate::storage::ProtoOutboxCommandEnvelope {
            codec_version: 3,
            command_payload: raw_v2_payload.clone(),
        };
        assert_eq!(
            envelope.encode_to_vec(),
            vec![0x08, 0x03, 0x12, 0x05, 0x0a, 0x01, b'x', 0x10, 0x02]
        );
        assert_ne!(envelope.encode_to_vec(), raw_v2_payload);
        assert_eq!(
            crate::storage::ProtoOutboxCommandEnvelope::decode(envelope.encode_to_vec().as_slice())
                .unwrap(),
            envelope
        );
    }

    #[test]
    fn log_apply_wire_tags_remain_stable() {
        let commit = crate::log_apply::ProtoLogApplyCommit {
            resource_version: 0,
            mutations: Vec::new(),
            outbox_watermark: None,
        };
        assert_eq!(commit.encode_to_vec(), Vec::<u8>::new());

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

        let finalization = crate::log_apply::ProtoLogApplyMutation {
            mutation: Some(
                crate::log_apply::proto_log_apply_mutation::Mutation::FinalizeBoundPod(
                    crate::log_apply::ProtoLogApplyPodActorFinalization {
                        namespace: String::new(),
                        name: String::new(),
                        pod_uid: String::new(),
                        node_name: String::new(),
                    },
                ),
            ),
        };
        assert_eq!(finalization.encode_to_vec(), vec![0xb2, 0x01, 0x00]);
    }
}
