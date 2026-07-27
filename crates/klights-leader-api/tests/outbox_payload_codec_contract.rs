use std::sync::Arc;

use klights_cluster_core::{ResourcePreconditions, StorageCommand};
use klights_leader_api::{OutboxPayloadCodec, OutboxPayloadCodecError};

struct FakeCodec;

impl OutboxPayloadCodec for FakeCodec {
    fn encode(&self, command: &StorageCommand) -> Result<Arc<[u8]>, OutboxPayloadCodecError> {
        Ok(Arc::from(serde_json::to_vec(command).unwrap()))
    }

    fn decode(&self, payload: &[u8]) -> Result<StorageCommand, OutboxPayloadCodecError> {
        serde_json::from_slice(payload)
            .map_err(|error| OutboxPayloadCodecError::invalid_payload(error.to_string()))
    }
}

fn assert_object_safe(_: &dyn OutboxPayloadCodec) {}

#[test]
fn durable_outbox_codec_is_transport_neutral_and_object_safe() {
    let codec = FakeCodec;
    assert_object_safe(&codec);
    let command = StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "pod-a".to_string(),
        status: serde_json::json!({"phase": "Running"}),
        expected_rv: None,
        preconditions: ResourcePreconditions::uid("uid-a"),
        observed_status_stamp: None,
    };

    let encoded = codec.encode(&command).unwrap();
    assert_eq!(codec.decode(encoded.as_ref()).unwrap(), command);
}
