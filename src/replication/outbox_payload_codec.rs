use std::sync::Arc;

use klights_leader_api::{OutboxPayloadCodec, OutboxPayloadCodecError};

pub(crate) struct InternalProtobufOutboxPayloadCodec;

impl OutboxPayloadCodec for InternalProtobufOutboxPayloadCodec {
    fn encode(
        &self,
        command: &klights_cluster_core::StorageCommand,
    ) -> Result<Arc<[u8]>, OutboxPayloadCodecError> {
        klights_leader_rpc::storage_wire_codec::encode_outbox_payload_protobuf(
            &klights_cluster_core::OutboxPayload::new(command.clone()),
        )
        .map(Arc::from)
        .map_err(|error| OutboxPayloadCodecError::encoding_failed(error.to_string()))
    }

    fn decode(
        &self,
        payload: &[u8],
    ) -> Result<klights_cluster_core::StorageCommand, OutboxPayloadCodecError> {
        klights_leader_rpc::storage_wire_codec::decode_outbox_payload_protobuf(payload)
            .map(klights_cluster_core::OutboxPayload::into_command)
            .map_err(|error| OutboxPayloadCodecError::invalid_payload(error.to_string()))
    }
}

pub(crate) fn new_codec() -> Arc<dyn OutboxPayloadCodec> {
    Arc::new(InternalProtobufOutboxPayloadCodec)
}
