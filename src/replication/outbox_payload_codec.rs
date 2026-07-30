use std::sync::Arc;

use klights_leader_api::OutboxPayloadCodec;

pub(crate) fn new_codec() -> Arc<dyn OutboxPayloadCodec> {
    Arc::new(klights_leader_rpc::storage_wire_codec::InternalProtobufOutboxPayloadCodec)
}
