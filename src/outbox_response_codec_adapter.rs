use crate::datastore::OutboxResponseCodec;

pub(crate) struct ClusterDatastoreOutboxResponseCodec;

impl OutboxResponseCodec for ClusterDatastoreOutboxResponseCodec {
    fn encode(&self, response: &klights_cluster_core::StorageResponse) -> Result<Vec<u8>, String> {
        crate::replication::outbox_response_wire::encode_outbox_response(response)
            .map_err(|error| error.to_string())
    }

    fn decode(&self, bytes: &[u8]) -> Result<klights_cluster_core::StorageResponse, String> {
        crate::replication::outbox_response_wire::decode_outbox_response(bytes)
            .map_err(|error| error.to_string())
    }
}

pub(crate) fn new_codec() -> std::sync::Arc<dyn OutboxResponseCodec> {
    std::sync::Arc::new(ClusterDatastoreOutboxResponseCodec)
}
