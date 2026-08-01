#![cfg(test)]

use std::sync::Arc;

use klights_cluster_core::{StorageCommand, StorageResponse};
use klights_cluster_store::OutboxResponseCodec;

pub(crate) struct JsonOutboxResponseCodec;

impl OutboxResponseCodec for JsonOutboxResponseCodec {
    fn encode(&self, response: &StorageResponse) -> Result<Vec<u8>, String> {
        serde_json::to_vec(response).map_err(|error| error.to_string())
    }

    fn decode(&self, bytes: &[u8]) -> Result<StorageResponse, String> {
        serde_json::from_slice(bytes).map_err(|error| error.to_string())
    }
}

pub(crate) fn new_codec() -> Arc<dyn OutboxResponseCodec> {
    Arc::new(JsonOutboxResponseCodec)
}

pub(crate) struct EncodedOutboxCommand(Vec<u8>);

impl EncodedOutboxCommand {
    pub(crate) fn from_command(command: StorageCommand) -> Self {
        Self(serde_json::to_vec(&command).expect("encode destination test command"))
    }

    pub(crate) fn encode_protobuf(self) -> Result<Vec<u8>, String> {
        Ok(self.0)
    }
}

pub(crate) fn test_outbox_command(bytes: &[u8]) -> StorageCommand {
    serde_json::from_slice(bytes).expect("decode destination test command")
}
