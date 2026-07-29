//! Opaque response codec used by durable outbox persistence.
//!
//! Persistence owns only the stable cluster-core response value. Generated
//! protobuf wire types remain in the root adapter that implements this port.

use klights_cluster_core::StorageResponse;

pub trait OutboxResponseCodec: Send + Sync {
    fn encode(&self, response: &StorageResponse) -> Result<Vec<u8>, String>;
    fn decode(&self, bytes: &[u8]) -> Result<StorageResponse, String>;
}
