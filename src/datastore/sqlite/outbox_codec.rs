use crate::datastore::OutboxResponseCodec;

pub(crate) struct TransactionContext<'a> {
    outbox_codec: &'a dyn OutboxResponseCodec,
}

impl<'a> TransactionContext<'a> {
    pub(crate) fn new(outbox_codec: &'a dyn OutboxResponseCodec) -> Self {
        Self { outbox_codec }
    }

    pub(crate) fn encode(
        &self,
        response: &klights_cluster_core::StorageResponse,
    ) -> Result<Vec<u8>, String> {
        self.outbox_codec.encode(response)
    }

    pub(crate) fn decode(
        &self,
        bytes: &[u8],
    ) -> Result<klights_cluster_core::StorageResponse, String> {
        self.outbox_codec.decode(bytes)
    }
}

#[cfg(test)]
pub(crate) fn encode(response: &klights_cluster_core::StorageResponse) -> Result<Vec<u8>, String> {
    crate::outbox_response_codec_adapter::new_codec().encode(response)
}

#[cfg(test)]
pub(crate) fn decode(bytes: &[u8]) -> Result<klights_cluster_core::StorageResponse, String> {
    crate::outbox_response_codec_adapter::new_codec().decode(bytes)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct FakeCodec {
        encodes: AtomicUsize,
        decodes: AtomicUsize,
        fail: bool,
    }

    impl OutboxResponseCodec for FakeCodec {
        fn encode(
            &self,
            _response: &klights_cluster_core::StorageResponse,
        ) -> Result<Vec<u8>, String> {
            self.encodes.fetch_add(1, Ordering::Relaxed);
            if self.fail {
                Err("fake encode error".to_string())
            } else {
                Ok(vec![3])
            }
        }

        fn decode(&self, _bytes: &[u8]) -> Result<klights_cluster_core::StorageResponse, String> {
            self.decodes.fetch_add(1, Ordering::Relaxed);
            if self.fail {
                Err("fake decode error".to_string())
            } else {
                Ok(klights_cluster_core::StorageResponse::Ack {
                    resource_version: 7,
                })
            }
        }
    }

    #[test]
    fn explicit_context_uses_exact_injected_codec_and_propagates_errors() {
        let success = FakeCodec {
            encodes: AtomicUsize::new(0),
            decodes: AtomicUsize::new(0),
            fail: false,
        };
        let context = TransactionContext::new(&success);
        assert_eq!(
            context
                .encode(&klights_cluster_core::StorageResponse::Ack {
                    resource_version: 7,
                })
                .unwrap(),
            vec![3]
        );
        assert!(matches!(
            context.decode(&[3]).unwrap(),
            klights_cluster_core::StorageResponse::Ack {
                resource_version: 7
            }
        ));
        assert_eq!(success.encodes.load(Ordering::Relaxed), 1);
        assert_eq!(success.decodes.load(Ordering::Relaxed), 1);

        let failing = FakeCodec {
            encodes: AtomicUsize::new(0),
            decodes: AtomicUsize::new(0),
            fail: true,
        };
        let context = TransactionContext::new(&failing);
        assert_eq!(
            context
                .encode(&klights_cluster_core::StorageResponse::Ack {
                    resource_version: 0,
                })
                .unwrap_err(),
            "fake encode error"
        );
        assert_eq!(context.decode(&[]).unwrap_err(), "fake decode error");
    }
}
