#[cfg(test)]
pub(crate) fn encode(response: &klights_cluster_core::StorageResponse) -> Result<Vec<u8>, String> {
    crate::outbox_response_codec_adapter::new_codec().encode(response)
}

#[cfg(test)]
pub(crate) fn decode(bytes: &[u8]) -> Result<klights_cluster_core::StorageResponse, String> {
    crate::outbox_response_codec_adapter::new_codec().decode(bytes)
}
