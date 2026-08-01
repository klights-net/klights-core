#[cfg(test)]
pub(crate) fn encode(response: &klights_cluster_core::StorageResponse) -> Result<Vec<u8>, String> {
    crate::test_fixtures::outbox::new_codec().encode(response)
}

#[cfg(test)]
pub(crate) fn decode(bytes: &[u8]) -> Result<klights_cluster_core::StorageResponse, String> {
    crate::test_fixtures::outbox::new_codec().decode(bytes)
}
