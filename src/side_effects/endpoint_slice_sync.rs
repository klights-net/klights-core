//! Side effect to sync service rules after EndpointSlice changes.

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_endpoint_slice_sync_name() {
        let effect = crate::endpoint_slice_sync_side_effect_adapter::effect(None);
        assert_eq!(effect.name(), "endpoint_slice_sync");
    }
}
