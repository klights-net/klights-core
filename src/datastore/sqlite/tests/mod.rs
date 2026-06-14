use super::*;
use crate::label_selector::LabelRequirement;
use serde_json::json;

async fn create_and_fetch_via_backend(db: &dyn DatastoreBackend) -> Result<Option<Resource>> {
    db.create_resource(
        "v1",
        "ConfigMap",
        Some("default"),
        "trait-cm",
        json!({"metadata": {"name": "trait-cm"}, "data": {"k":"v"}}),
    )
    .await?;
    db.get_resource("v1", "ConfigMap", Some("default"), "trait-cm")
        .await
}

mod backend_trait_and_core_crud_tests;
mod encryption_tests;
mod event_compat_tests;
mod fingerprint_tests;
mod ipam_and_network_tests;
mod namespace_and_watch_tests;
mod owner_reference_query_tests;
mod pod_endpoints_tests;
mod pod_workqueue_tests;
mod selector_index_tests;
mod selectors_and_filter_tests;
mod status_subresource_tests;
