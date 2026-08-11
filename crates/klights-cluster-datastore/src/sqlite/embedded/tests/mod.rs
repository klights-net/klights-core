#![cfg(test)]

use super::*;
use crate::test_fixtures::live_apply::TestCommittedApplyResult;
use serde_json::json;

async fn create_and_fetch_via_focused_ports(db: &Datastore) -> Result<Option<Resource>> {
    klights_cluster_store::ClusterResourceMutation::create_resource(
        db,
        "v1",
        "ConfigMap",
        Some("default"),
        "trait-cm",
        json!({"metadata": {"name": "trait-cm"}, "data": {"k":"v"}}),
    )
    .await?;
    klights_cluster_store::ClusterResourceRead::get_resource(
        db.focused_read_store().as_ref(),
        klights_cluster_store::ResourceGetRequest::new(
            "v1",
            "ConfigMap",
            Some("default".to_string()),
            "trait-cm",
        ),
    )
    .await
    .map_err(anyhow::Error::new)
}

mod applied_outbox_gc_tests;
mod backend_trait_and_core_crud_tests;
mod encryption_tests;
mod event_compat_tests;
mod fingerprint_tests;
mod ipam_and_network_tests;
mod live_apply_coordinator_tests;
mod namespace_and_watch_tests;
mod owner_reference_query_tests;
mod pod_status_stamp_tests;
mod resource_quota_crud_tests;
mod selector_index_tests;
mod selectors_and_filter_tests;
mod status_subresource_tests;
