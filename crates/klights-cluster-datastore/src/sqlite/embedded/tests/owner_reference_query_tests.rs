#![cfg(test)]

use super::*;
use serde_json::json;

#[tokio::test]
async fn test_list_resources_with_field_selector_filters_events() {
    let db = Datastore::new_in_memory().await.unwrap();

    // Create two events for different pods
    let event_a = json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {"name": "pod-a.evt1", "namespace": "default"},
        "involvedObject": {"name": "pod-a", "namespace": "default", "uid": "uid-a", "kind": "Pod"},
        "reason": "Started",
        "message": "Started container in pod-a"
    });
    db.create_resource("v1", "Event", Some("default"), "pod-a.evt1", event_a)
        .await
        .unwrap();

    let event_b = json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {"name": "pod-b.evt1", "namespace": "default"},
        "involvedObject": {"name": "pod-b", "namespace": "default", "uid": "uid-b", "kind": "Pod"},
        "reason": "Pulling",
        "message": "Pulling image for pod-b"
    });
    db.create_resource("v1", "Event", Some("default"), "pod-b.evt1", event_b)
        .await
        .unwrap();

    // Without field selector: returns all events
    let all = db
        .list_resources(
            "v1",
            "Event",
            Some("default"),
            klights_cluster_store::ResourceListOptions::all(),
        )
        .await
        .unwrap();
    assert_eq!(all.items.len(), 2, "Without selector, both events returned");

    // With field selector: only pod-a events
    let filtered = db
        .list_resources(
            "v1",
            "Event",
            Some("default"),
            klights_cluster_store::ResourceListOptions::new(
                None,
                Some("involvedObject.name=pod-a"),
                None,
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        filtered.items.len(),
        1,
        "Field selector should filter to pod-a events only"
    );
    assert_eq!(filtered.items[0].name, "pod-a.evt1");
}
