use super::*;
use serde_json::json;
#[test]
fn test_resolve_field_path_top_level() {
    let data = json!({"reason": "Started", "type": "Normal"});
    assert_eq!(
        resolve_field_path(&data, "reason").as_deref(),
        Some("Started")
    );
    assert_eq!(resolve_field_path(&data, "type").as_deref(), Some("Normal"));
}

#[test]
fn test_resolve_field_path_nested() {
    let data = json!({"involvedObject": {"name": "my-pod", "uid": "abc-123"}});
    assert_eq!(
        resolve_field_path(&data, "involvedObject.name").as_deref(),
        Some("my-pod")
    );
    assert_eq!(
        resolve_field_path(&data, "involvedObject.uid").as_deref(),
        Some("abc-123")
    );
}

#[test]
fn test_resolve_field_path_missing_returns_none() {
    let data = json!({"metadata": {"name": "test"}});
    assert_eq!(resolve_field_path(&data, "involvedObject.name"), None);
    assert_eq!(resolve_field_path(&data, "nonexistent"), None);
}

#[test]
fn test_resolve_field_path_boolean() {
    let data = json!({"spec": {"unschedulable": false}});
    assert_eq!(
        resolve_field_path(&data, "spec.unschedulable").as_deref(),
        Some("false")
    );
    let data2 = json!({"spec": {"unschedulable": true}});
    assert_eq!(
        resolve_field_path(&data2, "spec.unschedulable").as_deref(),
        Some("true")
    );
}

#[test]
fn test_filter_by_field_selector_involvedobject_name_filters_correctly() {
    let items = vec![
        Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-a.event1".to_string(),
            uid: "uid-event-a-1".to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(json!({
                "involvedObject": {"name": "pod-a", "uid": "uid-a", "kind": "Pod"},
                "reason": "Started",
                "message": "Started container"
            })),
        },
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-b.event1".to_string(),
            uid: "uid-event-b-1".to_string(),
            resource_version: 2,
            data: std::sync::Arc::new(json!({
                "involvedObject": {"name": "pod-b", "uid": "uid-b", "kind": "Pod"},
                "reason": "Pulling",
                "message": "Pulling image"
            })),
        },
    ];

    let filtered = filter_by_field_selector(items, "involvedObject.name=pod-a");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "pod-a.event1");
}

#[test]
fn test_filter_by_field_selector_multiple_conditions_all_applied() {
    let items = vec![
        Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-a.event1".to_string(),
            uid: "uid-event-a-1".to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(json!({
                "involvedObject": {"name": "pod-a", "uid": "uid-a", "kind": "Pod"},
                "reason": "Started"
            })),
        },
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-a.event2".to_string(),
            uid: "uid-event-a-2".to_string(),
            resource_version: 2,
            data: std::sync::Arc::new(json!({
                "involvedObject": {"name": "pod-a", "uid": "uid-a-different", "kind": "Pod"},
                "reason": "Failed"
            })),
        },
    ];

    // Both conditions must match
    let filtered =
        filter_by_field_selector(items, "involvedObject.name=pod-a,involvedObject.uid=uid-a");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "pod-a.event1");
}

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
            crate::datastore::ResourceListQuery::all(),
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
            crate::datastore::ResourceListQuery::new(
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

#[test]
fn test_filter_by_field_selector_inequality() {
    let items = vec![
        Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "evt-normal".to_string(),
            uid: "uid-event-normal".to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(json!({"type": "Normal"})),
        },
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "evt-warning".to_string(),
            uid: "uid-event-warning".to_string(),
            resource_version: 2,
            data: std::sync::Arc::new(json!({"type": "Warning"})),
        },
    ];

    let filtered = filter_by_field_selector(items, "type!=Normal");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "evt-warning");
}

#[test]
fn test_filter_by_field_selector_metadata_name() {
    let items = vec![
        Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-a".to_string(),
            uid: "uid-pod-a".to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(
                json!({"metadata": {"name": "pod-a"}, "status": {"phase": "Running"}}),
            ),
        },
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-b".to_string(),
            uid: "uid-pod-b".to_string(),
            resource_version: 2,
            data: std::sync::Arc::new(
                json!({"metadata": {"name": "pod-b"}, "status": {"phase": "Pending"}}),
            ),
        },
    ];

    let filtered = filter_by_field_selector(items.clone(), "metadata.name=pod-a");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "pod-a");

    let filtered = filter_by_field_selector(items, "status.phase=Running");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "pod-a");
}

#[test]
fn test_filter_by_field_selector_empty_returns_all() {
    let items = vec![Resource {
        id: 0,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "test".to_string(),
        uid: "uid-test".to_string(),
        resource_version: 1,
        data: std::sync::Arc::new(json!({})),
    }];
    let filtered = filter_by_field_selector(items, "");
    assert_eq!(filtered.len(), 1);
}
