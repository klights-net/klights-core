use std::sync::Arc;

use klights_cluster_core::Resource;
use klights_leader_api::{ResourceEvent, WatchEventType, WatchRequest};
use klights_watch::WatchSelectorMembership;

fn request(label_selector: Option<&str>, field_selector: Option<&str>) -> WatchRequest {
    WatchRequest::try_new(
        "v1",
        if field_selector.is_some() {
            "Pod"
        } else {
            "ConfigMap"
        },
        None,
        label_selector.map(str::to_string),
        field_selector.map(str::to_string),
        Some(0),
        None,
    )
    .unwrap()
}

fn resource(
    namespace: Option<&str>,
    name: &str,
    resource_version: i64,
    selected: bool,
) -> Resource {
    let mut metadata = serde_json::json!({
        "name": name,
        "uid": format!("uid-{name}"),
        "labels": {"track": if selected { "yes" } else { "no" }},
        "resourceVersion": resource_version.to_string()
    });
    if let Some(namespace) = namespace {
        metadata["namespace"] = serde_json::Value::String(namespace.to_string());
    }
    Resource::try_from_data(Arc::new(serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": metadata,
        "data": {"version": resource_version.to_string()}
    })))
    .unwrap()
}

fn pod(node_name: &str, resource_version: i64) -> Resource {
    Resource::try_from_data(Arc::new(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": "default",
            "name": "moving",
            "uid": "uid-moving",
            "resourceVersion": resource_version.to_string()
        },
        "spec": {"nodeName": node_name}
    })))
    .unwrap()
}

fn event(event_type: WatchEventType, resource: Resource) -> ResourceEvent {
    ResourceEvent::try_new(event_type, resource, None).unwrap()
}

fn apply(membership: &mut WatchSelectorMembership, event: ResourceEvent) -> Option<ResourceEvent> {
    let pending = membership.prepare(event).unwrap();
    let projected = pending.event().cloned();
    membership.commit(pending);
    projected
}

#[test]
fn same_name_different_namespace_transitions_are_independent() {
    let mut membership =
        WatchSelectorMembership::try_new(&request(Some("track=yes"), None)).unwrap();
    assert_eq!(
        apply(
            &mut membership,
            event(
                WatchEventType::Added,
                resource(Some("a"), "shared", 1, true)
            )
        )
        .unwrap()
        .event_type(),
        WatchEventType::Added
    );
    apply(
        &mut membership,
        event(
            WatchEventType::Added,
            resource(Some("b"), "shared", 2, true),
        ),
    )
    .unwrap();
    assert_eq!(membership.len(), 2);

    let left = apply(
        &mut membership,
        event(
            WatchEventType::Modified,
            resource(Some("a"), "shared", 3, false),
        ),
    )
    .unwrap();
    assert_eq!(left.event_type(), WatchEventType::Deleted);
    assert_eq!(left.resource().namespace.as_deref(), Some("a"));
    assert_eq!(membership.len(), 1);

    let still_selected = apply(
        &mut membership,
        event(
            WatchEventType::Modified,
            resource(Some("b"), "shared", 4, true),
        ),
    )
    .unwrap();
    assert_eq!(still_selected.event_type(), WatchEventType::Modified);
}

#[test]
fn field_selector_leave_synthesizes_deleted_from_cached_object() {
    let mut membership =
        WatchSelectorMembership::try_new(&request(None, Some("spec.nodeName=node-a"))).unwrap();
    let selected = pod("node-a", 1);
    apply(
        &mut membership,
        event(WatchEventType::Added, selected.clone()),
    )
    .unwrap();

    let deleted = apply(
        &mut membership,
        event(WatchEventType::Modified, pod("node-b", 2)),
    )
    .unwrap();
    assert_eq!(deleted.event_type(), WatchEventType::Deleted);
    assert_eq!(deleted.resource().data["spec"]["nodeName"], "node-a");
    assert_eq!(deleted.resource().resource_version, 1);
    assert!(membership.is_empty());
}

#[test]
fn cluster_scoped_none_namespace_transition_is_preserved() {
    let mut membership =
        WatchSelectorMembership::try_new(&request(Some("track=yes"), None)).unwrap();
    let projected = apply(
        &mut membership,
        event(
            WatchEventType::Added,
            resource(None, "cluster-object", 1, true),
        ),
    )
    .unwrap();
    assert_eq!(projected.resource().namespace, None);
    assert_eq!(membership.len(), 1);
}

#[test]
fn cluster_scoped_baseline_rejects_namespaced_resource() {
    let mut membership =
        WatchSelectorMembership::try_new(&request(Some("track=yes"), None)).unwrap();
    membership.replace(&[resource(None, "same", 1, true)]);
    let added = apply(
        &mut membership,
        event(
            WatchEventType::Added,
            resource(Some("default"), "same", 2, true),
        ),
    )
    .unwrap();
    assert_eq!(added.event_type(), WatchEventType::Added);
    assert_eq!(membership.len(), 2);
}

#[test]
fn selector_leave_uses_the_last_matching_object() {
    let mut membership =
        WatchSelectorMembership::try_new(&request(Some("track=yes"), None)).unwrap();
    apply(
        &mut membership,
        event(
            WatchEventType::Added,
            resource(Some("default"), "selected", 1, true),
        ),
    )
    .unwrap();
    apply(
        &mut membership,
        event(
            WatchEventType::Modified,
            resource(Some("default"), "selected", 2, true),
        ),
    )
    .unwrap();

    let deleted = apply(
        &mut membership,
        event(
            WatchEventType::Modified,
            resource(Some("default"), "selected", 3, false),
        ),
    )
    .unwrap();
    assert_eq!(deleted.event_type(), WatchEventType::Deleted);
    assert_eq!(deleted.resource().resource_version, 2);
    assert_eq!(
        deleted.resource().data["metadata"]["labels"]["track"],
        "yes"
    );
}
