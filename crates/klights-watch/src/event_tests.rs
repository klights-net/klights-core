use crate::*;

#[test]
fn watch_event_filter_matches_hydrated_labels() {
    let event = WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "cm-with-labels",
            "namespace": "default",
            "resourceVersion": "42",
            "labels": {"watch-this-configmap": "multiple-watchers-A"}
        }
    }));

    assert!(event.matches_filter(
        "ConfigMap",
        Some("default"),
        Some("watch-this-configmap=multiple-watchers-A"),
    ));
    assert!(!event.matches_filter(
        "ConfigMap",
        Some("default"),
        Some("watch-this-configmap=multiple-watchers-B"),
    ));
    assert!(event.matches_filter(
        "ConfigMap",
        Some("default"),
        Some("watch-this-configmap!=multiple-watchers-B"),
    ));
    assert!(!event.matches_filter(
        "ConfigMap",
        Some("default"),
        Some("watch-this-configmap!=multiple-watchers-A"),
    ));
}

#[test]
fn watch_event_constructors_preserve_wire_type_and_object() {
    let added = WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "example", "resourceVersion": "7"}
    }));
    let modified = WatchEvent::modified((*added.object).clone());
    let deleted = WatchEvent::deleted((*added.object).clone());

    assert_eq!(added.event_type, EventType::Added);
    assert_eq!(modified.event_type, EventType::Modified);
    assert_eq!(deleted.event_type, EventType::Deleted);
    assert_eq!(added.resource_version(), Some(7));
}

#[test]
fn watch_event_filters_keep_bookmarks_and_match_resource_scope() {
    let pod = WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "frontend",
            "namespace": "default",
            "labels": {"app": "web"}
        },
        "spec": {"nodeName": "worker-a"}
    }));

    assert!(pod.matches_filter("Pod", Some("default"), Some("app=web")));
    assert!(!pod.matches_filter("Pod", Some("other"), Some("app=web")));
    assert!(pod.matches_field_selector(Some("spec.nodeName=worker-a")));
    assert!(!pod.matches_field_selector(Some("spec.nodeName=worker-b")));

    let bookmark = WatchEvent::bookmark_typed(9, "v1", "Pod");
    assert!(bookmark.matches_filter("ConfigMap", Some("other"), Some("app=other")));
    assert!(bookmark.matches_field_selector(Some("spec.nodeName=other")));
}

#[test]
fn watch_event_selection_combines_identity_and_selectors() {
    let event = WatchEvent::added(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "frontend",
            "namespace": "default",
            "labels": {"app": "web"}
        },
        "spec": {"nodeName": "worker-a"}
    }));
    let selection = WatchEventSelection::new("v1", "Pod")
        .namespace(Some("default"))
        .label_selector(Some("app=web"))
        .field_selector(Some("spec.nodeName=worker-a"));

    assert!(selection.matches(&event));
    assert!(
        !selection
            .clone()
            .field_selector(Some("spec.nodeName=worker-b"))
            .matches(&event)
    );
    assert!(!WatchEventSelection::new("apps/v1", "Pod").matches(&event));
}
