//! Root-owned active-watch integration over passive SQLite commit facts.

use klights_cluster_core::{
    LogApplyMutation, LogApplyWatchEventRow, ResourceBatchOperation, ResourceBatchPutMode,
    ResourcePreconditions,
};
use serde_json::json;

#[test]
fn watch_event_filter_matches_hydrated_labels() {
    let event = klights_watch::WatchEvent::added(json!({
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

#[tokio::test]
async fn sqlite_create_reaches_the_root_active_watch_adapter_after_commit() {
    let db = crate::datastore::test_support::in_memory().await;
    let mut events = db.subscribe_watch(klights_watch::WatchTopic::new("v1", "ConfigMap"));
    let created = db
        .create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "active-watch",
            json!({"metadata": {"name": "active-watch", "namespace": "default"}}),
        )
        .await
        .unwrap();

    let event = events
        .try_recv()
        .expect("root adapter must publish the commit");
    assert_eq!(event.resource_version(), Some(created.resource_version));
    assert_eq!(event.object["metadata"]["name"], "active-watch");
}

#[tokio::test]
async fn sqlite_batch_reaches_root_watch_topics_with_one_commit_position() {
    let db = crate::datastore::test_support::in_memory().await;
    let mut endpoints = db.subscribe_watch(klights_watch::WatchTopic::new("v1", "Endpoints"));
    let mut slices = db.subscribe_watch(klights_watch::WatchTopic::new(
        "discovery.k8s.io/v1",
        "EndpointSlice",
    ));

    db.apply_resource_batch(vec![
        ResourceBatchOperation::Put {
            api_version: "v1".to_string(),
            kind: "Endpoints".to_string(),
            namespace: Some("default".to_string()),
            name: "watch-ep".to_string(),
            data: json!({"metadata":{"name":"watch-ep","namespace":"default"}}),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        },
        ResourceBatchOperation::Put {
            api_version: "discovery.k8s.io/v1".to_string(),
            kind: "EndpointSlice".to_string(),
            namespace: Some("default".to_string()),
            name: "watch-eps".to_string(),
            data: json!({"metadata":{"name":"watch-eps","namespace":"default"}}),
            mode: ResourceBatchPutMode::Create,
            preconditions: ResourcePreconditions::default(),
        },
    ])
    .await
    .unwrap();

    assert_eq!(
        endpoints.try_recv().unwrap().resource_version(),
        slices.try_recv().unwrap().resource_version(),
    );
}

#[tokio::test]
async fn apply_log_apply_commit_broadcasts_explicit_watch_event() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "bound-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "bound-pod",
                "namespace": "default",
                "uid": "pod-uid"
            },
            "spec": {"containers": [{"name": "c", "image": "pause"}]}
        }),
    )
    .await
    .unwrap();
    let mut watch_rx = db.subscribe_watch(klights_watch::WatchTopic::new("v1", "Pod"));

    let leader_watch_row = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "bound-pod",
            "namespace": "default",
            "uid": "pod-uid",
            "resourceVersion": "7"
        },
        "spec": {
            "nodeName": "mn-controlplane3",
            "containers": [{"name": "c", "image": "pause"}]
        }
    });

    db.apply_log_apply_commit(crate::datastore::test_support::test_live_commit(
        7,
        vec![LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
            event_id: None,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "bound-pod".to_string(),
            resource_version: 7,
            event_type: "MODIFIED".to_string(),
            data: leader_watch_row,
        })],
    ))
    .await
    .unwrap();
    let applied_rv = db.get_current_resource_version().await.unwrap();

    let event = watch_rx
        .try_recv()
        .expect("explicit watch-history apply must wake local watchers");
    assert_eq!(event.event_type, klights_watch::EventType::Modified);
    assert_eq!(event.resource_version(), Some(applied_rv));
    assert_eq!(
        event
            .object
            .pointer("/spec/nodeName")
            .and_then(serde_json::Value::as_str),
        Some("mn-controlplane3")
    );
}
