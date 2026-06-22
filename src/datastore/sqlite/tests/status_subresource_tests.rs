use super::*;
use serde_json::json;
#[test]
fn test_filter_by_field_selector_boolean_spec_unschedulable() {
    let schedulable = Resource {
        id: 0,
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "node-a".to_string(),
        uid: "uid-node-a".to_string(),
        resource_version: 1,
        data: std::sync::Arc::new(json!({"spec": {"unschedulable": false}})),
    };
    let cordoned = Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "node-b".to_string(),
        uid: "uid-node-b".to_string(),
        resource_version: 2,
        data: std::sync::Arc::new(json!({"spec": {"unschedulable": true}})),
    };
    // Node with omitted unschedulable field (omitempty behavior — default is false)
    let omitted = Resource {
        id: 2,
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "node-c".to_string(),
        uid: "uid-node-c".to_string(),
        resource_version: 3,
        data: std::sync::Arc::new(json!({"spec": {}})),
    };
    let items = vec![schedulable, cordoned, omitted];

    // Both explicit false and omitted (default false) should match
    let filtered = filter_by_field_selector(items.clone(), "spec.unschedulable=false");
    assert_eq!(filtered.len(), 2);
    assert!(filtered.iter().any(|r| r.name == "node-a"));
    assert!(filtered.iter().any(|r| r.name == "node-c"));

    let filtered = filter_by_field_selector(items, "spec.unschedulable=true");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "node-b");
}

#[test]
fn test_filter_by_field_selector_event_source_alias() {
    let items = vec![
        Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "event-a".to_string(),
            uid: "uid-event-a".to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(json!({
                "metadata": {"name": "event-a", "namespace": "default"},
                "source": {"component": "event-test"},
                "reason": "Test"
            })),
        },
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "event-b".to_string(),
            uid: "uid-event-b".to_string(),
            resource_version: 2,
            data: std::sync::Arc::new(json!({
                "metadata": {"name": "event-b", "namespace": "default"},
                "source": {"component": "other"},
                "reason": "Test"
            })),
        },
    ];

    let filtered = filter_by_field_selector(items, "source=event-test");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "event-a");
}

#[test]
fn test_filter_by_field_selector_event_source_alias_events_v1_shape() {
    let items = vec![
        Resource {
            id: 0,
            api_version: "events.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "event-a".to_string(),
            uid: "uid-event-a".to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(json!({
                "metadata": {"name": "event-a", "namespace": "default"},
                "reportingController": "event-test",
                "reason": "Test"
            })),
        },
        Resource {
            id: 1,
            api_version: "events.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "event-b".to_string(),
            uid: "uid-event-b".to_string(),
            resource_version: 2,
            data: std::sync::Arc::new(json!({
                "metadata": {"name": "event-b", "namespace": "default"},
                "reportingController": "other",
                "reason": "Test"
            })),
        },
    ];

    let filtered = filter_by_field_selector(items, "source=event-test");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "event-a");
}

#[test]
fn test_filter_by_field_selector_event_source_alias_ignores_empty_deprecated_source() {
    let items = vec![Resource {
        id: 0,
        api_version: "events.k8s.io/v1".to_string(),
        kind: "Event".to_string(),
        namespace: Some("default".to_string()),
        name: "event-a".to_string(),
        uid: "uid-event-a".to_string(),
        resource_version: 1,
        data: std::sync::Arc::new(json!({
            "metadata": {"name": "event-a", "namespace": "default"},
            "deprecatedSource": {"component": ""},
            "reportingController": "event-test",
            "reason": "Test"
        })),
    }];

    let filtered = filter_by_field_selector(items, "source=event-test");

    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "event-a");
}

#[test]
fn test_filter_by_field_selector_event_source_alias_reporting_component() {
    let items = vec![
        Resource {
            id: 0,
            api_version: "events.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "event-a".to_string(),
            uid: "uid-event-a".to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(json!({
                "metadata": {"name": "event-a", "namespace": "default"},
                "reportingComponent": "event-test",
                "reason": "Test"
            })),
        },
        Resource {
            id: 1,
            api_version: "events.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "event-b".to_string(),
            uid: "uid-event-b".to_string(),
            resource_version: 2,
            data: std::sync::Arc::new(json!({
                "metadata": {"name": "event-b", "namespace": "default"},
                "reportingComponent": "other",
                "reason": "Test"
            })),
        },
    ];

    let filtered = filter_by_field_selector(items, "source=event-test");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "event-a");
}

#[test]
fn test_filter_by_field_selector_event_involved_object_alias_events_v1_shape() {
    let items = vec![
        Resource {
            id: 0,
            api_version: "events.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "event-a".to_string(),
            uid: "uid-event-a".to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(json!({
                "metadata": {"name": "event-a", "namespace": "default"},
                "regarding": {"kind": "Pod", "name": "pod-a", "namespace": "default"}
            })),
        },
        Resource {
            id: 1,
            api_version: "events.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "event-b".to_string(),
            uid: "uid-event-b".to_string(),
            resource_version: 2,
            data: std::sync::Arc::new(json!({
                "metadata": {"name": "event-b", "namespace": "default"},
                "regarding": {"kind": "Pod", "name": "pod-b", "namespace": "default"}
            })),
        },
    ];

    let filtered = filter_by_field_selector(items, "involvedObject.name=pod-a");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "event-a");
}

// ---------- P0-API-01: update_status_only ----------

#[tokio::test]
async fn test_update_status_only_preserves_spec_namespaced() {
    let db = Datastore::new_in_memory().await.unwrap();

    let created = db
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "rs-a",
            json!({
                "metadata": {"name": "rs-a", "namespace": "default"},
                "spec": {"replicas": 3, "selector": {"matchLabels": {"app": "x"}}},
                "status": {"replicas": 0}
            }),
        )
        .await
        .unwrap();

    let updated = db
        .update_status_only(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "rs-a",
            json!({"replicas": 3, "readyReplicas": 3, "availableReplicas": 3}),
            Some(created.resource_version),
        )
        .await
        .unwrap();

    assert_eq!(updated.data["spec"]["replicas"], 3);
    assert_eq!(updated.data["spec"]["selector"]["matchLabels"]["app"], "x");
    assert_eq!(updated.data["status"]["replicas"], 3);
    assert_eq!(updated.data["status"]["readyReplicas"], 3);
    assert_eq!(updated.data["status"]["availableReplicas"], 3);
    assert_eq!(updated.data["metadata"]["name"], "rs-a");
    assert!(updated.resource_version > created.resource_version);
}

#[tokio::test]
async fn test_update_status_only_preserves_spec_when_user_concurrently_changes_spec() {
    // Demonstrates the bug fix: user PATCH .spec between read and write must not be lost.
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "rs-b",
            json!({
                "metadata": {"name": "rs-b", "namespace": "default"},
                "spec": {"replicas": 3},
                "status": {"replicas": 0}
            }),
        )
        .await
        .unwrap();

    // Simulate user PATCH between controller's read and write: user scales to 7.
    let user_update = db
        .update_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "rs-b",
            json!({
                "metadata": {"name": "rs-b", "namespace": "default"},
                "spec": {"replicas": 7},
                "status": {"replicas": 0}
            }),
            created.resource_version,
        )
        .await
        .unwrap();

    // Controller now writes status using the original RV — must NOT clobber user spec.
    // With CAS skipped (None) the controller's status write succeeds without 409.
    let after_status = db
        .update_status_only(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "rs-b",
            json!({"replicas": 3, "readyReplicas": 3}),
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        after_status.data["spec"]["replicas"], 7,
        "user's spec edit must be preserved across status write"
    );
    assert_eq!(after_status.data["status"]["replicas"], 3);
    assert_eq!(after_status.data["status"]["readyReplicas"], 3);
    assert!(after_status.resource_version > user_update.resource_version);
}

#[tokio::test]
async fn test_main_update_preserves_latest_status_without_rv_precondition() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "deploy-main",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "deploy-main", "namespace": "default"},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "deploy-main"}},
                    "template": {
                        "metadata": {"labels": {"app": "deploy-main"}},
                        "spec": {"containers": [{"name": "web", "image": "webserver:old"}]}
                    }
                },
                "status": {"replicas": 1, "readyReplicas": 1}
            }),
        )
        .await
        .unwrap();

    let stale_main_update = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "deploy-main", "namespace": "default"},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "deploy-main"}},
            "template": {
                "metadata": {"labels": {"app": "deploy-main"}},
                "spec": {"containers": [{"name": "web", "image": "webserver:new"}]}
            }
        },
        "status": {"replicas": 1, "readyReplicas": 1}
    });

    db.update_status_only_with_preconditions(
        "apps/v1",
        "Deployment",
        Some("default"),
        "deploy-main",
        json!({"replicas": 2, "readyReplicas": 2}),
        ResourcePreconditions::uid(&created.uid),
    )
    .await
    .unwrap();

    let updated = db
        .update_main_resource_with_preconditions(
            "apps/v1",
            "Deployment",
            Some("default"),
            "deploy-main",
            stale_main_update,
            ResourcePreconditions::uid(created.uid),
        )
        .await
        .unwrap();

    assert_eq!(
        updated.data["status"],
        json!({"replicas": 2, "readyReplicas": 2}),
        "main-resource update without resourceVersion must preserve the latest status subresource state"
    );
    assert_eq!(
        updated.data["spec"]["template"]["spec"]["containers"][0]["image"],
        "webserver:new"
    );
}

#[tokio::test]
async fn test_update_status_only_resource_version_conflict_returns_error() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Service",
            Some("default"),
            "svc-a",
            json!({
                "metadata": {"name": "svc-a", "namespace": "default"},
                "spec": {"clusterIP": "10.0.0.1"},
                "status": {}
            }),
        )
        .await
        .unwrap();

    let stale_rv = created.resource_version;
    // Bump RV via a real update.
    db.update_resource(
        "v1",
        "Service",
        Some("default"),
        "svc-a",
        json!({
            "metadata": {"name": "svc-a", "namespace": "default"},
            "spec": {"clusterIP": "10.0.0.1", "type": "ClusterIP"},
            "status": {}
        }),
        stale_rv,
    )
    .await
    .unwrap();

    // Status write with stale RV must conflict.
    let err = db
        .update_status_only(
            "v1",
            "Service",
            Some("default"),
            "svc-a",
            json!({"loadBalancer": {}}),
            Some(stale_rv),
        )
        .await
        .expect_err("stale RV should fail");
    assert!(
        err.to_string().contains("409") || err.to_string().to_lowercase().contains("conflict"),
        "expected 409/conflict error, got: {}",
        err
    );
}

#[tokio::test]
async fn test_update_status_only_resource_version_conflict_is_typed() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "typed-conflict",
            json!({
                "metadata": {"name": "typed-conflict", "namespace": "default"},
                "status": {"replicas": 0}
            }),
        )
        .await
        .unwrap();

    let err = db
        .update_status_only(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "typed-conflict",
            json!({"replicas": 1}),
            Some(created.resource_version + 100),
        )
        .await
        .expect_err("stale status writer must fail with a typed conflict");

    assert!(
        err.downcast_ref::<crate::datastore::errors::DatastoreError>()
            .is_some_and(crate::datastore::errors::DatastoreError::is_conflict),
        "expected typed datastore conflict, got {err:#}"
    );
}

#[tokio::test]
async fn test_update_status_only_cluster_scoped_persistentvolume() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "PersistentVolume",
            None,
            "pv-a",
            json!({
                "metadata": {"name": "pv-a"},
                "spec": {"capacity": {"storage": "1Gi"}},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

    let updated = db
        .update_status_only(
            "v1",
            "PersistentVolume",
            None,
            "pv-a",
            json!({"phase": "Available"}),
            Some(created.resource_version),
        )
        .await
        .unwrap();

    assert_eq!(updated.data["spec"]["capacity"]["storage"], "1Gi");
    assert_eq!(updated.data["status"]["phase"], "Available");
}

#[tokio::test]
async fn test_update_status_only_emits_modified_watch_event() {
    use crate::watch::EventType;
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "p-a",
            json!({
                "metadata": {"name": "p-a", "namespace": "default"},
                "spec": {"containers": [{"name": "c", "image": "x"}]},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

    db.update_status_only(
        "v1",
        "Pod",
        Some("default"),
        "p-a",
        json!({"phase": "Running"}),
        Some(created.resource_version),
    )
    .await
    .unwrap();

    let replay = db
        .list_watch_events_since(&[WatchTarget::namespaced("v1", "Pod")], 0)
        .await
        .unwrap();
    // Two events: ADDED from create_resource, MODIFIED from update_status_only.
    assert_eq!(replay.len(), 2, "expected ADDED + MODIFIED");
    let modified = replay.last().unwrap().clone().into_watch_event();
    assert_eq!(modified.event_type, EventType::Modified);
    assert_eq!(modified.object["status"]["phase"], "Running");
    assert_eq!(modified.object["spec"]["containers"][0]["image"], "x");
}

#[tokio::test]
async fn test_update_status_only_noop_does_not_advance_rv_or_emit_watch_event() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "p-noop",
            json!({
                "metadata": {"name": "p-noop", "namespace": "default"},
                "spec": {"containers": [{"name": "c", "image": "x"}]},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();

    let unchanged = db
        .update_status_only(
            "v1",
            "Pod",
            Some("default"),
            "p-noop",
            json!({"phase": "Pending"}),
            Some(created.resource_version),
        )
        .await
        .unwrap();

    assert_eq!(
        unchanged.resource_version, created.resource_version,
        "unchanged status must not advance resourceVersion"
    );
    assert_eq!(unchanged.data, created.data);

    let replay = db
        .list_watch_events_since(&[WatchTarget::namespaced("v1", "Pod")], 0)
        .await
        .unwrap();
    assert_eq!(
        replay.len(),
        1,
        "unchanged status must not append a MODIFIED watch event"
    );
}

#[tokio::test]
async fn test_update_status_only_missing_resource_returns_error() {
    let db = Datastore::new_in_memory().await.unwrap();
    let err = db
        .update_status_only(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "missing",
            json!({}),
            None,
        )
        .await
        .expect_err("missing resource should fail");
    assert!(
        err.to_string().contains("not found")
            || err.to_string().contains("404")
            || err.to_string().contains("409"),
        "unexpected error: {}",
        err
    );
}

// OCC for the status subresource: a status PATCH/PUT carrying a stale
// `metadata.resourceVersion` must be rejected with a 409/conflict at the
// precondition-validation layer that the API status subresource feeds.
#[tokio::test]
async fn pod_status_patch_with_resource_version_conflict_returns_409() {
    let db = Datastore::new_in_memory().await.unwrap();
    let created = db
        .create_resource(
            "v1",
            "Pod",
            Some("default"),
            "occ-pod",
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "occ-pod",
                    "namespace": "default",
                    "uid": "uid-occ"
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .unwrap();
    let stale_rv = created.resource_version;

    // Advance the live resourceVersion via a regular update before the stale
    // status writer runs, mirroring a concurrent edit between the client's
    // read and its /status write.
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "occ-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "occ-pod",
                "namespace": "default",
                "uid": "uid-occ",
                "resourceVersion": stale_rv.to_string()
            },
            "spec": {
                "nodeName": "worker-a",
                "containers": [{"name": "app", "image": "nginx:1.25"}]
            },
            "status": {"phase": "Pending"}
        }),
        stale_rv,
    )
    .await
    .unwrap();

    // Status write carrying the STALE resource_version precondition must be
    // rejected. This is the exact call the API status PATCH/PUT handler makes
    // once it forwards `metadata.resourceVersion` into ResourcePreconditions.
    let result = db
        .update_status_only_with_preconditions(
            "v1",
            "Pod",
            Some("default"),
            "occ-pod",
            json!({"phase": "Running"}),
            ResourcePreconditions {
                uid: Some("uid-occ".to_string()),
                resource_version: Some(stale_rv),
            },
        )
        .await;

    assert!(
        result.is_err(),
        "stale resourceVersion precondition must be rejected (OCC 409)"
    );
    let err = result.unwrap_err();
    assert!(
        err.downcast_ref::<crate::datastore::errors::DatastoreError>()
            .is_some_and(crate::datastore::errors::DatastoreError::is_conflict)
            || err.to_string().contains("409")
            || err.to_string().to_lowercase().contains("conflict"),
        "expected 409/conflict error, got: {}",
        err
    );
}
