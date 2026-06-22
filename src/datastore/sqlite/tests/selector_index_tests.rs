use super::*;
use serde_json::json;

/// Helper: create a pod with given labels, nodeName, and phase.
async fn create_pod(
    db: &Datastore,
    name: &str,
    namespace: &str,
    labels: serde_json::Value,
    node_name: &str,
    phase: &str,
) -> Resource {
    db.create_resource(
        "v1",
        "Pod",
        Some(namespace),
        name,
        json!({
            "metadata": {"name": name, "namespace": namespace, "labels": labels},
            "spec": {"nodeName": node_name, "restartPolicy": "Always"},
            "status": {"phase": phase}
        }),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn label_equality_uses_index_and_returns_matching_pods() {
    let db = Datastore::new_in_memory().await.unwrap();
    create_pod(
        &db,
        "pod-a",
        "default",
        json!({"app": "nginx"}),
        "node-1",
        "Running",
    )
    .await;
    create_pod(
        &db,
        "pod-b",
        "default",
        json!({"app": "redis"}),
        "node-1",
        "Running",
    )
    .await;
    create_pod(
        &db,
        "pod-c",
        "default",
        json!({"app": "nginx"}),
        "node-2",
        "Pending",
    )
    .await;

    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=nginx"), None, Some(10), None),
        )
        .await
        .unwrap();

    assert_eq!(result.items.len(), 2);
    assert!(result.items.iter().all(|r| r.name != "pod-b"));
}

#[tokio::test]
async fn field_selector_spec_node_name_uses_index() {
    let db = Datastore::new_in_memory().await.unwrap();
    create_pod(&db, "pod-a", "default", json!({}), "node-1", "Running").await;
    create_pod(&db, "pod-b", "default", json!({}), "node-2", "Running").await;
    create_pod(&db, "pod-c", "default", json!({}), "node-1", "Pending").await;

    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                None,
                Some("spec.nodeName=node-1"),
                Some(10),
                None,
            ),
        )
        .await
        .unwrap();

    assert_eq!(result.items.len(), 2);
    assert!(result.items.iter().all(|r| r.name != "pod-b"));
}

#[tokio::test]
async fn status_phase_uses_index() {
    let db = Datastore::new_in_memory().await.unwrap();
    create_pod(&db, "pod-a", "default", json!({}), "node-1", "Running").await;
    create_pod(&db, "pod-b", "default", json!({}), "node-1", "Pending").await;
    create_pod(&db, "pod-c", "default", json!({}), "node-1", "Running").await;

    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                None,
                Some("status.phase=Running"),
                Some(10),
                None,
            ),
        )
        .await
        .unwrap();

    assert_eq!(result.items.len(), 2);
    assert!(result.items.iter().all(|r| r.name != "pod-b"));
}

#[tokio::test]
async fn combined_label_and_field_selector() {
    let db = Datastore::new_in_memory().await.unwrap();
    create_pod(
        &db,
        "pod-a",
        "default",
        json!({"app": "nginx"}),
        "node-1",
        "Running",
    )
    .await;
    create_pod(
        &db,
        "pod-b",
        "default",
        json!({"app": "nginx"}),
        "node-2",
        "Running",
    )
    .await;
    create_pod(
        &db,
        "pod-c",
        "default",
        json!({"app": "redis"}),
        "node-1",
        "Running",
    )
    .await;

    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                Some("app=nginx"),
                Some("spec.nodeName=node-1"),
                Some(10),
                None,
            ),
        )
        .await
        .unwrap();

    assert_eq!(result.items.len(), 1);
    assert_eq!(result.items[0].name, "pod-a");
}

#[tokio::test]
async fn exists_label_selector_pushdown() {
    let db = Datastore::new_in_memory().await.unwrap();
    create_pod(
        &db,
        "pod-a",
        "default",
        json!({"app": "nginx"}),
        "node-1",
        "Running",
    )
    .await;
    create_pod(&db, "pod-b", "default", json!({}), "node-1", "Running").await;

    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app"), None, Some(10), None),
        )
        .await
        .unwrap();

    assert_eq!(result.items.len(), 1);
    assert_eq!(result.items[0].name, "pod-a");
}

#[tokio::test]
async fn not_exists_label_selector_pushdown() {
    let db = Datastore::new_in_memory().await.unwrap();
    create_pod(
        &db,
        "pod-a",
        "default",
        json!({"app": "nginx"}),
        "node-1",
        "Running",
    )
    .await;
    create_pod(&db, "pod-b", "default", json!({}), "node-1", "Running").await;

    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("!app"), None, Some(10), None),
        )
        .await
        .unwrap();

    assert_eq!(result.items.len(), 1);
    assert_eq!(result.items[0].name, "pod-b");
}

#[tokio::test]
async fn in_operator_pushdown() {
    let db = Datastore::new_in_memory().await.unwrap();
    create_pod(
        &db,
        "pod-a",
        "default",
        json!({"tier": "frontend"}),
        "node-1",
        "Running",
    )
    .await;
    create_pod(
        &db,
        "pod-b",
        "default",
        json!({"tier": "backend"}),
        "node-1",
        "Running",
    )
    .await;
    create_pod(
        &db,
        "pod-c",
        "default",
        json!({"tier": "database"}),
        "node-1",
        "Running",
    )
    .await;

    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                Some("tier in (frontend, backend)"),
                None,
                Some(10),
                None,
            ),
        )
        .await
        .unwrap();

    assert_eq!(result.items.len(), 2);
    assert!(result.items.iter().all(|r| r.name != "pod-c"));
}

#[tokio::test]
async fn inequality_stays_residual() {
    let db = Datastore::new_in_memory().await.unwrap();
    create_pod(
        &db,
        "pod-a",
        "default",
        json!({"app": "nginx"}),
        "node-1",
        "Running",
    )
    .await;
    create_pod(
        &db,
        "pod-b",
        "default",
        json!({"app": "redis"}),
        "node-1",
        "Running",
    )
    .await;
    create_pod(&db, "pod-c", "default", json!({}), "node-1", "Running").await;

    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app!=nginx"), None, Some(10), None),
        )
        .await
        .unwrap();

    // pod-b has app=redis (matches), pod-c has no app label (matches via Inequality semantics)
    assert_eq!(result.items.len(), 2);
    assert!(result.items.iter().all(|r| r.name != "pod-a"));
}

#[tokio::test]
async fn pagination_with_label_selector() {
    let db = Datastore::new_in_memory().await.unwrap();
    for i in 0..10 {
        create_pod(
            &db,
            &format!("pod-{i:02}"),
            "default",
            json!({"app": "nginx"}),
            "node-1",
            "Running",
        )
        .await;
    }
    // Create a non-matching pod
    create_pod(
        &db,
        "pod-other",
        "default",
        json!({"app": "redis"}),
        "node-1",
        "Running",
    )
    .await;

    // Page 1: limit=3
    let page1 = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=nginx"), None, Some(3), None),
        )
        .await
        .unwrap();

    assert_eq!(page1.items.len(), 3);
    assert!(page1.continue_token.is_some());
    // Selector queries omit exact remainingItemCount
    assert_eq!(page1.remaining_item_count, None);

    // Page 2: continue from page1
    let page2 = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                Some("app=nginx"),
                None,
                Some(3),
                page1.continue_token.as_deref(),
            ),
        )
        .await
        .unwrap();

    assert_eq!(page2.items.len(), 3);

    // Verify no overlap
    let page1_names: Vec<&str> = page1.items.iter().map(|r| r.name.as_str()).collect();
    let page2_names: Vec<&str> = page2.items.iter().map(|r| r.name.as_str()).collect();
    for name in &page1_names {
        assert!(!page2_names.contains(name));
    }
}

#[tokio::test]
async fn index_updated_on_resource_update() {
    let db = Datastore::new_in_memory().await.unwrap();
    let pod = create_pod(
        &db,
        "pod-a",
        "default",
        json!({"app": "v1"}),
        "node-1",
        "Running",
    )
    .await;

    // Update labels
    let mut updated_data = pod.data.as_ref().clone();
    updated_data["metadata"]["labels"]["app"] = json!("v2");
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        "pod-a",
        updated_data,
        pod.resource_version,
    )
    .await
    .unwrap();

    // Old label should not match
    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=v1"), None, Some(10), None),
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 0);

    // New label should match
    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=v2"), None, Some(10), None),
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 1);
}

#[tokio::test]
async fn index_updated_on_status_update() {
    let db = Datastore::new_in_memory().await.unwrap();
    let pod = create_pod(&db, "pod-a", "default", json!({}), "node-1", "Pending").await;

    // Update status.phase
    db.update_status_only(
        "v1",
        "Pod",
        Some("default"),
        "pod-a",
        json!({"phase": "Running"}),
        Some(pod.resource_version),
    )
    .await
    .unwrap();

    // Old phase should not match
    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                None,
                Some("status.phase=Pending"),
                Some(10),
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 0);

    // New phase should match
    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                None,
                Some("status.phase=Running"),
                Some(10),
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 1);
}

#[tokio::test]
async fn index_cleaned_up_on_resource_delete() {
    let db = Datastore::new_in_memory().await.unwrap();
    create_pod(
        &db,
        "pod-a",
        "default",
        json!({"app": "nginx"}),
        "node-1",
        "Running",
    )
    .await;

    db.delete_resource("v1", "Pod", Some("default"), "pod-a")
        .await
        .unwrap();

    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=nginx"), None, Some(10), None),
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 0);
}

#[tokio::test]
async fn index_updated_on_patch() {
    let db = Datastore::new_in_memory().await.unwrap();
    let _pod = create_pod(
        &db,
        "pod-a",
        "default",
        json!({"app": "v1"}),
        "node-1",
        "Running",
    )
    .await;

    // Patch labels
    db.patch_resource_latest(
        "v1",
        "Pod",
        Some("default"),
        "pod-a",
        PatchKind::Merge,
        json!({"metadata": {"labels": {"app": "patched"}}}),
    )
    .await
    .unwrap();

    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=patched"), None, Some(10), None),
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 1);

    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=v1"), None, Some(10), None),
        )
        .await
        .unwrap();
    assert_eq!(result.items.len(), 0);
}

#[tokio::test]
async fn cluster_scoped_node_field_selector() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Node",
        None,
        "node-1",
        json!({"metadata": {"name": "node-1"}, "spec": {"unschedulable": true}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Node",
        None,
        "node-2",
        json!({"metadata": {"name": "node-2"}, "spec": {}}),
    )
    .await
    .unwrap();

    let result = db
        .list_resources(
            "v1",
            "Node",
            None,
            crate::datastore::ResourceListQuery::new(
                None,
                Some("spec.unschedulable=true"),
                Some(10),
                None,
            ),
        )
        .await
        .unwrap();

    assert_eq!(result.items.len(), 1);
    assert_eq!(result.items[0].name, "node-1");
}

#[tokio::test]
async fn node_field_selector_unschedulable_false_matches_node_without_explicit_field() {
    // Regression: production sonobuoy cascade where every scheduling-aware
    // conformance test fails with "no ready, schedulable nodes in the cluster".
    // Root cause: the framework's `?fieldSelector=spec.unschedulable=false`
    // misses Nodes whose JSON has `spec: {}` (e.g. after a merge-patch round-
    // trip elides the default-valued boolean). The pushdown EXISTS clause
    // looks up `resource_fields`, and no row was inserted for the absent
    // field. The Kubernetes spec default for `spec.unschedulable` is `false`,
    // so absence must match `=false`.
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Node",
        None,
        "schedulable-explicit",
        json!({
            "metadata": {"name": "schedulable-explicit"},
            "spec": {"unschedulable": false}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Node",
        None,
        "schedulable-default",
        json!({
            "metadata": {"name": "schedulable-default"},
            "spec": {}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Node",
        None,
        "cordoned",
        json!({
            "metadata": {"name": "cordoned"},
            "spec": {"unschedulable": true}
        }),
    )
    .await
    .unwrap();

    let result = db
        .list_resources(
            "v1",
            "Node",
            None,
            crate::datastore::ResourceListQuery::new(
                None,
                Some("spec.unschedulable=false"),
                Some(10),
                None,
            ),
        )
        .await
        .unwrap();

    let mut names: Vec<&str> = result.items.iter().map(|r| r.name.as_str()).collect();
    names.sort();
    assert_eq!(
        names,
        vec!["schedulable-default", "schedulable-explicit"],
        "spec.unschedulable=false must match both explicit-false and default-absent nodes"
    );
}

#[tokio::test]
async fn selector_with_limit_returns_correct_remaining_count() {
    let db = Datastore::new_in_memory().await.unwrap();
    for i in 0..5 {
        create_pod(
            &db,
            &format!("pod-{i}"),
            "default",
            json!({"app": "nginx"}),
            "node-1",
            "Running",
        )
        .await;
    }

    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=nginx"), None, Some(2), None),
        )
        .await
        .unwrap();

    assert_eq!(result.items.len(), 2);
    assert!(result.continue_token.is_some());
    // Selector queries omit exact remainingItemCount — only continue_token
    // signals more items exist.
    assert_eq!(result.remaining_item_count, None);
}

/// Regression: a metadata-only annotation patch (labels untouched) must keep
/// the pod visible under its existing label selector, and must preserve
/// `metadata.ownerReferences`. This pins the conformance scenario where
/// kubectl patches an RC-owned pod's annotations and the pod must remain
/// selector-visible with its ownerRefs intact — the index refresh on the
/// datastore patch path must rebuild from the merged object, not the patch
/// body, so unchanged labels and ownerRefs survive.
#[tokio::test]
async fn metadata_annotation_patch_preserves_selector_visibility_and_owner_refs() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "agnhost-primary",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "agnhost-primary",
                "namespace": "default",
                "uid": "agnhost-primary-uid",
                "labels": {"name": "agnhost-primary"},
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "name": "agnhost-primary",
                    "uid": "agnhost-rc-uid",
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {"nodeName": "node-1", "restartPolicy": "Always"},
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    db.patch_resource_latest(
        "v1",
        "Pod",
        Some("default"),
        "agnhost-primary",
        PatchKind::Merge,
        json!({"metadata": {"annotations": {"patched": "true"}}}),
    )
    .await
    .unwrap();

    let result = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                Some("name=agnhost-primary"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        result.items.len(),
        1,
        "metadata annotation patch must keep pod selector-visible"
    );
    assert_eq!(
        result.items[0].data.pointer("/metadata/labels/name"),
        Some(&json!("agnhost-primary")),
        "metadata annotation patch must preserve selector label"
    );
    assert_eq!(
        result.items[0]
            .data
            .pointer("/metadata/ownerReferences/0/uid"),
        Some(&json!("agnhost-rc-uid")),
        "metadata annotation patch must preserve RC ownerRef"
    );
    assert_eq!(
        result.items[0]
            .data
            .pointer("/metadata/annotations/patched"),
        Some(&json!("true")),
        "annotation patch must be applied"
    );
}
