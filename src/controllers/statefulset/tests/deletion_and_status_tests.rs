use super::*;

#[tokio::test]
async fn test_statefulset_infers_current_revision_from_pods_when_status_missing() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let old_template = json!({
        "metadata": {"labels": {"app": "demo"}},
        "spec": {"containers": [{"name": "app", "image": "app:v1"}]}
    });
    let new_template = json!({
        "metadata": {"labels": {"app": "demo"}},
        "spec": {"containers": [{"name": "app", "image": "app:v2"}]}
    });
    let old_rev = compute_statefulset_update_revision("demo-sts", &old_template);
    let new_rev = compute_statefulset_update_revision("demo-sts", &new_template);
    assert_ne!(old_rev, new_rev);

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "demo-sts",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {"name": "demo-sts", "namespace": "test-ns", "uid": "sts-demo-uid"},
                "spec": {
                    "replicas": 3,
                    "serviceName": "demo-headless",
                    "podManagementPolicy": "Parallel",
                    "updateStrategy": {"type": "RollingUpdate", "rollingUpdate": {"partition": 2}},
                    "selector": {"matchLabels": {"app": "demo"}},
                    "template": old_template
                },
                "status": {"currentRevision": old_rev, "updateRevision": old_rev}
            }),
        )
        .await
        .unwrap();

    let first = crate::api::inject_resource_version(created.data, created.resource_version);
    reconcile_statefulset_test(&db, &first, "test-node")
        .await
        .unwrap();

    let current = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "demo-sts")
        .await
        .unwrap()
        .unwrap();
    let mut sts_update: serde_json::Value = (*current.data).clone();
    sts_update["spec"]["template"] = new_template;
    sts_update["status"] = json!(null);

    let updated = db
        .update_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "demo-sts",
            sts_update,
            current.resource_version,
        )
        .await
        .unwrap();

    let second = crate::api::inject_resource_version(updated.data, updated.resource_version);
    reconcile_statefulset_test(&db, &second, "test-node")
        .await
        .unwrap();

    let reconciled = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "demo-sts")
        .await
        .unwrap()
        .unwrap();
    let status = &reconciled.data["status"];
    let current_rev = status
        .get("currentRevision")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let update_rev = status
        .get("updateRevision")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    assert_eq!(update_rev, new_rev);
    assert_eq!(
        current_rev, old_rev,
        "currentRevision should stay old while pods still carry the old revision"
    );
    assert_ne!(current_rev, update_rev);
}

#[tokio::test]
async fn test_statefulset_respects_pod_resourcequota() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("test-ns"),
        "pods-zero",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "pods-zero", "namespace": "test-ns"},
            "spec": {"hard": {"pods": "0"}},
            "status": {"hard": {"pods": "0"}, "used": {"pods": "0"}}
        }),
    )
    .await
    .unwrap();

    let sts = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": "quota-sts",
            "namespace": "test-ns",
            "uid": "sts-uid-quota"
        },
        "spec": {
            "replicas": 1,
            "serviceName": "quota-headless",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "quota"}},
            "template": {
                "metadata": {"labels": {"app": "quota"}},
                "spec": {"containers": [{"name": "app", "image": "busybox"}]}
            }
        }
    });

    let sts_with_rv = crate::controllers::test_utils::store_and_prepare(
        &db,
        "apps/v1",
        "StatefulSet",
        Some("test-ns"),
        "quota-sts",
        sts,
    )
    .await;

    let result = reconcile_statefulset_test(&db, &sts_with_rv, "test-node").await;
    assert!(
        result.is_err(),
        "StatefulSet reconcile should fail on quota deny"
    );

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 0, "quota deny must prevent pod creation");
}
