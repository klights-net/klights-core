use super::*;

/// readyReplicas and availableReplicas must reflect actual pod Ready condition,
/// not be hardcoded to 0. Sonobuoy: RS scaled to 3 but ReadyReplicas stays 0.
#[tokio::test]
async fn test_replicaset_status_ready_replicas_reflects_pod_conditions() {
    let db = crate::internal_test_support::in_memory().await;
    let __pod_repo = crate::internal_test_support::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs_uid = "rs-uid-ready-test";
    let replicaset = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "ready-rs", "namespace": "test-ns", "uid": rs_uid},
        "spec": {
            "replicas": 3,
            "selector": {"matchLabels": {"app": "ready-test"}},
            "template": {
                "metadata": {"labels": {"app": "ready-test"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("test-ns"),
            "ready-rs",
            replicaset,
        )
        .await
        .unwrap();

    let mut rs_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = rs_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // First reconcile: creates 3 pods (all Pending, none Ready)
    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &rs_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Simulate 2 of 3 pods becoming Ready
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::internal_test_support::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        3,
        "Should have 3 pods after first reconcile"
    );

    let ready_condition = json!([{"type": "Ready", "status": "True"}]);
    for (i, pod) in pods.items.iter().enumerate().take(2) {
        let mut updated_pod: serde_json::Value = (*pod.data).clone();
        updated_pod["status"] = json!({"phase": "Running", "conditions": ready_condition});
        db.update_resource(
            "v1",
            "Pod",
            Some("test-ns"),
            &pods.items[i].name.clone(),
            updated_pod,
            pod.resource_version,
        )
        .await
        .unwrap();
    }

    // Second reconcile: no pods to create/delete, must update status from pod states
    let rs_after = db
        .get_resource("apps/v1", "ReplicaSet", Some("test-ns"), "ready-rs")
        .await
        .unwrap()
        .unwrap();
    let mut rs_with_rv2: serde_json::Value = (*rs_after.data).clone();
    if let Some(meta) = rs_with_rv2
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(rs_after.resource_version.to_string()),
        );
    }
    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &rs_with_rv2,
        "test-node",
    )
    .await
    .unwrap();

    let updated_rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("test-ns"), "ready-rs")
        .await
        .unwrap()
        .unwrap();

    let status = &updated_rs.data["status"];
    assert_eq!(status["replicas"], 3, "status.replicas must be 3");
    assert_eq!(
        status["readyReplicas"], 2,
        "readyReplicas must reflect pods with Ready=True (2), not hardcoded 0, got: {}",
        status["readyReplicas"]
    );
    assert_eq!(
        status["availableReplicas"], 2,
        "availableReplicas must reflect ready pods (2), not hardcoded 0, got: {}",
        status["availableReplicas"]
    );
}

#[tokio::test]
async fn test_replicaset_deletes_itself_when_controller_deployment_missing() {
    let db = crate::internal_test_support::in_memory().await;
    let __pod_repo = crate::internal_test_support::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "gc-race-ns",
        json!({"metadata": {"name": "gc-race-ns"}}),
    )
    .await
    .unwrap();

    let rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "late-rs",
            "namespace": "gc-race-ns",
            "uid": "late-rs-uid",
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": "gone-deploy",
                "uid": "gone-deploy-uid",
                "controller": true
            }]
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "late-rs"}},
            "template": {
                "metadata": {"labels": {"app": "late-rs"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });
    let created = db
        .create_resource("apps/v1", "ReplicaSet", Some("gc-race-ns"), "late-rs", rs)
        .await
        .unwrap();

    let rs_with_rv = crate::internal_test_support::inject_resource_version(
        created.data,
        created.resource_version,
    );
    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &rs_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let rs_after = db
        .get_resource("apps/v1", "ReplicaSet", Some("gc-race-ns"), "late-rs")
        .await
        .unwrap();
    assert!(
        rs_after.is_none(),
        "ReplicaSet with missing controller Deployment should self-delete"
    );
}

#[tokio::test]
async fn test_replicaset_skips_reconcile_when_deletion_timestamp_set() {
    let db = crate::internal_test_support::in_memory().await;
    let __pod_repo = crate::internal_test_support::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "deleting-rs",
            "namespace": "test-ns",
            "uid": "rs-uid-del",
            "deletionTimestamp": "2026-04-12T00:00:00Z"
        },
        "spec": {
            "replicas": 3,
            "selector": {"matchLabels": {"app": "test"}},
            "template": {
                "metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource("apps/v1", "ReplicaSet", Some("test-ns"), "defaults-rs", rs)
        .await
        .unwrap();
    let mut rs_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = rs_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &rs_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::internal_test_support::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        0,
        "No pods should be created for a ReplicaSet being deleted"
    );
}

#[tokio::test]
async fn test_replicaset_stale_snapshot_after_delete_does_not_recreate_pods() {
    let db = crate::internal_test_support::in_memory().await;
    let __pod_repo = crate::internal_test_support::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "stale-rs",
            "namespace": "test-ns",
            "uid": "rs-uid-stale"
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "stale"}},
            "template": {
                "metadata": {"labels": {"app": "stale"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource("apps/v1", "ReplicaSet", Some("test-ns"), "stale-rs", rs)
        .await
        .unwrap();
    let stale_snapshot = created.data.clone();

    db.delete_resource("apps/v1", "ReplicaSet", Some("test-ns"), "stale-rs")
        .await
        .unwrap();

    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &stale_snapshot,
        "test-node",
    )
    .await
    .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::internal_test_support::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        0,
        "stale ReplicaSet reconcile after delete must not recreate pods"
    );
}

/// P0-API-01 race regression: a controller status write must never lose
/// a concurrent user `kubectl scale` (PATCH `.spec.replicas`).
///
/// Pre-fix: the controller did a read-modify-write through `update_resource`
/// using the spec it had snapshotted — if the user PATCHed `.spec.replicas`
/// after the snapshot but before the write, the user's value was clobbered
/// (~50% of races). Post-fix: status writes go through `write_status` →
/// `update_status_only` which uses `json_set(data, '$.status', ?)` so spec
/// is never read or written by the status path.
///
/// Asserts spec.replicas == 7 across race iterations. 25 iterations is enough
/// to exercise the user-scale-vs-controller-status race reliably under tokio's
/// scheduler — the original 100 was overkill (the bug, when present, fires in
/// most iterations, not 1 in 100).
#[tokio::test]
async fn test_replicaset_status_write_never_clobbers_user_scale_under_race() {
    use std::sync::Arc;

    let db = Arc::new(crate::internal_test_support::in_memory().await);

    for iteration in 0..25 {
        let name = format!("rs-race-{iteration}");
        let initial = json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": &name,
                "namespace": "default",
                "uid": format!("uid-race-{iteration}")
            },
            "spec": {
                "replicas": 3,
                "selector": {"matchLabels": {"app": "race"}},
                "template": {
                    "metadata": {"labels": {"app": "race"}},
                    "spec": {"containers": [{"name": "c", "image": "x"}]}
                }
            },
            "status": {"replicas": 0}
        });

        // Create the RS and capture the controller's snapshot at this RV.
        db.create_resource("apps/v1", "ReplicaSet", Some("default"), &name, initial)
            .await
            .unwrap();

        let user_db = Arc::clone(&db);
        let user_name = name.clone();
        let user_handle = tokio::spawn(async move {
            // User scales to 7 via merge-patch (kubectl scale-style — no
            // explicit resourceVersion CAS so it cannot 409 against the
            // controller's status write).
            user_db
                .patch_resource_merge_latest(
                    "apps/v1",
                    "ReplicaSet",
                    Some("default"),
                    &user_name,
                    json!({"spec": {"replicas": 7}}),
                )
                .await
                .unwrap();
        });

        let ctl_db = Arc::clone(&db);
        let ctl_name = name.clone();
        let ctl_handle = tokio::spawn(async move {
            // Controller writes its computed status through the safe path.
            let new_status = json!({
                "replicas": 3,
                "readyReplicas": 3,
                "availableReplicas": 3,
                "fullyLabeledReplicas": 3,
                "observedGeneration": 1
            });
            crate::common::ControllerStatusStore::update_status(
                ctl_db.as_ref(),
                "apps/v1",
                "ReplicaSet",
                Some("default"),
                &ctl_name,
                new_status,
                klights_cluster_core::ResourcePreconditions::default(),
            )
            .await
            .unwrap();
        });

        user_handle.await.unwrap();
        ctl_handle.await.unwrap();

        // After both writes, the user's spec.replicas=7 must still be visible.
        let after = db
            .get_resource("apps/v1", "ReplicaSet", Some("default"), &name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after.data["spec"]["replicas"], 7,
            "iteration {iteration}: user scale to 7 was clobbered by controller status write",
        );
    }
}
