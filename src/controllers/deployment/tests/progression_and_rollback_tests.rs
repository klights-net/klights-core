use super::*;
/// Rolling update loops to completion in a single reconcile: new RS scales up to desired,
/// old RS scales down to 0, respecting maxSurge/maxUnavailable at each step.
#[tokio::test]
async fn test_reconcile_deployment_rolling_update_completes_after_pods_become_ready() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-progressive";

    // Create deployment with 3 replicas, maxSurge=1, maxUnavailable=1
    let deploy = make_deployment_with_image("app", "default", deploy_uid, 3, "0", "nginx:1.14");
    let created = db
        .create_resource("apps/v1", "Deployment", Some("default"), "app", deploy)
        .await
        .unwrap();
    let deploy_with_rv =
        crate::api::inject_resource_version(created.data, created.resource_version);
    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Trigger rolling update
    let current = db
        .get_resource("apps/v1", "Deployment", Some("default"), "app")
        .await
        .unwrap()
        .unwrap();
    let updated_deploy =
        make_deployment_with_image("app", "default", deploy_uid, 3, "0", "nginx:1.16");
    let updated = db
        .update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "app",
            updated_deploy,
            current.resource_version,
        )
        .await
        .unwrap();
    let deploy_with_rv =
        crate::api::inject_resource_version(updated.data, updated.resource_version);

    // First reconcile: creates new RS with initial replicas
    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    // Simulate kubelet readiness so old RS can be safely scaled down.
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    for pod in pods.items {
        let mut ready_pod: serde_json::Value = (*pod.data).clone();
        ready_pod["status"] = json!({
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}]
        });
        db.update_resource(
            "v1",
            "Pod",
            Some("default"),
            &pod.name,
            ready_pod,
            pod.resource_version,
        )
        .await
        .unwrap();
    }

    // Second reconcile: progresses rollout based on currently ready pods.
    let current2 = db
        .get_resource("apps/v1", "Deployment", Some("default"), "app")
        .await
        .unwrap()
        .unwrap();
    let deploy_with_rv2 =
        crate::api::inject_resource_version(current2.data, current2.resource_version);
    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv2,
        "test-node",
    )
    .await
    .unwrap();

    // New pods may have been created in the second reconcile; mark them ready and
    // reconcile once more to allow final old RS scale-down.
    let pods_after = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    for pod in pods_after.items {
        let mut ready_pod: serde_json::Value = (*pod.data).clone();
        ready_pod["status"] = json!({
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}]
        });
        db.update_resource(
            "v1",
            "Pod",
            Some("default"),
            &pod.name,
            ready_pod,
            pod.resource_version,
        )
        .await
        .unwrap();
    }
    let rs_after_ready = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    for rs in rs_after_ready.items {
        let rs_with_rv = crate::api::inject_resource_version(rs.data, rs.resource_version);
        crate::controllers::replicaset::reconcile_replicaset(
            &db,
            __pod_repo.as_ref(),
            __pod_repo.as_ref(),
            __pod_repo.as_ref(),
            &rs_with_rv,
            "test-node",
        )
        .await
        .unwrap();
    }

    let current3 = db
        .get_resource("apps/v1", "Deployment", Some("default"), "app")
        .await
        .unwrap()
        .unwrap();
    let deploy_with_rv3 =
        crate::api::inject_resource_version(current3.data, current3.resource_version);
    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv3,
        "test-node",
    )
    .await
    .unwrap();

    // One more readiness + reconcile cycle to account for the newly created pod
    // from the previous step before final old RS scale-down.
    let pods_after2 = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    for pod in pods_after2.items {
        let mut ready_pod: serde_json::Value = (*pod.data).clone();
        ready_pod["status"] = json!({
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}]
        });
        db.update_resource(
            "v1",
            "Pod",
            Some("default"),
            &pod.name,
            ready_pod,
            pod.resource_version,
        )
        .await
        .unwrap();
    }
    let rs_after_ready2 = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    for rs in rs_after_ready2.items {
        let rs_with_rv = crate::api::inject_resource_version(rs.data, rs.resource_version);
        crate::controllers::replicaset::reconcile_replicaset(
            &db,
            __pod_repo.as_ref(),
            __pod_repo.as_ref(),
            __pod_repo.as_ref(),
            &rs_with_rv,
            "test-node",
        )
        .await
        .unwrap();
    }
    let current4 = db
        .get_resource("apps/v1", "Deployment", Some("default"), "app")
        .await
        .unwrap()
        .unwrap();
    let deploy_with_rv4 =
        crate::api::inject_resource_version(current4.data, current4.resource_version);
    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv4,
        "test-node",
    )
    .await
    .unwrap();

    // After completion: new RS at desired (3), old RS at 0
    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(rs_list.items.len(), 2, "Should have old and new RS");

    let new_rs = rs_list
        .items
        .iter()
        .find(|rs| rs.data["spec"]["template"]["spec"]["containers"][0]["image"] == "nginx:1.16")
        .expect("New RS must exist");
    let old_rs = rs_list
        .items
        .iter()
        .find(|rs| rs.data["spec"]["template"]["spec"]["containers"][0]["image"] == "nginx:1.14")
        .expect("Old RS must exist");

    let new_replicas = new_rs.data["spec"]["replicas"].as_i64().unwrap_or(0);
    let old_replicas = old_rs.data["spec"]["replicas"].as_i64().unwrap_or(0);

    assert_eq!(
        new_replicas, 3,
        "New RS must be at desired replicas (3), got {}",
        new_replicas
    );
    assert_eq!(
        old_replicas, 0,
        "old RS should scale down after replacement pods are ready, got {}",
        old_replicas
    );
    let old_controlled = old_rs
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .map(|refs| {
            refs.iter().any(|r| {
                r.get("controller").and_then(|c| c.as_bool()) == Some(true)
                    && r.get("uid").and_then(|u| u.as_str()) == Some(deploy_uid)
            })
        })
        .unwrap_or(false);
    assert!(
        old_controlled,
        "old RS should remain controlled even after scaling to zero"
    );
}

#[tokio::test]
async fn test_deployment_skips_reconcile_when_deletion_timestamp_set() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    let deploy = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "deleting-deploy",
            "namespace": "default",
            "uid": "deploy-uid-del",
            "resourceVersion": "1",
            "deletionTimestamp": "2026-04-12T00:00:00Z"
        },
        "spec": {
            "replicas": 3,
            "selector": {"matchLabels": {"app": "deleting"}},
            "template": {
                "metadata": {"labels": {"app": "deleting"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("default"),
        "deleting-deploy",
        deploy.clone(),
    )
    .await
    .unwrap();

    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy,
        "test-node",
    )
    .await
    .unwrap();

    // No ReplicaSets should be created
    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        rs_list.items.len(),
        0,
        "No ReplicaSets should be created for a Deployment being deleted"
    );
}

#[tokio::test]
async fn test_deployment_stale_snapshot_after_delete_does_not_recreate_replicasets_or_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    let deploy = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "stale-deploy",
            "namespace": "default",
            "uid": "deploy-uid-stale",
            "resourceVersion": "1"
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "stale-deploy"}},
            "template": {
                "metadata": {"labels": {"app": "stale-deploy"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx:1.14"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "stale-deploy",
            deploy.clone(),
        )
        .await
        .unwrap();
    let stale_snapshot =
        crate::api::inject_resource_version(created.data, created.resource_version);

    db.delete_resource("apps/v1", "Deployment", Some("default"), "stale-deploy")
        .await
        .unwrap();

    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &stale_snapshot,
        "test-node",
    )
    .await
    .unwrap();

    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert!(
        rs_list.items.is_empty(),
        "stale Deployment reconcile after delete must not recreate ReplicaSets"
    );

    let pod_list = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert!(
        pod_list.items.is_empty(),
        "stale Deployment reconcile after delete must not recreate Pods"
    );
}

#[tokio::test]
async fn test_reconcile_deployment_adopts_existing_owned_replicaset() {
    // Regression: concurrent reconcile calls can both try to create the same RS.
    // The second call must adopt the already-created RS rather than failing with 409.
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-concurrent";

    let deploy = make_deployment("myapp", "default", deploy_uid, 2, "0");
    let created_deploy = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "myapp",
            deploy.clone(),
        )
        .await
        .unwrap();
    let deploy_with_rv = crate::api::inject_resource_version(
        created_deploy.data.clone(),
        created_deploy.resource_version,
    );

    // Compute the RS name the controller will use (same hash as controller would)
    let template = deploy.get("spec").unwrap().get("template").unwrap();
    let hash = compute_pod_template_hash(template);
    let rs_name = format!("myapp-{}", hash);

    // Pre-create the RS with the same ownerReference (simulating concurrent reconcile)
    db.create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            &rs_name.clone(),
            serde_json::json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": rs_name,
                    "namespace": "default",
                    "ownerReferences": [{"uid": deploy_uid, "kind": "Deployment", "controller": true, "blockOwnerDeletion": true, "apiVersion": "apps/v1", "name": "myapp"}]
                },
                "spec": {
                    "replicas": 2,
                    "selector": {"matchLabels": {"app": "myapp"}},
                    "template": *template
                }
            }),
        )
        .await
        .unwrap();

    // Second reconcile should adopt the existing RS, not fail
    let result = reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await;
    assert!(
        result.is_ok(),
        "Second reconcile must adopt existing RS: {:?}",
        result.err()
    );

    // Still exactly 1 ReplicaSet
    let rs_list = db
        .list_resources(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        rs_list.items.len(),
        1,
        "Must have exactly 1 RS after adopting"
    );
}

#[tokio::test]
async fn test_reconcile_deployment_adopts_orphan_matching_replicaset() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let deploy_uid = "deploy-uid-adopt-orphan";

    let deploy = make_deployment("myapp", "default", deploy_uid, 1, "0");
    let created_deploy = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "myapp",
            deploy.clone(),
        )
        .await
        .unwrap();

    // Orphan RS with matching labels/selectors (no ownerReferences)
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "orphan-rs",
        serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "orphan-rs",
                "namespace": "default",
                "labels": {"app": "myapp"}
            },
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "myapp"}},
                "template": deploy["spec"]["template"].clone()
            }
        }),
    )
    .await
    .unwrap();

    let deploy_with_rv = crate::api::inject_resource_version(
        created_deploy.data.clone(),
        created_deploy.resource_version,
    );
    reconcile_deployment(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &deploy_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("default"), "orphan-rs")
        .await
        .unwrap()
        .unwrap();

    let owner_refs = rs
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        owner_refs.iter().any(|o| {
            o.get("uid").and_then(|v| v.as_str()) == Some(deploy_uid)
                && o.get("kind").and_then(|v| v.as_str()) == Some("Deployment")
        }),
        "orphan matching RS must be adopted by Deployment controller"
    );
}
