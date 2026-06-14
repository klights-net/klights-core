use super::*;

#[tokio::test]
async fn test_statefulset_ordered_scale_down_halts_when_unhealthy() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-ordered-halt-001";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "ordered-halt", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 3,
            "serviceName": "ordered-halt-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "ordered-halt"}},
            "template": {
                "metadata": {"labels": {"app": "ordered-halt"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "ordered-halt",
            statefulset,
        )
        .await
        .unwrap();

    let mut sts_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = sts_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // Create all pods once (Parallel policy).
    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    // Mark all pods Ready.
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    for pod in pods.items {
        let pod_name = pod
            .data
            .pointer("/metadata/name")
            .and_then(|n| n.as_str())
            .unwrap()
            .to_string();
        let mut ready_pod: serde_json::Value = (*pod.data).clone();
        ready_pod["status"] = json!({
            "phase": "Running",
            "conditions": [{
                "type": "Ready",
                "status": "True"
            }]
        });
        db.update_resource(
            "v1",
            "Pod",
            Some("test-ns"),
            &pod_name,
            ready_pod,
            pod.resource_version,
        )
        .await
        .unwrap();
    }

    // Mark one existing pod unhealthy.
    let unhealthy = db
        .get_resource("v1", "Pod", Some("test-ns"), "ordered-halt-1")
        .await
        .unwrap()
        .unwrap();
    let mut unhealthy_pod: serde_json::Value = (*unhealthy.data).clone();
    unhealthy_pod["status"] = json!({
        "phase": "Running",
        "conditions": [{
            "type": "Ready",
            "status": "False"
        }]
    });
    db.update_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "ordered-halt-1",
        unhealthy_pod,
        unhealthy.resource_version,
    )
    .await
    .unwrap();

    // Scale down to 0 and reconcile: OrderedReady should halt scale-down.
    let current_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "ordered-halt")
        .await
        .unwrap()
        .unwrap();
    let updated = db
        .update_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "ordered-halt",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {"name": "ordered-halt", "namespace": "test-ns", "uid": sts_uid},
                "spec": {
                    "replicas": 0,
                    "serviceName": "ordered-halt-svc",
                    "podManagementPolicy": "OrderedReady",
                    "selector": {"matchLabels": {"app": "ordered-halt"}},
                    "template": {
                        "metadata": {"labels": {"app": "ordered-halt"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                    }
                }
            }),
            current_sts.resource_version,
        )
        .await
        .unwrap();
    let mut sts_scale_down: serde_json::Value = (*updated.data).clone();
    if let Some(meta) = sts_scale_down
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(updated.resource_version.to_string()),
        );
    }
    reconcile_statefulset_test(&db, &sts_scale_down, "test-node")
        .await
        .unwrap();

    let pods_after = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods_after.items.len(),
        3,
        "OrderedReady scale-down must halt when any pod is unhealthy"
    );
}

#[tokio::test]
async fn statefulset_zero_ordinal_scale_down_with_parity() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-ordered-zero-delete-001";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "ordered-zero", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 1,
            "serviceName": "ordered-zero-svc",
            "podManagementPolicy": "OrderedReady",
            "selector": {"matchLabels": {"app": "ordered-zero"}},
            "template": {
                "metadata": {"labels": {"app": "ordered-zero"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "ordered-zero",
            statefulset,
        )
        .await
        .unwrap();
    let mut sts_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = sts_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    let pod = db
        .get_resource("v1", "Pod", Some("test-ns"), "ordered-zero-0")
        .await
        .unwrap()
        .unwrap();
    let mut unready_pod: serde_json::Value = (*pod.data).clone();
    unready_pod["status"] = json!({
        "phase": "Pending",
        "conditions": [{
            "type": "Ready",
            "status": "False"
        }]
    });
    db.update_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "ordered-zero-0",
        unready_pod,
        pod.resource_version,
    )
    .await
    .unwrap();

    let current_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "ordered-zero")
        .await
        .unwrap()
        .unwrap();
    let updated = db
        .update_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "ordered-zero",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {"name": "ordered-zero", "namespace": "test-ns", "uid": sts_uid},
                "spec": {
                    "replicas": 0,
                    "serviceName": "ordered-zero-svc",
                    "podManagementPolicy": "OrderedReady",
                    "selector": {"matchLabels": {"app": "ordered-zero"}},
                    "template": {
                        "metadata": {"labels": {"app": "ordered-zero"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                    }
                }
            }),
            current_sts.resource_version,
        )
        .await
        .unwrap();
    let mut scaled_down: serde_json::Value = (*updated.data).clone();
    if let Some(meta) = scaled_down
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(updated.resource_version.to_string()),
        );
    }

    reconcile_statefulset_test(&db, &scaled_down, "test-node")
        .await
        .unwrap();

    let pod_after = db
        .get_resource("v1", "Pod", Some("test-ns"), "ordered-zero-0")
        .await
        .unwrap()
        .unwrap();
    assert!(
        pod_after
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some(),
        "OrderedReady scale-down to zero must delete ordinal 0 even when that pod is not Ready"
    );
}

#[tokio::test]
async fn test_statefulset_ordered_scale_down_deletes_one_pod_per_reconcile() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-ordered-single-delete-001";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "ordered-delete", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 3,
            "serviceName": "ordered-delete-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "ordered-delete"}},
            "template": {
                "metadata": {"labels": {"app": "ordered-delete"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "ordered-delete",
            statefulset,
        )
        .await
        .unwrap();
    let mut sts_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = sts_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 3);
    for pod in pods.items {
        let pod_name = pod
            .data
            .pointer("/metadata/name")
            .and_then(|n| n.as_str())
            .unwrap()
            .to_string();
        let mut ready_pod: serde_json::Value = (*pod.data).clone();
        ready_pod["status"] = json!({
            "phase": "Running",
            "conditions": [{
                "type": "Ready",
                "status": "True"
            }]
        });
        db.update_resource(
            "v1",
            "Pod",
            Some("test-ns"),
            &pod_name,
            ready_pod,
            pod.resource_version,
        )
        .await
        .unwrap();
    }

    let current_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "ordered-delete")
        .await
        .unwrap()
        .unwrap();
    let updated = db
        .update_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "ordered-delete",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {
                    "name": "ordered-delete",
                    "namespace": "test-ns",
                    "uid": sts_uid
                },
                "spec": {
                    "replicas": 0,
                    "serviceName": "ordered-delete-svc",
                    "podManagementPolicy": "OrderedReady",
                    "selector": {"matchLabels": {"app": "ordered-delete"}},
                    "template": {
                        "metadata": {"labels": {"app": "ordered-delete"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                    }
                }
            }),
            current_sts.resource_version,
        )
        .await
        .unwrap();
    let mut scaled_down: serde_json::Value = (*updated.data).clone();
    if let Some(meta) = scaled_down
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(updated.resource_version.to_string()),
        );
    }

    reconcile_statefulset_test(&db, &scaled_down, "test-node")
        .await
        .unwrap();

    let pods_after = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let terminating: Vec<String> = pods_after
        .items
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_some())
        .filter_map(|pod| {
            pod.data
                .pointer("/metadata/name")
                .and_then(|name| name.as_str())
                .map(str::to_string)
        })
        .collect();

    assert_eq!(
        terminating,
        vec!["ordered-delete-2".to_string()],
        "OrderedReady scale-down must delete only the highest ordinal per reconcile"
    );

    let fresh_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "ordered-delete")
        .await
        .unwrap()
        .unwrap();
    let mut fresh_scaled_down: serde_json::Value = (*fresh_sts.data).clone();
    if let Some(meta) = fresh_scaled_down
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(fresh_sts.resource_version.to_string()),
        );
    }

    reconcile_statefulset_test(&db, &fresh_scaled_down, "test-node")
        .await
        .unwrap();

    let pods_after_second_reconcile = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let terminating_after_second_reconcile: Vec<String> = pods_after_second_reconcile
        .items
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_some())
        .filter_map(|pod| {
            pod.data
                .pointer("/metadata/name")
                .and_then(|name| name.as_str())
                .map(str::to_string)
        })
        .collect();

    assert_eq!(
        terminating_after_second_reconcile,
        vec!["ordered-delete-2".to_string()],
        "OrderedReady scale-down must wait for a terminating higher ordinal to be finalized"
    );
}

#[tokio::test]
async fn test_statefulset_scale_up_waits_for_ready() {
    // OrderedReady: when pod-0 is not Ready, reconcile should not create pod-1
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-ready-003";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "ready-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 3,
            "serviceName": "ready-svc",
            "podManagementPolicy": "OrderedReady",
            "selector": {"matchLabels": {"app": "ready"}},
            "template": {
                "metadata": {"labels": {"app": "ready"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "ready-sts",
            statefulset,
        )
        .await
        .unwrap();

    let mut sts_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = sts_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // First reconcile - creates pod-0
    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 1, "Should create only pod-0");

    // Mark pod-0 as NOT Ready (phase=Pending, Ready condition False)
    let pod_0 = &pods.items[0];
    let mut updated_pod_0: serde_json::Value = (*pod_0.data).clone();
    if let Some(obj) = updated_pod_0.as_object_mut() {
        obj.insert(
            "status".to_string(),
            json!({
                "phase": "Pending",
                "conditions": [{
                    "type": "Ready",
                    "status": "False"
                }]
            }),
        );
    }

    db.update_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "ready-sts-0",
        updated_pod_0,
        pod_0.resource_version,
    )
    .await
    .unwrap();

    // Get updated STS
    let current_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "ready-sts")
        .await
        .unwrap()
        .unwrap();

    let mut sts_with_rv_2: serde_json::Value = (*current_sts.data).clone();
    if let Some(meta) = sts_with_rv_2
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(current_sts.resource_version.to_string()),
        );
    }

    // Second reconcile - should NOT create pod-1 because pod-0 is not Ready
    reconcile_statefulset_test(&db, &sts_with_rv_2, "test-node")
        .await
        .unwrap();

    let pods_after = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods_after.items.len(),
        1,
        "Should still have only 1 pod (pod-0 not Ready)"
    );
}

#[tokio::test]
async fn test_statefulset_parallel_policy() {
    // Parallel policy creates all pods at once, regardless of Ready status
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-parallel-004";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "parallel-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 3,
            "serviceName": "parallel-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "parallel"}},
            "template": {
                "metadata": {"labels": {"app": "parallel"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "parallel-sts",
            statefulset,
        )
        .await
        .unwrap();

    let mut sts_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = sts_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // Single reconcile should create all 3 pods at once (Parallel policy)
    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    assert_eq!(
        pods.items.len(),
        3,
        "Parallel policy should create all pods at once"
    );

    // Verify all pod names are correct
    let pod_names: Vec<String> = pods
        .items
        .iter()
        .map(|p| {
            p.data
                .pointer("/metadata/name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();

    assert!(pod_names.contains(&"parallel-sts-0".to_string()));
    assert!(pod_names.contains(&"parallel-sts-1".to_string()));
    assert!(pod_names.contains(&"parallel-sts-2".to_string()));
}

#[tokio::test]
async fn test_statefulset_pod_naming() {
    // Verify deterministic pod names: {statefulset-name}-{ordinal}
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-naming-005";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "my-app", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 5,
            "serviceName": "my-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "myapp"}},
            "template": {
                "metadata": {"labels": {"app": "myapp"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "my-app",
            statefulset,
        )
        .await
        .unwrap();

    let mut sts_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = sts_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    assert_eq!(pods.items.len(), 5, "Should create 5 pods");

    let mut pod_names: Vec<String> = pods
        .items
        .iter()
        .map(|p| {
            p.data
                .pointer("/metadata/name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();
    pod_names.sort();

    // Verify deterministic naming: my-app-0, my-app-1, ..., my-app-4
    assert_eq!(pod_names[0], "my-app-0");
    assert_eq!(pod_names[1], "my-app-1");
    assert_eq!(pod_names[2], "my-app-2");
    assert_eq!(pod_names[3], "my-app-3");
    assert_eq!(pod_names[4], "my-app-4");
}

// S4.4 StatefulSet stable network identity tests

#[tokio::test]
async fn test_statefulset_pod_hostname() {
    // Verify each StatefulSet pod gets hostname = pod name
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-hostname-001";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "web", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 3,
            "serviceName": "web-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "web"}},
            "template": {
                "metadata": {"labels": {"app": "web"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "web",
            statefulset,
        )
        .await
        .unwrap();

    let mut sts_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = sts_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    assert_eq!(pods.items.len(), 3, "Should create 3 pods");

    // Verify each pod has hostname = pod name
    for pod in &pods.items {
        let pod_name = pod
            .data
            .pointer("/metadata/name")
            .and_then(|n| n.as_str())
            .unwrap();
        let hostname = pod
            .data
            .pointer("/spec/hostname")
            .and_then(|h| h.as_str())
            .unwrap();

        assert_eq!(
            hostname, pod_name,
            "Pod hostname must match pod name for stable network identity"
        );
    }
}

#[tokio::test]
async fn test_statefulset_pod_subdomain() {
    // Verify each StatefulSet pod gets subdomain = serviceName
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-subdomain-002";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "db", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 2,
            "serviceName": "db-headless",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "database"}},
            "template": {
                "metadata": {"labels": {"app": "database"}},
                "spec": {"containers": [{"name": "postgres", "image": "postgres"}]}
            }
        }
    });

    let created = db
        .create_resource("apps/v1", "StatefulSet", Some("test-ns"), "db", statefulset)
        .await
        .unwrap();

    let mut sts_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = sts_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    assert_eq!(pods.items.len(), 2, "Should create 2 pods");

    // Verify each pod has subdomain = serviceName
    for pod in &pods.items {
        let subdomain = pod
            .data
            .pointer("/spec/subdomain")
            .and_then(|s| s.as_str())
            .unwrap();

        assert_eq!(
            subdomain, "db-headless",
            "Pod subdomain must match StatefulSet serviceName"
        );
    }
}

#[tokio::test]
#[ignore = "Requires root for nftables/netlink"]
async fn test_statefulset_headless_service_endpoints() {
    // Verify headless service gets individual pod IPs in Endpoints
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    // Create headless service (clusterIP: None)
    let headless_svc = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "web-headless", "namespace": "test-ns"},
        "spec": {
            "clusterIP": "None",
            "selector": {"app": "web"},
            "ports": [{"port": 80, "targetPort": 80}]
        }
    });

    db.create_resource(
        "v1",
        "Service",
        Some("test-ns"),
        "web-headless",
        headless_svc,
    )
    .await
    .unwrap();

    // Create StatefulSet with serviceName = web-headless
    let sts_uid = "sts-headless-003";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "web", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 2,
            "serviceName": "web-headless",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "web"}},
            "template": {
                "metadata": {"labels": {"app": "web"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "web",
            statefulset,
        )
        .await
        .unwrap();

    let mut sts_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(meta) = sts_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    // Manually assign pod IPs (in real kubelet this happens via CRI)
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    let mut pod_ips = vec![];
    for (idx, pod) in pods.items.iter().enumerate() {
        let pod_ip = format!("10.43.0.{}", idx + 10);
        pod_ips.push(pod_ip.clone());

        let pod_name = pod
            .data
            .pointer("/metadata/name")
            .and_then(|n| n.as_str())
            .unwrap();

        let mut updated_pod: serde_json::Value = (*pod.data).clone();
        if let Some(obj) = updated_pod.as_object_mut() {
            obj.insert(
                "status".to_string(),
                json!({
                    "phase": "Running",
                    "podIP": pod_ip,
                    "conditions": [{
                        "type": "Ready",
                        "status": "True"
                    }]
                }),
            );
        }

        db.update_resource(
            "v1",
            "Pod",
            Some("test-ns"),
            pod_name,
            updated_pod,
            pod.resource_version,
        )
        .await
        .unwrap();
    }

    // Trigger endpoint reconciliation (normally done by endpoint controller)
    // For this test, we verify that pod_manager reconciles endpoints
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    crate::kubelet::pod_endpoints::reconcile_endpoints_for_pod(
        &db,
        pod_repo.as_ref(),
        &pods.items[0].data,
        None,
    )
    .await
    .unwrap();
    crate::kubelet::pod_endpoints::reconcile_endpoints_for_pod(
        &db,
        pod_repo.as_ref(),
        &pods.items[1].data,
        None,
    )
    .await
    .unwrap();

    // Verify Endpoints resource has individual pod IPs
    let endpoints = db
        .get_resource("v1", "Endpoints", Some("test-ns"), "web-headless")
        .await
        .unwrap();

    assert!(
        endpoints.is_some(),
        "Endpoints should exist for headless service"
    );

    let ep = endpoints.unwrap();
    let subsets = ep
        .data
        .pointer("/subsets")
        .and_then(|s| s.as_array())
        .unwrap();

    assert!(
        !subsets.is_empty(),
        "Endpoints should have subsets with pod IPs"
    );

    // Verify individual pod IPs are in endpoints
    let addresses = subsets[0]
        .get("addresses")
        .and_then(|a| a.as_array())
        .unwrap();

    assert_eq!(
        addresses.len(),
        2,
        "Endpoints should have 2 addresses (one per pod)"
    );

    let endpoint_ips: Vec<String> = addresses
        .iter()
        .filter_map(|addr| addr.get("ip").and_then(|ip| ip.as_str()))
        .map(String::from)
        .collect();

    for pod_ip in &pod_ips {
        assert!(
            endpoint_ips.contains(pod_ip),
            "Endpoints must include pod IP {}",
            pod_ip
        );
    }
}

#[tokio::test]
async fn test_is_pod_ready_nonexistent_pod_returns_false() {
    let db = crate::datastore::test_support::in_memory().await;
    let ready = is_pod_ready(
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
        "nonexistent-pod",
    )
    .await
    .unwrap();
    assert!(!ready, "Nonexistent pod must not be considered Ready");
}

#[tokio::test]
async fn test_is_pod_ready_no_status_returns_false() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "no-status-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "no-status-pod", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "nginx"}]}
        }),
    )
    .await
    .unwrap();

    let ready = is_pod_ready(
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
        "no-status-pod",
    )
    .await
    .unwrap();
    assert!(!ready, "Pod without status must not be Ready");
}

#[tokio::test]
async fn test_is_pod_ready_no_ready_condition_returns_false() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "no-condition-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "no-condition-pod", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "nginx"}]},
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Initialized", "status": "True"}]
            }
        }),
    )
    .await
    .unwrap();

    let ready = is_pod_ready(
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
        "no-condition-pod",
    )
    .await
    .unwrap();
    assert!(!ready, "Pod without Ready condition must not be Ready");
}

#[tokio::test]
async fn test_is_pod_ready_condition_true_returns_true() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "ready-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "ready-pod", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "nginx"}]},
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        }),
    )
    .await
    .unwrap();

    let ready = is_pod_ready(
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
        "ready-pod",
    )
    .await
    .unwrap();
    assert!(ready, "Pod with Ready=True must be Ready");
}
