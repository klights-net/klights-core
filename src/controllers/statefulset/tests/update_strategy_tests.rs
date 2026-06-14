use super::*;

fn active_pods(items: &[crate::datastore::Resource]) -> Vec<&crate::datastore::Resource> {
    items
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_none())
        .collect()
}

async fn current_statefulset(
    db: &crate::datastore::sqlite::Datastore,
    namespace: &str,
    name: &str,
) -> serde_json::Value {
    let sts = db
        .get_resource("apps/v1", "StatefulSet", Some(namespace), name)
        .await
        .unwrap()
        .unwrap();
    crate::api::inject_resource_version(sts.data, sts.resource_version)
}

async fn reconcile_current_statefulset(
    db: &crate::datastore::sqlite::Datastore,
    namespace: &str,
    name: &str,
) {
    let sts = current_statefulset(db, namespace, name).await;
    reconcile_statefulset_test(db, &sts, "test-node")
        .await
        .unwrap();
}

async fn mark_statefulset_pods_ready(db: &crate::datastore::sqlite::Datastore, namespace: &str) {
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some(namespace),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    for pod in pods.items {
        if pod.data.pointer("/metadata/deletionTimestamp").is_some() {
            continue;
        }
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
            Some(namespace),
            &pod_name,
            ready_pod,
            pod.resource_version,
        )
        .await
        .unwrap();
    }
}

async fn update_statefulset_spec(
    db: &crate::datastore::sqlite::Datastore,
    namespace: &str,
    name: &str,
    update: impl FnOnce(&mut serde_json::Value),
) {
    let current = db
        .get_resource("apps/v1", "StatefulSet", Some(namespace), name)
        .await
        .unwrap()
        .unwrap();
    let mut body: serde_json::Value = (*current.data).clone();
    update(&mut body);
    db.update_resource(
        "apps/v1",
        "StatefulSet",
        Some(namespace),
        name,
        body,
        current.resource_version,
    )
    .await
    .unwrap();
}

async fn finalize_pod(db: &crate::datastore::sqlite::Datastore, namespace: &str, name: &str) {
    db.delete_resource("v1", "Pod", Some(namespace), name)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_is_pod_ready_condition_false_returns_false() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "not-ready-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "not-ready-pod", "namespace": "default"},
            "spec": {"containers": [{"name": "c", "image": "nginx"}]},
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "False"}]
            }
        }),
    )
    .await
    .unwrap();

    let ready = is_pod_ready(
        crate::controllers::test_utils::pod_repository_for_test(&db).as_ref(),
        "default",
        "not-ready-pod",
    )
    .await
    .unwrap();
    assert!(!ready, "Pod with Ready=False must not be Ready");
}

#[tokio::test]
async fn test_statefulset_ordered_creates_next_after_ready() {
    // OrderedReady: once pod-0 becomes Ready, pod-1 should be created on next reconcile
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-next-ready-001";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "next-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 3,
            "serviceName": "next-svc",
            "podManagementPolicy": "OrderedReady",
            "selector": {"matchLabels": {"app": "next"}},
            "template": {
                "metadata": {"labels": {"app": "next"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "next-sts",
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
    assert_eq!(pods.items.len(), 1, "Should create only pod-0 initially");

    // Mark pod-0 as Ready
    let pod_0 = &pods.items[0];
    let mut updated_pod: serde_json::Value = (*pod_0.data).clone();
    updated_pod["status"] = json!({
        "phase": "Running",
        "conditions": [{"type": "Ready", "status": "True"}]
    });
    db.update_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "next-sts-0",
        updated_pod,
        pod_0.resource_version,
    )
    .await
    .unwrap();

    // Re-fetch STS after first reconcile updated it
    let current_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "next-sts")
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

    // Second reconcile - pod-0 is Ready, should now create pod-1
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
        2,
        "Should create pod-1 after pod-0 becomes Ready"
    );

    let pod_names: Vec<String> = pods_after
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
    assert!(pod_names.contains(&"next-sts-0".to_string()));
    assert!(pod_names.contains(&"next-sts-1".to_string()));
}

#[tokio::test]
async fn test_statefulset_labels_propagated_from_template() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-labels-001";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "labels-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 1,
            "serviceName": "labels-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "labels-test"}},
            "template": {
                "metadata": {"labels": {"app": "labels-test", "tier": "backend", "version": "v2"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "labels-sts",
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
    assert_eq!(pods.items.len(), 1);

    let pod_labels = pods.items[0].data.pointer("/metadata/labels").unwrap();
    assert_eq!(pod_labels["app"].as_str(), Some("labels-test"));
    assert_eq!(pod_labels["tier"].as_str(), Some("backend"));
    assert_eq!(pod_labels["version"].as_str(), Some("v2"));
}

#[tokio::test]
async fn test_statefulset_skips_reconcile_when_deletion_timestamp_set() {
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

    let sts = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": "deleting-sts",
            "namespace": "test-ns",
            "uid": "sts-uid-del",
            "deletionTimestamp": "2026-04-12T00:00:00Z"
        },
        "spec": {
            "replicas": 3,
            "serviceName": "test-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "test"}},
            "template": {
                "metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    reconcile_statefulset_test(&db, &sts, "test-node")
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
        0,
        "No pods should be created for a StatefulSet being deleted"
    );
}

#[tokio::test]
async fn test_statefulset_canary_update_with_partition() {
    // partition=2 with 3 replicas: only pod-2 should be updated to new image
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-canary-001";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "canary-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 3,
            "serviceName": "canary-svc",
            "podManagementPolicy": "Parallel",
            "updateStrategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"partition": 2}
            },
            "selector": {"matchLabels": {"app": "canary"}},
            "template": {
                "metadata": {"labels": {"app": "canary"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx:1.0"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "canary-sts",
            statefulset,
        )
        .await
        .unwrap();

    let sts_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);

    // Initial reconcile - creates 3 pods with nginx:1.0
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

    // Mark pods Ready so rolling update can proceed.
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

    // Now update the template to nginx:2.0 with partition=2
    let current_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "canary-sts")
        .await
        .unwrap()
        .unwrap();

    let updated = db
        .update_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "canary-sts",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {"name": "canary-sts", "namespace": "test-ns", "uid": sts_uid},
                "spec": {
                    "replicas": 3,
                    "serviceName": "canary-svc",
                    "podManagementPolicy": "Parallel",
                    "updateStrategy": {
                        "type": "RollingUpdate",
                        "rollingUpdate": {"partition": 2}
                    },
                    "selector": {"matchLabels": {"app": "canary"}},
                    "template": {
                        "metadata": {"labels": {"app": "canary"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx:2.0"}]}
                    }
                }
            }),
            current_sts.resource_version,
        )
        .await
        .unwrap();

    let sts_with_rv2 = crate::api::inject_resource_version(updated.data, updated.resource_version);

    // Reconcile with updated template - should only update pod-2.
    reconcile_statefulset_test(&db, &sts_with_rv2, "test-node")
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
        "terminating old pod remains until actor finalization"
    );
    let active_after = active_pods(&pods_after.items);
    assert_eq!(
        active_after.len(),
        2,
        "partitioned rolling update should wait for actor finalization before recreating same-name pod"
    );

    // Verify pod-0 and pod-1 still have nginx:1.0, and pod-2 is terminating
    // on the old UID before the same-name replacement is allowed.
    for pod in &pods_after.items {
        let pod_name = pod
            .data
            .pointer("/metadata/name")
            .and_then(|n| n.as_str())
            .unwrap();
        let image = pod
            .data
            .pointer("/spec/containers/0/image")
            .and_then(|i| i.as_str())
            .unwrap();

        if pod_name == "canary-sts-0" || pod_name == "canary-sts-1" {
            assert_eq!(
                image, "nginx:1.0",
                "Pod {} should keep old image (ordinal < partition)",
                pod_name
            );
        } else if pod_name == "canary-sts-2" {
            assert_eq!(
                image, "nginx:1.0",
                "Pod {} keeps old image until actor finalizes the old UID",
                pod_name
            );
            assert!(
                pod.data.pointer("/metadata/deletionTimestamp").is_some(),
                "Pod {} should be terminating before same-name recreation",
                pod_name
            );
        }
    }
}

#[tokio::test]
async fn test_statefulset_partitioned_recreate_preserves_current_revision_template() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-partition-recreate-001";
    let old_template = json!({
        "metadata": {"labels": {"app": "partition"}},
        "spec": {
            "containers": [{
                "name": "webserver",
                "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"
            }]
        }
    });
    let new_template = json!({
        "metadata": {"labels": {"app": "partition"}},
        "spec": {
            "containers": [{
                "name": "webserver",
                "image": "registry.k8s.io/e2e-test-images/httpd:2.4.39-4"
            }]
        }
    });
    let old_revision = compute_statefulset_update_revision("partition-sts", &old_template);
    let new_revision = compute_statefulset_update_revision("partition-sts", &new_template);
    assert_ne!(old_revision, new_revision);

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "partition-sts",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {
                    "name": "partition-sts",
                    "namespace": "test-ns",
                    "uid": sts_uid
                },
                "spec": {
                    "replicas": 3,
                    "serviceName": "partition-svc",
                    "podManagementPolicy": "OrderedReady",
                    "updateStrategy": {
                        "type": "RollingUpdate",
                        "rollingUpdate": {"partition": 3}
                    },
                    "selector": {"matchLabels": {"app": "partition"}},
                    "template": old_template
                },
                "status": {
                    "currentRevision": old_revision,
                    "updateRevision": old_revision
                }
            }),
        )
        .await
        .unwrap();
    let sts_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);

    for _ in 0..3 {
        reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
            .await
            .unwrap();
        mark_statefulset_pods_ready(&db, "test-ns").await;
    }

    update_statefulset_spec(&db, "test-ns", "partition-sts", |sts| {
        sts["spec"]["template"] = new_template.clone();
    })
    .await;
    reconcile_current_statefulset(&db, "test-ns", "partition-sts").await;

    update_statefulset_spec(&db, "test-ns", "partition-sts", |sts| {
        sts["spec"]["updateStrategy"]["rollingUpdate"]["partition"] = json!(2);
    })
    .await;
    reconcile_current_statefulset(&db, "test-ns", "partition-sts").await;
    finalize_pod(&db, "test-ns", "partition-sts-2").await;
    reconcile_current_statefulset(&db, "test-ns", "partition-sts").await;
    mark_statefulset_pods_ready(&db, "test-ns").await;

    finalize_pod(&db, "test-ns", "partition-sts-0").await;
    finalize_pod(&db, "test-ns", "partition-sts-2").await;
    reconcile_current_statefulset(&db, "test-ns", "partition-sts").await;
    mark_statefulset_pods_ready(&db, "test-ns").await;
    reconcile_current_statefulset(&db, "test-ns", "partition-sts").await;
    mark_statefulset_pods_ready(&db, "test-ns").await;

    update_statefulset_spec(&db, "test-ns", "partition-sts", |sts| {
        sts["spec"]["updateStrategy"]["rollingUpdate"]["partition"] = json!(1);
    })
    .await;
    reconcile_current_statefulset(&db, "test-ns", "partition-sts").await;
    finalize_pod(&db, "test-ns", "partition-sts-1").await;
    reconcile_current_statefulset(&db, "test-ns", "partition-sts").await;
    mark_statefulset_pods_ready(&db, "test-ns").await;

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let active = active_pods(&pods.items);
    assert_eq!(active.len(), 3);

    for pod in active {
        let pod_name = pod
            .data
            .pointer("/metadata/name")
            .and_then(|n| n.as_str())
            .unwrap();
        let image = pod
            .data
            .pointer("/spec/containers/0/image")
            .and_then(|i| i.as_str())
            .unwrap();
        let revision = pod
            .data
            .pointer("/metadata/labels/controller-revision-hash")
            .and_then(|r| r.as_str())
            .unwrap();

        match pod_name {
            "partition-sts-0" => {
                assert_eq!(image, "registry.k8s.io/e2e-test-images/httpd:2.4.38-4");
                assert_eq!(revision, old_revision);
            }
            "partition-sts-1" | "partition-sts-2" => {
                assert_eq!(image, "registry.k8s.io/e2e-test-images/httpd:2.4.39-4");
                assert_eq!(revision, new_revision);
            }
            other => panic!("unexpected StatefulSet pod {other}"),
        }
    }
}

#[tokio::test]
async fn test_statefulset_partitioned_update_waits_for_terminating_higher_ordinal() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let old_template = json!({
        "metadata": {"labels": {"app": "partition-wait"}},
        "spec": {"containers": [{"name": "webserver", "image": "httpd:old"}]}
    });
    let new_template = json!({
        "metadata": {"labels": {"app": "partition-wait"}},
        "spec": {"containers": [{"name": "webserver", "image": "httpd:new"}]}
    });
    let old_revision = compute_statefulset_update_revision("partition-wait", &old_template);
    let new_revision = compute_statefulset_update_revision("partition-wait", &new_template);
    let sts_uid = "sts-partition-wait-001";

    let statefulset = crate::controllers::test_utils::store_and_prepare(
        &db,
        "apps/v1",
        "StatefulSet",
        Some("test-ns"),
        "partition-wait",
        json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {
                "name": "partition-wait",
                "namespace": "test-ns",
                "uid": sts_uid
            },
            "spec": {
                "replicas": 3,
                "serviceName": "partition-wait-svc",
                "podManagementPolicy": "OrderedReady",
                "updateStrategy": {
                    "type": "RollingUpdate",
                    "rollingUpdate": {"partition": 1}
                },
                "selector": {"matchLabels": {"app": "partition-wait"}},
                "template": new_template
            },
            "status": {
                "currentRevision": old_revision,
                "updateRevision": new_revision
            }
        }),
    )
    .await;

    for (ordinal, revision, image, deleting) in [
        (0, old_revision.as_str(), "httpd:old", true),
        (1, old_revision.as_str(), "httpd:old", false),
        (2, new_revision.as_str(), "httpd:new", true),
    ] {
        let pod_name = format!("partition-wait-{ordinal}");
        let mut pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "test-ns",
                "uid": format!("pod-{ordinal}"),
                "labels": {
                    "app": "partition-wait",
                    "controller-revision-hash": revision
                },
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "StatefulSet",
                    "name": "partition-wait",
                    "uid": sts_uid,
                    "controller": true
                }]
            },
            "spec": {
                "nodeName": "test-node",
                "containers": [{"name": "webserver", "image": image}]
            },
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        if deleting {
            pod["metadata"]["deletionTimestamp"] = json!("2026-05-16T19:03:16Z");
            pod["metadata"]["deletionGracePeriodSeconds"] = json!(0);
        }
        db.create_resource("v1", "Pod", Some("test-ns"), &pod_name, pod)
            .await
            .unwrap();
    }

    reconcile_statefulset_test(&db, &statefulset, "test-node")
        .await
        .unwrap();

    let pod1 = db
        .get_resource("v1", "Pod", Some("test-ns"), "partition-wait-1")
        .await
        .unwrap()
        .unwrap();
    assert!(
        pod1.data.pointer("/metadata/deletionTimestamp").is_none(),
        "partitioned rolling update must wait for higher ordinals to finish terminating before deleting the next lower ordinal"
    );
}

#[tokio::test]
async fn test_statefulset_rolling_update_waits_for_actor_finalization() {
    // Without partition (partition=0), StatefulSet starts from the highest
    // ordinal but must not recreate the same Pod name until the actor finalizes
    // the old UID.
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-roll-001";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "roll-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 2,
            "serviceName": "roll-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "roll"}},
            "template": {
                "metadata": {"labels": {"app": "roll"}},
                "spec": {"containers": [{"name": "app", "image": "app:v1"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "roll-sts",
            statefulset,
        )
        .await
        .unwrap();

    let sts_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);
    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    // Mark pods Ready so rolling update can proceed.
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

    // Update template to app:v2
    let current_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "roll-sts")
        .await
        .unwrap()
        .unwrap();

    let updated = db
        .update_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "roll-sts",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {"name": "roll-sts", "namespace": "test-ns", "uid": sts_uid},
                "spec": {
                    "replicas": 2,
                    "serviceName": "roll-svc",
                    "podManagementPolicy": "Parallel",
                    "selector": {"matchLabels": {"app": "roll"}},
                    "template": {
                        "metadata": {"labels": {"app": "roll"}},
                        "spec": {"containers": [{"name": "app", "image": "app:v2"}]}
                    }
                }
            }),
            current_sts.resource_version,
        )
        .await
        .unwrap();

    let sts_with_rv2 = crate::api::inject_resource_version(updated.data, updated.resource_version);

    // Rolling update advances one ordinal per reconcile.
    reconcile_statefulset_test(&db, &sts_with_rv2, "test-node")
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
    assert_eq!(pods.items.len(), 2);
    assert_eq!(
        active_pods(&pods.items).len(),
        1,
        "rolling update should leave only one active pod until same-name UID is finalized"
    );

    let pod1 = db
        .get_resource("v1", "Pod", Some("test-ns"), "roll-sts-1")
        .await
        .unwrap()
        .unwrap();
    assert!(
        pod1.data.pointer("/metadata/deletionTimestamp").is_some(),
        "highest ordinal should be marked terminating and wait for actor finalization"
    );
    assert_eq!(
        pod1.data
            .pointer("/spec/containers/0/image")
            .and_then(|i| i.as_str()),
        Some("app:v1"),
        "same-name replacement must not be created before old UID finalization"
    );
}

#[tokio::test]
async fn test_statefulset_on_delete_strategy_skips_rolling_update() {
    // OnDelete strategy: pods are not automatically updated
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-ondel-001";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "ondel-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 2,
            "serviceName": "ondel-svc",
            "podManagementPolicy": "Parallel",
            "updateStrategy": {"type": "OnDelete"},
            "selector": {"matchLabels": {"app": "ondel"}},
            "template": {
                "metadata": {"labels": {"app": "ondel"}},
                "spec": {"containers": [{"name": "app", "image": "app:v1"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "ondel-sts",
            statefulset,
        )
        .await
        .unwrap();

    let sts_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);
    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    // Update template to app:v2 with OnDelete strategy
    let current_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "ondel-sts")
        .await
        .unwrap()
        .unwrap();

    let updated = db
        .update_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "ondel-sts",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {"name": "ondel-sts", "namespace": "test-ns", "uid": sts_uid},
                "spec": {
                    "replicas": 2,
                    "serviceName": "ondel-svc",
                    "podManagementPolicy": "Parallel",
                    "updateStrategy": {"type": "OnDelete"},
                    "selector": {"matchLabels": {"app": "ondel"}},
                    "template": {
                        "metadata": {"labels": {"app": "ondel"}},
                        "spec": {"containers": [{"name": "app", "image": "app:v2"}]}
                    }
                }
            }),
            current_sts.resource_version,
        )
        .await
        .unwrap();

    let sts_with_rv2 = crate::api::inject_resource_version(updated.data, updated.resource_version);
    reconcile_statefulset_test(&db, &sts_with_rv2, "test-node")
        .await
        .unwrap();

    // Pods should NOT be updated (OnDelete means user must delete pods manually)
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 2);

    for pod in &pods.items {
        let image = pod
            .data
            .pointer("/spec/containers/0/image")
            .and_then(|i| i.as_str())
            .unwrap();
        assert_eq!(
            image, "app:v1",
            "OnDelete: pods should keep old image until manually deleted"
        );
    }
}

#[tokio::test]
async fn test_statefulset_status_includes_revision_info() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-rev-001";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "rev-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 1,
            "serviceName": "rev-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "rev"}},
            "template": {
                "metadata": {"labels": {"app": "rev"}},
                "spec": {"containers": [{"name": "app", "image": "app:v1"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "rev-sts",
            statefulset,
        )
        .await
        .unwrap();

    let sts_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);
    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    let updated_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "rev-sts")
        .await
        .unwrap()
        .unwrap();

    let status = &updated_sts.data["status"];
    assert!(
        status.get("currentRevision").is_some(),
        "Status should have currentRevision"
    );
    assert!(
        status.get("updateRevision").is_some(),
        "Status should have updateRevision"
    );
    assert!(
        status.get("updatedReplicas").is_some(),
        "Status should have updatedReplicas"
    );
}

#[tokio::test]
async fn test_statefulset_rolling_update_keeps_current_revision_until_ready() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-rollout-rev-001";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "rollout-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 3,
            "serviceName": "rollout-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "rollout"}},
            "template": {
                "metadata": {"labels": {"app": "rollout"}},
                "spec": {"containers": [{"name": "app", "image": "app:v1"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "rollout-sts",
            statefulset,
        )
        .await
        .unwrap();

    let sts_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);
    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    let current_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "rollout-sts")
        .await
        .unwrap()
        .unwrap();

    let mut sts_update: serde_json::Value = (*current_sts.data).clone();
    sts_update["spec"]["template"]["spec"]["containers"][0]["image"] = json!("app:v2");
    db.update_resource(
        "apps/v1",
        "StatefulSet",
        Some("test-ns"),
        "rollout-sts",
        sts_update,
        current_sts.resource_version,
    )
    .await
    .unwrap();

    let after_spec_update = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "rollout-sts")
        .await
        .unwrap()
        .unwrap();
    let sts_with_rv2 = crate::api::inject_resource_version(
        after_spec_update.data,
        after_spec_update.resource_version,
    );
    reconcile_statefulset_test(&db, &sts_with_rv2, "test-node")
        .await
        .unwrap();

    let reconciled = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "rollout-sts")
        .await
        .unwrap()
        .unwrap();
    let status = &reconciled.data["status"];
    let current_revision = status
        .get("currentRevision")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let update_revision = status
        .get("updateRevision")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let ready_replicas = status
        .get("readyReplicas")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    assert_ne!(
        current_revision, update_revision,
        "currentRevision must remain old while rollout is not fully ready"
    );
    assert_eq!(
        ready_replicas, 0,
        "newly recreated pods should not be ready immediately in this controller unit flow"
    );
}

#[test]
fn test_compute_statefulset_update_revision_includes_template_metadata() {
    let template_v1 = json!({
        "metadata": {"labels": {"app": "demo"}},
        "spec": {"containers": [{"name": "app", "image": "app:v1"}]}
    });
    let template_v2 = json!({
        "metadata": {"labels": {"app": "demo"}, "annotations": {"rollout": "1"}},
        "spec": {"containers": [{"name": "app", "image": "app:v1"}]}
    });

    let rev1 = compute_statefulset_update_revision("demo-sts", &template_v1);
    let rev2 = compute_statefulset_update_revision("demo-sts", &template_v2);

    assert_ne!(
        rev1, rev2,
        "template metadata changes must produce a new StatefulSet updateRevision"
    );
}
