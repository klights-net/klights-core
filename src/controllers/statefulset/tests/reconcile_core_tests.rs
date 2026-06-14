use super::*;
use crate::datastore::Resource;
use crate::kubelet::pod_repository::PodObjectWriter;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn active_pod_count(items: &[crate::datastore::Resource]) -> usize {
    items
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_none())
        .count()
}

struct ScaleDownDuringStatefulSetCreateWriter {
    db: crate::datastore::sqlite::Datastore,
    creates: AtomicUsize,
}

#[async_trait::async_trait]
impl PodObjectWriter for ScaleDownDuringStatefulSetCreateWriter {
    async fn create_controller_pod(
        &self,
        ns: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        let count = self.creates.fetch_add(1, Ordering::SeqCst) + 1;
        let created = self
            .db
            .create_resource("v1", "Pod", Some(ns), name, pod)
            .await?;

        if count == 1 {
            let current_sts = self
                .db
                .get_resource("apps/v1", "StatefulSet", Some(ns), "scale-down-sts")
                .await?
                .expect("StatefulSet should exist");
            let mut scaled_sts: serde_json::Value = (*current_sts.data).clone();
            scaled_sts["spec"]["replicas"] = json!(1);
            self.db
                .update_resource(
                    "apps/v1",
                    "StatefulSet",
                    Some(ns),
                    "scale-down-sts",
                    scaled_sts,
                    current_sts.resource_version,
                )
                .await?;
        }

        Ok(created)
    }

    async fn delete_pod(&self, ns: &str, name: &str) -> anyhow::Result<()> {
        self.db.delete_resource("v1", "Pod", Some(ns), name).await
    }

    async fn update_pod_owner_references(
        &self,
        ns: &str,
        name: &str,
        owner_refs: Vec<serde_json::Value>,
    ) -> anyhow::Result<Resource> {
        let current = self
            .db
            .get_resource("v1", "Pod", Some(ns), name)
            .await?
            .expect("Pod should exist");
        let mut pod: serde_json::Value = (*current.data).clone();
        pod["metadata"]["ownerReferences"] = serde_json::Value::Array(owner_refs);
        self.db
            .update_resource("v1", "Pod", Some(ns), name, pod, current.resource_version)
            .await
    }

    async fn merge_pod_labels(
        &self,
        ns: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> anyhow::Result<Resource> {
        let current = self
            .db
            .get_resource("v1", "Pod", Some(ns), name)
            .await?
            .expect("Pod should exist");
        let mut pod: serde_json::Value = (*current.data).clone();
        let label_map = pod["metadata"]
            .as_object_mut()
            .unwrap()
            .entry("labels".to_string())
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .unwrap();
        for (key, value) in labels {
            label_map.insert(key, json!(value));
        }
        self.db
            .update_resource("v1", "Pod", Some(ns), name, pod, current.resource_version)
            .await
    }
}

#[tokio::test]
async fn test_statefulset_stale_snapshot_after_delete_does_not_recreate_pods() {
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

    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": "stale-sts",
            "namespace": "test-ns",
            "uid": "stale-sts-uid"
        },
        "spec": {
            "replicas": 1,
            "serviceName": "test-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "stale-sts"}},
            "template": {
                "metadata": {"labels": {"app": "stale-sts"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });
    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "stale-sts",
            statefulset,
        )
        .await
        .unwrap();
    let stale_snapshot = created.data.clone();

    db.delete_resource("apps/v1", "StatefulSet", Some("test-ns"), "stale-sts")
        .await
        .unwrap();

    reconcile_statefulset_test(&db, &stale_snapshot, "test-node")
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
    assert!(
        pods.items.is_empty(),
        "stale StatefulSet reconcile after delete must not recreate Pods"
    );
}

#[tokio::test]
async fn test_statefulset_create_loop_observes_live_scale_down() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_reader = crate::controllers::test_utils::pod_repository_for_test(&db);
    let pod_writer = Arc::new(ScaleDownDuringStatefulSetCreateWriter {
        db: db.clone(),
        creates: AtomicUsize::new(0),
    });

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": "scale-down-sts",
            "namespace": "test-ns",
            "uid": "scale-down-sts-uid"
        },
        "spec": {
            "replicas": 3,
            "serviceName": "test-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "scale-down-sts"}},
            "template": {
                "metadata": {"labels": {"app": "scale-down-sts"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });
    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "scale-down-sts",
            statefulset,
        )
        .await
        .unwrap();
    let sts_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);

    crate::controllers::statefulset::reconcile_statefulset(
        &db,
        pod_reader.as_ref(),
        pod_writer.as_ref(),
        pod_reader.as_ref(),
        &sts_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::new(Some("app=scale-down-sts"), None, None, None),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        1,
        "StatefulSet reconcile must stop creating Pods after live spec.replicas is lowered"
    );
}

#[tokio::test]
async fn test_reconcile_statefulset_creates_pods() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    // Create StatefulSet with 3 replicas (Parallel policy for quick creation in tests)
    let sts_uid = "sts-test-uid-001";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": "test-sts",
            "namespace": "test-ns",
            "uid": sts_uid
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

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "test-sts",
            statefulset.clone(),
        )
        .await
        .unwrap();

    // Inject resourceVersion before reconcile
    let mut sts_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(obj) = sts_with_rv.as_object_mut()
        && let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // Reconcile (should create 3 pods)
    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    // Verify 3 pods created
    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    assert_eq!(pods.items.len(), 3, "Expected 3 pods to be created");

    // Verify all pods have correct owner reference
    for pod in &pods.items {
        let owner_refs = pod
            .data
            .get("metadata")
            .and_then(|m| m.get("ownerReferences"))
            .and_then(|o| o.as_array())
            .unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(
            owner_refs[0].get("uid").and_then(|u| u.as_str()),
            Some(sts_uid)
        );
        assert_eq!(
            owner_refs[0].get("kind").and_then(|k| k.as_str()),
            Some("StatefulSet")
        );
    }
}

#[tokio::test]
async fn test_statefulset_deletes_failed_pod_for_recreation() {
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

    let sts_uid = "sts-test-uid-failed-recreate";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": "test-sts",
            "namespace": "test-ns",
            "uid": sts_uid
        },
        "spec": {
            "replicas": 1,
            "serviceName": "test-svc",
            "selector": {"matchLabels": {"app": "test"}},
            "template": {
                "metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let sts_with_rv = crate::controllers::test_utils::store_and_prepare(
        &db,
        "apps/v1",
        "StatefulSet",
        Some("test-ns"),
        "test-sts",
        statefulset,
    )
    .await;

    db.create_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "test-sts-0",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-sts-0",
                "namespace": "test-ns",
                "uid": "failed-pod-uid",
                "labels": {
                    "app": "test",
                    "controller-revision-hash": "old-rev"
                },
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "StatefulSet",
                    "name": "test-sts",
                    "uid": sts_uid,
                    "controller": true
                }]
            },
            "spec": {"nodeName": "test-node", "containers": [{"name": "nginx", "image": "nginx"}]},
            "status": {"phase": "Failed"}
        }),
    )
    .await
    .unwrap();

    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    let pod = db
        .get_resource("v1", "Pod", Some("test-ns"), "test-sts-0")
        .await
        .unwrap()
        .expect("failed pod row remains until actor finalization");
    assert!(
        pod.data.pointer("/metadata/deletionTimestamp").is_some(),
        "StatefulSet must mark Failed owned pods terminating so the actor can finalize the UID"
    );
}

#[tokio::test]
async fn test_reconcile_statefulset_scale_down() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create namespace
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let sts_uid = "sts-test-uid-002";

    // Create StatefulSet with 5 replicas initially (Parallel for quick setup)
    let statefulset_5 = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": "test-sts",
            "namespace": "test-ns",
            "uid": sts_uid
        },
        "spec": {
            "replicas": 5,
            "serviceName": "test-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "test"}},
            "template": {
                "metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "test-sts",
            statefulset_5.clone(),
        )
        .await
        .unwrap();

    // Inject resourceVersion before reconcile
    let mut sts_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(obj) = sts_with_rv.as_object_mut()
        && let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // Reconcile to create 5 pods
    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    let pods_before = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods_before.items.len(), 5, "Expected 5 pods initially");

    // Get the current StatefulSet state after first reconcile
    let current_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "test-sts")
        .await
        .unwrap()
        .unwrap();

    // Now scale down to 2 replicas - update via DB
    let updated = db
        .update_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "test-sts",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {
                    "name": "test-sts",
                    "namespace": "test-ns",
                    "uid": sts_uid
                },
                "spec": {
                    "replicas": 2,
                    "serviceName": "test-svc",
                    "podManagementPolicy": "Parallel",
                    "selector": {"matchLabels": {"app": "test"}},
                    "template": {
                        "metadata": {"labels": {"app": "test"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                    }
                }
            }),
            current_sts.resource_version,
        )
        .await
        .unwrap();

    // Inject resourceVersion before reconcile
    let mut sts_with_rv_2: serde_json::Value = (*updated.data).clone();
    if let Some(obj) = sts_with_rv_2.as_object_mut()
        && let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(updated.resource_version.to_string()),
        );
    }

    // Reconcile with updated replica count
    reconcile_statefulset_test(&db, &sts_with_rv_2, "test-node")
        .await
        .unwrap();

    // Verify only 2 pods remain
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
        active_pod_count(&pods_after.items),
        2,
        "Expected 2 active pods after scale down"
    );

    // Verify remaining pods are test-sts-0 and test-sts-1 (deleted from highest ordinal)
    let pod_names: Vec<String> = pods_after
        .items
        .iter()
        .filter(|p| p.data.pointer("/metadata/deletionTimestamp").is_none())
        .map(|p| {
            p.data
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();
    assert!(
        pod_names.contains(&"test-sts-0".to_string()),
        "Pod test-sts-0 should remain"
    );
    assert!(
        pod_names.contains(&"test-sts-1".to_string()),
        "Pod test-sts-1 should remain"
    );
}

#[tokio::test]
async fn test_statefulset_recreates_lowest_missing_ordinal_gap() {
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

    let sts_uid = "sts-test-uid-gap";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": "test-sts",
            "namespace": "test-ns",
            "uid": sts_uid
        },
        "spec": {
            "replicas": 3,
            "serviceName": "test-svc",
            "selector": {"matchLabels": {"app": "test"}},
            "template": {
                "metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let sts_with_rv = crate::controllers::test_utils::store_and_prepare(
        &db,
        "apps/v1",
        "StatefulSet",
        Some("test-ns"),
        "test-sts",
        statefulset,
    )
    .await;

    db.create_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "test-sts-1",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-sts-1",
                "namespace": "test-ns",
                "uid": "existing-ordinal-1",
                "labels": {"app": "test"},
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "StatefulSet",
                    "name": "test-sts",
                    "uid": sts_uid,
                    "controller": true
                }]
            },
            "spec": {"nodeName": "test-node", "containers": [{"name": "nginx", "image": "nginx"}]},
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [{"name": "nginx", "ready": true}]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    assert!(
        db.get_resource("v1", "Pod", Some("test-ns"), "test-sts-0")
            .await
            .unwrap()
            .is_some(),
        "OrderedReady StatefulSet must recreate the lowest missing ordinal first"
    );
    assert!(
        db.get_resource("v1", "Pod", Some("test-ns"), "test-sts-2")
            .await
            .unwrap()
            .is_none(),
        "OrderedReady StatefulSet must wait for lower missing ordinals before creating higher ones"
    );
}

#[tokio::test]
async fn test_statefulset_recreates_partitioned_lower_ordinal_with_current_revision() {
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

    let sts_uid = "sts-test-uid-partition-gap";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": "test-sts",
            "namespace": "test-ns",
            "uid": sts_uid
        },
        "spec": {
            "replicas": 3,
            "serviceName": "test-svc",
            "updateStrategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"partition": 2}
            },
            "selector": {"matchLabels": {"app": "test"}},
            "template": {
                "metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx:new"}]}
            }
        },
        "status": {
            "currentRevision": "test-sts-old",
            "updateRevision": "test-sts-new"
        }
    });

    let sts_with_rv = crate::controllers::test_utils::store_and_prepare(
        &db,
        "apps/v1",
        "StatefulSet",
        Some("test-ns"),
        "test-sts",
        statefulset,
    )
    .await;

    db.create_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "test-sts-1",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "test-sts-1",
                "namespace": "test-ns",
                "uid": "existing-ordinal-1",
                "labels": {
                    "app": "test",
                    "controller-revision-hash": "test-sts-old"
                },
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "StatefulSet",
                    "name": "test-sts",
                    "uid": sts_uid,
                    "controller": true
                }]
            },
            "spec": {"nodeName": "test-node", "containers": [{"name": "nginx", "image": "nginx:old"}]},
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [{"name": "nginx", "ready": true}]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    let recreated = db
        .get_resource("v1", "Pod", Some("test-ns"), "test-sts-0")
        .await
        .unwrap()
        .expect("missing lower ordinal should be recreated");
    assert_eq!(
        recreated.data.pointer("/spec/containers/0/image"),
        Some(&json!("nginx:old"))
    );
    assert_eq!(
        recreated
            .data
            .pointer("/metadata/labels/controller-revision-hash"),
        Some(&json!("test-sts-old"))
    );
}

#[tokio::test]
async fn test_reconcile_statefulset_zero_replicas() {
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

    let sts_uid = "sts-test-uid-zero";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "zero-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 0,
            "serviceName": "zero-svc",
            "selector": {"matchLabels": {"app": "zero"}},
            "template": {
                "metadata": {"labels": {"app": "zero"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "zero-sts",
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
    assert_eq!(pods.items.len(), 0, "Zero replicas should create zero pods");
}

#[tokio::test]
async fn test_reconcile_statefulset_ordinal_names() {
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

    let sts_uid = "sts-test-uid-ordinal";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "ordinal-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 3,
            "serviceName": "ordinal-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "ordinal"}},
            "template": {
                "metadata": {"labels": {"app": "ordinal"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "ordinal-sts",
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

    // Verify pod names are ordinal-sts-0, ordinal-sts-1, ordinal-sts-2
    let mut pod_names: Vec<String> = pods
        .items
        .iter()
        .map(|p| {
            p.data
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();
    pod_names.sort();

    assert_eq!(pod_names[0], "ordinal-sts-0");
    assert_eq!(pod_names[1], "ordinal-sts-1");
    assert_eq!(pod_names[2], "ordinal-sts-2");
}

#[tokio::test]
async fn test_reconcile_statefulset_owner_references() {
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

    let sts_uid = "sts-test-uid-owner";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "owner-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 1,
            "serviceName": "owner-svc",
            "selector": {"matchLabels": {"app": "owner"}},
            "template": {
                "metadata": {"labels": {"app": "owner"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "owner-sts",
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

    let owner_refs = pods.items[0].data["metadata"]["ownerReferences"]
        .as_array()
        .unwrap();
    assert_eq!(owner_refs.len(), 1);
    assert_eq!(owner_refs[0]["uid"], sts_uid);
    assert_eq!(owner_refs[0]["kind"], "StatefulSet");
    assert_eq!(owner_refs[0]["name"], "owner-sts");
    assert_eq!(owner_refs[0]["controller"], true);
    assert_eq!(owner_refs[0]["blockOwnerDeletion"], true);
    assert_eq!(owner_refs[0]["apiVersion"], "apps/v1");
}

#[tokio::test]
async fn test_reconcile_statefulset_idempotent() {
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

    let sts_uid = "sts-test-uid-idem";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "idem-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 2,
            "serviceName": "idem-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "idem"}},
            "template": {
                "metadata": {"labels": {"app": "idem"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "idem-sts",
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

    // First reconcile
    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    let pods_first = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods_first.items.len(), 2);

    // Re-fetch STS (status was updated) and reconcile again
    let current_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "idem-sts")
        .await
        .unwrap()
        .unwrap();

    let mut sts_with_rv2: serde_json::Value = (*current_sts.data).clone();
    if let Some(meta) = sts_with_rv2
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(current_sts.resource_version.to_string()),
        );
    }

    reconcile_statefulset_test(&db, &sts_with_rv2, "test-node")
        .await
        .unwrap();

    // Should still have exactly 2 pods
    let pods_second = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods_second.items.len(),
        2,
        "Idempotent reconcile must not create extra pods"
    );
}

#[tokio::test]
async fn test_reconcile_statefulset_status_updated() {
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

    let sts_uid = "sts-test-uid-status";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "status-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 4,
            "serviceName": "status-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "status"}},
            "template": {
                "metadata": {"labels": {"app": "status"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "status-sts",
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

    // Verify STS status was updated
    let updated_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "status-sts")
        .await
        .unwrap()
        .unwrap();

    let status = &updated_sts.data["status"];
    assert_eq!(status["replicas"], 4, "Status.replicas must match spec");
}

// S4.3 StatefulSet ordered create/delete tests

#[tokio::test]
async fn test_statefulset_ordered_creation() {
    // OrderedReady policy: create pods one at a time in order (0, 1, 2)
    // This test verifies that only the first pod is created on initial reconcile
    // when podManagementPolicy is OrderedReady (default)
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-ordered-001";
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "ordered-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 3,
            "serviceName": "ordered-svc",
            "podManagementPolicy": "OrderedReady",
            "selector": {"matchLabels": {"app": "ordered"}},
            "template": {
                "metadata": {"labels": {"app": "ordered"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "ordered-sts",
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

    // First reconcile - should create only ordered-sts-0
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
        1,
        "OrderedReady: should create only first pod (ordinal 0)"
    );
    assert_eq!(
        pods.items[0]
            .data
            .pointer("/metadata/name")
            .and_then(|n| n.as_str()),
        Some("ordered-sts-0"),
        "First pod must be ordinal 0"
    );
}

#[tokio::test]
async fn test_statefulset_reverse_deletion() {
    // Verify pods are deleted in reverse ordinal order (highest first)
    let db = crate::datastore::test_support::in_memory().await;

    db.create_namespace("test-ns", json!({"metadata": {"name": "test-ns"}}))
        .await
        .unwrap();

    let sts_uid = "sts-reverse-002";

    // Create StatefulSet with 4 replicas using Parallel policy for quick setup
    let statefulset = json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "reverse-sts", "namespace": "test-ns", "uid": sts_uid},
        "spec": {
            "replicas": 4,
            "serviceName": "reverse-svc",
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": "reverse"}},
            "template": {
                "metadata": {"labels": {"app": "reverse"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "reverse-sts",
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

    // Create all 4 pods
    reconcile_statefulset_test(&db, &sts_with_rv, "test-node")
        .await
        .unwrap();

    // Verify 4 pods exist
    let pods_before = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods_before.items.len(), 4);

    // Scale down to 2 replicas
    let current_sts = db
        .get_resource("apps/v1", "StatefulSet", Some("test-ns"), "reverse-sts")
        .await
        .unwrap()
        .unwrap();

    let updated = db
        .update_resource(
            "apps/v1",
            "StatefulSet",
            Some("test-ns"),
            "reverse-sts",
            json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {"name": "reverse-sts", "namespace": "test-ns", "uid": sts_uid},
                "spec": {
                    "replicas": 2,
                    "serviceName": "reverse-svc",
                    "podManagementPolicy": "Parallel",
                    "selector": {"matchLabels": {"app": "reverse"}},
                    "template": {
                        "metadata": {"labels": {"app": "reverse"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                    }
                }
            }),
            current_sts.resource_version,
        )
        .await
        .unwrap();

    let mut sts_with_rv_2: serde_json::Value = (*updated.data).clone();
    if let Some(meta) = sts_with_rv_2
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(updated.resource_version.to_string()),
        );
    }

    // Reconcile with scale-down
    reconcile_statefulset_test(&db, &sts_with_rv_2, "test-node")
        .await
        .unwrap();

    // Verify only reverse-sts-0 and reverse-sts-1 remain (deleted 3, then 2)
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
        active_pod_count(&pods_after.items),
        2,
        "Should have 2 active pods after scale-down"
    );

    let pod_names: Vec<String> = pods_after
        .items
        .iter()
        .filter(|p| p.data.pointer("/metadata/deletionTimestamp").is_none())
        .map(|p| {
            p.data
                .pointer("/metadata/name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();

    assert!(
        pod_names.contains(&"reverse-sts-0".to_string()),
        "reverse-sts-0 should remain"
    );
    assert!(
        pod_names.contains(&"reverse-sts-1".to_string()),
        "reverse-sts-1 should remain"
    );
    assert!(
        !pod_names.contains(&"reverse-sts-2".to_string()),
        "reverse-sts-2 should be marked terminating"
    );
    assert!(
        !pod_names.contains(&"reverse-sts-3".to_string()),
        "reverse-sts-3 should be marked terminating"
    );
}
