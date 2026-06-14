use super::*;
use crate::datastore::Resource;
use crate::kubelet::pod_repository::PodApiWriter;
use crate::kubelet::pod_repository::PodObjectWriter;
use anyhow::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct ScaleDownDuringCreateWriter {
    db: crate::datastore::sqlite::Datastore,
    creates: AtomicUsize,
}

#[async_trait::async_trait]
impl PodObjectWriter for ScaleDownDuringCreateWriter {
    async fn create_controller_pod(
        &self,
        ns: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> Result<Resource> {
        let count = self.creates.fetch_add(1, Ordering::SeqCst) + 1;
        let created = self
            .db
            .create_resource("v1", "Pod", Some(ns), name, pod)
            .await?;

        if count == 5 {
            let current_rs = self
                .db
                .get_resource("apps/v1", "ReplicaSet", Some(ns), "test-rs")
                .await?
                .expect("ReplicaSet should exist");
            let mut scaled_rs: serde_json::Value = (*current_rs.data).clone();
            scaled_rs["spec"]["replicas"] = json!(5);
            self.db
                .update_resource(
                    "apps/v1",
                    "ReplicaSet",
                    Some(ns),
                    "test-rs",
                    scaled_rs,
                    current_rs.resource_version,
                )
                .await?;
        }

        Ok(created)
    }

    async fn delete_pod(&self, ns: &str, name: &str) -> Result<()> {
        self.db.delete_resource("v1", "Pod", Some(ns), name).await
    }

    async fn update_pod_owner_references(
        &self,
        ns: &str,
        name: &str,
        owner_refs: Vec<serde_json::Value>,
    ) -> Result<Resource> {
        let current = self
            .db
            .get_resource("v1", "Pod", Some(ns), name)
            .await?
            .expect("Pod should exist");
        let mut pod: serde_json::Value = (*current.data).clone();
        pod["metadata"]["ownerReferences"] = serde_json::Value::Array(owner_refs);
        Ok(self
            .db
            .update_resource("v1", "Pod", Some(ns), name, pod, current.resource_version)
            .await?)
    }

    async fn merge_pod_labels(
        &self,
        ns: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource> {
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
        Ok(self
            .db
            .update_resource("v1", "Pod", Some(ns), name, pod, current.resource_version)
            .await?)
    }
}

struct SlowFirstCreateWriter {
    db: crate::datastore::sqlite::Datastore,
    creates: AtomicUsize,
}

#[async_trait::async_trait]
impl PodObjectWriter for SlowFirstCreateWriter {
    async fn create_controller_pod(
        &self,
        ns: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> Result<Resource> {
        if self.creates.fetch_add(1, Ordering::SeqCst) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        self.db
            .create_resource("v1", "Pod", Some(ns), name, pod)
            .await
    }

    async fn delete_pod(&self, ns: &str, name: &str) -> Result<()> {
        self.db.delete_resource("v1", "Pod", Some(ns), name).await
    }

    async fn update_pod_owner_references(
        &self,
        ns: &str,
        name: &str,
        owner_refs: Vec<serde_json::Value>,
    ) -> Result<Resource> {
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
    ) -> Result<Resource> {
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
async fn test_concurrent_replicaset_reconcile_creates_only_desired_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_reader = crate::controllers::test_utils::pod_repository_for_test(&db);
    let pod_writer = Arc::new(SlowFirstCreateWriter {
        db: db.clone(),
        creates: AtomicUsize::new(0),
    });

    let rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "race-rs",
            "namespace": "test-ns",
            "uid": "race-rs-uid"
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "race"}},
            "template": {
                "metadata": {"labels": {"app": "race"}},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }
        }
    });
    let created = db
        .create_resource("apps/v1", "ReplicaSet", Some("test-ns"), "race-rs", rs)
        .await
        .unwrap();
    let rs_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);

    let first = reconcile_replicaset(
        &db,
        pod_reader.as_ref(),
        pod_writer.as_ref(),
        pod_reader.as_ref(),
        &rs_with_rv,
        "test-node",
    );
    let second = reconcile_replicaset(
        &db,
        pod_reader.as_ref(),
        pod_writer.as_ref(),
        pod_reader.as_ref(),
        &rs_with_rv,
        "test-node",
    );
    let (first_result, second_result) = tokio::join!(first, second);
    first_result.unwrap();
    second_result.unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::new(Some("app=race"), None, None, None),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        1,
        "concurrent ReplicaSet reconciles must not over-create pods"
    );
}

#[tokio::test]
async fn test_replicaset_replaces_terminating_owned_pod() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs_uid = "rs-test-uid-terminating";
    let rs = db
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("test-ns"),
            "test-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "test-rs",
                    "namespace": "test-ns",
                    "uid": rs_uid
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "test"}},
                    "template": {
                        "metadata": {"labels": {"app": "test"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                    }
                }
            }),
        )
        .await
        .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "terminating-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "terminating-pod",
                "namespace": "test-ns",
                "labels": {"app": "test"},
                "deletionTimestamp": "2026-05-01T00:00:00Z",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "name": "test-rs",
                    "uid": rs_uid,
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {"containers": [{"name": "nginx", "image": "nginx"}]},
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    reconcile_replicaset(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &rs.data,
        "test-node",
    )
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
        2,
        "ReplicaSet must create a replacement while an owned pod is terminating"
    );
    assert!(
        pods.items.iter().any(|pod| pod.name != "terminating-pod"
            && pod.data.pointer("/metadata/deletionTimestamp").is_none()),
        "expected a new non-terminating replacement pod, got {:?}",
        pods.items
    );
}

#[tokio::test]
async fn test_replicaset_replaces_terminal_node_lost_owned_pod() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs_uid = "rs-test-uid-node-lost";
    let rs = db
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("test-ns"),
            "test-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "test-rs",
                    "namespace": "test-ns",
                    "uid": rs_uid
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "test"}},
                    "template": {
                        "metadata": {"labels": {"app": "test"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                    }
                }
            }),
        )
        .await
        .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("test-ns"),
        "node-lost-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "node-lost-pod",
                "namespace": "test-ns",
                "labels": {"app": "test"},
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "name": "test-rs",
                    "uid": rs_uid,
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {"containers": [{"name": "nginx", "image": "nginx"}]},
            "status": {
                "phase": "Failed",
                "reason": "NodeLost"
            }
        }),
    )
    .await
    .unwrap();

    reconcile_replicaset(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &rs.data,
        "test-node",
    )
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
        2,
        "ReplicaSet must create a replacement while an owned pod is terminal NodeLost"
    );
    assert!(
        pods.items.iter().any(|pod| pod.name != "node-lost-pod"
            && pod.data.pointer("/metadata/deletionTimestamp").is_none()),
        "expected a new active replacement pod, got {:?}",
        pods.items
    );
    let rs_after = db
        .get_resource("apps/v1", "ReplicaSet", Some("test-ns"), "test-rs")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rs_after.data["status"]["replicas"], json!(1));
}

#[tokio::test]
async fn test_replicaset_child_pods_are_scheduled_by_pod_create_pipeline() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

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
        "Node",
        None,
        "test-node",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "test-node"},
            "spec": {},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "allocatable": {
                    "cpu": "8",
                    "memory": "8Gi",
                    "pods": "110",
                    "example.com/fakecpu": "0"
                }
            }
        }),
    )
    .await
    .unwrap();

    let rs = db
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("test-ns"),
            "test-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "test-rs",
                    "namespace": "test-ns",
                    "uid": "rs-test-uid-scheduler"
                },
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "test"}},
                    "template": {
                        "metadata": {"labels": {"app": "test"}},
                        "spec": {
                            "containers": [{
                                "name": "nginx",
                                "image": "nginx",
                                "resources": {
                                    "requests": {"example.com/fakecpu": "1"}
                                }
                            }]
                        }
                    }
                }
            }),
        )
        .await
        .unwrap();

    reconcile_replicaset(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &rs.data,
        "test-node",
    )
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
    assert!(
        pods.items[0].data.pointer("/spec/nodeName").is_none(),
        "ReplicaSet child pods must not bypass scheduler resource fit by pre-setting nodeName: {:?}",
        pods.items[0].data
    );
    assert_eq!(
        pods.items[0]
            .data
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
                })
            })
            .and_then(|condition| condition.get("status"))
            .and_then(|v| v.as_str()),
        Some("False")
    );
}

#[tokio::test]
async fn test_replicaset_child_pods_participate_in_priority_preemption() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

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
        "Node",
        None,
        "test-node",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "test-node"},
            "spec": {"unschedulable": false},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
                "allocatable": {
                    "cpu": "8",
                    "memory": "32Gi",
                    "pods": "110",
                    "example.com/fakecpu": "1k"
                }
            }
        }),
    )
    .await
    .unwrap();

    for (name, value) in [("p1", 1), ("p2", 2), ("p3", 3), ("p4", 4)] {
        db.create_resource(
            "scheduling.k8s.io/v1",
            "PriorityClass",
            None,
            name,
            json!({
                "apiVersion": "scheduling.k8s.io/v1",
                "kind": "PriorityClass",
                "metadata": {"name": name},
                "value": value
            }),
        )
        .await
        .unwrap();
    }

    for (rs_name, uid, request, priority_class) in [
        ("rs-one", "rs-one-uid", "200", "p1"),
        ("rs-two", "rs-two-uid", "300", "p2"),
        ("rs-three", "rs-three-uid", "450", "p3"),
    ] {
        let rs = db
            .create_resource(
                "apps/v1",
                "ReplicaSet",
                Some("test-ns"),
                rs_name,
                json!({
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "metadata": {
                        "name": rs_name,
                        "namespace": "test-ns",
                        "uid": uid
                    },
                    "spec": {
                        "replicas": 1,
                        "selector": {"matchLabels": {"app": rs_name}},
                        "template": {
                            "metadata": {"labels": {"app": rs_name}},
                            "spec": {
                                "priorityClassName": priority_class,
                                "containers": [{
                                    "name": "c",
                                    "image": "registry.k8s.io/pause:3.10",
                                    "resources": {
                                        "requests": {"example.com/fakecpu": request}
                                    }
                                }]
                            }
                        }
                    }
                }),
            )
            .await
            .unwrap();

        reconcile_replicaset(
            &db,
            pod_repo.as_ref(),
            pod_repo.as_ref(),
            pod_repo.as_ref(),
            &rs.data,
            "test-node",
        )
        .await
        .unwrap();
    }

    pod_repo.schedule_all_unbound_pods().await.unwrap();

    pod_repo
        .api_create_pod(crate::kubelet::pod_repository::PodApiCreateRequest {
            namespace: "test-ns".to_string(),
            name: "pod4".to_string(),
            body: json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "pod4", "namespace": "test-ns"},
                "spec": {
                    "priorityClassName": "p4",
                    "containers": [{
                        "name": "c",
                        "image": "registry.k8s.io/pause:3.10",
                        "resources": {
                            "requests": {"example.com/fakecpu": "500"}
                        }
                    }]
                }
            }),
            dry_run: false,
            run_admission: true,
        })
        .await
        .unwrap();

    pod_repo.schedule_all_unbound_pods().await.unwrap();
    let scheduled = db
        .get_resource("v1", "Pod", Some("test-ns"), "pod4")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        scheduled
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str()),
        Some("test-node")
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
    let active_rs_pods: Vec<_> = pods
        .items
        .iter()
        .filter(|pod| {
            pod.name != "pod4" && pod.data.pointer("/metadata/deletionTimestamp").is_none()
        })
        .collect();
    assert_eq!(
        active_rs_pods.len(),
        1,
        "high-priority pod must preempt enough lower-priority ReplicaSet children, got {:?}",
        pods.items
    );
    assert_eq!(
        active_rs_pods[0]
            .data
            .pointer("/spec/priorityClassName")
            .and_then(|v| v.as_str()),
        Some("p3")
    );
}

#[tokio::test]
async fn test_replicaset_scale_up_creates_missing_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

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

    // Create ReplicaSet with 3 replicas
    let rs_uid = "rs-test-uid-001";
    let replicaset = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "test-rs",
            "namespace": "test-ns",
            "uid": rs_uid
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
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("test-ns"),
            "test-rs",
            replicaset.clone(),
        )
        .await
        .unwrap();

    // Inject resourceVersion before reconcile
    let mut rs_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(obj) = rs_with_rv.as_object_mut()
        && let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // Reconcile (should create 3 pods)
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
            Some(rs_uid)
        );
    }
}

#[tokio::test]
async fn test_replicaset_create_loop_observes_live_scale_down() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);
    let writer = Arc::new(ScaleDownDuringCreateWriter {
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

    let rs_uid = "rs-test-uid-live-scale-down";
    let replicaset = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "test-rs",
            "namespace": "test-ns",
            "uid": rs_uid
        },
        "spec": {
            "replicas": 7,
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
            "ReplicaSet",
            Some("test-ns"),
            "test-rs",
            replicaset,
        )
        .await
        .unwrap();
    let rs_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);

    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        writer.as_ref(),
        __pod_repo.as_ref(),
        &rs_with_rv,
        "test-node",
    )
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
        5,
        "ReplicaSet reconcile must stop creating pods after live spec.replicas is lowered"
    );

    let current_rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("test-ns"), "test-rs")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current_rs.data["status"]["replicas"], 5);
}

#[tokio::test]
async fn test_replicaset_scale_down_deletes_excess_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

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

    let rs_uid = "rs-test-uid-002";

    // Create ReplicaSet with 5 replicas initially
    let replicaset_5 = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {
            "name": "test-rs",
            "namespace": "test-ns",
            "uid": rs_uid
        },
        "spec": {
            "replicas": 5,
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
            "ReplicaSet",
            Some("test-ns"),
            "test-rs",
            replicaset_5.clone(),
        )
        .await
        .unwrap();

    // Inject resourceVersion before reconcile
    let mut rs_with_rv: serde_json::Value = (*created.data).clone();
    if let Some(obj) = rs_with_rv.as_object_mut()
        && let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    // Reconcile to create 5 pods
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

    // Get the current ReplicaSet state after first reconcile
    let current_rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("test-ns"), "test-rs")
        .await
        .unwrap()
        .unwrap();

    // Now scale down to 2 replicas - update via DB
    let updated = db
        .update_resource(
            "apps/v1",
            "ReplicaSet",
            Some("test-ns"),
            "test-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {
                    "name": "test-rs",
                    "namespace": "test-ns",
                    "uid": rs_uid
                },
                "spec": {
                    "replicas": 2,
                    "selector": {"matchLabels": {"app": "test"}},
                    "template": {
                        "metadata": {"labels": {"app": "test"}},
                        "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
                    }
                }
            }),
            current_rs.resource_version,
        )
        .await
        .unwrap();

    // Inject resourceVersion before reconcile
    let mut rs_with_rv_2: serde_json::Value = (*updated.data).clone();
    if let Some(obj) = rs_with_rv_2.as_object_mut()
        && let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(updated.resource_version.to_string()),
        );
    }

    // Reconcile with updated replica count
    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &rs_with_rv_2,
        "test-node",
    )
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
    let active_after: Vec<_> = pods_after
        .items
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_none())
        .collect();

    assert_eq!(
        active_after.len(),
        2,
        "Expected 2 active pods after scale down"
    );

    // Verify remaining pods still have correct owner reference
    for pod in active_after {
        let owner_refs = pod
            .data
            .get("metadata")
            .and_then(|m| m.get("ownerReferences"))
            .and_then(|o| o.as_array())
            .unwrap();
        assert_eq!(
            owner_refs[0].get("uid").and_then(|u| u.as_str()),
            Some(rs_uid)
        );
    }
}

#[tokio::test]
async fn test_replicaset_zero_replicas_creates_no_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs_uid = "rs-test-uid-zero";
    let replicaset = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "zero-rs", "namespace": "test-ns", "uid": rs_uid},
        "spec": {
            "replicas": 0,
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
            "ReplicaSet",
            Some("test-ns"),
            "zero-rs",
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
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 0, "Zero replicas should create zero pods");
}

#[tokio::test]
async fn test_replicaset_pods_have_correct_labels_from_template() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs_uid = "rs-test-uid-labels";
    let replicaset = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "labeled-rs", "namespace": "test-ns", "uid": rs_uid},
        "spec": {
            "replicas": 2,
            "selector": {"matchLabels": {"app": "web", "tier": "frontend"}},
            "template": {
                "metadata": {"labels": {"app": "web", "tier": "frontend"}},
                "spec": {"containers": [{"name": "nginx", "image": "nginx"}]}
            }
        }
    });

    let created = db
        .create_resource(
            "apps/v1",
            "ReplicaSet",
            Some("test-ns"),
            "labeled-rs",
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
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 2);

    for pod in &pods.items {
        let labels = pod
            .data
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(|l| l.as_object())
            .expect("Pod must have labels");
        assert_eq!(
            labels.get("app").and_then(|v| v.as_str()),
            Some("web"),
            "Pod label 'app' must match template"
        );
        assert_eq!(
            labels.get("tier").and_then(|v| v.as_str()),
            Some("frontend"),
            "Pod label 'tier' must match template"
        );
    }
}

#[tokio::test]
async fn test_replicaset_idempotent_reconcile_no_extra_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs_uid = "rs-test-uid-idem";
    let replicaset = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "idem-rs", "namespace": "test-ns", "uid": rs_uid},
        "spec": {
            "replicas": 2,
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
            "ReplicaSet",
            Some("test-ns"),
            "idem-rs",
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

    // First reconcile
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

    // Re-fetch RS (status was updated) and reconcile again
    let current_rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("test-ns"), "idem-rs")
        .await
        .unwrap()
        .unwrap();

    let mut rs_with_rv2: serde_json::Value = (*current_rs.data).clone();
    if let Some(meta) = rs_with_rv2
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(current_rs.resource_version.to_string()),
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
async fn test_replicaset_status_updated_after_reconcile() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs_uid = "rs-test-uid-status";
    let replicaset = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "status-rs", "namespace": "test-ns", "uid": rs_uid},
        "spec": {
            "replicas": 4,
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
            "ReplicaSet",
            Some("test-ns"),
            "status-rs",
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

    // Verify RS status was updated
    let updated_rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("test-ns"), "status-rs")
        .await
        .unwrap()
        .unwrap();

    let status = &updated_rs.data["status"];
    assert_eq!(status["replicas"], 4, "Status.replicas must match spec");
    assert_eq!(
        status["observedGeneration"], 1,
        "Status.observedGeneration must reflect metadata.generation"
    );
}

#[tokio::test]
async fn test_replicaset_pods_have_owner_reference_with_controller_true() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs_uid = "rs-test-uid-owner";
    let replicaset = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "owner-rs", "namespace": "test-ns", "uid": rs_uid},
        "spec": {
            "replicas": 1,
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
            "ReplicaSet",
            Some("test-ns"),
            "owner-rs",
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
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(pods.items.len(), 1);

    let owner_refs = pods.items[0].data["metadata"]["ownerReferences"]
        .as_array()
        .unwrap();
    assert_eq!(owner_refs.len(), 1);
    assert_eq!(owner_refs[0]["uid"], rs_uid);
    assert_eq!(owner_refs[0]["kind"], "ReplicaSet");
    assert_eq!(owner_refs[0]["name"], "owner-rs");
    assert_eq!(owner_refs[0]["controller"], true);
    assert_eq!(owner_refs[0]["blockOwnerDeletion"], true);
    assert_eq!(owner_refs[0]["apiVersion"], "apps/v1");
}

#[tokio::test]
async fn test_replicaset_adopts_orphan_matching_pod() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

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
        "Pod",
        Some("test-ns"),
        "orphan-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "orphan-pod",
                "namespace": "test-ns",
                "labels": {"app": "adopt"}
            },
            "spec": {"containers": [{"name": "c", "image": "nginx"}]},
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    let rs_uid = "rs-adopt-uid";
    let rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "adopt-rs", "namespace": "test-ns", "uid": rs_uid},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "adopt"}},
            "template": {
                "metadata": {"labels": {"app": "adopt"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    });
    let created = db
        .create_resource("apps/v1", "ReplicaSet", Some("test-ns"), "adopt-rs", rs)
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
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        1,
        "must adopt existing orphan, not create extra"
    );

    let orphan = &pods.items[0].data;
    let owner_refs = orphan
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .expect("adopted pod must have ownerReferences");
    assert!(owner_refs.iter().any(|o| {
        o.get("kind").and_then(|v| v.as_str()) == Some("ReplicaSet")
            && o.get("uid").and_then(|v| v.as_str()) == Some(rs_uid)
    }));
}

#[tokio::test]
async fn test_replicaset_releases_pod_when_selector_changes() {
    let db = crate::datastore::test_support::in_memory().await;
    let __pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "test-ns",
        json!({"metadata": {"name": "test-ns"}}),
    )
    .await
    .unwrap();

    let rs_uid = "rs-release-uid";
    let rs = json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "metadata": {"name": "release-rs", "namespace": "test-ns", "uid": rs_uid},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "a"}},
            "template": {
                "metadata": {"labels": {"app": "a"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    });
    let created = db
        .create_resource("apps/v1", "ReplicaSet", Some("test-ns"), "release-rs", rs)
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

    let pods_before = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let old_pod_name = pods_before.items[0].name.clone();

    let current_rs = db
        .get_resource("apps/v1", "ReplicaSet", Some("test-ns"), "release-rs")
        .await
        .unwrap()
        .unwrap();

    let updated = db
        .update_resource(
            "apps/v1",
            "ReplicaSet",
            Some("test-ns"),
            "release-rs",
            json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "metadata": {"name": "release-rs", "namespace": "test-ns", "uid": rs_uid},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"app": "b"}},
                    "template": {
                        "metadata": {"labels": {"app": "b"}},
                        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
                    }
                }
            }),
            current_rs.resource_version,
        )
        .await
        .unwrap();
    let mut updated_with_rv: serde_json::Value = (*updated.data).clone();
    if let Some(meta) = updated_with_rv
        .get_mut("metadata")
        .and_then(|m| m.as_object_mut())
    {
        meta.insert(
            "resourceVersion".to_string(),
            json!(updated.resource_version.to_string()),
        );
    }

    reconcile_replicaset(
        &db,
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        __pod_repo.as_ref(),
        &updated_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let old_pod = db
        .get_resource("v1", "Pod", Some("test-ns"), &old_pod_name)
        .await
        .unwrap()
        .unwrap();
    let owner_refs = old_pod
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        owner_refs.iter().all(|o| {
            !(o.get("kind").and_then(|v| v.as_str()) == Some("ReplicaSet")
                && o.get("uid").and_then(|v| v.as_str()) == Some(rs_uid))
        }),
        "old pod must be released when it no longer matches selector"
    );
}
