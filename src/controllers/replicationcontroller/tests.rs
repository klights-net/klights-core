use super::*;

use crate::datastore::Resource;
use crate::kubelet::pod_repository::PodObjectWriter;
use anyhow::Result;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Barrier, Notify};

struct SlowFirstCreateWriter {
    db: crate::datastore::sqlite::Datastore,
    creates: AtomicUsize,
}

struct ScaleDownDuringRcCreateWriter {
    db: crate::datastore::sqlite::Datastore,
    creates: AtomicUsize,
}

struct BlockingSecondCreateWriter {
    db: crate::datastore::sqlite::Datastore,
    creates: AtomicUsize,
    first_create_persisted: Notify,
    second_create_started: Notify,
    release_second_create: Barrier,
}

struct BlockingFirstCreateWriter {
    db: crate::datastore::sqlite::Datastore,
    creates: AtomicUsize,
    first_create_started: Notify,
    second_create_started: Notify,
    release_first_create: Barrier,
}

impl BlockingSecondCreateWriter {
    fn new(db: crate::datastore::sqlite::Datastore) -> Self {
        Self {
            db,
            creates: AtomicUsize::new(0),
            first_create_persisted: Notify::new(),
            second_create_started: Notify::new(),
            release_second_create: Barrier::new(2),
        }
    }
}

impl BlockingFirstCreateWriter {
    fn new(db: crate::datastore::sqlite::Datastore) -> Self {
        Self {
            db,
            creates: AtomicUsize::new(0),
            first_create_started: Notify::new(),
            second_create_started: Notify::new(),
            release_first_create: Barrier::new(2),
        }
    }
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

#[async_trait::async_trait]
impl PodObjectWriter for ScaleDownDuringRcCreateWriter {
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
            let current_rc = self
                .db
                .get_resource("v1", "ReplicationController", Some(ns), "scale-down-rc")
                .await?
                .expect("ReplicationController should exist");
            let mut scaled_rc: serde_json::Value = (*current_rc.data).clone();
            scaled_rc["spec"]["replicas"] = json!(5);
            self.db
                .update_resource(
                    "v1",
                    "ReplicationController",
                    Some(ns),
                    "scale-down-rc",
                    scaled_rc,
                    current_rc.resource_version,
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

#[async_trait::async_trait]
impl PodObjectWriter for BlockingSecondCreateWriter {
    async fn create_controller_pod(
        &self,
        ns: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> Result<Resource> {
        let create_index = self.creates.fetch_add(1, Ordering::SeqCst);
        if create_index == 1 {
            self.second_create_started.notify_one();
            self.release_second_create.wait().await;
        }

        let created = self
            .db
            .create_resource("v1", "Pod", Some(ns), name, pod)
            .await?;
        if create_index == 0 {
            self.first_create_persisted.notify_one();
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

#[async_trait::async_trait]
impl PodObjectWriter for BlockingFirstCreateWriter {
    async fn create_controller_pod(
        &self,
        ns: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> Result<Resource> {
        let create_index = self.creates.fetch_add(1, Ordering::SeqCst);
        if create_index == 0 {
            self.first_create_started.notify_one();
            self.release_first_create.wait().await;
        } else if create_index == 1 {
            self.second_create_started.notify_one();
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

/// Test-only shim wrapping `reconcile_replicationcontroller` with the
/// repository-backed argument list, mirroring the pre-Task-18 signature.
async fn reconcile_rc_test(
    db: &crate::datastore::sqlite::Datastore,
    rc: &Value,
    node_name: &str,
) -> Result<()> {
    let repo = crate::controllers::test_utils::pod_repository_for_test(db);
    super::reconcile_replicationcontroller(
        db,
        repo.as_ref(),
        repo.as_ref(),
        repo.as_ref(),
        rc,
        node_name,
    )
    .await
}

#[tokio::test]
async fn test_replicationcontroller_status_counts_available_ready_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {
            "name": "ready-rc",
            "namespace": "default",
            "uid": "ready-rc-uid",
            "generation": 1
        },
        "spec": {
            "replicas": 1,
            "selector": {"app": "ready-rc"},
            "template": {
                "metadata": {"labels": {"app": "ready-rc"}},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }
        }
    });
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "ready-rc",
        rc.clone(),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "ready-rc-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "ready-rc-pod",
                "namespace": "default",
                "labels": {"app": "ready-rc"},
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "name": "ready-rc",
                    "uid": "ready-rc-uid",
                    "controller": true
                }]
            },
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        }),
    )
    .await
    .unwrap();

    reconcile_rc_test(&db, &rc, "test-node").await.unwrap();

    let updated = db
        .get_resource("v1", "ReplicationController", Some("default"), "ready-rc")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated.data["status"]["readyReplicas"], json!(1));
    assert_eq!(
        updated.data["status"]["availableReplicas"],
        json!(1),
        "availableReplicas must reflect ready RC pods so e2e does not wait forever after pods become Ready"
    );
}

#[tokio::test]
async fn test_concurrent_replicationcontroller_reconcile_creates_only_desired_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_reader = crate::controllers::test_utils::pod_repository_for_test(&db);
    let pod_writer = Arc::new(SlowFirstCreateWriter {
        db: db.clone(),
        creates: AtomicUsize::new(0),
    });

    let rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {
            "name": "race-rc",
            "namespace": "test-ns",
            "uid": "race-rc-uid"
        },
        "spec": {
            "replicas": 1,
            "selector": {"app": "race"},
            "template": {
                "metadata": {"labels": {"app": "race"}},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }
        }
    });
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("test-ns"),
        "race-rc",
        rc.clone(),
    )
    .await
    .unwrap();

    let first = reconcile_replicationcontroller(
        &db,
        pod_reader.as_ref(),
        pod_writer.as_ref(),
        pod_reader.as_ref(),
        &rc,
        "test-node",
    );
    let second = reconcile_replicationcontroller(
        &db,
        pod_reader.as_ref(),
        pod_writer.as_ref(),
        pod_reader.as_ref(),
        &rc,
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
        "concurrent ReplicationController reconciles must not over-create pods"
    );
}

#[tokio::test]
async fn test_replicationcontroller_skips_reconcile_when_deletion_timestamp_set() {
    let db = crate::datastore::test_support::in_memory().await;
    let rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {
            "name": "terminating-rc",
            "namespace": "default",
            "uid": "terminating-rc-uid",
            "deletionTimestamp": "2026-05-17T00:00:00Z",
            "finalizers": ["foregroundDeletion"]
        },
        "spec": {
            "replicas": 1,
            "selector": {"app": "terminating-rc"},
            "template": {
                "metadata": {"labels": {"app": "terminating-rc"}},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }
        }
    });
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "terminating-rc",
        rc.clone(),
    )
    .await
    .unwrap();

    reconcile_rc_test(&db, &rc, "test-node").await.unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=terminating-rc"), None, None, None),
        )
        .await
        .unwrap();
    assert!(
        pods.items.is_empty(),
        "terminating ReplicationController must not create replacement Pods"
    );
}

#[tokio::test]
async fn test_replicationcontroller_stale_snapshot_after_delete_does_not_recreate_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_repo = crate::controllers::test_utils::pod_repository_for_test(&db);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {
            "name": "stale-rc",
            "namespace": "default",
            "uid": "stale-rc-uid"
        },
        "spec": {
            "replicas": 1,
            "selector": {"app": "stale-rc"},
            "template": {
                "metadata": {"labels": {"app": "stale-rc"}},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }
        }
    });
    let created = db
        .create_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "stale-rc",
            rc,
        )
        .await
        .unwrap();
    let stale_snapshot = created.data.clone();

    db.delete_resource("v1", "ReplicationController", Some("default"), "stale-rc")
        .await
        .unwrap();

    reconcile_replicationcontroller(
        &db,
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        pod_repo.as_ref(),
        &stale_snapshot,
        "test-node",
    )
    .await
    .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert!(
        pods.items.is_empty(),
        "stale ReplicationController reconcile after delete must not recreate Pods"
    );
}

#[tokio::test]
async fn test_replicationcontroller_create_loop_observes_live_scale_down() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_reader = crate::controllers::test_utils::pod_repository_for_test(&db);
    let pod_writer = Arc::new(ScaleDownDuringRcCreateWriter {
        db: db.clone(),
        creates: AtomicUsize::new(0),
    });

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {
            "name": "scale-down-rc",
            "namespace": "default",
            "uid": "scale-down-rc-uid"
        },
        "spec": {
            "replicas": 7,
            "selector": {"app": "scale-down-rc"},
            "template": {
                "metadata": {"labels": {"app": "scale-down-rc"}},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }
        }
    });
    let created = db
        .create_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "scale-down-rc",
            rc,
        )
        .await
        .unwrap();
    let rc_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);

    reconcile_replicationcontroller(
        &db,
        pod_reader.as_ref(),
        pod_writer.as_ref(),
        pod_reader.as_ref(),
        &rc_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(Some("app=scale-down-rc"), None, None, None),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        5,
        "ReplicationController reconcile must stop creating Pods after live spec.replicas is lowered"
    );

    let current_rc = db
        .get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "scale-down-rc",
        )
        .await
        .unwrap()
        .expect("ReplicationController must still exist");
    assert_eq!(current_rc.data.pointer("/status/replicas"), Some(&json!(5)));
}

#[tokio::test]
async fn test_rc_status_replicas_reflects_newly_created_pods() {
    // Regression test: RC controller was updating status with pre-creation owned_pods
    // (always empty on first reconcile), so status.replicas stayed 0 forever.
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let rc_spec = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "test-rc", "namespace": "default", "uid": "rc-uid-test"},
        "spec": {
            "replicas": 3,
            "selector": {"app": "test"},
            "template": {
                "metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }
        }
    });

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "test-rc",
        rc_spec.clone(),
    )
    .await
    .unwrap();

    reconcile_rc_test(&db, &rc_spec, "node1").await.unwrap();

    // Status must reflect 3 newly created pods, NOT 0 (pre-creation count)
    let rc = db
        .get_resource("v1", "ReplicationController", Some("default"), "test-rc")
        .await
        .unwrap()
        .expect("RC must still exist");

    let replicas = rc.data["status"]["replicas"]
        .as_u64()
        .expect("status.replicas must be a number");
    assert_eq!(
        replicas, 3,
        "status.replicas must equal newly created pod count (3), not pre-creation count (0)"
    );
}

#[tokio::test]
async fn test_rc_status_advances_while_large_scale_up_is_still_creating_pods() {
    // Conformance waits only two minutes for a 100-replica RC to report the
    // desired status. Status must advance as Pods are created instead of
    // staying at zero until the whole create loop finishes.
    let db = crate::datastore::test_support::in_memory().await;
    let pod_reader = crate::controllers::test_utils::pod_repository_for_test(&db);
    let pod_writer = Arc::new(BlockingSecondCreateWriter::new(db.clone()));

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "slow-rc", "namespace": "default", "uid": "slow-rc-uid"},
        "spec": {
            "replicas": 3,
            "selector": {"app": "slow"},
            "template": {
                "metadata": {"labels": {"app": "slow"}},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }
        }
    });

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "slow-rc",
        rc.clone(),
    )
    .await
    .unwrap();

    let db_for_task = db.clone();
    let reader_for_task = pod_reader.clone();
    let writer_for_task = pod_writer.clone();
    let rc_for_task = rc.clone();
    let reconcile = tokio::spawn(async move {
        reconcile_replicationcontroller(
            &db_for_task,
            reader_for_task.as_ref(),
            writer_for_task.as_ref(),
            reader_for_task.as_ref(),
            &rc_for_task,
            "test-node",
        )
        .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        pod_writer.first_create_persisted.notified(),
    )
    .await
    .expect("first child Pod must be created");
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        pod_writer.second_create_started.notified(),
    )
    .await
    .expect("reconcile must block on the second create");

    let observed_replicas = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if let Some(replicas) = db
                .get_resource("v1", "ReplicationController", Some("default"), "slow-rc")
                .await
                .unwrap()
                .expect("RC must exist")
                .data
                .pointer("/status/replicas")
                .and_then(|value| value.as_u64())
            {
                return replicas;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("RC status.replicas must advance before the full scale-up loop completes");

    pod_writer.release_second_create.wait().await;
    tokio::time::timeout(std::time::Duration::from_secs(1), reconcile)
        .await
        .expect("reconcile should finish after releasing second create")
        .expect("reconcile task should not panic")
        .expect("reconcile should succeed");

    assert_eq!(
        observed_replicas, 1,
        "RC status.replicas must reflect successfully created Pods before the full scale-up loop completes"
    );
}

#[tokio::test]
async fn test_rc_large_scale_up_starts_next_create_while_prior_create_is_in_flight() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_reader = crate::controllers::test_utils::pod_repository_for_test(&db);
    let pod_writer = Arc::new(BlockingFirstCreateWriter::new(db.clone()));

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "parallel-rc", "namespace": "default", "uid": "parallel-rc-uid"},
        "spec": {
            "replicas": 2,
            "selector": {"app": "parallel"},
            "template": {
                "metadata": {"labels": {"app": "parallel"}},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }
        }
    });

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "parallel-rc",
        rc.clone(),
    )
    .await
    .unwrap();

    let db_for_task = db.clone();
    let reader_for_task = pod_reader.clone();
    let writer_for_task = pod_writer.clone();
    let rc_for_task = rc.clone();
    let reconcile = tokio::spawn(async move {
        reconcile_replicationcontroller(
            &db_for_task,
            reader_for_task.as_ref(),
            writer_for_task.as_ref(),
            reader_for_task.as_ref(),
            &rc_for_task,
            "test-node",
        )
        .await
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        pod_writer.first_create_started.notified(),
    )
    .await
    .expect("first child Pod create must start");
    let second_started_before_release = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        pod_writer.second_create_started.notified(),
    )
    .await;

    pod_writer.release_first_create.wait().await;
    tokio::time::timeout(std::time::Duration::from_secs(1), reconcile)
        .await
        .expect("reconcile should finish after releasing first create")
        .expect("reconcile task should not panic")
        .expect("reconcile should succeed");

    assert!(
        second_started_before_release.is_ok(),
        "ReplicationController scale-up must start more than one child create at a time so conformance-scale RCs complete under delayed control-plane writes"
    );
}

#[tokio::test]
async fn test_rc_large_scale_up_does_not_write_status_after_every_child_create() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "large-rc", "namespace": "default", "uid": "large-rc-uid"},
        "spec": {
            "replicas": 12,
            "selector": {"app": "large"},
            "template": {
                "metadata": {"labels": {"app": "large"}},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }
        }
    });

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "large-rc",
        rc.clone(),
    )
    .await
    .unwrap();

    reconcile_rc_test(&db, &rc, "node1").await.unwrap();

    let rc_events = db
        .list_watch_events_since(
            &[crate::datastore::WatchTarget::namespaced_in_namespace(
                "v1",
                "ReplicationController",
                "default",
            )],
            0,
        )
        .await
        .unwrap();
    let modified_replicas = rc_events
        .iter()
        .filter(|event| event.event_type.as_ref() == "MODIFIED")
        .filter(|event| event.resource.name == "large-rc")
        .map(|event| {
            event
                .resource
                .data
                .pointer("/status/replicas")
                .and_then(|value| value.as_u64())
        })
        .collect::<Vec<_>>();

    assert!(
        modified_replicas.len() <= 4,
        "large RC scale-up must not write RC status after every child Pod create; wrote {} updates with replicas {:?}",
        modified_replicas.len(),
        modified_replicas
    );

    let updated = db
        .get_resource("v1", "ReplicationController", Some("default"), "large-rc")
        .await
        .unwrap()
        .expect("RC must still exist");
    assert_eq!(updated.data.pointer("/status/replicas"), Some(&json!(12)));
}

#[tokio::test]
async fn test_rc_ignores_ownerref_pods_that_do_not_match_selector() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "stay", "namespace": "default", "uid": "rc-stay-uid"},
        "spec": {
            "replicas": 0,
            "selector": {"app": "stay"},
            "template": {
                "metadata": {"labels": {"app": "stay"}},
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }
        }
    });

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "stay",
        rc.clone(),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "foreign-label-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "foreign-label-pod",
                "namespace": "default",
                "uid": "pod-uid",
                "labels": {"app": "delete"},
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "name": "stay",
                    "uid": "rc-stay-uid"
                }]
            },
            "status": {"phase": "Running"},
            "spec": {"containers": [{"name": "app", "image": "nginx"}]}
        }),
    )
    .await
    .unwrap();

    reconcile_rc_test(&db, &rc, "test-node").await.unwrap();

    let pod = db
        .get_resource("v1", "Pod", Some("default"), "foreign-label-pod")
        .await
        .unwrap();
    assert!(
        pod.is_some(),
        "RC controller must not scale down an ownerRef pod outside its selector"
    );
    let owner_refs = pod
        .unwrap()
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        owner_refs.len(),
        1,
        "non-controller ownerRef must be preserved by RC reconcile"
    );
    assert_eq!(owner_refs[0]["uid"], "rc-stay-uid");
}

#[tokio::test]
async fn test_rc_create_pod_has_apiversion_kind_status_and_labels() {
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

    let rc_uid = "rc-test-uid-fields";
    let rc_spec = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "test-rc", "namespace": "test-ns", "uid": rc_uid},
        "spec": {
            "replicas": 1,
            "selector": {"app": "myapp"},
            "template": {
                "metadata": {"labels": {"app": "myapp", "env": "test"}},
                "spec": {"containers": [{"name": "myapp", "image": "nginx"}]}
            }
        }
    });

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("test-ns"),
        "test-rc",
        rc_spec.clone(),
    )
    .await
    .unwrap();

    reconcile_rc_test(&db, &rc_spec, "test-node").await.unwrap();

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("test-ns"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    assert_eq!(pods.items.len(), 1, "Should create exactly 1 pod");
    let pod = &pods.items[0];

    // apiVersion must be "v1"
    assert_eq!(
        pod.data["apiVersion"].as_str(),
        Some("v1"),
        "Pod must have apiVersion: v1"
    );

    // kind must be "Pod"
    assert_eq!(
        pod.data["kind"].as_str(),
        Some("Pod"),
        "Pod must have kind: Pod"
    );

    // status.phase must be "Pending"
    assert_eq!(
        pod.data["status"]["phase"].as_str(),
        Some("Pending"),
        "Pod must have status.phase: Pending"
    );
    assert!(
        pod.data.pointer("/spec/nodeName").is_none(),
        "ReplicationController child Pods must not bypass scheduler resource fit by pre-setting nodeName: {:?}",
        pod.data
    );

    // Labels from template must be propagated
    let labels = pod.data["metadata"]["labels"]
        .as_object()
        .expect("Pod must have metadata.labels");
    assert_eq!(
        labels.get("app").and_then(|v| v.as_str()),
        Some("myapp"),
        "Pod label 'app' must match template"
    );
    assert_eq!(
        labels.get("env").and_then(|v| v.as_str()),
        Some("test"),
        "Pod label 'env' must match template"
    );
}

#[tokio::test]
async fn test_rc_releases_pod_when_selector_changes() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let rc_uid = "rc-release-uid";
    let rc_initial = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "rc-release", "namespace": "default", "uid": rc_uid},
        "spec": {
            "replicas": 1,
            "selector": {"app": "a"},
            "template": {
                "metadata": {"labels": {"app": "a"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    });
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "rc-release",
        rc_initial.clone(),
    )
    .await
    .unwrap();

    reconcile_rc_test(&db, &rc_initial, "test-node")
        .await
        .unwrap();

    let pods_before = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let old_pod_name = pods_before.items[0].name.clone();

    let current_rc = db
        .get_resource("v1", "ReplicationController", Some("default"), "rc-release")
        .await
        .unwrap()
        .unwrap();
    let rc_updated = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "rc-release", "namespace": "default", "uid": rc_uid},
        "spec": {
            "replicas": 1,
            "selector": {"app": "b"},
            "template": {
                "metadata": {"labels": {"app": "b"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    });
    db.update_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "rc-release",
        rc_updated.clone(),
        current_rc.resource_version,
    )
    .await
    .unwrap();

    reconcile_rc_test(&db, &rc_updated, "test-node")
        .await
        .unwrap();

    let old_pod = db
        .get_resource("v1", "Pod", Some("default"), &old_pod_name)
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
        owner_refs
            .iter()
            .all(|o| { !(o["kind"] == "ReplicationController" && o["uid"] == rc_uid) }),
        "RC must release old pod when selector changes and pod no longer matches"
    );
}

#[tokio::test]
async fn test_rc_does_not_adopt_pod_with_foreign_controller_owner() {
    let db = crate::datastore::test_support::in_memory().await;

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "rc1", "namespace": "default", "uid": "rc1-uid"},
        "spec": {
            "replicas": 0,
            "selector": {"app": "shared"},
            "template": {
                "metadata": {"labels": {"app": "shared"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}
            }
        }
    });

    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "rc1",
        rc.clone(),
    )
    .await
    .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "shared-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "shared-pod",
                "namespace": "default",
                "uid": "shared-pod-uid",
                "labels": {"app": "shared"},
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "name": "rc2",
                    "uid": "rc2-uid",
                    "controller": true
                }]
            },
            "spec": {"containers": [{"name": "c", "image": "nginx"}]},
            "status": {"phase": "Running"}
        }),
    )
    .await
    .unwrap();

    reconcile_rc_test(&db, &rc, "test-node").await.unwrap();

    let pod = db
        .get_resource("v1", "Pod", Some("default"), "shared-pod")
        .await
        .unwrap()
        .expect("pod must exist");

    let owner_refs = pod
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    assert_eq!(
        owner_refs.len(),
        1,
        "RC must not adopt pod already owned by another controller"
    );
    assert_eq!(owner_refs[0]["uid"], "rc2-uid");
}

// --- F4-02: Flat selector tests ---

#[test]
fn rc_flat_selector_matches_only_labeled_pods() {
    let selector = json!({"app": "test", "tier": "fe"});
    let parsed = crate::label_selector::LabelSelector::from_flat_match_labels(&selector)
        .expect("flat selector parse should succeed");

    let matching_pod = json!({"metadata": {"labels": {"app": "test", "tier": "fe"}}});
    assert!(
        parsed.matches_resource(&matching_pod),
        "pod with all selector labels must match"
    );

    let partial_pod = json!({"metadata": {"labels": {"app": "test"}}});
    assert!(
        !parsed.matches_resource(&partial_pod),
        "pod missing a selector label must not match"
    );

    let wrong_val_pod = json!({"metadata": {"labels": {"app": "other", "tier": "fe"}}});
    assert!(
        !parsed.matches_resource(&wrong_val_pod),
        "pod with wrong label value must not match"
    );

    let no_labels_pod = json!({"metadata": {}});
    assert!(
        !parsed.matches_resource(&no_labels_pod),
        "pod with no labels must not match"
    );
}

#[test]
fn rc_empty_selector_matches_no_pods() {
    let selector = json!({});
    let parsed = crate::label_selector::LabelSelector::from_flat_match_labels(&selector)
        .expect("empty selector should parse");
    assert!(
        parsed.requirements().is_empty(),
        "empty flat selector should have zero requirements"
    );

    // RC adoption: empty selector matches nothing for safety.
    // pod_matches_selector returns false when requirements are empty.
    let pod_with_labels = json!({"metadata": {"labels": {"app": "x"}}});
    assert!(
        !pod_matches_selector(&pod_with_labels, &parsed),
        "empty RC selector must not match any pod"
    );
}

#[test]
fn rc_selector_with_non_string_value_returns_error() {
    let selector = json!({"app": 123});
    let result = crate::label_selector::LabelSelector::from_flat_match_labels(&selector);
    assert!(
        result.is_err(),
        "non-string selector value must be rejected"
    );
}
