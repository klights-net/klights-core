use super::*;

use crate::datastore::Resource;
use crate::kubelet::pod_repository::PodObjectWriter;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Test-only shim wrapping `reconcile_job` with the repository-backed
/// argument list, mirroring the pre-Task-18 signature.
async fn reconcile_job_test(
    db: &crate::datastore::sqlite::Datastore,
    job: &Value,
    node_name: &str,
) -> Result<Value> {
    let repo = crate::controllers::test_utils::pod_repository_for_test(db);
    super::reconcile_job(
        db,
        repo.as_ref(),
        repo.as_ref(),
        repo.as_ref(),
        job,
        node_name,
    )
    .await
}

/// Helper to fetch latest Job from DB with resourceVersion injected
async fn get_job(db: &dyn DatastoreBackend, namespace: &str, name: &str) -> Value {
    let resource = db
        .get_resource("batch/v1", "Job", Some(namespace), name)
        .await
        .unwrap()
        .unwrap();

    let mut job: Value = std::sync::Arc::unwrap_or_clone(resource.data);
    if let Some(meta) = job.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        meta.insert(
            "resourceVersion".to_string(),
            json!(resource.resource_version.to_string()),
        );
    }
    job
}

struct ScaleDownDuringJobCreateWriter {
    db: crate::datastore::sqlite::Datastore,
    creates: AtomicUsize,
}

#[async_trait::async_trait]
impl PodObjectWriter for ScaleDownDuringJobCreateWriter {
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

        if count == 1 {
            let current_job = self
                .db
                .get_resource("batch/v1", "Job", Some(ns), "scale-down-job")
                .await?
                .expect("Job should exist");
            let mut scaled_job: serde_json::Value = (*current_job.data).clone();
            scaled_job["spec"]["parallelism"] = json!(1);
            self.db
                .update_resource(
                    "batch/v1",
                    "Job",
                    Some(ns),
                    "scale-down-job",
                    scaled_job,
                    current_job.resource_version,
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

#[tokio::test]
async fn test_job_stale_snapshot_after_delete_does_not_recreate_pods() {
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

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "stale-job", "namespace": "default", "uid": "stale-job-uid"},
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });
    let created = db
        .create_resource("batch/v1", "Job", Some("default"), "stale-job", job)
        .await
        .unwrap();
    let stale_snapshot = created.data.clone();

    db.delete_resource("batch/v1", "Job", Some("default"), "stale-job")
        .await
        .unwrap();

    reconcile_job_test(&db, &stale_snapshot, "test-node")
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
        "stale Job reconcile after delete must not recreate Pods"
    );
}

#[tokio::test]
async fn test_job_create_loop_observes_live_parallelism_scale_down() {
    let db = crate::datastore::test_support::in_memory().await;
    let pod_reader = crate::controllers::test_utils::pod_repository_for_test(&db);
    let pod_writer = Arc::new(ScaleDownDuringJobCreateWriter {
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

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "scale-down-job", "namespace": "default", "uid": "scale-down-job-uid"},
        "spec": {
            "completions": 4,
            "parallelism": 4,
            "template": {
                "metadata": {"labels": {"job": "scale-down-job"}},
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });
    let created = db
        .create_resource("batch/v1", "Job", Some("default"), "scale-down-job", job)
        .await
        .unwrap();
    let job_with_rv = crate::api::inject_resource_version(created.data, created.resource_version);

    super::reconcile_job(
        &db,
        pod_reader.as_ref(),
        pod_writer.as_ref(),
        pod_reader.as_ref(),
        &job_with_rv,
        "test-node",
    )
    .await
    .unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "scale-down-job-uid")
        .await
        .unwrap();
    assert_eq!(
        pods.len(),
        1,
        "Job reconcile must stop creating Pods after live spec.parallelism is lowered"
    );
}
mod canonical_pod_tests;
mod indexed_job_tests;
mod job_status_and_policy_tests;
