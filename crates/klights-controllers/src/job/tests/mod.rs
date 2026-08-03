use super::*;
use anyhow::Result;
use klights_cluster_core::Resource;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn coordination() -> &'static crate::ControllerCoordination {
    static COORDINATION: std::sync::LazyLock<crate::ControllerCoordination> =
        std::sync::LazyLock::new(crate::ControllerCoordination::new);
    &COORDINATION
}

fn derive_job_status_from_owned_pods(job: &Value, owned_pods: &[Resource]) -> Value {
    derive_job_status_from_owned_pods_at(job, owned_pods, chrono::Utc::now())
}

/// Test-only shim wrapping `reconcile_job` with the repository-backed
/// argument list, mirroring the pre-Task-18 signature.
async fn reconcile_job_test(
    db: &crate::test_support::TestStore,
    job: &Value,
    node_name: &str,
) -> Result<Value> {
    let identity = crate::test_support::deterministic_controller_identity();
    reconcile_job_test_with_identity(db, job, node_name, identity.as_ref()).await
}

async fn reconcile_job_test_with_identity(
    db: &crate::test_support::TestStore,
    job: &Value,
    node_name: &str,
    identity: &dyn crate::ControllerIdentityGenerator,
) -> Result<Value> {
    super::reconcile_job(
        db,
        db,
        db,
        identity,
        db,
        db,
        job,
        crate::test_support::test_reconcile_context(coordination(), node_name),
        chrono::Utc::now(),
    )
    .await
}

#[tokio::test]
async fn job_consumes_one_uid_per_pod_and_preserves_five_hex_name_derivation() {
    let db = crate::test_support::in_memory().await;
    db.create_resource(
        "batch/v1",
        "Job",
        Some("default"),
        "spy-job",
        json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": "spy-job", "namespace": "default", "uid": "spy-job-uid"},
            "spec": {
                "completions": 2,
                "parallelism": 2,
                "template": {"spec": {"containers": [{"name": "worker", "image": "busybox"}]}}
            }
        }),
    )
    .await
    .unwrap();
    let job = get_job(&db, "default", "spy-job").await;
    let identity = crate::test_support::ScriptedControllerIdentityGenerator::with_uids([
        "abcde111-0000-4000-8000-000000000000",
        "f0123222-0000-4000-8000-000000000000",
    ]);

    reconcile_job_test_with_identity(&db, &job, "test-node", &identity)
        .await
        .unwrap();

    let mut names = crate::test_support::find_owned_pods(&db, "default", "spy-job-uid")
        .await
        .unwrap()
        .into_iter()
        .map(|pod| pod.name)
        .collect::<Vec<_>>();
    names.sort_unstable();
    assert_eq!(names, ["spy-job-abcde", "spy-job-f0123"]);
    assert_eq!(identity.uid_calls(), 2);
}

/// Helper to fetch latest Job from DB with resourceVersion injected
async fn get_job(db: &crate::test_support::TestStore, namespace: &str, name: &str) -> Value {
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
    db: crate::test_support::TestStore,
    creates: AtomicUsize,
}

#[async_trait::async_trait]
impl JobPodMutation for ScaleDownDuringJobCreateWriter {
    async fn create_job_pod(
        &self,
        namespace: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        let count = self.creates.fetch_add(1, Ordering::SeqCst) + 1;
        let created = self
            .db
            .create_resource("v1", "Pod", Some(namespace), name, pod)
            .await?;
        if count == 1 {
            let current = self
                .db
                .get_resource("batch/v1", "Job", Some(namespace), "scale-down-job")
                .await?
                .expect("Job should exist");
            let mut job = (*current.data).clone();
            job["spec"]["parallelism"] = json!(1);
            self.db
                .update_resource(
                    "batch/v1",
                    "Job",
                    Some(namespace),
                    "scale-down-job",
                    job,
                    current.resource_version,
                )
                .await?;
        }
        Ok(created)
    }

    async fn replace_job_pod_owner_references(
        &self,
        namespace: &str,
        name: &str,
        owner_references: Vec<serde_json::Value>,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        let current = self
            .db
            .get_resource("v1", "Pod", Some(namespace), name)
            .await?
            .expect("Pod should exist");
        let mut pod = (*current.data).clone();
        pod["metadata"]["ownerReferences"] = serde_json::Value::Array(owner_references);
        self.db
            .update_resource(
                "v1",
                "Pod",
                Some(namespace),
                name,
                pod,
                current.resource_version,
            )
            .await
    }
}

#[tokio::test]
async fn test_job_stale_snapshot_after_delete_does_not_recreate_pods() {
    let db = crate::test_support::in_memory().await;
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
            crate::test_support::ResourceListQuery::all(),
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
    let db = crate::test_support::in_memory().await;
    let pod_reader = crate::test_support::pod_repository_for_test(&db);
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
    let job_with_rv =
        crate::test_support::inject_resource_version(created.data, created.resource_version);

    super::reconcile_job(
        &db,
        pod_reader.as_ref(),
        pod_writer.as_ref(),
        crate::test_support::deterministic_controller_identity().as_ref(),
        pod_reader.as_ref(),
        &db,
        &job_with_rv,
        crate::test_support::test_reconcile_context(coordination(), "test-node"),
        chrono::Utc::now(),
    )
    .await
    .unwrap();

    let pods = crate::test_support::find_owned_pods(&db, "default", "scale-down-job-uid")
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
