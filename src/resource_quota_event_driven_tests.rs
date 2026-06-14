//! Event-driven ResourceQuota convergence tests (T12).
//!
//! Asserts that `Status.Hard` and `Status.Used` converge through the
//! `ResourceQuotaEffect` side-effect dispatch path alone — without the
//! 30s `runtime_resourcequota_reconciler` periodic timer that T12 deletes.
//!
//! Each test simulates the API mutation handler's post-commit
//! `run_hooks_logged` call (via `SideEffectRegistry::run_hooks`) so the
//! same side-effect fan-out the running server uses is exercised here.

use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::side_effects::{SideEffectMetrics, SideEffectRegistry, default_registry};

const CONVERGENCE_BUDGET: Duration = Duration::from_secs(1);

async fn make_registry_and_repo() -> (
    crate::datastore::sqlite::Datastore,
    crate::datastore::DatastoreHandle,
    Arc<SideEffectRegistry>,
) {
    let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
    let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    ));
    let metrics = SideEffectMetrics::new();
    let registry = Arc::new(default_registry(
        metrics.clone(),
        None,
        None,
        Some(db_handle.clone()),
    ));
    let pod_repo = Arc::new(crate::kubelet::pod_repository::PodRepository::new(
        db_handle.clone(),
        supervisor,
        registry.clone(),
        metrics,
    ));
    registry.set_pod_repository(pod_repo);
    (db, db_handle, registry)
}

fn rq_with_pods_hard(name: &str, pods: u32) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "ResourceQuota",
        "metadata": {
            "name": name,
            "namespace": "default",
        },
        "spec": {
            "hard": { "pods": pods.to_string() }
        },
        "status": {
            "hard": {},
            "used": {}
        }
    })
}

fn pod(name: &str) -> serde_json::Value {
    json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": "default",
        },
        "spec": {
            "containers": [{ "name": "c", "image": "busybox" }]
        }
    })
}

async fn read_rq_status(db: &crate::datastore::sqlite::Datastore, name: &str) -> serde_json::Value {
    let r = db
        .get_resource("v1", "ResourceQuota", Some("default"), name)
        .await
        .unwrap()
        .expect("ResourceQuota present");
    r.data
        .pointer("/status")
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

fn assert_within(start: Instant, label: &str) {
    let elapsed = start.elapsed();
    assert!(
        elapsed < CONVERGENCE_BUDGET,
        "{label}: convergence took {elapsed:?}, exceeded {CONVERGENCE_BUDGET:?} budget"
    );
}

/// Status.Hard must mirror Spec.Hard within 1s of a ResourceQuota create —
/// achieved by the side-effect fan-out registering ResourceQuota itself.
#[tokio::test]
async fn rq_status_hard_synced_within_1s() {
    let (db, db_handle, registry) = make_registry_and_repo().await;
    let created = db
        .create_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            "rq",
            rq_with_pods_hard("rq", 5),
        )
        .await
        .unwrap();

    let start = Instant::now();
    registry
        .run_hooks(&created.data, db_handle.as_ref())
        .await
        .unwrap();
    assert_within(start, "Status.Hard initial sync");

    let status = read_rq_status(&db, "rq").await;
    assert_eq!(
        status.pointer("/hard/pods").and_then(|v| v.as_str()),
        Some("5"),
        "Status.Hard.pods must mirror Spec.Hard.pods after RQ create side effect"
    );
}

/// Status.Used.pods must increment to 1 within 1s of a Pod create.
#[tokio::test]
async fn rq_status_used_increments_on_pod_create_within_1s() {
    let (db, db_handle, registry) = make_registry_and_repo().await;
    let rq = db
        .create_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            "rq",
            rq_with_pods_hard("rq", 5),
        )
        .await
        .unwrap();
    registry
        .run_hooks(&rq.data, db_handle.as_ref())
        .await
        .unwrap();

    let pod_resource = db
        .create_resource("v1", "Pod", Some("default"), "p1", pod("p1"))
        .await
        .unwrap();

    let start = Instant::now();
    registry
        .run_hooks(&pod_resource.data, db_handle.as_ref())
        .await
        .unwrap();
    assert_within(start, "Status.Used after Pod create");

    let status = read_rq_status(&db, "rq").await;
    assert_eq!(
        status.pointer("/used/pods").and_then(|v| v.as_str()),
        Some("1"),
        "Status.Used.pods must reflect 1 live Pod after side effect"
    );
}

/// Status.Used.pods must decrement back to 0 within 1s of a Pod delete.
#[tokio::test]
async fn rq_status_used_decrements_on_pod_delete_within_1s() {
    let (db, db_handle, registry) = make_registry_and_repo().await;
    let rq = db
        .create_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            "rq",
            rq_with_pods_hard("rq", 5),
        )
        .await
        .unwrap();
    registry
        .run_hooks(&rq.data, db_handle.as_ref())
        .await
        .unwrap();

    let pod_resource = db
        .create_resource("v1", "Pod", Some("default"), "p1", pod("p1"))
        .await
        .unwrap();
    registry
        .run_hooks(&pod_resource.data, db_handle.as_ref())
        .await
        .unwrap();
    let after_create = read_rq_status(&db, "rq").await;
    assert_eq!(
        after_create.pointer("/used/pods").and_then(|v| v.as_str()),
        Some("1")
    );

    // Capture the pod's pre-delete representation; the API delete handler
    // runs hooks against the about-to-be-removed object, then commits the
    // deletion. We mimic the same order.
    let pod_snapshot: serde_json::Value = (*pod_resource.data).clone();
    db.delete_resource("v1", "Pod", Some("default"), "p1")
        .await
        .unwrap();

    let start = Instant::now();
    registry
        .run_hooks(&pod_snapshot, db_handle.as_ref())
        .await
        .unwrap();
    assert_within(start, "Status.Used after Pod delete");

    let status = read_rq_status(&db, "rq").await;
    assert_eq!(
        status.pointer("/used/pods").and_then(|v| v.as_str()),
        Some("0"),
        "Status.Used.pods must drop back to 0 after Pod delete side effect"
    );
}

/// A subsequent side-effect run after a `/status` PATCH must restore
/// `Status.Hard = Spec.Hard`. The post-PATCH spawn at
/// `src/api_status/helpers.rs` is what arms this in production; here we
/// just verify the side-effect path itself accomplishes the resync.
#[tokio::test]
async fn rq_status_hard_resyncs_after_status_patch_within_1s() {
    let (db, db_handle, registry) = make_registry_and_repo().await;
    let created = db
        .create_resource(
            "v1",
            "ResourceQuota",
            Some("default"),
            "rq",
            rq_with_pods_hard("rq", 5),
        )
        .await
        .unwrap();
    registry
        .run_hooks(&created.data, db_handle.as_ref())
        .await
        .unwrap();
    let before = read_rq_status(&db, "rq").await;
    assert_eq!(
        before.pointer("/hard/pods").and_then(|v| v.as_str()),
        Some("5")
    );

    // Simulate a `/status` PATCH that diverges Status.Hard from Spec.Hard.
    let mut tampered: serde_json::Value = (*created.data).clone();
    if let Some(obj) = tampered.as_object_mut() {
        obj.insert(
            "status".to_string(),
            json!({
                "hard": { "pods": "99" },
                "used": { "pods": "0" }
            }),
        );
    }
    db.update_resource_with_preconditions(
        "v1",
        "ResourceQuota",
        Some("default"),
        "rq",
        tampered.clone(),
        crate::datastore::ResourcePreconditions::default(),
    )
    .await
    .unwrap();
    let after_patch = read_rq_status(&db, "rq").await;
    assert_eq!(
        after_patch.pointer("/hard/pods").and_then(|v| v.as_str()),
        Some("99"),
        "tampered status must be visible before reconcile runs"
    );

    let start = Instant::now();
    registry
        .run_hooks(&tampered, db_handle.as_ref())
        .await
        .unwrap();
    assert_within(start, "Status.Hard resync after /status PATCH");

    let status = read_rq_status(&db, "rq").await;
    assert_eq!(
        status.pointer("/hard/pods").and_then(|v| v.as_str()),
        Some("5"),
        "Status.Hard must be re-synced from Spec.Hard by the side effect"
    );
}
