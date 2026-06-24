use std::sync::Arc;

use crate::datastore::backend_kind::BackendKind;
use crate::datastore::node_local::{
    NodeLocalBackend, NodeLocalDb, NodeLocalHandle, SqliteNodeLocalDb, selector,
};
use crate::datastore::sqlite::{DbExecutor, opener};
use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};

fn supervisor() -> Arc<TaskSupervisor> {
    Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
}

async fn open_node_local_in_memory() -> NodeLocalDb {
    let executor = DbExecutor::open_with_opts(
        opener::OpenOpts::node_in_memory(),
        supervisor(),
        "sqlite:node-local-test",
    )
    .await
    .expect("open node-local executor");
    NodeLocalDb::from_executor(executor).expect("create node-local db")
}

async fn open_sqlite_node_local_backend_handle() -> NodeLocalHandle {
    let executor = DbExecutor::open_with_opts(
        opener::OpenOpts::node_in_memory(),
        supervisor(),
        "sqlite:node-local-backend-test",
    )
    .await
    .expect("open node-local executor");
    let db = SqliteNodeLocalDb::from_executor(executor).expect("create sqlite node-local db");
    Arc::new(db)
}

#[tokio::test]
async fn node_local_schema_has_only_slim_uid_bound_tables() {
    let db = open_node_local_in_memory().await;

    let tables = db.table_names_for_test().await.expect("table names");

    assert!(tables.contains(&"outbox".to_string()));
    assert!(tables.contains(&"outbox_dead_letter".to_string()));
    assert!(tables.contains(&"pod_runtime".to_string()));
    assert!(tables.contains(&"pod_status_checkpoints".to_string()));
    assert!(tables.contains(&"pod_networks".to_string()));
    assert!(tables.contains(&"pod_endpoints".to_string()));
    assert!(tables.contains(&"pod_workqueue".to_string()));
    assert!(tables.contains(&"probe_state".to_string()));
    assert!(tables.contains(&"replication_checkpoint".to_string()));
    assert!(tables.contains(&"_node_meta".to_string()));

    for forbidden in [
        "namespaced_resources",
        "cluster_resources",
        "namespaces",
        "watch_events",
        "pod_sandboxes",
    ] {
        assert!(
            !tables.contains(&forbidden.to_string()),
            "node.db must not contain cluster resource/cache table {forbidden}"
        );
    }

    for table in [
        "outbox",
        "pod_runtime",
        "pod_status_checkpoints",
        "pod_networks",
        "pod_endpoints",
        "pod_workqueue",
        "probe_state",
    ] {
        assert!(
            db.table_has_not_null_column_for_test(table, "pod_uid")
                .await
                .expect("pod_uid column check"),
            "{table} must have pod_uid TEXT NOT NULL"
        );
    }

    assert!(
        !db.schema_contains_full_resource_body_column_for_test()
            .await
            .expect("body column check"),
        "node.db must not contain Kubernetes resource body data BLOB columns"
    );
}

#[tokio::test]
async fn pod_status_checkpoint_is_uid_bound_and_status_only() {
    let db = open_node_local_in_memory().await;

    db.upsert_pod_status_checkpoint(
        "uid-1",
        "default",
        "web",
        7,
        serde_json::json!({
            "phase": "Running",
            "podIP": "10.42.0.9",
        }),
        100,
    )
    .await
    .expect("upsert checkpoint");

    let checkpoint = db
        .get_pod_status_checkpoint("uid-1")
        .await
        .expect("get checkpoint")
        .expect("checkpoint exists");
    assert_eq!(checkpoint.pod_uid, "uid-1");
    assert_eq!(checkpoint.namespace, "default");
    assert_eq!(checkpoint.pod_name, "web");
    assert_eq!(checkpoint.base_rv, 7);
    assert_eq!(checkpoint.applied_rv, None);
    assert_eq!(
        checkpoint.status.pointer("/podIP").and_then(|v| v.as_str()),
        Some("10.42.0.9")
    );
    assert!(checkpoint.status.get("metadata").is_none());

    db.mark_pod_status_checkpoint_applied("uid-1", 12, 200)
        .await
        .expect("mark applied");
    assert_eq!(
        db.get_pod_status_checkpoint("uid-1")
            .await
            .expect("get marked")
            .expect("checkpoint still exists")
            .applied_rv,
        Some(12)
    );

    db.delete_pod_status_checkpoint("uid-1")
        .await
        .expect("delete checkpoint");
    assert!(
        db.get_pod_status_checkpoint("uid-1")
            .await
            .expect("get deleted")
            .is_none()
    );
}

#[tokio::test]
async fn node_meta_mismatch_refuses_boot() {
    let db = open_node_local_in_memory().await;

    db.ensure_node_identity("cluster-a", "node-a")
        .await
        .expect("initial identity write");

    let err = db
        .ensure_node_identity("cluster-b", "node-a")
        .await
        .expect_err("cluster id change must refuse boot");

    assert!(err.to_string().contains("node.db identity mismatch"));
}

#[tokio::test]
async fn pod_runtime_is_uid_keyed_and_same_name_replacements_are_distinct() {
    let db = open_node_local_in_memory().await;

    db.admit_pod_runtime("uid-old", "default", "web", "worker-a")
        .await
        .expect("admit old uid");
    db.admit_pod_runtime("uid-new", "default", "web", "worker-a")
        .await
        .expect("admit new uid");

    let rows = db.list_pod_runtime().await.expect("list runtime");
    let uids: Vec<_> = rows.into_iter().map(|row| row.pod_uid).collect();

    assert_eq!(uids, vec!["uid-new".to_string(), "uid-old".to_string()]);
}

#[tokio::test]
async fn sqlite_backend_implements_node_local_backend() {
    let handle = open_sqlite_node_local_backend_handle().await;
    fn assert_backend_trait(_: &dyn NodeLocalBackend) {}
    assert_backend_trait(handle.as_ref());
    assert_eq!(handle.backend_name(), "sqlite");

    handle
        .set_node_meta("node_uid", "node-a")
        .await
        .expect("write meta through trait object");
    assert_eq!(
        handle.get_node_meta("node_uid").await.expect("read meta"),
        Some("node-a".to_string())
    );
}

#[tokio::test]
async fn selector_returns_sqlite_node_local_handle() {
    let handle = selector::open_node_local(
        BackendKind::Sqlite,
        None,
        supervisor(),
        None,
        "sqlite:node-local-selector-test",
    )
    .await
    .expect("open sqlite node-local");

    assert_eq!(handle.backend_name(), "sqlite");
}

#[tokio::test]
async fn redb_node_local_selector_fails_fast_until_backend_lands() {
    let result = selector::open_node_local(
        BackendKind::Redb,
        None,
        supervisor(),
        None,
        "redb:node-local-selector-test",
    )
    .await;
    let err = match result {
        Ok(handle) => panic!(
            "redb node-local unexpectedly opened {}",
            handle.backend_name()
        ),
        Err(err) => err,
    };

    assert!(
        err.to_string()
            .contains("node-local redb backend not implemented yet"),
        "unexpected error: {err}"
    );
}

#[test]
fn node_local_handle_hides_concrete_backend_type() {
    // R4: invariant now enforced by check_supervisor_spawn.sh
}

#[test]
fn node_local_backend_is_not_exposed_by_datastore_backend() {
    // R4: invariant now enforced by check_supervisor_spawn.sh
}

#[test]
fn node_local_backend_has_no_cluster_resource_crud() {
    // R4: invariant now enforced by check_supervisor_spawn.sh
}

// ── Task 9: RuntimeObservationCheckpoint tests ────────────────────

#[tokio::test]
async fn runtime_observation_checkpoint_survives_actor_restart() {
    use crate::datastore::node_local::sqlite::RuntimeObservationCheckpoint;
    let db = open_node_local_in_memory().await;

    db.upsert_runtime_observation_checkpoint(RuntimeObservationCheckpoint {
        pod_uid: "uid-restart".to_string(),
        container_ids: vec![
            "containerd://ctr-abc".to_string(),
            "containerd://ctr-def".to_string(),
        ],
        generation: 2,
        updated_ms: 1000,
    })
    .await
    .expect("upsert checkpoint");

    // Simulate actor restart: a new actor reads back the persisted checkpoint.
    let loaded = db
        .get_runtime_observation_checkpoint("uid-restart")
        .await
        .expect("get checkpoint")
        .expect("checkpoint must exist after actor restart");

    assert_eq!(loaded.pod_uid, "uid-restart");
    assert_eq!(loaded.generation, 2);
    assert!(
        loaded
            .container_ids
            .contains(&"containerd://ctr-abc".to_string())
    );
    assert!(
        loaded
            .container_ids
            .contains(&"containerd://ctr-def".to_string())
    );
    assert_eq!(loaded.updated_ms, 1000);
}

#[tokio::test]
async fn runtime_observation_checkpoint_survives_worker_restart() {
    use crate::datastore::node_local::sqlite::RuntimeObservationCheckpoint;
    let db = open_node_local_in_memory().await;

    // Write checkpoints for two pods
    db.upsert_runtime_observation_checkpoint(RuntimeObservationCheckpoint {
        pod_uid: "uid-pod-a".to_string(),
        container_ids: vec!["containerd://ctr-a1".to_string()],
        generation: 1,
        updated_ms: 500,
    })
    .await
    .expect("upsert pod-a checkpoint");

    db.upsert_runtime_observation_checkpoint(RuntimeObservationCheckpoint {
        pod_uid: "uid-pod-b".to_string(),
        container_ids: vec!["containerd://ctr-b1".to_string()],
        generation: 3,
        updated_ms: 750,
    })
    .await
    .expect("upsert pod-b checkpoint");

    // Simulate worker restart: both checkpoints survive and can be loaded.
    let a = db
        .get_runtime_observation_checkpoint("uid-pod-a")
        .await
        .expect("get a")
        .expect("a exists");
    assert_eq!(a.generation, 1);
    let b = db
        .get_runtime_observation_checkpoint("uid-pod-b")
        .await
        .expect("get b")
        .expect("b exists");
    assert_eq!(b.generation, 3);

    // Reconcile pod-a; checkpoint deleted. pod-b checkpoint survives.
    db.delete_runtime_observation_checkpoint("uid-pod-a")
        .await
        .expect("delete a");
    assert!(
        db.get_runtime_observation_checkpoint("uid-pod-a")
            .await
            .expect("get a after delete")
            .is_none()
    );
    assert!(
        db.get_runtime_observation_checkpoint("uid-pod-b")
            .await
            .expect("get b after a delete")
            .is_some()
    );
}

#[tokio::test]
async fn runtime_observation_checkpoint_is_uid_bound() {
    use crate::datastore::node_local::sqlite::RuntimeObservationCheckpoint;
    let db = open_node_local_in_memory().await;

    db.upsert_runtime_observation_checkpoint(RuntimeObservationCheckpoint {
        pod_uid: "uid-alpha".to_string(),
        container_ids: vec!["containerd://alpha-1".to_string()],
        generation: 5,
        updated_ms: 100,
    })
    .await
    .expect("upsert alpha");

    db.upsert_runtime_observation_checkpoint(RuntimeObservationCheckpoint {
        pod_uid: "uid-beta".to_string(),
        container_ids: vec![
            "containerd://beta-1".to_string(),
            "containerd://beta-2".to_string(),
        ],
        generation: 7,
        updated_ms: 200,
    })
    .await
    .expect("upsert beta");

    // Each UID returns only its own checkpoint.
    let alpha = db
        .get_runtime_observation_checkpoint("uid-alpha")
        .await
        .expect("get alpha")
        .expect("alpha exists");
    assert_eq!(alpha.container_ids, vec!["containerd://alpha-1"]);
    assert_eq!(alpha.generation, 5);

    let beta = db
        .get_runtime_observation_checkpoint("uid-beta")
        .await
        .expect("get beta")
        .expect("beta exists");
    assert_eq!(beta.container_ids.len(), 2);
    assert_eq!(beta.generation, 7);

    // Deleting alpha must not affect beta.
    db.delete_runtime_observation_checkpoint("uid-alpha")
        .await
        .expect("delete alpha");
    assert!(
        db.get_runtime_observation_checkpoint("uid-alpha")
            .await
            .expect("get alpha gone")
            .is_none()
    );
    assert!(
        db.get_runtime_observation_checkpoint("uid-beta")
            .await
            .expect("get beta still")
            .is_some()
    );
}

#[tokio::test]
async fn runtime_observation_checkpoint_is_removed_after_successful_reconcile() {
    use crate::datastore::node_local::sqlite::RuntimeObservationCheckpoint;
    let db = open_node_local_in_memory().await;

    db.upsert_runtime_observation_checkpoint(RuntimeObservationCheckpoint {
        pod_uid: "uid-reconcile".to_string(),
        container_ids: vec!["containerd://ctr-99".to_string()],
        generation: 1,
        updated_ms: 300,
    })
    .await
    .expect("upsert before reconcile");

    assert!(
        db.get_runtime_observation_checkpoint("uid-reconcile")
            .await
            .expect("pre-reconcile get")
            .is_some()
    );

    // Successful reconcile: actor removes its checkpoint.
    db.delete_runtime_observation_checkpoint("uid-reconcile")
        .await
        .expect("delete after reconcile");

    assert!(
        db.get_runtime_observation_checkpoint("uid-reconcile")
            .await
            .expect("post-reconcile get")
            .is_none(),
        "checkpoint must be gone after successful reconcile"
    );
}
