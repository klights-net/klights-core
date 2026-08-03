use crate::datastore::sqlite::Datastore;
use serde_json::{Value, json};

/// Test-only shim wrapping `reconcile_replicationcontroller` with the
/// repository-backed argument list, mirroring the pre-Task-18 signature.
async fn reconcile_rc_test(db: &Datastore, rc: &Value, node_name: &str) -> anyhow::Result<()> {
    let repo = crate::controller_test_support::pod_repository_for_test(db);
    let coordination = klights_controllers::ControllerCoordination::new();
    let store = crate::controller_test_support::controller_store_for_test(db);
    super::reconcile_replicationcontroller(
        &store,
        repo.as_ref(),
        repo.as_ref(),
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
        repo.as_ref(),
        crate::controller_test_support::non_pod_finalization_port_for_test(),
        rc,
        crate::controller_test_support::test_reconcile_context(&coordination, node_name),
    )
    .await
}

async fn setup_db_with_rc(db: &Datastore, rc_name: &str) {
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata":{"name":"default"}}),
    )
    .await
    .unwrap();
    let rc = json!({
        "apiVersion": "v1", "kind": "ReplicationController",
        "metadata": {"name": rc_name, "namespace": "default", "uid": "rc-uid-1"},
        "spec": {"replicas": 1, "selector": {"app": "test"},
            "template": {"metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}}}
    });
    db.create_resource("v1", "ReplicationController", Some("default"), rc_name, rc)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_rc_publishes_replica_failure_condition_on_create_failure() {
    let db = crate::datastore::test_support::in_memory().await;
    setup_db_with_rc(&db, "test-rc").await;
    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "deny-pods",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "deny-pods", "namespace": "default"},
            "spec": {"hard": {"pods": "0"}}
        }),
    )
    .await
    .unwrap();
    let rc = db
        .get_resource("v1", "ReplicationController", Some("default"), "test-rc")
        .await
        .unwrap()
        .unwrap();
    assert!(
        reconcile_rc_test(&db, rc.data.as_ref(), "node1")
            .await
            .is_err()
    );
    let updated = db
        .get_resource("v1", "ReplicationController", Some("default"), "test-rc")
        .await
        .unwrap()
        .unwrap();
    let conds = updated
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .expect("conditions must be present");
    let failure = conds
        .iter()
        .find(|c| c["type"] == "ReplicaFailure")
        .expect("ReplicaFailure condition must exist");
    assert_eq!(failure["status"], "True");
    assert_eq!(failure["reason"], "FailedCreate");
}

#[tokio::test]
async fn test_rc_clears_replica_failure_condition_when_healthy() {
    let db = crate::datastore::test_support::in_memory().await;
    setup_db_with_rc(&db, "test-rc-ok").await;
    let rc = db
        .get_resource("v1", "ReplicationController", Some("default"), "test-rc-ok")
        .await
        .unwrap()
        .unwrap();
    reconcile_rc_test(&db, rc.data.as_ref(), "node1")
        .await
        .unwrap();
    let updated = db
        .get_resource("v1", "ReplicationController", Some("default"), "test-rc-ok")
        .await
        .unwrap()
        .unwrap();
    let conds = updated
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .expect("conditions present");
    assert!(
        conds.iter().all(|c| c["type"] != "ReplicaFailure"),
        "ReplicaFailure must be removed when controller is healthy"
    );
}

#[tokio::test]
async fn test_rc_returns_error_when_quota_blocks_pod_create() {
    let db = crate::datastore::test_support::in_memory().await;
    setup_db_with_rc(&db, "test-rc-quota").await;

    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "rq-pods-2",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "rq-pods-2", "namespace": "default"},
            "spec": {"hard": {"pods": "2"}}
        }),
    )
    .await
    .unwrap();

    let current_rc = db
        .get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "test-rc-quota",
        )
        .await
        .unwrap()
        .unwrap();
    let updated_rc = json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {"name": "test-rc-quota", "namespace": "default", "uid": "rc-uid-1"},
        "spec": {
            "replicas": 3,
            "selector": {"app": "test"},
            "template": {"metadata": {"labels": {"app": "test"}},
                "spec": {"containers": [{"name": "c", "image": "nginx"}]}}
        }
    });
    db.update_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "test-rc-quota",
        updated_rc.clone(),
        current_rc.resource_version,
    )
    .await
    .unwrap();

    let result = reconcile_rc_test(&db, &updated_rc, "node1").await;
    assert!(result.is_err(), "quota denial should fail reconcile");

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        2,
        "reconcile should stop at quota boundary and return error"
    );

    let rc_after = db
        .get_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "test-rc-quota",
        )
        .await
        .unwrap()
        .expect("RC must still exist");
    let failure = rc_after
        .data
        .pointer("/status/conditions")
        .and_then(|v| v.as_array())
        .and_then(|conds| conds.iter().find(|c| c["type"] == "ReplicaFailure"))
        .cloned()
        .expect("quota-denied reconcile must publish ReplicaFailure condition");
    assert_eq!(failure["status"], "True");
    assert_eq!(failure["reason"], "FailedCreate");
}
