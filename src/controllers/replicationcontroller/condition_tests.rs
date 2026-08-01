use super::*;
use crate::datastore::sqlite::Datastore;
use serde_json::json;

/// Test-only shim wrapping `reconcile_replicationcontroller` with the
/// repository-backed argument list, mirroring the pre-Task-18 signature.
async fn reconcile_rc_test(db: &Datastore, rc: &Value, node_name: &str) -> anyhow::Result<()> {
    let repo = crate::controllers::test_utils::pod_repository_for_test(db);
    let coordination = klights_controllers::ControllerCoordination::new();
    let store = crate::controllers::test_utils::controller_store_for_test(db);
    super::reconcile_replicationcontroller(
        &store,
        repo.as_ref(),
        repo.as_ref(),
        crate::controllers::test_utils::deterministic_controller_identity().as_ref(),
        repo.as_ref(),
        crate::controllers::test_utils::non_pod_finalization_port_for_test(),
        rc,
        crate::controllers::test_reconcile_context(&coordination, node_name),
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
    update_replicationcontroller_status(
        &crate::controllers::test_utils::controller_store_for_test(&db),
        "test-rc",
        "default",
        &[],
        Some("exceeded quota: pods count limit"),
        chrono::DateTime::UNIX_EPOCH,
    )
    .await
    .unwrap();
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
    update_replicationcontroller_status(
        &crate::controllers::test_utils::controller_store_for_test(&db),
        "test-rc-ok",
        "default",
        &[],
        None,
        chrono::DateTime::UNIX_EPOCH,
    )
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

/// P0-E2E-20260424b-06: GET/PUT/PATCH /replicationcontrollers/{name}/scale must work.
#[tokio::test]
async fn test_replicationcontroller_scale_subresource() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    setup_db_with_rc(&db, "test-rc-scale").await;

    let _config = std::sync::Arc::new({
        let ns = "klights-test";
        crate::KlightsConfig {
            bridge_name: ns.to_string(),
            pod_subnet: "10.43.0.0/17".to_string(),
            cluster_cidr: "10.42.0.0/16".to_string(),
            service_cidr: "10.43.128.0/17".to_string(),
            tls_port: 7443,
            api_fqdn: None,
            log_file: None,
            containerd_namespace: ns.to_string(),
            containerd_socket: None,
            registry_proxy: klights_kubelet::registry_proxy::RegistryProxyConfig::from_inputs(
                false, None, false,
            )
            .unwrap(),
            node_name: "test-node".to_string(),
            node_ip: None,
            anonymous_auth: true,
            dataplane_encryption: klights_networking::wireguard::DataplaneEncryption::Enabled,
            external_endpoint: None,
            worker_dataplane_no_ingress: false,
            wireguard_device: klights_networking::wireguard::DEFAULT_WIREGUARD_DEVICE.to_string(),
            wireguard_port: klights_networking::wireguard::DEFAULT_WIREGUARD_PORT,
            cluster_db_path: crate::paths::test_data_root_path(ns)
                .join("db")
                .join("sqlite")
                .join("cluster.db"),
            node_db_path: crate::paths::test_data_root_path(ns)
                .join("db")
                .join("sqlite")
                .join("node.db"),
            data_root: crate::paths::test_data_root_path(ns),
            api_slow_log_threshold: std::time::Duration::from_millis(
                crate::bootstrap::config::DEFAULT_API_SLOW_LOG_MS,
            ),
            node_not_ready_pod_eviction_grace: std::time::Duration::ZERO,
            max_watch_events: crate::bootstrap::config::DEFAULT_MAX_WATCH_EVENTS,
            gc_interval: std::time::Duration::from_secs(
                crate::bootstrap::config::DEFAULT_GC_INTERVAL_SECONDS,
            ),
            in_memory: true,
            db_encryption: crate::DbEncryption::Disabled,
            db_key_file: None,
            datastore_backend: crate::datastore::backend_kind::BackendKind::Sqlite,
            node_local_backend: crate::datastore::backend_kind::BackendKind::Sqlite,
            oidc_issuer_url: None,
            oidc_client_id: None,
            oidc_username_claim: "sub".to_string(),
            oidc_groups_claim: "groups".to_string(),
            oidc_groups_prefix: String::new(),
            oidc_ca_bundle: None,
            webhook_auth_url: None,
            webhook_auth_client_cert: None,
            webhook_auth_client_key: None,
            webhook_auth_audiences: String::new(),
            webhook_auth_cache_authorized_ttl_secs: 300,
            webhook_auth_cache_unauthorized_ttl_secs: 30,
            webhook_auth_ca_bundle: None,
        }
    });
    let service_ipam = std::sync::Arc::new(klights_controllers::service::ServiceIpam::new(
        "10.43.128.0/17",
    ));
    let _controller_dispatcher = std::sync::Arc::new(
        crate::controllers::ControllerDispatcher::new(service_ipam.clone()),
    );
    let state =
        crate::crd_tests::build_test_app_state(db, crate::controllers::crd::CrdRegistry::new())
            .await;
    let app = crate::api::build_router(state);

    // GET /api/v1/namespaces/default/replicationcontrollers/test-rc-scale/scale
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/namespaces/default/replicationcontrollers/test-rc-scale/scale")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "GET scale must return 200");
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let scale: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(scale["kind"], "Scale");
    assert_eq!(scale["spec"]["replicas"], 1);

    // PUT to update replicas to 3
    let put_body = serde_json::json!({
        "apiVersion": "autoscaling/v1", "kind": "Scale",
        "metadata": {"name": "test-rc-scale", "namespace": "default"},
        "spec": {"replicas": 3}
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/v1/namespaces/default/replicationcontrollers/test-rc-scale/scale")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&put_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "PUT scale must return 200");
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let scale: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        scale["spec"]["replicas"], 3,
        "replicas must be updated to 3"
    );

    // PATCH replicas to 5 via merge-patch
    let patch_body = serde_json::json!({"spec": {"replicas": 5}});
    let resp = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/namespaces/default/replicationcontrollers/test-rc-scale/scale")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(serde_json::to_vec(&patch_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "PATCH scale must return 200");
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let scale: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        scale["spec"]["replicas"], 5,
        "replicas must be updated to 5"
    );
}
