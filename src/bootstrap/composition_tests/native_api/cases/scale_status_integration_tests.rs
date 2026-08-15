use klights_cluster_core::ResourcePreconditions;
use serde_json::{Value, json};

#[tokio::test]
async fn patch_statefulset_scale_does_not_conflict_with_concurrent_status_updates() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state =
        crate::bootstrap::composition_tests::native_api::support::build_test_app_state().await;
    let db = state.resource_store();
    let app = state.router();
    db.create_resource(
        "apps/v1",
        "StatefulSet",
        Some("default"),
        "scale-race",
        json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {
                "name": "scale-race",
                "namespace": "default",
                "uid": "scale-race-uid"
            },
            "spec": {
                "replicas": 1,
                "serviceName": "scale-race",
                "selector": {"matchLabels": {"app": "scale-race"}},
                "template": {
                    "metadata": {"labels": {"app": "scale-race"}},
                    "spec": {"containers": [{"name": "web", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
                }
            },
            "status": {"replicas": 1, "readyReplicas": 1}
        }),
    )
    .await
    .unwrap();

    let pause = state.install_resource_mutation_pause(
        klights_cluster_datastore::test_support::ResourceMutationPauseOperation::BuildPatchCommand,
        "apps/v1",
        "StatefulSet",
        Some("default"),
        "scale-race",
    );
    let request = tokio::spawn(
        app.clone().oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/apis/apps/v1/namespaces/default/statefulsets/scale-race/scale")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(json!({"spec": {"replicas": 7}}).to_string()))
                .unwrap(),
        ),
    );
    pause.wait_until_reached().await;
    db.update_status_only_with_preconditions(
        "apps/v1",
        "StatefulSet",
        Some("default"),
        "scale-race",
        json!({"replicas": 5, "readyReplicas": 4}),
        ResourcePreconditions::uid("scale-race-uid"),
    )
    .await
    .unwrap();
    pause.resume();

    let response = request.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let scale: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(scale["spec"]["replicas"], 7);
    assert_eq!(scale["status"]["replicas"], 5);
}

#[tokio::test]
async fn patch_replicaset_scale_does_not_conflict_with_concurrent_status_updates() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state =
        crate::bootstrap::composition_tests::native_api::support::build_test_app_state().await;
    let db = state.resource_store();
    let app = state.router();
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "scale-race-rs",
        json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "scale-race-rs",
                "namespace": "default",
                "uid": "scale-race-rs-uid"
            },
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "scale-race-rs"}},
                "template": {
                    "metadata": {"labels": {"app": "scale-race-rs"}},
                    "spec": {"containers": [{"name": "web", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
                }
            },
            "status": {"replicas": 1, "readyReplicas": 1}
        }),
    )
    .await
    .unwrap();

    let pause = state.install_resource_mutation_pause(
        klights_cluster_datastore::test_support::ResourceMutationPauseOperation::BuildPatchCommand,
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "scale-race-rs",
    );
    let request = tokio::spawn(
        app.clone().oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/apis/apps/v1/namespaces/default/replicasets/scale-race-rs/scale")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(json!({"spec": {"replicas": 7}}).to_string()))
                .unwrap(),
        ),
    );
    pause.wait_until_reached().await;
    db.update_status_only_with_preconditions(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "scale-race-rs",
        json!({"replicas": 5, "readyReplicas": 4}),
        ResourcePreconditions::uid("scale-race-rs-uid"),
    )
    .await
    .unwrap();
    pause.resume();

    let response = request.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let scale: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(scale["spec"]["replicas"], 7);
    assert_eq!(scale["status"]["replicas"], 5);
}

#[tokio::test]
async fn update_replicaset_scale_with_empty_resource_version_is_unconditional() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state =
        crate::bootstrap::composition_tests::native_api::support::build_test_app_state().await;
    let db = state.resource_store();
    let app = state.router();
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "scale-put-race-rs",
        json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "scale-put-race-rs",
                "namespace": "default",
                "uid": "scale-put-race-rs-uid"
            },
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "scale-put-race-rs"}},
                "template": {
                    "metadata": {"labels": {"app": "scale-put-race-rs"}},
                    "spec": {"containers": [{"name": "web", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
                }
            },
            "status": {"replicas": 1, "readyReplicas": 1}
        }),
    )
    .await
    .unwrap();

    let pause = state.install_resource_mutation_pause(
        klights_cluster_datastore::test_support::ResourceMutationPauseOperation::BuildPatchCommand,
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "scale-put-race-rs",
    );
    let request = tokio::spawn(
        app.clone().oneshot(
            Request::builder()
                .method("PUT")
                .uri("/apis/apps/v1/namespaces/default/replicasets/scale-put-race-rs/scale")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "apiVersion": "autoscaling/v1",
                        "kind": "Scale",
                        "metadata": {
                            "name": "scale-put-race-rs",
                            "namespace": "default",
                            "resourceVersion": ""
                        },
                        "spec": {"replicas": 7}
                    })
                    .to_string(),
                ))
                .unwrap(),
        ),
    );
    pause.wait_until_reached().await;
    db.update_status_only_with_preconditions(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "scale-put-race-rs",
        json!({"replicas": 5, "readyReplicas": 4}),
        ResourcePreconditions::uid("scale-put-race-rs-uid"),
    )
    .await
    .unwrap();
    pause.resume();

    let response = request.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let scale: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(scale["spec"]["replicas"], 7);
    assert_eq!(scale["status"]["replicas"], 5);
}

#[tokio::test]
async fn update_replicaset_scale_with_stale_resource_version_returns_conflict() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let (app, db) =
        crate::bootstrap::composition_tests::native_api::support::build_test_router_with_db().await;
    db.create_resource(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "scale-put-stale-rs",
        json!({
            "apiVersion": "apps/v1",
            "kind": "ReplicaSet",
            "metadata": {
                "name": "scale-put-stale-rs",
                "namespace": "default",
                "uid": "scale-put-stale-rs-uid"
            },
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": "scale-put-stale-rs"}},
                "template": {
                    "metadata": {"labels": {"app": "scale-put-stale-rs"}},
                    "spec": {"containers": [{"name": "web", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
                }
            },
            "status": {"replicas": 1, "readyReplicas": 1}
        }),
    )
    .await
    .unwrap();

    let initial = db
        .get_resource(
            "apps/v1",
            "ReplicaSet",
            Some("default"),
            "scale-put-stale-rs",
        )
        .await
        .unwrap()
        .unwrap();
    db.update_status_only_with_preconditions(
        "apps/v1",
        "ReplicaSet",
        Some("default"),
        "scale-put-stale-rs",
        json!({"replicas": 1, "readyReplicas": 1, "observedGeneration": 1}),
        ResourcePreconditions::uid("scale-put-stale-rs-uid"),
    )
    .await
    .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/apis/apps/v1/namespaces/default/replicasets/scale-put-stale-rs/scale")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "apiVersion": "autoscaling/v1",
                        "kind": "Scale",
                        "metadata": {
                            "name": "scale-put-stale-rs",
                            "namespace": "default",
                            "resourceVersion": initial.resource_version.to_string()
                        },
                        "spec": {"replicas": 2}
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "stale non-empty scale resourceVersion must remain a CAS precondition: {}",
        String::from_utf8_lossy(&body),
    );
}

#[tokio::test]
async fn patch_replicationcontroller_scale_does_not_conflict_with_concurrent_status_updates() {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state =
        crate::bootstrap::composition_tests::native_api::support::build_test_app_state().await;
    let db = state.resource_store();
    let app = state.router();
    db.create_resource(
        "v1",
        "ReplicationController",
        Some("default"),
        "scale-race-rc",
        json!({
            "apiVersion": "v1",
            "kind": "ReplicationController",
            "metadata": {
                "name": "scale-race-rc",
                "namespace": "default",
                "uid": "scale-race-rc-uid"
            },
            "spec": {
                "replicas": 1,
                "selector": {"app": "scale-race-rc"},
                "template": {
                    "metadata": {"labels": {"app": "scale-race-rc"}},
                    "spec": {"containers": [{"name": "web", "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
                }
            },
            "status": {"replicas": 1, "readyReplicas": 1}
        }),
    )
    .await
    .unwrap();

    let pause = state.install_resource_mutation_pause(
        klights_cluster_datastore::test_support::ResourceMutationPauseOperation::BuildPatchCommand,
        "v1",
        "ReplicationController",
        Some("default"),
        "scale-race-rc",
    );
    let request = tokio::spawn(
        app.clone().oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/namespaces/default/replicationcontrollers/scale-race-rc/scale")
                .header("content-type", "application/merge-patch+json")
                .body(Body::from(json!({"spec": {"replicas": 7}}).to_string()))
                .unwrap(),
        ),
    );
    pause.wait_until_reached().await;
    db.update_status_only_with_preconditions(
        "v1",
        "ReplicationController",
        Some("default"),
        "scale-race-rc",
        json!({"replicas": 5, "readyReplicas": 4}),
        ResourcePreconditions::uid("scale-race-rc-uid"),
    )
    .await
    .unwrap();
    pause.resume();

    let response = request.await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let scale: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(scale["spec"]["replicas"], 7);
    assert_eq!(scale["status"]["replicas"], 5);
}
