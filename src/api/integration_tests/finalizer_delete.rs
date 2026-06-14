use super::*;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use tower::ServiceExt;

async fn post_json(
    app: &axum::Router,
    uri: &str,
    body: serde_json::Value,
) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn request_json(
    app: &axum::Router,
    method: &str,
    uri: &str,
    content_type: &str,
    body: serde_json::Value,
) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", content_type)
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn response_json(resp: axum::response::Response) -> serde_json::Value {
    serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap()
}

async fn create_widget_crd(app: &axum::Router) {
    let resp = post_json(
        app,
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "widgets.example.com"},
            "spec": {
                "group": "example.com",
                "scope": "Namespaced",
                "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
                "versions": [{
                    "name": "v1",
                    "served": true,
                    "storage": true,
                    "schema": {"openAPIV3Schema": {"type": "object", "x-kubernetes-preserve-unknown-fields": true}}
                }]
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_custom_resource_delete_with_finalizer_marks_terminating_until_finalizer_drains() {
    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}}),
    )
    .await
    .unwrap();
    create_widget_crd(&app).await;

    let create = post_json(
        &app,
        "/apis/example.com/v1/namespaces/default/widgets",
        json!({
            "apiVersion": "example.com/v1",
            "kind": "Widget",
            "metadata": {
                "name": "held",
                "namespace": "default",
                "finalizers": ["example.com/hold"]
            },
            "spec": {"value": "before-delete"}
        }),
    )
    .await;
    assert_eq!(create.status(), StatusCode::CREATED);

    let delete = request_json(
        &app,
        "DELETE",
        "/apis/example.com/v1/namespaces/default/widgets/held",
        "application/json",
        json!({}),
    )
    .await;
    assert_eq!(delete.status(), StatusCode::OK);
    let delete_body = response_json(delete).await;
    assert_eq!(delete_body["kind"], "Widget");
    assert!(
        delete_body.pointer("/metadata/deletionTimestamp").is_some(),
        "custom resource DELETE with finalizers must return the terminating object: {delete_body:?}"
    );

    let held = db
        .get_resource("example.com/v1", "Widget", Some("default"), "held")
        .await
        .unwrap()
        .expect("finalizer-held custom resource must remain in datastore");
    assert!(
        held.data.pointer("/metadata/deletionTimestamp").is_some(),
        "finalizer-held custom resource must be marked terminating: {:?}",
        held.data
    );
    assert_eq!(
        held.data.pointer("/metadata/finalizers/0"),
        Some(&json!("example.com/hold"))
    );

    let patch = request_json(
        &app,
        "PATCH",
        "/apis/example.com/v1/namespaces/default/widgets/held",
        "application/merge-patch+json",
        json!({"metadata": {"finalizers": []}}),
    )
    .await;
    assert_eq!(patch.status(), StatusCode::OK);
    assert!(
        db.get_resource("example.com/v1", "Widget", Some("default"), "held")
            .await
            .unwrap()
            .is_none(),
        "clearing the last finalizer on a terminating custom resource must hard-delete it"
    );
}

#[tokio::test]
async fn test_custom_resource_foreground_delete_preserves_user_finalizer_until_drain() {
    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}}),
    )
    .await
    .unwrap();
    create_widget_crd(&app).await;

    let create = post_json(
        &app,
        "/apis/example.com/v1/namespaces/default/widgets",
        json!({
            "apiVersion": "example.com/v1",
            "kind": "Widget",
            "metadata": {
                "name": "foreground-held",
                "namespace": "default",
                "finalizers": ["example.com/hold"]
            }
        }),
    )
    .await;
    assert_eq!(create.status(), StatusCode::CREATED);

    let delete = request_json(
        &app,
        "DELETE",
        "/apis/example.com/v1/namespaces/default/widgets/foreground-held",
        "application/json",
        json!({"propagationPolicy": "Foreground"}),
    )
    .await;
    assert_eq!(delete.status(), StatusCode::OK);

    let held = db
        .get_resource(
            "example.com/v1",
            "Widget",
            Some("default"),
            "foreground-held",
        )
        .await
        .unwrap()
        .expect("foreground delete must not hard-delete while a user finalizer remains");
    assert!(
        held.data.pointer("/metadata/deletionTimestamp").is_some(),
        "foreground delete must mark the custom resource terminating: {:?}",
        held.data
    );
    let finalizers = held
        .data
        .pointer("/metadata/finalizers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert_eq!(finalizers, vec![json!("example.com/hold")]);

    let patch = request_json(
        &app,
        "PATCH",
        "/apis/example.com/v1/namespaces/default/widgets/foreground-held",
        "application/merge-patch+json",
        json!({"metadata": {"finalizers": []}}),
    )
    .await;
    assert_eq!(patch.status(), StatusCode::OK);
    assert!(
        db.get_resource(
            "example.com/v1",
            "Widget",
            Some("default"),
            "foreground-held"
        )
        .await
        .unwrap()
        .is_none(),
        "foreground custom resource must be removed after user finalizer drains"
    );
}
