use super::*;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use tower::ServiceExt;

async fn request(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = if let Some(value) = body {
        builder = builder.header("content-type", "application/json");
        Body::from(serde_json::to_vec(&value).unwrap())
    } else {
        Body::empty()
    };

    app.clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap()
}

async fn response_json(response: axum::response::Response) -> serde_json::Value {
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

async fn create_widget_crd(app: &axum::Router) {
    let response = request(
        app,
        "POST",
        "/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
        Some(json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "widgets.example.com"},
            "spec": {
                "group": "example.com",
                "scope": "Namespaced",
                "names": {
                    "plural": "widgets",
                    "singular": "widget",
                    "kind": "Widget"
                },
                "versions": [{
                    "name": "v1",
                    "served": true,
                    "storage": true,
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "x-kubernetes-preserve-unknown-fields": true
                        }
                    }
                }]
            }
        })),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
}

async fn create_configmap(app: &axum::Router, name: &str, metadata: serde_json::Value) {
    let response = request(
        app,
        "POST",
        "/api/v1/namespaces/default/configmaps",
        Some(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": metadata,
            "data": {"key": name}
        })),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
}

async fn create_secret(app: &axum::Router, name: &str, metadata: serde_json::Value) {
    let response = request(
        app,
        "POST",
        "/api/v1/namespaces/default/secrets",
        Some(json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": metadata,
            "stringData": {"key": name}
        })),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
}

async fn create_pod(app: &axum::Router, name: &str, metadata: serde_json::Value) {
    let response = request(
        app,
        "POST",
        "/api/v1/namespaces/default/pods",
        Some(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": metadata,
            "spec": {
                "containers": [{
                    "name": "main",
                    "image": "registry.k8s.io/pause:3.10"
                }]
            }
        })),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED, "create pod {name}");
}

async fn create_widget(app: &axum::Router, name: &str, metadata: serde_json::Value) {
    let response = request(
        app,
        "POST",
        "/apis/example.com/v1/namespaces/default/widgets",
        Some(json!({
            "apiVersion": "example.com/v1",
            "kind": "Widget",
            "metadata": metadata,
            "spec": {"value": name}
        })),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
}

async fn get_json(app: &axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let response = request(app, "GET", uri, None).await;
    let status = response.status();
    (status, response_json(response).await)
}

fn assert_has_resource_version(value: &serde_json::Value) {
    assert!(
        value
            .pointer("/metadata/resourceVersion")
            .and_then(|rv| rv.as_str())
            .is_some_and(|rv| !rv.is_empty()),
        "response must include metadata.resourceVersion: {value:?}"
    );
}

fn assert_no_deletion_timestamp(value: &serde_json::Value) {
    assert!(
        value.pointer("/metadata/deletionTimestamp").is_none(),
        "object must not have deletionTimestamp persisted: {value:?}"
    );
}

fn item_names(list: &serde_json::Value) -> Vec<String> {
    let mut names: Vec<String> = list["items"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|item| {
            item.pointer("/metadata/name")
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
        })
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn mutation_dry_run_create_does_not_persist_generated_crd_or_pod() {
    let state = build_test_app_state().await;
    let pod_repository = state.pod_repository.clone();
    let app = crate::api::build_router(state);
    create_widget_crd(&app).await;

    let generated = request(
        &app,
        "POST",
        "/api/v1/namespaces/default/configmaps?dryRun=All",
        Some(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "dry-run-cm", "namespace": "default"},
            "data": {"key": "value"}
        })),
    )
    .await;
    assert_eq!(generated.status(), StatusCode::CREATED);
    let generated_body = response_json(generated).await;
    assert_eq!(generated_body["kind"], "ConfigMap");

    let (status, missing) =
        get_json(&app, "/api/v1/namespaces/default/configmaps/dry-run-cm").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing["kind"], "Status");

    let crd = request(
        &app,
        "POST",
        "/apis/example.com/v1/namespaces/default/widgets?dryRun=All",
        Some(json!({
            "apiVersion": "example.com/v1",
            "kind": "Widget",
            "metadata": {"name": "dry-run-widget", "namespace": "default"},
            "spec": {"value": "dry"}
        })),
    )
    .await;
    assert_eq!(crd.status(), StatusCode::CREATED);
    let crd_body = response_json(crd).await;
    assert_eq!(crd_body["kind"], "Widget");

    let (status, missing) = get_json(
        &app,
        "/apis/example.com/v1/namespaces/default/widgets/dry-run-widget",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing["kind"], "Status");

    let pod = request(
        &app,
        "POST",
        "/api/v1/namespaces/default/pods?dryRun=All",
        Some(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "dry-run-pod", "namespace": "default"},
            "spec": {
                "containers": [{
                    "name": "main",
                    "image": "registry.k8s.io/pause:3.10"
                }]
            }
        })),
    )
    .await;
    assert_eq!(pod.status(), StatusCode::CREATED);
    let pod_body = response_json(pod).await;
    assert_eq!(pod_body["kind"], "Pod");

    let persisted = crate::kubelet::pod_repository::PodReader::get_pod(
        pod_repository.as_ref(),
        "default",
        "dry-run-pod",
    )
    .await
    .unwrap();
    assert!(
        persisted.is_none(),
        "Pod dry-run create must not persist the Pod row"
    );
}

#[tokio::test]
async fn mutation_delete_dry_run_does_not_mark_or_remove_generated_crd_or_pod() {
    let state = build_test_app_state().await;
    let pod_repository = state.pod_repository.clone();
    let app = crate::api::build_router(state);
    create_widget_crd(&app).await;

    create_secret(
        &app,
        "dry-run-secret",
        json!({"name": "dry-run-secret", "namespace": "default"}),
    )
    .await;
    create_widget(
        &app,
        "dry-run-widget",
        json!({"name": "dry-run-widget", "namespace": "default"}),
    )
    .await;
    create_pod(
        &app,
        "dry-run-pod",
        json!({"name": "dry-run-pod", "namespace": "default"}),
    )
    .await;

    let generated = request(
        &app,
        "DELETE",
        "/api/v1/namespaces/default/secrets/dry-run-secret?dryRun=All",
        None,
    )
    .await;
    assert_eq!(generated.status(), StatusCode::OK);
    let generated_body = response_json(generated).await;
    assert_eq!(generated_body["kind"], "Secret");
    assert!(
        generated_body
            .pointer("/metadata/deletionTimestamp")
            .is_some()
    );
    assert_has_resource_version(&generated_body);

    let (status, live_secret) =
        get_json(&app, "/api/v1/namespaces/default/secrets/dry-run-secret").await;
    assert_eq!(status, StatusCode::OK);
    assert_no_deletion_timestamp(&live_secret);

    let crd = request(
        &app,
        "DELETE",
        "/apis/example.com/v1/namespaces/default/widgets/dry-run-widget?dryRun=All",
        None,
    )
    .await;
    assert_eq!(crd.status(), StatusCode::OK);
    let crd_body = response_json(crd).await;
    assert_eq!(crd_body["kind"], "Status");
    assert_eq!(crd_body["status"], "Success");

    let (status, live_widget) = get_json(
        &app,
        "/apis/example.com/v1/namespaces/default/widgets/dry-run-widget",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_no_deletion_timestamp(&live_widget);

    let pod = request(
        &app,
        "DELETE",
        "/api/v1/namespaces/default/pods/dry-run-pod?dryRun=All",
        None,
    )
    .await;
    assert_eq!(pod.status(), StatusCode::OK);
    let pod_body = response_json(pod).await;
    assert_eq!(pod_body["kind"], "Pod");
    assert!(pod_body.pointer("/metadata/deletionTimestamp").is_some());
    assert_has_resource_version(&pod_body);

    let live_pod = crate::kubelet::pod_repository::PodReader::get_pod(
        pod_repository.as_ref(),
        "default",
        "dry-run-pod",
    )
    .await
    .unwrap()
    .expect("dry-run delete must leave the Pod live");
    assert_no_deletion_timestamp(&live_pod.data);
}

#[tokio::test]
async fn mutation_delete_returns_accepted_when_resource_is_retained() {
    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);
    create_widget_crd(&app).await;

    create_configmap(
        &app,
        "held-cm",
        json!({
            "name": "held-cm",
            "namespace": "default",
            "finalizers": ["example.com/hold"]
        }),
    )
    .await;
    create_widget(
        &app,
        "held-widget",
        json!({
            "name": "held-widget",
            "namespace": "default",
            "finalizers": ["example.com/hold"]
        }),
    )
    .await;
    create_pod(
        &app,
        "held-pod",
        json!({"name": "held-pod", "namespace": "default"}),
    )
    .await;

    let generated = request(
        &app,
        "DELETE",
        "/api/v1/namespaces/default/configmaps/held-cm",
        None,
    )
    .await;
    assert_eq!(generated.status(), StatusCode::ACCEPTED);
    let generated_body = response_json(generated).await;
    assert_eq!(generated_body["kind"], "ConfigMap");
    assert!(
        generated_body
            .pointer("/metadata/deletionTimestamp")
            .is_some()
    );
    assert_has_resource_version(&generated_body);
    assert!(
        db.get_resource("v1", "ConfigMap", Some("default"), "held-cm")
            .await
            .unwrap()
            .expect("finalizer-held ConfigMap must remain")
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some()
    );

    let crd = request(
        &app,
        "DELETE",
        "/apis/example.com/v1/namespaces/default/widgets/held-widget",
        None,
    )
    .await;
    assert_eq!(crd.status(), StatusCode::ACCEPTED);
    let crd_body = response_json(crd).await;
    assert_eq!(crd_body["kind"], "Widget");
    assert!(crd_body.pointer("/metadata/deletionTimestamp").is_some());
    assert_has_resource_version(&crd_body);
    assert!(
        db.get_resource("example.com/v1", "Widget", Some("default"), "held-widget")
            .await
            .unwrap()
            .expect("finalizer-held custom resource must remain")
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some()
    );

    let pod = request(
        &app,
        "DELETE",
        "/api/v1/namespaces/default/pods/held-pod",
        None,
    )
    .await;
    assert_eq!(pod.status(), StatusCode::ACCEPTED);
    let pod_body = response_json(pod).await;
    assert_eq!(pod_body["kind"], "Pod");
    assert!(pod_body.pointer("/metadata/deletionTimestamp").is_some());
    assert_has_resource_version(&pod_body);
    assert!(
        db.get_resource("v1", "Pod", Some("default"), "held-pod")
            .await
            .unwrap()
            .expect("actor-owned Pod delete must retain the row")
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some()
    );
}

#[tokio::test]
async fn mutation_deletecollection_dry_run_returns_status_without_deleting_any_matching_items() {
    let state = build_test_app_state().await;
    let pod_repository = state.pod_repository.clone();
    let app = crate::api::build_router(state);
    create_widget_crd(&app).await;

    for name in ["cm-dry-1", "cm-dry-2"] {
        create_configmap(
            &app,
            name,
            json!({
                "name": name,
                "namespace": "default",
                "labels": {"mutation": "dry-run"}
            }),
        )
        .await;
    }
    for name in ["widget-dry-1", "widget-dry-2"] {
        create_widget(
            &app,
            name,
            json!({
                "name": name,
                "namespace": "default",
                "labels": {"mutation": "dry-run"}
            }),
        )
        .await;
    }
    for name in ["pod-dry-1", "pod-dry-2"] {
        create_pod(
            &app,
            name,
            json!({
                "name": name,
                "namespace": "default",
                "labels": {"mutation": "dry-run"}
            }),
        )
        .await;
    }

    for uri in [
        "/api/v1/namespaces/default/configmaps?labelSelector=mutation%3Ddry-run&dryRun=All",
        "/apis/example.com/v1/namespaces/default/widgets?labelSelector=mutation%3Ddry-run&dryRun=All",
        "/api/v1/namespaces/default/pods?labelSelector=mutation%3Ddry-run&dryRun=All",
    ] {
        let response = request(&app, "DELETE", uri, None).await;
        assert_eq!(response.status(), StatusCode::OK, "deletecollection {uri}");
        let body = response_json(response).await;
        assert_eq!(body["kind"], "Status");
        assert_eq!(body["status"], "Success");
    }

    let (status, configmaps) = get_json(
        &app,
        "/api/v1/namespaces/default/configmaps?labelSelector=mutation%3Ddry-run",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(item_names(&configmaps), vec!["cm-dry-1", "cm-dry-2"]);

    let (status, widgets) = get_json(
        &app,
        "/apis/example.com/v1/namespaces/default/widgets?labelSelector=mutation%3Ddry-run",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(item_names(&widgets), vec!["widget-dry-1", "widget-dry-2"]);

    for name in ["pod-dry-1", "pod-dry-2"] {
        let pod = crate::kubelet::pod_repository::PodReader::get_pod(
            pod_repository.as_ref(),
            "default",
            name,
        )
        .await
        .unwrap()
        .expect("dry-run deletecollection must leave matching Pod live");
        assert_no_deletion_timestamp(&pod.data);
    }
}
