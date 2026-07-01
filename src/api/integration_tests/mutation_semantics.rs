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

async fn request_json_with_content_type(
    app: &axum::Router,
    method: &str,
    uri: &str,
    content_type: &str,
    body: serde_json::Value,
) -> axum::response::Response {
    request_raw(
        app,
        method,
        uri,
        Some(content_type),
        serde_json::to_vec(&body).unwrap(),
    )
    .await
}

async fn request_raw(
    app: &axum::Router,
    method: &str,
    uri: &str,
    content_type: Option<&str>,
    body: Vec<u8>,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(content_type) = content_type {
        builder = builder.header("content-type", content_type);
    }
    app.clone()
        .oneshot(builder.body(Body::from(body)).unwrap())
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

async fn create_deployment(app: &axum::Router, name: &str, metadata: serde_json::Value) {
    let response = request(
        app,
        "POST",
        "/apis/apps/v1/namespaces/default/deployments",
        Some(json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": metadata,
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": name}},
                "template": {
                    "metadata": {"labels": {"app": name}},
                    "spec": {
                        "containers": [{
                            "name": "main",
                            "image": "registry.k8s.io/pause:3.10"
                        }]
                    }
                }
            }
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

fn assert_finalizers_include(value: &serde_json::Value, expected: &str) {
    assert!(
        value
            .pointer("/metadata/finalizers")
            .and_then(|finalizers| finalizers.as_array())
            .is_some_and(|finalizers| finalizers
                .iter()
                .any(|finalizer| finalizer.as_str() == Some(expected))),
        "object finalizers must include {expected}: {value:?}"
    );
}

fn assert_pod_create_defaults(value: &serde_json::Value) {
    assert_eq!(value["spec"]["serviceAccountName"], "default");
    assert_eq!(value["spec"]["serviceAccount"], "default");
    assert_eq!(value["spec"]["dnsPolicy"], "ClusterFirst");
    assert_eq!(value["spec"]["schedulerName"], "default-scheduler");
    assert_eq!(value["spec"]["terminationGracePeriodSeconds"], 30);
    assert_eq!(value["metadata"]["generation"], 1);
    assert_eq!(value["status"]["phase"], "Pending");
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

#[tokio::test]
async fn mutation_dry_run_does_not_enqueue_service_or_controller_side_effects() {
    let state = build_test_app_state().await;
    state
        .db
        .create_resource(
            "v1",
            "Service",
            Some("default"),
            "dry-run-svc",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "dry-run-svc", "namespace": "default"},
                "spec": {
                    "selector": {"app": "dry-run-web"},
                    "ports": [{"port": 80, "targetPort": 8080}]
                }
            }),
        )
        .await
        .unwrap();
    let dispatcher = state.controller_dispatcher.clone();
    let app = crate::api::build_router(state);

    let response = request(
        &app,
        "POST",
        "/api/v1/namespaces/default/pods?dryRun=All",
        Some(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "dry-run-pod-side-effects",
                "namespace": "default",
                "labels": {"app": "dry-run-web"}
            },
            "spec": {
                "containers": [{
                    "name": "main",
                    "image": "registry.k8s.io/pause:3.10"
                }]
            },
            "status": {"podIP": "10.42.0.77"}
        })),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = response_json(response).await;
    assert_eq!(body["kind"], "Pod");
    assert_eq!(body["metadata"]["name"], "dry-run-pod-side-effects");

    let queued = dispatcher.queued_reconcile_keys_for_test().await;
    assert!(
        queued.is_empty(),
        "dry-run mutation must not enqueue side-effect reconcile keys: {queued:?}"
    );
}

#[tokio::test]
async fn mutation_delete_uid_precondition_conflict_is_consistent_across_paths() {
    let state = build_test_app_state().await;
    let pod_repository = state.pod_repository.clone();
    let app = crate::api::build_router(state);
    create_widget_crd(&app).await;

    create_secret(
        &app,
        "uid-secret",
        json!({"name": "uid-secret", "namespace": "default", "uid": "uid-secret-live"}),
    )
    .await;
    create_widget(
        &app,
        "uid-widget",
        json!({"name": "uid-widget", "namespace": "default", "uid": "uid-widget-live"}),
    )
    .await;
    create_pod(
        &app,
        "uid-pod",
        json!({"name": "uid-pod", "namespace": "default", "uid": "uid-pod-live"}),
    )
    .await;

    let wrong_uid_options = json!({
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "preconditions": {"uid": "wrong-uid"}
    });

    let generated = request(
        &app,
        "DELETE",
        "/api/v1/namespaces/default/secrets/uid-secret",
        Some(wrong_uid_options.clone()),
    )
    .await;
    assert_eq!(generated.status(), StatusCode::CONFLICT);
    let (status, live_secret) =
        get_json(&app, "/api/v1/namespaces/default/secrets/uid-secret").await;
    assert_eq!(status, StatusCode::OK);
    assert_no_deletion_timestamp(&live_secret);

    let crd = request(
        &app,
        "DELETE",
        "/apis/example.com/v1/namespaces/default/widgets/uid-widget",
        Some(wrong_uid_options.clone()),
    )
    .await;
    assert_eq!(crd.status(), StatusCode::CONFLICT);
    let (status, live_widget) = get_json(
        &app,
        "/apis/example.com/v1/namespaces/default/widgets/uid-widget",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_no_deletion_timestamp(&live_widget);

    let pod = request(
        &app,
        "DELETE",
        "/api/v1/namespaces/default/pods/uid-pod",
        Some(wrong_uid_options),
    )
    .await;
    assert_eq!(pod.status(), StatusCode::CONFLICT);
    let live_pod = crate::kubelet::pod_repository::PodReader::get_pod(
        pod_repository.as_ref(),
        "default",
        "uid-pod",
    )
    .await
    .unwrap()
    .expect("UID precondition conflict must leave Pod live");
    assert_no_deletion_timestamp(&live_pod.data);
}

#[tokio::test]
async fn mutation_delete_foreground_adds_finalizer_without_hard_deleting_non_pod_resources() {
    let state = build_test_app_state().await;
    let db = state.db.clone();
    let pod_repository = state.pod_repository.clone();
    let app = crate::api::build_router(state);
    create_widget_crd(&app).await;

    create_configmap(
        &app,
        "fg-cm",
        json!({
            "name": "fg-cm",
            "namespace": "default",
            "uid": "fg-cm-live",
            "finalizers": ["example.com/hold"]
        }),
    )
    .await;
    create_widget(
        &app,
        "fg-widget",
        json!({
            "name": "fg-widget",
            "namespace": "default",
            "uid": "fg-widget-live",
            "finalizers": ["example.com/hold"]
        }),
    )
    .await;
    create_pod(
        &app,
        "fg-pod",
        json!({"name": "fg-pod", "namespace": "default", "uid": "fg-pod-live"}),
    )
    .await;
    create_secret(
        &app,
        "fg-cm-child",
        json!({
            "name": "fg-cm-child",
            "namespace": "default",
            "finalizers": ["example.com/child-hold"],
            "ownerReferences": [{
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "name": "fg-cm",
                "uid": "fg-cm-live",
                "blockOwnerDeletion": true
            }]
        }),
    )
    .await;
    create_configmap(
        &app,
        "fg-widget-child",
        json!({
            "name": "fg-widget-child",
            "namespace": "default",
            "finalizers": ["example.com/child-hold"],
            "ownerReferences": [{
                "apiVersion": "example.com/v1",
                "kind": "Widget",
                "name": "fg-widget",
                "uid": "fg-widget-live",
                "blockOwnerDeletion": true
            }]
        }),
    )
    .await;

    let generated = request(
        &app,
        "DELETE",
        "/api/v1/namespaces/default/configmaps/fg-cm",
        Some(json!({
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "propagationPolicy": "Foreground",
            "preconditions": {"uid": "fg-cm-live"}
        })),
    )
    .await;
    assert_eq!(generated.status(), StatusCode::ACCEPTED);
    let generated_body = response_json(generated).await;
    assert_finalizers_include(&generated_body, "foregroundDeletion");
    let persisted_cm = db
        .get_resource("v1", "ConfigMap", Some("default"), "fg-cm")
        .await
        .unwrap()
        .expect("foreground ConfigMap delete must retain the row");
    assert!(
        persisted_cm
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some()
    );
    assert_finalizers_include(&persisted_cm.data, "foregroundDeletion");

    let crd = request(
        &app,
        "DELETE",
        "/apis/example.com/v1/namespaces/default/widgets/fg-widget",
        Some(json!({
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "propagationPolicy": "Foreground",
            "preconditions": {"uid": "fg-widget-live"}
        })),
    )
    .await;
    assert_eq!(crd.status(), StatusCode::ACCEPTED);
    let crd_body = response_json(crd).await;
    assert_finalizers_include(&crd_body, "foregroundDeletion");
    let persisted_widget = db
        .get_resource("example.com/v1", "Widget", Some("default"), "fg-widget")
        .await
        .unwrap()
        .expect("foreground custom resource delete must retain the row");
    assert!(
        persisted_widget
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some()
    );
    assert_finalizers_include(&persisted_widget.data, "foregroundDeletion");

    let pod = request(
        &app,
        "DELETE",
        "/api/v1/namespaces/default/pods/fg-pod",
        Some(json!({
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "propagationPolicy": "Foreground",
            "preconditions": {"uid": "fg-pod-live"}
        })),
    )
    .await;
    assert_eq!(pod.status(), StatusCode::ACCEPTED);
    let live_pod = crate::kubelet::pod_repository::PodReader::get_pod(
        pod_repository.as_ref(),
        "default",
        "fg-pod",
    )
    .await
    .unwrap()
    .expect("foreground Pod delete must leave actor-owned row");
    assert!(
        live_pod
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some()
    );
}

#[tokio::test]
async fn mutation_create_defaults_are_persisted_for_json_and_protobuf_pod_paths() {
    let state = build_test_app_state().await;
    let pod_repository = state.pod_repository.clone();
    let app = crate::api::build_router(state);

    let json_response = request(
        &app,
        "POST",
        "/api/v1/namespaces/default/pods",
        Some(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "defaults-json-pod", "namespace": "default"},
            "spec": {
                "containers": [{
                    "name": "main",
                    "image": "registry.k8s.io/pause:3.10"
                }]
            }
        })),
    )
    .await;
    assert_eq!(json_response.status(), StatusCode::CREATED);
    let json_body = response_json(json_response).await;
    assert_pod_create_defaults(&json_body);

    let persisted_json = crate::kubelet::pod_repository::PodReader::get_pod(
        pod_repository.as_ref(),
        "default",
        "defaults-json-pod",
    )
    .await
    .unwrap()
    .expect("JSON-created Pod must persist");
    assert_pod_create_defaults(&persisted_json.data);

    let protobuf_pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "defaults-protobuf-pod", "namespace": "default"},
        "spec": {
            "containers": [{
                "name": "main",
                "image": "registry.k8s.io/pause:3.10"
            }]
        }
    });
    let protobuf_body = crate::protobuf::encode_protobuf(&protobuf_pod).unwrap();
    let protobuf_response = request_raw(
        &app,
        "POST",
        "/api/v1/namespaces/default/pods",
        Some("application/vnd.kubernetes.protobuf"),
        protobuf_body,
    )
    .await;
    assert_eq!(protobuf_response.status(), StatusCode::CREATED);
    let protobuf_created = response_json(protobuf_response).await;
    assert_pod_create_defaults(&protobuf_created);

    let persisted_protobuf = crate::kubelet::pod_repository::PodReader::get_pod(
        pod_repository.as_ref(),
        "default",
        "defaults-protobuf-pod",
    )
    .await
    .unwrap()
    .expect("protobuf-created Pod must persist");
    assert_pod_create_defaults(&persisted_protobuf.data);
}

#[tokio::test]
async fn mutation_update_bumps_generation_only_when_spec_changes_for_generated_and_crd_paths() {
    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);
    create_widget_crd(&app).await;

    create_configmap(
        &app,
        "gen-cm",
        json!({"name": "gen-cm", "namespace": "default"}),
    )
    .await;
    let (status, mut cm) = get_json(&app, "/api/v1/namespaces/default/configmaps/gen-cm").await;
    assert_eq!(status, StatusCode::OK);
    let cm_generation = cm["metadata"]["generation"].clone();
    cm["data"] = json!({"key": "changed"});
    let cm_update = request(
        &app,
        "PUT",
        "/api/v1/namespaces/default/configmaps/gen-cm",
        Some(cm),
    )
    .await;
    assert_eq!(cm_update.status(), StatusCode::OK);
    let cm_body = response_json(cm_update).await;
    assert_eq!(
        cm_body["metadata"]["generation"], cm_generation,
        "ConfigMap data updates must not bump generation"
    );

    create_deployment(
        &app,
        "gen-deploy",
        json!({"name": "gen-deploy", "namespace": "default"}),
    )
    .await;
    let (status, mut deployment) = get_json(
        &app,
        "/apis/apps/v1/namespaces/default/deployments/gen-deploy",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let deployment_generation = deployment["metadata"]["generation"].as_i64().unwrap();
    deployment["spec"]["replicas"] = json!(2);
    let deployment_update = request(
        &app,
        "PUT",
        "/apis/apps/v1/namespaces/default/deployments/gen-deploy",
        Some(deployment),
    )
    .await;
    assert_eq!(deployment_update.status(), StatusCode::OK);
    let deployment_body = response_json(deployment_update).await;
    assert_eq!(
        deployment_body["metadata"]["generation"].as_i64(),
        Some(deployment_generation + 1),
        "Deployment spec updates must bump generation once"
    );

    create_widget(
        &app,
        "gen-widget",
        json!({"name": "gen-widget", "namespace": "default"}),
    )
    .await;
    let (status, mut widget) = get_json(
        &app,
        "/apis/example.com/v1/namespaces/default/widgets/gen-widget",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let widget_generation = widget
        .pointer("/metadata/generation")
        .and_then(|value| value.as_i64())
        .unwrap_or(1);
    widget["spec"] = json!({"value": "changed"});
    let widget_update = request(
        &app,
        "PUT",
        "/apis/example.com/v1/namespaces/default/widgets/gen-widget",
        Some(widget),
    )
    .await;
    assert_eq!(widget_update.status(), StatusCode::OK);
    let widget_body = response_json(widget_update).await;
    assert_eq!(
        widget_body["metadata"]["generation"].as_i64(),
        Some(widget_generation + 1),
        "custom resource spec updates must bump generation once"
    );
}

#[tokio::test]
async fn mutation_patch_preserves_deletion_timestamp_and_generation_rules() {
    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);
    create_widget_crd(&app).await;

    create_deployment(
        &app,
        "patch-deploy",
        json!({
            "name": "patch-deploy",
            "namespace": "default",
            "uid": "patch-deploy-uid",
            "finalizers": ["example.com/hold"]
        }),
    )
    .await;
    let delete_deployment = request(
        &app,
        "DELETE",
        "/apis/apps/v1/namespaces/default/deployments/patch-deploy",
        None,
    )
    .await;
    assert_eq!(delete_deployment.status(), StatusCode::ACCEPTED);
    let delete_deployment_body = response_json(delete_deployment).await;
    let deployment_deletion_timestamp =
        delete_deployment_body["metadata"]["deletionTimestamp"].clone();
    let deployment_generation = delete_deployment_body["metadata"]["generation"]
        .as_i64()
        .unwrap();
    let deployment_patch = request_json_with_content_type(
        &app,
        "PATCH",
        "/apis/apps/v1/namespaces/default/deployments/patch-deploy",
        "application/merge-patch+json",
        json!({"spec": {"replicas": 3}}),
    )
    .await;
    assert_eq!(deployment_patch.status(), StatusCode::OK);
    let deployment_body = response_json(deployment_patch).await;
    assert_eq!(
        deployment_body["metadata"]["deletionTimestamp"],
        deployment_deletion_timestamp
    );
    assert_eq!(
        deployment_body["metadata"]["generation"].as_i64(),
        Some(deployment_generation + 1)
    );

    create_widget(
        &app,
        "patch-widget",
        json!({
            "name": "patch-widget",
            "namespace": "default",
            "uid": "patch-widget-uid",
            "finalizers": ["example.com/hold"]
        }),
    )
    .await;
    let delete_widget = request(
        &app,
        "DELETE",
        "/apis/example.com/v1/namespaces/default/widgets/patch-widget",
        None,
    )
    .await;
    assert_eq!(delete_widget.status(), StatusCode::ACCEPTED);
    let delete_widget_body = response_json(delete_widget).await;
    let widget_deletion_timestamp = delete_widget_body["metadata"]["deletionTimestamp"].clone();
    let widget_generation = delete_widget_body
        .pointer("/metadata/generation")
        .and_then(|value| value.as_i64())
        .unwrap_or(1);
    let widget_patch = request_json_with_content_type(
        &app,
        "PATCH",
        "/apis/example.com/v1/namespaces/default/widgets/patch-widget",
        "application/merge-patch+json",
        json!({"spec": {"value": "patched"}}),
    )
    .await;
    assert_eq!(widget_patch.status(), StatusCode::OK);
    let widget_body = response_json(widget_patch).await;
    assert_eq!(
        widget_body["metadata"]["deletionTimestamp"],
        widget_deletion_timestamp
    );
    assert_eq!(
        widget_body["metadata"]["generation"].as_i64(),
        Some(widget_generation + 1)
    );
}
