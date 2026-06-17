use super::*;

#[tokio::test]
async fn test_patch_crd_apply_yaml_create_registers_route_for_duplicate_field_detection() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use hyper::header::CONTENT_TYPE;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({
            "apiVersion":"v1",
            "kind":"Namespace",
            "metadata":{"name":"default"}
        }),
    )
    .await
    .unwrap();

    let crd_apply_yaml = r#"
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: foorvcdla.example.com
spec:
  group: example.com
  scope: Namespaced
  names:
    kind: Foo
    plural: foorvcdla
    singular: foo
  versions:
  - name: v1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
        properties:
          spec:
            type: object
            x-kubernetes-preserve-unknown-fields: true
"#;

    let patch_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(
                    "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/foorvcdla.example.com",
                )
                .header(CONTENT_TYPE, "application/apply-patch+yaml")
                .body(Body::from(crd_apply_yaml))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        patch_crd.status().is_success(),
        "CRD apply PATCH should create/register CRD, got {}",
        patch_crd.status()
    );

    let duplicate_apply_yaml = r#"
apiVersion: example.com/v1
kind: Foo
metadata:
  name: mytest
spec:
  foo: foo1
  foo: foo2
"#;

    let patch_cr = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/apis/example.com/v1/namespaces/default/foorvcdla/mytest?fieldValidation=Strict")
                .header(CONTENT_TYPE, "application/apply-patch+yaml")
                .body(Body::from(duplicate_apply_yaml))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_cr.status(), StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(patch_cr.into_body(), usize::MAX)
        .await
        .unwrap();
    let msg = String::from_utf8_lossy(&bytes);
    assert!(
        msg.contains("already set in map"),
        "expected duplicate field parser error, got: {}",
        msg
    );
    assert!(
        !msg.contains("not found"),
        "custom resource route must be available after CRD apply PATCH create: {}",
        msg
    );
}

#[tokio::test]
async fn test_apis_includes_scheduling_k8s_io_group() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let req = Request::builder()
        .method("GET")
        .uri("/apis")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(body["kind"], "APIGroupList");
    let groups = body["groups"].as_array().unwrap();
    let group_names: Vec<&str> = groups.iter().filter_map(|g| g["name"].as_str()).collect();

    // scheduling.k8s.io must appear in /apis
    assert!(
        group_names.contains(&"scheduling.k8s.io"),
        "scheduling.k8s.io missing from /apis groups: {:?}",
        group_names
    );
}

#[tokio::test]
async fn test_apis_scheduling_group_discovery_endpoint() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let req = Request::builder()
        .method("GET")
        .uri("/apis/scheduling.k8s.io")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(body["name"], "scheduling.k8s.io");
    assert_eq!(
        body["preferredVersion"]["groupVersion"],
        "scheduling.k8s.io/v1"
    );
    assert_eq!(body["versions"][0]["groupVersion"], "scheduling.k8s.io/v1");
    assert_eq!(body["versions"][0]["version"], "v1");
}

#[tokio::test]
async fn test_metrics_k8s_io_discovery_advertises_node_and_pod_metrics() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let group_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/metrics.k8s.io")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(group_resp.status(), StatusCode::OK);
    let group_body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(group_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(group_body["name"], "metrics.k8s.io");
    assert_eq!(
        group_body["preferredVersion"]["groupVersion"],
        "metrics.k8s.io/v1beta1"
    );

    let resources_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/metrics.k8s.io/v1beta1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resources_resp.status(), StatusCode::OK);
    let resources_body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(resources_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(resources_body["groupVersion"], "metrics.k8s.io/v1beta1");
    let resources = resources_body["resources"].as_array().unwrap();
    assert!(resources.iter().any(|r| {
        r["name"] == "nodes"
            && r["kind"] == "NodeMetrics"
            && r["namespaced"] == false
            && r["verbs"] == json!(["get", "list"])
    }));
    assert!(resources.iter().any(|r| {
        r["name"] == "pods"
            && r["kind"] == "PodMetrics"
            && r["namespaced"] == true
            && r["verbs"] == json!(["get", "list"])
    }));

    let aggregated_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis")
                .header(
                    "accept",
                    "application/json;v=v2;g=apidiscovery.k8s.io;as=APIGroupDiscoveryList",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(aggregated_resp.status(), StatusCode::OK);
    let aggregated_body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(aggregated_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let metrics_group = aggregated_body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["metadata"]["name"] == "metrics.k8s.io")
        .expect("metrics.k8s.io missing from aggregated discovery");
    let resources = metrics_group["versions"][0]["resources"]
        .as_array()
        .unwrap();
    assert!(resources.iter().any(|r| {
        r["resource"] == "nodes"
            && r["responseKind"]["kind"] == "NodeMetrics"
            && r["scope"] == "Cluster"
    }));
    assert!(resources.iter().any(|r| {
        r["resource"] == "pods"
            && r["responseKind"]["kind"] == "PodMetrics"
            && r["scope"] == "Namespaced"
    }));
}

#[tokio::test]
async fn test_metrics_k8s_io_lists_and_gets_metrics_for_existing_nodes_and_pods() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let (app, db) = build_test_router_with_db().await;
    db.create_resource(
        "v1",
        "Node",
        None,
        "node-a",
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "node-a"}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "metrics-ns",
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "metrics-ns"}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("metrics-ns"),
        "pod-a",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "pod-a", "namespace": "metrics-ns"},
            "spec": {
                "nodeName": "node-a",
                "containers": [
                    {"name": "app", "image": "busybox"},
                    {"name": "sidecar", "image": "busybox"}
                ]
            }
        }),
    )
    .await
    .unwrap();

    let node_list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/metrics.k8s.io/v1beta1/nodes")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(node_list_resp.status(), StatusCode::OK);
    let node_list: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(node_list_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(node_list["apiVersion"], "metrics.k8s.io/v1beta1");
    assert_eq!(node_list["kind"], "NodeMetricsList");
    assert_eq!(node_list["items"][0]["kind"], "NodeMetrics");
    assert_eq!(node_list["items"][0]["metadata"]["name"], "node-a");
    assert!(node_list["items"][0]["timestamp"].as_str().is_some());
    assert_eq!(node_list["items"][0]["window"], "30s");
    assert_eq!(node_list["items"][0]["usage"]["cpu"], "0");
    assert_eq!(node_list["items"][0]["usage"]["memory"], "0");

    let node_get_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/metrics.k8s.io/v1beta1/nodes/node-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(node_get_resp.status(), StatusCode::OK);
    let node_get: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(node_get_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(node_get["kind"], "NodeMetrics");
    assert_eq!(node_get["metadata"]["name"], "node-a");

    let pod_list_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/metrics.k8s.io/v1beta1/namespaces/metrics-ns/pods")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pod_list_resp.status(), StatusCode::OK);
    let pod_list: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(pod_list_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(pod_list["apiVersion"], "metrics.k8s.io/v1beta1");
    assert_eq!(pod_list["kind"], "PodMetricsList");
    assert_eq!(pod_list["items"][0]["kind"], "PodMetrics");
    assert_eq!(pod_list["items"][0]["metadata"]["namespace"], "metrics-ns");
    assert_eq!(pod_list["items"][0]["metadata"]["name"], "pod-a");
    assert_eq!(pod_list["items"][0]["containers"][0]["name"], "app");
    assert_eq!(pod_list["items"][0]["containers"][1]["name"], "sidecar");
    assert_eq!(pod_list["items"][0]["containers"][0]["usage"]["cpu"], "0");
    assert_eq!(
        pod_list["items"][0]["containers"][0]["usage"]["memory"],
        "0"
    );

    let pod_get_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/metrics.k8s.io/v1beta1/namespaces/metrics-ns/pods/pod-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pod_get_resp.status(), StatusCode::OK);
    let pod_get: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(pod_get_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(pod_get["kind"], "PodMetrics");
    assert_eq!(pod_get["metadata"]["name"], "pod-a");
    assert_eq!(pod_get["containers"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn test_priorityclass_value_immutable_on_patch_and_update() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use hyper::header::CONTENT_TYPE;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let create = Request::builder()
        .method("POST")
        .uri("/apis/scheduling.k8s.io/v1/priorityclasses")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"apiVersion":"scheduling.k8s.io/v1","kind":"PriorityClass","metadata":{"name":"pc-immutable"},"value":10,"description":"initial"}"#,
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);

    let patch = Request::builder()
        .method("PATCH")
        .uri("/apis/scheduling.k8s.io/v1/priorityclasses/pc-immutable")
        .header(CONTENT_TYPE, "application/strategic-merge-patch+json")
        .body(Body::from(r#"{"value":100}"#))
        .unwrap();
    let patch_resp = app.clone().oneshot(patch).await.unwrap();
    assert_eq!(patch_resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let update = Request::builder()
        .method("PUT")
        .uri("/apis/scheduling.k8s.io/v1/priorityclasses/pc-immutable")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"apiVersion":"scheduling.k8s.io/v1","kind":"PriorityClass","metadata":{"name":"pc-immutable"},"value":100,"description":"updated"}"#,
        ))
        .unwrap();
    let update_resp = app.clone().oneshot(update).await.unwrap();
    assert_eq!(update_resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let preemption_patch = Request::builder()
        .method("PATCH")
        .uri("/apis/scheduling.k8s.io/v1/priorityclasses/pc-immutable")
        .header(CONTENT_TYPE, "application/strategic-merge-patch+json")
        .body(Body::from(r#"{"preemptionPolicy":"Never"}"#))
        .unwrap();
    let preemption_patch_resp = app.clone().oneshot(preemption_patch).await.unwrap();
    assert_eq!(
        preemption_patch_resp.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );

    let preemption_update = Request::builder()
        .method("PUT")
        .uri("/apis/scheduling.k8s.io/v1/priorityclasses/pc-immutable")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"apiVersion":"scheduling.k8s.io/v1","kind":"PriorityClass","metadata":{"name":"pc-immutable"},"value":10,"preemptionPolicy":"Never","description":"updated"}"#,
        ))
        .unwrap();
    let preemption_update_resp = app.clone().oneshot(preemption_update).await.unwrap();
    assert_eq!(
        preemption_update_resp.status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );

    let desc_patch = Request::builder()
        .method("PATCH")
        .uri("/apis/scheduling.k8s.io/v1/priorityclasses/pc-immutable")
        .header(CONTENT_TYPE, "application/strategic-merge-patch+json")
        .body(Body::from(r#"{"description":"updated"}"#))
        .unwrap();
    let desc_resp = app.oneshot(desc_patch).await.unwrap();
    assert_eq!(desc_resp.status(), StatusCode::OK);
}

/// Helper to fetch aggregated discovery body from /apis with the K8s aggregated-discovery Accept header.
async fn fetch_aggregated_discovery_body() -> serde_json::Value {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/apis")
        .header(
            "Accept",
            "application/json;v=v2;g=apidiscovery.k8s.io;as=APIGroupDiscoveryList",
        )
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body_bytes).unwrap()
}

/// Helper to find resources for a specific group+version in the aggregated discovery response.
fn aggregated_resources_for(body: &serde_json::Value, group: &str, version: &str) -> Vec<String> {
    let items = body["items"].as_array().unwrap();
    for item in items {
        if item["metadata"]["name"].as_str() == Some(group) {
            let versions = item["versions"].as_array().unwrap();
            for v in versions {
                if v["version"].as_str() == Some(version) {
                    return v["resources"]
                        .as_array()
                        .unwrap_or(&vec![])
                        .iter()
                        .filter_map(|r| r["resource"].as_str().map(str::to_string))
                        .collect();
                }
            }
        }
    }
    vec![]
}

#[tokio::test]
async fn test_aggregated_discovery_storage_v1_includes_csinodes() {
    let body = fetch_aggregated_discovery_body().await;
    let resources = aggregated_resources_for(&body, "storage.k8s.io", "v1");
    assert!(
        resources.contains(&"csinodes".to_string()),
        "storage.k8s.io/v1 aggregated discovery must include csinodes, got: {:?}",
        resources
    );
}

#[tokio::test]
async fn test_aggregated_discovery_storage_v1_includes_volumeattachments() {
    let body = fetch_aggregated_discovery_body().await;
    let resources = aggregated_resources_for(&body, "storage.k8s.io", "v1");
    assert!(
        resources.contains(&"volumeattachments".to_string()),
        "storage.k8s.io/v1 aggregated discovery must include volumeattachments, got: {:?}",
        resources
    );
}

#[tokio::test]
async fn test_aggregated_discovery_storage_v1_includes_csistoragecapacities() {
    let body = fetch_aggregated_discovery_body().await;
    let resources = aggregated_resources_for(&body, "storage.k8s.io", "v1");
    assert!(
        resources.contains(&"csistoragecapacities".to_string()),
        "storage.k8s.io/v1 aggregated discovery must include csistoragecapacities, got: {:?}",
        resources
    );
}

#[tokio::test]
async fn test_storage_v1_standard_discovery_includes_csinodes() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let app = crate::api::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/apis/storage.k8s.io/v1")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    let resources = body["resources"]
        .as_array()
        .expect("resources must be an array");
    let names: Vec<&str> = resources
        .iter()
        .filter_map(|r| r["name"].as_str())
        .collect();
    assert!(
        names.contains(&"csinodes"),
        "/apis/storage.k8s.io/v1 must include csinodes, got: {:?}",
        names
    );
}

#[tokio::test]
async fn test_aggregated_discovery_returns_apigroupdiscoverylist() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Send GET /apis with the aggregated discovery Accept header
    let req = Request::builder()
        .method("GET")
        .uri("/apis")
        .header(
            "Accept",
            "application/json;v=v2;g=apidiscovery.k8s.io;as=APIGroupDiscoveryList",
        )
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Response Content-Type must indicate aggregated discovery format
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("apidiscovery.k8s.io"),
        "Content-Type must contain apidiscovery.k8s.io, got: {}",
        ct
    );

    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(body["apiVersion"], "apidiscovery.k8s.io/v2");
    assert_eq!(body["kind"], "APIGroupDiscoveryList");

    let items = body["items"].as_array().unwrap();
    let group_names: Vec<&str> = items
        .iter()
        .filter_map(|g| g["metadata"]["name"].as_str())
        .collect();

    // Known groups must be present
    assert!(
        group_names.contains(&"apps"),
        "apps missing from items: {:?}",
        group_names
    );
    assert!(
        group_names.contains(&"batch"),
        "batch missing from items: {:?}",
        group_names
    );
    assert!(
        group_names.contains(&"rbac.authorization.k8s.io"),
        "rbac.authorization.k8s.io missing from items: {:?}",
        group_names
    );

    // Each group must have at least one version with resources
    let apps = items
        .iter()
        .find(|g| g["metadata"]["name"] == "apps")
        .unwrap();
    let versions = apps["versions"].as_array().unwrap();
    assert!(!versions.is_empty(), "apps must have at least one version");
    assert!(
        versions[0]["version"].as_str().is_some(),
        "apps version must have 'version' field"
    );
    let resources = versions[0]["resources"].as_array().unwrap();
    assert!(
        !resources.is_empty(),
        "apps/v1 must have at least one resource"
    );
    // Deployments must appear
    let has_deployments = resources.iter().any(|r| r["resource"] == "deployments");
    assert!(has_deployments, "apps/v1 must include deployments resource");
}

#[tokio::test]
async fn test_aggregated_discovery_includes_crd_resources() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;

    // Register a CRD in the registry
    state
        .crd_registry
        .register(crate::controllers::crd::CrdResourceInfo {
            group: "stable.example.com".to_string(),
            version: "v1".to_string(),
            kind: "CronTab".to_string(),
            plural: "crontabs".to_string(),
            singular: "crontab".to_string(),
            namespaced: true,
            selectable_fields: Vec::new(),
        })
        .await;

    let app = crate::api::build_router(state);

    let req = Request::builder()
        .method("GET")
        .uri("/apis")
        .header(
            "Accept",
            "application/json;v=v2;g=apidiscovery.k8s.io;as=APIGroupDiscoveryList",
        )
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(body["kind"], "APIGroupDiscoveryList");

    let items = body["items"].as_array().unwrap();

    // Find the CRD group
    let crd_group = items
        .iter()
        .find(|g| g["metadata"]["name"] == "stable.example.com");
    assert!(
        crd_group.is_some(),
        "CRD group stable.example.com must appear in aggregated discovery, got groups: {:?}",
        items
            .iter()
            .filter_map(|g| g["metadata"]["name"].as_str())
            .collect::<Vec<_>>()
    );

    let crd_group = crd_group.unwrap();
    let versions = crd_group["versions"].as_array().unwrap();
    assert!(!versions.is_empty());

    let resources = versions[0]["resources"].as_array().unwrap();
    assert!(
        !resources.is_empty(),
        "CRD group must have resources in aggregated discovery"
    );

    let crontab = resources.iter().find(|r| r["resource"] == "crontabs");
    assert!(crontab.is_some(), "crontabs resource must appear");

    let crontab = crontab.unwrap();
    assert_eq!(crontab["responseKind"]["kind"], "CronTab");
    assert_eq!(crontab["scope"], "Namespaced");
    assert_eq!(crontab["singularResource"], "crontab");
}

#[tokio::test]
async fn test_aggregated_discovery_includes_crd_synced_from_datastore() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let crd_registry = state.crd_registry.clone();

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "testcrds.group-sync.example.com"},
        "spec": {
            "group": "group-sync.example.com",
            "scope": "Namespaced",
            "names": {
                "kind": "testcrd",
                "plural": "testcrds",
                "singular": "testcrd",
                "listKind": "testcrdList"
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
    });

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "testcrds.group-sync.example.com",
        crd,
    )
    .await
    .unwrap();
    crate::controllers::crd::sync_registry_from_datastore(db.as_ref(), &crd_registry)
        .await
        .unwrap();

    let app = crate::api::build_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis")
                .header(
                    "Accept",
                    "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    let items = body["items"].as_array().unwrap();
    let crd_group = items
        .iter()
        .find(|g| g["metadata"]["name"] == "group-sync.example.com")
        .expect("synced CRD group must appear in aggregated discovery");
    let version = crd_group["versions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["version"] == "v1")
        .expect("synced CRD version must appear in aggregated discovery");
    let resources = version["resources"].as_array().unwrap();
    assert!(
        resources.iter().any(|r| r["resource"] == "testcrds"),
        "synced CRD plural must appear in aggregated discovery resources: {resources:?}"
    );
}

#[tokio::test]
async fn test_apis_without_aggregated_header_returns_apigrouplist() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    // No special Accept header → standard APIGroupList
    let req = Request::builder()
        .method("GET")
        .uri("/apis")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(body["kind"], "APIGroupList");
    assert_eq!(body["apiVersion"], "v1");
    // Standard format uses top-level "groups" array
    assert!(
        body["groups"].is_array(),
        "standard /apis must return groups array"
    );
}

/// P0-S13-6: client-go's downloadAPIs sends a comma-separated Accept header with multiple
/// media-types. Our server must recognise the v2 aggregated-discovery type even when it appears
/// as the first entry in a comma-separated list (e.g.
/// "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,application/json;...").
/// If the server ignores that header and returns a plain APIGroupList, client-go's
/// GroupsAndMaybeResources() sets resourcesByGV=nil and every isGVRPresent() check fails,
/// which is what aggregated_discovery.go:282 observed.
#[tokio::test]
async fn test_aggregated_discovery_with_clientgo_accept_header_includes_validatingwebhookconfigurations()
 {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    // This is the exact Accept header that client-go's downloadAPIs() sends when calling
    // GroupsAndMaybeResources() — a comma-separated list of preferred media types.
    let clientgo_accept = "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,\
         application/json;g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList,\
         application/json";

    let req = Request::builder()
        .method("GET")
        .uri("/apis")
        .header("Accept", clientgo_accept)
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Server MUST respond with the aggregated Content-Type so client-go routes through
    // SplitGroupsAndResources — without it resourcesByGV is nil and all GVR lookups fail.
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("apidiscovery.k8s.io"),
        "Content-Type must contain apidiscovery.k8s.io when comma-separated Accept includes v2, got: {}",
        ct
    );

    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(body["kind"], "APIGroupDiscoveryList");

    // Find admissionregistration.k8s.io group and verify validatingwebhookconfigurations is listed.
    let items = body["items"].as_array().unwrap();
    let admreg = items
        .iter()
        .find(|g| g["metadata"]["name"] == "admissionregistration.k8s.io")
        .expect("admissionregistration.k8s.io must appear in aggregated discovery items");

    let versions = admreg["versions"].as_array().unwrap();
    let v1 = versions
        .iter()
        .find(|v| v["version"] == "v1")
        .expect("admissionregistration.k8s.io must have a v1 version entry");

    let resources = v1["resources"].as_array().unwrap();
    let has_vwc = resources
        .iter()
        .any(|r| r["resource"] == "validatingwebhookconfigurations");
    assert!(
        has_vwc,
        "admissionregistration.k8s.io/v1 must include validatingwebhookconfigurations in aggregated discovery, \
         got resources: {:?}",
        resources
            .iter()
            .filter_map(|r| r["resource"].as_str())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_field_validation_strict_rejects_unknown_fields() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Create namespace first
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"fv-test"}}"#,
        ))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    // POST ConfigMap with unknownTopLevelField and fieldValidation=Strict.
    // Upstream returns 400 BadRequest (not 422) for strict-decoding errors.
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/fv-test/configmaps?fieldValidation=Strict")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm1"},"data":{"k":"v"},"unknownTopLevelField":"oops"}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "Strict mode must reject unknown fields with 400 BadRequest"
    );
}

/// Strict field validation must catch *nested* unknown fields (not just
/// top-level/metadata), e.g. `spec.bogus` on a Pod, via the typed schema.
#[tokio::test]
async fn test_field_validation_strict_rejects_nested_unknown_field() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Unknown nested field spec.bogus on a Pod ⇒ 400.
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/pods?fieldValidation=Strict")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p1"},"spec":{"bogus":true,"containers":[{"name":"c","image":"x"}]}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "Strict mode must reject nested unknown fields (spec.bogus)"
    );

    // The same Pod without the bogus field is accepted under Strict.
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/pods?fieldValidation=Strict")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p2"},"spec":{"containers":[{"name":"c","image":"x"}]}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "Strict mode must accept a valid Pod with no unknown fields"
    );
}

#[tokio::test]
async fn test_field_validation_strict_accepts_valid_fields() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"fv-test2"}}"#,
        ))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    // POST ConfigMap with only known fields and fieldValidation=Strict → expect 201
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/fv-test2/configmaps?fieldValidation=Strict")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm2"},"data":{"k":"v"}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "Strict mode must accept valid known fields"
    );
}

#[tokio::test]
async fn test_field_validation_ignore_accepts_unknown_fields() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"fv-test3"}}"#,
        ))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    // POST with unknown field and fieldValidation=Ignore (or absent) → expect 201
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/fv-test3/configmaps?fieldValidation=Ignore")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm3"},"data":{"k":"v"},"unknownField":"ok"}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "Ignore mode must accept unknown fields"
    );
}

#[tokio::test]
async fn test_field_validation_strict_typed_deployment_rejects_unknown_and_duplicate_fields() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"fv-deploy"}} "#,
        ))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/apis/apps/v1/namespaces/fv-deploy/deployments?fieldValidation=Strict")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
  "apiVersion":"apps/v1",
  "kind":"Deployment",
  "metadata":{"name":"my-dep"},
  "spec":{
    "unknownField":"foo",
    "replicas":2,
    "replicas":3,
    "selector":{"matchLabels":{"app":"nginx"}},
    "template":{
      "metadata":{"labels":{"app":"nginx"}},
      "spec":{"containers":[{"name":"nginx","image":"nginx:latest"}]}
    }
  }
}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    // Upstream returns 400 BadRequest for strict-decoding errors.
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    let status_json: serde_json::Value = serde_json::from_str(&text).unwrap();
    let message = status_json["message"].as_str().unwrap_or("");
    assert!(
        message.contains(
            r#"strict decoding error: unknown field "spec.unknownField", duplicate field "spec.replicas""#
        ),
        "error must report both unknown and duplicate strict decode failures, got: {}",
        text
    );
}

/// S13-API-1 regression test: garbage_collector.go:436 — pods created by RC must survive
/// when the RC is deleted with propagationPolicy=Orphan.
///
/// The K8s e2e test (garbage_collector.go:388) creates an RC, waits for RC to create N pods,
/// deletes the RC with PropagationPolicy=Orphan, waits 30s, then expects N pods to exist.
/// The K8s GC controller (running in Sonobuoy) sees the RC DELETED watch event and checks
/// for owned pods. If ownerReferences are removed BEFORE the DELETED event is broadcast,
/// the GC controller finds no owned pods → no cascade delete → pods survive.
/// If the DELETED event fires first (buggy order: delete_resource then orphan_children),
/// the GC controller deletes all pods before orphan_children runs.
///
/// Fix: orphan_children must run BEFORE delete_resource for propagationPolicy=Orphan.
#[tokio::test]
async fn test_garbage_collector_orphan_delete_pods_survive() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    // Create the test namespace
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "gc-orphan-ns",
        serde_json::json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"gc-orphan-ns"}}),
    )
    .await
    .unwrap();

    // Create an RC with 5 replicas — the reconcile_create_handler! triggers the RC controller
    // which creates 5 pods owned by this RC.
    let rc_uid = "gc-s13-rc-uid-fixed";
    let rc_body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ReplicationController",
        "metadata": {
            "name": "gc-orphan-rc",
            "namespace": "gc-orphan-ns",
            "uid": rc_uid
        },
        "spec": {
            "replicas": 5,
            "selector": {"app": "gc-orphan-test"},
            "template": {
                "metadata": {"labels": {"app": "gc-orphan-test"}},
                "spec": {"containers": [{"name": "c", "image": "busybox"}]}
            }
        }
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/gc-orphan-ns/replicationcontrollers")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&rc_body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "RC must be created");

    // Verify 5 pods were created by the RC controller
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/gc-orphan-ns/pods")
        .header("accept", "application/json")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let items = list["items"].as_array().expect("items must be an array");
    assert_eq!(
        items.len(),
        5,
        "RC controller must have created exactly 5 pods"
    );

    // Delete the RC with propagationPolicy=Orphan
    // Bug fix: orphan_children must run BEFORE delete_resource so that when the DELETED
    // watch event fires, pods already have ownerRefs removed.
    let req = Request::builder()
        .method("DELETE")
        .uri("/api/v1/namespaces/gc-orphan-ns/replicationcontrollers/gc-orphan-rc")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"propagationPolicy":"Orphan"}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "RC Orphan delete must return 200"
    );

    // RC must be gone
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/gc-orphan-ns/replicationcontrollers/gc-orphan-rc")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "RC must be deleted");

    // All 5 pods must still exist (Orphan = pods survive)
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/gc-orphan-ns/pods")
        .header("accept", "application/json")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "LIST pods must return 200");
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let items = list["items"].as_array().expect("items must be an array");
    assert_eq!(
        items.len(),
        5,
        "S13-API-1: expected 5 pods to survive Orphan deletion, got {}. \
         orphan_children must remove ownerRefs BEFORE delete_resource emits DELETED event \
         so the K8s GC controller does not cascade-delete the pods.",
        items.len()
    );

    // Each pod must have its ownerRef to the RC removed (they are now orphaned)
    for item in items {
        let has_rc_owner_ref = item["metadata"]["ownerReferences"]
            .as_array()
            .map(|refs| {
                refs.iter()
                    .any(|r| r.get("uid").and_then(|u| u.as_str()) == Some(rc_uid))
            })
            .unwrap_or(false);
        assert!(
            !has_rc_owner_ref,
            "Pod must have ownerRef to RC removed after Orphan deletion, \
             but still has ownerReference with uid={}",
            rc_uid
        );
    }
}

/// S13-API-1 additional regression: after Orphan deletion, pods have ownerRefs removed
/// and are still accessible by name. Verifies the end-state of orphan_children.
#[tokio::test]
async fn test_garbage_collector_orphan_pods_have_owner_refs_removed() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    // Create namespace
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "gc-owns-ns",
        serde_json::json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":"gc-owns-ns"}}),
    )
    .await
    .unwrap();

    // Create parent ConfigMap
    let parent_uid = "gc-owns-parent-uid";
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/gc-owns-ns/configmaps")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"apiVersion":"v1","kind":"ConfigMap","metadata":{{"name":"gc-owns-parent","namespace":"gc-owns-ns","uid":"{parent_uid}"}}}}"#
        )))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "parent must be created");

    // Create 3 pods with ownerRefs to parent
    for i in 0..3 {
        let pod_body = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": format!("gc-pod-{i}"),
                "namespace": "gc-owns-ns",
                "ownerReferences": [{"apiVersion":"v1","kind":"ConfigMap","name":"gc-owns-parent","uid":parent_uid,"controller":true}]
            },
            "spec": {"containers": [{"name": "c", "image": "busybox"}]}
        });
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/namespaces/gc-owns-ns/pods")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&pod_body).unwrap()))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "pod {i} must be created"
        );
    }

    // Delete parent with Orphan — pods should survive with ownerRefs removed
    let req = Request::builder()
        .method("DELETE")
        .uri("/api/v1/namespaces/gc-owns-ns/configmaps/gc-owns-parent")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"propagationPolicy":"Orphan"}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "Orphan delete must return 200"
    );

    // All 3 pods must still exist
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/gc-owns-ns/pods")
        .header("accept", "application/json")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let items = list["items"].as_array().expect("items must be an array");
    assert_eq!(
        items.len(),
        3,
        "S13-API-1: all 3 pods must survive Orphan deletion, got {}",
        items.len()
    );

    // Each pod must have the ownerRef to the parent removed
    for item in items {
        let has_parent_ref = item["metadata"]["ownerReferences"]
            .as_array()
            .map(|refs| {
                refs.iter()
                    .any(|r| r.get("uid").and_then(|u| u.as_str()) == Some(parent_uid))
            })
            .unwrap_or(false);
        assert!(
            !has_parent_ref,
            "S13-API-1: pod ownerRef to parent must be removed after Orphan deletion"
        );
    }
}

#[tokio::test]
async fn test_pod_eviction_marks_pod_terminating_and_returns_201() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Create namespace
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"evict-test"}}"#,
        ))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    // Create pod
    let req = Request::builder().method("POST").uri("/api/v1/namespaces/evict-test/pods")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"victim","namespace":"evict-test"},"spec":{"containers":[{"name":"c","image":"nginx"}]}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "pod create must succeed"
    );

    // Evict the pod
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/evict-test/pods/victim/eviction")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"victim","namespace":"evict-test"}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "eviction must return 201"
    );

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        result["kind"], "Eviction",
        "response must be an Eviction object"
    );

    // Eviction is deletion intent. The kubelet lifecycle actor owns final
    // hard-delete after runtime/cache cleanup, so the API row remains
    // terminating until actor finalization.
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/evict-test/pods/victim")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "pod row must remain until lifecycle actor finalizes eviction"
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let pod: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        pod.pointer("/metadata/deletionTimestamp").is_some(),
        "evicted pod must be marked terminating"
    );
}

#[tokio::test]
async fn test_pod_delete_returns_accepted_when_marked_terminating() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"pod-delete-accepted"}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/pod-delete-accepted/pods")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"victim","namespace":"pod-delete-accepted"},"spec":{"containers":[{"name":"c","image":"nginx"}]}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("DELETE")
        .uri("/api/v1/namespaces/pod-delete-accepted/pods/victim?gracePeriodSeconds=5")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(
        body.pointer("/metadata/deletionTimestamp").is_some(),
        "Pod DELETE must return the terminating Pod object: {body:?}"
    );
    assert_eq!(
        body.pointer("/metadata/deletionGracePeriodSeconds"),
        Some(&json!(5)),
        "Pod DELETE must honor query gracePeriodSeconds"
    );
}

#[tokio::test]
async fn test_pod_eviction_respects_pdb_and_returns_disruptionbudget_cause() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"evict-pdb-test"}}"#,
        ))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/evict-pdb-test/pods")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"victim","namespace":"evict-pdb-test","labels":{"app":"demo"}},"spec":{"containers":[{"name":"c","image":"nginx"}]}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("POST")
        .uri("/apis/policy/v1/namespaces/evict-pdb-test/poddisruptionbudgets")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
                "apiVersion":"policy/v1",
                "kind":"PodDisruptionBudget",
                "metadata":{"name":"pdb"},
                "spec":{
                    "minAvailable":1,
                    "selector":{"matchLabels":{"app":"demo"}}
                }
            }"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/evict-pdb-test/pods/victim/eviction")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"victim","namespace":"evict-pdb-test"}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "eviction must be rejected when matching PDB has disruptionsAllowed=0"
    );

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status["reason"], "TooManyRequests");
    let cause_reason = status
        .pointer("/details/causes/0/reason")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(cause_reason, "DisruptionBudget");
}

#[tokio::test]
async fn test_validatingadmissionpolicy_crud_and_status() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Create ValidatingAdmissionPolicy (cluster-scoped)
    let vap_body = r#"{
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": "test-vap"},
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {
                "resourceRules": [{"apiGroups":["apps"],"apiVersions":["v1"],"operations":["CREATE","UPDATE"],"resources":["deployments"]}]
            },
            "validations": [{"expression":"object.spec.replicas <= 5","message":"replicas must be <= 5"}]
        }
    }"#;
    let req = Request::builder()
        .method("POST")
        .uri("/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies")
        .header("content-type", "application/json")
        .body(Body::from(vap_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "VAP create must return 201"
    );

    // GET the VAP
    let req = Request::builder()
        .method("GET")
        .uri("/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/test-vap")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "VAP GET must return 200");
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let vap: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(vap["kind"], "ValidatingAdmissionPolicy");
    assert_eq!(vap["metadata"]["name"], "test-vap");

    // GET /status subresource
    let req = Request::builder()
        .method("GET")
        .uri("/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/test-vap/status")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "VAP GET /status must return 200 not 404"
    );

    // PUT /status subresource
    let status_body = r#"{
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": "test-vap"},
        "status": {"observedGeneration": 1}
    }"#;
    let req = Request::builder()
        .method("PUT")
        .uri("/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/test-vap/status")
        .header("content-type", "application/json")
        .body(Body::from(status_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "VAP PUT /status must return 200"
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let updated: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        updated["status"]["observedGeneration"], 1,
        "status must be updated"
    );

    // LIST ValidatingAdmissionPolicies
    let req = Request::builder()
        .method("GET")
        .uri("/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "VAP list must return 200");
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(list["kind"], "ValidatingAdmissionPolicyList");
    assert_eq!(list["items"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_validatingadmissionpolicy_binding_denies_matching_configmap() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let vap_body = serde_json::json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": "deny-blocked-configmaps"},
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {
                "resourceRules": [{
                    "apiGroups": [""],
                    "apiVersions": ["v1"],
                    "operations": ["CREATE"],
                    "resources": ["configmaps"]
                }]
            },
            "validations": [{
                "expression": "object.metadata.labels.blocked != 'true'",
                "message": "blocked configmaps are not allowed"
            }]
        }
    });
    let req = Request::builder()
        .method("POST")
        .uri("/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&vap_body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let binding_body = serde_json::json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicyBinding",
        "metadata": {"name": "deny-blocked-configmaps"},
        "spec": {
            "policyName": "deny-blocked-configmaps",
            "validationActions": ["Deny"]
        }
    });
    let req = Request::builder()
        .method("POST")
        .uri("/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&binding_body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let configmap_body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "blocked-cm",
            "namespace": "default",
            "labels": {"blocked": "true"}
        },
        "data": {"key": "value"}
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/configmaps")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&configmap_body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "ValidatingAdmissionPolicy with a Deny binding must reject matching requests"
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        status["message"]
            .as_str()
            .unwrap_or_default()
            .contains("blocked configmaps are not allowed"),
        "status message should include validation message: {status:?}"
    );
}

#[tokio::test]
async fn test_validatingadmissionpolicy_param_ref_denies_when_param_expression_fails() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let param_body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "vap-params", "namespace": "default"},
        "data": {"required": "enabled"}
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/configmaps")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&param_body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let vap_body = serde_json::json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": "require-param-value"},
        "spec": {
            "failurePolicy": "Fail",
            "paramKind": {"apiVersion": "v1", "kind": "ConfigMap"},
            "matchConstraints": {
                "resourceRules": [{
                    "apiGroups": [""],
                    "apiVersions": ["v1"],
                    "operations": ["CREATE"],
                    "resources": ["configmaps"]
                }]
            },
            "validations": [{
                "expression": "object.data.mode == params.data.required",
                "message": "mode must match the configured parameter"
            }]
        }
    });
    let req = Request::builder()
        .method("POST")
        .uri("/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&vap_body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let binding_body = serde_json::json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicyBinding",
        "metadata": {"name": "require-param-value"},
        "spec": {
            "policyName": "require-param-value",
            "validationActions": ["Deny"],
            "paramRef": {
                "name": "vap-params",
                "namespace": "default",
                "parameterNotFoundAction": "Deny"
            }
        }
    });
    let req = Request::builder()
        .method("POST")
        .uri("/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&binding_body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let target_body = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "wrong-param-value", "namespace": "default"},
        "data": {"mode": "disabled"}
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/default/configmaps")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&target_body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn test_validatingadmissionpolicy_binding_rejects_deny_and_warn_actions() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;
    let binding_body = serde_json::json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingAdmissionPolicyBinding",
        "metadata": {"name": "invalid-actions"},
        "spec": {
            "policyName": "any-policy",
            "validationActions": ["Deny", "Warn"]
        }
    });
    let req = Request::builder()
        .method("POST")
        .uri("/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&binding_body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn test_service_status_lifecycle_patch_and_put() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Create namespace
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Create Service
    let req = Request::builder()
        .method("POST").uri("/api/v1/namespaces/default/services")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"v1","kind":"Service","metadata":{"name":"test-svc","namespace":"default"},"spec":{"selector":{"app":"test"},"ports":[{"port":80,"targetPort":8080}]}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // GET /status — must return 200
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/default/services/test-svc/status")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "GET /status must return 200");

    // PUT /status — update status field
    let req = Request::builder()
        .method("PUT").uri("/api/v1/namespaces/default/services/test-svc/status")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"apiVersion":"v1","kind":"Service","metadata":{"name":"test-svc","namespace":"default"},"spec":{"selector":{"app":"test"},"ports":[{"port":80}]},"status":{"loadBalancer":{}}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "PUT /status must return 200");

    // PATCH /status with merge-patch — must return 200, NOT 405
    let req = Request::builder()
        .method("PATCH").uri("/api/v1/namespaces/default/services/test-svc/status")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(r#"{"status":{"conditions":[{"type":"Ready","status":"True","reason":"ServiceReady","message":"ok","lastTransitionTime":"2024-01-01T00:00:00Z"}]}}"#))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "PATCH /status must return 200 (not 405 MethodNotAllowed)"
    );

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let updated: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // Verify status.conditions was written
    assert_eq!(
        updated["status"]["conditions"][0]["type"], "Ready",
        "PATCH /status must persist status.conditions"
    );
}

#[tokio::test]
async fn test_service_create_defaults_session_affinity_none() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let app = build_test_router().await;

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"svc-session-affinity"}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/svc-session-affinity/services")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Service","metadata":{"name":"agnhost-primary","namespace":"svc-session-affinity"},"spec":{"selector":{"app":"agnhost","role":"primary"},"ports":[{"port":6379,"targetPort":"agnhost-server"}]}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        created["spec"]["sessionAffinity"], "None",
        "Service create must default spec.sessionAffinity=None"
    );
}

#[tokio::test]
async fn test_deployment_status_put_repairs_empty_type_meta_in_response() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"deploy-status"}}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let deployment = r#"{
      "apiVersion":"apps/v1",
      "kind":"Deployment",
      "metadata":{"name":"test-deployment","namespace":"deploy-status"},
      "spec":{
        "replicas":1,
        "selector":{"matchLabels":{"app":"x"}},
        "template":{
          "metadata":{"labels":{"app":"x"}},
          "spec":{"containers":[{"name":"c","image":"busybox"}]}
        }
      }
    }"#;
    let req = Request::builder()
        .method("POST")
        .uri("/apis/apps/v1/namespaces/deploy-status/deployments")
        .header("content-type", "application/json")
        .body(Body::from(deployment))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Emulate e2e-style status update with empty TypeMeta.
    let status_put = r#"{
      "apiVersion":"",
      "kind":"",
      "metadata":{"name":"test-deployment","namespace":"deploy-status"},
      "status":{"replicas":1,"readyReplicas":1}
    }"#;
    let req = Request::builder()
        .method("PUT")
        .uri("/apis/apps/v1/namespaces/deploy-status/deployments/test-deployment/status")
        .header("content-type", "application/json")
        .body(Body::from(status_put))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(
        body["apiVersion"], "apps/v1",
        "status response must repair empty apiVersion"
    );
    assert_eq!(
        body["kind"], "Deployment",
        "status response must repair empty kind"
    );
    assert_eq!(body["status"]["readyReplicas"], 1);

    // Deployment controller should re-populate status conditions after a
    // status subresource update clears/overwrites them.
    let mut saw_repopulated_conditions = false;
    for _ in 0..30 {
        let req = Request::builder()
            .method("GET")
            .uri("/apis/apps/v1/namespaces/deploy-status/deployments/test-deployment")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let current: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        if current
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .is_some_and(|arr| !arr.is_empty())
        {
            saw_repopulated_conditions = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        saw_repopulated_conditions,
        "deployment status conditions should be repopulated after status update"
    );
}

#[tokio::test]
async fn test_deployment_main_update_preserves_status_subresource() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "deploy-main-update-status",
        serde_json::json!({
            "apiVersion":"v1",
            "kind":"Namespace",
            "metadata":{"name":"deploy-main-update-status"}
        }),
    )
    .await
    .unwrap();

    db.create_resource(
        "apps/v1",
        "Deployment",
        Some("deploy-main-update-status"),
        "web",
        serde_json::json!({
            "apiVersion":"apps/v1",
            "kind":"Deployment",
            "metadata":{
                "name":"web",
                "namespace":"deploy-main-update-status",
                "generation":1
            },
            "spec":{
                "replicas":10,
                "selector":{"matchLabels":{"app":"web"}},
                "template":{
                    "metadata":{"labels":{"app":"web"}},
                    "spec":{"containers":[{"name":"web","image":"registry.k8s.io/e2e-test-images/httpd:2.4.38-4"}]}
                }
            },
            "status":{
                "observedGeneration":1,
                "replicas":10,
                "readyReplicas":10,
                "availableReplicas":10
            }
        }),
    )
    .await
    .unwrap();

    let update_body = r#"{
      "apiVersion":"apps/v1",
      "kind":"Deployment",
      "metadata":{
        "name":"web",
        "namespace":"deploy-main-update-status"
      },
      "spec":{
        "replicas":10,
        "selector":{"matchLabels":{"app":"web"}},
        "template":{
          "metadata":{"labels":{"app":"web"}},
          "spec":{"containers":[{"name":"web","image":"webserver:404"}]}
        }
      }
    }"#;
    let req = Request::builder()
        .method("PUT")
        .uri("/apis/apps/v1/namespaces/deploy-main-update-status/deployments/web")
        .header("content-type", "application/json")
        .body(Body::from(update_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let updated: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(
        updated["status"]["availableReplicas"], 10,
        "ordinary Deployment update must preserve existing status"
    );
    assert_eq!(
        updated["metadata"]["generation"], 2,
        "spec update should still bump generation"
    );

    let stored = db
        .get_resource(
            "apps/v1",
            "Deployment",
            Some("deploy-main-update-status"),
            "web",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.data["spec"]["template"]["spec"]["containers"][0]["image"],
        "webserver:404"
    );
}

#[tokio::test]
async fn test_deployment_unconditional_main_update_ignores_status_churn() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let app = crate::api::build_router(state);

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "deploy-main-update-race",
        serde_json::json!({
            "apiVersion":"v1",
            "kind":"Namespace",
            "metadata":{"name":"deploy-main-update-race"}
        }),
    )
    .await
    .unwrap();

    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("deploy-main-update-race"),
            "web",
            serde_json::json!({
                "apiVersion":"apps/v1",
                "kind":"Deployment",
                "metadata":{
                    "name":"web",
                    "namespace":"deploy-main-update-race"
                },
                "spec":{
                    "replicas":1,
                    "selector":{"matchLabels":{"app":"web"}},
                    "template":{
                        "metadata":{"labels":{"app":"web"}},
                        "spec":{"containers":[{"name":"web","image":"registry.k8s.io/pause:3.10.1"}]}
                    }
                },
                "status":{
                    "replicas":1,
                    "readyReplicas":1,
                    "availableReplicas":1
                }
            }),
        )
        .await
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let status_writes = Arc::new(AtomicUsize::new(0));
    let churn_db = db.clone();
    let churn_stop = stop.clone();
    let churn_count = status_writes.clone();
    let uid = created.uid.clone();
    let churn = tokio::spawn(async move {
        let mut replicas = 0_i64;
        while !churn_stop.load(Ordering::SeqCst) {
            let _ = churn_db
                .update_status_only_with_preconditions(
                    "apps/v1",
                    "Deployment",
                    Some("deploy-main-update-race"),
                    "web",
                    serde_json::json!({
                        "replicas": replicas,
                        "readyReplicas": replicas,
                        "availableReplicas": replicas
                    }),
                    crate::datastore::ResourcePreconditions::uid(&uid),
                )
                .await;
            replicas = (replicas + 1) % 3;
            churn_count.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
        }
    });

    while status_writes.load(Ordering::SeqCst) < 5 {
        tokio::task::yield_now().await;
    }

    let mut conflict_at = None;
    let mut unexpected = None;
    for version in 0..100 {
        let update_body = serde_json::json!({
            "apiVersion":"apps/v1",
            "kind":"Deployment",
            "metadata":{
                "name":"web",
                "namespace":"deploy-main-update-race"
            },
            "spec":{
                "replicas":1,
                "selector":{"matchLabels":{"app":"web"}},
                "template":{
                    "metadata":{"labels":{"app":"web"}},
                    "spec":{"containers":[{"name":"web","image":format!("webserver:{version}")}]}
                }
            }
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/apis/apps/v1/namespaces/deploy-main-update-race/deployments/web")
                    .header("content-type", "application/json")
                    .body(Body::from(update_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        if response.status() == StatusCode::CONFLICT {
            conflict_at = Some(version);
            break;
        }
        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            unexpected = Some((version, status, String::from_utf8_lossy(&body).to_string()));
            break;
        }
    }

    stop.store(true, Ordering::SeqCst);
    churn.await.unwrap();

    assert!(
        conflict_at.is_none(),
        "Deployment PUT without metadata.resourceVersion must not return 409 while status updates race; first conflict at image version {:?}",
        conflict_at
    );
    assert!(
        unexpected.is_none(),
        "unexpected Deployment PUT response while status updates race: {:?}",
        unexpected
    );
}

#[tokio::test]
async fn test_resourcequota_status_get_put_patch() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Create namespace
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"rq-test"}}"#,
        ))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    // Create ResourceQuota
    let rq_body = r#"{"apiVersion":"v1","kind":"ResourceQuota","metadata":{"name":"test-quota","namespace":"rq-test"},"spec":{"hard":{"secrets":"10","pods":"5"}}}"#;
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces/rq-test/resourcequotas")
        .header("content-type", "application/json")
        .body(Body::from(rq_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "ResourceQuota create must return 201"
    );

    // GET /status — must return 200 not 404
    let req = Request::builder()
        .method("GET")
        .uri("/api/v1/namespaces/rq-test/resourcequotas/test-quota/status")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "GET /status must return 200");

    // PUT /status — update status.used
    let put_body = r#"{"apiVersion":"v1","kind":"ResourceQuota","metadata":{"name":"test-quota","namespace":"rq-test"},"spec":{"hard":{"secrets":"10","pods":"5"}},"status":{"hard":{"secrets":"10","pods":"5"},"used":{"secrets":"3","pods":"1"}}}"#;
    let req = Request::builder()
        .method("PUT")
        .uri("/api/v1/namespaces/rq-test/resourcequotas/test-quota/status")
        .header("content-type", "application/json")
        .body(Body::from(put_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "PUT /status must return 200");
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let updated: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        updated["status"]["used"]["secrets"], "3",
        "PUT /status must update status.used.secrets"
    );

    // PATCH /status — merge patch
    let patch_body = r#"{"status":{"used":{"secrets":"5","pods":"2"}}}"#;
    let req = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/rq-test/resourcequotas/test-quota/status")
        .header("content-type", "application/merge-patch+json")
        .body(Body::from(patch_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "PATCH /status must return 200"
    );
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let patched: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        patched["status"]["used"]["secrets"], "5",
        "PATCH /status must update status.used.secrets"
    );
}
