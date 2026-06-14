use super::*;

#[tokio::test]
async fn test_create_statefulset_initializes_revision_status() {
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
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#,
        ))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let req = Request::builder()
        .method("POST")
        .uri("/apis/apps/v1/namespaces/default/statefulsets")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "metadata": {"name": "ss2"},
                "spec": {
                    "replicas": 3,
                    "serviceName": "test",
                    "podManagementPolicy": "Parallel",
                    "selector": {"matchLabels": {"foo": "bar"}},
                    "template": {
                        "metadata": {"labels": {"foo": "bar", "baz": "blah"}},
                        "spec": {
                            "containers": [{
                                "name": "webserver",
                                "image": "registry.k8s.io/e2e-test-images/httpd:2.4.38-4"
                            }]
                        }
                    }
                }
            }"#,
        ))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let current_revision = result
        .pointer("/status/currentRevision")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let update_revision = result
        .pointer("/status/updateRevision")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    assert!(!current_revision.is_empty());
    assert_eq!(
        current_revision, update_revision,
        "new StatefulSets must start with currentRevision equal to updateRevision"
    );
}

#[tokio::test]
async fn test_create_deployment_injects_metadata_fields() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    // Build the app with in-memory state
    let app = build_test_router().await;

    // Create namespace first
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}"#,
        ))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    // Create deployment without namespace/name in metadata (kubectl behavior)
    let req = Request::builder()
        .method("POST")
        .uri("/apis/apps/v1/namespaces/default/deployments")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "nginx"
            },
            "spec": {
                "replicas": 1,
                "selector": {
                    "matchLabels": {"app": "nginx"}
                },
                "template": {
                    "metadata": {"labels": {"app": "nginx"}},
                    "spec": {
                        "containers": [{"name": "nginx", "image": "nginx"}]
                    }
                }
            }
        }"#,
        ))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // P0 critical path: these fields MUST be injected for reconciliation to work
    assert_eq!(
        result["metadata"]["namespace"], "default",
        "namespace must be injected"
    );
    assert_eq!(result["metadata"]["name"], "nginx", "name must be injected");
    assert!(
        result["metadata"]["uid"].is_string(),
        "uid must be generated"
    );
    assert!(
        !result["metadata"]["uid"].as_str().unwrap().is_empty(),
        "uid must not be empty"
    );
    assert!(
        result["metadata"]["creationTimestamp"].is_string(),
        "creationTimestamp must be set"
    );
}

#[tokio::test]
async fn test_create_cluster_scoped_resource_injects_name() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    // Create a cluster-scoped Namespace resource
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/namespaces")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"test-ns"}}"#,
        ))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Cluster-scoped resources must also have name injected into metadata
    assert_eq!(
        result["metadata"]["name"], "test-ns",
        "name must be injected for cluster-scoped resources"
    );
    assert!(
        result["metadata"]["uid"].is_string(),
        "uid must be generated"
    );
    assert!(
        result["metadata"]["creationTimestamp"].is_string(),
        "creationTimestamp must be set"
    );
}

#[tokio::test]
async fn test_version_endpoint_returns_k8s_version_info() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    let app = build_test_router().await;

    let req = Request::builder()
        .method("GET")
        .uri("/version")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(result["major"], "1");
    assert_eq!(result["minor"], "34");
    // Version format: v1.34.6+klights1.0.0
    let git_version = result["gitVersion"].as_str().unwrap_or("unknown");
    assert!(
        git_version.starts_with("v1.34"),
        "gitVersion should start with 'v1.34', got: {}",
        git_version
    );
    assert!(
        git_version.contains("+klights"),
        "gitVersion should contain '+klights', got: {}",
        git_version
    );
    assert!(result["platform"].is_string());
    assert!(
        result["compiler"]
            .as_str()
            .unwrap_or("")
            .starts_with("rustc")
    );
}

#[tokio::test]
async fn test_create_namespace_auto_creates_default_service_account() {
    // Setup test database
    let db = crate::datastore::test_support::in_memory().await;

    // Create a new namespace directly via the create function
    let namespace_name = "test-namespace";
    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": namespace_name
        }
    });

    db.create_resource("v1", "Namespace", None, namespace_name, ns_body)
        .await
        .unwrap();

    // Call the function that should auto-create the default SA
    crate::controllers::namespace::create_default_service_account(&db, namespace_name)
        .await
        .unwrap();

    // Verify the default ServiceAccount was created
    let sa = db
        .get_resource("v1", "ServiceAccount", Some(namespace_name), "default")
        .await
        .unwrap();

    assert!(sa.is_some(), "Default ServiceAccount should exist");

    let sa_data = sa.unwrap().data;
    assert_eq!(sa_data["metadata"]["name"], "default");
    assert_eq!(sa_data["metadata"]["namespace"], namespace_name);
}

#[tokio::test]
async fn test_create_kube_root_ca_configmap_contains_ca_cert() {
    // Setup test database
    let db = crate::datastore::test_support::in_memory().await;

    let namespace_name = "test-namespace";
    let ca_cert_pem = "-----BEGIN CERTIFICATE-----\ntest-ca-cert\n-----END CERTIFICATE-----";

    // Call the function to create kube-root-ca.crt ConfigMap
    crate::controllers::namespace::create_kube_root_ca_configmap(&db, namespace_name, ca_cert_pem)
        .await
        .unwrap();

    // Verify the ConfigMap was created
    let cm = db
        .get_resource("v1", "ConfigMap", Some(namespace_name), "kube-root-ca.crt")
        .await
        .unwrap();

    assert!(cm.is_some(), "kube-root-ca.crt ConfigMap should exist");

    let cm_data = cm.unwrap().data;
    assert_eq!(cm_data["metadata"]["name"], "kube-root-ca.crt");
    assert_eq!(cm_data["metadata"]["namespace"], namespace_name);
    assert_eq!(cm_data["data"]["ca.crt"], ca_cert_pem);
}

#[tokio::test]
async fn test_dynamic_namespace_creates_kube_root_ca_configmap() {
    let db = crate::datastore::test_support::in_memory().await;
    let namespace_name = "dynamic-ns";
    let ca_cert_pem = "-----BEGIN CERTIFICATE-----\nmy-cluster-ca\n-----END CERTIFICATE-----";

    // Create namespace (simulates POST /api/v1/namespaces)
    let ns_body = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": namespace_name}
    });
    db.create_resource("v1", "Namespace", None, namespace_name, ns_body)
        .await
        .unwrap();

    // Simulate what the API handler does after namespace creation:
    // auto-create default SA and kube-root-ca.crt ConfigMap
    crate::controllers::namespace::create_default_service_account(&db, namespace_name)
        .await
        .unwrap();
    crate::controllers::namespace::create_kube_root_ca_configmap(&db, namespace_name, ca_cert_pem)
        .await
        .unwrap();

    // Verify kube-root-ca.crt ConfigMap exists in the dynamic namespace
    let cm = db
        .get_resource("v1", "ConfigMap", Some(namespace_name), "kube-root-ca.crt")
        .await
        .unwrap();

    assert!(
        cm.is_some(),
        "kube-root-ca.crt must be created in dynamically-created namespace"
    );

    let cm_data = cm.unwrap().data;
    assert_eq!(cm_data["metadata"]["name"], "kube-root-ca.crt");
    assert_eq!(cm_data["metadata"]["namespace"], namespace_name);
    assert_eq!(cm_data["data"]["ca.crt"], ca_cert_pem);

    // Verify default SA also exists
    let sa = db
        .get_resource("v1", "ServiceAccount", Some(namespace_name), "default")
        .await
        .unwrap();
    assert!(
        sa.is_some(),
        "default ServiceAccount must be created in dynamically-created namespace"
    );
}

#[tokio::test]
async fn test_kube_root_ca_configmap_idempotent() {
    let db = crate::datastore::test_support::in_memory().await;
    let ca_pem = "-----BEGIN CERTIFICATE-----\nca-data\n-----END CERTIFICATE-----";

    // Create configmap first time — should succeed
    crate::controllers::namespace::create_kube_root_ca_configmap(&db, "default", ca_pem)
        .await
        .unwrap();

    // Create again — should fail (duplicate) but not panic
    let result =
        crate::controllers::namespace::create_kube_root_ca_configmap(&db, "default", ca_pem).await;
    assert!(
        result.is_err(),
        "Duplicate kube-root-ca.crt creation should return error, not panic"
    );
}

#[tokio::test]
async fn test_patch_configmap_with_apply_patch_yaml_content_type() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use hyper::header::CONTENT_TYPE;
    use tower::ServiceExt;

    // Setup test database
    let (app, db) = build_test_router_with_db().await;

    // Create a ConfigMap first
    let configmap = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "test-config",
            "namespace": "default"
        },
        "data": {
            "key1": "value1"
        }
    });

    db.create_resource("v1", "ConfigMap", Some("default"), "test-config", configmap)
        .await
        .unwrap();

    // YAML patch body (valid YAML, NOT valid JSON)
    let yaml_body = r#"
data:
  key2: value2
  key3: value3
"#;

    // Send PATCH request with application/apply-patch+yaml content-type
    let req = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/default/configmaps/test-config")
        .header(CONTENT_TYPE, "application/apply-patch+yaml")
        .body(Body::from(yaml_body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "PATCH should succeed");

    // Verify the response body contains merged data
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    // Original key1 should be preserved
    assert_eq!(result["data"]["key1"], "value1");
    // New keys from YAML patch should be added
    assert_eq!(result["data"]["key2"], "value2");
    assert_eq!(result["data"]["key3"], "value3");
}

#[tokio::test]
async fn test_merge_patch_strict_rejects_unknown_field_generic() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use hyper::header::CONTENT_TYPE;
    use tower::ServiceExt;

    // T1.4: fieldValidation=Strict must reject unknown fields introduced by a
    // non-apply patch (merge/strategic/JSON), not only apply-patch.
    let (app, db) = build_test_router_with_db().await;

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "c1", "namespace": "default"},
        "data": {"k": "v"}
    });
    db.create_resource("v1", "ConfigMap", Some("default"), "c1", cm)
        .await
        .unwrap();

    let req = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/default/configmaps/c1?fieldValidation=Strict")
        .header(CONTENT_TYPE, "application/merge-patch+json")
        .body(Body::from(r#"{"bogusField":true}"#))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "strict merge patch with unknown field must 400"
    );
    let b = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert!(
        v["message"]
            .as_str()
            .unwrap_or_default()
            .contains("bogusField"),
        "message should name the unknown field: {v}"
    );
}

#[tokio::test]
async fn test_merge_patch_strict_rejects_unknown_nested_field_pod() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use hyper::header::CONTENT_TYPE;
    use tower::ServiceExt;

    // T1.4: the dedicated Pod patch handler must also deep-validate the merged
    // result so nested unknown fields (spec.bogus) are rejected under Strict.
    let (app, db) = build_test_router_with_db().await;

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p1", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
    });
    db.create_resource("v1", "Pod", Some("default"), "p1", pod)
        .await
        .unwrap();

    let req = Request::builder()
        .method("PATCH")
        .uri("/api/v1/namespaces/default/pods/p1?fieldValidation=Strict")
        .header(CONTENT_TYPE, "application/merge-patch+json")
        .body(Body::from(r#"{"spec":{"bogus":123}}"#))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "strict merge patch with unknown nested field must 400"
    );
    let b = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert!(
        v["message"].as_str().unwrap_or_default().contains("bogus"),
        "message should name the unknown field: {v}"
    );
}

#[tokio::test]
async fn test_patch_custom_resource_apply_strict_missing_resource_returns_schema_error_not_notfound()
 {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use hyper::header::CONTENT_TYPE;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let crd_registry = state.crd_registry.clone();
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

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.stable.example.com"},
        "spec": {
            "group": "stable.example.com",
            "scope": "Namespaced",
            "names": {"kind": "Widget", "plural": "widgets", "singular": "widget"},
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "color": {"type": "string"}
                                }
                            }
                        }
                    }
                }
            }]
        }
    });

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "widgets.stable.example.com",
        crd.clone(),
    )
    .await
    .unwrap();
    crate::controllers::crd::register_crd_from_value(&crd_registry, &crd)
        .await
        .unwrap();

    let apply_yaml = r#"
apiVersion: stable.example.com/v1
kind: Widget
metadata:
  name: mytest
spec:
  color: blue
  extraProperty: boom
"#;

    let req = Request::builder()
        .method("PATCH")
        .uri("/apis/stable.example.com/v1/namespaces/default/widgets/mytest?fieldValidation=Strict")
        .header(CONTENT_TYPE, "application/apply-patch+yaml")
        .body(Body::from(apply_yaml))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let msg = String::from_utf8_lossy(&bytes);
    assert!(
        msg.contains("field not declared in schema"),
        "expected strict schema validation error, got: {}",
        msg
    );
    assert!(
        msg.contains(".extraProperty: field not declared in schema"),
        "strict CRD validation should report kubernetes-style dotted field path, got: {}",
        msg
    );
    assert!(
        !msg.contains("not found"),
        "apply PATCH must not fail with NotFound on missing object: {}",
        msg
    );
}

#[tokio::test]
async fn test_patch_custom_resource_apply_yaml_duplicate_fields_reports_duplicate_error() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use hyper::header::CONTENT_TYPE;
    use tower::ServiceExt;

    let state = build_test_app_state().await;
    let db = state.db.clone();
    let crd_registry = state.crd_registry.clone();
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

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "fool5kxka.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"kind": "Foo", "plural": "fool5kxka", "singular": "foo"},
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "x-kubernetes-preserve-unknown-fields": true,
                                "properties": {
                                    "foo": {"type": "string"}
                                }
                            }
                        }
                    }
                }
            }]
        }
    });

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "fool5kxka.example.com",
        crd.clone(),
    )
    .await
    .unwrap();
    crate::controllers::crd::register_crd_from_value(&crd_registry, &crd)
        .await
        .unwrap();

    let duplicate_yaml = r#"
apiVersion: example.com/v1
kind: Foo
metadata:
  name: mytest
spec:
  foo: foo1
  foo: foo2
"#;

    let req = Request::builder()
        .method("PATCH")
        .uri("/apis/example.com/v1/namespaces/default/fool5kxka/mytest?fieldValidation=Strict")
        .header(CONTENT_TYPE, "application/apply-patch+yaml")
        .body(Body::from(duplicate_yaml))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let msg = String::from_utf8_lossy(&bytes);
    let status_json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let status_message = status_json["message"].as_str().unwrap_or_default();
    assert!(
        status_message.contains("line")
            && status_message.contains("key \"foo\" already set in map"),
        "expected kubernetes-style duplicate field parser error with line prefix, got: {}",
        msg
    );
    assert!(
        !status_message.contains("spec: key"),
        "error should use upstream 'line N: key ...' format instead of path-prefixed yaml error: {}",
        msg
    );
    assert!(
        !msg.contains("not found"),
        "duplicate-field apply PATCH must not fail with NotFound first: {}",
        msg
    );
}

#[tokio::test]
async fn test_patch_crd_apply_yaml_create_registers_route_for_strict_custom_resource_patch() {
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
  name: fool8jpda.example.com
spec:
  group: example.com
  scope: Namespaced
  names:
    kind: Foo
    plural: fool8jpda
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
            properties:
              known:
                type: string
"#;

    let patch_crd = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(
                    "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/fool8jpda.example.com",
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

    let cr_apply_yaml = r#"
apiVersion: example.com/v1
kind: Foo
metadata:
  name: mytest
spec:
  known: ok
  unknown: boom
"#;

    let patch_cr = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/apis/example.com/v1/namespaces/default/fool8jpda/mytest?fieldValidation=Strict")
                .header(CONTENT_TYPE, "application/apply-patch+yaml")
                .body(Body::from(cr_apply_yaml))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_cr.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let bytes = axum::body::to_bytes(patch_cr.into_body(), usize::MAX)
        .await
        .unwrap();
    let msg = String::from_utf8_lossy(&bytes);
    assert!(
        msg.contains("field not declared in schema"),
        "expected strict schema error, got: {}",
        msg
    );
    assert!(
        !msg.contains("not found"),
        "custom resource route must be available after CRD apply PATCH create: {}",
        msg
    );
}

// F1-01: PUT a NetworkPolicy with the protobuf Content-Type, GET it back with
// the protobuf Accept header, and assert the round-tripped object matches the
// posted spec. Before F1-01 the protobuf path EOF'd because single NetworkPolicy
// had no encode/decode dispatch and the namespaced CRUD route didn't exist.
#[tokio::test]
async fn api_put_get_networkpolicy_protobuf_roundtrip() {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use serde_json::json;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/namespaces")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"netpol-pb"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let np_json = json!({
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": "allow-frontend", "namespace": "netpol-pb"},
        "spec": {
            "podSelector": {"matchLabels": {"app": "backend"}},
            "policyTypes": ["Ingress", "Egress"],
            "ingress": [{
                "from": [{"podSelector": {"matchLabels": {"role": "frontend"}}}],
                "ports": [{"protocol": "TCP", "port": 8080}]
            }],
            "egress": [{
                "to": [{"ipBlock": {"cidr": "10.0.0.0/24", "except": ["10.0.0.5/32"]}}],
                "ports": [{"protocol": "TCP", "port": 5432}]
            }]
        }
    });

    let np_pb = crate::protobuf::encode_protobuf(&np_json).unwrap();
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/networking.k8s.io/v1/namespaces/netpol-pb/networkpolicies")
                .header("content-type", "application/vnd.kubernetes.protobuf")
                .body(Body::from(np_pb.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        create_resp.status(),
        StatusCode::CREATED,
        "POST networkpolicies with protobuf body must succeed"
    );

    let get_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(
                    "/apis/networking.k8s.io/v1/namespaces/netpol-pb/networkpolicies/allow-frontend",
                )
                .header("accept", "application/vnd.kubernetes.protobuf")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let ct = get_resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("application/vnd.kubernetes.protobuf"),
        "GET with protobuf Accept must respond protobuf, got {}",
        ct
    );
    let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        &body[0..4],
        &[0x6b, 0x38, 0x73, 0x00],
        "response body must carry the k8s magic prefix"
    );
    let decoded = crate::protobuf::decode_protobuf(&body[4..]).unwrap();
    assert_eq!(decoded["apiVersion"], "networking.k8s.io/v1");
    assert_eq!(decoded["kind"], "NetworkPolicy");
    assert_eq!(decoded["metadata"]["name"], "allow-frontend");
    assert_eq!(decoded["metadata"]["namespace"], "netpol-pb");
    assert_eq!(
        decoded["spec"]["podSelector"]["matchLabels"]["app"],
        "backend"
    );
    let policy_types = decoded["spec"]["policyTypes"]
        .as_array()
        .expect("policyTypes must roundtrip");
    assert!(policy_types.iter().any(|v| v == "Ingress"));
    assert!(policy_types.iter().any(|v| v == "Egress"));
    assert_eq!(
        decoded["spec"]["ingress"][0]["from"][0]["podSelector"]["matchLabels"]["role"],
        "frontend"
    );
    assert_eq!(decoded["spec"]["ingress"][0]["ports"][0]["protocol"], "TCP");
    assert_eq!(decoded["spec"]["ingress"][0]["ports"][0]["port"], 8080);
    assert_eq!(
        decoded["spec"]["egress"][0]["to"][0]["ipBlock"]["cidr"],
        "10.0.0.0/24"
    );
    assert_eq!(
        decoded["spec"]["egress"][0]["to"][0]["ipBlock"]["except"][0],
        "10.0.0.5/32"
    );
    assert_eq!(decoded["spec"]["egress"][0]["ports"][0]["port"], 5432);
}

/// Server-Side Apply (SSA) on a cluster-scoped resource must create the
/// resource when it does not yet exist. K8s spec: SSA is create-or-update.
/// `kubectl apply -f cluster-scoped.yaml` depends on this for any first-time
/// create of a PriorityClass, ClusterRole, StorageClass, etc.
///
/// Namespaced PATCH already implements this; the cluster path was missing
/// the create-when-not-exists branch and returned 404. This test gates the
/// generated_handlers macro collapse: the unified inner gives the cluster
/// path the same SSA create-or-update behavior as namespaced.
#[tokio::test]
async fn test_cluster_resource_apply_patch_creates_when_not_exists() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use hyper::header::CONTENT_TYPE;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let yaml_body = r#"
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: ssa-create-on-patch
value: 42
description: created via server-side apply
"#;

    let req = Request::builder()
        .method("PATCH")
        .uri("/apis/scheduling.k8s.io/v1/priorityclasses/ssa-create-on-patch")
        .header(CONTENT_TYPE, "application/apply-patch+yaml")
        .body(Body::from(yaml_body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "SSA PATCH on a non-existent cluster-scoped resource must create-or-update, not 404"
    );

    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(
        result["metadata"]["name"], "ssa-create-on-patch",
        "name must be set on SSA-created cluster resource"
    );
    assert!(
        result["metadata"]["uid"].is_string(),
        "uid must be generated on SSA-created cluster resource"
    );
    assert_eq!(
        result["value"], 42,
        "spec field from apply body must be persisted"
    );
    // Server-Side Apply records ownership in managedFields and must NOT write
    // the client-side last-applied-configuration annotation.
    let managed = result["metadata"]["managedFields"]
        .as_array()
        .expect("managedFields must be set on SSA-created resource");
    assert_eq!(managed.len(), 1, "one Apply manager entry expected");
    assert_eq!(managed[0]["operation"], "Apply");
    assert_eq!(managed[0]["fieldsType"], "FieldsV1");
    assert!(
        result["metadata"]["annotations"]
            .get("kubectl.kubernetes.io/last-applied-configuration")
            .is_none(),
        "SSA must not write the client-side last-applied-configuration marker"
    );

    // Subsequent GET returns the resource.
    let get_req = Request::builder()
        .method("GET")
        .uri("/apis/scheduling.k8s.io/v1/priorityclasses/ssa-create-on-patch")
        .body(Body::empty())
        .unwrap();
    let get_resp = app.oneshot(get_req).await.unwrap();
    assert_eq!(
        get_resp.status(),
        StatusCode::OK,
        "GET after SSA-create must succeed"
    );
}

/// Server-Side Apply conflict detection end-to-end: a second manager changing a
/// field owned by the first must get 409 Conflict, and `?force=true` must
/// resolve the conflict and transfer ownership.
#[tokio::test]
async fn test_server_side_apply_conflict_and_force() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use hyper::header::CONTENT_TYPE;
    use tower::ServiceExt;

    let app = build_test_router().await;

    let apply = |uri: String, body: String| {
        let app = app.clone();
        async move {
            let req = Request::builder()
                .method("PATCH")
                .uri(uri)
                .header(CONTENT_TYPE, "application/apply-patch+yaml")
                .body(Body::from(body))
                .unwrap();
            app.oneshot(req).await.unwrap()
        }
    };

    let cfg = |val: &str| {
        format!(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: ssa-conflict\ndata:\n  key: {val}\n"
        )
    };

    // mgr-a applies and takes ownership of data.key.
    let resp = apply(
        "/api/v1/namespaces/default/configmaps/ssa-conflict?fieldManager=mgr-a".to_string(),
        cfg("from-a"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    // mgr-b changing data.key without force ⇒ 409 Conflict.
    let resp = apply(
        "/api/v1/namespaces/default/configmaps/ssa-conflict?fieldManager=mgr-b".to_string(),
        cfg("from-b"),
    )
    .await;
    assert_eq!(
        resp.status(),
        StatusCode::CONFLICT,
        "second manager changing an owned field must 409"
    );

    // mgr-b with force=true ⇒ takes ownership, value updated.
    let resp = apply(
        "/api/v1/namespaces/default/configmaps/ssa-conflict?fieldManager=mgr-b&force=true"
            .to_string(),
        cfg("from-b"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK, "force must resolve conflict");
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(result["data"]["key"], "from-b");
    let managed = result["metadata"]["managedFields"].as_array().unwrap();
    let b_entry = managed
        .iter()
        .find(|e| e["manager"] == "mgr-b")
        .expect("mgr-b entry");
    assert!(b_entry["fieldsV1"]["f:data"].get("f:key").is_some());
}
