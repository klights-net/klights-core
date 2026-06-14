use super::*;
use serde_json::{Value, json};

/// P0-S13-3: apply_schema_defaults correctly handles nested object defaults.
#[test]
fn test_apply_schema_defaults_nested_object_defaults() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "replicas": {"type": "integer", "default": 3},
                    "image": {"type": "string", "default": "nginx:latest"}
                }
            }
        }
    });

    let mut cr = serde_json::json!({
        "apiVersion": "example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test"},
        "spec": {}
    });

    crate::api::apply_schema_defaults_pub(&mut cr, &schema);

    assert_eq!(cr.pointer("/spec/replicas"), Some(&serde_json::json!(3)));
    assert_eq!(
        cr.pointer("/spec/image"),
        Some(&serde_json::json!("nginx:latest"))
    );
}

/// P0-S13-3: apply_schema_defaults does not overwrite fields already set.
#[test]
fn test_apply_schema_defaults_does_not_overwrite_existing_fields() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "replicas": {"type": "integer", "default": 3}
                }
            }
        }
    });

    let mut cr = serde_json::json!({
        "apiVersion": "example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test"},
        "spec": {"replicas": 7}
    });

    crate::api::apply_schema_defaults_pub(&mut cr, &schema);

    assert_eq!(
        cr.pointer("/spec/replicas"),
        Some(&serde_json::json!(7)),
        "Existing value must not be overwritten by schema default"
    );
}

/// P0-S13-3: CRD openAPIV3Schema defaults must appear on GET (namespaced CR).
///
/// Sonobuoy custom_resource_definition.go:332 creates a CRD with
/// `spec.replicas.default = 3`, POSTs a CR omitting `spec.replicas`, then
/// GETs the CR and waits for `spec.replicas` to equal 3.  klights never
/// applied defaults at read time, so the GET returned the stored object
/// (without `spec.replicas`), causing a 30-second timeout.
#[tokio::test]
async fn test_crd_openapiv3schema_defaults_applied_on_get_namespaced_cr() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    // CRD with spec.replicas.default = 3
    let crd = serde_json::json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
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
                                    "replicas": {
                                        "type": "integer",
                                        "default": 3
                                    }
                                }
                            }
                        }
                    }
                }
            }]
        }
    });

    // Store CRD in DB and register it
    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "widgets.example.com",
        crd.clone(),
    )
    .await
    .unwrap();
    register_crd_from_value(&registry, &crd).await.unwrap();

    // Store the CR directly in the DB without `spec.replicas` (bypasses POST
    // defaulting — simulates a CR created before the default was added, or by
    // a client that skipped defaults).
    let cr_without_default = serde_json::json!({
        "apiVersion": "example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "my-widget", "namespace": "default"},
        "spec": {}
    });
    db.create_resource(
        "example.com/v1",
        "Widget",
        Some("default"),
        "my-widget",
        cr_without_default,
    )
    .await
    .unwrap();

    let app = crate::api::build_router(build_test_app_state(db, registry).await);

    // GET the CR — defaults must be applied on read
    let request = Request::builder()
        .method("GET")
        .uri("/apis/example.com/v1/namespaces/default/widgets/my-widget")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        value.pointer("/spec/replicas"),
        Some(&serde_json::json!(3)),
        "P0-S13-3: GET must apply openAPIV3Schema default (spec.replicas=3) \
        even when the stored CR omits the field. Got: {}",
        value
    );
}

/// P0-S13-3: CRD openAPIV3Schema defaults must appear on GET (cluster-scoped CR).
///
/// Same invariant as the namespaced case, but for cluster-scoped resources
/// (get_cluster_custom_resource handler).
#[tokio::test]
async fn test_crd_openapiv3schema_defaults_applied_on_get_cluster_scoped_cr() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    let crd = serde_json::json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "frobbers.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Cluster",
            "names": {"kind": "Frobber", "plural": "frobbers", "singular": "frobber"},
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
                                    "size": {
                                        "type": "string",
                                        "default": "medium"
                                    }
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
        "frobbers.example.com",
        crd.clone(),
    )
    .await
    .unwrap();
    register_crd_from_value(&registry, &crd).await.unwrap();

    // Store without defaults (bypasses write-time defaulting)
    db.create_resource(
        "example.com/v1",
        "Frobber",
        None,
        "my-frobber",
        serde_json::json!({
            "apiVersion": "example.com/v1",
            "kind": "Frobber",
            "metadata": {"name": "my-frobber"},
            "spec": {}
        }),
    )
    .await
    .unwrap();

    let app = crate::api::build_router(build_test_app_state(db, registry).await);

    let request = Request::builder()
        .method("GET")
        .uri("/apis/example.com/v1/frobbers/my-frobber")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        value.pointer("/spec/size"),
        Some(&serde_json::json!("medium")),
        "P0-S13-3: GET (cluster-scoped) must apply openAPIV3Schema defaults. Got: {}",
        value
    );
}

/// P0-S13-3 (architect REQUEST CHANGES): CRD openAPIV3Schema defaults must
/// also appear on LIST (namespaced CR). Sonobuoy verifies defaulting "on read"
/// and LIST is a read path. Each item in the returned list must have defaults
/// applied.
#[tokio::test]
async fn test_crd_openapiv3schema_defaults_applied_on_list_namespaced_cr() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    let crd = serde_json::json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
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
                                    "replicas": {
                                        "type": "integer",
                                        "default": 3
                                    }
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
        "widgets.example.com",
        crd.clone(),
    )
    .await
    .unwrap();
    register_crd_from_value(&registry, &crd).await.unwrap();

    // Create two CRs omitting spec.replicas (bypass POST defaulting by writing
    // straight to the DB — same data shape that would result from a CR created
    // before the default was added to the schema).
    for name in ["widget-a", "widget-b"] {
        db.create_resource(
            "example.com/v1",
            "Widget",
            Some("default"),
            name,
            serde_json::json!({
                "apiVersion": "example.com/v1",
                "kind": "Widget",
                "metadata": {"name": name, "namespace": "default"},
                "spec": {}
            }),
        )
        .await
        .unwrap();
    }

    let app = crate::api::build_router(build_test_app_state(db, registry).await);

    let request = Request::builder()
        .method("GET")
        .uri("/apis/example.com/v1/namespaces/default/widgets")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .expect("LIST response must include items array");
    assert_eq!(items.len(), 2, "LIST must return both CRs. Got: {}", value);
    for item in items {
        assert_eq!(
            item.pointer("/spec/replicas"),
            Some(&serde_json::json!(3)),
            "P0-S13-3: LIST item must have openAPIV3Schema default applied. Got: {}",
            item
        );
    }
}

/// P0-S13-3 (architect REQUEST CHANGES): CRD openAPIV3Schema defaults must
/// also appear on LIST (cluster-scoped CR). Same invariant as namespaced LIST,
/// but exercises the cluster-scoped handler.
#[tokio::test]
async fn test_crd_openapiv3schema_defaults_applied_on_list_cluster_scoped_cr() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    let crd = serde_json::json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "frobbers.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Cluster",
            "names": {"kind": "Frobber", "plural": "frobbers", "singular": "frobber"},
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
                                    "size": {
                                        "type": "string",
                                        "default": "medium"
                                    }
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
        "frobbers.example.com",
        crd.clone(),
    )
    .await
    .unwrap();
    register_crd_from_value(&registry, &crd).await.unwrap();

    for name in ["frobber-a", "frobber-b"] {
        db.create_resource(
            "example.com/v1",
            "Frobber",
            None,
            name,
            serde_json::json!({
                "apiVersion": "example.com/v1",
                "kind": "Frobber",
                "metadata": {"name": name},
                "spec": {}
            }),
        )
        .await
        .unwrap();
    }

    let app = crate::api::build_router(build_test_app_state(db, registry).await);

    let request = Request::builder()
        .method("GET")
        .uri("/apis/example.com/v1/frobbers")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .expect("LIST response must include items array");
    assert_eq!(items.len(), 2, "LIST must return both CRs. Got: {}", value);
    for item in items {
        assert_eq!(
            item.pointer("/spec/size"),
            Some(&serde_json::json!("medium")),
            "P0-S13-3: LIST (cluster-scoped) item must have default applied. Got: {}",
            item
        );
    }
}

/// P0-S13-10: CRD field selectors must reject unsupported field labels.
/// Namespaced CRDs only support metadata.name + metadata.namespace.
#[tokio::test]
async fn test_crd_field_selector_rejects_unsupported_field_for_namespaced_crd() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    let crd = make_crd_value("example.com", "Widget", "widgets", "Namespaced");
    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "widgets.example.com",
        crd.clone(),
    )
    .await
    .unwrap();
    register_crd_from_value(&registry, &crd).await.unwrap();

    let app = crate::api::build_router(build_test_app_state(db, registry).await);
    let request = Request::builder()
        .method("GET")
        .uri("/apis/example.com/v1/namespaces/default/widgets?fieldSelector=spec.foo%3Dbar")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let msg = value
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or_default();
    assert!(
        msg.contains("field label not supported: spec.foo"),
        "Expected unsupported field selector error, got: {}",
        msg
    );
}

/// P0-S13-10: cluster-scoped CRDs must reject metadata.namespace field selectors.
#[tokio::test]
async fn test_crd_field_selector_rejects_metadata_namespace_for_cluster_scoped_crd() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    let crd = make_crd_value("example.com", "Frobber", "frobbers", "Cluster");
    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "frobbers.example.com",
        crd.clone(),
    )
    .await
    .unwrap();
    register_crd_from_value(&registry, &crd).await.unwrap();

    let app = crate::api::build_router(build_test_app_state(db, registry).await);
    let request = Request::builder()
        .method("GET")
        .uri("/apis/example.com/v1/frobbers?fieldSelector=metadata.namespace%3Ddefault")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let msg = value
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or_default();
    assert!(
        msg.contains("field label not supported: metadata.namespace"),
        "Expected unsupported metadata.namespace error, got: {}",
        msg
    );
}

/// P0-S17-22: CRD list/watch field selectors must honor CRD selectableFields declarations.
#[tokio::test]
async fn test_crd_field_selector_accepts_declared_selectable_field() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"kind": "Widget", "plural": "widgets", "singular": "widget"},
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "selectableFields": [{"jsonPath": ".host"}],
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "host": {"type": "string"}
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
        "widgets.example.com",
        crd.clone(),
    )
    .await
    .unwrap();
    register_crd_from_value(&registry, &crd).await.unwrap();

    for (name, host) in [("w1", "host1"), ("w2", "host2")] {
        db.create_resource(
            "example.com/v1",
            "Widget",
            Some("default"),
            name,
            json!({
                "apiVersion": "example.com/v1",
                "kind": "Widget",
                "metadata": {"name": name, "namespace": "default"},
                "host": host
            }),
        )
        .await
        .unwrap();
    }

    let app = crate::api::build_router(build_test_app_state(db, registry).await);
    let request = Request::builder()
        .method("GET")
        .uri("/apis/example.com/v1/namespaces/default/widgets?fieldSelector=host%3Dhost1")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let items = value
        .get("items")
        .and_then(|v| v.as_array())
        .expect("response must contain items");
    assert_eq!(
        items.len(),
        1,
        "field selector host=host1 must filter to one item"
    );
    assert_eq!(
        items[0]
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .unwrap_or_default(),
        "w1"
    );
}

/// Regression: cluster-scoped custom resources must not be coerced into namespace "default".
#[tokio::test]
async fn test_cluster_scoped_custom_resource_storage_preserves_none_namespace() {
    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    let crd = make_crd_value("example.com", "ClusterWidget", "clusterwidgets", "Cluster");
    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "clusterwidgets.example.com",
        crd.clone(),
    )
    .await
    .unwrap();
    register_crd_from_value(&registry, &crd).await.unwrap();

    let created = db
        .create_resource(
            "example.com/v1",
            "ClusterWidget",
            None,
            "cw1",
            serde_json::json!({
                "apiVersion": "example.com/v1",
                "kind": "ClusterWidget",
                "metadata": {"name": "cw1"},
                "spec": {"k": "v"}
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        created.namespace, None,
        "cluster-scoped custom resource must store namespace as None"
    );

    let fetched = db
        .get_resource("example.com/v1", "ClusterWidget", None, "cw1")
        .await
        .unwrap()
        .expect("cluster custom resource should be retrievable without namespace");
    assert_eq!(
        fetched.namespace, None,
        "retrieved cluster custom resource must not carry default namespace"
    );
}

/// storedVersions: updating a CRD with a new storage version must preserve the old version.
#[tokio::test]
async fn test_crd_update_preserves_stored_versions_on_storage_version_change() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();
    let app = crate::api::build_router(build_test_app_state(db.clone(), registry.clone()).await);

    // Create CRD with v1 as storage version
    let crd_v1 = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"kind": "Widget", "plural": "widgets", "singular": "widget"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}}
            ]
        }
    });

    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd_v1).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let create_body: Value = serde_json::from_slice(
        &axum::body::to_bytes(create_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let stored = create_body["status"]["storedVersions"].as_array().unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0], "v1");

    // Update CRD to add v2 as the new storage version, v1 as non-storage
    let crd_v2 = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"kind": "Widget", "plural": "widgets", "singular": "widget"},
            "versions": [
                {"name": "v1", "served": true, "storage": false, "schema": {"openAPIV3Schema": {"type": "object"}}},
                {"name": "v2", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}}
            ]
        }
    });

    let update_resp = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/widgets.example.com")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd_v2).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(update_resp.status(), StatusCode::OK);
    let update_body: Value = serde_json::from_slice(
        &axum::body::to_bytes(update_resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let stored_after = update_body["status"]["storedVersions"].as_array().unwrap();
    assert_eq!(
        stored_after.len(),
        2,
        "storedVersions must preserve v1 and add v2: got {:?}",
        stored_after
    );
    assert!(
        stored_after.iter().any(|v| v == "v1"),
        "old storage version v1 must be preserved"
    );
    assert!(
        stored_after.iter().any(|v| v == "v2"),
        "new storage version v2 must be present"
    );
}

/// api-approved: CRD with group ending in .k8s.io must require the annotation.
#[tokio::test]
async fn test_crd_create_rejects_protected_group_without_approval() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();
    let app = crate::api::build_router(build_test_app_state(db, registry).await);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.myapp.k8s.io"},
        "spec": {
            "group": "myapp.k8s.io",
            "scope": "Namespaced",
            "names": {"kind": "Widget", "plural": "widgets", "singular": "widget"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}}
            ]
        }
    });

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(
        &axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let msg = body
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or_default();
    assert!(
        msg.contains("api-approved.kubernetes.io"),
        "error must mention annotation, got: {}",
        msg
    );
}

/// api-approved: CRD with group ending in .k8s.io and valid annotation is accepted.
#[tokio::test]
async fn test_crd_create_accepts_protected_group_with_approval() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();
    let app = crate::api::build_router(build_test_app_state(db, registry).await);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": "widgets.myapp.k8s.io",
            "annotations": {"api-approved.kubernetes.io": "https://github.com/kubernetes/kubernetes/pull/12345"}
        },
        "spec": {
            "group": "myapp.k8s.io",
            "scope": "Namespaced",
            "names": {"kind": "Widget", "plural": "widgets", "singular": "widget"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}}
            ]
        }
    });

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

/// api-approved: non-protected group doesn't need annotation.
#[tokio::test]
async fn test_crd_create_accepts_non_protected_group_without_approval() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();
    let app = crate::api::build_router(build_test_app_state(db, registry).await);

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.example.com"},
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": {"kind": "Widget", "plural": "widgets", "singular": "widget"},
            "versions": [
                {"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}}
            ]
        }
    });

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&crd).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}
