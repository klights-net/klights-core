use super::*;
use serde_json::json;

#[tokio::test]
async fn test_crd_with_status_subresource() {
    let db = crate::datastore::test_support::in_memory().await;
    let registry = CrdRegistry::new();

    // Create CRD with subresources: { status: {} }
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": "certificates.cert-manager.io"
        },
        "spec": {
            "group": "cert-manager.io",
            "scope": "Namespaced",
            "names": {
                "kind": "Certificate",
                "plural": "certificates",
                "singular": "certificate"
            },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "subresources": {
                    "status": {}
                },
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": {"type": "object"},
                            "status": {"type": "object"}
                        }
                    }
                }
            }]
        }
    });

    // Register it
    register_crd_from_value(&registry, &crd).await.unwrap();

    // Store the CRD in the database
    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "certificates.cert-manager.io",
        crd.clone(),
    )
    .await
    .unwrap();

    // Verify the CRD spec includes status subresource
    let retrieved = db
        .get_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "certificates.cert-manager.io",
        )
        .await
        .unwrap()
        .unwrap();

    let subresources = retrieved.data["spec"]["versions"][0]
        .get("subresources")
        .and_then(|s| s.get("status"));
    assert!(
        subresources.is_some(),
        "CRD should include status subresource"
    );
}

// ── T1: CRD authorization tests ──

/// Verify that CRD local operations are denied with a denying authorizer before
/// any datastore mutation or watch stream is opened.
#[tokio::test]
async fn crd_operations_denied_with_deny_authorizer() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let authorizer: std::sync::Arc<dyn crate::auth::authorizer::Authorizer> =
        std::sync::Arc::new(crate::auth::authorizer::DenyAuthorizer);
    let state = crate::api::test_support::build_test_app_state_with_authorizer(authorizer).await;

    // Register a CRD locally
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "denywidgets.example-deny.com"},
        "spec": {
            "group": "example-deny.com",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object"}}
            }],
            "scope": "Namespaced",
            "names": {
                "plural": "denywidgets",
                "singular": "denywidget",
                "kind": "DenyWidget"
            }
        }
    });
    state
        .db
        .create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "denywidgets.example-deny.com",
            crd,
        )
        .await
        .unwrap();

    // Now register the CRD in the registry (normally done by the controller, but for
    // tests we do it manually)
    let crd_info = crate::controllers::crd::CrdResourceInfo {
        group: "example-deny.com".to_string(),
        version: "v1".to_string(),
        kind: "DenyWidget".to_string(),
        plural: "denywidgets".to_string(),
        singular: "denywidget".to_string(),
        namespaced: true,
        selectable_fields: vec![],
    };
    state.crd_registry.register(crd_info).await;

    let app = crate::api::build_router(state);

    let tests = vec![
        (
            "GET",
            "/apis/example-deny.com/v1/namespaces/default/denywidgets",
            "list",
        ),
        (
            "POST",
            "/apis/example-deny.com/v1/namespaces/default/denywidgets",
            "create",
        ),
        (
            "DELETE",
            "/apis/example-deny.com/v1/namespaces/default/denywidgets",
            "deletecollection",
        ),
        (
            "GET",
            "/apis/example-deny.com/v1/namespaces/default/denywidgets/test",
            "get",
        ),
        (
            "PUT",
            "/apis/example-deny.com/v1/namespaces/default/denywidgets/test",
            "update",
        ),
        (
            "PATCH",
            "/apis/example-deny.com/v1/namespaces/default/denywidgets/test",
            "patch",
        ),
        (
            "DELETE",
            "/apis/example-deny.com/v1/namespaces/default/denywidgets/test",
            "delete",
        ),
    ];

    for (method, uri, _verb) in &tests {
        let builder = Request::builder()
            .method(*method)
            .uri(*uri)
            .header("content-type", "application/json");
        let req = if *method == "POST" || *method == "PUT" || *method == "PATCH" {
            builder.body(Body::from(
                serde_json::to_vec(&json!({"metadata":{"name":"test"}})).unwrap(),
            ))
        } else {
            builder.body(Body::empty())
        }
        .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "{method} {uri} should return 403 with deny-all authorizer, got {}",
            resp.status()
        );
    }
}

/// Verify that an allowed identity can access CRD resources and get normal responses.
#[tokio::test]
async fn crd_allowed_identity_gets_normal_response() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let authorizer: std::sync::Arc<dyn crate::auth::authorizer::Authorizer> =
        std::sync::Arc::new(crate::auth::authorizer::AllowAllAuthorizer);
    let state = crate::api::test_support::build_test_app_state_with_authorizer(authorizer).await;

    // Register a CRD
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "allowwidgets2.example-allow2.com"},
        "spec": {
            "group": "example-allow2.com",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object"}}
            }],
            "scope": "Namespaced",
            "names": {
                "plural": "allowwidgets2",
                "singular": "allowwidget2",
                "kind": "AllowWidget2"
            }
        }
    });
    state
        .db
        .create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "allowwidgets2.example-allow2.com",
            crd,
        )
        .await
        .unwrap();

    let crd_info = crate::controllers::crd::CrdResourceInfo {
        group: "example-allow2.com".to_string(),
        version: "v1".to_string(),
        kind: "AllowWidget2".to_string(),
        plural: "allowwidgets2".to_string(),
        singular: "allowwidget2".to_string(),
        namespaced: true,
        selectable_fields: vec![],
    };
    state.crd_registry.register(crd_info).await;

    let app = crate::api::build_router(state);

    // Create should succeed
    let widget = json!({"metadata": {"name": "test"}, "spec": {"value": 42}});
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/apis/example-allow2.com/v1/namespaces/default/allowwidgets2")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&widget).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Get should succeed
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example-allow2.com/v1/namespaces/default/allowwidgets2/test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // List should succeed
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/example-allow2.com/v1/namespaces/default/allowwidgets2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ── T1: selector propagation in CRD list/watch/deletecollection authz ──

/// Verify that fieldSelector and labelSelector are passed through to the
/// authorization request for CRD list, watch, and deletecollection operations.
#[tokio::test]
async fn crd_list_authorization_preserves_field_and_label_selectors() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    let recording = std::sync::Arc::new(crate::auth::authorizer::RecordingAuthorizer::allow());
    let authorizer: std::sync::Arc<dyn crate::auth::authorizer::Authorizer> =
        recording.clone() as std::sync::Arc<dyn crate::auth::authorizer::Authorizer>;
    let state =
        crate::api::test_support::build_test_app_state_with_authorizer(authorizer.clone()).await;

    // Register a CRD
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "selectorwidgets.example-sel.com"},
        "spec": {
            "group": "example-sel.com",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object"}}
            }],
            "scope": "Namespaced",
            "names": {
                "plural": "selectorwidgets",
                "singular": "selectorwidget",
                "kind": "SelectorWidget"
            }
        }
    });
    state
        .db
        .create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "selectorwidgets.example-sel.com",
            crd,
        )
        .await
        .unwrap();

    let crd_info = crate::controllers::crd::CrdResourceInfo {
        group: "example-sel.com".to_string(),
        version: "v1".to_string(),
        kind: "SelectorWidget".to_string(),
        plural: "selectorwidgets".to_string(),
        singular: "selectorwidget".to_string(),
        namespaced: true,
        selectable_fields: vec!["metadata.name".to_string()],
    };
    state.crd_registry.register(crd_info).await;

    let app = crate::api::build_router(state);
    let base = "/apis/example-sel.com/v1/namespaces/default/selectorwidgets";

    // 1. List with both selectors
    recording.take_requests().await;
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "{base}?fieldSelector=metadata.name%3Dfoo&labelSelector=a%3Db"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list_reqs = recording.take_requests().await;
    assert!(!list_reqs.is_empty(), "list request should be authorized");
    let list_authz = &list_reqs[0].1;
    assert_eq!(
        list_authz.field_selector.as_deref(),
        Some("metadata.name=foo")
    );
    assert_eq!(list_authz.label_selector.as_deref(), Some("a=b"));

    // 2. Watch with selectors
    recording.take_requests().await;
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "{base}?watch=true&fieldSelector=metadata.name%3Dbar&labelSelector=x%3Dy"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Watch returns 200 with chunked transfer encoding
    assert_eq!(resp.status(), StatusCode::OK);
    let watch_reqs = recording.take_requests().await;
    assert!(!watch_reqs.is_empty(), "watch request should be authorized");
    let watch_authz = &watch_reqs[0].1;
    assert_eq!(
        watch_authz.field_selector.as_deref(),
        Some("metadata.name=bar")
    );
    assert_eq!(watch_authz.label_selector.as_deref(), Some("x=y"));

    // 3. DeleteCollection with selectors
    recording.take_requests().await;
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!(
                    "{base}?fieldSelector=metadata.name%3Dbaz&labelSelector=c%3Dd"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let del_reqs = recording.take_requests().await;
    assert!(
        !del_reqs.is_empty(),
        "deletecollection request should be authorized"
    );
    let del_authz = &del_reqs[0].1;
    assert_eq!(
        del_authz.field_selector.as_deref(),
        Some("metadata.name=baz")
    );
    assert_eq!(del_authz.label_selector.as_deref(), Some("c=d"));
}

/// Verify that RBAC resourceNames evaluation considers the field selector.
/// Uses a RecordingAuthorizer to verify the selector is present in the authz
/// request made by the CRD list handler.
#[tokio::test]
async fn crd_rbac_resource_names_uses_field_selector() {
    use axum::body::Body;
    use axum::http::Request;
    use serde_json::json;
    use tower::ServiceExt;

    let recording = std::sync::Arc::new(crate::auth::authorizer::RecordingAuthorizer::allow());
    let authorizer: std::sync::Arc<dyn crate::auth::authorizer::Authorizer> =
        recording.clone() as std::sync::Arc<dyn crate::auth::authorizer::Authorizer>;
    let state =
        crate::api::test_support::build_test_app_state_with_authorizer(authorizer.clone()).await;

    // Register a CRD
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "rnswidgets.example-rns.com"},
        "spec": {
            "group": "example-rns.com",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object"}}
            }],
            "scope": "Namespaced",
            "names": {
                "plural": "rnswidgets",
                "singular": "rnswidget",
                "kind": "RnsWidget"
            }
        }
    });
    state
        .db
        .create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "rnswidgets.example-rns.com",
            crd,
        )
        .await
        .unwrap();

    let crd_info = crate::controllers::crd::CrdResourceInfo {
        group: "example-rns.com".to_string(),
        version: "v1".to_string(),
        kind: "RnsWidget".to_string(),
        plural: "rnswidgets".to_string(),
        singular: "rnswidget".to_string(),
        namespaced: true,
        selectable_fields: vec!["metadata.name".to_string()],
    };
    state.crd_registry.register(crd_info).await;

    let app = crate::api::build_router(state);
    let base = "/apis/example-rns.com/v1/namespaces/default/rnswidgets";

    // List without selectors — selector fields should be None
    recording.take_requests().await;
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(base)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let no_sel = recording.take_requests().await;
    assert!(no_sel[0].1.field_selector.is_none());
    assert!(no_sel[0].1.label_selector.is_none());

    // List with fieldSelector=metadata.name=allowed
    recording.take_requests().await;
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("{base}?fieldSelector=metadata.name%3Dallowed"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let sel = recording.take_requests().await;
    assert_eq!(
        sel[0].1.field_selector.as_deref(),
        Some("metadata.name=allowed")
    );
}

/// Verify that RBAC resourceNames evaluation requires a matching
/// fieldSelector for list and watch operations on CRD resources.
#[tokio::test]
async fn crd_rbac_resource_names_requires_matching_metadata_name_selector() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use serde_json::json;
    use tower::ServiceExt;

    // Build a real RBAC authorizer with bindings that grant list/watch
    // on the CRD plurals but restrict to resourceNames: ["allowed"].
    let namespaced_binding = crate::auth::rbac_policy_store::ResolvedBinding {
        subjects: vec![crate::auth::rbac_rule_evaluator::Subject {
            kind: crate::auth::rbac_rule_evaluator::SubjectKind::User,
            name: "test-user".to_string(),
            namespace: None,
        }],
        rules: vec![crate::auth::rbac_rule_evaluator::PolicyRule {
            verbs: vec!["list".to_string(), "watch".to_string()],
            api_groups: vec!["example-rbac.com".to_string()],
            resources: vec!["rbacwidgets".to_string()],
            resource_names: vec!["allowed".to_string()],
            non_resource_urls: vec![],
        }],
        namespace: None,
    };
    let cluster_binding = crate::auth::rbac_policy_store::ResolvedBinding {
        subjects: vec![crate::auth::rbac_rule_evaluator::Subject {
            kind: crate::auth::rbac_rule_evaluator::SubjectKind::User,
            name: "test-user".to_string(),
            namespace: None,
        }],
        rules: vec![crate::auth::rbac_rule_evaluator::PolicyRule {
            verbs: vec!["list".to_string(), "watch".to_string()],
            api_groups: vec!["example-rbac.com".to_string()],
            resources: vec!["clusterrbacs".to_string()],
            resource_names: vec!["allowed".to_string()],
            non_resource_urls: vec![],
        }],
        namespace: None,
    };
    let store = crate::auth::rbac_policy_store::InMemoryRbacPolicyStore::new(vec![
        namespaced_binding,
        cluster_binding,
    ]);
    let rbac: std::sync::Arc<dyn crate::auth::authorizer::Authorizer> = std::sync::Arc::new(
        crate::auth::rbac_authorizer::RbacAuthorizer::new(std::sync::Arc::new(store)),
    );

    let state = crate::api::test_support::build_test_app_state_with_authorizer(rbac).await;

    // Register a namespaced CRD with selectableFields: ["metadata.name"]
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "rbacwidgets.example-rbac.com"},
        "spec": {
            "group": "example-rbac.com",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object"}}
            }],
            "scope": "Namespaced",
            "names": {
                "plural": "rbacwidgets",
                "singular": "rbacwidget",
                "kind": "RbacWidget"
            }
        }
    });
    state
        .db
        .create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "rbacwidgets.example-rbac.com",
            crd,
        )
        .await
        .unwrap();

    let crd_info = crate::controllers::crd::CrdResourceInfo {
        group: "example-rbac.com".to_string(),
        version: "v1".to_string(),
        kind: "RbacWidget".to_string(),
        plural: "rbacwidgets".to_string(),
        singular: "rbacwidget".to_string(),
        namespaced: true,
        selectable_fields: vec!["metadata.name".to_string()],
    };
    state.crd_registry.register(crd_info).await;

    // Also register a cluster-scoped CRD before building the router
    let crd_cluster = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "clusterrbacs.example-rbac.com"},
        "spec": {
            "group": "example-rbac.com",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {"type": "object"}}
            }],
            "scope": "Cluster",
            "names": {
                "plural": "clusterrbacs",
                "singular": "clusterrbac",
                "kind": "ClusterRbac"
            }
        }
    });
    state
        .db
        .create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "clusterrbacs.example-rbac.com",
            crd_cluster,
        )
        .await
        .unwrap();
    let crd_info_cluster = crate::controllers::crd::CrdResourceInfo {
        group: "example-rbac.com".to_string(),
        version: "v1".to_string(),
        kind: "ClusterRbac".to_string(),
        plural: "clusterrbacs".to_string(),
        singular: "clusterrbac".to_string(),
        namespaced: false,
        selectable_fields: vec!["metadata.name".to_string()],
    };
    state.crd_registry.register(crd_info_cluster).await;

    let app = crate::api::build_router(state);
    let base = "/apis/example-rbac.com/v1/namespaces/default/rbacwidgets";
    let non_admin =
        crate::auth::identity::AuthenticatedIdentity::client_cert("test-user".to_string(), vec![]);

    // 1. List with fieldSelector=metadata.name=allowed should succeed
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("{base}?fieldSelector=metadata.name%3Dallowed"))
                .extension(non_admin.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "list with matching fieldSelector should succeed"
    );

    // 2. List without fieldSelector should return 403
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(base)
                .extension(non_admin.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "list without fieldSelector should return 403"
    );

    // 3. List with fieldSelector=metadata.name=denied should return 403
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("{base}?fieldSelector=metadata.name%3Ddenied"))
                .extension(non_admin.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "list with non-matching fieldSelector should return 403"
    );

    // 4. Watch with fieldSelector=metadata.name=allowed should succeed
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "{base}?watch=true&fieldSelector=metadata.name%3Dallowed"
                ))
                .extension(non_admin.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "watch with matching fieldSelector should succeed"
    );

    let cluster_base = "/apis/example-rbac.com/v1/clusterrbacs";

    // 5. Cluster-scoped list with matching fieldSelector should succeed
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "{cluster_base}?fieldSelector=metadata.name%3Dallowed"
                ))
                .extension(non_admin.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "cluster-scoped list with matching fieldSelector should succeed"
    );

    // 6. Cluster-scoped list without fieldSelector should return 403
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(cluster_base)
                .extension(non_admin.clone())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "cluster-scoped list without fieldSelector should return 403"
    );
}
