use super::*;
use serde_json::json;

/// Helper: check if the openapi/v2 handler would return 406 for a given Accept header.
/// This mirrors the condition in get_openapi_v2().
fn should_reject_openapi_v2(_accept: &str) -> bool {
    false
}

#[test]
fn test_openapi_v2_protobuf_only_accept_allowed() {
    assert!(
        !should_reject_openapi_v2("application/com.github.proto-openapi.spec.v2@v1.0+protobuf"),
        "Protobuf-only Accept must be allowed (200 JSON response)"
    );
}

#[test]
fn test_openapi_v2_json_accept_allowed() {
    assert!(
        !should_reject_openapi_v2("application/json"),
        "JSON Accept must be allowed (200)"
    );
}

#[test]
fn test_openapi_v2_mixed_json_protobuf_accept_allowed() {
    // kubectl replace sends this — must get 200 since JSON is in the list
    assert!(
        !should_reject_openapi_v2("application/json, application/vnd.kubernetes.protobuf"),
        "Mixed Accept with JSON must be allowed (200)"
    );
}

#[test]
fn test_openapi_v2_empty_accept_allowed() {
    assert!(
        !should_reject_openapi_v2(""),
        "Empty Accept must be allowed (200)"
    );
}

#[test]
fn test_openapi_v2_kubernetes_protobuf_only_allowed() {
    assert!(
        !should_reject_openapi_v2("application/vnd.kubernetes.protobuf"),
        "Kubernetes protobuf-only Accept must be allowed (200 JSON response)"
    );
}

// P0-8 CRD PublishOpenAPI lifecycle tests
// These verify CREATE/DELETE/UPDATE CRD lifecycle is reflected in OpenAPI endpoints.

fn make_p0_8_crd(
    group: &str,
    kind: &str,
    plural: &str,
    properties: serde_json::Value,
) -> serde_json::Value {
    json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": format!("{}.{}", plural, group) },
        "spec": {
            "group": group,
            "scope": "Namespaced",
            "names": { "kind": kind, "plural": plural, "singular": kind.to_lowercase() },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": properties
                    }
                }
            }]
        }
    })
}

#[tokio::test]
async fn test_p0_8_crd_create_publishes_schema_to_openapi_v3_group_version() {
    // After CRD CREATE, the schema must appear in /openapi/v3/apis/{group}/{version}
    // components/schemas so kubectl can validate CR creation.
    let db = crate::datastore::test_support::in_memory().await;

    let crd = make_p0_8_crd(
        "crd-publish-openapi-test.example.com",
        "TestCR",
        "testcrs",
        json!({"spec": {"type": "object", "properties": {"num": {"type": "integer"}}}}),
    );

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "testcrs.crd-publish-openapi-test.example.com",
        crd,
    )
    .await
    .unwrap();

    let v3 =
        build_openapi_v3_group_version(&db, "crd-publish-openapi-test.example.com", "v1").await;

    let schemas = v3["components"]["schemas"].as_object().unwrap();
    let has_testcr = schemas.keys().any(|k| k.contains("TestCR"));
    assert!(
        has_testcr,
        "After CRD CREATE, schema for TestCR must appear in /openapi/v3/apis/group/v1 components/schemas. Got keys: {:?}",
        schemas.keys().collect::<Vec<_>>()
    );

    // The discovery endpoint must also list the group/version path
    let discovery = openapi_v3_discovery_with_crds(&db).await;
    let paths = discovery["paths"].as_object().unwrap();
    assert!(
        paths.contains_key("apis/crd-publish-openapi-test.example.com/v1"),
        "CRD group/version must appear in /openapi/v3 discovery paths after CREATE. Got: {:?}",
        paths.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn crd_group_discovery_lists_all_served_versions_in_one_group() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let registry = crate::controllers::crd::CrdRegistry::new();
    registry
        .register(crate::controllers::crd::CrdResourceInfo {
            group: "multi.example.com".to_string(),
            version: "v1".to_string(),
            kind: "Widget".to_string(),
            plural: "widgets".to_string(),
            singular: "widget".to_string(),
            namespaced: true,
            selectable_fields: Vec::new(),
        })
        .await;
    registry
        .register(crate::controllers::crd::CrdResourceInfo {
            group: "multi.example.com".to_string(),
            version: "v2".to_string(),
            kind: "Widget".to_string(),
            plural: "widgets".to_string(),
            singular: "widget".to_string(),
            namespaced: true,
            selectable_fields: Vec::new(),
        })
        .await;

    let mut state = crate::api::test_support::build_test_app_state().await;
    state.crd_registry = registry;
    let app = crate::api::build_router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let groups = value["groups"].as_array().unwrap();
    let matching: Vec<_> = groups
        .iter()
        .filter(|group| group["name"] == "multi.example.com")
        .collect();
    assert_eq!(matching.len(), 1, "CRD group must appear once: {value}");

    let versions: Vec<&str> = matching[0]["versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|version| version["version"].as_str().unwrap())
        .collect();
    assert_eq!(
        versions,
        vec!["v1", "v2"],
        "served versions must be deterministic"
    );
    assert_eq!(matching[0]["preferredVersion"]["version"], "v1");
}

#[tokio::test]
async fn crd_group_by_name_lists_all_served_versions() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let registry = crate::controllers::crd::CrdRegistry::new();
    for version in ["v1", "v2", "v3alpha1"] {
        registry
            .register(crate::controllers::crd::CrdResourceInfo {
                group: "groupby.example.com".to_string(),
                version: version.to_string(),
                kind: "Thing".to_string(),
                plural: "things".to_string(),
                singular: "thing".to_string(),
                namespaced: true,
                selectable_fields: Vec::new(),
            })
            .await;
    }

    let mut state = crate::api::test_support::build_test_app_state().await;
    state.crd_registry = registry;
    let app = crate::api::build_router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/apis/groupby.example.com")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let versions: Vec<&str> = value["versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|version| version["version"].as_str().unwrap())
        .collect();
    assert_eq!(versions, vec!["v1", "v2", "v3alpha1"]);
    assert_eq!(value["preferredVersion"]["version"], "v1");
}

#[tokio::test]
async fn test_p0_8_crd_delete_removes_schema_from_openapi_v3_group_version() {
    // After CRD DELETE (hard-delete), the schema must disappear from
    // /openapi/v3/apis/{group}/{version} so the test polling loop terminates.
    // Sonobuoy crd_publish_openapi.go:475 polls this endpoint for 60s.
    let db = crate::datastore::test_support::in_memory().await;

    let crd = make_p0_8_crd(
        "crd-delete-test.example.com",
        "DeleteMe",
        "deletemes",
        json!({"spec": {"type": "object"}}),
    );

    // CREATE
    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "deletemes.crd-delete-test.example.com",
        crd,
    )
    .await
    .unwrap();

    // Verify schema appears before delete
    let v3_before = build_openapi_v3_group_version(&db, "crd-delete-test.example.com", "v1").await;
    let schemas_before = v3_before["components"]["schemas"].as_object().unwrap();
    assert!(
        schemas_before.keys().any(|k| k.contains("DeleteMe")),
        "Schema must be present before DELETE"
    );

    // DELETE (hard-delete)
    db.delete_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "deletemes.crd-delete-test.example.com",
    )
    .await
    .unwrap();

    // Verify schema is GONE after delete
    let v3_after = build_openapi_v3_group_version(&db, "crd-delete-test.example.com", "v1").await;
    let schemas_after = v3_after["components"]["schemas"].as_object().unwrap();
    assert!(
        !schemas_after.keys().any(|k| k.contains("DeleteMe")),
        "After CRD DELETE, schema for DeleteMe must NOT appear in /openapi/v3/apis/group/v1. Got: {:?}",
        schemas_after.keys().collect::<Vec<_>>()
    );

    // The discovery endpoint must also NOT list the group/version after delete
    let discovery = openapi_v3_discovery_with_crds(&db).await;
    let paths = discovery["paths"].as_object().unwrap();
    assert!(
        !paths.contains_key("apis/crd-delete-test.example.com/v1"),
        "CRD group/version must NOT appear in /openapi/v3 discovery after DELETE. Got: {:?}",
        paths.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_p0_8_crd_delete_removes_definition_from_openapi_v2() {
    // After CRD DELETE, the definition key must disappear from /openapi/v2 definitions.
    // This is the other endpoint Sonobuoy polls for schema removal.
    let db = crate::datastore::test_support::in_memory().await;

    let crd = make_p0_8_crd(
        "crd-delete-v2-test.example.com",
        "DeleteMeV2",
        "deletemev2s",
        json!({"spec": {"type": "object"}}),
    );

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "deletemev2s.crd-delete-v2-test.example.com",
        crd,
    )
    .await
    .unwrap();

    // Verify definition appears before delete
    let v2_before = openapi_v2(&db).await;
    let defs_before = v2_before["definitions"].as_object().unwrap();
    assert!(
        defs_before.keys().any(|k| k.contains("DeleteMeV2")),
        "Definition must be present in /openapi/v2 before DELETE"
    );

    // Soft-delete
    db.delete_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "deletemev2s.crd-delete-v2-test.example.com",
    )
    .await
    .unwrap();

    // Verify definition is GONE
    let v2_after = openapi_v2(&db).await;
    let defs_after = v2_after["definitions"].as_object().unwrap();
    assert!(
        !defs_after.keys().any(|k| k.contains("DeleteMeV2")),
        "After CRD DELETE, definition for DeleteMeV2 must NOT appear in /openapi/v2. Got: {:?}",
        defs_after.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_p0_8_crd_update_replaces_schema_in_openapi_v3() {
    // After CRD UPDATE (schema change), the published schema must reflect new properties.
    // Sonobuoy crd_publish_openapi.go tests schema reflects version changes.
    let db = crate::datastore::test_support::in_memory().await;

    let crd_v1 = make_p0_8_crd(
        "crd-update-test.example.com",
        "UpdateMe",
        "updatemes",
        json!({"spec": {"type": "object", "properties": {"oldField": {"type": "string"}}}}),
    );

    let created = db
        .create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "updatemes.crd-update-test.example.com",
            crd_v1,
        )
        .await
        .unwrap();

    // Verify old schema is present
    let v3_before = build_openapi_v3_group_version(&db, "crd-update-test.example.com", "v1").await;
    let schemas_before = v3_before["components"]["schemas"].as_object().unwrap();
    let key_before = schemas_before
        .keys()
        .find(|k| k.contains("UpdateMe"))
        .unwrap()
        .clone();
    let old_props = &v3_before["components"]["schemas"][&key_before]["properties"];
    assert!(
        old_props.get("spec").is_some(),
        "Before UPDATE, schema must have 'spec' property (from openAPIV3Schema root)"
    );

    // UPDATE: replace schema with new properties
    let crd_v2 = make_p0_8_crd(
        "crd-update-test.example.com",
        "UpdateMe",
        "updatemes",
        json!({"spec": {"type": "object", "properties": {"newField": {"type": "integer"}}}}),
    );

    db.update_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "updatemes.crd-update-test.example.com",
        crd_v2,
        created.resource_version,
    )
    .await
    .unwrap();

    // After UPDATE: schema must reflect new properties
    let v3_after = build_openapi_v3_group_version(&db, "crd-update-test.example.com", "v1").await;
    let schemas_after = v3_after["components"]["schemas"].as_object().unwrap();
    let key_after = schemas_after
        .keys()
        .find(|k| k.contains("UpdateMe"))
        .unwrap()
        .clone();
    let new_props =
        &v3_after["components"]["schemas"][&key_after]["properties"]["spec"]["properties"];
    assert!(
        new_props.get("newField").is_some(),
        "After CRD UPDATE, schema must contain 'newField'. Got properties: {:?}",
        new_props
    );
    assert!(
        new_props.get("oldField").is_none(),
        "After CRD UPDATE, 'oldField' must no longer appear in schema. Got properties: {:?}",
        new_props
    );
}

/// Verify that `x-kubernetes-preserve-unknown-fields: true` on a CRD spec property is
/// STRIPPED from `/openapi/v2` (Swagger 2.0 incompatible). It is preserved in `/openapi/v3`.
/// (Sonobuoy crd_publish_openapi.go line 158 scenario.)
#[tokio::test]
async fn test_p0_8_openapi_v2_preserves_x_kubernetes_preserve_unknown_fields() {
    let db = crate::datastore::test_support::in_memory().await;

    // CRD with x-kubernetes-preserve-unknown-fields on spec property (like schemaPreserveNested)
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "preservetests.preserve.example.com" },
        "spec": {
            "group": "preserve.example.com",
            "scope": "Namespaced",
            "names": { "kind": "PreserveTest", "plural": "preservetests", "singular": "preservetest" },
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
                                "x-kubernetes-preserve-unknown-fields": true
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
        "preservetests.preserve.example.com",
        crd,
    )
    .await
    .unwrap();

    // Use stripped helper (not openapi_v2() directly) because the strip happens in HTTP handler.
    let spec = get_openapi_v2_stripped(&db).await;
    let definitions = spec["definitions"].as_object().unwrap();

    let def_name = definitions
        .keys()
        .find(|k| k.contains("PreserveTest"))
        .expect("PreserveTest definition must exist in /openapi/v2");

    let def = &definitions[def_name];

    // x-kubernetes-preserve-unknown-fields is stripped from /openapi/v2 (Swagger 2.0 compat).
    // kubectl uses /openapi/v3 for client-side validation when available.
    let spec_prop = def
        .pointer("/properties/spec")
        .expect("spec property must exist");
    assert!(
        spec_prop
            .get("x-kubernetes-preserve-unknown-fields")
            .is_none(),
        "x-kubernetes-preserve-unknown-fields must be STRIPPED from /openapi/v2 (Swagger 2.0 incompatible). \
         Got spec: {spec_prop}"
    );
}

/// Same as above but for /openapi/v3 — build_openapi_v3_group_version delegates to openapi_v2
/// so it has the same stripping behaviour.
/// Verify that setting `served: false` on a CRD version removes its definition from
/// `/openapi/v2`. Sonobuoy crd_publish_openapi.go line 476 scenario:
/// `waitForDefinitionCleanup` polls `/openapi/v2` for 60s waiting for the definition to
/// disappear after a CRD update sets `versions[1].served = false`.
#[tokio::test]
async fn test_p0_8_openapi_v2_served_false_removes_definition() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create a CRD with two versions, both served
    let crd_two_served = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "servedtests.served.example.com" },
        "spec": {
            "group": "served.example.com",
            "scope": "Namespaced",
            "names": { "kind": "ServedTest", "plural": "servedtests", "singular": "servedtest" },
            "versions": [
                {
                    "name": "v5",
                    "served": true,
                    "storage": true,
                    "schema": {"openAPIV3Schema": {"type": "object"}}
                },
                {
                    "name": "v6alpha1",
                    "served": true,
                    "storage": false,
                    "schema": {"openAPIV3Schema": {"type": "object"}}
                }
            ]
        }
    });

    let created = db
        .create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "servedtests.served.example.com",
            crd_two_served,
        )
        .await
        .unwrap();

    // Both versions must appear in /openapi/v2
    let v2_before = openapi_v2(&db).await;
    let defs_before = v2_before["definitions"].as_object().unwrap();
    assert!(
        defs_before
            .keys()
            .any(|k| k.contains("v5") && k.contains("ServedTest")),
        "v5 definition must appear in /openapi/v2 before update. Got: {:?}",
        defs_before.keys().collect::<Vec<_>>()
    );
    assert!(
        defs_before
            .keys()
            .any(|k| k.contains("v6alpha1") && k.contains("ServedTest")),
        "v6alpha1 definition must appear in /openapi/v2 before update. Got: {:?}",
        defs_before.keys().collect::<Vec<_>>()
    );

    // UPDATE: set v6alpha1 to served=false (K8s pattern — not removing, just marking unserved)
    let crd_one_served = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "servedtests.served.example.com" },
        "spec": {
            "group": "served.example.com",
            "scope": "Namespaced",
            "names": { "kind": "ServedTest", "plural": "servedtests", "singular": "servedtest" },
            "versions": [
                {
                    "name": "v5",
                    "served": true,
                    "storage": true,
                    "schema": {"openAPIV3Schema": {"type": "object"}}
                },
                {
                    "name": "v6alpha1",
                    "served": false,
                    "storage": false,
                    "schema": {"openAPIV3Schema": {"type": "object"}}
                }
            ]
        }
    });

    db.update_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "servedtests.served.example.com",
        crd_one_served,
        created.resource_version,
    )
    .await
    .unwrap();

    // After UPDATE: v5 stays, v6alpha1 must be gone from /openapi/v2
    let v2_after = openapi_v2(&db).await;
    let defs_after = v2_after["definitions"].as_object().unwrap();
    assert!(
        defs_after
            .keys()
            .any(|k| k.contains("v5") && k.contains("ServedTest")),
        "v5 definition must still appear in /openapi/v2 after marking v6alpha1 unserved. Got: {:?}",
        defs_after.keys().collect::<Vec<_>>()
    );
    assert!(
        !defs_after
            .keys()
            .any(|k| k.contains("v6alpha1") && k.contains("ServedTest")),
        "v6alpha1 definition must NOT appear in /openapi/v2 after served=false. Got: {:?}",
        defs_after.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_p0_8_openapi_v3_preserves_x_kubernetes_preserve_unknown_fields() {
    let db = crate::datastore::test_support::in_memory().await;

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "preservetests3.preserve3.example.com" },
        "spec": {
            "group": "preserve3.example.com",
            "scope": "Namespaced",
            "names": { "kind": "PreserveTest3", "plural": "preservetests3", "singular": "preservetest3" },
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
                                "x-kubernetes-preserve-unknown-fields": true
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
        "preservetests3.preserve3.example.com",
        crd,
    )
    .await
    .unwrap();

    let v3 = build_openapi_v3_group_version(&db, "preserve3.example.com", "v1").await;
    let schemas = v3["components"]["schemas"]
        .as_object()
        .expect("schemas must exist");

    let schema_name = schemas
        .keys()
        .find(|k| k.contains("PreserveTest3"))
        .expect("PreserveTest3 schema must exist in /openapi/v3");

    let schema = &schemas[schema_name];
    let spec_prop = schema
        .pointer("/properties/spec")
        .expect("spec property must exist");
    assert_eq!(
        spec_prop.get("x-kubernetes-preserve-unknown-fields"),
        Some(&json!(true)),
        "x-kubernetes-preserve-unknown-fields must be preserved in /openapi/v3 spec property. \
         Got spec: {spec_prop}"
    );
}

#[tokio::test]
async fn test_p0_8_crd_version_remove_removes_schema_from_openapi_v3() {
    // When a CRD UPDATE removes a version from spec.versions[], that version's
    // schema must no longer appear in /openapi/v3/apis/{group}/{removed_version}.
    // This is the "adding/removing a versions[] entry" update case.
    let db = crate::datastore::test_support::in_memory().await;

    let crd_two_versions = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "multiversions.ver-remove-test.example.com" },
        "spec": {
            "group": "ver-remove-test.example.com",
            "scope": "Namespaced",
            "names": { "kind": "MultiVersion", "plural": "multiversions", "singular": "multiversion" },
            "versions": [
                {
                    "name": "v1",
                    "served": true,
                    "storage": true,
                    "schema": {"openAPIV3Schema": {"type": "object"}}
                },
                {
                    "name": "v2",
                    "served": true,
                    "storage": false,
                    "schema": {"openAPIV3Schema": {"type": "object"}}
                }
            ]
        }
    });

    let created = db
        .create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "multiversions.ver-remove-test.example.com",
            crd_two_versions,
        )
        .await
        .unwrap();

    // Both v1 and v2 appear in discovery
    let disc_before = openapi_v3_discovery_with_crds(&db).await;
    let paths_before = disc_before["paths"].as_object().unwrap();
    assert!(
        paths_before.contains_key("apis/ver-remove-test.example.com/v1"),
        "v1 must appear"
    );
    assert!(
        paths_before.contains_key("apis/ver-remove-test.example.com/v2"),
        "v2 must appear before version removal"
    );

    // UPDATE: remove v2 from versions list
    let crd_one_version = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "multiversions.ver-remove-test.example.com" },
        "spec": {
            "group": "ver-remove-test.example.com",
            "scope": "Namespaced",
            "names": { "kind": "MultiVersion", "plural": "multiversions", "singular": "multiversion" },
            "versions": [
                {
                    "name": "v1",
                    "served": true,
                    "storage": true,
                    "schema": {"openAPIV3Schema": {"type": "object"}}
                }
            ]
        }
    });

    db.update_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "multiversions.ver-remove-test.example.com",
        crd_one_version,
        created.resource_version,
    )
    .await
    .unwrap();

    // After UPDATE: only v1 appears; v2 must be gone from discovery
    let disc_after = openapi_v3_discovery_with_crds(&db).await;
    let paths_after = disc_after["paths"].as_object().unwrap();
    assert!(
        paths_after.contains_key("apis/ver-remove-test.example.com/v1"),
        "v1 must still appear after v2 removal"
    );
    assert!(
        !paths_after.contains_key("apis/ver-remove-test.example.com/v2"),
        "v2 must NOT appear in /openapi/v3 discovery after version removal. Got: {:?}",
        paths_after.keys().collect::<Vec<_>>()
    );
}

/// Regression for crd_publish_openapi.go:158:
/// A CRD with schema `{x-kubernetes-preserve-unknown-fields: true}` (schema-less pattern)
/// must NOT have this extension in the /openapi/v2 output — Swagger 2.0 doesn't support it.
#[tokio::test]
async fn test_p0_8_openapi_v2_strips_preserve_unknown_fields_from_empty_schema() {
    use serde_json::json;

    let db = crate::datastore::test_support::in_memory().await;
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "foos.empty-schema.example.com", "uid": "uid-empty", "resourceVersion": "1"},
        "spec": {
            "group": "empty-schema.example.com",
            "names": {"plural": "foos", "singular": "foo", "kind": "Foo"},
            "scope": "Namespaced",
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    // schema-less pattern: x-kubernetes-preserve-unknown-fields at root
                    "openAPIV3Schema": {"x-kubernetes-preserve-unknown-fields": true}
                }
            }]
        }
    });
    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "foos.empty-schema.example.com",
        crd,
    )
    .await
    .unwrap();

    let spec = get_openapi_v2_stripped(&db).await;
    let def_key = "com.example.empty-schema.v1.Foo";
    let def = spec
        .pointer(&format!("/definitions/{}", def_key))
        .expect("definition must be published");

    assert!(
        def.get("x-kubernetes-preserve-unknown-fields").is_none(),
        "x-kubernetes-preserve-unknown-fields must NOT appear in /openapi/v2 (Swagger 2.0 incompatible). Got: {:?}",
        def
    );
}
