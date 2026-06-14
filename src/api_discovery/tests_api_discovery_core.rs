use super::*;
use serde_json::json;

#[test]
fn test_aggregated_discovery_includes_short_names_for_v1_resources() {
    let resources = core_v1_aggregated_resources();
    let find = |name: &str| resources.iter().find(|r| r.resource == name).cloned();

    let rc = find("replicationcontrollers")
        .expect("replicationcontrollers must be in v1 aggregated discovery");
    assert_eq!(
        rc.short_names.as_deref(),
        Some(&["rc".to_string()][..]),
        "rc short name must be present"
    );

    let svc = find("services").expect("services must be in v1 aggregated discovery");
    assert_eq!(
        svc.short_names.as_deref(),
        Some(&["svc".to_string()][..]),
        "svc short name must be present"
    );

    let po = find("pods").expect("pods must be in v1 aggregated discovery");
    assert_eq!(
        po.short_names.as_deref(),
        Some(&["po".to_string()][..]),
        "po short name must be present"
    );
}

#[test]
fn test_aggregated_discovery_includes_short_names_for_apps_resources() {
    let resources = aggregated_resources_for_group_version("apps", "v1");
    let find = |name: &str| resources.iter().find(|r| r.resource == name).cloned();

    let deploy = find("deployments").expect("deployments must be in apps aggregated discovery");
    assert_eq!(
        deploy.short_names.as_deref(),
        Some(&["deploy".to_string()][..]),
        "deploy short name must be present"
    );

    let rs = find("replicasets").expect("replicasets must be in apps aggregated discovery");
    assert_eq!(
        rs.short_names.as_deref(),
        Some(&["rs".to_string()][..]),
        "rs short name must be present"
    );

    let sts = find("statefulsets").expect("statefulsets must be in apps aggregated discovery");
    assert_eq!(
        sts.short_names.as_deref(),
        Some(&["sts".to_string()][..]),
        "sts short name must be present"
    );

    let ds = find("daemonsets").expect("daemonsets must be in apps aggregated discovery");
    assert_eq!(
        ds.short_names.as_deref(),
        Some(&["ds".to_string()][..]),
        "ds short name must be present"
    );
}

#[tokio::test]
async fn test_api_v1_resources_expose_subresources() {
    // Upstream /api/v1 lists subresources (pods/status, pods/log, services/status,
    // nodes/status, namespaces/finalize, ...). Mirror that.
    let resources = serde_json::to_value(api_v1_resources().await.0).unwrap();
    let resource_names = resources["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|resource| resource["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    for expected in [
        "pods/status",
        "pods/log",
        "pods/exec",
        "pods/binding",
        "services/status",
        "nodes/status",
        "namespaces/finalize",
        "serviceaccounts/token",
        "replicationcontrollers/scale",
    ] {
        assert!(
            resource_names.contains(&expected),
            "core v1 discovery must expose {expected}: {resource_names:?}"
        );
    }
}

#[tokio::test]
async fn test_api_v1_primary_resources_advertise_storage_version_hash_and_deletecollection() {
    let resources = serde_json::to_value(api_v1_resources().await.0).unwrap();
    let pods = resources["resources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "pods")
        .expect("pods must be listed");
    assert!(
        pods["storageVersionHash"].is_string(),
        "primary resources must advertise storageVersionHash: {pods:?}"
    );
    assert!(
        pods["verbs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "deletecollection"),
        "primary resources must advertise the deletecollection verb: {pods:?}"
    );
    // categories must be omitted (not null) when absent.
    let secrets = resources["resources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "secrets")
        .expect("secrets must be listed");
    assert!(
        secrets.get("categories").is_none(),
        "categories must be omitted when absent, not null: {secrets:?}"
    );
    // Subresources must NOT advertise storageVersionHash.
    let pod_status = resources["resources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "pods/status")
        .expect("pods/status must be listed");
    assert!(
        pod_status.get("storageVersionHash").is_none(),
        "subresources must not advertise storageVersionHash: {pod_status:?}"
    );
}

#[tokio::test]
async fn test_aggregated_discovery_nests_subresources_under_parent() {
    let resources = core_v1_aggregated_resources();
    let pods = resources
        .iter()
        .find(|r| r.resource == "pods")
        .expect("pods must be in aggregated discovery");
    let sub_names: Vec<&str> = pods
        .subresources
        .iter()
        .map(|s| s.subresource.as_str())
        .collect();
    assert!(
        sub_names.contains(&"status") && sub_names.contains(&"log"),
        "pods aggregated discovery must nest status/log subresources: {sub_names:?}"
    );
    // Subresources must not also appear as top-level aggregated resources.
    assert!(
        !resources.iter().any(|r| r.resource.contains('/')),
        "aggregated discovery must not list subresources as siblings"
    );
}

#[tokio::test]
async fn test_apps_v1_resources_exposes_status_and_scale_subresources() {
    let resources = serde_json::to_value(apps_v1_resources().await.0).unwrap();
    let resource_names = resources["resources"]
        .as_array()
        .unwrap()
        .iter()
        .map(|resource| resource["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(
        resource_names.contains(&"deployments/status"),
        "apps/v1 discovery must include deployments/status: {:?}",
        resource_names
    );
    assert!(
        resource_names.contains(&"deployments/scale"),
        "apps/v1 discovery must include deployments/scale: {:?}",
        resource_names
    );
}

#[tokio::test]
async fn test_openapi_v2_multiple_crds_same_group_different_versions() {
    // Sonobuoy test: "works for multiple CRDs of same group but different versions"
    // Two separate CRDs in the same group with different versions must both appear
    let db = crate::datastore::test_support::in_memory().await;

    // CRD 1: group test.example.com, version v1, kind Foo
    let crd1 = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "foos.test.example.com" },
        "spec": {
            "group": "test.example.com",
            "scope": "Namespaced",
            "names": { "kind": "Foo", "plural": "foos", "singular": "foo" },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": { "type": "object" }
                        }
                    }
                }
            }]
        }
    });

    // CRD 2: same group, different version v2, different kind Bar
    let crd2 = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "bars.test.example.com" },
        "spec": {
            "group": "test.example.com",
            "scope": "Namespaced",
            "names": { "kind": "Bar", "plural": "bars", "singular": "bar" },
            "versions": [{
                "name": "v2",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "spec": { "type": "object" }
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
        "foos.test.example.com",
        crd1,
    )
    .await
    .unwrap();

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "bars.test.example.com",
        crd2,
    )
    .await
    .unwrap();

    let response = openapi_v2(&db).await;
    let definitions = response["definitions"].as_object().unwrap();

    // Both CRDs must appear with their own definitions
    assert!(
        definitions.contains_key("com.example.test.v1.Foo"),
        "Foo CRD definition missing. Keys: {:?}",
        definitions.keys().collect::<Vec<_>>()
    );
    assert!(
        definitions.contains_key("com.example.test.v2.Bar"),
        "Bar CRD definition missing. Keys: {:?}",
        definitions.keys().collect::<Vec<_>>()
    );

    // Each must have correct x-kubernetes-group-version-kind
    assert_eq!(
        definitions["com.example.test.v1.Foo"]["x-kubernetes-group-version-kind"][0]["kind"],
        "Foo"
    );
    assert_eq!(
        definitions["com.example.test.v2.Bar"]["x-kubernetes-group-version-kind"][0]["kind"],
        "Bar"
    );
}

#[tokio::test]
async fn test_openapi_v2_multiversion_crd_both_versions_appear() {
    // A single CRD with multiple versions must publish definitions for each version
    let db = crate::datastore::test_support::in_memory().await;

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "widgets.test.example.com" },
        "spec": {
            "group": "test.example.com",
            "scope": "Namespaced",
            "names": { "kind": "Widget", "plural": "widgets", "singular": "widget" },
            "versions": [
                {
                    "name": "v2",
                    "served": true,
                    "storage": true,
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "properties": {
                                "spec": { "type": "object" }
                            }
                        }
                    }
                },
                {
                    "name": "v3",
                    "served": true,
                    "storage": false,
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "properties": {
                                "spec": { "type": "object" }
                            }
                        }
                    }
                }
            ]
        }
    });

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "widgets.test.example.com",
        crd,
    )
    .await
    .unwrap();

    let response = openapi_v2(&db).await;
    let definitions = response["definitions"].as_object().unwrap();

    // Both versions of the same CRD must appear
    assert!(
        definitions.contains_key("com.example.test.v2.Widget"),
        "Widget v2 definition missing. Keys: {:?}",
        definitions.keys().collect::<Vec<_>>()
    );
    assert!(
        definitions.contains_key("com.example.test.v3.Widget"),
        "Widget v3 definition missing. Keys: {:?}",
        definitions.keys().collect::<Vec<_>>()
    );

    // Each version has its own x-kubernetes-group-version-kind
    assert_eq!(
        definitions["com.example.test.v2.Widget"]["x-kubernetes-group-version-kind"][0]["version"],
        "v2"
    );
    assert_eq!(
        definitions["com.example.test.v3.Widget"]["x-kubernetes-group-version-kind"][0]["version"],
        "v3"
    );
}

#[tokio::test]
async fn test_openapi_v2_schema_less_crd_publishes_definition() {
    // A CRD without schema.openAPIV3Schema must still get a definition published
    let db = crate::datastore::test_support::in_memory().await;

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "widgets.example.com" },
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": { "kind": "Widget", "plural": "widgets", "singular": "widget" },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true
            }]
        }
    });

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "widgets.example.com",
        crd,
    )
    .await
    .unwrap();

    let response = openapi_v2(&db).await;
    let definitions = response["definitions"].as_object().unwrap();

    assert!(
        definitions.contains_key("com.example.v1.Widget"),
        "Schema-less CRD must still appear. Keys: {:?}",
        definitions.keys().collect::<Vec<_>>()
    );
    assert_eq!(definitions["com.example.v1.Widget"]["type"], "object");
}

#[tokio::test]
async fn test_openapi_v3_discovery_lists_crd_groups() {
    // /openapi/v3 must list per-group/version paths for CRDs so clients can discover them
    let db = crate::datastore::test_support::in_memory().await;

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "foos.test.example.com" },
        "spec": {
            "group": "test.example.com",
            "scope": "Namespaced",
            "names": { "kind": "Foo", "plural": "foos", "singular": "foo" },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": { "type": "object" }
                }
            }]
        }
    });

    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "foos.test.example.com",
        crd,
    )
    .await
    .unwrap();

    let discovery = openapi_v3_discovery_with_crds(&db).await;
    let paths = discovery["paths"].as_object().unwrap();

    assert!(
        paths.contains_key("apis/test.example.com/v1"),
        "CRD group/version must appear in OpenAPI v3 discovery. Paths: {:?}",
        paths.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_openapi_v2_crd_with_schema_still_works() {
    // Ensure CRDs WITH schemas still work correctly
    let db = crate::datastore::test_support::in_memory().await;

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "certificates.cert-manager.io" },
        "spec": {
            "group": "cert-manager.io",
            "scope": "Namespaced",
            "names": { "kind": "Certificate", "plural": "certificates", "singular": "certificate" },
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
                                    "commonName": {"type": "string"}
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
        "certificates.cert-manager.io",
        crd,
    )
    .await
    .unwrap();

    let response = openapi_v2(&db).await;
    let definitions = response["definitions"].as_object().unwrap();
    assert!(definitions.contains_key("io.cert-manager.v1.Certificate"));
    assert_eq!(
        definitions["io.cert-manager.v1.Certificate"]["properties"]["spec"]["properties"]["commonName"]
            ["type"],
        "string"
    );
}

#[tokio::test]
async fn test_openapi_v2_crd_schema_includes_kubernetes_top_level_fields_for_explain() {
    let db = crate::datastore::test_support::in_memory().await;

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "foos.explain.example.com" },
        "spec": {
            "group": "explain.example.com",
            "scope": "Namespaced",
            "names": { "kind": "Foo", "plural": "foos", "singular": "foo" },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "description": "Foo CRD for Testing",
                        "properties": {
                            "spec": {
                                "type": "object",
                                "description": "Specification of Foo"
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
        "foos.explain.example.com",
        crd,
    )
    .await
    .unwrap();

    let response = openapi_v2(&db).await;
    let definitions = response["definitions"].as_object().unwrap();
    let def = &definitions["com.example.explain.v1.Foo"];

    let api_version = &def["properties"]["apiVersion"];
    assert_eq!(api_version["type"], "string");
    assert!(
        api_version["description"]
            .as_str()
            .unwrap_or("")
            .contains("APIVersion defines"),
        "apiVersion description should match kubectl explain expectations: {:?}",
        api_version
    );

    let metadata = &def["properties"]["metadata"];
    assert!(
        metadata["description"]
            .as_str()
            .unwrap_or("")
            .contains("Standard object's metadata."),
        "metadata description should match kubectl explain expectations: {:?}",
        metadata
    );

    let creation_timestamp = &metadata["properties"]["creationTimestamp"];
    assert_eq!(creation_timestamp["type"], "string");
    assert!(
        creation_timestamp["description"]
            .as_str()
            .unwrap_or("")
            .contains("CreationTimestamp is a timestamp"),
        "creationTimestamp description should match kubectl explain expectations: {:?}",
        creation_timestamp
    );
}

#[tokio::test]
async fn test_openapi_v3_api_v1_has_patch_with_field_validation() {
    // kubectl's queryParamVerifierV3 looks for PATCH operations with
    // x-kubernetes-group-version-kind and a fieldValidation query parameter.
    // Without patch ops, kubectl falls back to OpenAPI v2 protobuf which fails.
    let db = crate::datastore::test_support::in_memory().await;
    let v3 = build_openapi_v3_api_v1(&db).await;

    let paths = v3["paths"].as_object().expect("paths must be object");
    // Find a path for ConfigMap
    let cm_path = paths
        .iter()
        .find(|(k, _)| k.contains("configmaps"))
        .expect("configmaps path must exist");
    let path_item = cm_path.1;

    // Must have patch operation
    let patch = path_item.get("patch").expect("patch operation must exist");

    // patch must have x-kubernetes-group-version-kind
    let gvk = &patch["x-kubernetes-group-version-kind"];
    assert_eq!(gvk["group"], "");
    assert_eq!(gvk["version"], "v1");
    assert_eq!(gvk["kind"], "ConfigMap");

    // patch must have fieldValidation query parameter
    let params = patch["parameters"]
        .as_array()
        .expect("parameters must be array");
    let has_field_validation = params
        .iter()
        .any(|p| p["name"] == "fieldValidation" && p["in"] == "query");
    assert!(
        has_field_validation,
        "patch must have fieldValidation query parameter"
    );
}

#[tokio::test]
async fn test_openapi_v3_group_version_has_patch_with_field_validation() {
    // CRD group/version endpoints also need patch ops for kubectl validation
    let db = crate::datastore::test_support::in_memory().await;

    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": { "name": "foos.example.com" },
        "spec": {
            "group": "example.com",
            "scope": "Namespaced",
            "names": { "kind": "Foo", "plural": "foos", "singular": "foo" },
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": { "spec": { "type": "object" } }
                    }
                }
            }]
        }
    });
    db.create_resource(
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        None,
        "foos.example.com",
        crd,
    )
    .await
    .unwrap();

    let v3 = build_openapi_v3_group_version(&db, "example.com", "v1").await;
    let paths = v3["paths"].as_object().expect("paths must be object");

    // Must have at least one path with a patch operation for the CRD
    let has_patch = paths.values().any(|path_item| {
        if let Some(patch) = path_item.get("patch") {
            patch.get("x-kubernetes-group-version-kind").is_some()
                && patch["parameters"].as_array().is_some_and(|params| {
                    params
                        .iter()
                        .any(|p| p["name"] == "fieldValidation" && p["in"] == "query")
                })
        } else {
            false
        }
    });
    assert!(
        has_patch,
        "CRD group/version must have patch ops with fieldValidation"
    );
}

#[tokio::test]
async fn test_openapi_v3_discovery_advertises_apps_v1() {
    let db = crate::datastore::test_support::in_memory().await;
    let discovery = openapi_v3_discovery_with_crds(&db).await;

    assert_eq!(
        discovery.pointer("/paths/apis~1apps~1v1/serverRelativeURL"),
        Some(&json!("/openapi/v3/apis/apps/v1")),
        "kubectl validation must discover the apps/v1 OpenAPI v3 document"
    );
}

#[tokio::test]
async fn test_openapi_v3_apps_v1_includes_deployment_create_schema() {
    let db = crate::datastore::test_support::in_memory().await;
    let spec = build_openapi_v3_group_version(&db, "apps", "v1").await;
    let deployment_path = spec
        .pointer("/paths/~1apis~1apps~1v1~1namespaces~1{namespace}~1deployments")
        .expect("apps/v1 OpenAPI must include deployments collection path");

    assert_eq!(
        deployment_path.pointer("/post/x-kubernetes-group-version-kind"),
        Some(&json!({"group": "apps", "kind": "Deployment", "version": "v1"}))
    );

    let patch_params = deployment_path
        .pointer("/patch/parameters")
        .and_then(|v| v.as_array())
        .expect("deployment patch operation must expose parameters");
    assert!(
        patch_params
            .iter()
            .any(|p| p["name"] == "fieldValidation" && p["in"] == "query"),
        "deployment patch operation must advertise fieldValidation support"
    );

    assert!(
        spec.pointer("/components/schemas/io.k8s.api.apps.v1.Deployment")
            .is_some(),
        "apps/v1 OpenAPI must include a Deployment schema"
    );
}
