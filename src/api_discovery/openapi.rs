use super::*;
use std::sync::OnceLock;

/// Static portion of the OpenAPI v3 path map. Computed once; CRD-driven
/// entries are merged in per request because CRDs change at runtime.
fn static_openapi_v3_paths() -> &'static serde_json::Map<String, Value> {
    static PATHS: OnceLock<serde_json::Map<String, Value>> = OnceLock::new();
    PATHS.get_or_init(|| {
        let mut paths = serde_json::Map::new();
        paths.insert(
            "api/v1".to_string(),
            serde_json::json!({"serverRelativeURL": "/openapi/v3/api/v1"}),
        );
        for (group, version) in builtin_openapi_group_versions() {
            let key = format!("apis/{group}/{version}");
            let url = format!("/openapi/v3/apis/{group}/{version}");
            paths.insert(key, serde_json::json!({"serverRelativeURL": url}));
        }
        paths
    })
}

pub async fn openapi_v3_discovery_with_crds(db: &dyn DatastoreBackend) -> Value {
    let mut paths = static_openapi_v3_paths().clone();

    // Collect CRD group/version pairs
    let crds = db
        .list_resources(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap_or_else(|_| crate::datastore::ResourceList {
            items: vec![],
            resource_version: 0,
            continue_token: None,
            remaining_item_count: None,
        });

    // Use a set to avoid duplicate group/version entries
    let mut seen = std::collections::HashSet::new();

    for crd_resource in crds.items {
        let spec = match crd_resource.data.get("spec") {
            Some(s) => s,
            None => continue,
        };

        let group = spec.get("group").and_then(|g| g.as_str()).unwrap_or("");
        if group.is_empty() {
            continue;
        }

        if let Some(versions) = spec.get("versions").and_then(|v| v.as_array()) {
            for version in versions {
                let version_name = version.get("name").and_then(|n| n.as_str()).unwrap_or("");
                let served = version
                    .get("served")
                    .and_then(|s| s.as_bool())
                    .unwrap_or(false);

                if !served || version_name.is_empty() {
                    continue;
                }

                let key = format!("apis/{}/{}", group, version_name);
                if seen.insert(key.clone()) {
                    let url = format!("/openapi/v3/apis/{}/{}", group, version_name);
                    paths.insert(key, serde_json::json!({"serverRelativeURL": url}));
                }
            }
        }
    }

    serde_json::json!({"paths": paths})
}

/// Remove a specific extension key recursively (used for v2-only stripping of
/// fields that are valid in v3 but not Swagger 2.0).
fn strip_x_kubernetes_extension_recursive(v: &mut Value, key: &str) {
    let obj = match v.as_object_mut() {
        Some(o) => o,
        None => return,
    };
    obj.remove(key);
    for (_, child) in obj.iter_mut() {
        match child {
            Value::Object(_) => strip_x_kubernetes_extension_recursive(child, key),
            Value::Array(arr) => {
                for item in arr.iter_mut() {
                    strip_x_kubernetes_extension_recursive(item, key);
                }
            }
            _ => {}
        }
    }
}

fn strip_x_kubernetes_fields(v: &mut Value) {
    let obj = match v.as_object_mut() {
        Some(o) => o,
        None => return,
    };
    let keys_to_remove: Vec<String> = obj
        .keys()
        .filter(|k| {
            k.starts_with("x-kubernetes-")
                && *k != "x-kubernetes-group-version-kind"
                && *k != "x-kubernetes-preserve-unknown-fields"
        })
        .cloned()
        .collect();
    for k in keys_to_remove {
        obj.remove(&k);
    }
    // Recurse into nested schemas
    for (_, child) in obj.iter_mut() {
        match child {
            Value::Object(_) => strip_x_kubernetes_fields(child),
            Value::Array(arr) => {
                for item in arr.iter_mut() {
                    strip_x_kubernetes_fields(item);
                }
            }
            _ => {}
        }
    }
}

fn ensure_crd_top_level_object_fields(schema: &mut Value) {
    if !schema.is_object() {
        *schema = serde_json::json!({"type": "object"});
    }

    let root = schema
        .as_object_mut()
        .expect("schema must be object after normalization");
    root.entry("type".to_string())
        .or_insert_with(|| Value::String("object".to_string()));

    let properties = root
        .entry("properties".to_string())
        .or_insert_with(|| Value::Object(serde_json::Map::new()))
        .as_object_mut()
        .expect("schema.properties must be an object");

    properties
        .entry("apiVersion".to_string())
        .or_insert_with(|| {
            serde_json::json!({
                "type": "string",
                "description": "APIVersion defines the versioned schema of this representation of an object."
            })
        });

    properties.entry("kind".to_string()).or_insert_with(|| {
        serde_json::json!({
            "type": "string",
            "description": "Kind is a string value representing the REST resource this object represents."
        })
    });

    let metadata = properties
        .entry("metadata".to_string())
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    if !metadata.is_object() {
        *metadata = Value::Object(serde_json::Map::new());
    }
    let metadata_obj = metadata
        .as_object_mut()
        .expect("metadata schema must be object");
    metadata_obj
        .entry("type".to_string())
        .or_insert_with(|| Value::String("object".to_string()));
    metadata_obj
        .entry("description".to_string())
        .or_insert_with(|| Value::String("Standard object's metadata.".to_string()));
    let metadata_props = metadata_obj
        .entry("properties".to_string())
        .or_insert_with(|| Value::Object(serde_json::Map::new()))
        .as_object_mut()
        .expect("metadata.properties must be an object");
    metadata_props
        .entry("creationTimestamp".to_string())
        .or_insert_with(|| {
            serde_json::json!({
                "type": "string",
                "description": "CreationTimestamp is a timestamp representing the server time when this object was created."
            })
        });
}

/// OpenAPI v2 endpoint - returns Swagger 2.0 spec with CRD schemas
pub async fn openapi_v2(db: &dyn DatastoreBackend) -> Value {
    // Fetch all CRDs from the database
    let crds = db
        .list_resources(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap_or_else(|_| crate::datastore::ResourceList {
            items: vec![],
            resource_version: 0,
            continue_token: None,
            remaining_item_count: None,
        });

    // Build definitions from CRD schemas
    let mut definitions = serde_json::Map::new();

    for crd_resource in crds.items {
        // Extract CRD metadata
        let crd_data = &crd_resource.data;
        let spec = match crd_data.get("spec") {
            Some(s) => s,
            None => continue,
        };

        let group = spec.get("group").and_then(|g| g.as_str()).unwrap_or("");
        let names = match spec.get("names") {
            Some(n) => n,
            None => continue,
        };
        let kind = names.get("kind").and_then(|k| k.as_str()).unwrap_or("");

        // Extract schema from first served version
        if let Some(versions) = spec.get("versions").and_then(|v| v.as_array()) {
            for version in versions {
                let version_name = version.get("name").and_then(|n| n.as_str()).unwrap_or("");
                let served = version
                    .get("served")
                    .and_then(|s| s.as_bool())
                    .unwrap_or(false);

                if !served {
                    continue;
                }

                // Get the OpenAPI v3 schema, or use a permissive default for schema-less CRDs
                let schema = version
                    .pointer("/schema/openAPIV3Schema")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({"type": "object"}));

                // Create definition name using K8s format: reverse domain parts like Java package naming
                // Example: "crd-publish-openapi-test-common-group.example.com" becomes
                // "com.example.crd-publish-openapi-test-common-group.v6.e2e-test-crd-publish-openapi-3457-2462-crd"
                let definition_name = if !group.is_empty() {
                    // Reverse domain parts, use kind as-is (PascalCase)
                    // K8s format: com.example.group.v1.Kind
                    let parts: Vec<&str> = group.split('.').collect();
                    let reversed = parts.iter().rev().copied().collect::<Vec<_>>().join(".");
                    format!("{}.{}.{}", reversed, version_name, kind)
                } else {
                    format!("{}.{}", version_name, kind)
                };

                // Strip x-kubernetes-* extensions (not valid in Swagger 2.0),
                // then add x-kubernetes-group-version-kind which is needed by kubectl.
                // NOTE: x-kubernetes-preserve-unknown-fields is kept here (valid in OpenAPI v3
                // and required by build_openapi_v3_group_version which calls this function).
                // The HTTP handler for /openapi/v2 strips it from the final response after calling
                // this function, so v2 clients see it stripped while v3 clients see it preserved.
                let mut def = schema;
                ensure_crd_top_level_object_fields(&mut def);
                strip_x_kubernetes_fields(&mut def);
                if let Some(obj) = def.as_object_mut() {
                    obj.insert(
                        "x-kubernetes-group-version-kind".to_string(),
                        serde_json::json!([{
                            "group": group,
                            "version": version_name,
                            "kind": kind
                        }]),
                    );
                }
                definitions.insert(definition_name, def);
            }
        }
    }

    serde_json::json!({
        "swagger": "2.0",
        "info": {
            "title": "Kubernetes",
            "version": "1.34"
        },
        "paths": {
            "/api/": {
                "get": {
                    "description": "get available API versions",
                    "operationId": "getCoreAPIVersions",
                    "responses": {
                        "200": {
                            "description": "OK"
                        }
                    }
                }
            }
        },
        "definitions": definitions
    })
}

/// Handler for GET /openapi/v3
pub async fn get_openapi_v3_discovery(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(openapi_v3_discovery_with_crds(state.db.as_ref()).await)
}

/// Build OpenAPI v3 path operations for a resource.
/// Includes post and patch operations with fieldValidation query parameter.
/// kubectl's queryParamVerifierV3 requires PATCH ops with fieldValidation to
/// confirm the server supports field validation; without this, kubectl falls
/// back to OpenAPI v2 protobuf which klights doesn't serve.
fn openapi_v3_path_item(group: &str, kind: &str, version: &str, schema_ref: &str) -> Value {
    serde_json::json!({
        "post": {
            "x-kubernetes-action": "post",
            "x-kubernetes-group-version-kind": {"group": group, "kind": kind, "version": version},
            "requestBody": {"content": {"*/*": {"schema": {"$ref": format!("#/components/schemas/{}", schema_ref)}}}},
        },
        "patch": {
            "x-kubernetes-action": "patch",
            "x-kubernetes-group-version-kind": {"group": group, "kind": kind, "version": version},
            "parameters": [
                {"name": "fieldValidation", "in": "query", "type": "string"},
                {"name": "fieldManager", "in": "query", "type": "string"},
                {"name": "force", "in": "query", "type": "boolean"}
            ],
            "requestBody": {"content": {"*/*": {"schema": {"$ref": format!("#/components/schemas/{}", schema_ref)}}}},
        }
    })
}

fn builtin_openapi_group_versions() -> &'static [(&'static str, &'static str)] {
    &[
        ("admissionregistration.k8s.io", "v1"),
        ("apiextensions.k8s.io", "v1"),
        ("apiregistration.k8s.io", "v1"),
        ("apps", "v1"),
        ("autoscaling", "v1"),
        ("autoscaling", "v2"),
        ("batch", "v1"),
        ("coordination.k8s.io", "v1"),
        ("discovery.k8s.io", "v1"),
        ("flowcontrol.apiserver.k8s.io", "v1"),
        ("networking.k8s.io", "v1"),
        ("node.k8s.io", "v1"),
        ("policy", "v1"),
        ("rbac.authorization.k8s.io", "v1"),
        ("scheduling.k8s.io", "v1"),
        ("storage.k8s.io", "v1"),
    ]
}

fn builtin_openapi_resources(
    group: &str,
    version: &str,
) -> Option<&'static [(&'static str, &'static str, bool)]> {
    match (group, version) {
        ("admissionregistration.k8s.io", "v1") => Some(&[
            (
                "mutatingwebhookconfigurations",
                "MutatingWebhookConfiguration",
                false,
            ),
            (
                "validatingadmissionpolicies",
                "ValidatingAdmissionPolicy",
                false,
            ),
            (
                "validatingadmissionpolicybindings",
                "ValidatingAdmissionPolicyBinding",
                false,
            ),
            (
                "validatingwebhookconfigurations",
                "ValidatingWebhookConfiguration",
                false,
            ),
        ]),
        ("apiextensions.k8s.io", "v1") => Some(&[(
            "customresourcedefinitions",
            "CustomResourceDefinition",
            false,
        )]),
        ("apiregistration.k8s.io", "v1") => Some(&[("apiservices", "APIService", false)]),
        ("apps", "v1") => Some(&[
            ("controllerrevisions", "ControllerRevision", true),
            ("daemonsets", "DaemonSet", true),
            ("deployments", "Deployment", true),
            ("replicasets", "ReplicaSet", true),
            ("statefulsets", "StatefulSet", true),
        ]),
        ("autoscaling", "v1") => {
            Some(&[("horizontalpodautoscalers", "HorizontalPodAutoscaler", true)])
        }
        ("autoscaling", "v2") => {
            Some(&[("horizontalpodautoscalers", "HorizontalPodAutoscaler", true)])
        }
        ("batch", "v1") => Some(&[("cronjobs", "CronJob", true), ("jobs", "Job", true)]),
        ("coordination.k8s.io", "v1") => Some(&[("leases", "Lease", true)]),
        ("discovery.k8s.io", "v1") => Some(&[("endpointslices", "EndpointSlice", true)]),
        ("flowcontrol.apiserver.k8s.io", "v1") => Some(&[
            ("flowschemas", "FlowSchema", false),
            (
                "prioritylevelconfigurations",
                "PriorityLevelConfiguration",
                false,
            ),
        ]),
        ("networking.k8s.io", "v1") => Some(&[
            ("ingressclasses", "IngressClass", false),
            ("ingresses", "Ingress", true),
            ("networkpolicies", "NetworkPolicy", true),
        ]),
        ("node.k8s.io", "v1") => Some(&[("runtimeclasses", "RuntimeClass", false)]),
        ("policy", "v1") => Some(&[("poddisruptionbudgets", "PodDisruptionBudget", true)]),
        ("rbac.authorization.k8s.io", "v1") => Some(&[
            ("clusterrolebindings", "ClusterRoleBinding", false),
            ("clusterroles", "ClusterRole", false),
            ("rolebindings", "RoleBinding", true),
            ("roles", "Role", true),
        ]),
        ("scheduling.k8s.io", "v1") => Some(&[("priorityclasses", "PriorityClass", false)]),
        ("storage.k8s.io", "v1") => Some(&[
            ("csidrivers", "CSIDriver", false),
            ("csinodes", "CSINode", false),
            ("csistoragecapacities", "CSIStorageCapacity", true),
            ("storageclasses", "StorageClass", false),
            ("volumeattachments", "VolumeAttachment", false),
        ]),
        _ => None,
    }
}

fn builtin_openapi_schema_key(group: &str, version: &str, kind: &str) -> String {
    let package = match group {
        "apps" => "apps".to_string(),
        "autoscaling" => "autoscaling".to_string(),
        "batch" => "batch".to_string(),
        "policy" => "policy".to_string(),
        other => other.trim_end_matches(".k8s.io").to_string(),
    };
    format!("io.k8s.api.{package}.{version}.{kind}")
}

/// Build OpenAPI v3 spec for a specific CRD group/version (testable).
pub async fn build_openapi_v3_group_version(
    db: &dyn DatastoreBackend,
    group: &str,
    version: &str,
) -> Value {
    let v2 = openapi_v2(db).await;
    let all_definitions = v2.get("definitions").and_then(|d| d.as_object());

    let mut schemas = serde_json::Map::new();
    let mut paths = serde_json::Map::new();
    if let Some(resources) = builtin_openapi_resources(group, version) {
        for (plural, kind, namespaced) in resources {
            let schema_key = builtin_openapi_schema_key(group, version, kind);
            schemas.insert(
                schema_key.clone(),
                serde_json::json!({
                    "type": "object",
                    "x-kubernetes-group-version-kind": [{
                        "group": group,
                        "version": version,
                        "kind": kind
                    }]
                }),
            );
            let path = if *namespaced {
                format!(
                    "/apis/{}/{}/namespaces/{{namespace}}/{}",
                    group, version, plural
                )
            } else {
                format!("/apis/{}/{}/{}", group, version, plural)
            };
            paths.insert(
                path,
                openapi_v3_path_item(group, kind, version, &schema_key),
            );
        }
    }
    if let Some(defs) = all_definitions {
        for (key, schema) in defs {
            if let Some(gvk_list) = schema
                .get("x-kubernetes-group-version-kind")
                .and_then(|v| v.as_array())
            {
                for gvk in gvk_list {
                    let def_group = gvk.get("group").and_then(|g| g.as_str()).unwrap_or("");
                    let def_version = gvk.get("version").and_then(|v| v.as_str()).unwrap_or("");
                    if def_group == group && def_version == version {
                        schemas.insert(key.clone(), schema.clone());
                        // Extract plural name from the key for path generation
                        // Key format: "reversed.domain.version.Kind" — we need the plural
                        let kind = gvk.get("kind").and_then(|k| k.as_str()).unwrap_or("");
                        if !kind.is_empty() {
                            let plural = kind.to_lowercase() + "s";
                            let path = format!(
                                "/apis/{}/{}/namespaces/{{namespace}}/{}",
                                group, version, plural
                            );
                            paths.insert(path, openapi_v3_path_item(group, kind, version, key));
                        }
                    }
                }
            }
        }
    }

    serde_json::json!({
        "openapi": "3.0.0",
        "info": {"title": "Kubernetes", "version": "v1.34.6"},
        "paths": paths,
        "components": {
            "schemas": schemas
        }
    })
}

/// Handler for GET /openapi/v3/apis/:group/:version
pub async fn get_openapi_v3_group_version(
    State(state): State<Arc<AppState>>,
    Path((group, version)): Path<(String, String)>,
) -> Json<Value> {
    Json(build_openapi_v3_group_version(state.db.as_ref(), &group, &version).await)
}

/// Build OpenAPI v3 spec for core v1 resources (testable).
pub async fn build_openapi_v3_api_v1(db: &dyn DatastoreBackend) -> Value {
    let v2 = openapi_v2(db).await;
    let definitions = v2
        .get("definitions")
        .cloned()
        .unwrap_or(serde_json::json!({}));

    let mut schemas = if let Some(defs) = definitions.as_object() {
        defs.clone()
    } else {
        serde_json::Map::new()
    };

    for kind in &[
        "ConfigMap",
        "Secret",
        "Pod",
        "Service",
        "Namespace",
        "ServiceAccount",
        "Endpoints",
        "PersistentVolumeClaim",
        "PersistentVolume",
        "Node",
        "Event",
        "ResourceQuota",
        "LimitRange",
        "PodTemplate",
    ] {
        let key = format!("io.k8s.api.core.v1.{}", kind);
        if !schemas.contains_key(&key) {
            schemas.insert(
                key,
                serde_json::json!({
                    "type": "object",
                    "x-kubernetes-group-version-kind": [{
                        "group": "",
                        "kind": kind,
                        "version": "v1"
                    }]
                }),
            );
        }
    }

    let mut paths = serde_json::Map::new();
    for (plural, kind) in &[
        ("configmaps", "ConfigMap"),
        ("secrets", "Secret"),
        ("pods", "Pod"),
        ("services", "Service"),
        ("namespaces", "Namespace"),
        ("serviceaccounts", "ServiceAccount"),
        ("endpoints", "Endpoints"),
        ("persistentvolumeclaims", "PersistentVolumeClaim"),
        ("persistentvolumes", "PersistentVolume"),
        ("nodes", "Node"),
        ("events", "Event"),
        ("resourcequotas", "ResourceQuota"),
        ("limitranges", "LimitRange"),
        ("podtemplates", "PodTemplate"),
        ("replicationcontrollers", "ReplicationController"),
    ] {
        let schema_ref = format!("io.k8s.api.core.v1.{}", kind);
        paths.insert(
            format!("/api/v1/namespaces/{{namespace}}/{}", plural),
            openapi_v3_path_item("", kind, "v1", &schema_ref),
        );
    }

    serde_json::json!({
        "openapi": "3.0.0",
        "info": {"title": "Kubernetes", "version": "v1.34.6"},
        "paths": paths,
        "components": {
            "schemas": schemas
        }
    })
}

/// Handler for GET /openapi/v3/api/v1
pub async fn get_openapi_v3_api_v1(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(build_openapi_v3_api_v1(state.db.as_ref()).await)
}

/// Handler for GET /openapi/v3/apis
/// Return a minimal spec for API groups
pub async fn get_openapi_v3_apis() -> Json<Value> {
    Json(serde_json::json!({
        "openapi": "3.0.0",
        "info": {"title": "Kubernetes", "version": "1.34"},
        "paths": {}
    }))
}

/// Handler for GET /openapi/v2
/// kubectl may send protobuf-oriented Accept headers while still being able
/// to consume JSON OpenAPI from the apiserver for schema validation flows.
/// Always return JSON Swagger 2.0 here.
pub async fn get_openapi_v2(State(state): State<Arc<AppState>>, _headers: HeaderMap) -> Response {
    let mut spec = openapi_v2(state.db.as_ref()).await;
    // Strip x-kubernetes-preserve-unknown-fields from definitions: Swagger 2.0 clients
    // do not support this extension. The openapi_v2() function keeps it for v3 callers
    // (build_openapi_v3_group_version delegates to openapi_v2); strip it here for the
    // HTTP v2 response only.
    if let Some(defs) = spec.get_mut("definitions").and_then(|d| d.as_object_mut()) {
        for def in defs.values_mut() {
            strip_x_kubernetes_extension_recursive(def, "x-kubernetes-preserve-unknown-fields");
        }
    }
    Json(spec).into_response()
}

#[cfg(test)]
/// Test helper: apply the same v2-specific strip that get_openapi_v2 does in production.
/// Tests that directly call openapi_v2() see the v3-compatible version (field preserved).
/// Tests that want v2 HTTP behavior should call this instead.
#[cfg(test)]
pub async fn get_openapi_v2_stripped(db: &dyn DatastoreBackend) -> Value {
    let mut spec = openapi_v2(db).await;
    if let Some(defs) = spec.get_mut("definitions").and_then(|d| d.as_object_mut()) {
        for def in defs.values_mut() {
            strip_x_kubernetes_extension_recursive(def, "x-kubernetes-preserve-unknown-fields");
        }
    }
    spec
}
