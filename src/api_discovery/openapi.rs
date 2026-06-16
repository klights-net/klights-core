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

fn ensure_builtin_top_level_object_fields(
    schema: &mut Value,
    group: &str,
    version: &str,
    kind: &str,
) {
    ensure_crd_top_level_object_fields(schema);
    if let Some(obj) = schema.as_object_mut() {
        obj.insert(
            "x-kubernetes-group-version-kind".to_string(),
            serde_json::json!([{
                "group": group,
                "version": version,
                "kind": kind
            }]),
        );
    }
}

fn insert_generated_builtin_schema<T>(
    definitions: &mut serde_json::Map<String, Value>,
    group: &str,
    version: &str,
    kind: &str,
) where
    T: k8s_openapi::schemars::JsonSchema,
{
    let key = builtin_openapi_schema_key(group, version, kind);
    let root = k8s_openapi::schemars::r#gen::SchemaGenerator::default().into_root_schema_for::<T>();
    let mut root_value = serde_json::to_value(root).unwrap_or_else(|_| {
        serde_json::json!({
            "type": "object"
        })
    });

    let nested_definitions = root_value
        .as_object_mut()
        .and_then(|obj| obj.remove("definitions"))
        .and_then(|defs| defs.as_object().cloned())
        .unwrap_or_default();
    ensure_builtin_top_level_object_fields(&mut root_value, group, version, kind);
    strip_x_kubernetes_fields(&mut root_value);

    for (nested_key, mut nested_schema) in nested_definitions {
        strip_x_kubernetes_fields(&mut nested_schema);
        definitions.entry(nested_key).or_insert(nested_schema);
    }
    definitions.insert(key, root_value);
}

fn builtin_openapi_definitions() -> serde_json::Map<String, Value> {
    let mut definitions = serde_json::Map::new();

    insert_generated_builtin_schema::<
        k8s_openapi::api::admissionregistration::v1::MutatingWebhookConfiguration,
    >(
        &mut definitions,
        "admissionregistration.k8s.io",
        "v1",
        "MutatingWebhookConfiguration",
    );
    insert_generated_builtin_schema::<
        k8s_openapi::api::admissionregistration::v1::ValidatingAdmissionPolicy,
    >(
        &mut definitions,
        "admissionregistration.k8s.io",
        "v1",
        "ValidatingAdmissionPolicy",
    );
    insert_generated_builtin_schema::<
        k8s_openapi::api::admissionregistration::v1::ValidatingAdmissionPolicyBinding,
    >(
        &mut definitions,
        "admissionregistration.k8s.io",
        "v1",
        "ValidatingAdmissionPolicyBinding",
    );
    insert_generated_builtin_schema::<
        k8s_openapi::api::admissionregistration::v1::ValidatingWebhookConfiguration,
    >(
        &mut definitions,
        "admissionregistration.k8s.io",
        "v1",
        "ValidatingWebhookConfiguration",
    );
    insert_generated_builtin_schema::<k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition>(
        &mut definitions,
        "apiextensions.k8s.io",
        "v1",
        "CustomResourceDefinition",
    );
    insert_generated_builtin_schema::<
        k8s_openapi::kube_aggregator::pkg::apis::apiregistration::v1::APIService,
    >(
        &mut definitions,
        "apiregistration.k8s.io",
        "v1",
        "APIService",
    );

    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::ConfigMap>(
        &mut definitions,
        "",
        "v1",
        "ConfigMap",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::Secret>(
        &mut definitions,
        "",
        "v1",
        "Secret",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::Pod>(
        &mut definitions,
        "",
        "v1",
        "Pod",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::Service>(
        &mut definitions,
        "",
        "v1",
        "Service",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::Namespace>(
        &mut definitions,
        "",
        "v1",
        "Namespace",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::ServiceAccount>(
        &mut definitions,
        "",
        "v1",
        "ServiceAccount",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::Endpoints>(
        &mut definitions,
        "",
        "v1",
        "Endpoints",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::PersistentVolumeClaim>(
        &mut definitions,
        "",
        "v1",
        "PersistentVolumeClaim",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::PersistentVolume>(
        &mut definitions,
        "",
        "v1",
        "PersistentVolume",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::Node>(
        &mut definitions,
        "",
        "v1",
        "Node",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::Event>(
        &mut definitions,
        "",
        "v1",
        "Event",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::ResourceQuota>(
        &mut definitions,
        "",
        "v1",
        "ResourceQuota",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::LimitRange>(
        &mut definitions,
        "",
        "v1",
        "LimitRange",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::PodTemplate>(
        &mut definitions,
        "",
        "v1",
        "PodTemplate",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::core::v1::ReplicationController>(
        &mut definitions,
        "",
        "v1",
        "ReplicationController",
    );

    insert_generated_builtin_schema::<k8s_openapi::api::apps::v1::ControllerRevision>(
        &mut definitions,
        "apps",
        "v1",
        "ControllerRevision",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::apps::v1::DaemonSet>(
        &mut definitions,
        "apps",
        "v1",
        "DaemonSet",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::apps::v1::Deployment>(
        &mut definitions,
        "apps",
        "v1",
        "Deployment",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::apps::v1::ReplicaSet>(
        &mut definitions,
        "apps",
        "v1",
        "ReplicaSet",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::apps::v1::StatefulSet>(
        &mut definitions,
        "apps",
        "v1",
        "StatefulSet",
    );

    insert_generated_builtin_schema::<k8s_openapi::api::autoscaling::v1::HorizontalPodAutoscaler>(
        &mut definitions,
        "autoscaling",
        "v1",
        "HorizontalPodAutoscaler",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler>(
        &mut definitions,
        "autoscaling",
        "v2",
        "HorizontalPodAutoscaler",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::batch::v1::CronJob>(
        &mut definitions,
        "batch",
        "v1",
        "CronJob",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::batch::v1::Job>(
        &mut definitions,
        "batch",
        "v1",
        "Job",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::coordination::v1::Lease>(
        &mut definitions,
        "coordination.k8s.io",
        "v1",
        "Lease",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::discovery::v1::EndpointSlice>(
        &mut definitions,
        "discovery.k8s.io",
        "v1",
        "EndpointSlice",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::flowcontrol::v1::FlowSchema>(
        &mut definitions,
        "flowcontrol.apiserver.k8s.io",
        "v1",
        "FlowSchema",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::flowcontrol::v1::PriorityLevelConfiguration>(
        &mut definitions,
        "flowcontrol.apiserver.k8s.io",
        "v1",
        "PriorityLevelConfiguration",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::networking::v1::IngressClass>(
        &mut definitions,
        "networking.k8s.io",
        "v1",
        "IngressClass",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::networking::v1::Ingress>(
        &mut definitions,
        "networking.k8s.io",
        "v1",
        "Ingress",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::networking::v1::NetworkPolicy>(
        &mut definitions,
        "networking.k8s.io",
        "v1",
        "NetworkPolicy",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::node::v1::RuntimeClass>(
        &mut definitions,
        "node.k8s.io",
        "v1",
        "RuntimeClass",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::policy::v1::PodDisruptionBudget>(
        &mut definitions,
        "policy",
        "v1",
        "PodDisruptionBudget",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::rbac::v1::ClusterRoleBinding>(
        &mut definitions,
        "rbac.authorization.k8s.io",
        "v1",
        "ClusterRoleBinding",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::rbac::v1::ClusterRole>(
        &mut definitions,
        "rbac.authorization.k8s.io",
        "v1",
        "ClusterRole",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::rbac::v1::RoleBinding>(
        &mut definitions,
        "rbac.authorization.k8s.io",
        "v1",
        "RoleBinding",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::rbac::v1::Role>(
        &mut definitions,
        "rbac.authorization.k8s.io",
        "v1",
        "Role",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::scheduling::v1::PriorityClass>(
        &mut definitions,
        "scheduling.k8s.io",
        "v1",
        "PriorityClass",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::storage::v1::CSIDriver>(
        &mut definitions,
        "storage.k8s.io",
        "v1",
        "CSIDriver",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::storage::v1::CSINode>(
        &mut definitions,
        "storage.k8s.io",
        "v1",
        "CSINode",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::storage::v1::CSIStorageCapacity>(
        &mut definitions,
        "storage.k8s.io",
        "v1",
        "CSIStorageCapacity",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::storage::v1::StorageClass>(
        &mut definitions,
        "storage.k8s.io",
        "v1",
        "StorageClass",
    );
    insert_generated_builtin_schema::<k8s_openapi::api::storage::v1::VolumeAttachment>(
        &mut definitions,
        "storage.k8s.io",
        "v1",
        "VolumeAttachment",
    );

    definitions
}

fn rewrite_refs_to_openapi_v3_components(value: &mut Value) {
    match value {
        Value::Object(obj) => {
            if let Some(Value::String(reference)) = obj.get_mut("$ref")
                && let Some(suffix) = reference.strip_prefix("#/definitions/")
            {
                *reference = format!("#/components/schemas/{suffix}");
            }
            for child in obj.values_mut() {
                rewrite_refs_to_openapi_v3_components(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                rewrite_refs_to_openapi_v3_components(item);
            }
        }
        _ => {}
    }
}

fn openapi_v3_schemas_from_definitions(
    definitions: &serde_json::Map<String, Value>,
) -> serde_json::Map<String, Value> {
    definitions
        .iter()
        .map(|(key, schema)| {
            let mut schema = schema.clone();
            rewrite_refs_to_openapi_v3_components(&mut schema);
            (key.clone(), schema)
        })
        .collect()
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

    // Build definitions from generated built-in schemas plus CRD schemas.
    let mut definitions = builtin_openapi_definitions();

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
        "" => "core".to_string(),
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

    let mut schemas = all_definitions
        .map(openapi_v3_schemas_from_definitions)
        .unwrap_or_default();
    let mut paths = serde_json::Map::new();
    if let Some(resources) = builtin_openapi_resources(group, version) {
        for (plural, kind, namespaced) in resources {
            let schema_key = builtin_openapi_schema_key(group, version, kind);
            schemas.entry(schema_key.clone()).or_insert_with(|| {
                serde_json::json!({
                    "type": "object",
                    "x-kubernetes-group-version-kind": [{
                        "group": group,
                        "version": version,
                        "kind": kind
                    }]
                })
            });
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
                        let mut schema = schema.clone();
                        rewrite_refs_to_openapi_v3_components(&mut schema);
                        schemas.insert(key.clone(), schema);
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
        openapi_v3_schemas_from_definitions(defs)
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
    let mut paths = serde_json::Map::new();
    paths.insert(
        "/apis".to_string(),
        serde_json::json!({
            "get": {
                "description": "get available API groups",
                "operationId": "getAPIVersions",
                "responses": {"200": {"description": "OK"}}
            }
        }),
    );
    for (group, version) in builtin_openapi_group_versions() {
        paths.insert(
            format!("/apis/{group}/{version}"),
            serde_json::json!({
                "get": {
                    "description": format!("get resources for {group}/{version}"),
                    "operationId": format!("get{}{}APIResources", group.replace(['.', '-'], "_"), version),
                    "responses": {"200": {"description": "OK"}}
                }
            }),
        );
    }
    Json(serde_json::json!({
        "openapi": "3.0.0",
        "info": {"title": "Kubernetes", "version": "1.34"},
        "paths": paths
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
