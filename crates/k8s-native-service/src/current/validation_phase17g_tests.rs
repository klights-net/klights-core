//! Phase 17G code-local validation/defaulting regressions retained with the native owner.

use super::*;
use serde_json::json;

#[tokio::test]
async fn test_validate_against_schema_rejects_extra_properties() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "replicas": {"type": "integer"},
                    "image": {"type": "string"}
                }
            }
        }
    });

    let body = serde_json::json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test"},
        "spec": {
            "replicas": 3,
            "image": "nginx",
            "unknownField": "should-fail"
        }
    });

    let result = super::validate_against_schema(&body, &schema, "");
    assert!(
        result.is_err(),
        "Should reject extra field 'unknownField' in spec"
    );
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("unknownField"),
        "Error should mention the unknown field"
    );
}

#[tokio::test]
async fn test_validate_against_schema_allows_valid_properties() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "replicas": {"type": "integer"},
                    "image": {"type": "string"}
                }
            }
        }
    });

    let body = serde_json::json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test"},
        "spec": {
            "replicas": 3,
            "image": "nginx"
        }
    });

    let result = super::validate_against_schema(&body, &schema, "");
    assert!(result.is_ok(), "Should accept body with only known fields");
}

#[tokio::test]
async fn test_validate_against_schema_rejects_invalid_enum_value() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "mode": {
                        "type": "string",
                        "enum": ["strict", "permissive"]
                    }
                }
            }
        }
    });

    let body = serde_json::json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test"},
        "spec": {"mode": "invalid"}
    });

    let result = super::validate_against_schema(&body, &schema, "");
    let err = result.expect_err("Should reject values outside enum for schema-defined fields");
    match err {
        super::AppError::UnprocessableEntity(msg) => {
            assert!(
                msg.contains("Unsupported value: \"invalid\""),
                "Enum rejection must expose kubectl-parity Unsupported value format, got: {}",
                msg
            );
            assert!(
                msg.contains("spec.mode"),
                "Enum rejection should include the full field path, got: {}",
                msg
            );
        }
        other => panic!("expected UnprocessableEntity, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_validate_against_schema_rejects_missing_required_nested_field() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "bars": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["name"],
                            "properties": {
                                "name": {"type": "string"},
                                "age": {"type": "string"}
                            }
                        }
                    }
                }
            }
        }
    });

    let body = serde_json::json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test"},
        "spec": {
            "bars": [{"age": "10"}]
        }
    });

    let err = super::validate_against_schema(&body, &schema, "")
        .expect_err("Should reject missing required nested fields");
    match err {
        super::AppError::UnprocessableEntity(msg) => {
            assert!(
                msg.contains("spec.bars[0].name: Required value"),
                "Missing required field should report Kubernetes-style path/message, got: {}",
                msg
            );
        }
        other => panic!("expected UnprocessableEntity, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_validate_against_schema_allows_standard_top_level_fields() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "replicas": {"type": "integer"}
                }
            }
        }
    });

    let body = serde_json::json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test"},
        "status": {"ready": true},
        "spec": {"replicas": 1}
    });

    let result = super::validate_against_schema(&body, &schema, "");
    assert!(
        result.is_ok(),
        "Standard K8s top-level fields should always be allowed"
    );
}

#[test]
fn test_validate_metadata_fields_rejects_unknown() {
    let meta = serde_json::json!({
        "name": "test",
        "namespace": "default",
        "unknownField": "bad"
    });
    let meta_map = meta.as_object().unwrap();
    let result = super::validate_metadata_fields(meta_map);
    assert!(result.is_err());
    match result.unwrap_err() {
        super::AppError::UnprocessableEntity(msg) => {
            assert!(
                msg.contains("metadata.unknownField"),
                "Error must mention the unknown field: {}",
                msg
            );
        }
        other => panic!("Expected UnprocessableEntity, got {:?}", other),
    }
}

#[test]
fn test_validate_metadata_fields_accepts_known() {
    let meta = serde_json::json!({
        "name": "test",
        "namespace": "default",
        "labels": {"app": "test"},
        "annotations": {"note": "hi"},
        "finalizers": ["cleanup"],
        "ownerReferences": []
    });
    let meta_map = meta.as_object().unwrap();
    let result = super::validate_metadata_fields(meta_map);
    assert!(result.is_ok());
}

#[test]
fn test_apply_schema_defaults_fills_missing_fields() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "replicas": {"type": "integer", "default": 1},
                    "paused": {"type": "boolean", "default": false},
                    "name": {"type": "string"}
                }
            }
        }
    });
    let mut body = serde_json::json!({
        "apiVersion": "example.com/v1",
        "kind": "Foo",
        "metadata": {"name": "test"},
        "spec": {"name": "bar"}
    });
    super::apply_schema_defaults_pub(&mut body, &schema);
    assert_eq!(
        body["spec"]["replicas"], 1,
        "Missing field should get default"
    );
    assert_eq!(
        body["spec"]["paused"], false,
        "Missing bool field should get default"
    );
    assert_eq!(
        body["spec"]["name"], "bar",
        "Existing field should not be overwritten"
    );
}

#[test]
fn test_apply_schema_defaults_skips_existing_fields() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "replicas": {"type": "integer", "default": 1}
                }
            }
        }
    });
    let mut body = serde_json::json!({
        "spec": {"replicas": 5}
    });
    super::apply_schema_defaults_pub(&mut body, &schema);
    assert_eq!(
        body["spec"]["replicas"], 5,
        "Existing field must not be overwritten by default"
    );
}

#[test]
fn test_apply_schema_defaults_no_schema_properties_is_noop() {
    let schema = serde_json::json!({"type": "object"});
    let mut body = serde_json::json!({"spec": {"foo": "bar"}});
    let original = body.clone();
    super::apply_schema_defaults_pub(&mut body, &schema);
    assert_eq!(body, original, "No properties in schema = no changes");
}

struct StrictValidationCrdQuery {
    crd: klights_cluster_core::Resource,
}

impl klights_leader_api::LeaderResourceQuery for StrictValidationCrdQuery {
    fn get_resource(
        &self,
        request: klights_leader_api::ResourceGetRequest,
    ) -> klights_leader_api::ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>> {
        let expected = request.into_key();
        let crd = self.crd.clone();
        Box::pin(async move {
            Ok((expected.api_version == "apiextensions.k8s.io/v1"
                && expected.kind == "CustomResourceDefinition"
                && expected.name == "widgets.stable.example.com")
                .then_some(crd))
        })
    }

    fn list_resources(
        &self,
        request: klights_leader_api::ResourceListRequest,
    ) -> klights_leader_api::ResourceQueryFuture<'_, klights_leader_api::ResourceListResult> {
        let crd = self.crd.clone();
        Box::pin(async move {
            let items = (request.api_version() == "apiextensions.k8s.io/v1"
                && request.kind() == "CustomResourceDefinition")
                .then_some(crd)
                .into_iter()
                .collect();
            klights_leader_api::ResourceListResult::try_new(items, 1, None, None, None)
        })
    }
}

#[tokio::test]
async fn test_check_cr_field_validation_strict_with_crd_schema() {
    let crd = klights_cluster_core::Resource::try_from_data(Arc::new(serde_json::json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "name": "widgets.stable.example.com",
            "uid": "strict-validation-crd",
            "resourceVersion": "1"
        },
        "spec": {
            "group": "stable.example.com",
            "scope": "Namespaced",
            "names": {"kind": "Widget", "plural": "widgets", "singular": "widget"},
            "versions": [{
                "name": "v1",
                "served": true,
                "storage": true,
                "schema": {"openAPIV3Schema": {
                    "type": "object",
                    "properties": {"spec": {
                        "type": "object",
                        "properties": {
                            "color": {"type": "string"},
                            "size": {"type": "integer"}
                        }
                    }}
                }}
            }]
        }
    })))
    .expect("canonical CRD fixture");
    let query = StrictValidationCrdQuery { crd };

    let invalid = serde_json::json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test-widget"},
        "spec": {"color": "blue", "extraProperty": "should-fail"}
    });
    assert!(
        super::check_cr_field_validation_strict(
            &query,
            "stable.example.com",
            "v1",
            "Widget",
            &invalid,
        )
        .await
        .is_err()
    );

    let valid = serde_json::json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "test-widget"},
        "spec": {"color": "blue", "size": 5}
    });
    assert!(
        super::check_cr_field_validation_strict(
            &query,
            "stable.example.com",
            "v1",
            "Widget",
            &valid,
        )
        .await
        .is_ok()
    );
}

#[test]
fn test_validation_dns_subdomain_valid_names() {
    // Valid DNS subdomain names
    let valid_names = vec![
        "my-service",
        "nginx-1",
        "app.example.com",
        "test-123",
        "a",
        "1test",
        "test.with.dots",
        "test-with-hyphens",
    ];

    for name in valid_names {
        assert!(
            validate_dns_subdomain(name, "metadata.name").is_ok(),
            "Name '{}' should be valid",
            name
        );
    }
}

#[test]
fn test_validation_dns_subdomain_invalid_names() {
    // Invalid DNS subdomain names
    let invalid_names = vec![
        "MyService",             // Uppercase
        "my_service",            // Underscore
        "my service",            // Space
        "-starts-hyphen",        // Starts with hyphen
        "ends-hyphen-",          // Ends with hyphen
        ".starts-dot",           // Starts with dot
        "ends-dot.",             // Ends with dot
        "system:controller:foo", // Colon is valid only for RBAC path-segment names
        ":starts-colon",         // Starts with colon
        "ends-colon:",           // Ends with colon
        "has@special",           // Special characters
        "has!special",
        "", // Empty
    ];

    for name in invalid_names {
        assert!(
            validate_dns_subdomain(name, "metadata.name").is_err(),
            "Name '{}' should be invalid",
            name
        );
    }
}

#[test]
fn test_validation_dns_subdomain_length_limit() {
    // Max 253 characters
    let max_len_name = "a".repeat(253);
    assert!(
        validate_dns_subdomain(&max_len_name, "metadata.name").is_ok(),
        "253-char name should be valid"
    );

    let too_long_name = "a".repeat(254);
    assert!(
        validate_dns_subdomain(&too_long_name, "metadata.name").is_err(),
        "254-char name should be invalid"
    );
}

#[test]
fn test_metadata_name_validation_allows_colons_only_for_rbac_resources() {
    assert!(
        validate_metadata_name_for_kind(
            "rbac.authorization.k8s.io/v1",
            "ClusterRole",
            "system:controller:foo",
            "metadata.name",
        )
        .is_ok()
    );
    assert!(
        validate_metadata_name_for_kind(
            "rbac.authorization.k8s.io/v1",
            "ClusterRoleBinding",
            "wardler:aggregator-5592-sample-reader",
            "metadata.name",
        )
        .is_ok()
    );
    assert!(
        validate_metadata_name_for_kind(
            "rbac.authorization.k8s.io/v1",
            "Role",
            "namespace:reader",
            "metadata.name",
        )
        .is_ok()
    );
    assert!(
        validate_metadata_name_for_kind(
            "rbac.authorization.k8s.io/v1",
            "RoleBinding",
            "namespace:reader-binding",
            "metadata.name",
        )
        .is_ok()
    );

    for kind in ["Pod", "ConfigMap", "Secret", "Service", "Namespace"] {
        assert!(
            validate_metadata_name_for_kind("v1", kind, "system:controller:foo", "metadata.name",)
                .is_err(),
            "{kind} metadata.name must reject colons"
        );
    }
}

#[test]
fn test_namespace_metadata_name_uses_dns_label_validation() {
    assert!(
        validate_metadata_name_for_kind("v1", "Namespace", "team-alpha", "metadata.name",).is_ok()
    );
    assert!(
        validate_metadata_name_for_kind("v1", "Namespace", "team.alpha", "metadata.name").is_err(),
        "Namespace metadata.name must reject DNS subdomain dots"
    );
    assert!(
        validate_metadata_name_for_kind("v1", "Namespace", &"a".repeat(64), "metadata.name",)
            .is_err(),
        "Namespace metadata.name must enforce the DNS label length limit"
    );
}

#[test]
fn test_rbac_metadata_name_uses_path_segment_validation() {
    for invalid in [".", "..", "has/slash", "has%percent"] {
        assert!(
            validate_metadata_name_for_kind(
                "rbac.authorization.k8s.io/v1",
                "ClusterRole",
                invalid,
                "metadata.name",
            )
            .is_err(),
            "RBAC metadata.name must reject invalid path segment {invalid:?}"
        );
    }
}

#[test]
fn test_validation_dns_label_valid() {
    // Valid DNS labels (63 chars max, no dots)
    let valid_labels = vec!["nginx", "my-app-1", "test123", "a"];

    for label in valid_labels {
        assert!(
            validate_dns_label(label, "metadata.name").is_ok(),
            "Label '{}' should be valid",
            label
        );
    }
}

#[test]
fn test_validation_dns_label_invalid() {
    let invalid_labels = vec![
        "My-App", // Uppercase
        "-starts-hyphen",
        "ends-hyphen-",
        "has.dot", // Dots not allowed in labels
        "",
    ];

    for label in invalid_labels {
        assert!(
            validate_dns_label(label, "metadata.name").is_err(),
            "Label '{}' should be invalid",
            label
        );
    }
}

#[test]
fn test_validation_dns_label_length_limit() {
    // Max 63 characters for DNS label
    let max_len_label = "a".repeat(63);
    assert!(
        validate_dns_label(&max_len_label, "metadata.name").is_ok(),
        "63-char label should be valid"
    );

    let too_long_label = "a".repeat(64);
    assert!(
        validate_dns_label(&too_long_label, "metadata.name").is_err(),
        "64-char label should be invalid"
    );
}

#[test]
fn test_validate_pod_sysctls_allows_kubernetes_safe_sysctls() {
    let pod = json!({
        "spec": {
            "securityContext": {
                "sysctls": [
                    {"name": "kernel.shm_rmid_forced", "value": "1"},
                    {"name": "net.ipv4.ip_unprivileged_port_start", "value": "1024"}
                ]
            }
        }
    });
    assert!(validate_pod_sysctls(&pod).is_ok());
}

#[test]
fn test_validate_pod_sysctls_rejects_invalid_names() {
    let pod = json!({
        "spec": {
            "securityContext": {
                "sysctls": [
                    {"name": "foo-", "value": "bar"},
                    {"name": "kernel.shmmax", "value": "100000000"},
                    {"name": "safe-and-unsafe", "value": "100000000"},
                    {"name": "bar..", "value": "42"}
                ]
            }
        }
    });
    match validate_pod_sysctls(&pod) {
        Err(AppError::UnprocessableEntity(msg)) => {
            assert!(msg.contains("Invalid value: \"foo-\""), "{msg}");
            assert!(msg.contains("Invalid value: \"bar..\""), "{msg}");
            assert!(!msg.contains("safe-and-unsafe"), "{msg}");
            assert!(!msg.contains("kernel.shmmax"), "{msg}");
        }
        other => panic!("unexpected result: {:?}", other),
    }
}

#[test]
fn test_validate_pod_sysctls_allows_unsafe_names_at_api_validation() {
    let pod = json!({
        "spec": {
            "securityContext": {
                "sysctls": [
                    {"name": "kernel.shmmax", "value": "100000000"},
                    {"name": "safe-and-unsafe", "value": "100000000"}
                ]
            }
        }
    });
    assert!(validate_pod_sysctls(&pod).is_ok());
}

#[test]
fn test_validate_webhook_rejects_invalid_cel_in_match_conditions() {
    use serde_json::json;
    // The K8s conformance test "should reject validating webhook configurations with
    // invalid match conditions" sends a VWC with `invalid_cel!@#$` as the expression.
    let body = json!({
        "webhooks": [{
            "name": "test.k8s.io",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "clientConfig": {"url": "https://example.com/webhook"},
            "matchConditions": [{
                "name": "invalid-cond",
                "expression": "invalid_cel!@#$"
            }]
        }]
    });
    let result = validate_webhook_configuration(&body);
    assert!(
        result.is_err(),
        "must reject VWC with invalid CEL expression"
    );
    let err_str = format!("{:?}", result.unwrap_err());
    assert!(
        err_str.contains("compilation failed"),
        "error must mention compilation failure, got: {}",
        err_str
    );
}

#[test]
fn test_validate_webhook_rejects_cel_syntax_error_in_match_conditions() {
    use serde_json::json;
    // This uses only valid ASCII characters, so the old heuristic would incorrectly accept it.
    let body = json!({
        "webhooks": [{
            "name": "test.k8s.io",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "clientConfig": {"url": "https://example.com/webhook"},
            "matchConditions": [{
                "name": "syntax-error",
                "expression": "request.object.metadata.name =="
            }]
        }]
    });
    let result = validate_webhook_configuration(&body);
    assert!(
        result.is_err(),
        "must reject VWC with syntactically invalid CEL expression"
    );
}

#[test]
fn test_validate_webhook_accepts_valid_cel_in_match_conditions() {
    use serde_json::json;
    let body = json!({
        "webhooks": [{
            "name": "test.k8s.io",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "clientConfig": {"url": "https://example.com/webhook"},
            "matchConditions": [{
                "name": "allow-pods",
                "expression": "request.resource.resource == 'pods'"
            }]
        }]
    });
    let result = validate_webhook_configuration(&body);
    assert!(result.is_ok(), "must accept VWC with valid CEL expression");
}

#[test]
fn test_validate_webhook_rejects_empty_match_condition_name() {
    use serde_json::json;
    let body = json!({
        "webhooks": [{
            "name": "test.k8s.io",
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "clientConfig": {"url": "https://example.com/webhook"},
            "matchConditions": [{
                "name": "",
                "expression": "true"
            }]
        }]
    });
    let result = validate_webhook_configuration(&body);
    assert!(
        result.is_err(),
        "must reject matchCondition with empty name"
    );
}

#[test]
fn test_validate_against_schema_rejects_unknown_root_metadata_fields_for_schemaless_crs() {
    let schema = json!({
        "type": "object",
        "x-kubernetes-preserve-unknown-fields": true
    });
    let body = json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {
            "name": "root-meta",
            "unknownField": "must-fail"
        },
        "spec": {
            "freeform": {
                "nested": true
            }
        }
    });

    let result = validate_against_schema(&body, &schema, "");
    let err = result.expect_err("unknown root metadata fields must be rejected");
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("metadata.unknownField"),
        "error must mention the root metadata field path, got: {err_str}"
    );
}

#[test]
fn test_validate_against_schema_rejects_unknown_embedded_resource_metadata_fields() {
    let schema = json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "template": {
                        "type": "object",
                        "x-kubernetes-embedded-resource": true,
                        "properties": {
                            "spec": {
                                "type": "object",
                                "x-kubernetes-preserve-unknown-fields": true
                            }
                        }
                    }
                }
            }
        }
    });
    let body = json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "embedded-meta"},
        "spec": {
            "template": {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "nested-pod",
                    "unknownField": "must-fail"
                },
                "spec": {
                    "containers": []
                }
            }
        }
    });

    let result = validate_against_schema(&body, &schema, "");
    let err = result.expect_err("embedded resource metadata must reject unknown fields");
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("spec.template.metadata.unknownField"),
        "error must mention the embedded metadata field path, got: {err_str}"
    );
}

#[test]
fn test_validate_against_schema_rejects_unknown_fields_in_typed_array_items_under_schemaless_crs() {
    let schema = json!({
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "x-kubernetes-preserve-unknown-fields": true,
                "properties": {
                    "ports": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "containerPort": {"type": "integer"},
                                "protocol": {"type": "string"},
                                "hostPort": {"type": "integer"}
                            }
                        }
                    }
                }
            }
        }
    });
    let body = json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {"name": "schemaless-array"},
        "spec": {
            "freeform": {
                "stillAllowed": true
            },
            "ports": [{
                "name": "http",
                "containerPort": 8080,
                "protocol": "TCP",
                "hostPort": 8081,
                "unknownNested": "must-fail"
            }]
        }
    });

    let result = validate_against_schema(&body, &schema, "");
    let err =
        result.expect_err("typed array items under schemaless CRs must reject unknown fields");
    let err_str = format!("{err:?}");
    assert!(
        err_str.contains("spec.ports[0].unknownNested"),
        "error must mention the typed array item field path, got: {err_str}"
    );
}

#[test]
fn test_build_admission_context_for_delete_populates_old_object_and_options() {
    let old = json!({"metadata":{"name":"p0","namespace":"default"}});
    let ctx = build_admission_context(AdmissionContextRequest {
        api_version: "v1",
        kind: "Pod",
        operation: "DELETE",
        namespace: Some("default".to_string()),
        name: Some("p0".to_string()),
        object: Value::Null,
        old_object: Some(old.clone()),
        dry_run: true,
        subresource: None,
        options: Some(json!({"kind":"DeleteOptions","propagationPolicy":"Background"})),
    });
    assert_eq!(ctx.operation, "DELETE");
    assert_eq!(ctx.namespace.as_deref(), Some("default"));
    assert_eq!(ctx.name.as_deref(), Some("p0"));
    assert_eq!(ctx.object, Value::Null);
    assert_eq!(ctx.old_object, Some(old));
    assert_eq!(ctx.options.as_ref().unwrap()["kind"], "DeleteOptions");
    assert_eq!(ctx.dry_run, Some(true));
}

#[tokio::test]
async fn test_check_cr_field_validation_strict_accepts_valid_cr_with_schema_arrays_and_embedded_resource()
 {
    let crd = json!({
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "widgets.stable.example.com"},
        "spec": {
            "group": "stable.example.com",
            "scope": "Namespaced",
            "names": {
                "kind": "Widget",
                "plural": "widgets",
                "singular": "widget"
            },
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
                                    "knownField1": {"type": "string"},
                                    "ports": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "name": {"type": "string"},
                                                "containerPort": {"type": "integer"},
                                                "protocol": {"type": "string"},
                                                "hostPort": {"type": "integer"}
                                            }
                                        }
                                    },
                                    "embeddedObj": {
                                        "type": "object",
                                        "x-kubernetes-embedded-resource": true
                                    }
                                }
                            }
                        }
                    }
                }
            }]
        }
    });
    let crd = klights_cluster_core::Resource::try_from_data(Arc::new(crd))
        .expect("canonical CRD fixture");
    let query = StrictValidationCrdQuery { crd };

    let valid_body = json!({
        "apiVersion": "stable.example.com/v1",
        "kind": "Widget",
        "metadata": {
            "name": "valid-widget",
            "resourceVersion": "7"
        },
        "spec": {
            "knownField1": "val1",
            "ports": [{
                "name": "portName",
                "containerPort": 8080,
                "protocol": "TCP",
                "hostPort": 8081
            }],
            "embeddedObj": {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "my-cm"
                }
            }
        }
    });

    let result =
        check_cr_field_validation_strict(&query, "stable.example.com", "v1", "Widget", &valid_body)
            .await;
    assert!(
        result.is_ok(),
        "valid CR with schema-backed arrays and embedded resource must be accepted: {result:?}"
    );
}
