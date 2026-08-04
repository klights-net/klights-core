use super::request_context::{is_admission_operation, parse_api_group_version};
use super::selectors::matches_label_selector;
use super::webhook_response::{
    apply_mutation, build_admission_review, ensure_webhook_allowed, is_admission_allowed,
    webhook_denial_message, webhook_warnings,
};
use super::webhook_rules::{
    evaluate_match_conditions, matches_webhook_rules, should_call_webhook, should_reinvoke_webhook,
    webhook_side_effects_allow_dry_run, webhook_timeout_seconds,
};
use super::*;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct DeterministicApiIdentity;

impl crate::ApiIdentityGenerator for DeterministicApiIdentity {
    fn generate_name(&self, prefix: &str) -> String {
        format!("{prefix}00000")
    }

    fn new_uid(&self) -> String {
        "00000000-0000-4000-8000-000000000000".to_string()
    }
}

fn deterministic_api_identity() -> Arc<dyn crate::ApiIdentityGenerator> {
    Arc::new(DeterministicApiIdentity)
}

#[derive(Default)]
struct FakeAdmissionQuery {
    resources: Vec<AdmissionResource>,
}

#[async_trait::async_trait]
impl AdmissionQuery for FakeAdmissionQuery {
    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> std::result::Result<Option<AdmissionResource>, AdmissionDependencyError> {
        Ok(self
            .resources
            .iter()
            .find(|resource| {
                resource.name == name
                    && resource.data["apiVersion"] == api_version
                    && resource.data["kind"] == kind
                    && resource
                        .data
                        .pointer("/metadata/namespace")
                        .and_then(Value::as_str)
                        == namespace
            })
            .cloned())
    }

    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        _label_selector: Option<&str>,
    ) -> std::result::Result<Vec<AdmissionResource>, AdmissionDependencyError> {
        Ok(self
            .resources
            .iter()
            .filter(|resource| {
                resource.data["apiVersion"] == api_version
                    && resource.data["kind"] == kind
                    && resource
                        .data
                        .pointer("/metadata/namespace")
                        .and_then(Value::as_str)
                        == namespace
            })
            .cloned()
            .collect())
    }
}

struct FakeWebhookTargetResolver;

#[async_trait::async_trait]
impl WebhookTargetResolver for FakeWebhookTargetResolver {
    async fn resolve(
        &self,
        client_config: &Value,
    ) -> std::result::Result<WebhookTarget, AdmissionDependencyError> {
        Ok(WebhookTarget {
            base_url: client_config["url"]
                .as_str()
                .unwrap_or("https://fake")
                .to_string(),
            dns_override: None,
        })
    }
}

#[derive(Default)]
struct FakeAdmissionWebhookClient {
    requests: Mutex<Vec<AdmissionWebhookRequest>>,
}

#[async_trait::async_trait]
impl AdmissionWebhookClient for FakeAdmissionWebhookClient {
    async fn call(
        &self,
        request: AdmissionWebhookRequest,
    ) -> std::result::Result<Value, AdmissionDependencyError> {
        self.requests.lock().unwrap().push(request);
        Ok(json!({"response": {"allowed": true}}))
    }
}
fn test_ctx(
    api_version: &str,
    resource: &str,
    operation: &str,
    namespace: Option<&str>,
    subresource: Option<&str>,
) -> AdmissionRequestContext {
    let (group, version) = parse_api_group_version(api_version);
    AdmissionRequestContext {
        request_uid: "test-request".to_string(),
        api_version: api_version.to_string(),
        api_group: group,
        version,
        kind: "TestKind".to_string(),
        resource: resource.to_string(),
        subresource: subresource.map(ToString::to_string),
        operation: operation.to_string(),
        namespace: namespace.map(ToString::to_string),
        name: Some("obj".to_string()),
        dry_run: None,
        object: json!({"metadata":{"name":"obj"}}),
        old_object: None,
        options: None,
    }
}

// ========================
// matches_webhook_rules tests
// ========================

#[test]
fn test_matches_webhook_rules_exact_match() {
    let webhook = json!({
        "rules": [{
            "operations": ["CREATE"],
            "apiGroups": [""],
            "apiVersions": ["v1"],
            "resources": ["pods"]
        }]
    });
    let ctx = test_ctx("v1", "pods", "CREATE", Some("default"), None);
    assert!(matches_webhook_rules(&webhook, &ctx));
}

#[test]
fn test_matches_webhook_rules_wildcard_operation() {
    let webhook = json!({
        "rules": [{
            "operations": ["*"],
            "apiGroups": [""],
            "apiVersions": ["v1"],
            "resources": ["pods"]
        }]
    });
    let ctx = test_ctx("v1", "pods", "DELETE", Some("default"), None);
    assert!(matches_webhook_rules(&webhook, &ctx));
}

#[test]
fn test_matches_webhook_rules_no_match_wrong_kind() {
    let webhook = json!({
        "rules": [{
            "operations": ["CREATE"],
            "apiGroups": [""],
            "apiVersions": ["v1"],
            "resources": ["services"]
        }]
    });
    let ctx = test_ctx("v1", "pods", "CREATE", Some("default"), None);
    assert!(!matches_webhook_rules(&webhook, &ctx));
}

#[test]
fn test_matches_webhook_rules_no_match_wrong_operation() {
    let webhook = json!({
        "rules": [{
            "operations": ["CREATE"],
            "apiGroups": [""],
            "apiVersions": ["v1"],
            "resources": ["pods"]
        }]
    });
    let ctx = test_ctx("v1", "pods", "DELETE", Some("default"), None);
    assert!(!matches_webhook_rules(&webhook, &ctx));
}

#[test]
fn test_matches_webhook_rules_operation_case_insensitive_for_protobuf_shape() {
    let webhook = json!({
        "rules": [{
            "operations": ["Create"],
            "apiGroups": [""],
            "apiVersions": ["v1"],
            "resources": ["configmaps"]
        }]
    });
    let ctx = test_ctx("v1", "configmaps", "CREATE", Some("default"), None);
    assert!(
        matches_webhook_rules(&webhook, &ctx),
        "Admission rules must match protobuf-decoded operation spellings like 'Create'"
    );
}

#[test]
fn test_matches_webhook_rules_no_rules() {
    let webhook = json!({});
    let ctx = test_ctx("v1", "pods", "CREATE", Some("default"), None);
    assert!(!matches_webhook_rules(&webhook, &ctx));
}

#[test]
fn test_matches_webhook_rules_wildcard_api_version() {
    let webhook = json!({
        "rules": [{
            "operations": ["CREATE"],
            "apiGroups": ["apps"],
            "apiVersions": ["*"],
            "resources": ["deployments"]
        }]
    });
    let ctx = test_ctx("apps/v1", "deployments", "CREATE", Some("default"), None);
    assert!(matches_webhook_rules(&webhook, &ctx));
}

#[test]
fn test_matches_webhook_rules_subresource_match() {
    let webhook = json!({
        "rules": [{
            "operations": ["UPDATE"],
            "apiGroups": ["apps"],
            "apiVersions": ["v1"],
            "resources": ["deployments/status"]
        }]
    });
    let ctx = test_ctx(
        "apps/v1",
        "deployments",
        "UPDATE",
        Some("default"),
        Some("status"),
    );
    assert!(matches_webhook_rules(&webhook, &ctx));
}

#[test]
fn test_matches_webhook_rules_scope_namespaced_vs_cluster() {
    let webhook = json!({
        "rules": [{
            "operations": ["CREATE"],
            "apiGroups": [""],
            "apiVersions": ["v1"],
            "resources": ["pods"],
            "scope": "Cluster"
        }]
    });
    let namespaced_ctx = test_ctx("v1", "pods", "CREATE", Some("default"), None);
    assert!(!matches_webhook_rules(&webhook, &namespaced_ctx));

    let cluster_ctx = test_ctx("v1", "pods", "CREATE", None, None);
    assert!(matches_webhook_rules(&webhook, &cluster_ctx));
}

#[test]
fn test_matches_label_selector_expressions_matrix() {
    let labels = std::collections::BTreeMap::from([
        ("app".to_string(), "web".to_string()),
        ("tier".to_string(), "frontend".to_string()),
    ]);
    let selector = json!({
        "matchLabels": {"app": "web"},
        "matchExpressions": [
            {"key":"tier","operator":"In","values":["frontend","edge"]},
            {"key":"track","operator":"DoesNotExist"},
            {"key":"app","operator":"Exists"},
            {"key":"env","operator":"NotIn","values":["prod"]}
        ]
    });
    assert!(matches_label_selector(&selector, &labels));

    let fail_selector = json!({
        "matchExpressions": [
            {"key":"tier","operator":"In","values":["backend"]}
        ]
    });
    assert!(!matches_label_selector(&fail_selector, &labels));
}

#[test]
fn test_build_admission_review_includes_request_fields() {
    let mut ctx = test_ctx(
        "apps/v1",
        "deployments",
        "UPDATE",
        Some("default"),
        Some("status"),
    );
    ctx.kind = "Deployment".to_string();
    ctx.name = Some("d1".to_string());
    ctx.dry_run = Some(true);
    ctx.old_object = Some(json!({"metadata":{"name":"d1"},"spec":{"replicas":1}}));

    let new_obj = json!({"metadata":{"name":"d1"},"spec":{"replicas":2}});
    let review = build_admission_review(&ctx, &new_obj);
    assert_eq!(review["apiVersion"], "admission.k8s.io/v1");
    assert_eq!(review["kind"], "AdmissionReview");
    assert_eq!(review["request"]["operation"], "UPDATE");
    assert_eq!(review["request"]["namespace"], "default");
    assert_eq!(review["request"]["name"], "d1");
    assert_eq!(review["request"]["resource"]["resource"], "deployments");
    assert_eq!(review["request"]["subResource"], "status");
    assert_eq!(review["request"]["dryRun"], true);
    assert_eq!(review["request"]["oldObject"]["spec"]["replicas"], 1);
    assert_eq!(review["request"]["object"]["spec"]["replicas"], 2);
}

#[test]
fn test_build_admission_review_includes_options_for_delete() {
    let mut ctx = test_ctx("v1", "pods", "DELETE", Some("default"), None);
    ctx.kind = "Pod".to_string();
    ctx.name = Some("p0".to_string());
    ctx.object = serde_json::Value::Null;
    ctx.old_object = Some(serde_json::json!({"metadata":{"name":"p0"}}));
    ctx.options = Some(serde_json::json!({
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "propagationPolicy": "Background"
    }));

    let review = build_admission_review(&ctx, &ctx.object);
    assert_eq!(review["request"]["object"], serde_json::Value::Null);
    assert_eq!(review["request"]["oldObject"]["metadata"]["name"], "p0");
    assert_eq!(review["request"]["options"]["kind"], "DeleteOptions");
}

#[test]
fn test_webhook_timeout_seconds_default_and_clamp() {
    assert_eq!(webhook_timeout_seconds(&json!({})), 10);
    assert_eq!(webhook_timeout_seconds(&json!({"timeoutSeconds": 0})), 10);
    assert_eq!(webhook_timeout_seconds(&json!({"timeoutSeconds": 1})), 1);
    assert_eq!(webhook_timeout_seconds(&json!({"timeoutSeconds": 30})), 30);
    assert_eq!(webhook_timeout_seconds(&json!({"timeoutSeconds": 120})), 30);
}

#[test]
fn test_format_webhook_call_error_timeout_includes_deadline_phrase() {
    let msg = format_webhook_call_error(
        "https://e2e-test-webhook.default.svc:8443/pods?timeout=10s",
        "operation timed out",
        false,
    );
    assert!(msg.contains("context deadline exceeded"));
    assert!(msg.contains("timeout=10s"));
}

#[test]
fn test_format_webhook_call_error_non_timeout_keeps_original_shape() {
    let msg = format_webhook_call_error(
        "https://e2e-test-webhook.default.svc:8443/pods?timeout=10s",
        "connection refused",
        false,
    );
    assert!(!msg.contains("context deadline exceeded"));
    assert!(msg.contains("connection refused"));
}

#[test]
fn test_webhook_side_effects_allow_dry_run_matrix() {
    assert!(webhook_side_effects_allow_dry_run(
        &json!({"sideEffects": "None"})
    ));
    assert!(webhook_side_effects_allow_dry_run(
        &json!({"sideEffects": "NoneOnDryRun"})
    ));
    assert!(!webhook_side_effects_allow_dry_run(
        &json!({"sideEffects": "Some"})
    ));
    assert!(!webhook_side_effects_allow_dry_run(
        &json!({"sideEffects": "Unknown"})
    ));
    assert!(!webhook_side_effects_allow_dry_run(&json!({})));
}

#[test]
fn test_match_conditions_false_skips_webhook() {
    let conditions = vec![json!({
        "name": "skip",
        "expression": "false"
    })];
    let ctx = test_ctx("v1", "pods", "CREATE", Some("default"), None);
    assert!(!evaluate_match_conditions(&conditions, &ctx, &ctx.object, "Fail").unwrap());
}

#[test]
fn test_match_conditions_ignore_failure_policy_skips_on_runtime_error() {
    let conditions = vec![json!({
        "name": "explode",
        "expression": "request.doesNotExist.field == 'x'"
    })];
    let ctx = test_ctx("v1", "pods", "CREATE", Some("default"), None);
    assert!(!evaluate_match_conditions(&conditions, &ctx, &ctx.object, "Ignore").unwrap());
}

#[test]
fn test_match_conditions_fail_failure_policy_rejects_on_runtime_error() {
    let conditions = vec![json!({
        "name": "explode",
        "expression": "request.doesNotExist.field == 'x'"
    })];
    let ctx = test_ctx("v1", "pods", "CREATE", Some("default"), None);
    let err = evaluate_match_conditions(&conditions, &ctx, &ctx.object, "Fail")
        .unwrap_err()
        .to_string();
    assert!(err.contains("matchCondition evaluation failed"));
}

#[test]
fn test_match_conditions_request_expression_matches_context() {
    let conditions = vec![json!({
        "name": "create-pods-only",
        "expression": "request.operation == 'CREATE' && request.resource.resource == 'pods'"
    })];
    let ctx = test_ctx("v1", "pods", "CREATE", Some("default"), None);
    assert!(evaluate_match_conditions(&conditions, &ctx, &ctx.object, "Fail").unwrap());
}

#[test]
fn test_should_call_webhook_skip_me_match_condition_skips_webhook() {
    let webhook = json!({
        "rules": [{
            "operations": ["CREATE"],
            "apiGroups": [""],
            "apiVersions": ["v1"],
            "resources": ["configmaps"]
        }],
        "matchConditions": [{
            "name": "skip-me",
            "expression": "object.metadata.name != 'skip-me'"
        }]
    });
    let resource = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "skip-me", "namespace": "default"},
        "data": {"mutation-start": "yes"}
    });
    let ctx = test_ctx("v1", "configmaps", "CREATE", Some("default"), None);
    assert!(
        !should_call_webhook(&webhook, &ctx, &resource, None).unwrap(),
        "skip-me object must not match the matchCondition"
    );
}

#[test]
fn test_should_call_webhook_accepts_snake_case_match_conditions_key() {
    let webhook = json!({
        "rules": [{
            "operations": ["CREATE"],
            "apiGroups": [""],
            "apiVersions": ["v1"],
            "resources": ["configmaps"]
        }],
        "match_conditions": [{
            "name": "skip-me",
            "expression": "object.metadata.name != 'skip-me'"
        }]
    });
    let resource = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "skip-me", "namespace": "default"},
        "data": {"mutation-start": "yes"}
    });
    let ctx = test_ctx("v1", "configmaps", "CREATE", Some("default"), None);
    assert!(
        !should_call_webhook(&webhook, &ctx, &resource, None).unwrap(),
        "snake_case match_conditions key must be honored"
    );
}

#[test]
fn test_admission_webhook_objectselector_uses_cached_parse() {
    // CachedWebhook parses objectSelector once at construction. Mutating
    // the underlying webhook Value's objectSelector after caching MUST
    // NOT affect future calls — the cache is the source of truth, and a
    // stale cache reflects exactly the selector that was registered.
    use crate::admission::webhook_rules::CachedWebhook;

    let mut webhook = json!({
        "name": "obj-selector.example.com",
        "rules": [{
            "operations": ["CREATE"],
            "apiGroups": [""],
            "apiVersions": ["v1"],
            "resources": ["configmaps"]
        }],
        "objectSelector": {"matchLabels": {"app": "demo"}}
    });
    let cached = CachedWebhook::from_value(webhook.clone());

    let matching_resource = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cm", "namespace": "default", "labels": {"app": "demo"}},
    });
    let non_matching_resource = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cm", "namespace": "default", "labels": {"app": "other"}},
    });
    let ctx = test_ctx("v1", "configmaps", "CREATE", Some("default"), None);

    assert!(
        crate::admission::webhook_rules::should_call_cached_webhook(
            &cached,
            &ctx,
            &matching_resource,
            None
        )
        .unwrap()
    );
    assert!(
        !crate::admission::webhook_rules::should_call_cached_webhook(
            &cached,
            &ctx,
            &non_matching_resource,
            None
        )
        .unwrap()
    );

    // Mutate the source Value AFTER caching — cache must not see this.
    webhook["objectSelector"] = json!({"matchLabels": {"app": "other"}});
    // The cached selector is unchanged, so the demo-labeled resource still matches
    // and the other-labeled one still doesn't.
    assert!(
        crate::admission::webhook_rules::should_call_cached_webhook(
            &cached,
            &ctx,
            &matching_resource,
            None
        )
        .unwrap()
    );
    assert!(
        !crate::admission::webhook_rules::should_call_cached_webhook(
            &cached,
            &ctx,
            &non_matching_resource,
            None
        )
        .unwrap()
    );

    // Re-cache after the mutation — now the OTHER label matches.
    let recached = CachedWebhook::from_value(webhook);
    assert!(
        crate::admission::webhook_rules::should_call_cached_webhook(
            &recached,
            &ctx,
            &non_matching_resource,
            None
        )
        .unwrap()
    );
}

#[test]
fn test_admission_cached_webhook_objectselector_match_expressions() {
    use crate::admission::webhook_rules::CachedWebhook;

    let webhook = json!({
        "name": "expr.example.com",
        "rules": [{
            "operations": ["CREATE"],
            "apiGroups": [""],
            "apiVersions": ["v1"],
            "resources": ["configmaps"]
        }],
        "objectSelector": {
            "matchExpressions": [
                {"key": "tier", "operator": "In", "values": ["fe", "be"]}
            ]
        }
    });
    let cached = CachedWebhook::from_value(webhook);
    let ctx = test_ctx("v1", "configmaps", "CREATE", Some("default"), None);

    let fe_resource = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cm", "namespace": "default", "labels": {"tier": "fe"}},
    });
    let data_resource = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cm", "namespace": "default", "labels": {"tier": "data"}},
    });
    assert!(
        crate::admission::webhook_rules::should_call_cached_webhook(
            &cached,
            &ctx,
            &fe_resource,
            None
        )
        .unwrap()
    );
    assert!(
        !crate::admission::webhook_rules::should_call_cached_webhook(
            &cached,
            &ctx,
            &data_resource,
            None
        )
        .unwrap()
    );
}

#[test]
fn test_should_reinvoke_ifneeded_webhook_after_later_mutation() {
    assert!(should_reinvoke_webhook(true, Some("IfNeeded")));
    assert!(!should_reinvoke_webhook(false, Some("IfNeeded")));
    assert!(!should_reinvoke_webhook(true, None));
}

// ========================
// is_admission_allowed tests
// ========================

#[test]
fn test_is_admission_allowed_true() {
    let response = json!({"response": {"allowed": true}});
    assert!(is_admission_allowed(&response));
}

#[test]
fn test_is_admission_allowed_false() {
    let response = json!({"response": {"allowed": false}});
    assert!(!is_admission_allowed(&response));
}

#[test]
fn test_is_admission_allowed_missing_defaults_true() {
    // Per K8s spec, missing allowed field defaults to true
    let response = json!({"response": {}});
    assert!(is_admission_allowed(&response));
}

// ========================
// apply_mutation tests
// ========================

#[test]
fn test_apply_mutation_json_patch() {
    use base64::Engine;

    let resource = json!({
        "metadata": {"name": "test", "labels": {}},
        "spec": {"replicas": 1}
    });

    // JSON Patch: add a label
    let patch_ops = json!([
        {"op": "add", "path": "/metadata/labels/injected", "value": "true"}
    ]);
    let patch_b64 = base64::engine::general_purpose::STANDARD
        .encode(serde_json::to_string(&patch_ops).unwrap());

    let response = json!({
        "response": {
            "allowed": true,
            "patchType": "JSONPatch",
            "patch": patch_b64
        }
    });

    let result = apply_mutation(resource, response).unwrap();
    assert_eq!(result["metadata"]["labels"]["injected"], "true");
    assert_eq!(result["spec"]["replicas"], 1); // untouched
}

#[test]
fn test_apply_mutation_no_patch_returns_unchanged() {
    let resource = json!({"metadata": {"name": "test"}});
    let response = json!({"response": {"allowed": true}});

    let result = apply_mutation(resource.clone(), response).unwrap();
    assert_eq!(result, resource);
}

#[test]
fn test_apply_mutation_patch_without_patch_type_rejected() {
    use base64::Engine;
    let resource = json!({"metadata": {"name": "test", "labels": {}}});
    let patch_ops = json!([{"op": "add", "path": "/metadata/labels/x", "value": "y"}]);
    let patch_b64 = base64::engine::general_purpose::STANDARD
        .encode(serde_json::to_string(&patch_ops).unwrap());
    let response = json!({
        "response": {
            "allowed": true,
            "patch": patch_b64
        }
    });
    let err = apply_mutation(resource, response).unwrap_err().to_string();
    assert!(err.contains("missing patchType"));
}

#[test]
fn test_apply_mutation_unsupported_patch_type_rejected() {
    use base64::Engine;
    let resource = json!({"metadata": {"name": "test", "labels": {}}});
    let patch_ops = json!([{"op": "add", "path": "/metadata/labels/x", "value": "y"}]);
    let patch_b64 = base64::engine::general_purpose::STANDARD
        .encode(serde_json::to_string(&patch_ops).unwrap());
    let response = json!({
        "response": {
            "allowed": true,
            "patchType": "MergePatch",
            "patch": patch_b64
        }
    });
    let err = apply_mutation(resource, response).unwrap_err().to_string();
    assert!(err.contains("Unsupported webhook patchType"));
}

// ========================
// webhook_denial_message tests
// ========================

#[test]
fn test_webhook_denial_message_with_message() {
    let response = json!({
        "response": {
            "allowed": false,
            "status": {"message": "policy violation: no latest tag"}
        }
    });
    assert_eq!(
        webhook_denial_message(&response),
        "policy violation: no latest tag"
    );
}

#[test]
fn test_webhook_denial_message_falls_back_to_reason() {
    let response = json!({
        "response": {
            "allowed": false,
            "status": {"reason": "the custom resource contains unwanted data"}
        }
    });
    assert_eq!(
        webhook_denial_message(&response),
        "the custom resource contains unwanted data"
    );
}

#[test]
fn test_webhook_denial_message_falls_back_to_status_cause_message() {
    let response = json!({
        "response": {
            "allowed": false,
            "status": {
                "message": "webhook denied request",
                "details": {
                    "causes": [{
                        "message": "the custom resource contains unwanted data"
                    }]
                }
            }
        }
    });
    assert_eq!(
        webhook_denial_message(&response),
        "the custom resource contains unwanted data"
    );
}

#[test]
fn test_webhook_denial_message_default() {
    let response = json!({"response": {"allowed": false}});
    assert_eq!(webhook_denial_message(&response), "webhook denied request");
}

#[test]
fn test_webhook_warnings_extracts_strings() {
    let response = json!({
        "response": {
            "allowed": true,
            "warnings": ["w1", "w2", 3]
        }
    });
    let warnings = webhook_warnings(&response);
    assert_eq!(warnings, vec!["w1".to_string(), "w2".to_string()]);
}

#[test]
fn test_ensure_webhook_allowed_accepts_allowed_response() {
    let response = json!({"response": {"allowed": true}});
    assert!(ensure_webhook_allowed(&response).is_ok());
}

#[test]
fn test_ensure_webhook_allowed_rejects_denied_response() {
    let response = json!({
        "response": {
            "allowed": false,
            "status": {"message": "this webhook denies all requests"}
        }
    });
    let err = ensure_webhook_allowed(&response).unwrap_err().to_string();
    assert!(err.contains("Admission denied by webhook"));
    assert!(err.contains("this webhook denies all requests"));
}
#[tokio::test]
async fn test_admission_engine_accepts_focused_lookup_trait_object() {
    let query = FakeAdmissionQuery::default();
    let resolver = FakeWebhookTargetResolver;
    let client = FakeAdmissionWebhookClient::default();
    let identity = deterministic_api_identity();
    let engine = AdmissionEngine::new(identity.as_ref(), &query, &resolver, &client);
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "traitobj-p0", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });

    let got = engine
        .run_with_context(
            &AdmissionRequestContext::from_legacy(&pod, "v1", "Pod", "CREATE"),
            true,
        )
        .await
        .unwrap();
    assert_eq!(got, pod);
}

#[tokio::test]
async fn test_admission_policy_uses_fake_query_target_and_client_ports() {
    let query = FakeAdmissionQuery {
        resources: vec![AdmissionResource {
            name: "fake-mutator".to_string(),
            data: Arc::new(json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "MutatingWebhookConfiguration",
                "metadata": {"name": "fake-mutator"},
                "webhooks": [{
                    "name": "fake.mutator.example",
                    "clientConfig": {"url": "https://fake.example/mutate"},
                    "rules": [{
                        "operations": ["CREATE"],
                        "apiGroups": [""],
                        "apiVersions": ["v1"],
                        "resources": ["pods"]
                    }]
                }]
            })),
        }],
    };
    let resolver = FakeWebhookTargetResolver;
    let client = FakeAdmissionWebhookClient::default();
    let identity = deterministic_api_identity();
    let engine = AdmissionEngine::new(identity.as_ref(), &query, &resolver, &client);
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "fake-ports-pod", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });

    let admitted = engine
        .run_mutating(&pod, "v1", "Pod", "CREATE")
        .await
        .unwrap();

    assert_eq!(admitted, pod);
    let requests = client.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].target.base_url, "https://fake.example/mutate");
    assert_eq!(
        requests[0]
            .admission_review
            .pointer("/request/object/metadata/name")
            .and_then(Value::as_str),
        Some("fake-ports-pod")
    );
}

#[test]
fn test_is_admission_operation_matrix() {
    assert!(is_admission_operation("CREATE"));
    assert!(is_admission_operation("UPDATE"));
    assert!(is_admission_operation("DELETE"));
    assert!(is_admission_operation("CONNECT"));
    assert!(!is_admission_operation("GET"));
    assert!(!is_admission_operation("LIST"));
    assert!(!is_admission_operation("WATCH"));
}

#[test]
fn test_admission_request_context_from_legacy_fields() {
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p0", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });
    let ctx = AdmissionRequestContext::from_legacy(&pod, "v1", "Pod", "CREATE");
    assert_eq!(ctx.api_version, "v1");
    assert_eq!(ctx.kind, "Pod");
    assert_eq!(ctx.resource, "pods");
    assert_eq!(ctx.namespace.as_deref(), Some("default"));
    assert_eq!(ctx.operation, "CREATE");
}
