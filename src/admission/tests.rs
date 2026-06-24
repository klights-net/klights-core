use super::*;
use serde_json::json;

fn test_ctx(
    api_version: &str,
    resource: &str,
    operation: &str,
    namespace: Option<&str>,
    subresource: Option<&str>,
) -> AdmissionRequestContext {
    let (group, version) = parse_api_group_version(api_version);
    AdmissionRequestContext {
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

#[test]
fn test_webhook_http_client_for_invalid_cabundle_errors() {
    let cfg = json!({"caBundle": "%%%not-base64%%%"});
    let err = webhook_http_client_for(&cfg, None).unwrap_err().to_string();
    assert!(err.contains("Invalid base64"));
}

fn test_ca_bundle(name: &str) -> String {
    use base64::Engine;

    let cert = rcgen::generate_simple_self_signed(vec![name.to_string()]).unwrap();
    base64::engine::general_purpose::STANDARD.encode(cert.cert.pem().as_bytes())
}

#[test]
fn test_webhook_http_client_cache_source_uses_fingerprint_and_no_expect() {
    // R4: invariant now enforced by check_supervisor_spawn.sh
}

#[test]
fn test_webhook_ca_bundle_cache_hits_by_fingerprint() {
    let bundle = test_ca_bundle("cache-hit.example");
    let config = json!({"caBundle": bundle});
    let fingerprint = super::http_client::ca_bundle_fingerprint(&config).unwrap();
    let mut cache = super::http_client::CaBundleClientCache::new_for_test(4);

    cache.client_for(&config).unwrap();
    cache.client_for(&config).unwrap();

    assert_eq!(cache.len_for_test(), 1);
    assert!(cache.contains_for_test(&fingerprint));
}

#[test]
fn test_webhook_ca_bundle_cache_evicts_lru_entry() {
    let bundle_a = test_ca_bundle("cache-a.example");
    let bundle_b = test_ca_bundle("cache-b.example");
    let bundle_c = test_ca_bundle("cache-c.example");
    let config_a = json!({"caBundle": bundle_a});
    let config_b = json!({"caBundle": bundle_b});
    let config_c = json!({"caBundle": bundle_c});
    let fingerprint_a = super::http_client::ca_bundle_fingerprint(&config_a).unwrap();
    let fingerprint_b = super::http_client::ca_bundle_fingerprint(&config_b).unwrap();
    let fingerprint_c = super::http_client::ca_bundle_fingerprint(&config_c).unwrap();
    let mut cache = super::http_client::CaBundleClientCache::new_for_test(2);

    cache.client_for(&config_a).unwrap();
    cache.client_for(&config_b).unwrap();
    cache.client_for(&config_a).unwrap();
    cache.client_for(&config_c).unwrap();

    assert_eq!(cache.len_for_test(), 2);
    assert!(cache.contains_for_test(&fingerprint_a));
    assert!(!cache.contains_for_test(&fingerprint_b));
    assert!(cache.contains_for_test(&fingerprint_c));
}

#[test]
fn test_webhook_ca_bundle_cache_poisoned_lock_returns_error() {
    use std::sync::{Arc, Mutex};

    let cache = Arc::new(Mutex::new(
        super::http_client::CaBundleClientCache::new_for_test(1),
    ));
    let poisoned = Arc::clone(&cache);
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let result = std::thread::spawn(move || {
        let _guard = poisoned.lock().unwrap();
        panic!("poison caBundle cache");
    })
    .join();
    std::panic::set_hook(default_hook);
    assert!(result.is_err());

    let err = match super::http_client::lock_ca_bundle_cache_for_test(&cache) {
        Ok(_) => panic!("poisoned cache lock must return an error"),
        Err(err) => err.to_string(),
    };

    assert!(err.contains("caBundle client cache poisoned"));
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

// ========================
// resolve_webhook_target tests
// ========================

#[tokio::test]
async fn test_resolve_webhook_target_from_url_field() {
    let db = crate::datastore::test_support::in_memory().await;
    let client_config = json!({"url": "https://webhook.example.com/validate"});

    let target = resolve_webhook_target(&db, &client_config).await.unwrap();
    assert_eq!(target.base_url, "https://webhook.example.com/validate");
    assert_eq!(target.dns_override, None);
}

#[tokio::test]
async fn test_resolve_webhook_target_from_service_reference() {
    let db = crate::datastore::test_support::in_memory().await;

    // Create a Service in the DB
    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "webhook-service",
            "namespace": "cert-manager"
        },
        "spec": {
            "clusterIP": "10.43.200.50",
            "ports": [{"port": 443}]
        }
    });
    db.create_resource(
        "v1",
        "Service",
        Some("cert-manager"),
        "webhook-service",
        service,
    )
    .await
    .unwrap();

    let client_config = json!({
        "service": {
            "name": "webhook-service",
            "namespace": "cert-manager",
            "path": "/validate"
        }
    });

    let target = resolve_webhook_target(&db, &client_config).await.unwrap();
    assert_eq!(
        target.base_url,
        "https://webhook-service.cert-manager.svc:443/validate"
    );
    assert_eq!(
        target.dns_override,
        Some((
            "webhook-service.cert-manager.svc".to_string(),
            SocketAddr::from((std::net::Ipv4Addr::new(10, 43, 200, 50), 443)),
        ))
    );
}

#[tokio::test]
async fn test_resolve_webhook_target_service_with_port_specified() {
    let db = crate::datastore::test_support::in_memory().await;

    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "webhook-service",
            "namespace": "default"
        },
        "spec": {
            "clusterIP": "10.43.128.100",
            "ports": [{"port": 8443}, {"port": 9443}]
        }
    });
    db.create_resource("v1", "Service", Some("default"), "webhook-service", service)
        .await
        .unwrap();

    let client_config = json!({
        "service": {
            "name": "webhook-service",
            "namespace": "default",
            "port": 9443
        }
    });

    let target = resolve_webhook_target(&db, &client_config).await.unwrap();
    assert_eq!(target.base_url, "https://webhook-service.default.svc:9443");
    assert_eq!(
        target.dns_override,
        Some((
            "webhook-service.default.svc".to_string(),
            SocketAddr::from((std::net::Ipv4Addr::new(10, 43, 128, 100), 9443)),
        ))
    );
}

#[tokio::test]
async fn test_resolve_webhook_target_uses_cluster_ip_even_when_endpoints_exist() {
    let db = crate::datastore::test_support::in_memory().await;

    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "webhook-service",
            "namespace": "default"
        },
        "spec": {
            "clusterIP": "10.43.128.100",
            "ports": [{"name":"https","port":443}]
        }
    });
    db.create_resource("v1", "Service", Some("default"), "webhook-service", service)
        .await
        .unwrap();

    let endpoints = json!({
        "apiVersion": "v1",
        "kind": "Endpoints",
        "metadata": {
            "name": "webhook-service",
            "namespace": "default"
        },
        "subsets": [{
            "addresses": [{"ip": "10.42.0.55"}],
            "ports": [{"name":"https","port":9443}]
        }]
    });
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "webhook-service",
        endpoints,
    )
    .await
    .unwrap();

    let client_config = json!({
        "service": {
            "name": "webhook-service",
            "namespace": "default"
        }
    });

    let target = resolve_webhook_target(&db, &client_config).await.unwrap();
    assert_eq!(target.base_url, "https://webhook-service.default.svc:443");
    assert_eq!(
        target.dns_override,
        Some((
            "webhook-service.default.svc".to_string(),
            SocketAddr::from((std::net::Ipv4Addr::new(10, 43, 128, 100), 443)),
        ))
    );
}

#[tokio::test]
async fn test_resolve_webhook_target_service_not_found_returns_error() {
    let db = crate::datastore::test_support::in_memory().await;

    let client_config = json!({
        "service": {
            "name": "nonexistent",
            "namespace": "default"
        }
    });

    let result = resolve_webhook_target(&db, &client_config).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Service not found")
    );
}

#[tokio::test]
async fn test_get_namespace_labels_reads_namespace_table() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_namespace(
        "label-ns",
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": "label-ns",
                "labels": {
                    "webhook-ready": "true",
                    "team": "platform"
                }
            }
        }),
    )
    .await
    .unwrap();

    let labels = get_namespace_labels(&db, "label-ns").await;
    assert_eq!(
        labels.get("webhook-ready").map(String::as_str),
        Some("true")
    );
    assert_eq!(labels.get("team").map(String::as_str), Some("platform"));
}

#[tokio::test]
async fn test_admission_engine_shared_runner_no_webhooks_keeps_resource() {
    let db = crate::datastore::test_support::in_memory().await;
    let engine = AdmissionEngine::new(&db);
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p0", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });

    let mutated = engine
        .run_mutating(&pod, "v1", "Pod", "CREATE")
        .await
        .unwrap();
    assert_eq!(mutated, pod);

    let validated = engine
        .run_validating(&pod, "v1", "Pod", "CREATE")
        .await
        .unwrap();
    assert_eq!(validated, pod);
}

#[tokio::test]
async fn test_admission_engine_accepts_datastore_backend_trait_object() {
    let db = crate::datastore::test_support::in_memory().await;
    let db_backend: &dyn DatastoreBackend = &db;
    let engine = AdmissionEngine::new(db_backend);
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

#[tokio::test]
async fn test_engine_skips_non_write_operations_even_if_webhook_exists() {
    let db = crate::datastore::test_support::in_memory().await;
    let engine = AdmissionEngine::new(&db);

    // Matching webhook exists, but non-write ops must not trigger callout.
    let mwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mwc-skip-read-ops"},
        "webhooks": [{
            "name": "m.example.com",
            "clientConfig": {"url": "https://127.0.0.1:1/mutate"},
            "rules": [{
                "operations": ["*"],
                "apiVersions": ["v1"],
                "resources": ["pods"]
            }]
        }]
    });
    db.create_resource(
        "admissionregistration.k8s.io/v1",
        "MutatingWebhookConfiguration",
        None,
        "mwc-skip-read-ops",
        mwc,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p0", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });

    let got = engine.run_mutating(&pod, "v1", "Pod", "GET").await.unwrap();
    assert_eq!(got, pod);
}

#[tokio::test]
async fn test_namespace_selector_non_match_skips_namespaced_call() {
    let db = crate::datastore::test_support::in_memory().await;
    let engine = AdmissionEngine::new(&db);

    let ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "default", "labels": {"team": "a"}}
    });
    db.create_resource("v1", "Namespace", None, "default", ns)
        .await
        .unwrap();

    let mwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mwc-ns-selector-skip"},
        "webhooks": [{
            "name": "m.example.com",
            "failurePolicy": "Fail",
            "namespaceSelector": {"matchLabels": {"team": "b"}},
            "clientConfig": {"url": "https://127.0.0.1:1/mutate"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["pods"]
            }]
        }]
    });
    db.create_resource(
        "admissionregistration.k8s.io/v1",
        "MutatingWebhookConfiguration",
        None,
        "mwc-ns-selector-skip",
        mwc,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p0", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });

    // Selector mismatch must skip callout (no error despite unreachable webhook URL).
    let got = engine
        .run_mutating(&pod, "v1", "Pod", "CREATE")
        .await
        .unwrap();
    assert_eq!(got, pod);
}

#[tokio::test]
async fn test_namespace_selector_non_match_skips_failing_match_condition() {
    let db = crate::datastore::test_support::in_memory().await;
    let engine = AdmissionEngine::new(&db);

    let ns = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "default", "labels": {"team": "a"}}
    });
    db.create_resource("v1", "Namespace", None, "default", ns)
        .await
        .unwrap();

    let mwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mwc-ns-selector-before-match-condition"},
        "webhooks": [{
            "name": "m.example.com",
            "failurePolicy": "Fail",
            "namespaceSelector": {"matchLabels": {"team": "b"}},
            "matchConditions": [{
                "name": "would-error-if-evaluated",
                "expression": "request.doesNotExist.field == 'x'"
            }],
            "clientConfig": {"url": "https://127.0.0.1:1/mutate"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["pods"]
            }]
        }]
    });
    db.create_resource(
        "admissionregistration.k8s.io/v1",
        "MutatingWebhookConfiguration",
        None,
        "mwc-ns-selector-before-match-condition",
        mwc,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p0", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });

    let got = engine
        .run_mutating(&pod, "v1", "Pod", "CREATE")
        .await
        .expect("selector mismatch must skip matchConditions and callout");
    assert_eq!(got, pod);
}

#[tokio::test]
async fn test_namespace_selector_ignored_for_cluster_scoped_request() {
    let db = crate::datastore::test_support::in_memory().await;
    let engine = AdmissionEngine::new(&db);

    let mwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mwc-cluster-scope"},
        "webhooks": [{
            "name": "m.example.com",
            "failurePolicy": "Fail",
            "namespaceSelector": {"matchLabels": {"team": "b"}},
            "clientConfig": {"url": "https://127.0.0.1:1/mutate"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["namespaces"]
            }]
        }]
    });
    db.create_resource(
        "admissionregistration.k8s.io/v1",
        "MutatingWebhookConfiguration",
        None,
        "mwc-cluster-scope",
        mwc,
    )
    .await
    .unwrap();

    let ns_obj = json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {"name": "ns-a"}
    });

    // Cluster-scoped request: namespaceSelector must be ignored, so webhook call is attempted.
    let err = engine
        .run_mutating(&ns_obj, "v1", "Namespace", "CREATE")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("Webhook call failed"));
}

#[tokio::test]
async fn test_dry_run_rejects_webhook_with_side_effects() {
    let db = crate::datastore::test_support::in_memory().await;
    let engine = AdmissionEngine::new(&db);
    let mwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mwc-dryrun-sideeffects"},
        "webhooks": [{
            "name": "m.example.com",
            "sideEffects": "Some",
            "clientConfig": {"url": "https://127.0.0.1:1/mutate"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["pods"]
            }]
        }]
    });
    db.create_resource(
        "admissionregistration.k8s.io/v1",
        "MutatingWebhookConfiguration",
        None,
        "mwc-dryrun-sideeffects",
        mwc,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "p0", "namespace": "default"},
        "spec": {"containers": [{"name": "c", "image": "busybox"}]}
    });
    let mut ctx = AdmissionRequestContext::from_legacy(&pod, "v1", "Pod", "CREATE");
    ctx.dry_run = Some(true);
    let err = engine
        .run_with_context(&ctx, true)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("sideEffects does not allow dryRun"));
}

#[tokio::test]
async fn test_webhook_call_error_includes_timeout_query_parameter() {
    let db = crate::datastore::test_support::in_memory().await;
    let engine = AdmissionEngine::new(&db);

    let vwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "vwc-timeout-query"},
        "webhooks": [{
            "name": "v.example.com",
            "failurePolicy": "Fail",
            "timeoutSeconds": 1,
            "clientConfig": {"url": "https://127.0.0.1:1/always-allow-delay-5s"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": [""],
                "apiVersions": ["v1"],
                "resources": ["configmaps"]
            }]
        }]
    });
    db.create_resource(
        "admissionregistration.k8s.io/v1",
        "ValidatingWebhookConfiguration",
        None,
        "vwc-timeout-query",
        vwc,
    )
    .await
    .unwrap();

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "cm0", "namespace": "default"}
    });
    let err = engine
        .run_validating(&cm, "v1", "ConfigMap", "CREATE")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("/always-allow-delay-5s?timeout=1s"),
        "webhook errors must include timeout query parameter, got: {}",
        err
    );
}

#[tokio::test]
async fn test_webhook_configuration_objects_are_exempt_from_dynamic_admission() {
    let db = crate::datastore::test_support::in_memory().await;
    let engine = AdmissionEngine::new(&db);

    let mwc = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "MutatingWebhookConfiguration",
        "metadata": {"name": "mwc-block-webhook-config-create"},
        "webhooks": [{
            "name": "m.blocker.example.com",
            "failurePolicy": "Fail",
            "clientConfig": {"url": "https://127.0.0.1:1/block"},
            "rules": [{
                "operations": ["CREATE"],
                "apiGroups": ["admissionregistration.k8s.io"],
                "apiVersions": ["v1"],
                "resources": ["mutatingwebhookconfigurations", "validatingwebhookconfigurations"]
            }]
        }]
    });
    db.create_resource(
        "admissionregistration.k8s.io/v1",
        "MutatingWebhookConfiguration",
        None,
        "mwc-block-webhook-config-create",
        mwc,
    )
    .await
    .unwrap();

    let create_target = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {"name": "target-vwc"},
        "webhooks": []
    });
    let ctx = AdmissionRequestContext::from_legacy(
        &create_target,
        "admissionregistration.k8s.io/v1",
        "ValidatingWebhookConfiguration",
        "CREATE",
    );
    let got = engine.run_with_context(&ctx, true).await.unwrap();
    assert_eq!(
        got, create_target,
        "webhook configuration objects must bypass dynamic mutating admission"
    );
}

// ========================
// add_timeout_query tests
// ========================

use crate::admission::webhook_call::add_timeout_query;

#[test]
fn test_add_timeout_query_appends_timeout_seconds_when_no_existing_query() {
    let url = add_timeout_query("https://hook.example.com/v1/admit", 7).unwrap();
    let parsed = reqwest::Url::parse(&url).unwrap();
    let pairs: Vec<(String, String)> = parsed
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    assert_eq!(pairs, vec![("timeout".to_string(), "7s".to_string())]);
}

#[test]
fn test_add_timeout_query_preserves_existing_query_string() {
    let url = add_timeout_query("https://hook.example.com/admit?foo=bar", 30).unwrap();
    let parsed = reqwest::Url::parse(&url).unwrap();
    let pairs: Vec<(String, String)> = parsed
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    assert!(
        pairs.contains(&("foo".to_string(), "bar".to_string())),
        "existing query pair must be preserved: {pairs:?}"
    );
    assert!(
        pairs.contains(&("timeout".to_string(), "30s".to_string())),
        "timeout pair must be appended: {pairs:?}"
    );
}

#[test]
fn test_add_timeout_query_returns_error_for_unparseable_url() {
    let err = add_timeout_query("not a url", 10).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Invalid webhook URL") || msg.contains("not a url"),
        "error must reference the bad URL; got: {msg}"
    );
}
