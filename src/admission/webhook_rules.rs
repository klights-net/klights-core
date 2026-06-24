use crate::admission::request_context::AdmissionRequestContext;
use crate::label_selector::LabelSelector;
use anyhow::Result;
use serde_json::Value;
use std::sync::Arc;

/// Webhook record with its objectSelector and namespaceSelector parsed
/// once at construction so per-call admission evaluation reuses the
/// cached `LabelSelector` instead of rebuilding from raw JSON each time.
///
/// `Arc<Value>` for the raw payload keeps this type cheap to clone for
/// reinvocation queues; the selectors are tiny and so are stored owned.
#[derive(Clone, Debug)]
pub(super) struct CachedWebhook {
    pub raw: Arc<Value>,
    object_selector: SelectorCache,
    namespace_selector: SelectorCache,
}

#[derive(Clone, Debug, Default)]
struct SelectorCache {
    /// `None` when the webhook had no selector at all (admit-without-filter
    /// path). `Some(Ok(_))` when parsed successfully. `Some(Err(()))` when
    /// the cached parse failed; admission treats malformed selectors as
    /// match-none.
    parsed: Option<std::result::Result<LabelSelector, ()>>,
}

impl SelectorCache {
    fn from_optional_selector(selector: Option<&Value>) -> Self {
        Self {
            parsed: selector.map(|s| LabelSelector::from_k8s_selector(s).map_err(|_| ())),
        }
    }

    fn matches(&self, labels: Option<&serde_json::Map<String, Value>>) -> bool {
        match &self.parsed {
            None => true,
            Some(Ok(s)) => s.matches_labels(labels),
            Some(Err(())) => false,
        }
    }
}

impl CachedWebhook {
    pub(super) fn from_value(webhook: Value) -> Self {
        let object_selector = SelectorCache::from_optional_selector(webhook_selector_value(
            &webhook,
            "objectSelector",
            "object_selector",
        ));
        let namespace_selector = SelectorCache::from_optional_selector(webhook_selector_value(
            &webhook,
            "namespaceSelector",
            "namespace_selector",
        ));
        Self {
            raw: Arc::new(webhook),
            object_selector,
            namespace_selector,
        }
    }

    pub(super) fn raw(&self) -> &Value {
        &self.raw
    }
}

fn webhook_selector_value<'a>(
    webhook: &'a Value,
    camel_case: &str,
    snake_case: &str,
) -> Option<&'a Value> {
    webhook.get(camel_case).or_else(|| webhook.get(snake_case))
}

pub(super) fn should_reinvoke_webhook(
    mutation_happened_after: bool,
    reinvocation_policy: Option<&str>,
) -> bool {
    mutation_happened_after && reinvocation_policy == Some("IfNeeded")
}

pub(super) fn webhook_key(configuration_name: Option<&str>, webhook: &Value) -> String {
    format!(
        "{}/{}",
        configuration_name.unwrap_or(""),
        webhook
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("<unnamed>")
    )
}

/// Evaluate a cached webhook against an admission request. The cached
/// selectors avoid re-parsing JSON on every admission call — admission
/// runs on every CREATE / UPDATE / PATCH so the parse savings are paid
/// back at every request, every webhook.
///
/// `ns_labels` is the namespace's labels JSON map (borrowed straight
/// from the namespace resource), built once per request by the caller.
pub(super) fn should_call_cached_webhook(
    webhook: &CachedWebhook,
    context: &AdmissionRequestContext,
    resource: &Value,
    ns_labels: Option<&serde_json::Map<String, Value>>,
) -> Result<bool> {
    let raw = webhook.raw();

    if !matches_webhook_rules(raw, context) {
        return Ok(false);
    }

    // Cluster-scoped requests have no namespace, hence no namespace labels;
    // K8s spec ignores the namespaceSelector for those requests so the
    // webhook still gets a chance to handle them.
    if ns_labels.is_some() && !webhook.namespace_selector.matches(ns_labels) {
        return Ok(false);
    }

    let resource_labels = resource
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|l| l.as_object());
    if !webhook.object_selector.matches(resource_labels) {
        return Ok(false);
    }

    if let Some(conditions) = webhook_match_conditions(raw) {
        let failure_policy = raw
            .get("failurePolicy")
            .and_then(|p| p.as_str())
            .unwrap_or("Fail");
        if !evaluate_match_conditions(conditions, context, resource, failure_policy)? {
            return Ok(false);
        }
    }

    if context.dry_run == Some(true) && !webhook_side_effects_allow_dry_run(raw) {
        anyhow::bail!(
            "Webhook sideEffects does not allow dryRun requests: {}",
            raw.get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("<unnamed>")
        );
    }

    Ok(true)
}

/// Backward-compatible Value-based wrapper used by direct callers and
/// tests that haven't migrated to `CachedWebhook` yet. Constructs the
/// cache on the fly — fine for one-off test calls but the per-request
/// admission loop should call `should_call_cached_webhook` directly.
#[cfg(test)]
pub(super) fn should_call_webhook(
    webhook: &Value,
    context: &AdmissionRequestContext,
    resource: &Value,
    ns_labels: Option<&std::collections::BTreeMap<String, String>>,
) -> Result<bool> {
    let cached = CachedWebhook::from_value(webhook.clone());
    let ns_labels_value: Option<serde_json::Map<String, Value>> = ns_labels.map(|m| {
        m.iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect()
    });
    should_call_cached_webhook(&cached, context, resource, ns_labels_value.as_ref())
}

pub(super) fn webhook_match_conditions(webhook: &Value) -> Option<&Vec<Value>> {
    webhook
        .get("matchConditions")
        .or_else(|| webhook.get("match_conditions"))
        .and_then(|v| v.as_array())
}

/// Check if webhook rules match this request context.
pub(super) fn matches_webhook_rules(webhook: &Value, context: &AdmissionRequestContext) -> bool {
    let rules = match webhook.get("rules").and_then(|r| r.as_array()) {
        Some(r) => r,
        None => return false,
    };

    for rule in rules {
        let operations = rule.get("operations").and_then(|o| o.as_array());
        let api_groups = rule.get("apiGroups").and_then(|g| g.as_array());
        let api_versions = rule.get("apiVersions").and_then(|v| v.as_array());
        let resources = rule.get("resources").and_then(|r| r.as_array());
        let scope = rule.get("scope").and_then(|s| s.as_str()).unwrap_or("*");

        let operation_matches = operations
            .map(|ops| {
                ops.iter().any(|o| {
                    o.as_str().is_some_and(|rule_op| {
                        rule_op == "*" || rule_op.eq_ignore_ascii_case(context.operation.as_str())
                    })
                })
            })
            .unwrap_or(true);

        let api_group_matches = api_groups
            .map(|groups| {
                groups.iter().any(|g| {
                    g.as_str() == Some(context.api_group.as_str()) || g.as_str() == Some("*")
                })
            })
            .unwrap_or(true);

        let api_version_matches = api_versions
            .map(|versions| {
                versions.iter().any(|v| {
                    v.as_str() == Some(context.version.as_str()) || v.as_str() == Some("*")
                })
            })
            .unwrap_or(true);

        let resource_matches = resources
            .map(|res| {
                res.iter().any(|r| {
                    let Some(rule_resource) = r.as_str() else {
                        return false;
                    };
                    resource_rule_matches(
                        rule_resource,
                        &context.resource,
                        context.subresource.as_deref(),
                    )
                })
            })
            .unwrap_or(true);

        let scope_matches = match scope {
            "Cluster" => context.namespace.is_none(),
            "Namespaced" => context.namespace.is_some(),
            "*" => true,
            _ => true,
        };

        if operation_matches
            && api_group_matches
            && api_version_matches
            && resource_matches
            && scope_matches
        {
            return true;
        }
    }

    false
}

pub(super) fn resource_rule_matches(
    rule_resource: &str,
    resource: &str,
    subresource: Option<&str>,
) -> bool {
    if rule_resource == "*" {
        return true;
    }
    if let Some((rule_res, rule_sub)) = rule_resource.split_once('/') {
        let resource_matches = rule_res == "*" || rule_res == resource;
        if !resource_matches {
            return false;
        }
        if let Some(actual_sub) = subresource {
            return rule_sub == "*" || rule_sub == actual_sub;
        }
        return false;
    }
    rule_resource == resource
}

pub(super) fn webhook_timeout_seconds(webhook: &Value) -> u64 {
    let raw = webhook
        .get("timeoutSeconds")
        .and_then(|t| t.as_u64())
        .unwrap_or(10);
    if raw == 0 {
        return 10;
    }
    raw.clamp(1, 30)
}

pub(super) fn webhook_side_effects_allow_dry_run(webhook: &Value) -> bool {
    matches!(
        webhook.get("sideEffects").and_then(|s| s.as_str()),
        Some("None") | Some("NoneOnDryRun")
    )
}

pub(super) fn evaluate_match_conditions(
    conditions: &[Value],
    context: &AdmissionRequestContext,
    resource: &Value,
    failure_policy: &str,
) -> Result<bool> {
    let mut errors = Vec::new();
    for condition in conditions {
        let expr = condition
            .get("expression")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim();
        match evaluate_match_condition_expression(expr, context, resource) {
            Ok(true) => {}
            Ok(false) => return Ok(false),
            Err(err) => errors.push(err),
        }
    }

    if errors.is_empty() {
        return Ok(true);
    }
    if failure_policy == "Ignore" {
        return Ok(false);
    }
    anyhow::bail!(
        "matchCondition evaluation failed: {}",
        errors
            .into_iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    );
}

pub(super) fn evaluate_match_condition_expression(
    expression: &str,
    context: &AdmissionRequestContext,
    resource: &Value,
) -> Result<bool> {
    let program =
        cel::Program::compile(expression).map_err(|e| anyhow::anyhow!("compile failed: {}", e))?;
    let mut cel_context = cel::Context::default();
    cel_context
        .add_variable("object", resource.clone())
        .map_err(|e| anyhow::anyhow!("object binding failed: {}", e))?;
    cel_context
        .add_variable(
            "oldObject",
            context
                .old_object
                .clone()
                .unwrap_or(serde_json::Value::Null),
        )
        .map_err(|e| anyhow::anyhow!("oldObject binding failed: {}", e))?;
    cel_context
        .add_variable(
            "request",
            super::build_admission_review(context, resource)
                .get("request")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        )
        .map_err(|e| anyhow::anyhow!("request binding failed: {}", e))?;

    match program.execute(&cel_context) {
        Ok(cel::Value::Bool(matches)) => Ok(matches),
        Ok(other) => anyhow::bail!("expression returned non-bool value: {:?}", other),
        Err(err) => anyhow::bail!("runtime failed: {}", err),
    }
}

pub(super) fn should_track_reinvocable_webhook(
    mutation_happened_after: bool,
    reinvocation_policy: Option<&str>,
) -> bool {
    should_reinvoke_webhook(mutation_happened_after, reinvocation_policy)
        || reinvocation_policy == Some("IfNeeded")
}
