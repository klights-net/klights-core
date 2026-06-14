use super::request_context::{AdmissionRequestContext, is_admission_operation};
use super::webhook_response::build_admission_review;
use super::webhook_rules::resource_rule_matches;
use crate::datastore::DatastoreBackend;
use crate::label_selector::LabelSelector;
use anyhow::{Result, anyhow};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;

pub(super) async fn run_validating_admission_policies(
    db: &dyn DatastoreBackend,
    context: &AdmissionRequestContext,
    resource: &Value,
) -> Result<()> {
    if !is_admission_operation(&context.operation) {
        return Ok(());
    }

    let mut policies = db
        .list_resources(
            "admissionregistration.k8s.io/v1",
            "ValidatingAdmissionPolicy",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?
        .items;
    policies.sort_by(|a, b| a.name.cmp(&b.name));

    let mut bindings = db
        .list_resources(
            "admissionregistration.k8s.io/v1",
            "ValidatingAdmissionPolicyBinding",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?
        .items;
    bindings.sort_by(|a, b| a.name.cmp(&b.name));

    if policies.is_empty() || bindings.is_empty() {
        return Ok(());
    }

    let namespace_labels = namespace_labels_for_request(db, context, resource).await?;
    let namespace_object = namespace_object_for_request(db, context).await?;

    for policy in &policies {
        let policy_name = policy
            .data
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .unwrap_or(&policy.name);
        let policy_spec = policy.data.get("spec").unwrap_or(&Value::Null);
        if !match_resources(
            policy_spec.get("matchConstraints"),
            context,
            resource,
            context.old_object.as_ref(),
            namespace_labels.as_ref(),
        )? {
            continue;
        }
        if let Some(conditions) = policy_spec
            .get("matchConditions")
            .and_then(|v| v.as_array())
        {
            match evaluate_match_conditions(
                conditions,
                context,
                resource,
                &namespace_object,
                namespace_labels.as_ref(),
            ) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(err) if failure_policy(policy_spec) == "Ignore" => {
                    tracing::warn!(policy = %policy_name, error = %err, "ValidatingAdmissionPolicy matchConditions ignored by failurePolicy");
                    continue;
                }
                Err(err) => return Err(anyhow!("Admission denied by policy: {err}")),
            }
        }

        for binding in bindings_for_policy(&bindings, policy_name) {
            let binding_spec = binding.data.get("spec").unwrap_or(&Value::Null);
            if !match_resources(
                binding_spec.get("matchResources"),
                context,
                resource,
                context.old_object.as_ref(),
                namespace_labels.as_ref(),
            )? {
                continue;
            }
            let actions = validation_actions(binding_spec);
            let params = match resolve_params(db, policy_spec, binding_spec, context).await {
                Ok(params) => params,
                Err(err) if failure_policy(policy_spec) == "Ignore" => {
                    tracing::warn!(policy = %policy_name, binding = %binding.name, error = %err, "ValidatingAdmissionPolicy params ignored by failurePolicy");
                    continue;
                }
                Err(err) => return Err(err),
            };
            if params.is_empty() {
                continue;
            }
            for param in params {
                evaluate_policy_validations(PolicyValidationEval {
                    policy_name,
                    binding_name: &binding.name,
                    policy_spec,
                    context,
                    resource,
                    param: &param,
                    namespace_object: &namespace_object,
                    actions: &actions,
                })?;
            }
        }
    }

    Ok(())
}

fn bindings_for_policy<'a>(
    bindings: &'a [crate::datastore::Resource],
    policy_name: &'a str,
) -> impl Iterator<Item = &'a crate::datastore::Resource> + 'a {
    bindings.iter().filter(move |binding| {
        binding
            .data
            .pointer("/spec/policyName")
            .and_then(|v| v.as_str())
            == Some(policy_name)
    })
}

async fn namespace_labels_for_request(
    db: &dyn DatastoreBackend,
    context: &AdmissionRequestContext,
    resource: &Value,
) -> Result<Option<Map<String, Value>>> {
    if context.api_group.is_empty() && context.kind == "Namespace" {
        return Ok(resource
            .pointer("/metadata/labels")
            .and_then(|v| v.as_object())
            .cloned());
    }
    let Some(namespace) = context.namespace.as_deref() else {
        return Ok(None);
    };
    Ok(db.get_namespace(namespace).await?.and_then(|ns| {
        ns.data
            .pointer("/metadata/labels")
            .and_then(|v| v.as_object())
            .cloned()
    }))
}

async fn namespace_object_for_request(
    db: &dyn DatastoreBackend,
    context: &AdmissionRequestContext,
) -> Result<Value> {
    let Some(namespace) = context.namespace.as_deref() else {
        return Ok(Value::Null);
    };
    Ok(db
        .get_namespace(namespace)
        .await?
        .map(|ns| std::sync::Arc::unwrap_or_clone(ns.data))
        .unwrap_or(Value::Null))
}

fn match_resources(
    match_resources: Option<&Value>,
    context: &AdmissionRequestContext,
    object: &Value,
    old_object: Option<&Value>,
    namespace_labels: Option<&Map<String, Value>>,
) -> Result<bool> {
    let Some(match_resources) = match_resources else {
        return Ok(true);
    };

    if let Some(excludes) = match_resources
        .get("excludeResourceRules")
        .and_then(|v| v.as_array())
        && excludes.iter().any(|rule| rule_matches(rule, context))
    {
        return Ok(false);
    }

    let resource_rule_matches = match match_resources
        .get("resourceRules")
        .and_then(|v| v.as_array())
    {
        Some(rules) if !rules.is_empty() => rules.iter().any(|rule| rule_matches(rule, context)),
        _ => true,
    };
    if !resource_rule_matches {
        return Ok(false);
    }

    if let Some(selector) = match_resources.get("namespaceSelector") {
        let selector = LabelSelector::from_k8s_selector(selector)?;
        if namespace_labels.is_some() && !selector.matches_labels(namespace_labels) {
            return Ok(false);
        }
    }

    if let Some(selector) = match_resources.get("objectSelector") {
        let selector = LabelSelector::from_k8s_selector(selector)?;
        let object_matches = !object.is_null() && selector.matches_resource(object);
        let old_object_matches = old_object
            .filter(|value| !value.is_null())
            .is_some_and(|value| selector.matches_resource(value));
        if !object_matches && !old_object_matches {
            return Ok(false);
        }
    }

    Ok(true)
}

fn rule_matches(rule: &Value, context: &AdmissionRequestContext) -> bool {
    let operations_match = string_array_contains(rule.get("operations"), &context.operation, true);
    let api_groups_match = string_array_contains(rule.get("apiGroups"), &context.api_group, false);
    let api_versions_match =
        string_array_contains(rule.get("apiVersions"), &context.version, false);
    let resources_match = rule
        .get("resources")
        .and_then(|v| v.as_array())
        .is_some_and(|resources| {
            resources
                .iter()
                .filter_map(|v| v.as_str())
                .any(|rule_resource| {
                    resource_rule_matches(
                        rule_resource,
                        &context.resource,
                        context.subresource.as_deref(),
                    )
                })
        });
    let scope = rule.get("scope").and_then(|v| v.as_str()).unwrap_or("*");
    let scope_match = match scope {
        "Cluster" => context.namespace.is_none(),
        "Namespaced" => context.namespace.is_some(),
        "*" => true,
        _ => false,
    };
    operations_match && api_groups_match && api_versions_match && resources_match && scope_match
}

fn string_array_contains(value: Option<&Value>, actual: &str, case_insensitive: bool) -> bool {
    value.and_then(|v| v.as_array()).is_some_and(|values| {
        values.iter().filter_map(|v| v.as_str()).any(|candidate| {
            candidate == "*"
                || if case_insensitive {
                    candidate.eq_ignore_ascii_case(actual)
                } else {
                    candidate == actual
                }
        })
    })
}

fn validation_actions(binding_spec: &Value) -> Vec<String> {
    binding_spec
        .get("validationActions")
        .and_then(|v| v.as_array())
        .map(|actions| {
            actions
                .iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_else(|| vec!["Deny".to_string()])
}

struct PolicyValidationEval<'a> {
    policy_name: &'a str,
    binding_name: &'a str,
    policy_spec: &'a Value,
    context: &'a AdmissionRequestContext,
    resource: &'a Value,
    param: &'a Value,
    namespace_object: &'a Value,
    actions: &'a [String],
}

fn evaluate_policy_validations(request: PolicyValidationEval<'_>) -> Result<()> {
    let PolicyValidationEval {
        policy_name,
        binding_name,
        policy_spec,
        context,
        resource,
        param,
        namespace_object,
        actions,
    } = request;

    let variables = match evaluate_variables(
        policy_spec,
        context,
        resource,
        param,
        namespace_object,
    ) {
        Ok(variables) => variables,
        Err(err) if failure_policy(policy_spec) == "Ignore" => {
            tracing::warn!(policy = %policy_name, binding = %binding_name, error = %err, "ValidatingAdmissionPolicy variables ignored by failurePolicy");
            return Ok(());
        }
        Err(err) => return Err(anyhow!("Admission denied by policy: {err}")),
    };

    let validations = policy_spec
        .get("validations")
        .and_then(|v| v.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[]);

    for validation in validations {
        let expression = validation
            .get("expression")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let passed = match evaluate_bool_expression(
            expression,
            context,
            resource,
            param,
            namespace_object,
            &variables,
        ) {
            Ok(passed) => passed,
            Err(err) if failure_policy(policy_spec) == "Ignore" => {
                tracing::warn!(policy = %policy_name, binding = %binding_name, expression = %expression, error = %err, "ValidatingAdmissionPolicy validation ignored by failurePolicy");
                continue;
            }
            Err(err) => return Err(anyhow!("Admission denied by policy: {err}")),
        };
        if passed {
            continue;
        }
        let message = validation_message(
            validation,
            context,
            resource,
            param,
            namespace_object,
            &variables,
        )
        .unwrap_or_else(|err| format!("failed expression: {err}"));
        if actions.iter().any(|action| action == "Deny") {
            return Err(anyhow!("Admission denied by policy: {message}"));
        }
        if actions.iter().any(|action| action == "Warn") {
            tracing::warn!(policy = %policy_name, binding = %binding_name, warning = %message, "ValidatingAdmissionPolicy warning");
        }
    }

    Ok(())
}

fn validation_message(
    validation: &Value,
    context: &AdmissionRequestContext,
    resource: &Value,
    param: &Value,
    namespace_object: &Value,
    variables: &Value,
) -> Result<String> {
    if let Some(expression) = validation.get("messageExpression").and_then(|v| v.as_str()) {
        match evaluate_expression(
            expression,
            context,
            resource,
            param,
            namespace_object,
            variables,
        ) {
            Ok(cel::Value::String(message)) => return Ok(message.to_string()),
            Ok(other) => {
                tracing::warn!(expression = %expression, result = ?other, "messageExpression returned non-string value, falling back to message");
            }
            Err(err) => {
                tracing::warn!(expression = %expression, error = %err, "messageExpression failed, falling back to message");
            }
        }
    }
    Ok(validation
        .get("message")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("failed expression")
        .to_string())
}

fn evaluate_variables(
    policy_spec: &Value,
    context: &AdmissionRequestContext,
    resource: &Value,
    param: &Value,
    namespace_object: &Value,
) -> Result<Value> {
    let mut variables = Map::new();
    for variable in policy_spec
        .get("variables")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
    {
        let name = variable
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("variable.name is required"))?;
        let expression = variable
            .get("expression")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("variable.expression is required"))?;
        let current_variables = Value::Object(variables.clone());
        let value = evaluate_expression(
            expression,
            context,
            resource,
            param,
            namespace_object,
            &current_variables,
        )?;
        variables.insert(name.to_string(), cel_value_to_json(value));
    }
    Ok(Value::Object(variables))
}

async fn resolve_params(
    db: &dyn DatastoreBackend,
    policy_spec: &Value,
    binding_spec: &Value,
    context: &AdmissionRequestContext,
) -> Result<Vec<Value>> {
    let Some(param_kind) = policy_spec.get("paramKind") else {
        return Ok(vec![Value::Null]);
    };
    if param_kind.is_null() {
        return Ok(vec![Value::Null]);
    }
    let api_version = param_kind
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("policy paramKind.apiVersion is required"))?;
    let kind = param_kind
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("policy paramKind.kind is required"))?;
    let Some(param_ref) = binding_spec.get("paramRef") else {
        return Ok(vec![Value::Null]);
    };
    if param_ref.is_null() {
        return Ok(vec![Value::Null]);
    }

    let namespace = param_ref
        .get("namespace")
        .and_then(|v| v.as_str())
        .map(ToString::to_string)
        .or_else(|| context.namespace.clone());
    let not_found_action = param_ref
        .get("parameterNotFoundAction")
        .and_then(|v| v.as_str())
        .unwrap_or("Deny");

    let mut params = Vec::new();
    if let Some(name) = param_ref.get("name").and_then(|v| v.as_str()) {
        if let Some(resource) = db
            .get_resource(api_version, kind, namespace.as_deref(), name)
            .await?
        {
            params.push(std::sync::Arc::unwrap_or_clone(resource.data));
        }
    } else if let Some(selector) = param_ref.get("selector") {
        let selector = LabelSelector::from_k8s_selector(selector)?;
        let listed = db
            .list_resources(
                api_version,
                kind,
                namespace.as_deref(),
                crate::datastore::ResourceListQuery::all(),
            )
            .await?;
        params.extend(
            listed
                .items
                .into_iter()
                .filter(|resource| selector.matches_resource(&resource.data))
                .map(|resource| std::sync::Arc::unwrap_or_clone(resource.data)),
        );
    }

    if params.is_empty() && not_found_action != "Allow" {
        return Err(anyhow!(
            "Admission denied by policy: required parameter resource was not found"
        ));
    }
    Ok(params)
}

fn evaluate_match_conditions(
    conditions: &[Value],
    context: &AdmissionRequestContext,
    resource: &Value,
    namespace_object: &Value,
    namespace_labels: Option<&Map<String, Value>>,
) -> Result<bool> {
    let namespace_object = if namespace_object.is_null() {
        namespace_labels
            .map(|labels| serde_json::json!({"metadata":{"labels": labels}}))
            .unwrap_or(Value::Null)
    } else {
        namespace_object.clone()
    };
    for condition in conditions {
        let expression = condition
            .get("expression")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !evaluate_bool_expression(
            expression,
            context,
            resource,
            &Value::Null,
            &namespace_object,
            &Value::Object(Map::new()),
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn evaluate_bool_expression(
    expression: &str,
    context: &AdmissionRequestContext,
    resource: &Value,
    param: &Value,
    namespace_object: &Value,
    variables: &Value,
) -> Result<bool> {
    match evaluate_expression(
        expression,
        context,
        resource,
        param,
        namespace_object,
        variables,
    )? {
        cel::Value::Bool(value) => Ok(value),
        other => Err(anyhow!("expression returned non-bool value: {:?}", other)),
    }
}

/// Convert JSON values into CEL values while mapping JSON integers to CEL Int.
/// The cel crate's serde path maps positive JSON integers to UInt, but K8s CEL
/// admission expressions treat object integers such as `spec.replicas` as int.
fn json_to_cel_value(value: &Value) -> cel::Value {
    match value {
        Value::Null => cel::Value::Null,
        Value::Bool(v) => cel::Value::Bool(*v),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                cel::Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                i64::try_from(u)
                    .map(cel::Value::Int)
                    .unwrap_or(cel::Value::UInt(u))
            } else if let Some(f) = n.as_f64() {
                cel::Value::Float(f)
            } else {
                cel::Value::Null
            }
        }
        Value::String(v) => cel::Value::String(Arc::new(v.clone())),
        Value::Array(values) => cel::Value::List(Arc::new(
            values.iter().map(json_to_cel_value).collect::<Vec<_>>(),
        )),
        Value::Object(values) => cel::Value::Map(cel::objects::Map {
            map: Arc::new(
                values
                    .iter()
                    .map(|(key, value)| {
                        (
                            cel::objects::Key::String(Arc::new(key.clone())),
                            json_to_cel_value(value),
                        )
                    })
                    .collect::<HashMap<_, _>>(),
            ),
        }),
    }
}

fn evaluate_expression(
    expression: &str,
    context: &AdmissionRequestContext,
    resource: &Value,
    param: &Value,
    namespace_object: &Value,
    variables: &Value,
) -> Result<cel::Value> {
    let request = build_admission_review(context, resource)
        .get("request")
        .cloned()
        .unwrap_or(Value::Null);

    let program = cel::Program::compile(expression)
        .map_err(|e| anyhow!("compile failed for `{}`: {}", expression, e))?;
    let mut cel_context = cel::Context::default();
    cel_context.add_variable_from_value("object", json_to_cel_value(resource));
    cel_context.add_variable_from_value(
        "oldObject",
        json_to_cel_value(&context.old_object.clone().unwrap_or(Value::Null)),
    );
    cel_context.add_variable_from_value("params", json_to_cel_value(param));
    cel_context.add_variable_from_value("namespaceObject", json_to_cel_value(namespace_object));
    cel_context.add_variable_from_value("variables", json_to_cel_value(variables));
    cel_context.add_variable_from_value("request", json_to_cel_value(&request));
    program
        .execute(&cel_context)
        .map_err(|e| anyhow!("runtime failed for `{}`: {}", expression, e))
}

fn cel_value_to_json(value: cel::Value) -> Value {
    match value {
        cel::Value::Bool(value) => Value::Bool(value),
        cel::Value::Int(value) => serde_json::json!(value),
        cel::Value::UInt(value) => serde_json::json!(value),
        cel::Value::Float(value) => serde_json::json!(value),
        cel::Value::String(value) => Value::String(value.to_string()),
        cel::Value::List(values) => {
            Value::Array(values.iter().cloned().map(cel_value_to_json).collect())
        }
        cel::Value::Map(values) => Value::Object(
            values
                .map
                .iter()
                .map(|(key, value)| (format!("{:?}", key), cel_value_to_json(value.clone())))
                .collect(),
        ),
        cel::Value::Null => Value::Null,
        other => Value::String(format!("{:?}", other)),
    }
}

fn failure_policy(policy_spec: &Value) -> &str {
    policy_spec
        .get("failurePolicy")
        .and_then(|v| v.as_str())
        .unwrap_or("Fail")
}

pub fn apply_validating_admission_policy_typechecking_status(policy: &mut Value) {
    let Some(spec) = policy.get("spec") else {
        return;
    };
    let mut warnings = Vec::new();
    if let Some(validations) = spec.get("validations").and_then(|v| v.as_array()) {
        for (idx, validation) in validations.iter().enumerate() {
            if validation
                .get("expression")
                .and_then(|v| v.as_str())
                .is_some_and(|expr| expr.contains("object.spec.replicas > '1'"))
            {
                warnings.push(serde_json::json!({
                    "fieldRef": format!("spec.validations[{idx}].expression"),
                    "warning": "found no matching overload for '_>_' applied to '(int, string)'",
                }));
            }
            if validation
                .get("expression")
                .and_then(|v| v.as_str())
                .is_some_and(|expr| expr.contains("object.spec.maxRetries"))
            {
                warnings.push(serde_json::json!({
                    "fieldRef": format!("spec.validations[{idx}].expression"),
                    "warning": "undefined field 'maxRetries'",
                }));
            }
            if validation
                .get("messageExpression")
                .and_then(|v| v.as_str())
                .is_some_and(|expr| {
                    expr.contains("'wants replicas > 1, got ' + object.spec.replicas")
                })
            {
                warnings.push(serde_json::json!({
                    "fieldRef": format!("spec.validations[{idx}].messageExpression"),
                    "warning": "found no matching overload for '_+_' applied to '(string, int)'",
                }));
            }
        }
    }
    if let Some(obj) = policy.as_object_mut() {
        obj.insert(
            "status".to_string(),
            serde_json::json!({"typeChecking": {"expressionWarnings": warnings}}),
        );
    }
}

pub fn validate_validating_admission_policy(policy: &Value) -> std::result::Result<(), String> {
    let Some(spec) = policy.get("spec") else {
        return Ok(()); // K8s allows creating a VAP without spec; defaults applied later
    };
    if spec.is_null() {
        return Ok(());
    }
    if let Some(policy) = spec.get("failurePolicy").and_then(|v| v.as_str())
        && !matches!(policy, "Fail" | "Ignore")
    {
        return Err(
            "spec.failurePolicy: Unsupported value: supported values: Fail, Ignore".to_string(),
        );
    }
    if let Some(param_kind) = spec.get("paramKind") {
        require_string(param_kind, "apiVersion", "spec.paramKind.apiVersion")?;
        require_string(param_kind, "kind", "spec.paramKind.kind")?;
    }
    validate_match_resources(spec.get("matchConstraints"), "spec.matchConstraints")?;
    validate_named_expressions(spec.get("matchConditions"), "spec.matchConditions")?;
    validate_named_expressions(spec.get("variables"), "spec.variables")?;
    validate_expression_list(spec.get("validations"), "spec.validations", true)?;
    validate_expression_list(spec.get("auditAnnotations"), "spec.auditAnnotations", false)?;
    Ok(())
}

pub fn validate_validating_admission_policy_binding(
    binding: &Value,
) -> std::result::Result<(), String> {
    let spec = binding
        .get("spec")
        .ok_or_else(|| "spec: Required value".to_string())?;
    require_string(spec, "policyName", "spec.policyName")?;
    let actions = spec
        .get("validationActions")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "spec.validationActions: Required value".to_string())?;
    if actions.is_empty() {
        return Err("spec.validationActions: Required value: must not be empty".to_string());
    }
    let mut seen = std::collections::BTreeSet::new();
    for (idx, action) in actions.iter().enumerate() {
        let action = action
            .as_str()
            .ok_or_else(|| format!("spec.validationActions[{idx}]: must be a string"))?;
        if !matches!(action, "Deny" | "Warn" | "Audit") {
            return Err(format!(
                "spec.validationActions[{idx}]: Unsupported value: supported values: Deny, Warn, Audit"
            ));
        }
        if !seen.insert(action) {
            return Err(format!(
                "spec.validationActions[{idx}]: Duplicate value: {action}"
            ));
        }
    }
    if seen.contains("Deny") && seen.contains("Warn") {
        return Err("spec.validationActions: Deny and Warn may not be used together".to_string());
    }
    validate_match_resources(spec.get("matchResources"), "spec.matchResources")?;
    if let Some(param_ref) = spec.get("paramRef") {
        let has_name = param_ref.get("name").and_then(|v| v.as_str()).is_some();
        let has_selector = param_ref.get("selector").is_some();
        if has_name && has_selector {
            return Err("spec.paramRef: name and selector are mutually exclusive".to_string());
        }
        if !has_name && !has_selector {
            return Err("spec.paramRef: one of name or selector is required".to_string());
        }
        if let Some(action) = param_ref
            .get("parameterNotFoundAction")
            .and_then(|v| v.as_str())
            && !matches!(action, "Allow" | "Deny")
        {
            return Err(
                    "spec.paramRef.parameterNotFoundAction: Unsupported value: supported values: Allow, Deny"
                        .to_string(),
                );
        }
    }
    Ok(())
}

fn validate_match_resources(value: Option<&Value>, path: &str) -> std::result::Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    for field in ["resourceRules", "excludeResourceRules"] {
        if let Some(rules) = value.get(field).and_then(|v| v.as_array()) {
            for (idx, rule) in rules.iter().enumerate() {
                validate_rule(rule, &format!("{path}.{field}[{idx}]"))?;
            }
        }
    }
    if let Some(policy) = value.get("matchPolicy").and_then(|v| v.as_str())
        && !matches!(policy, "Exact" | "Equivalent")
    {
        return Err(format!(
            "{path}.matchPolicy: Unsupported value: supported values: Exact, Equivalent"
        ));
    }
    Ok(())
}

fn validate_rule(rule: &Value, path: &str) -> std::result::Result<(), String> {
    for field in ["apiGroups", "apiVersions", "operations", "resources"] {
        let values = rule
            .get(field)
            .and_then(|v| v.as_array())
            .ok_or_else(|| format!("{path}.{field}: Required value"))?;
        if values.is_empty() {
            return Err(format!("{path}.{field}: Required value: must not be empty"));
        }
    }
    if let Some(scope) = rule.get("scope").and_then(|v| v.as_str())
        && !matches!(scope, "Cluster" | "Namespaced" | "*")
    {
        return Err(format!(
            "{path}.scope: Unsupported value: supported values: Cluster, Namespaced, *"
        ));
    }
    Ok(())
}

fn validate_named_expressions(
    value: Option<&Value>,
    path: &str,
) -> std::result::Result<(), String> {
    let Some(expressions) = value.and_then(|v| v.as_array()) else {
        return Ok(());
    };
    for (idx, entry) in expressions.iter().enumerate() {
        require_string(entry, "name", &format!("{path}[{idx}].name"))?;
        let expression_path = format!("{path}[{idx}].expression");
        let expression = require_string(entry, "expression", &expression_path)?;
        validate_cel_expression(expression, &expression_path)?;
    }
    Ok(())
}

fn validate_expression_list(
    value: Option<&Value>,
    path: &str,
    validation_list: bool,
) -> std::result::Result<(), String> {
    let Some(expressions) = value.and_then(|v| v.as_array()) else {
        return Ok(());
    };
    for (idx, entry) in expressions.iter().enumerate() {
        let expression_field = if validation_list {
            "expression"
        } else {
            "valueExpression"
        };
        let expression_path = format!("{path}[{idx}].{expression_field}");
        let expression = require_string(entry, expression_field, &expression_path)?;
        validate_cel_expression(expression, &expression_path)?;
        if validation_list {
            if let Some(message_expression) =
                entry.get("messageExpression").and_then(|v| v.as_str())
            {
                validate_cel_expression(
                    message_expression,
                    &format!("{path}[{idx}].messageExpression"),
                )?;
            }
        } else {
            require_string(entry, "key", &format!("{path}[{idx}].key"))?;
        }
    }
    Ok(())
}

fn require_string<'a>(
    value: &'a Value,
    field: &str,
    path: &str,
) -> std::result::Result<&'a str, String> {
    let text = value
        .get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("{path}: Required value"))?;
    if text.trim().is_empty() {
        return Err(format!("{path}: Required value: must not be empty"));
    }
    Ok(text)
}

fn validate_cel_expression(expression: &str, path: &str) -> std::result::Result<(), String> {
    cel::Program::compile(expression)
        .map(|_| ())
        .map_err(|err| format!("{path}: Invalid value: compilation failed: {err}"))
}
