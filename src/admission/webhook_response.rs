use crate::admission::request_context::AdmissionRequestContext;
use anyhow::Result;
use serde_json::Value;

pub(super) fn build_admission_review(context: &AdmissionRequestContext, object: &Value) -> Value {
    let mut request = serde_json::json!({
        "uid": uuid::Uuid::new_v4().to_string(),
        "kind": {
            "group": context.api_group,
            "version": context.version,
            "kind": context.kind
        },
        "resource": {
            "group": context.api_group,
            "version": context.version,
            "resource": context.resource
        },
        "requestKind": {
            "group": context.api_group,
            "version": context.version,
            "kind": context.kind
        },
        "requestResource": {
            "group": context.api_group,
            "version": context.version,
            "resource": context.resource
        },
        "name": context.name,
        "namespace": context.namespace,
        "operation": context.operation,
        "object": object,
        "oldObject": context.old_object,
        "dryRun": context.dry_run,
        "options": context.options
    });

    if let Some(subresource) = &context.subresource {
        request["subResource"] = Value::String(subresource.clone());
        request["requestSubResource"] = Value::String(subresource.clone());
    }

    serde_json::json!({
        "apiVersion": "admission.k8s.io/v1",
        "kind": "AdmissionReview",
        "request": request
    })
}

pub(super) fn apply_mutation(resource: Value, response: Value) -> Result<Value> {
    let patch = response
        .get("response")
        .and_then(|r| r.get("patch"))
        .and_then(|p| p.as_str());

    if let Some(patch_b64) = patch {
        let patch_type = response
            .get("response")
            .and_then(|r| r.get("patchType"))
            .and_then(|p| p.as_str())
            .ok_or_else(|| anyhow::anyhow!("Webhook mutation response missing patchType"))?;
        if patch_type != "JSONPatch" {
            anyhow::bail!("Unsupported webhook patchType: {}", patch_type);
        }
        use base64::Engine;
        let patch_json = base64::engine::general_purpose::STANDARD.decode(patch_b64)?;
        let patch: Vec<::json_patch::PatchOperation> = serde_json::from_slice(&patch_json)?;

        let mut doc = resource;
        ::json_patch::patch(&mut doc, &patch)?;
        Ok(doc)
    } else {
        Ok(resource)
    }
}

pub(super) fn is_admission_allowed(response: &Value) -> bool {
    response
        .get("response")
        .and_then(|r| r.get("allowed"))
        .and_then(|a| a.as_bool())
        .unwrap_or(true)
}

pub(super) fn webhook_denial_message(response: &Value) -> String {
    fn clean_text(value: Option<&str>) -> Option<String> {
        let v = value?.trim();
        if v.is_empty() {
            return None;
        }
        Some(v.to_string())
    }

    fn is_generic_webhook_denial(text: &str) -> bool {
        let lower = text.to_ascii_lowercase();
        lower == "webhook denied request"
            || lower == "admission denied by webhook"
            || lower == "admission webhook denied the request"
    }

    let status = response.get("response").and_then(|r| r.get("status"));
    let message = clean_text(
        status
            .and_then(|s| s.get("message"))
            .and_then(|m| m.as_str()),
    );
    let reason = clean_text(
        status
            .and_then(|s| s.get("reason"))
            .and_then(|m| m.as_str()),
    );

    let cause_message = status
        .and_then(|s| s.get("details"))
        .and_then(|d| d.get("causes"))
        .and_then(|c| c.as_array())
        .and_then(|causes| {
            causes
                .iter()
                .find_map(|cause| clean_text(cause.get("message").and_then(|m| m.as_str())))
        });

    if let Some(msg) = message.as_deref()
        && !is_generic_webhook_denial(msg)
    {
        return msg.to_string();
    }

    if let Some(cause_msg) = cause_message {
        return cause_msg;
    }

    if let Some(rsn) = reason.as_deref()
        && !is_generic_webhook_denial(rsn)
    {
        return rsn.to_string();
    }

    message
        .or(reason)
        .unwrap_or_else(|| "webhook denied request".to_string())
}

pub(super) fn webhook_warnings(response: &Value) -> Vec<String> {
    response
        .get("response")
        .and_then(|r| r.get("warnings"))
        .and_then(|w| w.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn ensure_webhook_allowed(response: &Value) -> Result<()> {
    if !is_admission_allowed(response) {
        anyhow::bail!(
            "Admission denied by webhook: {}",
            webhook_denial_message(response)
        );
    }
    Ok(())
}
