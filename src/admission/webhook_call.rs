use crate::admission::request_context::AdmissionRequestContext;
use crate::admission::{AdmissionWebhookClient, AdmissionWebhookRequest, WebhookTargetResolver};
use anyhow::Result;
use serde_json::Value;

pub(super) async fn call_webhook(
    target_resolver: &dyn WebhookTargetResolver,
    webhook_client: &dyn AdmissionWebhookClient,
    webhook: &Value,
    resource: &Value,
    context: &AdmissionRequestContext,
    timeout_seconds: u64,
) -> Result<Value> {
    let client_config = webhook
        .get("clientConfig")
        .ok_or_else(|| anyhow::anyhow!("Webhook missing clientConfig"))?;

    let target = target_resolver.resolve(client_config).await?;
    webhook_client
        .call(AdmissionWebhookRequest {
            target,
            client_config: std::sync::Arc::new(client_config.clone()),
            admission_review: super::build_admission_review(context, resource),
            timeout_seconds,
        })
        .await
        .map_err(Into::into)
}

fn is_timeout_error_text(error_text: &str) -> bool {
    let normalized = error_text.to_ascii_lowercase();
    normalized.contains("deadline exceeded")
        || normalized.contains("timed out")
        || normalized.contains("timeout")
}

pub(crate) fn format_webhook_call_error(url: &str, error_text: &str, is_timeout: bool) -> String {
    if is_timeout || is_timeout_error_text(error_text) {
        return format!(
            "Failed to call webhook at {}: context deadline exceeded: {}",
            url, error_text
        );
    }
    format!("Failed to call webhook at {}: {}", url, error_text)
}
