#[cfg(not(any(test, feature = "test-support")))]
mod request_context;
#[cfg(any(test, feature = "test-support"))]
pub mod request_context;
#[cfg(not(any(test, feature = "test-support")))]
mod selectors;
#[cfg(any(test, feature = "test-support"))]
pub mod selectors;
mod service_target;
mod validating_policy;
mod webhook_call;
mod webhook_http;
#[cfg(not(any(test, feature = "test-support")))]
mod webhook_response;
#[cfg(any(test, feature = "test-support"))]
pub mod webhook_response;
#[cfg(not(any(test, feature = "test-support")))]
mod webhook_rules;
#[cfg(any(test, feature = "test-support"))]
pub mod webhook_rules;

#[cfg(test)]
mod tests;

use anyhow::Result;
pub use request_context::AdmissionRequestContext;
use request_context::{is_admission_operation, is_webhook_configuration_resource};
use selectors::get_namespace_labels_value;
use serde_json::Value;
pub use service_target::ServiceWebhookTargetResolver;
use std::net::SocketAddr;
use std::sync::Arc;
use validating_policy::run_validating_admission_policies;
pub use validating_policy::{
    apply_validating_admission_policy_typechecking_status, validate_validating_admission_policy,
    validate_validating_admission_policy_binding,
};
use webhook_call::call_webhook;
pub use webhook_call::format_webhook_call_error;
pub use webhook_http::ReqwestAdmissionWebhookClient;
use webhook_response::{
    apply_mutation, build_admission_review, ensure_webhook_allowed, webhook_warnings,
};
use webhook_rules::{
    CachedWebhook, should_call_cached_webhook, should_track_reinvocable_webhook, webhook_key,
    webhook_timeout_seconds,
};

#[derive(Clone, Debug)]
pub struct AdmissionResource {
    pub name: String,
    pub data: Arc<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionDependencyError {
    message: String,
}

impl AdmissionDependencyError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for AdmissionDependencyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AdmissionDependencyError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebhookTarget {
    pub base_url: String,
    pub dns_override: Option<(String, SocketAddr)>,
}

#[derive(Clone, Debug)]
pub struct AdmissionWebhookRequest {
    pub target: WebhookTarget,
    pub client_config: Arc<Value>,
    pub admission_review: Value,
    pub timeout_seconds: u64,
}

/// Admission-policy reads. Concrete datastore adaptation belongs to root.
#[async_trait::async_trait]
pub trait AdmissionQuery: Send + Sync {
    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> std::result::Result<Option<AdmissionResource>, AdmissionDependencyError>;

    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
    ) -> std::result::Result<Vec<AdmissionResource>, AdmissionDependencyError>;
}

#[async_trait::async_trait]
pub trait WebhookTargetResolver: Send + Sync {
    async fn resolve(
        &self,
        client_config: &Value,
    ) -> std::result::Result<WebhookTarget, AdmissionDependencyError>;
}

#[async_trait::async_trait]
pub trait AdmissionWebhookClient: Send + Sync {
    async fn call(
        &self,
        request: AdmissionWebhookRequest,
    ) -> std::result::Result<Value, AdmissionDependencyError>;
}

/// Shared OO runner for mutating and validating admission webhooks.
/// Mutating and validating paths share rule matching, selector checks and callout flow.
pub struct AdmissionEngine<'a> {
    identity: &'a dyn crate::ApiIdentityGenerator,
    query: &'a dyn AdmissionQuery,
    target_resolver: &'a dyn WebhookTargetResolver,
    webhook_client: &'a dyn AdmissionWebhookClient,
}

impl<'a> AdmissionEngine<'a> {
    pub fn new(
        identity: &'a dyn crate::ApiIdentityGenerator,
        query: &'a dyn AdmissionQuery,
        target_resolver: &'a dyn WebhookTargetResolver,
        webhook_client: &'a dyn AdmissionWebhookClient,
    ) -> Self {
        Self {
            identity,
            query,
            target_resolver,
            webhook_client,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn run_mutating(
        &self,
        resource: &Value,
        api_version: &str,
        kind: &str,
        operation: &str,
    ) -> Result<Value> {
        self.run(resource, api_version, kind, operation, true).await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn run_validating(
        &self,
        resource: &Value,
        api_version: &str,
        kind: &str,
        operation: &str,
    ) -> Result<Value> {
        self.run(resource, api_version, kind, operation, false)
            .await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn run(
        &self,
        resource: &Value,
        api_version: &str,
        kind: &str,
        operation: &str,
        is_mutating: bool,
    ) -> Result<Value> {
        let ctx = AdmissionRequestContext::from_legacy(resource, api_version, kind, operation);
        self.run_with_context(&ctx, is_mutating).await
    }

    pub async fn run_with_context(
        &self,
        context: &AdmissionRequestContext,
        is_mutating: bool,
    ) -> Result<Value> {
        let mut context = context.clone();
        context.request_uid = self.identity.new_uid();
        let context = &context;
        if !is_admission_operation(&context.operation) {
            return Ok(context.object.clone());
        }
        if is_webhook_configuration_resource(context) {
            return Ok(context.object.clone());
        }

        let webhook_kind = if is_mutating {
            "MutatingWebhookConfiguration"
        } else {
            "ValidatingWebhookConfiguration"
        };

        let resource_namespace = context.namespace.clone();

        let mut configs = self
            .query
            .list_resources("admissionregistration.k8s.io/v1", webhook_kind, None, None)
            .await?;
        configs.sort_by(|a, b| {
            let an = a
                .data
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or("");
            let bn = b
                .data
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or("");
            an.cmp(bn)
        });

        let mut mutated_resource = context.object.clone();
        let mut previously_invoked_reinvocable: Vec<(String, CachedWebhook)> = Vec::new();
        let mut reinvocation_queue: Vec<(String, CachedWebhook)> = Vec::new();
        // Namespaced requests carry a labels map (possibly empty) so the
        // namespaceSelector check applies; cluster-scoped requests carry
        // None so the check is skipped per K8s spec.
        let ns_labels: Option<serde_json::Map<String, Value>> =
            if let Some(ref ns) = resource_namespace {
                Some(
                    get_namespace_labels_value(self.query, ns)
                        .await
                        .unwrap_or_default(),
                )
            } else {
                None
            };

        for config in configs {
            let config_name = config
                .data
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .map(str::to_owned);
            // Pre-parse selectors once per webhook so per-call admission
            // evaluation reuses the cached LabelSelector — admission runs
            // on every CREATE / UPDATE / PATCH and the parse cost is paid
            // back at every request × webhook.
            let cached_webhooks: Vec<CachedWebhook> = config
                .data
                .get("webhooks")
                .and_then(|w| w.as_array())
                .map(|webhooks| {
                    webhooks
                        .iter()
                        .cloned()
                        .map(CachedWebhook::from_value)
                        .collect()
                })
                .unwrap_or_default();

            for cached in cached_webhooks {
                if !should_call_cached_webhook(
                    &cached,
                    context,
                    &mutated_resource,
                    ns_labels.as_ref(),
                )? {
                    continue;
                }

                let webhook = cached.raw();
                let failure_policy = webhook
                    .get("failurePolicy")
                    .and_then(|p| p.as_str())
                    .unwrap_or("Fail");
                let timeout_seconds = webhook_timeout_seconds(webhook);

                match call_webhook(
                    self.target_resolver,
                    self.webhook_client,
                    webhook,
                    &mutated_resource,
                    context,
                    timeout_seconds,
                )
                .await
                {
                    Ok(response) => {
                        if is_mutating {
                            for warning in webhook_warnings(&response) {
                                tracing::warn!("Admission webhook warning: {}", warning);
                            }
                            ensure_webhook_allowed(&response)?;
                            let before = mutated_resource.clone();
                            mutated_resource = apply_mutation(mutated_resource, response)?;
                            if mutated_resource != before {
                                for (key, prior_webhook) in &previously_invoked_reinvocable {
                                    if reinvocation_queue
                                        .iter()
                                        .any(|(queued_key, _)| queued_key == key)
                                    {
                                        continue;
                                    }
                                    reinvocation_queue.push((key.clone(), prior_webhook.clone()));
                                }
                            }
                            let reinvocation_policy =
                                webhook.get("reinvocationPolicy").and_then(|v| v.as_str());
                            if should_track_reinvocable_webhook(false, reinvocation_policy) {
                                let key = webhook_key(config_name.as_deref(), webhook);
                                previously_invoked_reinvocable.push((key, cached.clone()));
                            }
                        } else {
                            for warning in webhook_warnings(&response) {
                                tracing::warn!("Admission webhook warning: {}", warning);
                            }
                            ensure_webhook_allowed(&response)?;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Webhook call failed: {:#}", e);
                        if failure_policy == "Fail" {
                            anyhow::bail!("Webhook call failed and failurePolicy is Fail: {}", e);
                        }
                    }
                }
            }
        }

        if is_mutating {
            for (_, cached) in reinvocation_queue {
                if !should_call_cached_webhook(
                    &cached,
                    context,
                    &mutated_resource,
                    ns_labels.as_ref(),
                )? {
                    continue;
                }
                let webhook = cached.raw();
                let failure_policy = webhook
                    .get("failurePolicy")
                    .and_then(|p| p.as_str())
                    .unwrap_or("Fail");
                let timeout_seconds = webhook_timeout_seconds(webhook);
                match call_webhook(
                    self.target_resolver,
                    self.webhook_client,
                    webhook,
                    &mutated_resource,
                    context,
                    timeout_seconds,
                )
                .await
                {
                    Ok(response) => {
                        ensure_webhook_allowed(&response)?;
                        mutated_resource = apply_mutation(mutated_resource, response)?;
                    }
                    Err(e) => {
                        tracing::warn!("Webhook reinvocation failed: {:#}", e);
                        if failure_policy == "Fail" {
                            anyhow::bail!("Webhook call failed and failurePolicy is Fail: {}", e);
                        }
                    }
                }
            }
        }

        if !is_mutating {
            run_validating_admission_policies(self.query, context, &mutated_resource).await?;
        }

        Ok(mutated_resource)
    }
}
