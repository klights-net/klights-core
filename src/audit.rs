//! Structured Kubernetes API audit events.
//!
//! The hot path records events synchronously into an `AuditSink`. The default
//! sink writes one JSON object per event to the dedicated `klights_audit`
//! tracing target, so production callers get a structured stream without
//! blocking the async runtime on file I/O.

use crate::auth::authorizer::AuthorizationDecision;
use crate::auth::identity::AuthenticatedIdentity;
use crate::auth::request_attributes::AuthorizationRequest;
use serde::Serialize;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum AuditStage {
    Authorization,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditUser {
    pub username: String,
    pub groups: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
}

impl From<&AuthenticatedIdentity> for AuditUser {
    fn from(identity: &AuthenticatedIdentity) -> Self {
        Self {
            username: identity.username.clone(),
            groups: identity.groups.clone(),
            uid: identity.uid.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEvent {
    pub timestamp: String,
    pub stage: AuditStage,
    pub user: AuditUser,
    pub verb: String,
    pub resource_request: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subresource: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub non_resource_url: Option<String>,
    pub high_value: bool,
    pub allowed: bool,
    pub reason: String,
}

impl AuditEvent {
    pub fn authorization(
        identity: &AuthenticatedIdentity,
        request: &AuthorizationRequest,
        decision: &AuthorizationDecision,
    ) -> Self {
        Self {
            timestamp: crate::utils::k8s_microtime_now(),
            stage: AuditStage::Authorization,
            user: AuditUser::from(identity),
            verb: request.verb.clone(),
            resource_request: request.resource_request,
            api_group: request.api_group.clone(),
            api_version: request.api_version.clone(),
            resource: request.resource.clone(),
            subresource: request.subresource.clone(),
            namespace: request.namespace.clone(),
            name: request.name.clone(),
            non_resource_url: request.non_resource_url.clone(),
            high_value: is_high_value_request(request),
            allowed: decision.allowed,
            reason: decision.reason.clone(),
        }
    }
}

fn is_high_value_request(request: &AuthorizationRequest) -> bool {
    match (request.api_group.as_deref(), request.resource.as_deref()) {
        (_, Some("secrets")) => true,
        (_, Some("pods"))
            if matches!(
                request.subresource.as_deref(),
                Some("exec" | "attach" | "portforward")
            ) =>
        {
            true
        }
        (Some("rbac.authorization.k8s.io"), Some(resource))
            if matches!(
                resource,
                "roles" | "rolebindings" | "clusterroles" | "clusterrolebindings"
            ) && matches!(
                request.verb.as_str(),
                "create" | "update" | "patch" | "delete" | "deletecollection"
            ) =>
        {
            true
        }
        _ => false,
    }
}

pub trait AuditSink: Send + Sync {
    fn record(&self, event: AuditEvent);
}

#[derive(Default)]
pub struct TracingAuditSink;

impl AuditSink for TracingAuditSink {
    fn record(&self, event: AuditEvent) {
        match serde_json::to_string(&event) {
            Ok(json) => tracing::info!(target: "klights_audit", audit = %json, "audit_event"),
            Err(err) => tracing::warn!(
                target: "klights_audit",
                error = %err,
                "failed_to_serialize_audit_event"
            ),
        }
    }
}

#[cfg(test)]
#[derive(Default)]
pub struct MemoryAuditSink {
    events: std::sync::Mutex<Vec<AuditEvent>>,
}

#[cfg(test)]
impl MemoryAuditSink {
    pub fn events(&self) -> Vec<AuditEvent> {
        self.events.lock().expect("audit events lock").clone()
    }
}

#[cfg(test)]
impl AuditSink for MemoryAuditSink {
    fn record(&self, event: AuditEvent) {
        self.events.lock().expect("audit events lock").push(event);
    }
}

pub fn default_audit_sink() -> Arc<dyn AuditSink> {
    Arc::new(TracingAuditSink)
}
