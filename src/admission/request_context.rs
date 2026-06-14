use serde_json::Value;

/// Admission request context shared by mutating and validating webhook execution.
#[derive(Clone, Debug)]
pub struct AdmissionRequestContext {
    pub api_version: String,
    pub api_group: String,
    pub version: String,
    pub kind: String,
    pub resource: String,
    pub subresource: Option<String>,
    pub operation: String,
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub dry_run: Option<bool>,
    pub object: Value,
    pub old_object: Option<Value>,
    pub options: Option<Value>,
}

impl AdmissionRequestContext {
    pub fn from_legacy(resource: &Value, api_version: &str, kind: &str, operation: &str) -> Self {
        let (group, version) = parse_api_group_version(api_version);
        let namespace = resource
            .get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(|n| n.as_str())
            .map(ToString::to_string);
        let name = resource
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .map(ToString::to_string);
        Self {
            api_version: api_version.to_string(),
            api_group: group,
            version,
            kind: kind.to_string(),
            resource: kind.to_lowercase() + "s",
            subresource: None,
            operation: operation.to_string(),
            namespace,
            name,
            dry_run: None,
            object: resource.clone(),
            old_object: None,
            options: None,
        }
    }
}

pub(super) fn is_admission_operation(operation: &str) -> bool {
    matches!(operation, "CREATE" | "UPDATE" | "DELETE" | "CONNECT")
}

pub(super) fn is_webhook_configuration_resource(context: &AdmissionRequestContext) -> bool {
    context.api_group == "admissionregistration.k8s.io"
        && matches!(
            context.resource.as_str(),
            "mutatingwebhookconfigurations" | "validatingwebhookconfigurations"
        )
}

pub(super) fn parse_api_group_version(api_version: &str) -> (String, String) {
    if let Some((group, version)) = api_version.split_once('/') {
        (group.to_string(), version.to_string())
    } else {
        ("".to_string(), api_version.to_string())
    }
}
