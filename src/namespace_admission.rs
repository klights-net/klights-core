//! Framework-neutral Namespace lifecycle admission facts.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NamespaceCreateEligibility {
    Allowed,
    Missing,
    Terminating,
}

pub(crate) fn is_protected(name: &str) -> bool {
    ["default", "kube-system", "kube-public", "kube-node-lease"].contains(&name)
}

pub(crate) fn classify_namespace(
    namespace: &str,
    resource: Option<&serde_json::Value>,
) -> NamespaceCreateEligibility {
    let Some(resource) = resource else {
        return if is_protected(namespace) {
            NamespaceCreateEligibility::Allowed
        } else {
            NamespaceCreateEligibility::Missing
        };
    };
    if resource
        .pointer("/metadata/deletionTimestamp")
        .and_then(serde_json::Value::as_str)
        .is_some()
    {
        NamespaceCreateEligibility::Terminating
    } else {
        NamespaceCreateEligibility::Allowed
    }
}
