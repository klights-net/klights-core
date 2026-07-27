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

pub(crate) async fn create_eligibility(
    lookup: &(impl crate::admission::AdmissionLookup + ?Sized),
    namespace: &str,
) -> anyhow::Result<NamespaceCreateEligibility> {
    let Some(resource) = lookup
        .get_resource("v1", "Namespace", None, namespace)
        .await?
    else {
        return Ok(if is_protected(namespace) {
            NamespaceCreateEligibility::Allowed
        } else {
            NamespaceCreateEligibility::Missing
        });
    };
    if resource
        .data
        .pointer("/metadata/deletionTimestamp")
        .and_then(serde_json::Value::as_str)
        .is_some()
    {
        Ok(NamespaceCreateEligibility::Terminating)
    } else {
        Ok(NamespaceCreateEligibility::Allowed)
    }
}
