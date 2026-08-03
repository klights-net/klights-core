//! Framework-neutral Namespace lifecycle admission facts.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamespaceCreateEligibility {
    Allowed,
    Missing,
    Terminating,
}

fn is_protected(name: &str) -> bool {
    ["default", "kube-system", "kube-public", "kube-node-lease"].contains(&name)
}

pub fn classify_namespace(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_in_missing_namespace_is_forbidden() {
        assert_eq!(
            classify_namespace("ghost", None),
            NamespaceCreateEligibility::Missing
        );
    }

    #[test]
    fn create_in_existing_active_namespace_is_allowed() {
        let namespace = serde_json::json!({"metadata": {"name": "team-a"}});
        assert_eq!(
            classify_namespace("team-a", Some(&namespace)),
            NamespaceCreateEligibility::Allowed
        );
    }

    #[test]
    fn create_in_immortal_system_namespace_is_allowed_even_without_row() {
        for namespace in ["default", "kube-system", "kube-public", "kube-node-lease"] {
            assert_eq!(
                classify_namespace(namespace, None),
                NamespaceCreateEligibility::Allowed,
                "protected namespace {namespace} must remain eligible"
            );
        }
    }

    #[test]
    fn create_in_terminating_namespace_is_forbidden() {
        let namespace = serde_json::json!({
            "metadata": {
                "name": "team-b",
                "deletionTimestamp": "2026-06-13T00:00:00Z"
            }
        });
        assert_eq!(
            classify_namespace("team-b", Some(&namespace)),
            NamespaceCreateEligibility::Terminating
        );
    }
}
