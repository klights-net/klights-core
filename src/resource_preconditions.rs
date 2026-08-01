//! Transport-neutral resource precondition comparison.
//!
//! HTTP error adaptation stays in `k8s-native-service`; persistence and feature
//! owners can share the Kubernetes UID/resourceVersion comparison without
//! importing API mutation internals.

use std::error::Error;
use std::fmt;

use klights_cluster_core::{Resource, ResourcePreconditions};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResourcePreconditionError {
    UidMismatch,
    ResourceVersionMismatch { expected: i64, actual: i64 },
}

impl fmt::Display for ResourcePreconditionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UidMismatch => formatter.write_str("UID precondition failed"),
            Self::ResourceVersionMismatch { expected, actual } => write!(
                formatter,
                "resourceVersion precondition failed: expected {expected} got {actual}"
            ),
        }
    }
}

impl Error for ResourcePreconditionError {}

pub(crate) fn ensure_delete_preconditions_match(
    resource: &Resource,
    preconditions: &ResourcePreconditions,
) -> Result<(), ResourcePreconditionError> {
    if let Some(expected_uid) = preconditions.uid.as_deref()
        && resource.uid != expected_uid
    {
        return Err(ResourcePreconditionError::UidMismatch);
    }

    if let Some(expected) = preconditions.resource_version
        && resource.resource_version != expected
    {
        return Err(ResourcePreconditionError::ResourceVersionMismatch {
            expected,
            actual: resource.resource_version,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;

    fn resource(uid: &str, resource_version: i64) -> Resource {
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            uid: uid.to_string(),
            resource_version,
            data: Arc::new(json!({})),
        }
    }

    #[test]
    fn delete_precondition_comparison_preserves_kubernetes_conflicts() {
        let current = resource("uid-web", 7);
        let cases = [
            (
                ResourcePreconditions::uid_and_resource_version("uid-web", 7),
                None,
            ),
            (
                ResourcePreconditions::uid("stale-uid"),
                Some(ResourcePreconditionError::UidMismatch),
            ),
            (
                ResourcePreconditions::resource_version(8),
                Some(ResourcePreconditionError::ResourceVersionMismatch {
                    expected: 8,
                    actual: 7,
                }),
            ),
        ];

        for (preconditions, expected) in cases {
            assert_eq!(
                ensure_delete_preconditions_match(&current, &preconditions).err(),
                expected
            );
        }
    }

    #[test]
    fn delete_precondition_messages_remain_api_compatible() {
        assert_eq!(
            ResourcePreconditionError::UidMismatch.to_string(),
            "UID precondition failed"
        );
        assert_eq!(
            ResourcePreconditionError::ResourceVersionMismatch {
                expected: 8,
                actual: 7,
            }
            .to_string(),
            "resourceVersion precondition failed: expected 8 got 7"
        );
    }
}
