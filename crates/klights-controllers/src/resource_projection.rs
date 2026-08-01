use std::sync::Arc;

use serde_json::Value;

/// Project a stored resource body into the controller-facing Kubernetes shape.
///
/// This mirrors the API response identity projection without making
/// controllers depend on API-server ownership.
pub fn with_resource_version(
    data: impl Into<Arc<Value>>,
    resource_version: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> Value {
    let mut data = Arc::unwrap_or_clone(data.into());
    if let Some(metadata) = data
        .as_object_mut()
        .and_then(|object| object.get_mut("metadata"))
        .and_then(Value::as_object_mut)
    {
        metadata.insert(
            "resourceVersion".to_string(),
            Value::String(resource_version.to_string()),
        );
        if metadata.get("uid").is_none_or(|value| {
            value.is_null() || value.as_str().is_some_and(|uid| uid.trim().is_empty())
        }) {
            metadata.insert(
                "uid".to_string(),
                Value::String(uuid::Uuid::new_v4().to_string()),
            );
        }
        if metadata.get("creationTimestamp").is_none_or(Value::is_null) {
            metadata.insert(
                "creationTimestamp".to_string(),
                Value::String(klights_cluster_core::k8s_time::format_time(now)),
            );
        }
    }
    data
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn projects_resource_version_without_replacing_stable_identity() {
        let projected = with_resource_version(
            Arc::new(json!({
                "metadata": {
                    "name": "example",
                    "uid": "stable-uid",
                    "creationTimestamp": "2026-07-26T00:00:00Z"
                }
            })),
            42,
            chrono::Utc::now(),
        );

        assert_eq!(
            projected.pointer("/metadata/resourceVersion"),
            Some(&json!("42"))
        );
        assert_eq!(
            projected.pointer("/metadata/uid"),
            Some(&json!("stable-uid"))
        );
        assert_eq!(
            projected.pointer("/metadata/creationTimestamp"),
            Some(&json!("2026-07-26T00:00:00Z"))
        );
    }

    #[test]
    fn fills_missing_identity_like_api_projection() {
        let projected =
            with_resource_version(json!({"metadata": {"uid": "  "}}), 7, chrono::Utc::now());

        assert_eq!(
            projected.pointer("/metadata/resourceVersion"),
            Some(&json!("7"))
        );
        assert!(
            projected
                .pointer("/metadata/uid")
                .and_then(Value::as_str)
                .is_some_and(|uid| !uid.trim().is_empty())
        );
        assert!(
            projected
                .pointer("/metadata/creationTimestamp")
                .and_then(Value::as_str)
                .is_some_and(|timestamp| !timestamp.is_empty())
        );
    }
}
