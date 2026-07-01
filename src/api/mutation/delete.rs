use crate::api::AppError;
use crate::datastore::{Resource, ResourcePreconditions};

pub fn ensure_delete_preconditions_match(
    resource: &Resource,
    preconditions: &ResourcePreconditions,
) -> Result<(), AppError> {
    if let Some(expected_uid) = preconditions.uid.as_deref()
        && resource.uid != expected_uid
    {
        return Err(AppError::Conflict("UID precondition failed".to_string()));
    }

    if let Some(expected_rv) = preconditions.resource_version
        && resource.resource_version != expected_rv
    {
        return Err(AppError::Conflict(format!(
            "resourceVersion precondition failed: expected {expected_rv} got {}",
            resource.resource_version
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn resource(uid: &str, resource_version: i64) -> Resource {
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "cm".to_string(),
            uid: uid.to_string(),
            resource_version,
            data: Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "cm",
                    "namespace": "default",
                    "uid": uid,
                    "resourceVersion": resource_version.to_string(),
                }
            })),
        }
    }

    #[test]
    fn delete_preconditions_match_uid_and_resource_version() {
        let resource = resource("uid-1", 7);
        ensure_delete_preconditions_match(
            &resource,
            &ResourcePreconditions::uid_and_resource_version("uid-1", 7),
        )
        .unwrap();
    }

    #[test]
    fn delete_preconditions_reject_wrong_uid() {
        let resource = resource("uid-1", 7);
        assert!(matches!(
            ensure_delete_preconditions_match(
                &resource,
                &ResourcePreconditions::uid_and_resource_version("other", 7),
            ),
            Err(AppError::Conflict(_))
        ));
    }

    #[test]
    fn delete_preconditions_reject_wrong_resource_version() {
        let resource = resource("uid-1", 7);
        assert!(matches!(
            ensure_delete_preconditions_match(
                &resource,
                &ResourcePreconditions::uid_and_resource_version("uid-1", 8),
            ),
            Err(AppError::Conflict(_))
        ));
    }
}
