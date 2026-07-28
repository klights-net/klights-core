use crate::{DurableWatchTarget, WatchHistoryError};

pub(crate) fn validate_resource_identity(api_version: &str, kind: &str) -> Result<(), String> {
    validate_target(&DurableWatchTarget::cluster(api_version, kind))
}

pub(crate) fn validate_namespace(namespace: &str) -> Result<(), String> {
    validate_target(&DurableWatchTarget::namespaced_in_namespace(
        "v1",
        "NamespaceContent",
        namespace,
    ))
}

pub(crate) fn validate_optional_namespace(namespace: Option<&str>) -> Result<(), String> {
    if let Some(namespace) = namespace {
        validate_namespace(namespace)?;
    }
    Ok(())
}

pub(crate) fn validate_nonempty(value: &str, field: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.contains('\0') {
        return Err(format!("{field} must be non-empty and contain no NUL byte"));
    }
    Ok(())
}

pub(crate) fn validate_resource_version(resource_version: i64) -> Result<(), String> {
    if resource_version < 0 {
        return Err("resourceVersion must be non-negative".to_string());
    }
    Ok(())
}

fn validate_target(target: &DurableWatchTarget) -> Result<(), String> {
    crate::durable_recovery::validate_watch_target(target).map_err(|error| error.to_string())
}

pub(crate) fn map_invalid_request(message: String) -> crate::ResourceReadError {
    crate::ResourceReadError::InvalidRequest { message }
}

pub(crate) fn map_invalid_watch_request(message: String) -> WatchHistoryError {
    WatchHistoryError::InvalidTarget { message }
}
