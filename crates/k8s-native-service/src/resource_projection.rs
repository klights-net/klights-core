use serde_json::Value;

pub fn inject_resource_version(
    data: impl Into<std::sync::Arc<Value>>,
    resource_version: i64,
) -> Value {
    let mut data = std::sync::Arc::unwrap_or_clone(data.into());
    if let Some(metadata) = data
        .as_object_mut()
        .and_then(|object| object.get_mut("metadata"))
        .and_then(Value::as_object_mut)
    {
        metadata.insert(
            "resourceVersion".to_string(),
            Value::String(resource_version.to_string()),
        );
        if metadata.get("uid").is_none_or(|uid| {
            uid.is_null() || uid.as_str().is_some_and(|uid| uid.trim().is_empty())
        }) {
            metadata.insert(
                "uid".to_string(),
                Value::String(uuid::Uuid::new_v4().to_string()),
            );
        }
    }
    data
}
