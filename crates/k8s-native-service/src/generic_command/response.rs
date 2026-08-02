//! Kubernetes command response projection.

use std::sync::Arc;

use serde_json::Value;

pub fn persisted_object(data: impl Into<Arc<Value>>, resource_version: i64) -> Value {
    let mut data = Arc::unwrap_or_clone(data.into());
    if let Some(metadata) = data
        .as_object_mut()
        .and_then(|object| object.get_mut("metadata"))
        .and_then(Value::as_object_mut)
    {
        metadata.insert(
            "resourceVersion".to_string(),
            serde_json::json!(resource_version.to_string()),
        );
        let uid_missing_or_empty = metadata.get("uid").is_none_or(|value| {
            value.is_null() || value.as_str().is_some_and(|value| value.trim().is_empty())
        });
        if uid_missing_or_empty {
            metadata.insert(
                "uid".to_string(),
                Value::String(uuid::Uuid::new_v4().to_string()),
            );
        }
    }
    data
}

pub fn accepted_object(data: impl Into<Arc<Value>>, resource_version: i64) -> Value {
    persisted_object(data, resource_version)
}

pub fn delete_success_status(kind: &str, name: &str) -> Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Status",
        "metadata": {},
        "status": "Success",
        "details": {"name": name, "kind": kind},
        "code": 200,
    })
}

pub fn delete_collection_success_status() -> Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Status",
        "metadata": {},
        "status": "Success",
        "code": 200,
    })
}

pub fn accepted_delete_status() -> Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Status",
        "metadata": {},
        "status": "Success",
        "code": 202,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_object_response_preserves_resource_version() {
        let object = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "cm1", "namespace": "default"}
        });
        let value = accepted_object(object, 44);
        assert_eq!(value["metadata"]["resourceVersion"], "44");
    }

    #[test]
    fn delete_statuses_keep_kubernetes_shapes() {
        let deleted = delete_success_status("ConfigMap", "cm1");
        assert_eq!(deleted["apiVersion"], "v1");
        assert_eq!(deleted["kind"], "Status");
        assert_eq!(deleted["details"]["kind"], "ConfigMap");
        assert_eq!(deleted["details"]["name"], "cm1");

        let accepted = accepted_delete_status();
        assert_eq!(accepted["status"], "Success");
        assert_eq!(accepted["code"], 202);
    }
}
