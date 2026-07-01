use serde_json::Value;
use std::sync::Arc;

pub fn persisted_object(data: impl Into<Arc<Value>>, resource_version: i64) -> Value {
    crate::api::inject_resource_version(data, resource_version)
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
        "details": {
            "name": name,
            "kind": kind,
        },
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
    fn delete_success_status_has_kubernetes_shape() {
        let status = delete_success_status("ConfigMap", "cm1");
        assert_eq!(status["apiVersion"], "v1");
        assert_eq!(status["kind"], "Status");
        assert_eq!(status["status"], "Success");
        assert_eq!(status["details"]["kind"], "ConfigMap");
        assert_eq!(status["details"]["name"], "cm1");
    }

    #[test]
    fn accepted_delete_status_has_kubernetes_shape() {
        let status = accepted_delete_status();
        assert_eq!(status["apiVersion"], "v1");
        assert_eq!(status["kind"], "Status");
        assert_eq!(status["metadata"], serde_json::json!({}));
        assert_eq!(status["status"], "Success");
        assert_eq!(status["code"], 202);
    }

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
}
