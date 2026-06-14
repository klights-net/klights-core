//! Bookmark payload constructors used by watch events.

use serde_json::{Value, json};

pub fn build_bookmark(resource_version: i64) -> Value {
    json!({
        "metadata": {
            "resourceVersion": resource_version.to_string()
        }
    })
}

pub fn build_bookmark_typed(resource_version: i64, api_version: &str, kind: &str) -> Value {
    json!({
        "apiVersion": api_version,
        "kind": kind,
        "metadata": {
            "resourceVersion": resource_version.to_string()
        }
    })
}

pub fn build_bookmark_initial(resource_version: i64, api_version: &str, kind: &str) -> Value {
    json!({
        "apiVersion": api_version,
        "kind": kind,
        "metadata": {
            "resourceVersion": resource_version.to_string(),
            "annotations": {
                "k8s.io/initial-events-end": "true"
            }
        }
    })
}
