//! Schema-neutral Kubernetes resource metadata access.

/// Extract `resourceVersion` from a Kubernetes metadata object.
pub fn resource_version(metadata: &serde_json::Value) -> i64 {
    metadata
        .get("resourceVersion")
        .and_then(|rv| rv.as_str())
        .and_then(|rv| rv.parse::<i64>().ok())
        .unwrap_or(0)
}

/// Extract `resourceVersion` from a complete Kubernetes resource object.
pub fn object_resource_version(object: &serde_json::Value) -> i64 {
    object
        .pointer("/metadata/resourceVersion")
        .and_then(|rv| rv.as_str())
        .and_then(|rv| rv.parse::<i64>().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_and_object_shapes_are_explicit() {
        let object = serde_json::json!({"metadata": {"resourceVersion": "99"}});
        assert_eq!(resource_version(&object), 0);
        assert_eq!(resource_version(&object["metadata"]), 99);
        assert_eq!(object_resource_version(&object), 99);
    }
}
