use serde_json::Value;

pub fn prepare_create_metadata(ns: Option<&str>, body: &mut Value, resource_name: &str) {
    crate::api::inject_create_metadata(ns, body, resource_name);
}

pub fn prepare_builtin_generation_for_update(kind: &str, current: &Value, body: &mut Value) {
    crate::api::increment_generation_if_spec_changed(kind, current, body);
}

pub fn prepare_custom_generation_for_update(current: &Value, body: &mut Value) {
    crate::api::increment_generation_for_spec_change(current, body);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_create_metadata_stamps_identity_and_generation() {
        let mut body = serde_json::json!({"metadata": {}});
        prepare_create_metadata(Some("default"), &mut body, "cm1");

        assert_eq!(body["metadata"]["namespace"], "default");
        assert_eq!(body["metadata"]["name"], "cm1");
        assert_eq!(body["metadata"]["generation"], 1);
        assert!(
            body["metadata"]["uid"]
                .as_str()
                .is_some_and(|uid| !uid.is_empty())
        );
    }

    #[test]
    fn prepare_builtin_generation_for_update_uses_kind_policy() {
        let current = serde_json::json!({
            "metadata": {"generation": 3},
            "spec": {"replicas": 1}
        });
        let mut body = serde_json::json!({
            "metadata": {"generation": 3},
            "spec": {"replicas": 2}
        });

        prepare_builtin_generation_for_update("Deployment", &current, &mut body);

        assert_eq!(body["metadata"]["generation"], 4);
    }

    #[test]
    fn prepare_custom_generation_for_update_bumps_on_spec_change() {
        let current = serde_json::json!({
            "metadata": {"generation": 8},
            "spec": {"value": "old"}
        });
        let mut body = serde_json::json!({
            "metadata": {"generation": 8},
            "spec": {"value": "new"}
        });

        prepare_custom_generation_for_update(&current, &mut body);

        assert_eq!(body["metadata"]["generation"], 9);
    }
}
