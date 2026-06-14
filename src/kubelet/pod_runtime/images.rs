use serde_json::Value;

pub fn normalize_image_name(image: &str) -> String {
    let normalized = if !image.contains('/') {
        format!("docker.io/library/{image}")
    } else if !image.contains('.') && image.split('/').count() == 2 {
        format!("docker.io/{image}")
    } else {
        image.to_string()
    };

    if !normalized.contains(':') && !normalized.contains('@') {
        format!("{normalized}:latest")
    } else {
        normalized
    }
}

pub fn effective_pull_policy(container: &Value, normalized_image: &str) -> &'static str {
    let explicit = container
        .get("imagePullPolicy")
        .and_then(|p| p.as_str())
        .unwrap_or("");
    let lower = explicit.to_ascii_lowercase();
    match lower.as_str() {
        "always" => "Always",
        "ifnotpresent" => "IfNotPresent",
        "never" => "Never",
        _ => {
            let tag = normalized_image
                .rsplit_once(':')
                .map(|(_, tag)| tag)
                .unwrap_or("");
            if tag == "latest" || tag.is_empty() {
                "Always"
            } else {
                "IfNotPresent"
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_short_image_to_docker_library_latest() {
        assert_eq!(
            normalize_image_name("nginx"),
            "docker.io/library/nginx:latest"
        );
    }

    #[test]
    fn normalizes_two_part_image_to_docker_namespace_latest() {
        assert_eq!(
            normalize_image_name("library/nginx"),
            "docker.io/library/nginx:latest"
        );
    }

    #[test]
    fn preserves_registry_tag_and_digest_forms() {
        assert_eq!(
            normalize_image_name("registry.k8s.io/pause:3.10"),
            "registry.k8s.io/pause:3.10"
        );
        assert_eq!(
            normalize_image_name("registry.k8s.io/pause@sha256:abcdef"),
            "registry.k8s.io/pause@sha256:abcdef"
        );
    }

    #[test]
    fn explicit_pull_policy_wins_and_is_case_insensitive() {
        assert_eq!(
            effective_pull_policy(&serde_json::json!({"imagePullPolicy": "Always"}), "x:1.0"),
            "Always"
        );
        assert_eq!(
            effective_pull_policy(
                &serde_json::json!({"imagePullPolicy": "IFNOTPRESENT"}),
                "x:latest"
            ),
            "IfNotPresent"
        );
        assert_eq!(
            effective_pull_policy(&serde_json::json!({"imagePullPolicy": "never"}), "x:latest"),
            "Never"
        );
    }

    #[test]
    fn unknown_pull_policy_falls_back_to_defaulting() {
        assert_eq!(
            effective_pull_policy(&serde_json::json!({"imagePullPolicy": "garbage"}), "x:1.0"),
            "IfNotPresent"
        );
        assert_eq!(
            effective_pull_policy(
                &serde_json::json!({"imagePullPolicy": "garbage"}),
                "x:latest"
            ),
            "Always"
        );
    }

    #[test]
    fn default_pull_policy_matches_current_runtime_behavior() {
        assert_eq!(
            effective_pull_policy(&serde_json::json!({}), "docker.io/library/nginx:latest"),
            "Always"
        );
        assert_eq!(
            effective_pull_policy(&serde_json::json!({}), "docker.io/library/nginx:1.25"),
            "IfNotPresent"
        );
    }
}
