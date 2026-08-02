//! Kubernetes generated-name policy.

/// Root-owned system entropy adapter for API and controller names and UIDs.
#[derive(Debug, Default)]
pub(crate) struct SystemIdentityGenerator;

impl SystemIdentityGenerator {
    fn new_uid_value() -> String {
        uuid::Uuid::new_v4().to_string()
    }
}

impl klights_controllers::ControllerIdentityGenerator for SystemIdentityGenerator {
    fn generate_name(&self, prefix: &str) -> String {
        generate(prefix)
    }

    fn new_uid(&self) -> String {
        Self::new_uid_value()
    }
}

impl k8s_native_service::ApiIdentityGenerator for SystemIdentityGenerator {
    fn generate_name(&self, prefix: &str) -> String {
        generate(prefix)
    }

    fn new_uid(&self) -> String {
        Self::new_uid_value()
    }
}

/// Append the existing five-character lowercase alphanumeric suffix.
pub fn generate(prefix: &str) -> String {
    use rand::distr::{Distribution, Uniform};

    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    let range = Uniform::new(0, CHARSET.len()).expect("valid range");
    let suffix: String = (0..5)
        .map(|_| CHARSET[range.sample(&mut rng)] as char)
        .collect();
    format!("{prefix}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::{SystemIdentityGenerator, generate};
    use klights_controllers::ControllerIdentityGenerator;

    #[test]
    fn suffix_matches_kubernetes_name_contract() {
        let name = generate("my-app.v2-");
        let suffix = &name["my-app.v2-".len()..];
        assert_eq!(suffix.len(), 5);
        assert!(
            suffix
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        );
    }

    #[test]
    fn system_uids_are_non_reused_rfc4122_version_4_values() {
        let identity = SystemIdentityGenerator;
        let first = identity.new_uid();
        let second = identity.new_uid();

        assert_ne!(first, second);
        for raw in [first, second] {
            let uid = uuid::Uuid::parse_str(&raw).expect("system UID must parse as UUID");
            assert_eq!(uid.get_version(), Some(uuid::Version::Random));
            assert_eq!(uid.get_variant(), uuid::Variant::RFC4122);
        }
    }
}
