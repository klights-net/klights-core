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
fn generate(prefix: &str) -> String {
    use rand::distr::{Distribution, Uniform};

    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    let range = Uniform::new(0, CHARSET.len()).expect("valid range");
    let suffix: String = (0..5)
        .map(|_| CHARSET[range.sample(&mut rng)] as char)
        .collect();
    format!("{prefix}{suffix}")
}

#[cfg(any(test, feature = "integration-test-harness"))]
fn deterministic_generated_name(prefix: &str, value: u64) -> String {
    const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    const SUFFIX_SPACE: u64 = 36_u64.pow(5);
    // Five Kubernetes name characters have a finite namespace. Exhaustion is
    // the only point at which this deterministic fake wraps.
    let mut remaining = value % SUFFIX_SPACE;
    let mut suffix = [b'0'; 5];
    for slot in suffix.iter_mut().rev() {
        *slot = ALPHABET[(remaining % 36) as usize];
        remaining /= 36;
    }
    format!(
        "{prefix}{}",
        std::str::from_utf8(&suffix).expect("ASCII suffix")
    )
}

#[cfg(any(test, feature = "integration-test-harness"))]
fn deterministic_uuid_v4(value: u64) -> String {
    let first = ((value & 0x000f_ffff) << 12) | ((value >> 20) & 0x0fff);
    let second = (value >> 32) & 0xffff;
    let third = 0x4000 | ((value >> 48) & 0x0fff);
    let fourth = 0x8000 | ((value >> 60) & 0x000f);
    format!("{first:08x}-{second:04x}-{third:04x}-{fourth:04x}-000000000000")
}

#[cfg(any(test, feature = "integration-test-harness"))]
#[derive(Debug)]
struct DeterministicControllerIdentityGenerator {
    sequence: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[cfg(any(test, feature = "integration-test-harness"))]
impl klights_controllers::ControllerIdentityGenerator for DeterministicControllerIdentityGenerator {
    fn generate_name(&self, prefix: &str) -> String {
        let value = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        deterministic_generated_name(prefix, value)
    }

    fn new_uid(&self) -> String {
        let value = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        deterministic_uuid_v4(value)
    }
}

#[cfg(any(test, feature = "integration-test-harness"))]
#[derive(Clone, Debug, Default)]
pub(crate) struct ControllerIdentityTestGraph {
    sequence: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[cfg(any(test, feature = "integration-test-harness"))]
impl ControllerIdentityTestGraph {
    pub(crate) fn identity(
        &self,
    ) -> std::sync::Arc<dyn klights_controllers::ControllerIdentityGenerator> {
        std::sync::Arc::new(DeterministicControllerIdentityGenerator {
            sequence: self.sequence.clone(),
        })
    }
}

#[cfg(any(test, feature = "integration-test-harness"))]
pub(crate) fn deterministic_controller_identity()
-> std::sync::Arc<dyn klights_controllers::ControllerIdentityGenerator> {
    ControllerIdentityTestGraph::default().identity()
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
