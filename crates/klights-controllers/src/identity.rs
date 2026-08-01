//! Controller-owned identity generation capability.

/// Supplies the two ambient identity operations used by controller policy.
///
/// The production implementation belongs to the composition root. Controller
/// tests provide small deterministic fakes instead of consulting process-wide
/// entropy.
pub trait ControllerIdentityGenerator: Send + Sync {
    /// Append the Kubernetes-compatible generated-name suffix to `prefix`.
    fn generate_name(&self, prefix: &str) -> String;

    /// Return a new Kubernetes object UID.
    fn new_uid(&self) -> String;
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct DeterministicControllerIdentityGenerator {
    sequence: std::sync::atomic::AtomicU64,
}

#[cfg(test)]
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

#[cfg(test)]
fn deterministic_uuid_v4(value: u64) -> String {
    let first = ((value & 0x000f_ffff) << 12) | ((value >> 20) & 0x0fff);
    let second = (value >> 32) & 0xffff;
    let third = 0x4000 | ((value >> 48) & 0x0fff);
    let fourth = 0x8000 | ((value >> 60) & 0x000f);
    format!("{first:08x}-{second:04x}-{third:04x}-{fourth:04x}-000000000000")
}

#[cfg(test)]
impl DeterministicControllerIdentityGenerator {
    fn with_start(value: u64) -> Self {
        Self {
            sequence: std::sync::atomic::AtomicU64::new(value),
        }
    }
}

#[cfg(test)]
impl ControllerIdentityGenerator for DeterministicControllerIdentityGenerator {
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

#[cfg(test)]
mod tests {
    use super::{ControllerIdentityGenerator, DeterministicControllerIdentityGenerator};

    #[test]
    fn capability_is_object_safe() {
        fn assert_object_safe(_: Option<&dyn ControllerIdentityGenerator>) {}
        assert_object_safe(None);
    }

    #[test]
    fn deterministic_generator_advances_names_and_uids() {
        let identity = DeterministicControllerIdentityGenerator::default();

        assert_eq!(identity.generate_name("pod-"), "pod-00000");
        assert_eq!(identity.generate_name("pod-"), "pod-00001");
        let first_uid = identity.new_uid();
        let second_uid = identity.new_uid();
        assert_eq!(first_uid, "00002000-0000-4000-8000-000000000000");
        assert_eq!(second_uid, "00003000-0000-4000-8000-000000000000");
        assert_ne!(&first_uid[..5], &second_uid[..5]);
        assert_ne!(first_uid.split('-').next(), second_uid.split('-').next(),);
    }

    #[test]
    fn deterministic_generators_are_independent_and_parallel_hermetic() {
        let first = DeterministicControllerIdentityGenerator::default();
        let second = DeterministicControllerIdentityGenerator::default();
        assert_eq!(first.generate_name("pod-"), "pod-00000");
        assert_eq!(first.generate_name("pod-"), "pod-00001");
        assert_eq!(second.generate_name("pod-"), "pod-00000");

        let outputs = (0..4)
            .map(|_| {
                std::thread::spawn(|| {
                    let identity = DeterministicControllerIdentityGenerator::default();
                    (0..64)
                        .map(|_| identity.generate_name("pod-"))
                        .collect::<Vec<_>>()
                })
            })
            .map(|thread| thread.join().expect("identity fake thread"))
            .collect::<Vec<_>>();
        assert!(outputs.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[test]
    fn deterministic_generator_has_valid_large_counter_uuid_and_name_shapes() {
        let identity = DeterministicControllerIdentityGenerator::with_start(u64::MAX - 1);
        let first = identity.new_uid();
        let second = identity.new_uid();
        assert_ne!(first, second);
        for uid in [first, second] {
            assert_eq!(uid.len(), 36);
            assert_eq!(&uid[8..9], "-");
            assert_eq!(&uid[13..14], "-");
            assert_eq!(&uid[18..19], "-");
            assert_eq!(&uid[23..24], "-");
            assert_eq!(&uid[14..15], "4");
            assert!(matches!(&uid[19..20], "8" | "9" | "a" | "b"));
            assert!(uid.chars().enumerate().all(|(index, character)| {
                matches!(index, 8 | 13 | 18 | 23) || character.is_ascii_hexdigit()
            }));
        }

        let identity = DeterministicControllerIdentityGenerator::with_start(u64::MAX - 1);
        let names = [
            identity.generate_name("pod-"),
            identity.generate_name("pod-"),
        ];
        assert_ne!(names[0], names[1]);
        for name in names {
            let suffix = name.strip_prefix("pod-").expect("generated prefix");
            assert_eq!(suffix.len(), 5);
            assert!(
                suffix.chars().all(|character| {
                    character.is_ascii_lowercase() || character.is_ascii_digit()
                })
            );
        }
    }
}
