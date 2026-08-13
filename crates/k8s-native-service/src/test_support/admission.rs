//! Canonical admission fixtures for native-service consumer tests.

use std::sync::atomic::{AtomicU64, Ordering};

/// Deterministic request identity used by admission fixture builders.
#[derive(Default)]
pub struct DeterministicApiIdentity {
    next: AtomicU64,
}

/// Returns the stable fixture UUID used by cross-owner integration builders.
pub fn deterministic_uuid_v4(value: u64) -> String {
    let first = ((value & 0x000f_ffff) << 12) | ((value >> 20) & 0x0fff);
    let second = (value >> 32) & 0xffff;
    let third = 0x4000 | ((value >> 48) & 0x0fff);
    let fourth = 0x8000 | ((value >> 60) & 0x000f);
    format!("{first:08x}-{second:04x}-{third:04x}-{fourth:04x}-000000000000")
}

impl crate::ApiIdentityGenerator for DeterministicApiIdentity {
    fn generate_name(&self, prefix: &str) -> String {
        let value = self.next.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}{value:05}")
    }

    fn new_uid(&self) -> String {
        let value = self.next.fetch_add(1, Ordering::Relaxed);
        deterministic_uuid_v4(value)
    }
}

#[cfg(test)]
mod tests {
    use crate::ApiIdentityGenerator as _;

    use super::DeterministicApiIdentity;

    #[test]
    fn deterministic_identity_preserves_admission_request_defaults() {
        let identity = DeterministicApiIdentity::default();

        assert_eq!(identity.generate_name("admission-"), "admission-00000");
        assert_eq!(identity.new_uid(), "00001000-0000-4000-8000-000000000000");
    }
}
