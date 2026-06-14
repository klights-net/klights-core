use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LeaderTlsVerification {
    CaFile(PathBuf),
    SkipCa,
    SystemRoots,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LeaderTlsVerificationPolicy {
    ca_cert_path: Option<PathBuf>,
    skip_ca: bool,
}

impl LeaderTlsVerificationPolicy {
    pub(crate) fn new(ca_cert_path: Option<PathBuf>, skip_ca: bool) -> Self {
        Self {
            ca_cert_path,
            skip_ca,
        }
    }

    pub(crate) fn verification(&self) -> LeaderTlsVerification {
        if let Some(path) = &self.ca_cert_path {
            LeaderTlsVerification::CaFile(path.clone())
        } else if self.skip_ca {
            LeaderTlsVerification::SkipCa
        } else {
            LeaderTlsVerification::SystemRoots
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LeaderTlsVerification, LeaderTlsVerificationPolicy};
    use std::path::PathBuf;

    #[test]
    fn known_ca_path_takes_precedence_over_skip_ca() {
        let ca = PathBuf::from("/tmp/leader-ca.crt");
        let policy = LeaderTlsVerificationPolicy::new(Some(ca.clone()), true);

        assert_eq!(policy.verification(), LeaderTlsVerification::CaFile(ca));
    }

    #[test]
    fn skip_ca_is_only_used_without_known_ca_path() {
        let policy = LeaderTlsVerificationPolicy::new(None, true);

        assert_eq!(policy.verification(), LeaderTlsVerification::SkipCa);
    }
}
