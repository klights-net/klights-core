//! Auth-owned resolution of leader TLS verification material.

use anyhow::{Context as _, Result};
use klights_supervisor::TaskSupervisor;
use std::path::PathBuf;

/// Transport-neutral leader verification material.
///
/// `CaPem` contains only the configured public cluster CA certificate. Private
/// CA keys and certificate issuance remain outside this client trust policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedLeaderTlsVerification {
    CaPem(Vec<u8>),
    SkipCa,
    SystemRoots,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderTlsVerificationPolicy {
    ca_cert_path: Option<PathBuf>,
    skip_ca: bool,
}

impl LeaderTlsVerificationPolicy {
    pub fn new(ca_cert_path: Option<PathBuf>, skip_ca: bool) -> Self {
        Self {
            ca_cert_path,
            skip_ca,
        }
    }

    /// Resolve verification mode and load an explicit public CA certificate
    /// through the application-owned supervised filesystem boundary.
    pub async fn resolve(
        &self,
        supervisor: &TaskSupervisor,
    ) -> Result<ResolvedLeaderTlsVerification> {
        if let Some(path) = self.ca_cert_path.clone() {
            let key = path.to_string_lossy().to_string();
            let ca_pem = supervisor
                .run_blocking_file_keyed("leader_tls_policy_read_ca_pem", key, move || {
                    std::fs::read(path)
                })
                .await
                .context("failed to read leader CA certificate")?
                .context("failed to read leader CA certificate")?;
            Ok(ResolvedLeaderTlsVerification::CaPem(ca_pem))
        } else if self.skip_ca {
            Ok(ResolvedLeaderTlsVerification::SkipCa)
        } else {
            Ok(ResolvedLeaderTlsVerification::SystemRoots)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LeaderTlsVerificationPolicy, ResolvedLeaderTlsVerification};
    use std::sync::Arc;

    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

    #[tokio::test]
    async fn known_ca_path_takes_precedence_over_skip_ca() {
        let dir = tempfile::tempdir().unwrap();
        let missing_ca = dir.path().join("missing-leader-ca.crt");
        let policy = LeaderTlsVerificationPolicy::new(Some(missing_ca), true);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));

        let error = policy.resolve(supervisor.as_ref()).await.unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to read leader CA certificate"),
            "CA path must be loaded instead of falling back to skip-ca: {error:#}"
        );
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn skip_ca_is_only_used_without_known_ca_path() {
        let policy = LeaderTlsVerificationPolicy::new(None, true);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));

        assert_eq!(
            policy.resolve(supervisor.as_ref()).await.unwrap(),
            ResolvedLeaderTlsVerification::SkipCa
        );
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn resolve_loads_exact_ca_pem_and_prefers_it_over_skip_ca() {
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("leader-ca.crt");
        let ca_pem = b"-----BEGIN CERTIFICATE-----\nexact-public-ca\n-----END CERTIFICATE-----\n";
        std::fs::write(&ca_path, ca_pem).unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));

        let resolved = LeaderTlsVerificationPolicy::new(Some(ca_path), true)
            .resolve(supervisor.as_ref())
            .await
            .unwrap();

        assert_eq!(
            resolved,
            ResolvedLeaderTlsVerification::CaPem(ca_pem.to_vec())
        );
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn resolve_missing_ca_file_returns_contextual_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing_ca = dir.path().join("missing-leader-ca.crt");
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));

        let error = LeaderTlsVerificationPolicy::new(Some(missing_ca), false)
            .resolve(supervisor.as_ref())
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to read leader CA certificate"),
            "unexpected error: {error:#}"
        );
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }
}
