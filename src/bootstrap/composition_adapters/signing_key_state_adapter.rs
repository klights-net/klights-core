use std::sync::Arc;

#[cfg(not(test))]
use std::path::Path;

#[cfg(not(test))]
use anyhow::Result;

/// Root-owned composition adapter for the selected embedded signing state.
///
/// Persistence behavior remains in `klights-cluster-datastore`; root only
/// selects the current implementation and projects its neutral leader API.
pub(crate) struct RootServiceAccountSigningKeyState {
    state: Arc<klights_cluster_datastore::signing_key_state::FileServiceAccountSigningKeyState>,
}

impl RootServiceAccountSigningKeyState {
    #[cfg(not(test))]
    pub(crate) async fn load(
        path: &Path,
        supervisor: &klights_supervisor::TaskSupervisor,
    ) -> Result<Arc<Self>> {
        let state =
            klights_cluster_datastore::signing_key_state::FileServiceAccountSigningKeyState::load(
                path, supervisor,
            )
            .await?;
        Ok(Arc::new(Self { state }))
    }

    #[cfg(any(test, feature = "native-api-test-support"))]
    pub(crate) fn from_pem(pem: impl Into<String>) -> Arc<Self> {
        let state = klights_cluster_datastore::signing_key_state::
            FileServiceAccountSigningKeyState::try_from_pem(pem)
            .expect("test ServiceAccount signing key must be valid");
        Arc::new(Self { state })
    }

    #[cfg(any(test, feature = "native-api-test-support"))]
    pub(crate) fn for_test() -> Arc<Self> {
        use rand_core::OsRng;
        use rsa::RsaPrivateKey;
        use rsa::pkcs8::EncodePrivateKey as _;

        static PEM: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        Self::from_pem(
            PEM.get_or_init(|| {
                RsaPrivateKey::new(&mut OsRng, 2048)
                    .expect("test ServiceAccount RSA key generation")
                    .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
                    .expect("test ServiceAccount PKCS#8 serialization")
                    .to_string()
            })
            .clone(),
        )
    }
}

impl klights_leader_api::LeaderServiceAccountSigningKeyState for RootServiceAccountSigningKeyState {
    fn service_account_signing_key_pem(
        &self,
    ) -> klights_leader_api::ClusterIdentityFuture<
        '_,
        klights_leader_api::ServiceAccountSigningKeyPem,
    > {
        Box::pin(async move {
            klights_leader_api::ServiceAccountSigningKeyPem::try_new(self.state.pem().to_string())
        })
    }
}
