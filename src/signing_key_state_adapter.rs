use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use rand_core::OsRng;
use rsa::RsaPrivateKey;
use rsa::pkcs8::{DecodePrivateKey as _, EncodePrivateKey as _};

/// Root-owned projection of durable ServiceAccount signing state.
///
/// Auth and API consumers receive only the policy-facing provider trait and
/// never discover a host path or perform filesystem I/O.
pub(crate) struct RootServiceAccountSigningKeyState {
    pem: Arc<str>,
}

impl RootServiceAccountSigningKeyState {
    #[cfg(not(test))]
    pub(crate) async fn load(
        path: &Path,
        supervisor: &klights_supervisor::TaskSupervisor,
    ) -> Result<Arc<Self>> {
        let pem = read(path, supervisor).await?;
        validate(path, &pem)?;
        Ok(Arc::new(Self {
            pem: Arc::from(pem),
        }))
    }

    #[cfg(test)]
    pub(crate) fn from_pem(pem: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            pem: Arc::from(pem.into()),
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Arc<Self> {
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

#[async_trait::async_trait]
impl klights_auth::cluster_identity::ServiceAccountSigningKeyProvider
    for RootServiceAccountSigningKeyState
{
    async fn service_account_signing_key_pem(
        &self,
    ) -> Result<String, klights_auth::AuthenticationError> {
        Ok(self.pem.to_string())
    }
}

pub(crate) async fn read(
    path: &Path,
    supervisor: &klights_supervisor::TaskSupervisor,
) -> Result<String> {
    let owned_path = path.to_path_buf();
    let key = path.to_string_lossy().into_owned();
    let display = path.display().to_string();
    let pem = supervisor
        .run_blocking_file_keyed("sa_signer_state_read", key, move || {
            std::fs::read_to_string(owned_path)
        })
        .await?
        .with_context(|| format!("Failed to read ServiceAccount signing key {display}"))?;
    validate(path, &pem)?;
    Ok(pem)
}

pub(crate) async fn read_with_executor(
    path: &Path,
    file_process: &klights_supervisor::FileProcessExecutor,
) -> io::Result<String> {
    let owned_path = path.to_path_buf();
    let key = path.to_string_lossy().into_owned();
    let display = path.display().to_string();
    let pem = file_process
        .run_blocking_file_keyed("sa_signer_state_read_executor", key, move || {
            std::fs::read_to_string(owned_path).map_err(anyhow::Error::from)
        })
        .await
        .map_err(|error| {
            io::Error::other(format!(
                "Failed to read ServiceAccount signing key {display}: {error}"
            ))
        })?;
    validate(path, &pem).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(pem)
}

pub(crate) async fn persist(
    path: &Path,
    pem: &str,
    supervisor: &klights_supervisor::TaskSupervisor,
) -> Result<()> {
    validate(path, pem)?;
    let parent = path.parent().ok_or_else(|| {
        anyhow::anyhow!(
            "ServiceAccount signing key path has no parent: {}",
            path.display()
        )
    })?;
    let parent_owned = parent.to_path_buf();
    supervisor
        .run_blocking_file_keyed(
            "sa_signer_state_create_parent",
            parent.to_string_lossy().into_owned(),
            move || std::fs::create_dir_all(parent_owned),
        )
        .await??;
    let path_owned = path.to_path_buf();
    let contents = pem.to_string();
    supervisor
        .run_blocking_file_keyed(
            "sa_signer_state_write",
            path.to_string_lossy().into_owned(),
            move || {
                std::fs::write(&path_owned, contents)?;
                std::fs::set_permissions(&path_owned, std::fs::Permissions::from_mode(0o600))
            },
        )
        .await??;
    Ok(())
}

pub(crate) async fn ensure(
    path: &Path,
    allow_local_generation: bool,
    supervisor: &klights_supervisor::TaskSupervisor,
) -> Result<()> {
    let path_owned = path.to_path_buf();
    let exists = supervisor
        .run_blocking_file_keyed(
            "sa_signer_state_exists",
            path.to_string_lossy().into_owned(),
            move || path_owned.try_exists(),
        )
        .await??;
    if exists {
        let pem = read(path, supervisor).await?;
        validate(path, &pem)?;
        let path_owned = path.to_path_buf();
        supervisor
            .run_blocking_file_keyed(
                "sa_signer_state_chmod",
                path.to_string_lossy().into_owned(),
                move || {
                    std::fs::set_permissions(path_owned, std::fs::Permissions::from_mode(0o600))
                },
            )
            .await??;
        return Ok(());
    }
    if !allow_local_generation {
        anyhow::bail!(
            "ServiceAccount signing key {} is missing; joining controlplanes and replicas must receive it from the leader during CSR bootstrap",
            path.display()
        );
    }
    let pem = supervisor
        .run_blocking(
            klights_supervisor::TaskCategory::Others,
            "sa_signer_state_generate",
            || {
                let private_key = RsaPrivateKey::new(&mut OsRng, 2048).map_err(|error| {
                    anyhow::anyhow!("ServiceAccount RSA key generation failed: {error}")
                })?;
                Ok::<String, anyhow::Error>(
                    private_key
                        .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
                        .map_err(|error| {
                            anyhow::anyhow!("ServiceAccount PKCS#8 serialization failed: {error}")
                        })?
                        .to_string(),
                )
            },
        )
        .await??;
    persist(path, &pem, supervisor).await
}

fn validate(path: &Path, pem: &str) -> Result<()> {
    use rsa::pkcs1::DecodeRsaPrivateKey as _;

    if pem.trim().is_empty() {
        anyhow::bail!("ServiceAccount signing key {} is empty", path.display());
    }
    RsaPrivateKey::from_pkcs8_pem(pem)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
        .map(|_| ())
        .map_err(|error| {
            anyhow::anyhow!(
                "ServiceAccount signing key {} is invalid: {error}. delete this file to allow the seed leader to regenerate it",
                path.display()
            )
        })
}
