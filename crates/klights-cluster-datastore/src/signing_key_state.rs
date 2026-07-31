//! Passive file-backed persistence for the embedded ServiceAccount signer.
//!
//! Root composition selects this current embedded adapter and projects it
//! through the focused leader API. Authentication policy never observes the
//! host path or performs persistence.

use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use rand_core::OsRng;
use rsa::RsaPrivateKey;
use rsa::pkcs8::{DecodePrivateKey as _, EncodePrivateKey as _};

/// Loaded and validated file-backed ServiceAccount signing state.
pub struct FileServiceAccountSigningKeyState {
    pem: Arc<str>,
}

impl FileServiceAccountSigningKeyState {
    pub async fn load(
        path: &Path,
        supervisor: &klights_supervisor::TaskSupervisor,
    ) -> Result<Arc<Self>> {
        let pem = read(path, supervisor).await?;
        Ok(Arc::new(Self {
            pem: Arc::from(pem),
        }))
    }

    pub fn try_from_pem(pem: impl Into<String>) -> Result<Arc<Self>> {
        let pem = pem.into();
        validate(Path::new("<injected ServiceAccount signer>"), &pem)?;
        Ok(Arc::new(Self {
            pem: Arc::from(pem),
        }))
    }

    pub fn pem(&self) -> &str {
        &self.pem
    }
}

pub async fn read(path: &Path, supervisor: &klights_supervisor::TaskSupervisor) -> Result<String> {
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

pub async fn read_with_executor(
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

pub async fn persist(
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

pub async fn ensure(
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

#[cfg(test)]
mod tests {
    use super::*;

    fn supervisor() -> klights_supervisor::TaskSupervisor {
        klights_supervisor::TaskSupervisor::new(Default::default())
    }

    fn generated_test_pem() -> String {
        use rsa::pkcs8::EncodePrivateKey as _;

        RsaPrivateKey::new(&mut OsRng, 2048)
            .expect("test ServiceAccount RSA key generation")
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .expect("test ServiceAccount PKCS#8 serialization")
            .to_string()
    }

    #[tokio::test]
    async fn signer_state_generates_dedicated_key_for_seed_leader() {
        let directory = tempfile::tempdir().unwrap();
        let signer_path = directory.path().join("etc/service-account-signing.key");
        let supervisor = supervisor();

        ensure(&signer_path, true, &supervisor).await.unwrap();

        let signer_pem = read(&signer_path, &supervisor).await.unwrap();
        RsaPrivateKey::from_pkcs8_pem(&signer_pem)
            .expect("signer state must be an RSA PKCS#8 private key");
        let mode = std::fs::metadata(&signer_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "signer state must be owner-only");
    }

    #[tokio::test]
    async fn signer_state_persist_load_round_trips_owner_only_key() {
        let directory = tempfile::tempdir().unwrap();
        let signer_path = directory.path().join("nested/service-account-signing.key");
        let supervisor = supervisor();
        let expected = generated_test_pem();

        persist(&signer_path, &expected, &supervisor).await.unwrap();
        let loaded = FileServiceAccountSigningKeyState::load(&signer_path, &supervisor)
            .await
            .unwrap();

        assert_eq!(loaded.pem(), expected);
        let mode = std::fs::metadata(&signer_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "persisted signer state must be owner-only");
    }

    #[tokio::test]
    async fn signer_state_hard_fails_invalid_existing_key() {
        let directory = tempfile::tempdir().unwrap();
        let signer_path = directory.path().join("service-account-signing.key");
        std::fs::write(&signer_path, "not a private key").unwrap();
        let supervisor = supervisor();

        let error = ensure(&signer_path, true, &supervisor)
            .await
            .expect_err("invalid existing signer state must hard fail");
        let message = format!("{error:#}");
        assert!(message.contains(&signer_path.display().to_string()));
        assert!(message.contains("delete") && message.contains("regenerate"));
    }

    #[tokio::test]
    async fn signer_state_requires_downloaded_key_when_generation_disabled() {
        let directory = tempfile::tempdir().unwrap();
        let signer_path = directory.path().join("service-account-signing.key");
        let supervisor = supervisor();

        let error = ensure(&signer_path, false, &supervisor)
            .await
            .expect_err("joining nodes must receive signer state from the leader");
        let message = format!("{error:#}");
        assert!(message.contains(&signer_path.display().to_string()));
        assert!(message.contains("leader"));
    }
}
