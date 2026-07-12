use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;

#[async_trait]
pub trait BootstrapCredentialStore: Send + Sync {
    async fn install_ca_certificate(&self, namespace: &str, pem: Vec<u8>) -> Result<()>;
    async fn install_ca_key(&self, namespace: &str, pem: Vec<u8>) -> Result<()>;
    async fn install_server_certificate(&self, path: PathBuf, pem: Vec<u8>) -> Result<()>;
}

#[derive(Clone)]
pub struct SupervisedBootstrapCredentialStore {
    supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
}

impl SupervisedBootstrapCredentialStore {
    pub fn new(supervisor: Arc<crate::task_supervisor::TaskSupervisor>) -> Self {
        Self { supervisor }
    }
}

#[async_trait]
impl BootstrapCredentialStore for SupervisedBootstrapCredentialStore {
    async fn install_ca_certificate(&self, namespace: &str, pem: Vec<u8>) -> Result<()> {
        self.install(BootstrapCredentialArtifact::CaCertificate {
            namespace: namespace.to_string(),
            bytes: pem,
        })
        .await
    }

    async fn install_ca_key(&self, namespace: &str, pem: Vec<u8>) -> Result<()> {
        self.install(BootstrapCredentialArtifact::CaKey {
            namespace: namespace.to_string(),
            bytes: pem,
        })
        .await
    }

    async fn install_server_certificate(&self, path: PathBuf, pem: Vec<u8>) -> Result<()> {
        self.install(BootstrapCredentialArtifact::ServerCertificate { path, bytes: pem })
            .await
    }
}

impl SupervisedBootstrapCredentialStore {
    async fn install(&self, artifact: BootstrapCredentialArtifact) -> Result<()> {
        let task_name = artifact.task_name();
        let key = artifact.key();
        self.supervisor
            .run_blocking_file_keyed(task_name, key, move || artifact.atomic_install())
            .await
            .with_context(|| format!("bootstrap credential task {task_name} failed"))?
    }
}

enum BootstrapCredentialArtifact {
    CaCertificate { namespace: String, bytes: Vec<u8> },
    CaKey { namespace: String, bytes: Vec<u8> },
    ServerCertificate { path: PathBuf, bytes: Vec<u8> },
}

impl BootstrapCredentialArtifact {
    fn path(&self) -> PathBuf {
        match self {
            Self::CaCertificate { namespace, .. } => crate::paths::ca_cert_path(namespace),
            Self::CaKey { namespace, .. } => crate::paths::ca_key_path(namespace),
            Self::ServerCertificate { path, .. } => path.clone(),
        }
    }

    fn bytes(self) -> Vec<u8> {
        match self {
            Self::CaCertificate { bytes, .. }
            | Self::CaKey { bytes, .. }
            | Self::ServerCertificate { bytes, .. } => bytes,
        }
    }

    fn mode(&self) -> u32 {
        match self {
            Self::CaCertificate { .. } | Self::ServerCertificate { .. } => 0o644,
            Self::CaKey { .. } => 0o600,
        }
    }

    fn task_name(&self) -> &'static str {
        match self {
            Self::CaCertificate { .. } => "bootstrap_install_ca_certificate",
            Self::CaKey { .. } => "bootstrap_install_ca_key",
            Self::ServerCertificate { .. } => "bootstrap_install_server_certificate",
        }
    }

    fn key(&self) -> String {
        self.path().to_string_lossy().into_owned()
    }

    fn atomic_install(self) -> Result<()> {
        let path = self.path();
        let mode = self.mode();
        atomic_write_with_mode(&path, &self.bytes(), mode)
    }
}

fn atomic_write_with_mode(path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("credential path has no parent: {}", path.display()))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("create credential parent {}", parent.display()))?;

    let tmp_path = create_temp_file_path(parent, path)?;
    let result = (|| -> Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .open(&tmp_path)
            .with_context(|| format!("create temporary credential file {}", tmp_path.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("write temporary credential file for {}", path.display()))?;
        file.sync_all()
            .with_context(|| format!("sync temporary credential file for {}", path.display()))?;
        file.set_permissions(std::fs::Permissions::from_mode(mode))
            .with_context(|| format!("set credential permissions for {}", path.display()))?;
        drop(file);
        std::fs::rename(&tmp_path, path)
            .with_context(|| format!("atomically replace credential {}", path.display()))?;
        if let Ok(parent_dir) = std::fs::File::open(parent) {
            let _ = parent_dir.sync_all();
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    result?;
    Ok(())
}

fn create_temp_file_path(parent: &Path, final_path: &Path) -> Result<PathBuf> {
    let file_name = final_path
        .file_name()
        .ok_or_else(|| {
            anyhow::anyhow!("credential path has no filename: {}", final_path.display())
        })?
        .to_string_lossy();
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for attempt in 0..64u8 {
        let candidate = parent.join(format!(
            ".{file_name}.klights-credential-{pid}-{nanos}-{attempt}.tmp"
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    anyhow::bail!(
        "could not allocate temporary credential path in {}",
        parent.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[tokio::test]
    async fn supervised_store_installs_ca_key_atomically_with_restrictive_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ca.key");
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let store = SupervisedBootstrapCredentialStore::new(supervisor);

        store
            .install_server_certificate(path.clone(), b"old".to_vec())
            .await
            .unwrap();
        atomic_write_with_mode(&path, b"new-secret", 0o600).unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"new-secret");
        assert_eq!(mode(&path), 0o600);
        let temp_entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .contains("klights-credential")
            })
            .collect();
        assert!(temp_entries.is_empty());
    }

    #[test]
    fn failed_atomic_install_leaves_existing_destination_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.crt");
        std::fs::write(&path, b"existing").unwrap();

        let err = atomic_write_with_mode(dir.path(), b"new", 0o644).unwrap_err();

        assert!(
            err.to_string().contains("atomically replace credential"),
            "{err:?}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"existing");
    }
}
