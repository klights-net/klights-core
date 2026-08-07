//! Private supervised filesystem adapter for auth-owned worker credentials.

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use klights_auth::{
    CredentialOperationError,
    worker_credential::{WorkerCredential, WorkerCredentialStore},
};
use klights_supervisor::TaskSupervisor;

pub(crate) struct SupervisedFilesystemWorkerCredentialStore {
    dir: PathBuf,
    node_name: String,
    supervisor: Arc<TaskSupervisor>,
}

impl SupervisedFilesystemWorkerCredentialStore {
    pub(crate) fn new(dir: PathBuf, node_name: &str, supervisor: Arc<TaskSupervisor>) -> Self {
        Self {
            dir,
            node_name: node_name.to_string(),
            supervisor,
        }
    }

    pub(crate) fn for_namespace(
        namespace: &str,
        node_name: &str,
        supervisor: Arc<TaskSupervisor>,
    ) -> Self {
        Self::new(crate::paths::etc_dir_path(namespace), node_name, supervisor)
    }

    fn key(&self) -> String {
        self.dir.to_string_lossy().to_string()
    }
}

#[async_trait]
impl WorkerCredentialStore for SupervisedFilesystemWorkerCredentialStore {
    async fn load(&self) -> Result<Option<WorkerCredential>, CredentialOperationError> {
        let dir = self.dir.clone();
        let node_name = self.node_name.clone();
        self.supervisor
            .run_blocking_file_keyed("worker_credential_load", self.key(), move || {
                load_credential(&dir, &node_name)
            })
            .await
            .map_err(|error| {
                CredentialOperationError::dependency_failure(format!(
                    "worker credential load task failed: {error}"
                ))
            })?
    }

    async fn save(&self, credential: &WorkerCredential) -> Result<(), CredentialOperationError> {
        let dir = self.dir.clone();
        let credential = credential.clone();
        self.supervisor
            .run_blocking_file_keyed("worker_credential_save", self.key(), move || {
                save_credential(&dir, &credential)
            })
            .await
            .map_err(|error| {
                CredentialOperationError::dependency_failure(format!(
                    "worker credential save task failed: {error}"
                ))
            })?
    }

    async fn delete(&self) -> Result<(), CredentialOperationError> {
        let dir = self.dir.clone();
        self.supervisor
            .run_blocking_file_keyed("worker_credential_delete", self.key(), move || {
                delete_credential(&dir)
            })
            .await
            .map_err(|error| {
                CredentialOperationError::dependency_failure(format!(
                    "worker credential delete task failed: {error}"
                ))
            })?
    }
}

fn load_credential(
    dir: &std::path::Path,
    node_name: &str,
) -> Result<Option<WorkerCredential>, CredentialOperationError> {
    let cert_path = dir.join("node.crt");
    let key_path = dir.join("node.key");
    if !cert_path.exists() || !key_path.exists() {
        return Ok(None);
    }
    let certificate_pem = std::fs::read_to_string(&cert_path).map_err(|error| {
        CredentialOperationError::dependency_failure(format!(
            "failed to read {}: {error}",
            cert_path.display()
        ))
    })?;
    let private_key_pem = std::fs::read_to_string(&key_path).map_err(|error| {
        CredentialOperationError::dependency_failure(format!(
            "failed to read {}: {error}",
            key_path.display()
        ))
    })?;
    let kubeconfig_path = dir.join("node_kubeconfig.yaml");
    let kubeconfig_yaml = if kubeconfig_path.exists() {
        std::fs::read_to_string(&kubeconfig_path).unwrap_or_default()
    } else {
        String::new()
    };
    WorkerCredential::try_new(
        certificate_pem,
        private_key_pem,
        node_name.to_string(),
        kubeconfig_yaml,
    )
    .map(Some)
}

fn save_credential(
    dir: &std::path::Path,
    credential: &WorkerCredential,
) -> Result<(), CredentialOperationError> {
    std::fs::create_dir_all(dir).map_err(|error| {
        CredentialOperationError::dependency_failure(format!(
            "failed to create {}: {error}",
            dir.display()
        ))
    })?;
    let cert_path = dir.join("node.crt");
    let key_path = dir.join("node.key");
    atomic_write(&cert_path, credential.certificate_pem().as_bytes())?;
    atomic_write(&key_path, credential.private_key_pem().as_bytes())?;
    set_owner_only(&key_path)?;
    if !credential.kubeconfig_yaml().is_empty() {
        atomic_write(
            &dir.join("node_kubeconfig.yaml"),
            credential.kubeconfig_yaml().as_bytes(),
        )?;
    }
    Ok(())
}

fn delete_credential(dir: &std::path::Path) -> Result<(), CredentialOperationError> {
    for path in [
        dir.join("node.crt"),
        dir.join("node.key"),
        dir.join("node_kubeconfig.yaml"),
    ] {
        if path.exists() {
            std::fs::remove_file(&path).map_err(|error| {
                CredentialOperationError::dependency_failure(format!(
                    "failed to remove {}: {error}",
                    path.display()
                ))
            })?;
        }
    }
    Ok(())
}

fn atomic_write(
    final_path: &std::path::Path,
    content: &[u8],
) -> Result<(), CredentialOperationError> {
    use std::io::Write as _;

    let temporary_path = PathBuf::from(format!("{}.tmp", final_path.display()));
    let mut file = std::fs::File::create(&temporary_path).map_err(|error| {
        CredentialOperationError::dependency_failure(format!(
            "failed to create {}: {error}",
            temporary_path.display()
        ))
    })?;
    file.write_all(content).map_err(|error| {
        CredentialOperationError::dependency_failure(format!(
            "failed to write {}: {error}",
            temporary_path.display()
        ))
    })?;
    file.sync_all().map_err(|error| {
        CredentialOperationError::dependency_failure(format!(
            "failed to sync {}: {error}",
            temporary_path.display()
        ))
    })?;
    drop(file);
    std::fs::rename(&temporary_path, final_path).map_err(|error| {
        CredentialOperationError::dependency_failure(format!(
            "failed to rename {} -> {}: {error}",
            temporary_path.display(),
            final_path.display()
        ))
    })
}

#[cfg(unix)]
fn set_owner_only(path: &std::path::Path) -> Result<(), CredentialOperationError> {
    use std::os::unix::fs::PermissionsExt as _;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(|error| {
        CredentialOperationError::dependency_failure(format!(
            "failed to set 0600 permissions on {}: {error}",
            path.display()
        ))
    })
}

#[cfg(not(unix))]
fn set_owner_only(_path: &std::path::Path) -> Result<(), CredentialOperationError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_credential() -> WorkerCredential {
        WorkerCredential::try_new(
            "certificate".to_string(),
            "private-key".to_string(),
            "worker-a".to_string(),
            "current-context: worker-a".to_string(),
        )
        .expect("valid sample credential")
    }

    #[tokio::test]
    async fn supervised_store_round_trips_and_deletes_credential() {
        let dir = tempfile::tempdir().expect("temporary credential root");
        let supervisor = Arc::new(TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let store = SupervisedFilesystemWorkerCredentialStore::new(
            dir.path().join("etc"),
            "worker-a",
            supervisor.clone(),
        );
        let credential = sample_credential();

        store.save(&credential).await.expect("save credential");
        assert_eq!(
            store.load().await.expect("load credential"),
            Some(credential)
        );
        store.delete().await.expect("delete credential");
        assert!(store.load().await.expect("load empty store").is_none());
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn supervised_store_protects_private_key_permissions() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("temporary credential root");
        let supervisor = Arc::new(TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let store = SupervisedFilesystemWorkerCredentialStore::new(
            dir.path().join("etc"),
            "worker-a",
            supervisor.clone(),
        );
        store
            .save(&sample_credential())
            .await
            .expect("save credential");
        let mode = std::fs::metadata(dir.path().join("etc/node.key"))
            .expect("private key metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        let _ = supervisor.shutdown(std::time::Duration::from_secs(1)).await;
    }
}
