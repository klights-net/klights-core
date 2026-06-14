use std::{io, path::PathBuf, sync::Arc};

use tonic::Status;

use crate::task_supervisor::TaskSupervisor;

#[derive(Clone)]
pub(super) struct ControlplaneCaFiles {
    containerd_namespace: String,
    supervisor: Arc<TaskSupervisor>,
}

impl ControlplaneCaFiles {
    pub(super) fn new(supervisor: Arc<TaskSupervisor>) -> Self {
        Self {
            containerd_namespace: String::new(),
            supervisor,
        }
    }

    pub(super) fn set_namespace(&mut self, namespace: &str) {
        self.containerd_namespace = namespace.to_string();
    }

    pub(super) fn containerd_namespace(&self) -> Result<&str, Status> {
        if self.containerd_namespace.is_empty() {
            return Err(Status::failed_precondition(
                "ServiceAccount signing key not available on this node",
            ));
        }
        Ok(&self.containerd_namespace)
    }

    pub(super) async fn join_response_ca_cert_pem(&self) -> Result<String, Status> {
        if self.containerd_namespace.is_empty() {
            return Ok(String::new());
        }

        match self
            .read_text_file(
                "grpc_controlplane_join_ca_cert",
                crate::paths::ca_cert_path(&self.containerd_namespace),
            )
            .await?
        {
            Ok(pem) => Ok(pem),
            Err(_) => Ok(String::new()),
        }
    }

    pub(super) async fn signing_ca_cert_pem(&self) -> Result<String, Status> {
        let pem = self
            .read_signing_ca_cert_pem_or_empty("grpc_controlplane_sign_ca_cert")
            .await?;
        if pem.is_empty() {
            return Err(Status::failed_precondition(
                "cluster CA cert not available on this node",
            ));
        }
        Ok(pem)
    }

    pub(super) async fn signing_ca_key_pem(&self) -> Result<String, Status> {
        if self.containerd_namespace.is_empty() {
            return Err(Status::failed_precondition(
                "cluster CA key not available on this node",
            ));
        }

        self.read_text_file(
            "grpc_controlplane_sign_ca_key",
            crate::paths::ca_key_path(&self.containerd_namespace),
        )
        .await?
        .map_err(|err| Status::failed_precondition(format!("CA key not available: {err}")))
    }

    async fn read_signing_ca_cert_pem_or_empty(
        &self,
        task_name: &'static str,
    ) -> Result<String, Status> {
        if self.containerd_namespace.is_empty() {
            return Ok(String::new());
        }

        match self
            .read_text_file(
                task_name,
                crate::paths::ca_cert_path(&self.containerd_namespace),
            )
            .await?
        {
            Ok(pem) => Ok(pem),
            Err(_) => Ok(String::new()),
        }
    }

    async fn read_text_file(
        &self,
        task_name: &'static str,
        path: PathBuf,
    ) -> Result<io::Result<String>, Status> {
        let key = path.to_string_lossy().to_string();
        let path_for_task = path.clone();
        self.supervisor
            .run_blocking_file_keyed(task_name, key, move || {
                std::fs::read_to_string(path_for_task)
            })
            .await
            .map_err(|err| Status::internal(format!("supervised CA file read failed: {err}")))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tonic::Code;

    use super::ControlplaneCaFiles;
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};

    fn supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
    }

    fn temp_namespace(temp: &tempfile::TempDir) -> String {
        temp.path().to_string_lossy().to_string()
    }

    #[tokio::test]
    async fn reads_existing_ca_material_through_supervisor() {
        let temp = tempfile::tempdir().unwrap();
        let namespace = temp_namespace(&temp);
        std::fs::create_dir_all(crate::paths::etc_dir_path(&namespace)).unwrap();
        std::fs::write(crate::paths::ca_cert_path(&namespace), "cert-pem").unwrap();
        std::fs::write(crate::paths::ca_key_path(&namespace), "key-pem").unwrap();
        let supervisor = supervisor();
        let mut files = ControlplaneCaFiles::new(supervisor.clone());
        files.set_namespace(&namespace);

        assert_eq!(files.join_response_ca_cert_pem().await.unwrap(), "cert-pem");
        assert_eq!(files.signing_ca_cert_pem().await.unwrap(), "cert-pem");
        assert_eq!(files.signing_ca_key_pem().await.unwrap(), "key-pem");

        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn missing_ca_material_preserves_grpc_status_behavior() {
        let temp = tempfile::tempdir().unwrap();
        let namespace = temp_namespace(&temp);
        let supervisor = supervisor();
        let mut files = ControlplaneCaFiles::new(supervisor.clone());
        files.set_namespace(&namespace);

        assert_eq!(files.join_response_ca_cert_pem().await.unwrap(), "");

        let cert_error = files.signing_ca_cert_pem().await.unwrap_err();
        assert_eq!(cert_error.code(), Code::FailedPrecondition);
        assert_eq!(
            cert_error.message(),
            "cluster CA cert not available on this node"
        );

        let key_error = files.signing_ca_key_pem().await.unwrap_err();
        assert_eq!(key_error.code(), Code::FailedPrecondition);
        assert!(key_error.message().contains("CA key not available"));

        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }
}
