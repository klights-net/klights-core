//! Root adapter for the CNI feature's supervised Unix-socket filesystem port.

use std::sync::Arc;

use anyhow::Context;

use crate::cni_plugin::{CniSocketFilesystem, CniSocketFuture, CniSocketPath};

pub(crate) struct RootCniSocketFilesystem {
    file_process: klights_supervisor::FileProcessExecutor,
}

impl RootCniSocketFilesystem {
    pub(crate) fn shared(
        file_process: klights_supervisor::FileProcessExecutor,
    ) -> Arc<dyn CniSocketFilesystem> {
        Arc::new(Self { file_process })
    }
}

impl CniSocketFilesystem for RootCniSocketFilesystem {
    fn bind_listener(
        &self,
        socket_path: &CniSocketPath,
    ) -> CniSocketFuture<'_, tokio::net::UnixListener> {
        let socket_path = socket_path.clone();
        Box::pin(async move {
            let path = std::path::Path::new(socket_path.as_str());
            if let Some(parent) = path.parent() {
                crate::runtime_fs::create_dir_all_async(&self.file_process, parent)
                    .await
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            let _ = crate::runtime_fs::remove_file_if_exists_async(&self.file_process, path).await;
            tokio::net::UnixListener::bind(path)
                .with_context(|| format!("failed to bind {}", path.display()))
        })
    }

    fn remove_socket(&self, socket_path: &CniSocketPath) -> CniSocketFuture<'_, ()> {
        let socket_path = socket_path.clone();
        Box::pin(async move {
            crate::runtime_fs::remove_file_if_exists_async(&self.file_process, socket_path.as_str())
                .await
                .map(|_| ())
        })
    }
}
