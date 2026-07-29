//! Blocking filesystem primitives and their supervised async boundary.

pub fn read_utf8(path: impl AsRef<std::path::Path>) -> std::io::Result<String> {
    let bytes = std::fs::read(path)?;
    String::from_utf8(bytes)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

pub fn create_dir_all(path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    std::fs::create_dir_all(path)
}

pub fn open_append(path: impl AsRef<std::path::Path>) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}

pub async fn read_utf8_async(
    file_process: &crate::FileProcessExecutor,
    path: impl AsRef<std::path::Path>,
) -> std::io::Result<String> {
    let path = path.as_ref().to_path_buf();
    let key = path.to_string_lossy().into_owned();
    file_process
        .run_blocking_file_keyed("runtime_fs_read_utf8", key, move || {
            read_utf8(path).map_err(anyhow::Error::from)
        })
        .await
        .map_err(|error| {
            if let Some(io_error) = error.downcast_ref::<std::io::Error>() {
                std::io::Error::new(io_error.kind(), io_error.to_string())
            } else {
                std::io::Error::other(error.to_string())
            }
        })
}

pub async fn create_dir_all_async(
    file_process: &crate::FileProcessExecutor,
    path: impl AsRef<std::path::Path>,
) -> anyhow::Result<()> {
    let path = path.as_ref().to_path_buf();
    let key = path.to_string_lossy().into_owned();
    file_process
        .run_blocking_file_keyed("runtime_fs_create_dir_all", key, move || {
            std::fs::create_dir_all(path).map_err(anyhow::Error::from)
        })
        .await
}

pub async fn write_async(
    file_process: &crate::FileProcessExecutor,
    path: impl AsRef<std::path::Path>,
    contents: impl AsRef<[u8]>,
) -> anyhow::Result<()> {
    let path = path.as_ref().to_path_buf();
    let bytes = contents.as_ref().to_vec();
    let key = path.to_string_lossy().into_owned();
    file_process
        .run_blocking_file_keyed("runtime_fs_write", key, move || {
            std::fs::write(path, bytes).map_err(anyhow::Error::from)
        })
        .await
}

pub async fn exists_async(
    file_process: &crate::FileProcessExecutor,
    path: impl AsRef<std::path::Path>,
) -> anyhow::Result<bool> {
    let path = path.as_ref().to_path_buf();
    let key = path.to_string_lossy().into_owned();
    file_process
        .run_blocking_file_keyed("runtime_fs_exists", key, move || {
            match std::fs::metadata(path) {
                Ok(_) => Ok(true),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(error) => Err(anyhow::Error::from(error)),
            }
        })
        .await
}

pub async fn canonicalize_async(
    file_process: &crate::FileProcessExecutor,
    path: impl AsRef<std::path::Path>,
) -> anyhow::Result<std::path::PathBuf> {
    let path = path.as_ref().to_path_buf();
    let key = path.to_string_lossy().into_owned();
    file_process
        .run_blocking_file_keyed("runtime_fs_canonicalize", key, move || {
            std::fs::canonicalize(path).map_err(anyhow::Error::from)
        })
        .await
}

pub async fn remove_file_if_exists_async(
    file_process: &crate::FileProcessExecutor,
    path: impl AsRef<std::path::Path>,
) -> anyhow::Result<bool> {
    remove_if_exists(
        file_process,
        path,
        "runtime_fs_remove_file",
        std::fs::remove_file,
    )
    .await
}

pub async fn remove_dir_if_exists_async(
    file_process: &crate::FileProcessExecutor,
    path: impl AsRef<std::path::Path>,
) -> anyhow::Result<bool> {
    remove_if_exists(
        file_process,
        path,
        "runtime_fs_remove_dir",
        std::fs::remove_dir,
    )
    .await
}

pub async fn remove_dir_all_if_exists_async(
    file_process: &crate::FileProcessExecutor,
    path: impl AsRef<std::path::Path>,
) -> anyhow::Result<bool> {
    remove_if_exists(
        file_process,
        path,
        "runtime_fs_remove_dir_all",
        std::fs::remove_dir_all,
    )
    .await
}

async fn remove_if_exists(
    file_process: &crate::FileProcessExecutor,
    path: impl AsRef<std::path::Path>,
    label: &'static str,
    remove: fn(std::path::PathBuf) -> std::io::Result<()>,
) -> anyhow::Result<bool> {
    let path = path.as_ref().to_path_buf();
    let key = path.to_string_lossy().into_owned();
    file_process
        .run_blocking_file_keyed(label, key, move || match remove(path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(anyhow::Error::from(error)),
        })
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::{FileProcessExecutor, TaskCategoryConfig, TaskSupervisor};

    fn executor() -> (Arc<TaskSupervisor>, FileProcessExecutor) {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let file_process = FileProcessExecutor::new(supervisor.clone());
        (supervisor, file_process)
    }

    #[tokio::test]
    async fn supervised_primitives_preserve_utf8_and_missing_path_semantics() {
        let temp = tempfile::tempdir().unwrap();
        let text = temp.path().join("nested").join("value");
        let missing = temp.path().join("missing");
        let (supervisor, file_process) = executor();

        create_dir_all_async(&file_process, text.parent().unwrap())
            .await
            .unwrap();
        write_async(&file_process, &text, b"klights").await.unwrap();
        assert_eq!(
            read_utf8_async(&file_process, &text).await.unwrap(),
            "klights"
        );
        assert!(exists_async(&file_process, &text).await.unwrap());
        assert!(!exists_async(&file_process, &missing).await.unwrap());
        assert!(
            !remove_file_if_exists_async(&file_process, &missing)
                .await
                .unwrap()
        );
        assert!(
            remove_file_if_exists_async(&file_process, &text)
                .await
                .unwrap()
        );

        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn supervised_utf8_read_preserves_invalid_data_error_kind() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("invalid");
        std::fs::write(&path, [0xff]).unwrap();
        let (supervisor, file_process) = executor();

        let error = read_utf8_async(&file_process, path).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

        let _ = supervisor.shutdown(Duration::from_secs(1)).await;
    }
}
