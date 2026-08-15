use std::path::PathBuf;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const TRUE_LOG_FILE_VALUE: &str = "true";

pub(crate) fn resolve_log_file_path(raw: &str, containerd_namespace: &str) -> PathBuf {
    resolve_log_file_path_under(raw, &crate::paths::data_root_path(containerd_namespace))
}

fn resolve_log_file_path_under(raw: &str, data_root: &std::path::Path) -> PathBuf {
    if raw.trim().eq_ignore_ascii_case(TRUE_LOG_FILE_VALUE) {
        data_root.join("logs").join("klights.log")
    } else {
        PathBuf::from(raw)
    }
}

pub(crate) fn log_file_path_from_env(containerd_namespace: &str) -> Option<PathBuf> {
    std::env::var("KLIGHTS_LOG_FILE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| resolve_log_file_path(&value, containerd_namespace))
}

pub(crate) fn init_tracing_from_env(containerd_namespace: &str) {
    let registry = tracing_subscriber::registry().with(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "klights=debug,tower_http=debug".into()),
    );

    if let Some(log_path) = log_file_path_from_env(containerd_namespace) {
        if let Some(parent) = log_path.parent() {
            klights_supervisor::runtime_fs::create_dir_all(parent).unwrap_or_else(|err| {
                panic!("failed to create log directory {}: {err}", parent.display())
            });
        }
        let file = klights_supervisor::runtime_fs::open_append(&log_path)
            .unwrap_or_else(|err| panic!("failed to open log file {}: {err}", log_path.display()));
        registry
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(std::sync::Mutex::new(file)),
            )
            .init();
        return;
    }

    registry
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .without_time(),
        )
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_file_true_uses_data_root_klights_log_case_insensitive() {
        let path =
            resolve_log_file_path_under("TrUe", std::path::Path::new("/tmp/klights/log-test"));
        assert_eq!(
            path,
            PathBuf::from("/tmp/klights/log-test/logs/klights.log")
        );
    }

    #[test]
    fn log_file_non_true_value_is_full_path() {
        assert_eq!(
            resolve_log_file_path("/tmp/custom-klights.log", "ignored-ns"),
            PathBuf::from("/tmp/custom-klights.log")
        );
    }
}
