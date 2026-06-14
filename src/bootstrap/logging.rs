use std::path::PathBuf;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const TRUE_LOG_FILE_VALUE: &str = "true";

pub(crate) fn resolve_log_file_path(raw: &str, containerd_namespace: &str) -> PathBuf {
    if raw.trim().eq_ignore_ascii_case(TRUE_LOG_FILE_VALUE) {
        crate::paths::data_root_path(containerd_namespace)
            .join("logs")
            .join("klights.log")
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
            crate::utils::create_dir_all(parent).unwrap_or_else(|err| {
                panic!("failed to create log directory {}: {err}", parent.display())
            });
        }
        let file = crate::utils::open_append_file(&log_path)
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
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn log_file_true_uses_data_root_klights_log_case_insensitive() {
        let _guard = ENV_LOCK.lock().unwrap();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_DATA_ROOT", "/tmp/klights-log-test") };
        let path = resolve_log_file_path("TrUe", "ignored-ns");
        assert_eq!(
            path,
            PathBuf::from("/tmp/klights-log-test/logs/klights.log")
        );
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::remove_var("KLIGHTS_DATA_ROOT") };
    }

    #[test]
    fn log_file_non_true_value_is_full_path() {
        assert_eq!(
            resolve_log_file_path("/tmp/custom-klights.log", "ignored-ns"),
            PathBuf::from("/tmp/custom-klights.log")
        );
    }
}
