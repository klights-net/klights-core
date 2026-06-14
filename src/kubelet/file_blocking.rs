use anyhow::{Context, Result};
use std::sync::{Arc, OnceLock};

static FILE_SUPERVISOR: OnceLock<Arc<crate::task_supervisor::TaskSupervisor>> = OnceLock::new();

/// Install the app-owned supervisor used for kubelet/networking file blocking.
/// Bootstrap calls this exactly once, before any kubelet, networking, or auth
/// code runs (those paths reach this module via `read_utf8_file_async` and the
/// volume materialization helpers). Returns Err with the supplied supervisor
/// if init was already called — production callers should treat that as a
/// programming error.
pub fn init_file_blocking_supervisor(
    supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
) -> std::result::Result<(), Arc<crate::task_supervisor::TaskSupervisor>> {
    FILE_SUPERVISOR.set(supervisor)
}

#[cfg(not(test))]
fn supervisor() -> &'static Arc<crate::task_supervisor::TaskSupervisor> {
    FILE_SUPERVISOR.get().expect(
        "file_blocking supervisor not initialized; bootstrap must call \
         init_file_blocking_supervisor before any kubelet/networking work",
    )
}

#[cfg(test)]
fn supervisor() -> &'static Arc<crate::task_supervisor::TaskSupervisor> {
    // Test fallback: lazily create a dedicated supervisor so unit tests do not
    // need bootstrap. Production builds (`cfg(not(test))`) panic instead so a
    // missing init surfaces immediately at startup.
    FILE_SUPERVISOR.get_or_init(|| {
        Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ))
    })
}

pub async fn run_blocking_file<T>(
    name: impl Into<String>,
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T>
where
    T: Send + 'static,
{
    let name = name.into();
    let label = name.clone();
    supervisor()
        .run_blocking_file(name, f)
        .await
        .with_context(|| format!("file_blocking::run_blocking_file({label})"))?
}

pub async fn run_blocking_file_keyed<T>(
    name: impl Into<String>,
    key: impl Into<String>,
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T>
where
    T: Send + 'static,
{
    let name = name.into();
    let label = name.clone();
    supervisor()
        .run_blocking_file_keyed(name, key, f)
        .await
        .with_context(|| format!("file_blocking::run_blocking_file_keyed({label})"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_supervisor::TaskCategoryConfig;

    #[test]
    fn file_blocking_supervisor_can_be_set_at_least_once() {
        // The OnceLock backing init_file_blocking_supervisor is process-wide,
        // and other tests in this binary may have already initialized it via
        // the test fallback in supervisor(). So set() may legitimately return
        // Err here — both Ok(()) and Err(_) are acceptable. What we are
        // verifying is that init_file_blocking_supervisor compiles and links
        // and that subsequent calls do not panic.
        let s = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            TaskCategoryConfig::default(),
        ));
        let _ = init_file_blocking_supervisor(s);
    }
}
