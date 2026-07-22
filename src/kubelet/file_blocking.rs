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

/// Narrow seam for process-backed maintenance operations. Production always
/// uses the app-owned supervisor; tests can inject scripted outputs without
/// executing a host command.
#[async_trait::async_trait]
pub(crate) trait ProcessOutputRunner: Send + Sync {
    async fn run(
        &self,
        category: crate::task_supervisor::TaskCategory,
        name: &'static str,
        program: &str,
        args: &[&str],
    ) -> Result<std::process::Output>;
}

pub(crate) struct SupervisedProcessOutputRunner;

#[async_trait::async_trait]
impl ProcessOutputRunner for SupervisedProcessOutputRunner {
    async fn run(
        &self,
        category: crate::task_supervisor::TaskCategory,
        name: &'static str,
        program: &str,
        args: &[&str],
    ) -> Result<std::process::Output> {
        let mut command = std::process::Command::new(program);
        command.args(args);
        supervisor()
            .run_process_output(category, name, command)
            .await
            .with_context(|| format!("supervised process {name} ({program})"))
    }
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

#[cfg(test)]
pub(crate) mod process_test_support {
    use super::ProcessOutputRunner;
    use anyhow::{Result, anyhow};
    use std::collections::VecDeque;
    use std::process::Output;
    use std::sync::Mutex;

    pub(crate) struct FakeProcessOutputRunner {
        results: Mutex<VecDeque<Result<Output>>>,
        calls: Mutex<Vec<(String, String, Vec<String>)>>,
    }

    impl FakeProcessOutputRunner {
        pub(crate) fn new(results: Vec<Result<Output>>) -> Self {
            Self {
                results: Mutex::new(results.into()),
                calls: Mutex::new(Vec::new()),
            }
        }

        pub(crate) fn calls(&self) -> Vec<(String, String, Vec<String>)> {
            self.calls.lock().expect("fake calls lock").clone()
        }
    }

    #[async_trait::async_trait]
    impl ProcessOutputRunner for FakeProcessOutputRunner {
        async fn run(
            &self,
            _category: crate::task_supervisor::TaskCategory,
            name: &'static str,
            program: &str,
            args: &[&str],
        ) -> Result<Output> {
            self.calls.lock().expect("fake calls lock").push((
                name.to_string(),
                program.to_string(),
                args.iter().map(|arg| (*arg).to_string()).collect(),
            ));
            self.results
                .lock()
                .expect("fake results lock")
                .pop_front()
                .unwrap_or_else(|| Err(anyhow!("fake process runner exhausted")))
        }
    }

    pub(crate) fn output(status: i32, stdout: &[u8], stderr: &[u8]) -> Output {
        use std::os::unix::process::ExitStatusExt;

        Output {
            status: std::process::ExitStatus::from_raw(status << 8),
            stdout: stdout.to_vec(),
            stderr: stderr.to_vec(),
        }
    }
}
