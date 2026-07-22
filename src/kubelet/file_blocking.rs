use anyhow::{Context, Result};

#[cfg(test)]
pub(crate) fn test_file_process_executor() -> klights_supervisor::FileProcessExecutor {
    klights_supervisor::FileProcessExecutor::new(std::sync::Arc::new(
        klights_supervisor::TaskSupervisor::new(klights_supervisor::TaskCategoryConfig::default()),
    ))
}

/// Narrow seam for process-backed maintenance operations. Production always
/// uses the app-owned supervisor; tests can inject scripted outputs without
/// executing a host command.
#[async_trait::async_trait]
pub(crate) trait ProcessOutputRunner: Send + Sync {
    async fn run(
        &self,
        category: klights_supervisor::TaskCategory,
        name: &'static str,
        program: &str,
        args: &[&str],
    ) -> Result<std::process::Output>;
}

#[async_trait::async_trait]
impl ProcessOutputRunner for klights_supervisor::FileProcessExecutor {
    async fn run(
        &self,
        category: klights_supervisor::TaskCategory,
        name: &'static str,
        program: &str,
        args: &[&str],
    ) -> Result<std::process::Output> {
        let mut command = std::process::Command::new(program);
        command.args(args);
        self.run_process_output(category, name, command)
            .await
            .with_context(|| format!("supervised process {name} ({program})"))
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
            _category: klights_supervisor::TaskCategory,
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
