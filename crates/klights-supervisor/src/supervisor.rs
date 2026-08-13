use super::category::{TaskCategory, TaskCategoryConfig};
use super::task::{
    ActiveTask, ActiveTaskStatus, DbQueryLoggingStatus, ShutdownReport, TaskAdmissionError,
    TaskCategoryStatus, TaskJoinError, TaskOutcome, TaskOutcomeStatus,
};
use crate::{DbCallResult, DbClosureResult, DbError};
use anyhow::{Context, Result, anyhow};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::process::{ExitStatus, Output};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessShutdownPolicy {
    /// Terminate and reap the child when root shutdown begins.
    KillAndReap,
    /// Intentionally leave a still-running child alive during root shutdown
    /// while releasing its supervisor registration. This is reserved for
    /// externally recoverable daemons whose shutdown contract explicitly
    /// preserves their workloads. Dropping the control handle without root
    /// shutdown always kills and reaps the child.
    Preserve,
}

#[derive(Debug)]
pub enum ProcessError {
    Admission(TaskAdmissionError),
    Spawn(std::io::Error),
    Io {
        operation: &'static str,
        source: std::io::Error,
    },
    Join(TaskJoinError),
    Cancelled,
    Preserved,
    ControlClosed,
}

impl fmt::Display for ProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(error) => error.fmt(formatter),
            Self::Spawn(error) => write!(formatter, "failed to spawn process: {error}"),
            Self::Io { operation, source } => {
                write!(formatter, "process {operation} failed: {source}")
            }
            Self::Join(error) => write!(formatter, "supervised process task failed: {error}"),
            Self::Cancelled => formatter.write_str("process cancelled by supervisor shutdown"),
            Self::Preserved => {
                formatter.write_str("process intentionally preserved after shutdown")
            }
            Self::ControlClosed => formatter.write_str("supervised process control channel closed"),
        }
    }
}

impl std::error::Error for ProcessError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Admission(error) => Some(error),
            Self::Spawn(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            Self::Join(error) => Some(error),
            Self::Cancelled | Self::Preserved | Self::ControlClosed => None,
        }
    }
}

impl From<TaskAdmissionError> for ProcessError {
    fn from(error: TaskAdmissionError) -> Self {
        Self::Admission(error)
    }
}

impl From<TaskJoinError> for ProcessError {
    fn from(error: TaskJoinError) -> Self {
        Self::Join(error)
    }
}

#[derive(Clone, Debug)]
enum ProcessTerminal {
    Exited(ExitStatus),
    Io {
        operation: &'static str,
        kind: std::io::ErrorKind,
        message: String,
    },
    Preserved,
}

impl ProcessTerminal {
    fn wait_result(&self) -> std::result::Result<ExitStatus, ProcessError> {
        match self {
            Self::Exited(status) => Ok(*status),
            Self::Io {
                operation,
                kind,
                message,
            } => Err(ProcessError::Io {
                operation,
                source: std::io::Error::new(*kind, message.clone()),
            }),
            Self::Preserved => Err(ProcessError::Preserved),
        }
    }
}

enum ProcessCommand {
    Wait(oneshot::Sender<std::result::Result<ExitStatus, ProcessError>>),
    Kill(oneshot::Sender<std::result::Result<(), ProcessError>>),
}

/// Control handle for a child whose lifetime is owned and observed by
/// [`TaskSupervisor`]. Dropping the handle kills and reaps the child. The
/// explicit shutdown policy applies only when root shutdown begins, allowing
/// selected daemons to be deliberately detached for an external owner to
/// reuse.
#[derive(Debug)]
pub struct SupervisedChild {
    pid: u32,
    control: Option<mpsc::Sender<ProcessCommand>>,
    terminal: Arc<Mutex<Option<ProcessTerminal>>>,
}

impl SupervisedChild {
    pub fn id(&self) -> Option<u32> {
        Some(self.pid)
    }

    pub async fn wait(&mut self) -> std::result::Result<ExitStatus, ProcessError> {
        if let Some(terminal) = lock_recover(&self.terminal).clone() {
            return terminal.wait_result();
        }
        let (reply, result) = oneshot::channel();
        let Some(control) = &self.control else {
            return Err(ProcessError::ControlClosed);
        };
        if control.send(ProcessCommand::Wait(reply)).await.is_err() {
            return lock_recover(&self.terminal)
                .clone()
                .map_or(Err(ProcessError::ControlClosed), |terminal| {
                    terminal.wait_result()
                });
        }
        result.await.unwrap_or_else(|_| {
            lock_recover(&self.terminal)
                .clone()
                .map_or(Err(ProcessError::ControlClosed), |terminal| {
                    terminal.wait_result()
                })
        })
    }

    pub async fn kill(&mut self) -> std::result::Result<(), ProcessError> {
        if let Some(terminal) = lock_recover(&self.terminal).clone() {
            return terminal.wait_result().map(|_| ());
        }
        let (reply, result) = oneshot::channel();
        let Some(control) = &self.control else {
            return Err(ProcessError::ControlClosed);
        };
        control
            .send(ProcessCommand::Kill(reply))
            .await
            .map_err(|_| ProcessError::ControlClosed)?;
        result.await.unwrap_or(Err(ProcessError::ControlClosed))
    }
}

#[derive(Clone)]
pub struct TaskSupervisor {
    inner: Arc<TaskSupervisorInner>,
}

/// Narrow, explicitly injected access to supervised filesystem and process
/// execution.
#[derive(Clone)]
pub struct FileProcessExecutor {
    supervisor: Arc<TaskSupervisor>,
}

/// Narrow, explicitly injected access to CPU-heavy certificate, key, and
/// token operations.
#[derive(Clone)]
pub struct CryptoExecutor {
    supervisor: Arc<TaskSupervisor>,
}

impl CryptoExecutor {
    pub fn new(supervisor: Arc<TaskSupervisor>) -> Self {
        Self { supervisor }
    }

    pub fn from_supervisor(supervisor: &TaskSupervisor) -> Self {
        Self::new(Arc::new(supervisor.clone()))
    }

    pub async fn run_blocking<T>(
        &self,
        name: impl Into<String>,
        f: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        self.supervisor
            .run_blocking(TaskCategory::Others, name, f)
            .await
    }
}

impl FileProcessExecutor {
    pub fn new(supervisor: Arc<TaskSupervisor>) -> Self {
        Self { supervisor }
    }

    pub fn from_supervisor(supervisor: &TaskSupervisor) -> Self {
        Self::new(Arc::new(supervisor.clone()))
    }

    pub fn crypto_executor(&self) -> CryptoExecutor {
        CryptoExecutor::new(self.supervisor.clone())
    }

    pub async fn run_blocking_file<T>(
        &self,
        name: impl Into<String>,
        f: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        let name = name.into();
        let label = name.clone();
        self.supervisor
            .run_blocking_file(name, f)
            .await
            .with_context(|| format!("file_blocking::run_blocking_file({label})"))?
    }

    pub async fn run_blocking_file_keyed<T>(
        &self,
        name: impl Into<String>,
        key: impl Into<String>,
        f: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<T>
    where
        T: Send + 'static,
    {
        let name = name.into();
        let label = name.clone();
        self.supervisor
            .run_blocking_file_keyed(name, key, f)
            .await
            .with_context(|| format!("file_blocking::run_blocking_file_keyed({label})"))?
    }

    pub async fn run_process_output(
        &self,
        category: TaskCategory,
        name: impl Into<String>,
        command: std::process::Command,
    ) -> std::result::Result<Output, ProcessError> {
        self.supervisor
            .run_process_output(category, name, command)
            .await
    }
}

struct TaskSupervisorInner {
    config: TaskCategoryConfig,
    next_task_id: AtomicU64,
    root_cancellation: CancellationToken,
    db_query_logging_enabled: AtomicBool,
    lifecycle: Mutex<LifecycleState>,
    managed_task_change: Notify,
    queued_by_category: Mutex<HashMap<TaskCategory, usize>>,
    file_keyed_guards: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    semaphores: HashMap<TaskCategory, Arc<Semaphore>>,
    category_free_notifies: HashMap<TaskCategory, Arc<Notify>>,
    #[cfg(test)]
    terminal_cleanup_pause: Mutex<Option<Arc<TerminalCleanupPause>>>,
    #[cfg(test)]
    process_startup_pause: Mutex<Option<Arc<ProcessStartupPause>>>,
}

#[derive(Clone)]
struct ManagedTaskControl {
    abort_handle: tokio::task::AbortHandle,
    completion: Arc<TaskCompletion>,
    abort_source: Arc<AtomicU8>,
    abort_on_shutdown_timeout: bool,
}

struct LifecycleState {
    accepting: bool,
    active_tasks: HashMap<u64, ActiveTask>,
    managed_tasks: HashMap<u64, ManagedTaskControl>,
    recent_outcomes: VecDeque<TaskOutcomeStatus>,
}

struct TaskCompletion {
    terminal: AtomicBool,
    outcome: AtomicU8,
}

const OUTCOME_HISTORY_LIMIT: usize = 256;
const OUTCOME_PENDING: u8 = 0;
const ABORT_NONE: u8 = 0;
const ABORT_CALLER: u8 = 1;
const ABORT_SHUTDOWN: u8 = 2;

impl TaskSupervisor {
    pub fn new(config: TaskCategoryConfig) -> Self {
        let mut semaphores = HashMap::new();
        let mut category_free_notifies = HashMap::new();
        for category in TaskCategory::all() {
            let limit = config.limit_for(category);
            if limit > 0 {
                semaphores.insert(category, Arc::new(Semaphore::new(limit)));
            }
            category_free_notifies.insert(category, Arc::new(Notify::new()));
        }
        Self {
            inner: Arc::new(TaskSupervisorInner {
                config,
                next_task_id: AtomicU64::new(1),
                root_cancellation: CancellationToken::new(),
                db_query_logging_enabled: AtomicBool::new(false),
                lifecycle: Mutex::new(LifecycleState {
                    accepting: true,
                    active_tasks: HashMap::new(),
                    managed_tasks: HashMap::new(),
                    recent_outcomes: VecDeque::with_capacity(OUTCOME_HISTORY_LIMIT),
                }),
                managed_task_change: Notify::new(),
                queued_by_category: Mutex::new(HashMap::new()),
                file_keyed_guards: Mutex::new(HashMap::new()),
                semaphores,
                category_free_notifies,
                #[cfg(test)]
                terminal_cleanup_pause: Mutex::new(None),
                #[cfg(test)]
                process_startup_pause: Mutex::new(None),
            }),
        }
    }

    pub fn config(&self) -> TaskCategoryConfig {
        self.inner.config.clone()
    }

    pub fn semaphore_limit(&self, category: TaskCategory) -> Option<usize> {
        self.inner
            .semaphores
            .get(&category)
            .map(|semaphore| semaphore.available_permits())
    }

    pub fn is_category_free(&self, category: TaskCategory) -> bool {
        let Some(semaphore) = self.inner.semaphores.get(&category) else {
            return true;
        };
        semaphore.available_permits() > 0
    }

    pub fn category_free_notify(&self, category: TaskCategory) -> Arc<Notify> {
        self.inner
            .category_free_notifies
            .get(&category)
            .cloned()
            .unwrap_or_else(|| Arc::new(Notify::new()))
    }

    pub fn category_statuses(&self) -> Vec<TaskCategoryStatus> {
        let lifecycle = lock_recover(&self.inner.lifecycle);
        let queued = lock_recover(&self.inner.queued_by_category);
        let mut active_by_category = HashMap::<TaskCategory, usize>::new();
        for task in lifecycle.active_tasks.values() {
            *active_by_category.entry(task.category).or_insert(0) += 1;
        }

        TaskCategory::all()
            .into_iter()
            .map(|category| TaskCategoryStatus {
                category,
                limit: self.inner.config.limit_for(category),
                active: active_by_category
                    .get(&category)
                    .copied()
                    .unwrap_or_default(),
                queued: queued.get(&category).copied().unwrap_or_default(),
            })
            .collect()
    }

    pub fn active_tasks(&self, category: Option<TaskCategory>) -> Vec<ActiveTaskStatus> {
        let lifecycle = lock_recover(&self.inner.lifecycle);
        let mut rows: Vec<ActiveTaskStatus> = lifecycle
            .active_tasks
            .values()
            .filter(|task| category.is_none_or(|selected| selected == task.category))
            .map(ActiveTask::to_status)
            .collect();
        rows.sort_by_key(|row| row.id);
        rows
    }

    pub fn db_query_logging_status(&self) -> DbQueryLoggingStatus {
        DbQueryLoggingStatus {
            enabled: self.inner.db_query_logging_enabled.load(Ordering::Relaxed),
        }
    }

    pub fn set_db_query_logging(&self, enabled: bool) -> DbQueryLoggingStatus {
        self.inner
            .db_query_logging_enabled
            .store(enabled, Ordering::Relaxed);
        self.db_query_logging_status()
    }

    pub fn root_cancellation_token(&self) -> CancellationToken {
        self.inner.root_cancellation.clone()
    }

    pub fn recent_task_outcomes(&self) -> Vec<TaskOutcomeStatus> {
        lock_recover(&self.inner.lifecycle)
            .recent_outcomes
            .iter()
            .cloned()
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn pause_next_terminal_cleanup(&self) -> Arc<TerminalCleanupPause> {
        let pause = Arc::new(TerminalCleanupPause::new());
        *lock_recover(&self.inner.terminal_cleanup_pause) = Some(pause.clone());
        pause
    }

    #[cfg(test)]
    pub(crate) fn pause_next_process_startup_handoff(&self) -> Arc<ProcessStartupPause> {
        let pause = Arc::new(ProcessStartupPause::new());
        *lock_recover(&self.inner.process_startup_pause) = Some(pause.clone());
        pause
    }

    pub async fn spawn_async<T, F>(
        &self,
        category: TaskCategory,
        name: impl Into<String>,
        future: F,
    ) -> std::result::Result<SupervisedJoinHandle<T>, TaskAdmissionError>
    where
        T: Send + 'static,
        F: std::future::Future<Output = T> + Send + 'static,
    {
        self.spawn_async_with_shutdown_abort(category, name, future, true)
            .await
    }

    async fn spawn_async_with_shutdown_abort<T, F>(
        &self,
        category: TaskCategory,
        name: impl Into<String>,
        future: F,
        abort_on_shutdown_timeout: bool,
    ) -> std::result::Result<SupervisedJoinHandle<T>, TaskAdmissionError>
    where
        T: Send + 'static,
        F: std::future::Future<Output = T> + Send + 'static,
    {
        let permit = self.acquire_permit(category).await?;
        let name = name.into();
        let mut lifecycle = lock_recover(&self.inner.lifecycle);
        if !lifecycle.accepting {
            return Err(TaskAdmissionError::ShuttingDown);
        }
        let task_id = self.inner.next_task_id.fetch_add(1, Ordering::Relaxed);
        let completion = Arc::new(TaskCompletion {
            terminal: AtomicBool::new(false),
            outcome: AtomicU8::new(OUTCOME_PENDING),
        });
        let abort_source = Arc::new(AtomicU8::new(ABORT_NONE));
        let (start_tx, start_rx) = tokio::sync::oneshot::channel::<()>();
        let guard = ManagedTaskGuard {
            supervisor: self.clone(),
            task: ActiveTask {
                id: task_id,
                category,
                name: name.clone(),
            },
            completion: completion.clone(),
            abort_source: abort_source.clone(),
            outcome: TaskOutcome::RuntimeCancelled,
        };
        let handle = tokio::spawn(async move {
            let _ = start_rx.await;
            let mut guard = guard;
            let _permit = permit;
            let mut future = Box::pin(future);
            let outcome = std::future::poll_fn(|context| {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    future.as_mut().poll(context)
                })) {
                    Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
                    Ok(std::task::Poll::Ready(output)) => std::task::Poll::Ready(Ok(output)),
                    Err(payload) => std::task::Poll::Ready(Err(payload)),
                }
            })
            .await;
            match outcome {
                Ok(output) => {
                    guard.outcome = TaskOutcome::Completed;
                    output
                }
                Err(payload) => {
                    guard.outcome = TaskOutcome::Panicked;
                    std::panic::resume_unwind(payload)
                }
            }
        });
        lifecycle.active_tasks.insert(
            task_id,
            ActiveTask {
                id: task_id,
                category,
                name,
            },
        );
        lifecycle.managed_tasks.insert(
            task_id,
            ManagedTaskControl {
                abort_handle: handle.abort_handle(),
                completion,
                abort_source: abort_source.clone(),
                abort_on_shutdown_timeout,
            },
        );
        let _ = start_tx.send(());
        drop(lifecycle);
        Ok(SupervisedJoinHandle {
            inner: handle,
            abort_source,
        })
    }

    pub async fn run_blocking<T, F>(
        &self,
        category: TaskCategory,
        name: impl Into<String>,
        f: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let name: String = name.into();
        let permit = self.acquire_permit(category).await?;
        let task_id = self.start_task(category, name.clone())?;

        // Detach into a task that holds the permit for the true duration
        // of the blocking work. If the caller future is cancelled, the
        // permit remains held until spawn_blocking finishes, preventing
        // over-admission past the category cap.
        let (tx, rx) = tokio::sync::oneshot::channel();
        let supervisor = self.clone();
        tokio::spawn(async move {
            let _guard = BlockingTaskGuard {
                supervisor,
                task_id,
                _permit: permit,
            };
            let result = tokio::task::spawn_blocking(f).await;
            let _ = tx.send(result);
        });

        rx.await
            .map_err(|_| anyhow!("supervised blocking task '{name}' was dropped"))?
            .map_err(|error| anyhow!("supervised blocking task '{name}' panicked: {error}"))
    }

    pub async fn run_blocking_file<T, F>(&self, name: impl Into<String>, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.run_blocking(TaskCategory::File, name, f).await
    }

    /// Run a short-lived command to completion as an asynchronous managed
    /// task. Both output pipes are drained concurrently, and root shutdown
    /// kills and reaps the child before the task registration is released.
    pub async fn run_process_output(
        &self,
        category: TaskCategory,
        name: impl Into<String>,
        command: std::process::Command,
    ) -> std::result::Result<Output, ProcessError> {
        let cancellation = self.root_cancellation_token();
        self.spawn_async_with_shutdown_abort(
            category,
            name,
            async move { run_process_output(cancellation, command).await },
            false,
        )
        .await?
        .join()
        .await?
    }

    /// Spawn a child whose lifecycle remains supervised until it exits, is
    /// killed and reaped, or is explicitly preserved by shutdown policy.
    pub async fn spawn_process(
        &self,
        category: TaskCategory,
        name: impl Into<String>,
        command: std::process::Command,
        shutdown_policy: ProcessShutdownPolicy,
    ) -> std::result::Result<SupervisedChild, ProcessError> {
        let cancellation = self.root_cancellation_token();
        let (control_tx, control_rx) = mpsc::channel(4);
        let (startup_tx, startup_rx) = oneshot::channel();
        let terminal = Arc::new(Mutex::new(None));
        let actor_terminal = terminal.clone();
        #[cfg(test)]
        let startup_pause = lock_recover(&self.inner.process_startup_pause).take();
        let managed = self
            .spawn_async_with_shutdown_abort(
                category,
                name,
                async move {
                    run_process_actor(
                        cancellation,
                        command,
                        shutdown_policy,
                        control_rx,
                        startup_tx,
                        actor_terminal,
                        #[cfg(test)]
                        startup_pause,
                    )
                    .await;
                },
                false,
            )
            .await?;

        match startup_rx.await {
            Ok(Ok(pid)) => {
                drop(managed);
                Ok(SupervisedChild {
                    pid,
                    control: Some(control_tx),
                    terminal,
                })
            }
            Ok(Err(error)) => {
                managed.join().await?;
                Err(error)
            }
            Err(_) => {
                managed.join().await?;
                Err(ProcessError::ControlClosed)
            }
        }
    }

    /// Backend-neutral supervised DB blocking helper.
    ///
    /// Use this for any blocking DB work that doesn't go through the
    /// SQLite-specific `call_db` (which wraps `tokio_rusqlite::Connection::call`).
    /// Examples: redb commits/compaction, SQLite online backup, future backend
    /// snapshots, and large scans that need a blocking boundary.
    ///
    /// Uses the same `TaskCategory::Db` semaphore and observability as
    /// `call_db` so all DB-category work shares one concurrency limit.
    pub async fn run_db_blocking<T, F>(
        &self,
        name: impl Into<String>,
        backend_key: impl Into<String>,
        f: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let name = name.into();
        let _key = backend_key.into(); // reserved for future keyed serialization
        self.run_blocking(TaskCategory::Db, name, f).await
    }

    pub async fn run_blocking_file_keyed<T, F>(
        &self,
        name: impl Into<String>,
        key: impl Into<String>,
        f: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let key_lock = {
            let key = key.into();
            let mut keyed = lock_recover(&self.inner.file_keyed_guards);
            keyed
                .entry(key)
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone()
        };

        let _key_guard = key_lock.lock().await;
        self.run_blocking(TaskCategory::File, name, f).await
    }

    pub async fn call_db<T, F>(
        &self,
        query_name: impl Into<String>,
        connection_key: impl Into<String>,
        connection: tokio_rusqlite::Connection,
        f: F,
    ) -> DbCallResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> DbClosureResult<T> + Send + 'static,
    {
        self.call_db_with_category(TaskCategory::Db, query_name, connection_key, connection, f)
            .await
    }

    pub async fn call_db_read<T, F>(
        &self,
        query_name: impl Into<String>,
        connection_key: impl Into<String>,
        connection: tokio_rusqlite::Connection,
        f: F,
    ) -> DbCallResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> DbClosureResult<T> + Send + 'static,
    {
        self.call_db_with_category(
            TaskCategory::DbRead,
            query_name,
            connection_key,
            connection,
            f,
        )
        .await
    }

    async fn call_db_with_category<T, F>(
        &self,
        category: TaskCategory,
        query_name: impl Into<String>,
        connection_key: impl Into<String>,
        connection: tokio_rusqlite::Connection,
        f: F,
    ) -> DbCallResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> DbClosureResult<T> + Send + 'static,
    {
        let query_name: String = query_name.into();
        let connection_key: String = connection_key.into();
        let permit = self.acquire_permit(category).await.map_err(|e| {
            tokio_rusqlite::Error::Error(DbError::Application(Box::new(std::io::Error::other(
                e.to_string(),
            ))))
        })?;
        let task_id = self
            .start_task(category, query_name.clone())
            .map_err(|error| tokio_rusqlite::Error::Error(DbError::Application(Box::new(error))))?;

        // Detach into a task that holds the DB permit for the true duration
        // of the DB call. If the caller future is cancelled, the permit
        // remains held until connection.call finishes.
        let (tx, rx) = tokio::sync::oneshot::channel();
        let supervisor = self.clone();
        let query_name_for_log = query_name.clone();
        let connection_key_for_log = connection_key.clone();
        let db_logging = self.db_query_logging_status().enabled;
        tokio::spawn(async move {
            let _guard = BlockingTaskGuard {
                supervisor: supervisor.clone(),
                task_id,
                _permit: permit,
            };
            let started = std::time::Instant::now();
            let result = connection.call(f).await;
            if db_logging {
                tracing::info!(
                    target: "klights::task_supervisor::db",
                    query_name = %query_name_for_log,
                    connection_key = %connection_key_for_log,
                    duration_ms = started.elapsed().as_millis() as u64,
                    "db_query"
                );
            }
            let _ = tx.send(result);
        });

        match rx.await {
            Ok(result) => result,
            Err(_) => Err(tokio_rusqlite::Error::Error(DbError::Application(
                Box::new(std::io::Error::other("supervised db task was dropped")),
            ))),
        }
    }

    pub async fn spawn_delay<F>(
        &self,
        name: impl Into<String>,
        delay: std::time::Duration,
        future: F,
    ) -> Result<SupervisedJoinHandle<()>>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let token = self.root_cancellation_token();
        Ok(self
            .spawn_async(TaskCategory::Timer, name, async move {
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {
                        future.await;
                    }
                    _ = token.cancelled() => {
                        // dropped — future never runs
                    }
                }
            })
            .await?)
    }

    pub async fn spawn_interval<F, Fut>(
        &self,
        name: impl Into<String>,
        period: std::time::Duration,
        mut tick: F,
    ) -> Result<SupervisedJoinHandle<()>>
    where
        F: FnMut(u64) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let token = self.root_cancellation_token();
        Ok(self
            .spawn_async(TaskCategory::Timer, name, async move {
                let mut count = 0u64;
                let mut interval = tokio::time::interval(period);
                loop {
                    tokio::select! {
                        _ = token.cancelled() => break,
                        _ = interval.tick() => {
                            tick(count).await;
                            count += 1;
                        }
                    }
                }
            })
            .await?)
    }

    pub async fn sleep(
        &self,
        name: impl Into<String>,
        duration: std::time::Duration,
    ) -> Result<()> {
        let permit = self.acquire_permit(TaskCategory::Timer).await?;
        let task_id = self.start_task(TaskCategory::Timer, name.into())?;
        let _guard = RunningTaskGuard {
            supervisor: self.clone(),
            task_id,
        };
        let token = self.root_cancellation_token();
        tokio::select! {
            _ = tokio::time::sleep(duration) => {}
            _ = token.cancelled() => {}
        }
        drop(permit);
        Ok(())
    }

    pub async fn sleep_until(
        &self,
        name: impl Into<String>,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        let permit = self.acquire_permit(TaskCategory::Timer).await?;
        let task_id = self.start_task(TaskCategory::Timer, name.into())?;
        let _guard = RunningTaskGuard {
            supervisor: self.clone(),
            task_id,
        };
        let token = self.root_cancellation_token();
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {}
            _ = token.cancelled() => {}
        }
        drop(permit);
        Ok(())
    }

    pub async fn timeout<F>(
        &self,
        name: impl Into<String>,
        duration: std::time::Duration,
        future: F,
    ) -> Result<std::result::Result<F::Output, tokio::time::error::Elapsed>>
    where
        F: std::future::Future,
    {
        let permit = self.acquire_permit(TaskCategory::Timer).await?;
        let task_id = self.start_task(TaskCategory::Timer, name.into())?;
        let _guard = RunningTaskGuard {
            supervisor: self.clone(),
            task_id,
        };
        let token = self.root_cancellation_token();
        let result = tokio::select! {
            result = tokio::time::timeout(duration, future) => result,
            _ = token.cancelled() => {
                return Err(anyhow!("supervised timeout cancelled by root shutdown"));
            }
        };
        drop(permit);
        Ok(result)
    }

    pub async fn shutdown(&self, timeout: std::time::Duration) -> ShutdownReport {
        let managed = {
            let mut lifecycle = lock_recover(&self.inner.lifecycle);
            lifecycle.accepting = false;
            lifecycle
                .managed_tasks
                .values()
                .cloned()
                .collect::<Vec<_>>()
        };
        self.inner.root_cancellation.cancel();

        let total_managed = managed.len();
        let mut pending = managed;
        let mut joined = 0usize;

        let shutdown_deadline = tokio::time::Instant::now() + timeout;
        self.wait_for_managed_tasks(&mut pending, shutdown_deadline, Some(&mut joined))
            .await;

        // A completion at the graceful deadline wins over an abort request.
        Self::retain_pending(&mut pending, Some(&mut joined));
        let timed_out = !pending.is_empty();
        let mut aborted = 0usize;
        let mut abort_requests = Vec::new();
        if timed_out {
            for control in &pending {
                if !control.abort_on_shutdown_timeout {
                    continue;
                }
                control
                    .abort_source
                    .compare_exchange(
                        ABORT_NONE,
                        ABORT_SHUTDOWN,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .ok();
                control.abort_handle.abort();
                aborted += 1;
                abort_requests.push(control.clone());
            }
            let abort_deadline =
                tokio::time::Instant::now() + std::time::Duration::from_millis(100);
            self.wait_for_managed_tasks(&mut pending, abort_deadline, None)
                .await;
        }

        let remaining_active = self.active_tasks(None).len();
        let abort_confirmed = abort_requests
            .iter()
            .filter(|control| {
                matches!(
                    decode_outcome(control.completion.outcome.load(Ordering::Acquire)),
                    Some(TaskOutcome::CallerAborted | TaskOutcome::ShutdownAborted)
                )
            })
            .count();
        ShutdownReport {
            total_managed,
            joined,
            aborted,
            abort_confirmed,
            timed_out,
            remaining_active,
        }
    }

    async fn wait_for_managed_tasks(
        &self,
        pending: &mut Vec<ManagedTaskControl>,
        deadline: tokio::time::Instant,
        mut joined: Option<&mut usize>,
    ) {
        loop {
            // Register before inspecting the atomic flags so completion cannot
            // race between the scan and the await. One notification is enough:
            // every wake rescans all controls and removes every completed task.
            let completion = self.inner.managed_task_change.notified();
            tokio::pin!(completion);
            completion.as_mut().enable();

            Self::retain_pending(pending, joined.as_deref_mut());
            if pending.is_empty() || tokio::time::Instant::now() >= deadline {
                return;
            }

            tokio::select! {
                _ = &mut completion => {}
                _ = tokio::time::sleep_until(deadline) => {
                    Self::retain_pending(pending, joined.as_deref_mut());
                    return;
                },
            }
        }
    }

    fn retain_pending(pending: &mut Vec<ManagedTaskControl>, mut joined: Option<&mut usize>) {
        pending.retain(|control| {
            if !control.completion.terminal.load(Ordering::Acquire) {
                return true;
            }
            if decode_outcome(control.completion.outcome.load(Ordering::Acquire))
                == Some(TaskOutcome::Completed)
                && let Some(count) = joined.as_deref_mut()
            {
                *count += 1;
            }
            false
        });
    }

    fn start_task(
        &self,
        category: TaskCategory,
        name: String,
    ) -> std::result::Result<u64, TaskAdmissionError> {
        let mut lifecycle = lock_recover(&self.inner.lifecycle);
        if !lifecycle.accepting {
            return Err(TaskAdmissionError::ShuttingDown);
        }
        let id = self.inner.next_task_id.fetch_add(1, Ordering::Relaxed);
        let active = ActiveTask { id, category, name };
        lifecycle.active_tasks.insert(id, active);
        Ok(id)
    }

    fn finish_task(&self, task_id: u64) {
        lock_recover(&self.inner.lifecycle)
            .active_tasks
            .remove(&task_id);
    }

    async fn acquire_permit(
        &self,
        category: TaskCategory,
    ) -> std::result::Result<Option<CategoryPermit>, TaskAdmissionError> {
        if !lock_recover(&self.inner.lifecycle).accepting {
            return Err(TaskAdmissionError::ShuttingDown);
        }
        let Some(semaphore) = self.inner.semaphores.get(&category).cloned() else {
            return Ok(None);
        };

        // Cancel-safe queued accounting: the guard decrements the `queued`
        // gauge on drop, so a caller cancelled *while still waiting* for the
        // permit (the `.acquire_owned().await` below is a cancellation point)
        // cannot leak the counter. Without this, a dropped API request future
        // that was parked here leaves `queued` stuck high forever (observed as
        // steady `db queued>0, active=0` on the live leader).
        let queued_guard = QueuedGuard::new(self.clone(), category);
        let cancellation = self.inner.root_cancellation.clone();
        let permit = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(TaskAdmissionError::ShuttingDown),
            permit = semaphore.acquire_owned() => {
                permit.map_err(|_| TaskAdmissionError::CategoryClosed(category))?
            }
        };
        drop(queued_guard);
        if !lock_recover(&self.inner.lifecycle).accepting {
            drop(permit);
            return Err(TaskAdmissionError::ShuttingDown);
        }
        Ok(Some(CategoryPermit {
            permit: Some(permit),
            notify: self.inner.category_free_notifies.get(&category).cloned(),
        }))
    }

    fn bump_queued(&self, category: TaskCategory, delta: isize) {
        let mut queued = lock_recover(&self.inner.queued_by_category);
        let entry = queued.entry(category).or_insert(0);
        if delta.is_negative() {
            *entry = entry.saturating_sub(delta.unsigned_abs());
            return;
        }
        *entry += delta.unsigned_abs();
    }
}

/// RAII guard that bumps the `queued` gauge on construction and decrements it
/// on drop, so the counter is decremented even if the awaiting caller future
/// is cancelled before it acquires the permit.
struct QueuedGuard {
    supervisor: TaskSupervisor,
    category: TaskCategory,
}

impl QueuedGuard {
    fn new(supervisor: TaskSupervisor, category: TaskCategory) -> Self {
        supervisor.bump_queued(category, 1);
        Self {
            supervisor,
            category,
        }
    }
}

impl Drop for QueuedGuard {
    fn drop(&mut self) {
        self.supervisor.bump_queued(self.category, -1);
    }
}

struct CategoryPermit {
    permit: Option<OwnedSemaphorePermit>,
    notify: Option<Arc<Notify>>,
}

impl Drop for CategoryPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        if let Some(notify) = &self.notify {
            notify.notify_one();
        }
    }
}

struct RunningTaskGuard {
    supervisor: TaskSupervisor,
    task_id: u64,
}

impl Drop for RunningTaskGuard {
    fn drop(&mut self) {
        self.supervisor.finish_task(self.task_id);
    }
}

/// RAII guard for `run_blocking` / `call_db` / `run_db_blocking`.
///
/// Holds both the category semaphore permit and the active-task entry so that
/// cancellation (dropping the calling future) releases the permit and removes
/// the task from `active_tasks` in one atomic drop. Without this guard the old
/// code would leak the active-task row and release the permit while the
/// blocking work was still in flight, which could skew admission accounting.
struct BlockingTaskGuard {
    supervisor: TaskSupervisor,
    task_id: u64,
    _permit: Option<CategoryPermit>,
}

impl Drop for BlockingTaskGuard {
    fn drop(&mut self) {
        self.supervisor.finish_task(self.task_id);
        // `_permit` drops here, releasing the semaphore slot and notifying
        // any waiter on `category_free_notify`.
    }
}

/// Sole finalizer for `spawn_async` tasks. Replaces both `RunningTaskGuard`
/// (active_tasks cleanup) and the post-`future.await` `done.store(true)` line
/// from the previous design, so panicked / aborted tasks also clean up
/// correctly. Drops in the spawned task's stack, so unwinding from a panic
/// runs all three cleanups.
struct ManagedTaskGuard {
    supervisor: TaskSupervisor,
    task: ActiveTask,
    completion: Arc<TaskCompletion>,
    abort_source: Arc<AtomicU8>,
    outcome: TaskOutcome,
}

impl Drop for ManagedTaskGuard {
    fn drop(&mut self) {
        let outcome = if self.outcome == TaskOutcome::Panicked || std::thread::panicking() {
            TaskOutcome::Panicked
        } else if self.outcome == TaskOutcome::Completed {
            TaskOutcome::Completed
        } else {
            match self.abort_source.load(Ordering::SeqCst) {
                ABORT_CALLER => TaskOutcome::CallerAborted,
                ABORT_SHUTDOWN => TaskOutcome::ShutdownAborted,
                _ => TaskOutcome::RuntimeCancelled,
            }
        };
        #[cfg(test)]
        if let Some(pause) = lock_recover(&self.supervisor.inner.terminal_cleanup_pause).take() {
            pause.block_cleanup();
        }
        let mut lifecycle = lock_recover(&self.supervisor.inner.lifecycle);
        lifecycle.active_tasks.remove(&self.task.id);
        lifecycle.managed_tasks.remove(&self.task.id);
        if lifecycle.recent_outcomes.len() == OUTCOME_HISTORY_LIMIT {
            lifecycle.recent_outcomes.pop_front();
        }
        lifecycle.recent_outcomes.push_back(TaskOutcomeStatus {
            id: self.task.id,
            category: self.task.category,
            name: self.task.name.clone(),
            outcome,
        });
        drop(lifecycle);
        self.completion
            .outcome
            .store(encode_outcome(outcome), Ordering::Release);
        self.completion.terminal.store(true, Ordering::Release);
        self.supervisor.inner.managed_task_change.notify_one();
    }
}

#[cfg(test)]
pub(crate) struct TerminalCleanupPause {
    entered: AtomicBool,
    entered_notify: Notify,
    released: Mutex<bool>,
    release_condvar: std::sync::Condvar,
}

#[cfg(test)]
impl TerminalCleanupPause {
    fn new() -> Self {
        Self {
            entered: AtomicBool::new(false),
            entered_notify: Notify::new(),
            released: Mutex::new(false),
            release_condvar: std::sync::Condvar::new(),
        }
    }

    fn block_cleanup(&self) {
        self.entered.store(true, Ordering::Release);
        self.entered_notify.notify_one();
        let mut released = lock_recover(&self.released);
        while !*released {
            released = match self.release_condvar.wait(released) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }

    pub(crate) async fn wait_until_entered(&self) {
        let entered = self.entered_notify.notified();
        tokio::pin!(entered);
        entered.as_mut().enable();
        if !self.entered.load(Ordering::Acquire) {
            entered.await;
        }
    }

    pub(crate) fn release(&self) {
        *lock_recover(&self.released) = true;
        self.release_condvar.notify_one();
    }
}

#[cfg(test)]
pub(crate) struct ProcessStartupPause {
    pid: AtomicU64,
    entered: AtomicBool,
    entered_notify: Notify,
    released: AtomicBool,
    release_notify: Notify,
}

#[cfg(test)]
impl ProcessStartupPause {
    fn new() -> Self {
        Self {
            pid: AtomicU64::new(0),
            entered: AtomicBool::new(false),
            entered_notify: Notify::new(),
            released: AtomicBool::new(false),
            release_notify: Notify::new(),
        }
    }

    async fn pause(&self, pid: u32) {
        self.pid.store(u64::from(pid), Ordering::Release);
        self.entered.store(true, Ordering::Release);
        self.entered_notify.notify_one();
        let released = self.release_notify.notified();
        tokio::pin!(released);
        released.as_mut().enable();
        if !self.released.load(Ordering::Acquire) {
            released.await;
        }
    }

    pub(crate) async fn wait_until_entered(&self) {
        let entered = self.entered_notify.notified();
        tokio::pin!(entered);
        entered.as_mut().enable();
        if !self.entered.load(Ordering::Acquire) {
            entered.await;
        }
    }

    pub(crate) fn pid(&self) -> Option<u32> {
        u32::try_from(self.pid.load(Ordering::Acquire))
            .ok()
            .filter(|pid| *pid != 0)
    }

    pub(crate) fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.release_notify.notify_one();
    }
}

#[derive(Debug)]
pub struct SupervisedJoinHandle<T> {
    inner: tokio::task::JoinHandle<T>,
    abort_source: Arc<AtomicU8>,
}

impl<T> SupervisedJoinHandle<T> {
    pub fn abort(&self) {
        self.abort_source
            .compare_exchange(ABORT_NONE, ABORT_CALLER, Ordering::SeqCst, Ordering::SeqCst)
            .ok();
        self.inner.abort();
    }

    pub async fn join(self) -> std::result::Result<T, TaskJoinError> {
        self.inner.await.map_err(TaskJoinError::new)
    }
}

async fn run_process_output(
    cancellation: CancellationToken,
    command: std::process::Command,
) -> std::result::Result<Output, ProcessError> {
    if cancellation.is_cancelled() {
        return Err(ProcessError::Admission(TaskAdmissionError::ShuttingDown));
    }
    let mut command = tokio::process::Command::from(command);
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().map_err(ProcessError::Spawn)?;
    let mut stdout = child.stdout.take().ok_or_else(|| ProcessError::Io {
        operation: "capture stdout",
        source: std::io::Error::other("spawned process has no stdout pipe"),
    })?;
    let mut stderr = child.stderr.take().ok_or_else(|| ProcessError::Io {
        operation: "capture stderr",
        source: std::io::Error::other("spawned process has no stderr pipe"),
    })?;
    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();

    enum Completion {
        Finished(
            std::io::Result<ExitStatus>,
            std::io::Result<usize>,
            std::io::Result<usize>,
        ),
        Cancelled,
    }

    let completion = {
        let completion = async {
            tokio::join!(
                child.wait(),
                stdout.read_to_end(&mut stdout_bytes),
                stderr.read_to_end(&mut stderr_bytes)
            )
        };
        tokio::pin!(completion);
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Completion::Cancelled,
            result = &mut completion => Completion::Finished(result.0, result.1, result.2),
        }
    };

    match completion {
        Completion::Finished(status, stdout_result, stderr_result) => {
            let status = status.map_err(|source| ProcessError::Io {
                operation: "wait",
                source,
            })?;
            stdout_result.map_err(|source| ProcessError::Io {
                operation: "read stdout",
                source,
            })?;
            stderr_result.map_err(|source| ProcessError::Io {
                operation: "read stderr",
                source,
            })?;
            Ok(Output {
                status,
                stdout: stdout_bytes,
                stderr: stderr_bytes,
            })
        }
        Completion::Cancelled => {
            let kill_error = child.start_kill().err();
            let (status, stdout_result, stderr_result) = tokio::join!(
                child.wait(),
                stdout.read_to_end(&mut stdout_bytes),
                stderr.read_to_end(&mut stderr_bytes)
            );
            if let Err(source) = status {
                return Err(ProcessError::Io {
                    operation: if kill_error.is_some() {
                        "kill and reap during shutdown"
                    } else {
                        "reap during shutdown"
                    },
                    source: kill_error.unwrap_or(source),
                });
            }
            stdout_result.map_err(|source| ProcessError::Io {
                operation: "drain stdout during shutdown",
                source,
            })?;
            stderr_result.map_err(|source| ProcessError::Io {
                operation: "drain stderr during shutdown",
                source,
            })?;
            Err(ProcessError::Cancelled)
        }
    }
}

async fn run_process_actor(
    cancellation: CancellationToken,
    command: std::process::Command,
    shutdown_policy: ProcessShutdownPolicy,
    mut control_rx: mpsc::Receiver<ProcessCommand>,
    startup_tx: oneshot::Sender<std::result::Result<u32, ProcessError>>,
    terminal_state: Arc<Mutex<Option<ProcessTerminal>>>,
    #[cfg(test)] startup_pause: Option<Arc<ProcessStartupPause>>,
) {
    if cancellation.is_cancelled() {
        let _ = startup_tx.send(Err(ProcessError::Admission(
            TaskAdmissionError::ShuttingDown,
        )));
        return;
    }

    let mut command = tokio::process::Command::from(command);
    command.kill_on_drop(shutdown_policy == ProcessShutdownPolicy::KillAndReap);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            let _ = startup_tx.send(Err(ProcessError::Spawn(error)));
            return;
        }
    };
    let pid = child.id().expect("a freshly spawned child must have a pid");
    #[cfg(test)]
    if let Some(pause) = startup_pause {
        pause.pause(pid).await;
    }
    if startup_tx.send(Ok(pid)).is_err() {
        finish_process_child(
            &mut child,
            ProcessShutdownPolicy::KillAndReap,
            &terminal_state,
        )
        .await;
        return;
    }

    let mut waiters = Vec::new();
    loop {
        enum Event {
            Shutdown,
            Command(Option<ProcessCommand>),
            Exited(std::io::Result<ExitStatus>),
        }

        let event = {
            let wait = child.wait();
            tokio::pin!(wait);
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Event::Shutdown,
                command = control_rx.recv() => Event::Command(command),
                status = &mut wait => Event::Exited(status),
            }
        };

        let terminal = match event {
            Event::Shutdown => {
                finish_process_child(&mut child, shutdown_policy, &terminal_state).await
            }
            Event::Command(None) => {
                finish_process_child(
                    &mut child,
                    ProcessShutdownPolicy::KillAndReap,
                    &terminal_state,
                )
                .await
            }
            Event::Command(Some(ProcessCommand::Wait(reply))) => {
                waiters.push(reply);
                continue;
            }
            Event::Command(Some(ProcessCommand::Kill(reply))) => {
                let terminal = kill_and_reap_child(&mut child).await;
                let _ = reply.send(terminal.wait_result().map(|_| ()));
                set_process_terminal(&terminal_state, terminal.clone());
                terminal
            }
            Event::Exited(status) => {
                let terminal = terminal_from_wait("wait", status);
                set_process_terminal(&terminal_state, terminal.clone());
                terminal
            }
        };

        for waiter in waiters {
            let _ = waiter.send(terminal.wait_result());
        }
        return;
    }
}

async fn finish_process_child(
    child: &mut tokio::process::Child,
    shutdown_policy: ProcessShutdownPolicy,
    terminal_state: &Mutex<Option<ProcessTerminal>>,
) -> ProcessTerminal {
    let terminal = match child.try_wait() {
        Ok(Some(status)) => ProcessTerminal::Exited(status),
        Ok(None) if shutdown_policy == ProcessShutdownPolicy::Preserve => {
            ProcessTerminal::Preserved
        }
        Ok(None) => kill_and_reap_child(child).await,
        Err(error) => stored_process_io("inspect before shutdown", error),
    };
    set_process_terminal(terminal_state, terminal.clone());
    terminal
}

async fn kill_and_reap_child(child: &mut tokio::process::Child) -> ProcessTerminal {
    let kill_error = child.start_kill().err();
    match child.wait().await {
        Ok(status) => ProcessTerminal::Exited(status),
        Err(wait_error) => stored_process_io(
            if kill_error.is_some() {
                "kill and reap"
            } else {
                "reap"
            },
            kill_error.unwrap_or(wait_error),
        ),
    }
}

fn terminal_from_wait(
    operation: &'static str,
    result: std::io::Result<ExitStatus>,
) -> ProcessTerminal {
    match result {
        Ok(status) => ProcessTerminal::Exited(status),
        Err(error) => stored_process_io(operation, error),
    }
}

fn stored_process_io(operation: &'static str, error: std::io::Error) -> ProcessTerminal {
    ProcessTerminal::Io {
        operation,
        kind: error.kind(),
        message: error.to_string(),
    }
}

fn set_process_terminal(state: &Mutex<Option<ProcessTerminal>>, terminal: ProcessTerminal) {
    *lock_recover(state) = Some(terminal);
}

fn encode_outcome(outcome: TaskOutcome) -> u8 {
    match outcome {
        TaskOutcome::Completed => 1,
        TaskOutcome::Panicked => 2,
        TaskOutcome::CallerAborted => 3,
        TaskOutcome::ShutdownAborted => 4,
        TaskOutcome::RuntimeCancelled => 5,
    }
}

fn decode_outcome(value: u8) -> Option<TaskOutcome> {
    match value {
        1 => Some(TaskOutcome::Completed),
        2 => Some(TaskOutcome::Panicked),
        3 => Some(TaskOutcome::CallerAborted),
        4 => Some(TaskOutcome::ShutdownAborted),
        5 => Some(TaskOutcome::RuntimeCancelled),
        _ => None,
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
