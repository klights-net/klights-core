use super::category::{TaskCategory, TaskCategoryConfig};
use super::task::{ActiveTaskStatus, DbQueryLoggingStatus, ShutdownReport, TaskCategoryStatus};
use crate::task_supervisor::task::ActiveTask;
use crate::utils::lock_recover;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct TaskSupervisor {
    inner: Arc<TaskSupervisorInner>,
}

struct TaskSupervisorInner {
    config: TaskCategoryConfig,
    next_task_id: AtomicU64,
    root_cancellation: CancellationToken,
    db_query_logging_enabled: AtomicBool,
    active_tasks: Mutex<HashMap<u64, ActiveTask>>,
    managed_tasks: Mutex<HashMap<u64, ManagedTaskControl>>,
    db_query_logs: Mutex<Vec<DbQueryLogEntry>>,
    queued_by_category: Mutex<HashMap<TaskCategory, usize>>,
    file_keyed_guards: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    semaphores: HashMap<TaskCategory, Arc<Semaphore>>,
    category_free_notifies: HashMap<TaskCategory, Arc<Notify>>,
}

struct ManagedTaskControl {
    abort_handle: tokio::task::AbortHandle,
    done: Arc<AtomicBool>,
}

#[derive(Clone, serde::Serialize)]
pub struct DbQueryLogEntry {
    pub query_name: String,
    pub connection_key: String,
    pub duration_ms: u64,
}

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
                active_tasks: Mutex::new(HashMap::new()),
                managed_tasks: Mutex::new(HashMap::new()),
                db_query_logs: Mutex::new(Vec::new()),
                queued_by_category: Mutex::new(HashMap::new()),
                file_keyed_guards: Mutex::new(HashMap::new()),
                semaphores,
                category_free_notifies,
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
        let active = lock_recover(&self.inner.active_tasks);
        let queued = lock_recover(&self.inner.queued_by_category);
        let mut active_by_category = HashMap::<TaskCategory, usize>::new();
        for task in active.values() {
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
        let active = lock_recover(&self.inner.active_tasks);
        let mut rows: Vec<ActiveTaskStatus> = active
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

    pub fn managed_task_count(&self) -> usize {
        lock_recover(&self.inner.managed_tasks).len()
    }

    pub fn db_query_logs_for_test(&self) -> Vec<DbQueryLogEntry> {
        lock_recover(&self.inner.db_query_logs).clone()
    }

    pub fn start_task_for_test(
        &self,
        category: TaskCategory,
        name: impl Into<String>,
    ) -> SupervisedTaskGuard {
        let id = self.start_task(category, name.into());
        SupervisedTaskGuard {
            supervisor: self.clone(),
            task_id: id,
        }
    }

    pub async fn spawn_async<T, F>(
        &self,
        category: TaskCategory,
        name: impl Into<String>,
        future: F,
    ) -> Result<SupervisedJoinHandle<T>>
    where
        T: Send + 'static,
        F: std::future::Future<Output = T> + Send + 'static,
    {
        let permit = self.acquire_permit(category).await?;
        let task_id = self.start_task(category, name.into());
        let done_flag = Arc::new(AtomicBool::new(false));
        let guard = ManagedTaskGuard {
            supervisor: self.clone(),
            task_id,
            done: done_flag.clone(),
        };
        let handle = tokio::spawn(async move {
            // Sole owner of finalization. Drops on normal return, panic, or abort.
            let _guard = guard;
            let _permit = permit;
            future.await
        });
        {
            let mut managed = lock_recover(&self.inner.managed_tasks);
            managed.insert(
                task_id,
                ManagedTaskControl {
                    abort_handle: handle.abort_handle(),
                    done: done_flag.clone(),
                },
            );
            // Race: a very fast task may have completed (and its drop may have
            // tried to remove an entry that did not yet exist) before we got
            // the lock. Detect and clean up.
            if done_flag.load(Ordering::SeqCst) {
                managed.remove(&task_id);
            }
        }
        Ok(SupervisedJoinHandle { inner: handle })
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
        let task_id = self.start_task(category, name.clone());

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
    ) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<T> + Send + 'static,
    {
        let query_name: String = query_name.into();
        let connection_key: String = connection_key.into();
        let permit = self.acquire_permit(TaskCategory::Db).await.map_err(|e| {
            tokio_rusqlite::Error::Other(Box::new(std::io::Error::other(e.to_string())))
        })?;
        let task_id = self.start_task(TaskCategory::Db, query_name.clone());

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
                let entry = DbQueryLogEntry {
                    query_name: query_name_for_log,
                    connection_key: connection_key_for_log,
                    duration_ms: started.elapsed().as_millis() as u64,
                };
                tracing::info!(
                    target: "klights::task_supervisor::db",
                    "db_query query_name={} connection_key={} duration_ms={}",
                    entry.query_name,
                    entry.connection_key,
                    entry.duration_ms
                );
                lock_recover(&supervisor.inner.db_query_logs).push(entry);
            }
            let _ = tx.send(result);
        });

        rx.await.map_err(|_| {
            tokio_rusqlite::Error::Other(Box::new(std::io::Error::other(
                "supervised db task was dropped",
            )))
        })?
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
        self.spawn_async(TaskCategory::Timer, name, async move {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {
                    future.await;
                }
                _ = token.cancelled() => {
                    // dropped — future never runs
                }
            }
        })
        .await
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
        self.spawn_async(TaskCategory::Timer, name, async move {
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
        .await
    }

    pub async fn sleep(
        &self,
        name: impl Into<String>,
        duration: std::time::Duration,
    ) -> Result<()> {
        let permit = self.acquire_permit(TaskCategory::Timer).await?;
        let task_id = self.start_task(TaskCategory::Timer, name.into());
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
        let task_id = self.start_task(TaskCategory::Timer, name.into());
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
        let task_id = self.start_task(TaskCategory::Timer, name.into());
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
        self.inner.root_cancellation.cancel();

        let managed = {
            let mut managed = lock_recover(&self.inner.managed_tasks);
            std::mem::take(&mut *managed)
        };

        let total_managed = managed.len();
        let mut pending = Vec::new();
        let mut joined = 0usize;
        for control in managed.into_values() {
            if control.done.load(Ordering::SeqCst) {
                joined += 1;
            } else {
                pending.push(control);
            }
        }

        let start = tokio::time::Instant::now();
        while !pending.is_empty() && start.elapsed() < timeout {
            pending.retain(|control| {
                if control.done.load(Ordering::SeqCst) {
                    joined += 1;
                    return false;
                }
                true
            });
            if pending.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let timed_out = !pending.is_empty();
        let mut aborted = 0usize;
        if timed_out {
            for control in &pending {
                control.abort_handle.abort();
                aborted += 1;
            }
            for _ in 0..10 {
                if pending
                    .iter()
                    .all(|control| control.done.load(Ordering::SeqCst))
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }

        let remaining_active = self.active_tasks(None).len();
        ShutdownReport {
            total_managed,
            joined,
            aborted,
            timed_out,
            remaining_active,
        }
    }

    fn start_task(&self, category: TaskCategory, name: String) -> u64 {
        let id = self.inner.next_task_id.fetch_add(1, Ordering::Relaxed);
        let active = ActiveTask { id, category, name };
        lock_recover(&self.inner.active_tasks).insert(id, active);
        id
    }

    fn finish_task(&self, task_id: u64) {
        lock_recover(&self.inner.active_tasks).remove(&task_id);
    }

    async fn acquire_permit(&self, category: TaskCategory) -> Result<Option<CategoryPermit>> {
        let Some(semaphore) = self.inner.semaphores.get(&category).cloned() else {
            return Ok(None);
        };

        self.bump_queued(category, 1);
        let permit = semaphore
            .acquire_owned()
            .await
            .map_err(|error| anyhow!("task category semaphore closed: {error}"))?;
        self.bump_queued(category, -1);
        Ok(Some(CategoryPermit {
            _permit: permit,
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

pub struct SupervisedTaskGuard {
    supervisor: TaskSupervisor,
    task_id: u64,
}

struct CategoryPermit {
    _permit: OwnedSemaphorePermit,
    notify: Option<Arc<Notify>>,
}

impl Drop for CategoryPermit {
    fn drop(&mut self) {
        if let Some(notify) = &self.notify {
            notify.notify_one();
        }
    }
}

impl Drop for SupervisedTaskGuard {
    fn drop(&mut self) {
        self.supervisor.finish_task(self.task_id);
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
    task_id: u64,
    done: Arc<AtomicBool>,
}

impl Drop for ManagedTaskGuard {
    fn drop(&mut self) {
        // Order matters: set `done` first so a concurrent `shutdown()` that
        // already took the managed_tasks map sees this entry as joined rather
        // than aborting it.
        self.done.store(true, Ordering::SeqCst);
        self.supervisor.finish_task(self.task_id);
        lock_recover(&self.supervisor.inner.managed_tasks).remove(&self.task_id);
    }
}

pub struct SupervisedJoinHandle<T> {
    inner: tokio::task::JoinHandle<T>,
}

impl<T> SupervisedJoinHandle<T> {
    pub fn abort(&self) {
        self.inner.abort();
    }

    pub async fn join(self) -> std::result::Result<T, tokio::task::JoinError> {
        self.inner.await
    }
}
