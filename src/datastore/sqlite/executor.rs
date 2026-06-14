use std::cell::Cell;
use std::sync::Arc;

#[cfg(test)]
use crate::task_supervisor::TaskCategoryConfig;
use crate::task_supervisor::TaskSupervisor;
use thiserror::Error;
use tokio_rusqlite::Connection;

use super::opener::{
    self, OpenOpts, OpenPath, apply_pragmas, check_db_health_for as check_db_health_for_executor,
    ensure_root_only, init_schema_for as init_schema_for_executor,
};
use crate::datastore::errors::OpenError;

/// Allow `OpenError` to convert to `tokio_rusqlite::Error` for use in
/// the supervised DB call path.  This is the SQLite-specific error bridge;
/// other backends bring their own conversion.
impl From<OpenError> for tokio_rusqlite::Error {
    fn from(err: OpenError) -> Self {
        tokio_rusqlite::Error::Other(Box::new(err))
    }
}

thread_local! {
    static DB_CALL_DEPTH: Cell<usize> = const { Cell::new(0) };
}

#[derive(Clone)]
pub struct DbExecutor {
    inner: Arc<DbExecutorInner>,
}

struct DbExecutorInner {
    connection: Connection,
    task_supervisor: Arc<TaskSupervisor>,
    connection_key: String,
}

#[derive(Debug, Error)]
pub enum DbError {
    #[error("reentrant db call rejected before enqueue: query_name={query_name}")]
    ReentrantCall { query_name: String },
}

pub struct DbCallGuard;

impl DbCallGuard {
    fn enter() -> Self {
        DB_CALL_DEPTH.with(|depth| depth.set(depth.get() + 1));
        Self
    }
}

impl Drop for DbCallGuard {
    fn drop(&mut self) {
        DB_CALL_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

impl DbExecutor {
    pub fn new(
        connection: Connection,
        task_supervisor: Arc<TaskSupervisor>,
        connection_key: impl Into<String>,
    ) -> Self {
        Self {
            inner: Arc::new(DbExecutorInner {
                connection,
                task_supervisor,
                connection_key: connection_key.into(),
            }),
        }
    }

    /// Open a connection through the centralized `OpenOpts` path.
    ///
    /// For `Disk(path)`: hardens parent dir to `0700`, opens the
    /// connection, applies the PRAGMA profile, initializes the schema,
    /// and runs corruption/fingerprint checks inside a supervised
    /// closure.
    ///
    /// For `InMemory`: just opens and applies PRAGMAs, then initializes
    /// schema and runs health checks.
    pub async fn open_with_opts(
        opts: OpenOpts,
        task_supervisor: Arc<TaskSupervisor>,
        connection_key: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let db_path = match &opts.path {
            OpenPath::InMemory => None,
            OpenPath::Disk(p) => Some(p.clone()),
        };

        let connection = match &opts.path {
            OpenPath::InMemory => Connection::open_in_memory().await?,
            OpenPath::Disk(path) => {
                // Detect orphaned WAL before SQLite silently creates a new DB.
                opener::check_orphaned_wal(path)?;
                ensure_root_only(&task_supervisor, path, opts.allow_existing_perms).await?;
                Connection::open(path).await?
            }
        };
        let executor = Self::new(connection, task_supervisor.clone(), connection_key);
        let profile = opts.profile;
        let schema_kind = opts.schema;
        // Build a display path for error messages: real path for disk DBs,
        // "<in-memory>" for transient connections.
        let db_display = db_path
            .as_deref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "<in-memory>".to_string());

        // Read SQLCipher key if present (DSB-06).
        #[cfg(feature = "sqlcipher")]
        let sqlcipher_key: Option<Vec<u8>> = match &opts.key_source {
            Some(opener::KeySource::File(path)) => {
                Some(opener::read_key_file(&task_supervisor, path).await?)
            }
            _ => None,
        };
        #[cfg(not(feature = "sqlcipher"))]
        let _sqlcipher_key: () = ();

        executor
            .call_raw("opener:apply_pragmas_and_init", move |conn| {
                // Apply SQLCipher key first, before any PRAGMA reads
                #[cfg(feature = "sqlcipher")]
                if let Some(ref key) = sqlcipher_key {
                    conn.pragma_update(None, "key", &key[..])?;
                }
                apply_pragmas(conn, profile)?;
                init_schema_for_executor(conn, schema_kind)?;
                // Run integrity + fingerprint checks for ALL database types.
                // In-memory DBs get the same checks so bugs in the fingerprint
                // path are caught early in development.
                let db_path = std::path::Path::new(&db_display);
                check_db_health_for_executor(conn, db_path, schema_kind)?;
                Ok(())
            })
            .await?;

        if let OpenPath::Disk(path) = &opts.path {
            // Re-tighten now that WAL/SHM may exist after first writes.
            ensure_root_only(&task_supervisor, path, opts.allow_existing_perms).await?;
        }
        Ok(executor)
    }

    pub fn task_supervisor(&self) -> Arc<TaskSupervisor> {
        self.inner.task_supervisor.clone()
    }

    pub async fn open_in_memory(
        task_supervisor: Arc<TaskSupervisor>,
        connection_key: impl Into<String>,
    ) -> Result<Self, tokio_rusqlite::Error> {
        Self::open_with_opts(OpenOpts::in_memory(), task_supervisor, connection_key)
            .await
            .map_err(|e| {
                tokio_rusqlite::Error::Other(Box::new(std::io::Error::other(e.to_string())))
            })
    }

    /// Test-only convenience that creates a private TaskSupervisor — fragments
    /// observability and shutdown, so production callers must thread the
    /// app-owned supervisor explicitly via `open_in_memory(...)`.
    #[cfg(test)]
    pub async fn open_in_memory_with_default_supervisor(
        connection_key: impl Into<String>,
    ) -> Result<Self, tokio_rusqlite::Error> {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        Self::open_in_memory(supervisor, connection_key).await
    }

    pub async fn call_raw<T, F>(&self, query_name: &'static str, f: F) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<T> + Send + 'static,
    {
        if DB_CALL_DEPTH.with(|depth| depth.get() > 0) {
            return Err(tokio_rusqlite::Error::Other(Box::new(
                DbError::ReentrantCall {
                    query_name: query_name.to_string(),
                },
            )));
        }

        let connection_key = self.inner.connection_key.clone();
        let connection = self.inner.connection.clone();
        self.inner
            .task_supervisor
            .call_db(query_name, connection_key, connection, move |conn| {
                let _guard = DbCallGuard::enter();
                f(conn)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::{DbError, DbExecutor};
    use crate::task_supervisor::{TaskCategory, TaskCategoryConfig, TaskSupervisor};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    fn wait_on_gate(gate: &(Mutex<usize>, Condvar)) {
        let (lock, cond) = gate;
        let mut permits = lock.lock().unwrap();
        while *permits == 0 {
            permits = cond.wait(permits).unwrap();
        }
        *permits -= 1;
    }

    fn release_gate(gate: &(Mutex<usize>, Condvar), n: usize) {
        let (lock, cond) = gate;
        let mut permits = lock.lock().unwrap();
        *permits += n;
        cond.notify_all();
    }

    fn category_status(supervisor: &TaskSupervisor, category: TaskCategory) -> usize {
        supervisor
            .category_statuses()
            .into_iter()
            .find(|row| row.category == category)
            .map(|row| row.queued)
            .unwrap_or_default()
    }

    fn assert_reentrant_db_error(err: &tokio_rusqlite::Error, expected_query_name: &str) {
        let tokio_rusqlite::Error::Other(inner) = err else {
            panic!("expected tokio_rusqlite::Error::Other(DbError), got {err}");
        };
        let Some(DbError::ReentrantCall { query_name }) = inner.downcast_ref::<DbError>() else {
            panic!("expected DbError::ReentrantCall in inner error, got {inner}");
        };
        assert_eq!(query_name, expected_query_name);
    }

    #[tokio::test]
    async fn db_executor_rejects_nested_call_before_timeout() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let executor = DbExecutor::open_in_memory(supervisor, "nested-test")
            .await
            .unwrap();
        let nested_executor = executor.clone();
        let handle = tokio::runtime::Handle::current();

        let result = tokio::time::timeout(Duration::from_millis(250), async move {
            executor
                .call_raw("outer", move |_conn| {
                    let nested = handle.block_on(nested_executor.call_raw("inner", |_conn| Ok(())));
                    Ok::<_, tokio_rusqlite::Error>(nested)
                })
                .await
        })
        .await
        .expect("nested db call should fail quickly instead of timing out")
        .expect("outer call should complete");

        let nested_err = result.expect_err("inner call must fail with reentrant error");
        assert_reentrant_db_error(&nested_err, "inner");
    }

    #[tokio::test]
    async fn db_executor_releases_guard_after_error() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let executor = DbExecutor::open_in_memory(supervisor, "guard-release-test")
            .await
            .unwrap();
        let nested_executor = executor.clone();
        let handle = tokio::runtime::Handle::current();

        let nested = executor
            .call_raw("outer", move |_conn| {
                let nested = handle.block_on(nested_executor.call_raw("inner", |_conn| Ok(())));
                Ok::<_, tokio_rusqlite::Error>(nested)
            })
            .await
            .unwrap();
        let nested_err = nested.expect_err("inner call must fail with reentrant error");
        assert_reentrant_db_error(&nested_err, "inner");

        let value: i64 = executor
            .call_raw("post_error_query", move |conn| {
                Ok::<_, tokio_rusqlite::Error>(
                    conn.query_row("SELECT 41 + 1", [], |row| row.get(0))?,
                )
            })
            .await
            .unwrap();
        assert_eq!(value, 42);
    }

    #[tokio::test]
    async fn db_executor_serializes_normal_concurrent_calls() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let executor = Arc::new(
            DbExecutor::open_in_memory(supervisor.clone(), "serialize-test")
                .await
                .unwrap(),
        );
        let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
        let started = Arc::new(AtomicUsize::new(0));

        let first = {
            let executor = executor.clone();
            let gate = gate.clone();
            let started = started.clone();
            tokio::spawn(async move {
                executor
                    .call_raw("first", move |_conn| {
                        started.fetch_add(1, Ordering::SeqCst);
                        wait_on_gate(&gate);
                        Ok::<_, tokio_rusqlite::Error>(())
                    })
                    .await
                    .unwrap();
            })
        };

        while started.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let second = {
            let executor = executor.clone();
            tokio::spawn(async move {
                executor
                    .call_raw("second", move |_conn| Ok::<_, tokio_rusqlite::Error>(()))
                    .await
                    .unwrap();
            })
        };

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(category_status(&supervisor, TaskCategory::Db), 1);

        release_gate(&gate, 1);
        first.await.unwrap();
        second.await.unwrap();
    }

    #[tokio::test]
    async fn db_executor_query_logging_metadata_only() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        supervisor.set_db_query_logging(true);
        let executor = DbExecutor::open_in_memory(supervisor.clone(), "query-log-test")
            .await
            .unwrap();
        let secret_value = "very-secret-token-value";

        executor
            .call_raw("insert_secret_like_data", move |conn| {
                conn.execute("CREATE TABLE t (v TEXT)", [])?;
                conn.execute("INSERT INTO t(v) VALUES (?1)", [secret_value])?;
                Ok::<_, tokio_rusqlite::Error>(())
            })
            .await
            .unwrap();

        let logs = supervisor.db_query_logs_for_test();
        assert!(!logs.is_empty(), "query log should include metadata entry");
        let serialized = serde_json::to_string(&logs).unwrap();
        assert!(
            !serialized.contains(secret_value),
            "query metadata logs must not contain SQL parameter or row values"
        );
        assert!(serialized.contains("insert_secret_like_data"));
    }
}
