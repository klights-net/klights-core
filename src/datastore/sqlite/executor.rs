use std::cell::Cell;
use std::sync::Arc;

#[cfg(test)]
use klights_supervisor::TaskCategoryConfig;
use klights_supervisor::{TaskCategory, TaskSupervisor};
use thiserror::Error;
use tokio_rusqlite::Connection;

use super::opener::{
    self, OpenOpts, OpenPath, apply_pragmas, apply_read_pragmas,
    check_db_health_for as check_db_health_for_executor, ensure_root_only,
    init_schema_for as init_schema_for_executor,
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
    call_category: TaskCategory,
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
        Self::new_with_category(
            connection,
            task_supervisor,
            connection_key,
            TaskCategory::Db,
        )
    }

    pub fn new_read_only(
        connection: Connection,
        task_supervisor: Arc<TaskSupervisor>,
        connection_key: impl Into<String>,
    ) -> Self {
        Self::new_with_category(
            connection,
            task_supervisor,
            connection_key,
            TaskCategory::DbRead,
        )
    }

    fn new_with_category(
        connection: Connection,
        task_supervisor: Arc<TaskSupervisor>,
        connection_key: impl Into<String>,
        call_category: TaskCategory,
    ) -> Self {
        Self {
            inner: Arc::new(DbExecutorInner {
                connection,
                task_supervisor,
                connection_key: connection_key.into(),
                call_category,
            }),
        }
    }

    pub fn read_lane_clone(&self) -> Self {
        Self::new_with_category(
            self.inner.connection.clone(),
            self.inner.task_supervisor.clone(),
            self.inner.connection_key.clone(),
            TaskCategory::DbRead,
        )
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

    pub async fn open_read_only_with_opts(
        opts: OpenOpts,
        task_supervisor: Arc<TaskSupervisor>,
        connection_key: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let OpenPath::Disk(path) = &opts.path else {
            return Err(anyhow::anyhow!(
                "read-only SQLite executor requires an on-disk database"
            ));
        };
        let db_path = path.clone();
        opener::check_orphaned_wal(&db_path)?;
        ensure_root_only(&task_supervisor, &db_path, opts.allow_existing_perms).await?;
        let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
            | rusqlite::OpenFlags::SQLITE_OPEN_URI
            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let connection = Connection::open_with_flags(&db_path, flags).await?;
        let executor = Self::new_read_only(connection, task_supervisor.clone(), connection_key);
        let profile = opts.profile;
        let schema_kind = opts.schema;
        let db_display = db_path.display().to_string();

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
            .call_raw("opener:apply_read_pragmas_and_check", move |conn| {
                #[cfg(feature = "sqlcipher")]
                if let Some(ref key) = sqlcipher_key {
                    conn.pragma_update(None, "key", &key[..])?;
                }
                apply_read_pragmas(conn, profile)?;
                let db_path = std::path::Path::new(&db_display);
                check_db_health_for_executor(conn, db_path, schema_kind)?;
                Ok(())
            })
            .await?;

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
        let supervisor = self.inner.task_supervisor.clone();
        let call_category = self.inner.call_category;
        let call = move |conn: &mut rusqlite::Connection| {
            let _guard = DbCallGuard::enter();
            f(conn)
        };
        match call_category {
            TaskCategory::DbRead => {
                supervisor
                    .call_db_read(query_name, connection_key, connection, call)
                    .await
            }
            _ => {
                supervisor
                    .call_db(query_name, connection_key, connection, call)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DbError, DbExecutor, OpenOpts};
    use klights_supervisor::{TaskCategory, TaskCategoryConfig, TaskSupervisor};
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
    async fn db_executor_read_lane_is_not_blocked_by_write_lane() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let write_executor = Arc::new(
            DbExecutor::open_in_memory(supervisor.clone(), "write-lane-test")
                .await
                .unwrap(),
        );
        let read_connection = tokio_rusqlite::Connection::open_in_memory().await.unwrap();
        let read_executor = Arc::new(DbExecutor::new_read_only(
            read_connection,
            supervisor.clone(),
            "read-lane-test",
        ));
        let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
        let write_started = Arc::new(AtomicUsize::new(0));
        let read_started = Arc::new(AtomicUsize::new(0));

        let write = {
            let executor = write_executor.clone();
            let gate = gate.clone();
            let write_started = write_started.clone();
            tokio::spawn(async move {
                executor
                    .call_raw("write_hold", move |_conn| {
                        write_started.fetch_add(1, Ordering::SeqCst);
                        wait_on_gate(&gate);
                        Ok::<_, tokio_rusqlite::Error>(())
                    })
                    .await
                    .unwrap();
            })
        };

        while write_started.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let read = {
            let executor = read_executor.clone();
            let read_started = read_started.clone();
            tokio::spawn(async move {
                executor
                    .call_raw("read_while_write_held", move |_conn| {
                        read_started.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, tokio_rusqlite::Error>(())
                    })
                    .await
                    .unwrap();
            })
        };

        tokio::time::timeout(Duration::from_secs(2), async {
            while read_started.load(Ordering::SeqCst) != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("read-lane DB call must start while write lane is occupied");
        assert_eq!(category_status(&supervisor, TaskCategory::Db), 0);
        assert_eq!(category_status(&supervisor, TaskCategory::DbRead), 0);

        release_gate(&gate, 1);
        write.await.unwrap();
        read.await.unwrap();
    }

    #[tokio::test]
    async fn persistent_read_only_executor_reads_while_write_lane_holds_transaction() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("cluster.db");
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let mut write_opts = OpenOpts::disk(db_path.clone());
        write_opts.allow_existing_perms = true;
        let mut read_opts = OpenOpts::disk(db_path.clone());
        read_opts.allow_existing_perms = true;
        let write_executor = Arc::new(
            DbExecutor::open_with_opts(
                write_opts,
                supervisor.clone(),
                "persistent-write-lane-test",
            )
            .await
            .unwrap(),
        );
        let read_executor = Arc::new(
            DbExecutor::open_read_only_with_opts(
                read_opts,
                supervisor.clone(),
                "persistent-read-lane-test",
            )
            .await
            .unwrap(),
        );

        write_executor
            .call_raw("seed_committed_metadata", move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO metadata(key, value) VALUES('read_lane_probe', 'old')",
                    [],
                )?;
                Ok::<_, tokio_rusqlite::Error>(())
            })
            .await
            .unwrap();

        let gate = Arc::new((Mutex::new(0usize), Condvar::new()));
        let write_started = Arc::new(AtomicUsize::new(0));
        let read_started = Arc::new(AtomicUsize::new(0));

        let write = {
            let executor = write_executor.clone();
            let gate = gate.clone();
            let write_started = write_started.clone();
            tokio::spawn(async move {
                executor
                    .call_raw("persistent_write_hold", move |conn| {
                        let tx = conn
                            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                        tx.execute(
                            "UPDATE metadata SET value = 'new' WHERE key = 'read_lane_probe'",
                            [],
                        )?;
                        write_started.fetch_add(1, Ordering::SeqCst);
                        wait_on_gate(&gate);
                        tx.commit()?;
                        Ok::<_, tokio_rusqlite::Error>(())
                    })
                    .await
                    .unwrap();
            })
        };

        while write_started.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let read = {
            let executor = read_executor.clone();
            let read_started = read_started.clone();
            tokio::spawn(async move {
                executor
                    .call_raw("persistent_read_while_write_held", move |conn| {
                        let value: String = conn.query_row(
                            "SELECT value FROM metadata WHERE key = 'read_lane_probe'",
                            [],
                            |row| row.get(0),
                        )?;
                        assert_eq!(value, "old");
                        read_started.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, tokio_rusqlite::Error>(())
                    })
                    .await
                    .unwrap();
            })
        };

        tokio::time::timeout(Duration::from_secs(2), async {
            while read_started.load(Ordering::SeqCst) != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("persistent read-only DB call must start while write transaction is open");
        assert_eq!(category_status(&supervisor, TaskCategory::Db), 0);
        assert_eq!(category_status(&supervisor, TaskCategory::DbRead), 0);

        release_gate(&gate, 1);
        write.await.unwrap();
        read.await.unwrap();
    }
}
