use std::cell::Cell;
use std::fmt;
use std::sync::Arc;

#[cfg(test)]
use crate::TaskCategoryConfig;
use crate::{TaskCategory, TaskSupervisor};
use tokio_rusqlite::Connection;

use crate::sqlite_open as opener;
use crate::sqlite_open::{OpenOpts, OpenPath, apply_pragmas, apply_read_pragmas, ensure_root_only};

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
    reopen_opts: Option<OpenOpts>,
}

#[derive(Debug)]
pub enum DbError {
    ReentrantCall { query_name: String },
}

impl fmt::Display for DbError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReentrantCall { query_name } => write!(
                formatter,
                "reentrant db call rejected before enqueue: query_name={query_name}"
            ),
        }
    }
}

impl std::error::Error for DbError {}

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
        Self::new_with_category_and_opts(
            connection,
            task_supervisor,
            connection_key,
            TaskCategory::Db,
            None,
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
        Self::new_with_category_and_opts(
            connection,
            task_supervisor,
            connection_key,
            call_category,
            None,
        )
    }

    fn new_with_category_and_opts(
        connection: Connection,
        task_supervisor: Arc<TaskSupervisor>,
        connection_key: impl Into<String>,
        call_category: TaskCategory,
        reopen_opts: Option<OpenOpts>,
    ) -> Self {
        Self {
            inner: Arc::new(DbExecutorInner {
                connection,
                task_supervisor,
                connection_key: connection_key.into(),
                call_category,
                reopen_opts,
            }),
        }
    }

    pub fn read_lane_clone(&self) -> Self {
        Self::new_with_category_and_opts(
            self.inner.connection.clone(),
            self.inner.task_supervisor.clone(),
            self.inner.connection_key.clone(),
            TaskCategory::DbRead,
            self.inner.reopen_opts.clone(),
        )
    }

    /// Open a connection through the centralized `OpenOpts` path.
    ///
    /// For `Disk(path)`: hardens parent dir to `0700`, opens the
    /// connection, and applies the PRAGMA profile inside a supervised closure.
    /// The cluster and node-local owners initialize and validate their schemas
    /// through their focused open adapters.
    ///
    /// For `InMemory`: opens and applies PRAGMAs without selecting a schema.
    pub async fn open_with_opts(
        opts: OpenOpts,
        task_supervisor: Arc<TaskSupervisor>,
        connection_key: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let connection = match &opts.path {
            OpenPath::SharedMemory(uri) => {
                let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                    | rusqlite::OpenFlags::SQLITE_OPEN_URI
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
                Connection::open_with_flags(uri, flags).await?
            }
            OpenPath::Disk(path) => {
                // Detect orphaned WAL before SQLite silently creates a new DB.
                opener::check_orphaned_wal(&task_supervisor, path).await?;
                ensure_root_only(&task_supervisor, path, opts.allow_existing_perms).await?;
                Connection::open(path).await?
            }
        };
        let executor = Self::new_with_category_and_opts(
            connection,
            task_supervisor.clone(),
            connection_key,
            TaskCategory::Db,
            Some(opts.clone()),
        );
        let profile = opts.profile;

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
            .call_raw("opener:apply_pragmas", move |conn| {
                // Apply SQLCipher key first, before any PRAGMA reads
                #[cfg(feature = "sqlcipher")]
                if let Some(ref key) = sqlcipher_key {
                    conn.pragma_update(None, "key", &key[..])?;
                }
                apply_pragmas(conn, profile)?;
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
        let (db_path, flags) = match &opts.path {
            OpenPath::Disk(path) => {
                opener::check_orphaned_wal(&task_supervisor, path).await?;
                ensure_root_only(&task_supervisor, path, opts.allow_existing_perms).await?;
                (
                    path.clone(),
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | rusqlite::OpenFlags::SQLITE_OPEN_URI
                        | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
            }
            OpenPath::SharedMemory(uri) => (
                uri.clone(),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_URI
                    | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            ),
        };
        let connection = Connection::open_with_flags(&db_path, flags).await?;
        let executor = Self::new_with_category_and_opts(
            connection,
            task_supervisor.clone(),
            connection_key,
            TaskCategory::DbRead,
            Some(opts.clone()),
        );
        let profile = opts.profile;

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
            .call_raw("opener:apply_read_pragmas", move |conn| {
                #[cfg(feature = "sqlcipher")]
                if let Some(ref key) = sqlcipher_key {
                    conn.pragma_update(None, "key", &key[..])?;
                }
                apply_read_pragmas(conn, profile)?;
                Ok(())
            })
            .await?;

        Ok(executor)
    }

    pub fn task_supervisor(&self) -> Arc<TaskSupervisor> {
        self.inner.task_supervisor.clone()
    }

    pub fn snapshot_open_opts(&self) -> Option<OpenOpts> {
        self.inner.reopen_opts.clone()
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

    /// Run a DB closure and then synchronously publish its post-commit payload
    /// from a separately supervised async owner. Once admitted, cancellation
    /// of the request future cannot strand a committed mutation before its
    /// notification is published.
    pub async fn call_raw_with_post_commit<T, P, F, C>(
        &self,
        query_name: &'static str,
        f: F,
        post_commit: C,
    ) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        P: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<(T, P)> + Send + 'static,
        C: FnOnce(P) + Send + 'static,
    {
        let executor = self.clone();
        let supervisor = self.inner.task_supervisor.clone();
        let handle = supervisor
            .spawn_async(TaskCategory::Others, query_name, async move {
                let (result, payload) = executor.call_raw(query_name, f).await?;
                post_commit(payload);
                Ok(result)
            })
            .await
            .map_err(|error| {
                tokio_rusqlite::Error::Other(Box::new(std::io::Error::other(error.to_string())))
            })?;
        handle.join().await.map_err(|error| {
            tokio_rusqlite::Error::Other(Box::new(std::io::Error::other(error.to_string())))
        })?
    }
}

#[cfg(test)]
mod tests {
    use super::{DbError, DbExecutor, OpenOpts};
    use crate::{TaskCategory, TaskCategoryConfig, TaskSupervisor};
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
                    "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
                    [],
                )?;
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

    #[tokio::test]
    async fn raw_open_boundary_does_not_select_an_owned_schema() {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let executor = DbExecutor::open_with_opts(
            OpenOpts::in_memory(),
            supervisor,
            "schema-neutral-open-test",
        )
        .await
        .unwrap();
        let tables = executor
            .call_raw("schema-neutral-open:tables", |conn| {
                let count = conn.query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
                    [],
                    |row| row.get::<_, i64>(0),
                )?;
                Ok::<_, tokio_rusqlite::Error>(count)
            })
            .await
            .unwrap();
        assert_eq!(tables, 0);
    }
}
