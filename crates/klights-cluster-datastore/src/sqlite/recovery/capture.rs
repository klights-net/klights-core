use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

use anyhow::Result;
use klights_cluster_store::{
    SnapshotCaptureHeader, SnapshotCapturePage, SnapshotCaptureRequest, SnapshotCaptureSession,
    SnapshotPersistenceError, SnapshotPersistenceFuture,
};

use super::snapshot::{Phase, map_sqlite_snapshot_error, read_header, read_page};
use klights_supervisor::DbExecutor;
use klights_supervisor::sqlite_open::{OpenOpts, OpenPath};

const MAX_CONCURRENT_SNAPSHOT_SESSIONS: usize = 2;

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone)]
pub struct SnapshotCapturePagePause {
    pub reached: Arc<tokio::sync::Notify>,
    pub resume: Arc<tokio::sync::Notify>,
}

#[cfg(any(test, feature = "test-support"))]
static SNAPSHOT_CAPTURE_PAGE_PAUSE: std::sync::Mutex<Option<SnapshotCapturePagePause>> =
    std::sync::Mutex::new(None);

#[cfg(any(test, feature = "test-support"))]
pub fn install_snapshot_capture_page_pause() -> SnapshotCapturePagePause {
    let pause = SnapshotCapturePagePause {
        reached: Arc::new(tokio::sync::Notify::new()),
        resume: Arc::new(tokio::sync::Notify::new()),
    };
    *SNAPSHOT_CAPTURE_PAGE_PAUSE.lock().unwrap() = Some(pause.clone());
    pause
}

#[derive(Clone)]
pub struct SqliteSnapshotFactory {
    opts: OpenOpts,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    permits: Arc<tokio::sync::Semaphore>,
    serialize_writes: bool,
}

impl SqliteSnapshotFactory {
    pub fn new(opts: OpenOpts, supervisor: Arc<klights_supervisor::TaskSupervisor>) -> Self {
        let serialize_writes = matches!(&opts.path, OpenPath::SharedMemory(_));
        Self {
            opts,
            supervisor,
            permits: Arc::new(tokio::sync::Semaphore::new(
                MAX_CONCURRENT_SNAPSHOT_SESSIONS,
            )),
            serialize_writes,
        }
    }

    pub async fn begin_snapshot(
        &self,
        request: SnapshotCaptureRequest,
        fence: klights_cluster_store::SnapshotExclusiveFence,
    ) -> Result<Box<dyn SnapshotCaptureSession>> {
        self.open(request).await?.pin(fence).await
    }

    async fn open(&self, request: SnapshotCaptureRequest) -> Result<PreparedSqliteSnapshot> {
        let permit = self.permits.clone().try_acquire_owned().map_err(|_| {
            anyhow::Error::new(SnapshotPersistenceError::ResourceExhausted {
                message: "snapshot session capacity exhausted".to_string(),
            })
        })?;
        let executor = crate::sqlite::open_read_only_with_opts(
            self.opts.clone(),
            self.supervisor.clone(),
            "sqlite:cluster-snapshot",
        )
        .await?;
        Ok(PreparedSqliteSnapshot {
            executor,
            request,
            permit,
            serialize_writes: self.serialize_writes,
        })
    }
}

struct PreparedSqliteSnapshot {
    executor: DbExecutor,
    request: SnapshotCaptureRequest,
    permit: tokio::sync::OwnedSemaphorePermit,
    serialize_writes: bool,
}

impl PreparedSqliteSnapshot {
    async fn pin(
        self,
        fence: klights_cluster_store::SnapshotExclusiveFence,
    ) -> Result<Box<dyn SnapshotCaptureSession>> {
        let Self {
            executor,
            request,
            permit,
            serialize_writes,
        } = self;
        let header = executor
            .call_raw("snapshot:begin", |conn| {
                conn.execute_batch("BEGIN")?;
                Ok(read_header(conn)?)
            })
            .await
            .map_err(map_sqlite_snapshot_error)
            .map_err(anyhow::Error::new)?;
        let retained_fence = serialize_writes.then_some(fence);
        let max_lifetime = request
            .max_lifetime()
            .min(std::time::Duration::from_secs(300));
        let deadline = Instant::now().checked_add(max_lifetime).ok_or_else(|| {
            anyhow::Error::new(SnapshotPersistenceError::InvalidSnapshot {
                message: "snapshot lifetime overflows the monotonic clock".to_string(),
            })
        })?;
        let cleanup = Arc::new(SessionCleanup {
            executor: executor.clone(),
            state: AtomicU8::new(0),
            dropped: tokio::sync::Notify::new(),
            permit: std::sync::Mutex::new(Some(permit)),
            fence: std::sync::Mutex::new(retained_fence),
            #[cfg(any(test, feature = "test-support"))]
            fail_rollback: std::sync::atomic::AtomicBool::new(false),
        });
        let drop_cleanup = cleanup.clone();
        executor
            .task_supervisor()
            .spawn_async(
                klights_supervisor::TaskCategory::Background,
                "sqlite-snapshot-drop-cleanup",
                async move {
                    drop_cleanup.dropped.notified().await;
                    if let Err(error) = drop_cleanup.rollback(1).await {
                        tracing::error!(%error, "SQLite snapshot drop cleanup failed");
                    }
                },
            )
            .await?;
        let expiry_cleanup = cleanup.clone();
        let expiry = match executor
            .task_supervisor()
            .spawn_delay("sqlite-snapshot-expiry", max_lifetime, async move {
                if let Err(error) = expiry_cleanup.rollback(2).await {
                    tracing::error!(%error, "SQLite snapshot expiry cleanup failed");
                }
            })
            .await
        {
            Ok(expiry) => expiry,
            Err(spawn_error) => {
                let rollback_error = cleanup.rollback(1).await.err();
                cleanup.dropped.notify_one();
                return Err(anyhow::anyhow!(
                    "failed to register SQLite snapshot expiry: {spawn_error}; rollback: {}",
                    rollback_error
                        .map(|error| error.to_string())
                        .unwrap_or_else(|| "completed".to_string())
                ));
            }
        };
        Ok(Box::new(SqliteSnapshotSession {
            executor,
            cleanup,
            expiry: Some(expiry),
            header,
            phase: Phase::Namespace(None),
            page_limit: request.page_limit().get(),
            deadline,
        }))
    }
}

pub(super) struct SqliteSnapshotSession {
    executor: DbExecutor,
    cleanup: Arc<SessionCleanup>,
    expiry: Option<klights_supervisor::SupervisedJoinHandle<()>>,
    header: SnapshotCaptureHeader,
    phase: Phase,
    page_limit: usize,
    deadline: Instant,
}

impl SnapshotCaptureSession for SqliteSnapshotSession {
    fn header(&self) -> &SnapshotCaptureHeader {
        &self.header
    }

    fn next_page(&mut self) -> SnapshotPersistenceFuture<'_, Option<SnapshotCapturePage>> {
        Box::pin(async move {
            match self.cleanup.state.load(Ordering::Acquire) {
                1 => return Err(SnapshotPersistenceError::Cancelled),
                2 => return Err(SnapshotPersistenceError::Timeout),
                3 => return Ok(None),
                _ => {}
            }
            if Instant::now() >= self.deadline {
                self.cleanup.rollback(2).await?;
                return Err(SnapshotPersistenceError::Timeout);
            }
            loop {
                let phase = self.phase.clone();
                let limit = self.page_limit;
                let current_rv = self.header.metadata().current_rv;
                let high_event_id = self.header.position().event_id;
                let (page, next) = self
                    .executor
                    .call_raw("snapshot:next-page", move |conn| {
                        Ok(read_page(conn, phase, limit, current_rv, high_event_id)?)
                    })
                    .await
                    .map_err(map_sqlite_snapshot_error)?;
                #[cfg(any(test, feature = "test-support"))]
                if page.is_some() {
                    let pause = SNAPSHOT_CAPTURE_PAGE_PAUSE.lock().unwrap().take();
                    if let Some(pause) = pause {
                        pause.reached.notify_one();
                        pause.resume.notified().await;
                    }
                }
                self.phase = next;
                if page.is_some() || matches!(self.phase, Phase::Complete) {
                    if matches!(self.phase, Phase::Complete) {
                        self.cleanup.rollback(3).await?;
                        if let Some(expiry) = self.expiry.take() {
                            expiry.abort();
                        }
                    }
                    return Ok(page);
                }
            }
        })
    }

    fn cancel(&mut self) -> SnapshotPersistenceFuture<'_> {
        Box::pin(async move {
            if let Some(expiry) = self.expiry.take() {
                expiry.abort();
            }
            self.cleanup.rollback(1).await?;
            Ok(())
        })
    }
}

impl Drop for SqliteSnapshotSession {
    fn drop(&mut self) {
        if let Some(expiry) = self.expiry.take() {
            expiry.abort();
        }
        self.cleanup.dropped.notify_one();
    }
}

struct SessionCleanup {
    executor: DbExecutor,
    state: AtomicU8,
    dropped: tokio::sync::Notify,
    permit: std::sync::Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
    fence: std::sync::Mutex<Option<klights_cluster_store::SnapshotExclusiveFence>>,
    #[cfg(any(test, feature = "test-support"))]
    fail_rollback: std::sync::atomic::AtomicBool,
}

impl SessionCleanup {
    async fn rollback(&self, state: u8) -> Result<(), SnapshotPersistenceError> {
        if self
            .state
            .compare_exchange(0, state, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let rollback = if cfg!(any(test, feature = "test-support")) && {
            #[cfg(any(test, feature = "test-support"))]
            {
                self.fail_rollback.load(Ordering::Acquire)
            }
            #[cfg(not(any(test, feature = "test-support")))]
            {
                false
            }
        } {
            Err(SnapshotPersistenceError::PersistenceFailed {
                message: "injected snapshot rollback failure".to_string(),
            })
        } else {
            self.executor
                .call_raw("snapshot:rollback", |conn| {
                    conn.execute_batch("ROLLBACK")?;
                    Ok(())
                })
                .await
                .map_err(|error| SnapshotPersistenceError::PersistenceFailed {
                    message: format!("snapshot rollback failed: {error}"),
                })
        };
        // Capacity is a process-local admission control, not a transaction
        // success marker. Never leak it permanently when rollback itself
        // fails; the failure remains observable to explicit callers and logs.
        self.permit.lock().unwrap().take();
        self.fence.lock().unwrap().take();
        rollback
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_cluster_core::{LogApplyAppliedOutboxRow, LogApplyMutation};
    use klights_cluster_store::{SnapshotExclusiveFence, SnapshotMutationFence};
    use serde_json::json;

    struct TestStore {
        executor: DbExecutor,
        factory: SqliteSnapshotFactory,
        snapshot_fence: Arc<tokio::sync::RwLock<()>>,
    }

    impl TestStore {
        async fn open(opts: OpenOpts) -> Self {
            let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
                klights_supervisor::TaskCategoryConfig::default(),
            ));
            let executor =
                crate::sqlite::open_with_opts(opts.clone(), supervisor.clone(), "recovery-test")
                    .await
                    .unwrap();
            Self {
                executor,
                factory: SqliteSnapshotFactory::new(opts, supervisor),
                snapshot_fence: Arc::new(tokio::sync::RwLock::new(())),
            }
        }

        async fn new_in_memory() -> Self {
            Self::open(OpenOpts::shared_memory("recovery-test")).await
        }

        async fn set_klights_meta(&self, key: &'static str, value: &'static str) {
            self.executor
                .call_raw("recovery-test:set-meta", move |conn| {
                    conn.execute(crate::sqlite::META_INSERT, (key, value))?;
                    Ok(())
                })
                .await
                .unwrap();
        }

        async fn create_resource(&self, name: &'static str, data: serde_json::Value) {
            let data = serde_json::to_vec(&data).unwrap();
            self.executor
                .call_raw("recovery-test:create-resource", move |conn| {
                    conn.execute(
                        "INSERT INTO namespaced_resources
                         (api_version,kind,namespace,name,uid,resource_version,created_rv,data)
                         VALUES('v1','ConfigMap','default',?1,?2,1,1,?3)",
                        rusqlite::params![name, format!("uid-{name}"), data],
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
        }

        async fn has_resource(&self, name: &'static str) -> bool {
            self.executor
                .call_raw("recovery-test:has-resource", move |conn| {
                    Ok(conn.query_row(
                        "SELECT EXISTS(
                            SELECT 1 FROM namespaced_resources
                            WHERE api_version='v1' AND kind='ConfigMap'
                              AND namespace='default' AND name=?1
                         )",
                        [name],
                        |row| row.get::<_, bool>(0),
                    )?)
                })
                .await
                .unwrap()
        }

        async fn insert_applied_outbox(&self, row: LogApplyAppliedOutboxRow) {
            self.executor
                .call_raw("recovery-test:insert-applied-outbox", move |conn| {
                    conn.execute(
                        "INSERT INTO applied_outbox
                         (idempotency_key,subject_key,operation,first_seen_ms,applied_rv,
                          result_proto,status_stamp)
                         VALUES(?1,?2,?3,?4,?5,?6,?7)",
                        rusqlite::params![
                            row.idempotency_key,
                            row.subject_key,
                            row.operation,
                            row.first_seen_ms,
                            row.applied_rv,
                            row.result_proto,
                            row.status_stamp,
                        ],
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
        }

        async fn db_call<T, F>(&self, query_name: &'static str, call: F) -> T
        where
            T: Send + 'static,
            F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<T> + Send + 'static,
        {
            self.executor.call_raw(query_name, call).await.unwrap()
        }

        async fn acquire_snapshot_exclusive_fence(&self) -> SnapshotExclusiveFence {
            SnapshotExclusiveFence::new(self.snapshot_fence.clone().write_owned().await)
        }

        async fn acquire_snapshot_mutation_fence(&self) -> SnapshotMutationFence {
            SnapshotMutationFence::new(self.snapshot_fence.clone().read_owned().await)
        }

        async fn begin_pinned_snapshot_capture(
            &self,
            request: SnapshotCaptureRequest,
            fence: SnapshotExclusiveFence,
        ) -> Box<dyn SnapshotCaptureSession> {
            self.factory.begin_snapshot(request, fence).await.unwrap()
        }
    }

    async fn persistent_store() -> (tempfile::TempDir, TestStore) {
        let root = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let db = TestStore::open(OpenOpts::disk(root.path().join("cluster.db"))).await;
        db.set_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY, "pinned-cluster")
            .await;
        db.set_klights_meta(klights_cluster_store::LEADER_EPOCH_META_KEY, "1")
            .await;
        (root, db)
    }

    fn request() -> SnapshotCaptureRequest {
        SnapshotCaptureRequest::try_new(
            klights_cluster_store::SnapshotPageLimit::try_new(
                klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE,
            )
            .unwrap(),
            std::time::Duration::from_secs(30),
        )
        .unwrap()
    }

    async fn begin_capture(
        db: &TestStore,
        request: SnapshotCaptureRequest,
    ) -> Box<dyn SnapshotCaptureSession> {
        let fence = db.acquire_snapshot_exclusive_fence().await;
        db.begin_pinned_snapshot_capture(request, fence).await
    }

    #[tokio::test]
    async fn sqlite_in_memory_capture_round_trips_through_pinned_session() {
        let db = Arc::new(TestStore::new_in_memory().await);
        db.set_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY, "memory-cluster")
            .await;
        db.set_klights_meta(klights_cluster_store::LEADER_EPOCH_META_KEY, "1")
            .await;
        db.create_resource(
            "before-anchor",
            json!({"metadata":{"name":"before-anchor","namespace":"default"}}),
        )
        .await;
        let mut session = begin_capture(db.as_ref(), request()).await;
        let writer_db = db.clone();
        let writer = tokio::spawn(async move {
            let _mutation_fence = writer_db.acquire_snapshot_mutation_fence().await;
            writer_db
                .create_resource(
                    "after-snapshot",
                    json!({"metadata":{"name":"after-snapshot","namespace":"default"}}),
                )
                .await;
        });
        tokio::task::yield_now().await;
        assert!(
            !writer.is_finished(),
            "shared-memory writes must wait behind the pinned session, not fail SQLITE_LOCKED"
        );

        let mut names = Vec::new();
        while let Some(page) = session.next_page().await.unwrap() {
            if let Some(operations) = page.operations() {
                for operation in operations {
                    for mutation in operation.mutations() {
                        if let LogApplyMutation::PutResource(row) = mutation {
                            names.push(row.name.clone());
                        }
                    }
                }
            }
        }
        assert!(names.iter().any(|name| name == "before-anchor"));
        assert_eq!(session.header().metadata().cluster_id, "memory-cluster");
        writer.await.unwrap();
        assert!(db.has_resource("after-snapshot").await);
    }

    #[tokio::test]
    async fn sqlite_pinned_session_releases_fence_and_excludes_post_anchor_apply() {
        let (_root, db) = persistent_store().await;
        db.create_resource(
            "before-anchor",
            json!({"metadata":{"name":"before-anchor","namespace":"default"}}),
        )
        .await;
        let mut session = begin_capture(&db, request()).await;

        let mutation = db.acquire_snapshot_mutation_fence();
        let mutation = tokio::time::timeout(std::time::Duration::from_secs(2), mutation)
            .await
            .expect("begin_capture must release the short exclusive fence");
        db.create_resource(
            "after-anchor",
            json!({"metadata":{"name":"after-anchor","namespace":"default"}}),
        )
        .await;
        assert!(
            db.has_resource("after-anchor").await,
            "ordinary shared read lane must observe current state while snapshot lane is pinned"
        );
        drop(mutation);

        let mut names = Vec::new();
        while let Some(page) = session.next_page().await.unwrap() {
            if let Some(operations) = page.operations() {
                for operation in operations {
                    for mutation in operation.mutations() {
                        if let LogApplyMutation::PutResource(row) = mutation {
                            names.push(row.name.clone());
                        }
                    }
                }
            }
        }
        assert!(names.iter().any(|name| name == "before-anchor"));
        assert!(!names.iter().any(|name| name == "after-anchor"));
    }

    #[tokio::test]
    async fn sqlite_rollback_failure_is_observable_and_releases_capacity() {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = crate::sqlite::open_with_opts(
            klights_supervisor::OpenOpts::in_memory(),
            supervisor,
            "snapshot-rollback-failure",
        )
        .await
        .unwrap();
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = permits.clone().try_acquire_owned().unwrap();
        let cleanup = SessionCleanup {
            executor,
            state: AtomicU8::new(0),
            dropped: tokio::sync::Notify::new(),
            permit: std::sync::Mutex::new(Some(permit)),
            fence: std::sync::Mutex::new(None),
            fail_rollback: std::sync::atomic::AtomicBool::new(true),
        };
        assert!(matches!(
            cleanup.rollback(1).await,
            Err(SnapshotPersistenceError::PersistenceFailed { .. })
        ));
        assert_eq!(
            permits.available_permits(),
            1,
            "rollback failure must not leak snapshot admission capacity"
        );
    }

    #[tokio::test]
    async fn sqlite_pinned_session_pages_513_applied_rows_without_full_list() {
        let (_root, db) = persistent_store().await;
        for index in 0..=klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
            db.insert_applied_outbox(LogApplyAppliedOutboxRow {
                idempotency_key: format!("pinned-{index:04}"),
                subject_key: "subject".to_string(),
                operation: "Update".to_string(),
                first_seen_ms: index as i64,
                applied_rv: Some(1),
                result_proto: vec![index as u8],
                status_stamp: None,
            })
            .await;
        }
        let mut session = begin_capture(&db, request()).await;
        let mut lengths = Vec::new();
        while let Some(page) = session.next_page().await.unwrap() {
            if page.applied_outbox().is_some() {
                lengths.push(page.len());
            }
        }
        assert_eq!(
            lengths,
            vec![klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE, 1]
        );
    }

    #[tokio::test]
    async fn sqlite_pinned_session_orders_watermarks_before_applied_ledger() {
        let (_root, db) = persistent_store().await;
        db.insert_applied_outbox(LogApplyAppliedOutboxRow {
            idempotency_key: "ledger".to_string(),
            subject_key: "subject".to_string(),
            operation: "Update".to_string(),
            first_seen_ms: 1,
            applied_rv: Some(1),
            result_proto: Vec::new(),
            status_stamp: None,
        })
        .await;
        db.db_call("seed-watermark", |conn| {
            conn.execute(
                "INSERT INTO outbox_stream_watermarks(client_id,stream_id,last_seq)
                 VALUES('worker',1,1)",
                [],
            )?;
            Ok(())
        })
        .await;
        let mut session = begin_capture(&db, request()).await;
        let mut kinds = Vec::new();
        while let Some(page) = session.next_page().await.unwrap() {
            kinds.push(page.kind());
        }
        let watermark = kinds
            .iter()
            .position(|kind| {
                *kind == klights_cluster_store::SnapshotCapturePageKind::OutboxWatermarks
            })
            .unwrap();
        let applied = kinds
            .iter()
            .position(|kind| *kind == klights_cluster_store::SnapshotCapturePageKind::AppliedOutbox)
            .unwrap();
        assert!(watermark < applied);
    }

    #[tokio::test]
    async fn sqlite_pinned_session_rejects_corrupt_dataplane_port_structurally() {
        let (_root, db) = persistent_store().await;
        db.db_call("seed-corrupt-port", |conn| {
            conn.execute(
                "INSERT INTO node_dataplane
                 (node_name,mode,encryption,public_key,endpoint,port,updated_at)
                 VALUES('node-a','root','enabled',NULL,'127.0.0.1',70000,0)",
                [],
            )?;
            Ok(())
        })
        .await;
        let mut session = begin_capture(&db, request()).await;
        loop {
            match session.next_page().await {
                Ok(Some(_)) => continue,
                Err(SnapshotPersistenceError::CorruptData { .. }) => break,
                other => panic!("expected typed corrupt-data error, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn sqlite_pinned_session_deadline_rolls_back_without_next_page_polling() {
        let (_root, db) = persistent_store().await;
        let request = SnapshotCaptureRequest::try_new(
            klights_cluster_store::SnapshotPageLimit::try_new(1).unwrap(),
            std::time::Duration::from_millis(20),
        )
        .unwrap();
        let mut session = begin_capture(&db, request).await;
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        assert_eq!(
            session.next_page().await.unwrap_err(),
            SnapshotPersistenceError::Timeout
        );
    }
}
