use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;

use anyhow::Result;
use klights_cluster_core::{
    ClusterMembership, ClusterMetadata, ClusterMutation, LogApplyNodeDataplaneRow,
    LogApplyNodeSubnetRow, NetworkMutation, Resource, SnapshotRestoreOperation,
    WatchReplayPosition,
};
use klights_cluster_store::{
    DurableReplayFloor, DurableReplayTarget, SnapshotCaptureHeader, SnapshotCapturePage,
    SnapshotCaptureRequest, SnapshotCaptureSession, SnapshotMembership, SnapshotPersistenceError,
    SnapshotPersistenceFuture,
};
use rusqlite::{OptionalExtension, params};

use super::DbExecutor;
use super::opener::{OpenOpts, OpenPath};
use crate::datastore::PodCleanupIntent;

const MAX_CONCURRENT_SNAPSHOT_SESSIONS: usize = 2;

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct SnapshotCapturePagePause {
    pub(crate) reached: Arc<tokio::sync::Notify>,
    pub(crate) resume: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
static SNAPSHOT_CAPTURE_PAGE_PAUSE: std::sync::Mutex<Option<SnapshotCapturePagePause>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) fn install_snapshot_capture_page_pause() -> SnapshotCapturePagePause {
    let pause = SnapshotCapturePagePause {
        reached: Arc::new(tokio::sync::Notify::new()),
        resume: Arc::new(tokio::sync::Notify::new()),
    };
    *SNAPSHOT_CAPTURE_PAGE_PAUSE.lock().unwrap() = Some(pause.clone());
    pause
}

#[derive(Clone)]
pub(super) struct SqliteSnapshotFactory {
    opts: OpenOpts,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    permits: Arc<tokio::sync::Semaphore>,
    serialize_writes: bool,
}

impl SqliteSnapshotFactory {
    pub(super) fn new(opts: OpenOpts, supervisor: Arc<klights_supervisor::TaskSupervisor>) -> Self {
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

    pub(super) async fn open(
        &self,
        request: SnapshotCaptureRequest,
    ) -> Result<PreparedSqliteSnapshot> {
        let permit = self.permits.clone().try_acquire_owned().map_err(|_| {
            anyhow::Error::new(SnapshotPersistenceError::ResourceExhausted {
                message: "snapshot session capacity exhausted".to_string(),
            })
        })?;
        let executor = super::open::open_read_only_with_opts(
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

pub(super) struct PreparedSqliteSnapshot {
    executor: DbExecutor,
    request: SnapshotCaptureRequest,
    permit: tokio::sync::OwnedSemaphorePermit,
    serialize_writes: bool,
}

impl PreparedSqliteSnapshot {
    pub(super) async fn pin(
        self,
        fence: crate::datastore::backend::SnapshotExclusiveFence,
        anchor: Option<&dyn crate::datastore::backend::SnapshotCaptureAnchor>,
    ) -> Result<Box<dyn SnapshotCaptureSession>> {
        let Self {
            executor,
            request,
            permit,
            serialize_writes,
        } = self;
        if let Some(anchor) = anchor {
            anchor.pin_under_snapshot_fence().await?;
        }
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
            #[cfg(test)]
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

#[derive(Clone)]
enum Phase {
    Namespace(Option<String>),
    ClusterResource(Option<(String, String, String)>),
    NamespacedResource(Option<(String, String, String, String)>),
    WatchEvent(i64),
    NodeSubnet(Option<String>),
    NodeDataplane(Option<String>),
    PodCleanup(Option<(String, String, String, String, String)>),
    AppliedOutbox(Option<String>),
    Watermark(Option<(String, i64)>),
    ReplayFloor(Option<(String, String, String)>),
    Complete,
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
                #[cfg(test)]
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
    fence: std::sync::Mutex<Option<crate::datastore::backend::SnapshotExclusiveFence>>,
    #[cfg(test)]
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
        let rollback = if cfg!(test) && {
            #[cfg(test)]
            {
                self.fail_rollback.load(Ordering::Acquire)
            }
            #[cfg(not(test))]
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

fn read_header(conn: &rusqlite::Connection) -> rusqlite::Result<SnapshotCaptureHeader> {
    let current_rv = conn
        .query_row(
            "SELECT value FROM metadata WHERE key='resource_version'",
            [],
            |row| row.get::<_, String>(0),
        )?
        .parse::<i64>()
        .map_err(text_error)?;
    let event_id = conn
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name='watch_events'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .unwrap_or(0);
    let get_meta = |key: &str| {
        conn.query_row(
            "SELECT value FROM _klights_meta WHERE key=?1",
            [key],
            |row| row.get::<_, String>(0),
        )
        .optional()
    };
    let command_codec_activation_version =
        get_meta(crate::datastore::raft::node::KEY_COMMAND_CODEC_ACTIVATION_VERSION)?
            .map(|raw| raw.parse::<u32>().map_err(text_error))
            .transpose()?;
    let cluster_id = get_meta(klights_cluster_store::CLUSTER_ID_META_KEY)?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| text_error("cluster_id is missing"))?;
    let leader_epoch = get_meta(klights_cluster_store::LEADER_EPOCH_META_KEY)?
        .ok_or_else(|| text_error("leader_epoch is missing"))?
        .parse::<i64>()
        .map_err(text_error)?;
    let membership = match (
        get_meta(klights_cluster_store::RAFT_VOTERS_META_KEY)?,
        get_meta(klights_cluster_store::RAFT_TERM_META_KEY)?,
        get_meta(klights_cluster_store::RAFT_LEADER_HINT_META_KEY)?,
    ) {
        (None, None, None) => SnapshotMembership::AuthoritativeAbsent,
        (Some(voters), Some(term), Some(hint)) => SnapshotMembership::Present(ClusterMembership {
            cluster_id: cluster_id.clone(),
            voters: serde_json::from_str(&voters).map_err(text_error)?,
            term: term.parse().map_err(text_error)?,
            leader_hint: (!hint.is_empty()).then_some(hint),
        }),
        _ => return Err(text_error("membership metadata is incomplete")),
    };
    SnapshotCaptureHeader::try_new(
        command_codec_activation_version,
        WatchReplayPosition {
            resource_version: current_rv,
            event_id,
            resource_version_filter_through_event_id: 0,
        },
        ClusterMetadata {
            cluster_id,
            leader_epoch,
            current_rv,
        },
        membership,
    )
    .map_err(text_error)
}

fn read_page(
    conn: &rusqlite::Connection,
    phase: Phase,
    limit: usize,
    current_rv: i64,
    high_event_id: i64,
) -> rusqlite::Result<(Option<SnapshotCapturePage>, Phase)> {
    match phase {
        Phase::Namespace(after) => {
            let mut stmt = conn.prepare(
                "SELECT name, uid, resource_version, data FROM namespaces
                 WHERE name > ?1 ORDER BY name LIMIT ?2",
            )?;
            let rows = stmt
                .query_map(
                    params![after.as_deref().unwrap_or(""), limit as i64],
                    |row| resource_from_row(row, "v1", "Namespace", None),
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            commit_page(rows, Phase::ClusterResource(None), |row| {
                Phase::Namespace(Some(row.name.clone()))
            })
        }
        Phase::ClusterResource(after) => {
            let after = after.unwrap_or_default();
            let mut stmt = conn.prepare(
                "SELECT name, uid, resource_version, data, api_version, kind
                 FROM cluster_resources
                 WHERE (api_version,kind,name) > (?1,?2,?3)
                 ORDER BY api_version,kind,name LIMIT ?4",
            )?;
            let rows = stmt
                .query_map(params![after.0, after.1, after.2, limit as i64], |row| {
                    let api: String = row.get(4)?;
                    let kind: String = row.get(5)?;
                    resource_from_row(row, &api, &kind, None)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            commit_page(rows, Phase::NamespacedResource(None), |row| {
                Phase::ClusterResource(Some((
                    row.api_version.clone(),
                    row.kind.clone(),
                    row.name.clone(),
                )))
            })
        }
        Phase::NamespacedResource(after) => {
            let after = after.unwrap_or_default();
            let mut stmt = conn.prepare(
                "SELECT name, uid, resource_version, data, api_version, kind, namespace
                 FROM namespaced_resources
                 WHERE (api_version,kind,namespace,name) > (?1,?2,?3,?4)
                 ORDER BY api_version,kind,namespace,name LIMIT ?5",
            )?;
            let rows = stmt
                .query_map(
                    params![after.0, after.1, after.2, after.3, limit as i64],
                    |row| {
                        let api: String = row.get(4)?;
                        let kind: String = row.get(5)?;
                        let namespace: String = row.get(6)?;
                        resource_from_row(row, &api, &kind, Some(namespace))
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            commit_page(rows, Phase::WatchEvent(0), |row| {
                Phase::NamespacedResource(Some((
                    row.api_version.clone(),
                    row.kind.clone(),
                    row.namespace.clone().unwrap_or_default(),
                    row.name.clone(),
                )))
            })
        }
        Phase::WatchEvent(after) => {
            let mut stmt = conn.prepare(
                "SELECT id,api_version,kind,namespace,name,resource_version,event_type,data
                 FROM watch_events WHERE id>?1 AND id<=?2 ORDER BY id LIMIT ?3",
            )?;
            let rows = stmt
                .query_map(params![after, high_event_id, limit as i64], |row| {
                    let id: i64 = row.get(0)?;
                    let data: Vec<u8> = row.get(7)?;
                    let data = serde_json::from_slice(&data).map_err(text_error)?;
                    let resource = Resource {
                        id: 0,
                        api_version: row.get(1)?,
                        kind: row.get(2)?,
                        namespace: row.get(3)?,
                        name: row.get(4)?,
                        uid: Resource::uid_from_data(&data),
                        resource_version: row.get(5)?,
                        data: Arc::new(data),
                    };
                    let event_type: String = row.get(6)?;
                    Ok((id, resource, event_type))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            if rows.is_empty() {
                return Ok((None, Phase::NodeSubnet(None)));
            }
            let next = Phase::WatchEvent(rows.last().unwrap().0);
            let commits = rows
                .into_iter()
                .map(|(id, resource, event_type)| {
                    snapshot_operation(
                        resource.resource_version,
                        vec![crate::datastore::snapshot_export::watch_event_mutation(
                            id, resource, event_type,
                        )],
                    )
                })
                .collect();
            Ok((Some(page_commits(commits)?), next))
        }
        Phase::NodeSubnet(after) => {
            let mut stmt = conn.prepare(
                "SELECT node_name,subnet,subnet_base_int,gateway_ip,node_ip,mode,hostport_range
                 FROM node_subnets WHERE node_name>?1 ORDER BY node_name LIMIT ?2",
            )?;
            let rows = stmt
                .query_map(
                    params![after.as_deref().unwrap_or(""), limit as i64],
                    |row| {
                        Ok(LogApplyNodeSubnetRow {
                            node_name: row.get(0)?,
                            subnet: row.get(1)?,
                            subnet_base_int: row.get(2)?,
                            gateway_ip: row.get(3)?,
                            node_ip: row.get(4)?,
                            mode: row.get(5)?,
                            hostport_range: row.get(6)?,
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            network_page(
                rows,
                current_rv,
                Phase::NodeDataplane(None),
                |row| Phase::NodeSubnet(Some(row.node_name.clone())),
                NetworkMutation::PutNodeSubnet,
            )
        }
        Phase::NodeDataplane(after) => {
            let mut stmt = conn.prepare(
                "SELECT node_name,mode,encryption,public_key,endpoint,port
                 FROM node_dataplane WHERE node_name>?1 ORDER BY node_name LIMIT ?2",
            )?;
            let rows = stmt
                .query_map(
                    params![after.as_deref().unwrap_or(""), limit as i64],
                    |row| {
                        Ok(LogApplyNodeDataplaneRow {
                            node_name: row.get(0)?,
                            mode: row.get(1)?,
                            encryption: row.get(2)?,
                            public_key: row.get(3)?,
                            endpoint: row.get(4)?,
                            port: row
                                .get::<_, Option<i64>>(5)?
                                .map(u16::try_from)
                                .transpose()
                                .map_err(text_error)?,
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            network_page(
                rows,
                current_rv,
                Phase::PodCleanup(None),
                |row| Phase::NodeDataplane(Some(row.node_name.clone())),
                NetworkMutation::PutNodeDataplane,
            )
        }
        Phase::PodCleanup(after) => read_pod_cleanup(conn, after, limit, current_rv),
        Phase::Watermark(after) => read_watermarks(conn, after, limit),
        Phase::AppliedOutbox(after) => read_applied(conn, after, limit),
        Phase::ReplayFloor(after) => read_floors(conn, after, limit),
        Phase::Complete => Ok((None, Phase::Complete)),
    }
}

fn resource_from_row(
    row: &rusqlite::Row<'_>,
    api_version: &str,
    kind: &str,
    namespace: Option<String>,
) -> rusqlite::Result<Resource> {
    let data: Vec<u8> = row.get(3)?;
    Ok(Resource {
        id: 0,
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace,
        name: row.get(0)?,
        uid: row.get(1)?,
        resource_version: row.get(2)?,
        data: Arc::new(serde_json::from_slice(&data).map_err(text_error)?),
    })
}

fn commit_page(
    rows: Vec<Resource>,
    empty_next: Phase,
    cursor: impl Fn(&Resource) -> Phase,
) -> rusqlite::Result<(Option<SnapshotCapturePage>, Phase)> {
    if rows.is_empty() {
        return Ok((None, empty_next));
    }
    let next = cursor(rows.last().unwrap());
    let commits = rows
        .iter()
        .map(crate::datastore::snapshot_export::resource_restore_operation)
        .collect();
    Ok((Some(page_commits(commits)?), next))
}

fn network_page<T>(
    rows: Vec<T>,
    current_rv: i64,
    empty_next: Phase,
    cursor: impl Fn(&T) -> Phase,
    mutation: impl Fn(T) -> NetworkMutation,
) -> rusqlite::Result<(Option<SnapshotCapturePage>, Phase)> {
    if rows.is_empty() {
        return Ok((None, empty_next));
    }
    let next = cursor(rows.last().unwrap());
    let commits = rows
        .into_iter()
        .map(|row| snapshot_operation(current_rv, vec![ClusterMutation::Network(mutation(row))]))
        .collect();
    Ok((Some(page_commits(commits)?), next))
}

fn read_pod_cleanup(
    conn: &rusqlite::Connection,
    after: Option<(String, String, String, String, String)>,
    limit: usize,
    _current_rv: i64,
) -> rusqlite::Result<(Option<SnapshotCapturePage>, Phase)> {
    let after = after.unwrap_or_default();
    let mut stmt = conn.prepare(
        "SELECT node_name,namespace,pod_name,pod_uid,reason,resource_version,created_at_ms,pod_data
         FROM pod_cleanup_intents
         WHERE (node_name,namespace,pod_name,pod_uid,reason)>(?1,?2,?3,?4,?5)
         ORDER BY node_name,namespace,pod_name,pod_uid,reason LIMIT ?6",
    )?;
    let rows = stmt
        .query_map(
            params![after.0, after.1, after.2, after.3, after.4, limit as i64],
            |row| {
                let data: Vec<u8> = row.get(7)?;
                Ok(PodCleanupIntent {
                    node_name: row.get(0)?,
                    namespace: row.get(1)?,
                    pod_name: row.get(2)?,
                    pod_uid: row.get(3)?,
                    reason: row.get(4)?,
                    resource_version: row.get(5)?,
                    created_at_ms: row.get(6)?,
                    pod_data: serde_json::from_slice(&data).map_err(text_error)?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.is_empty() {
        return Ok((None, Phase::Watermark(None)));
    }
    let last = rows.last().unwrap();
    let next = Phase::PodCleanup(Some((
        last.node_name.clone(),
        last.namespace.clone(),
        last.pod_name.clone(),
        last.pod_uid.clone(),
        last.reason.clone(),
    )));
    let commits = rows
        .into_iter()
        .map(|intent| {
            snapshot_operation(
                intent.resource_version,
                vec![
                    crate::datastore::snapshot_export::cluster_pod_cleanup_mutation_from_intent(
                        intent,
                    ),
                ],
            )
        })
        .collect();
    Ok((Some(page_commits(commits)?), next))
}

fn read_applied(
    conn: &rusqlite::Connection,
    after: Option<String>,
    limit: usize,
) -> rusqlite::Result<(Option<SnapshotCapturePage>, Phase)> {
    let mut stmt = conn.prepare(
        "SELECT idempotency_key,subject_key,operation,first_seen_ms,applied_rv,result_proto,status_stamp
         FROM applied_outbox WHERE idempotency_key>?1 ORDER BY idempotency_key LIMIT ?2",
    )?;
    let rows = stmt
        .query_map(
            params![after.as_deref().unwrap_or(""), limit as i64],
            |row| {
                Ok(klights_cluster_core::LogApplyAppliedOutboxRow {
                    idempotency_key: row.get(0)?,
                    subject_key: row.get(1)?,
                    operation: row.get(2)?,
                    first_seen_ms: row.get(3)?,
                    applied_rv: row.get(4)?,
                    result_proto: row.get(5)?,
                    status_stamp: row.get(6)?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.is_empty() {
        return Ok((None, Phase::ReplayFloor(None)));
    }
    let next = Phase::AppliedOutbox(Some(rows.last().unwrap().idempotency_key.clone()));
    let page = SnapshotCapturePage::try_applied_outbox(rows).map_err(text_error)?;
    Ok((Some(page), next))
}

fn read_watermarks(
    conn: &rusqlite::Connection,
    after: Option<(String, i64)>,
    limit: usize,
) -> rusqlite::Result<(Option<SnapshotCapturePage>, Phase)> {
    let after = after.unwrap_or_default();
    let mut stmt = conn.prepare(
        "SELECT client_id,stream_id,last_seq FROM outbox_stream_watermarks
         WHERE (client_id,stream_id)>(?1,?2) ORDER BY client_id,stream_id LIMIT ?3",
    )?;
    let rows = stmt
        .query_map(params![after.0, after.1, limit as i64], |row| {
            Ok(klights_cluster_core::OutboxStreamWatermark {
                client_id: row.get(0)?,
                stream_id: row.get(1)?,
                stream_seq: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.is_empty() {
        return Ok((None, Phase::AppliedOutbox(None)));
    }
    let last = rows.last().unwrap();
    let next = Phase::Watermark(Some((last.client_id.clone(), last.stream_id)));
    let page = SnapshotCapturePage::try_outbox_watermarks(rows).map_err(text_error)?;
    Ok((Some(page), next))
}

fn read_floors(
    conn: &rusqlite::Connection,
    after: Option<(String, String, String)>,
    limit: usize,
) -> rusqlite::Result<(Option<SnapshotCapturePage>, Phase)> {
    let after = after.unwrap_or_default();
    let mut stmt = conn.prepare(
        "SELECT api_version,kind,namespace_key,floor_rv,floor_event_id,floor_position_exact
         FROM watch_replay_floors
         WHERE (api_version,kind,namespace_key)>(?1,?2,?3)
         ORDER BY api_version,kind,namespace_key LIMIT ?4",
    )?;
    let rows = stmt
        .query_map(params![after.0, after.1, after.2, limit as i64], |row| {
            let api: String = row.get(0)?;
            let kind: String = row.get(1)?;
            let namespace: String = row.get(2)?;
            let target = match (api.as_str(), kind.as_str(), namespace.as_str()) {
                ("*", "*", "*") => DurableReplayTarget::All,
                (_, _, "#cluster") => DurableReplayTarget::Cluster {
                    api_version: api,
                    kind,
                },
                _ => DurableReplayTarget::Namespaced {
                    api_version: api,
                    kind,
                    namespace,
                },
            };
            DurableReplayFloor::new(target, row.get(3)?, row.get(4)?, row.get::<_, i64>(5)? != 0)
                .map_err(text_error)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.is_empty() {
        return Ok((None, Phase::Complete));
    }
    let last = rows.last().unwrap();
    let target = last.target();
    let key = match target {
        DurableReplayTarget::All => ("*".to_string(), "*".to_string(), "*".to_string()),
        DurableReplayTarget::Cluster { api_version, kind } => {
            (api_version.clone(), kind.clone(), "#cluster".to_string())
        }
        DurableReplayTarget::Namespaced {
            api_version,
            kind,
            namespace,
        } => (api_version.clone(), kind.clone(), namespace.clone()),
    };
    let page = SnapshotCapturePage::try_replay_floors(rows).map_err(text_error)?;
    Ok((Some(page), Phase::ReplayFloor(Some(key))))
}

fn snapshot_operation(
    resource_version: i64,
    mutations: Vec<ClusterMutation>,
) -> SnapshotRestoreOperation {
    SnapshotRestoreOperation::new(
        resource_version,
        None,
        mutations
            .into_iter()
            .map(ClusterMutation::into_log_apply_mutation)
            .collect(),
    )
}

fn page_commits(
    operations: Vec<SnapshotRestoreOperation>,
) -> rusqlite::Result<SnapshotCapturePage> {
    SnapshotCapturePage::try_operations(operations).map_err(text_error)
}

fn text_error(error: impl std::fmt::Display + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(SnapshotPersistenceError::CorruptData {
        message: error.to_string(),
    }))
}

fn map_sqlite_snapshot_error(error: tokio_rusqlite::Error) -> SnapshotPersistenceError {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = source {
        if let Some(snapshot) = current.downcast_ref::<SnapshotPersistenceError>() {
            return snapshot.clone();
        }
        source = current.source();
    }
    SnapshotPersistenceError::PersistenceFailed {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::sqlite::Datastore;
    use crate::datastore::{AppliedOutboxRecord, DatastoreBackend};
    use klights_cluster_core::LogApplyMutation;
    use serde_json::json;

    async fn persistent_store() -> (tempfile::TempDir, Datastore) {
        let root = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let db = Datastore::new_persistent_paths(&root.path().join("cluster.db"), supervisor, None)
            .await
            .unwrap();
        db.set_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY, "pinned-cluster")
            .await
            .unwrap();
        db.set_klights_meta(klights_cluster_store::LEADER_EPOCH_META_KEY, "1")
            .await
            .unwrap();
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

    #[tokio::test]
    async fn sqlite_in_memory_capture_round_trips_through_pinned_session() {
        let db = Arc::new(Datastore::new_in_memory().await.unwrap());
        db.set_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY, "memory-cluster")
            .await
            .unwrap();
        db.set_klights_meta(klights_cluster_store::LEADER_EPOCH_META_KEY, "1")
            .await
            .unwrap();
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "before-anchor",
            json!({"metadata":{"name":"before-anchor","namespace":"default"}}),
        )
        .await
        .unwrap();
        let mut session = db.begin_pinned_snapshot_capture(request()).await.unwrap();
        let writer_db = db.clone();
        let writer = tokio::spawn(async move {
            let _mutation_fence =
                DatastoreBackend::acquire_snapshot_mutation_fence(writer_db.as_ref())
                    .await
                    .unwrap()
                    .unwrap();
            writer_db
                .create_resource(
                    "v1",
                    "ConfigMap",
                    Some("default"),
                    "after-snapshot",
                    json!({"metadata":{"name":"after-snapshot","namespace":"default"}}),
                )
                .await
                .unwrap();
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
        assert!(
            db.get_resource("v1", "ConfigMap", Some("default"), "after-snapshot")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn sqlite_pinned_session_releases_fence_and_excludes_post_anchor_apply() {
        let (_root, db) = persistent_store().await;
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "before-anchor",
            json!({"metadata":{"name":"before-anchor","namespace":"default"}}),
        )
        .await
        .unwrap();
        let mut session = db.begin_pinned_snapshot_capture(request()).await.unwrap();

        let mutation = DatastoreBackend::acquire_snapshot_mutation_fence(&db);
        let mutation = tokio::time::timeout(std::time::Duration::from_secs(2), mutation)
            .await
            .expect("begin_capture must release the short exclusive fence")
            .unwrap()
            .unwrap();
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "after-anchor",
            json!({"metadata":{"name":"after-anchor","namespace":"default"}}),
        )
        .await
        .unwrap();
        assert!(
            db.get_resource("v1", "ConfigMap", Some("default"), "after-anchor")
                .await
                .unwrap()
                .is_some(),
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
        let executor = crate::datastore::sqlite::open::open_with_opts(
            crate::sqlite_open::OpenOpts::in_memory(),
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
            db.insert_applied_outbox(AppliedOutboxRecord {
                idempotency_key: format!("pinned-{index:04}"),
                subject_key: "subject".to_string(),
                operation: "Update".to_string(),
                first_seen_ms: index as i64,
                applied_rv: Some(1),
                result_proto: vec![index as u8],
                status_stamp: None,
            })
            .await
            .unwrap();
        }
        let mut session = db.begin_pinned_snapshot_capture(request()).await.unwrap();
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
        db.insert_applied_outbox(AppliedOutboxRecord {
            idempotency_key: "ledger".to_string(),
            subject_key: "subject".to_string(),
            operation: "Update".to_string(),
            first_seen_ms: 1,
            applied_rv: Some(1),
            result_proto: Vec::new(),
            status_stamp: None,
        })
        .await
        .unwrap();
        db.db_call("seed-watermark", |conn| {
            conn.execute(
                "INSERT INTO outbox_stream_watermarks(client_id,stream_id,last_seq)
                 VALUES('worker',1,1)",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();
        let mut session = db.begin_pinned_snapshot_capture(request()).await.unwrap();
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
        .await
        .unwrap();
        let mut session = db.begin_pinned_snapshot_capture(request()).await.unwrap();
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
        let mut session = db.begin_pinned_snapshot_capture(request).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        assert_eq!(
            session.next_page().await.unwrap_err(),
            SnapshotPersistenceError::Timeout
        );
    }
}
