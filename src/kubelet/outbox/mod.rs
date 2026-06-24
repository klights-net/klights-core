pub mod payload;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::control_plane::client::LeaderApiClient;
use crate::datastore::Resource;
use crate::datastore::command::StorageCommand;
use crate::datastore::node_local::sqlite::RuntimeObservationCheckpoint;
use crate::datastore::node_local::{NodeLocalHandle, OutboxInsert, OutboxRow};
use crate::task_supervisor::{SupervisedJoinHandle, TaskCategory, TaskSupervisor};

use self::payload::{OutboxOperation, OutboxPayload};

// bug-grpc: lease must outlast a worst-case pipelined WAN apply so a slow
// `apply_outbox` does not expire its own claim mid-flight (which would let
// `requeue_expired_outbox_leases` re-claim it and make the post-RPC
// `complete_outbox` race on a stale token). Sized at ~6× the gRPC connect
// timeout (10 s) so even a full handshake + slow round-trip stays inside.
const DEFAULT_LEASE_MS: i64 = 60_000;
const MAX_BACKOFF_MS: i64 = 60_000;
const MAX_OUTBOX_ATTEMPTS: i64 = 720;
// bug-grpc: in-flight window for pipelined leader dispatch. Matches the
// Status channel-lane pool size so concurrent `apply_outbox` calls spread
// one-per-connection across the lane (no single-connection TCP HOL).
pub const DEFAULT_DISPATCH_INFLIGHT: usize = 4;
// bug-grpc: backoff after a transient dispatch-iteration error so the
// dispatcher loop never exits (worker status reporting must not die).
const DISPATCH_ERROR_BACKOFF_MS: u64 = 500;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxApplyResult {
    Applied { applied_rv: i64 },
    AlreadyApplied { applied_rv: Option<i64> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxApplyError {
    Retryable(String),
    NotFound(String),
    UidMismatch { expected: String, actual: String },
    ConflictTerminal(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxSendRoute {
    /// Request was enqueued into node-local outbox and will be retried by
    /// `OutboxDispatcher`.
    Enqueued,
}

pub struct OutboxSendPlanner<'a> {
    outbox: Option<&'a Outbox>,
}

impl<'a> OutboxSendPlanner<'a> {
    pub const fn new(outbox: Option<&'a Outbox>) -> Self {
        Self { outbox }
    }

    pub async fn route(&self, command: OutboxCommand) -> Result<OutboxSendRoute> {
        let Some(outbox) = self.outbox else {
            anyhow::bail!(
                "outbox is unavailable for node-local queueing; caller must retry after outbox initialization"
            );
        };
        outbox.enqueue_command(command).await?;
        Ok(OutboxSendRoute::Enqueued)
    }
}

impl std::fmt::Display for OutboxApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retryable(err) => write!(f, "retryable outbox apply error: {err}"),
            Self::NotFound(err) => write!(f, "outbox target not found: {err}"),
            Self::UidMismatch { expected, actual } => {
                write!(
                    f,
                    "outbox UID mismatch: expected {expected}, actual {actual}"
                )
            }
            Self::ConflictTerminal(err) => write!(f, "terminal outbox conflict: {err}"),
        }
    }
}

impl std::error::Error for OutboxApplyError {}

impl OutboxApplyError {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::NotFound(_) | Self::UidMismatch { .. } | Self::ConflictTerminal(_)
        )
    }
}

#[async_trait]
pub trait OutboxApplyClient: Send + Sync {
    async fn apply_outbox(
        &self,
        idempotency_key: &str,
        operation: OutboxOperation,
        payload: Bytes,
    ) -> std::result::Result<OutboxApplyResult, OutboxApplyError>;
}

#[async_trait]
impl OutboxApplyClient for crate::replication::grpc::client::ReplicationGrpcClient {
    async fn apply_outbox(
        &self,
        idempotency_key: &str,
        operation: OutboxOperation,
        payload: Bytes,
    ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
        self.apply_outbox_rpc(idempotency_key, operation, payload)
            .await
    }
}

#[derive(Clone)]
pub struct LeaderApiOutboxClient {
    client: Arc<dyn LeaderApiClient>,
}

impl LeaderApiOutboxClient {
    pub const fn new(client: Arc<dyn LeaderApiClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl OutboxApplyClient for LeaderApiOutboxClient {
    async fn apply_outbox(
        &self,
        idempotency_key: &str,
        operation: OutboxOperation,
        payload: Bytes,
    ) -> std::result::Result<OutboxApplyResult, OutboxApplyError> {
        self.client
            .apply_outbox(idempotency_key, operation, payload)
            .await
    }
}

#[derive(Clone)]
pub struct Outbox {
    node_db: NodeLocalHandle,
    notify: Arc<Notify>,
    stamp: Arc<tokio::sync::Mutex<StampState>>,
}

/// In-memory state of the per-node status-stamp allocator. `next` is the last
/// stamp issued; `reserved` is the durable ceiling persisted to node-local meta
/// (see [`Outbox::next_status_stamp`]). `seeded` guards the one-time load of the
/// persisted ceiling on first use.
#[derive(Default)]
struct StampState {
    seeded: bool,
    next: i64,
    reserved: i64,
}

/// Node-local meta key holding the durable status-stamp high-water (the reserved
/// ceiling). Survives worker process restarts so stamps never regress.
const STATUS_STAMP_META_KEY: &str = "pod_status_stamp_high_water";
/// Headroom (in stamp units) reserved per node-local persistence write. The
/// ceiling is persisted at most once per this many issued stamps (or per this
/// many microseconds of wall-clock advance), bounding node-local writes while
/// keeping idle cost at zero.
const STATUS_STAMP_RESERVE_BLOCK: i64 = 5_000_000;

pub struct OutboxSubject {
    pub key: String,
    pub namespace: Option<String>,
    pub name: String,
    pub uid: Option<String>,
}

impl OutboxSubject {
    pub fn new(
        key: impl Into<String>,
        namespace: Option<String>,
        name: impl Into<String>,
        uid: Option<String>,
    ) -> Self {
        Self {
            key: key.into(),
            namespace,
            name: name.into(),
            uid,
        }
    }
}

pub struct OutboxCommand {
    pub idempotency_key: String,
    pub operation: OutboxOperation,
    pub subject: OutboxSubject,
    pub pod_uid: String,
    pub command: StorageCommand,
    pub now_ms: i64,
}

impl OutboxCommand {
    pub fn new(
        idempotency_key: impl Into<String>,
        operation: OutboxOperation,
        subject: OutboxSubject,
        pod_uid: impl Into<String>,
        command: StorageCommand,
        now_ms: i64,
    ) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
            operation,
            subject,
            pod_uid: pod_uid.into(),
            command,
            now_ms,
        }
    }
}

impl Outbox {
    #[cfg(test)]
    pub fn new(node_db: NodeLocalHandle) -> Self {
        Self::with_notify(node_db, Arc::new(Notify::new()))
    }

    pub fn with_notify(node_db: NodeLocalHandle, notify: Arc<Notify>) -> Self {
        Self {
            node_db,
            notify,
            stamp: Arc::new(tokio::sync::Mutex::new(StampState::default())),
        }
    }

    /// Issue a strictly-monotonic per-node status stamp for an outbound Pod
    /// status snapshot.
    ///
    /// The leader drops an outbox status whose stamp is `<=` the one it last
    /// applied for that Pod (the lost-update gate), so a stamp that regressed
    /// across a worker restart — e.g. an NTP step-back or VM clock skew — would
    /// make a genuinely newer status look stale and be silently discarded. To
    /// stay monotonic independent of the wall clock we persist a reserved
    /// ceiling to node-local meta *before* issuing any stamp that reaches it
    /// (a hi/lo allocator), so the seed on the next boot is always `>=` every
    /// stamp already issued. A wall-clock floor keeps freshly issued stamps
    /// comparable in magnitude with rows written before this allocator existed.
    pub async fn next_status_stamp(&self) -> Result<i64> {
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros().min(i64::MAX as u128) as i64)
            .unwrap_or(0);
        self.next_status_stamp_with_clock(now_us).await
    }

    /// Clock-injected core of [`Outbox::next_status_stamp`] for deterministic
    /// tests (including simulated clock regression across restart).
    async fn next_status_stamp_with_clock(&self, now_us: i64) -> Result<i64> {
        let mut st = self.stamp.lock().await;
        if !st.seeded {
            let persisted = self
                .node_db
                .get_node_meta(STATUS_STAMP_META_KEY)
                .await?
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            // Seed both the issue cursor and the reserved ceiling from the
            // persisted high-water so the first stamp after restart exceeds
            // every previously issued stamp even if the clock has regressed.
            st.next = persisted;
            st.reserved = persisted;
            st.seeded = true;
        }
        let candidate = now_us.max(st.next.saturating_add(1));
        if candidate >= st.reserved {
            // Reserve and durably persist a new ceiling BEFORE issuing, so a
            // crash can never lose a stamp below what was already handed out.
            let new_reserved = candidate.saturating_add(STATUS_STAMP_RESERVE_BLOCK);
            self.node_db
                .set_node_meta(STATUS_STAMP_META_KEY, &new_reserved.to_string())
                .await?;
            st.reserved = new_reserved;
        }
        st.next = candidate;
        Ok(candidate)
    }

    /// Create an outbox backed by an in-memory node-local store.
    /// For test use only.
    #[cfg(test)]
    pub async fn test_outbox() -> Self {
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let (handle, _) = crate::datastore::node_local::selector::open_node_local_with_sqlite(
            crate::datastore::backend_kind::BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:node-local-test",
        )
        .await
        .expect("open node-local for test outbox");
        Self::new(handle)
    }

    pub async fn enqueue_command(&self, command: OutboxCommand) -> Result<()> {
        let OutboxCommand {
            idempotency_key,
            operation,
            subject,
            pod_uid,
            command,
            now_ms,
        } = command;
        let OutboxSubject {
            key: subject_key,
            namespace: subject_namespace,
            name: subject_name,
            uid: subject_uid,
        } = subject;
        let payload = OutboxPayload::from_command(command).encode_protobuf()?;
        let (subject_api_version, subject_kind) = operation.subject_api_version_kind();
        self.node_db
            .enqueue_outbox(OutboxInsert {
                idempotency_key,
                enqueued_ms: now_ms,
                subject_key,
                subject_api_version: subject_api_version.to_string(),
                subject_kind: subject_kind.to_string(),
                subject_namespace,
                subject_name,
                subject_uid,
                pod_uid,
                operation: operation.as_str().to_string(),
                payload_proto: payload,
                next_due_ms: now_ms,
            })
            .await?;
        self.notify.notify_one();
        Ok(())
    }

    pub async fn record_pod_status_checkpoint(
        &self,
        pod: &Resource,
        status: Value,
        updated_ms: i64,
    ) -> Result<()> {
        let namespace = pod.namespace.as_deref().unwrap_or("default");
        self.node_db
            .upsert_pod_status_checkpoint(
                &pod.uid,
                namespace,
                &pod.name,
                pod.resource_version,
                status,
                updated_ms,
            )
            .await
    }

    pub async fn merge_pod_status_checkpoint(&self, mut pod: Resource) -> Result<Resource> {
        let Some(checkpoint) = self.node_db.get_pod_status_checkpoint(&pod.uid).await? else {
            return Ok(pod);
        };

        let namespace = pod.namespace.as_deref().unwrap_or("default");
        if checkpoint.namespace != namespace || checkpoint.pod_name != pod.name {
            self.node_db
                .delete_pod_status_checkpoint(&checkpoint.pod_uid)
                .await?;
            return Ok(pod);
        }

        if let Some(applied_rv) = checkpoint.applied_rv
            && pod.resource_version >= applied_rv
            && pod_status_contains_checkpoint(&pod.data, &checkpoint.status)
        {
            self.node_db
                .delete_pod_status_checkpoint(&checkpoint.pod_uid)
                .await?;
            return Ok(pod);
        }

        if pod.resource_version < checkpoint.base_rv {
            return Ok(pod);
        }

        let mut data = (*pod.data).clone();
        if !data.is_object() {
            return Ok(pod);
        }
        let Some(object) = data.as_object_mut() else {
            return Ok(pod);
        };
        let status_slot = object
            .entry("status".to_string())
            .or_insert_with(|| Value::Object(Default::default()));
        match (status_slot.as_object_mut(), checkpoint.status) {
            (Some(live), Value::Object(pending)) => {
                for (key, value) in pending {
                    live.insert(key, value);
                }
            }
            (_, pending) => {
                *status_slot = pending;
            }
        }
        pod.data = Arc::new(data);
        Ok(pod)
    }

    pub async fn mark_pod_status_checkpoint_applied_result(
        &self,
        pod_uid: &str,
        result: &OutboxApplyResult,
        updated_ms: i64,
    ) -> Result<()> {
        match result {
            OutboxApplyResult::Applied { applied_rv }
            | OutboxApplyResult::AlreadyApplied {
                applied_rv: Some(applied_rv),
            } => {
                self.node_db
                    .mark_pod_status_checkpoint_applied(pod_uid, *applied_rv, updated_ms)
                    .await
            }
            OutboxApplyResult::AlreadyApplied { applied_rv: None } => Ok(()),
        }
    }

    pub async fn delete_pod_status_checkpoint(&self, pod_uid: &str) -> Result<()> {
        self.node_db.delete_pod_status_checkpoint(pod_uid).await
    }

    pub async fn record_runtime_observation_checkpoint(
        &self,
        pod_uid: &str,
        container_ids: Vec<String>,
        generation: u64,
        updated_ms: i64,
    ) -> Result<()> {
        self.node_db
            .upsert_runtime_observation_checkpoint(RuntimeObservationCheckpoint {
                pod_uid: pod_uid.to_string(),
                container_ids,
                generation,
                updated_ms,
            })
            .await
    }

    pub async fn get_runtime_observation_checkpoint(
        &self,
        pod_uid: &str,
    ) -> Result<Option<RuntimeObservationCheckpoint>> {
        self.node_db
            .get_runtime_observation_checkpoint(pod_uid)
            .await
    }

    pub async fn delete_runtime_observation_checkpoint(&self, pod_uid: &str) -> Result<()> {
        self.node_db
            .delete_runtime_observation_checkpoint(pod_uid)
            .await
    }
}

fn pod_status_contains_checkpoint(pod: &Value, checkpoint_status: &Value) -> bool {
    let Some(live_status) = pod.pointer("/status") else {
        return false;
    };
    let Some(checkpoint) = checkpoint_status.as_object() else {
        return live_status == checkpoint_status;
    };
    let Some(live) = live_status.as_object() else {
        return false;
    };
    checkpoint
        .iter()
        .all(|(key, value)| live.get(key).is_some_and(|live_value| live_value == value))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchOutcome {
    Dispatched,
    Idle { next_wake_ms: Option<i64> },
}

pub struct OutboxDispatcher {
    node_db: NodeLocalHandle,
    client: Arc<dyn OutboxApplyClient>,
    notify: Arc<Notify>,
    lease_ms: i64,
    batch_mode: bool,
    batch_size: usize,
    dispatch_total: std::sync::Arc<std::sync::atomic::AtomicU64>,
    dispatch_errors_total: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl OutboxDispatcher {
    pub fn new(
        node_db: NodeLocalHandle,
        client: Arc<dyn OutboxApplyClient>,
        notify: Arc<Notify>,
    ) -> Self {
        Self {
            node_db,
            client,
            notify,
            lease_ms: DEFAULT_LEASE_MS,
            batch_mode: false,
            batch_size: 16,
            dispatch_total: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            dispatch_errors_total: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Return shared counters so callers (node_admin) can read them
    /// without going through node.db (future optimization).
    pub fn dispatch_counters(
        &self,
    ) -> (
        std::sync::Arc<std::sync::atomic::AtomicU64>,
        std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) {
        (
            self.dispatch_total.clone(),
            self.dispatch_errors_total.clone(),
        )
    }

    /// Enable leader dispatch batching: claims multiple rows per node.db
    /// transaction, applies each individually, then completes successes in
    /// a single node.db transaction.
    pub fn with_batch_mode(mut self, batch_size: usize) -> Self {
        self.batch_mode = true;
        self.batch_size = batch_size.clamp(1, 256);
        self
    }

    #[cfg(test)]
    pub fn for_tests(node_db: NodeLocalHandle, client: Arc<dyn OutboxApplyClient>) -> Self {
        Self::new(node_db, client, Arc::new(Notify::new()))
    }

    #[cfg(test)]
    pub fn batch_mode_for_tests(
        node_db: NodeLocalHandle,
        client: Arc<dyn OutboxApplyClient>,
        batch_size: usize,
    ) -> Self {
        Self::new(node_db, client, Arc::new(Notify::new())).with_batch_mode(batch_size)
    }

    pub async fn start(
        self,
        supervisor: Arc<TaskSupervisor>,
        cancel: CancellationToken,
    ) -> Result<SupervisedJoinHandle<()>> {
        let supervisor_for_run = supervisor.clone();
        supervisor
            .spawn_async(
                TaskCategory::Background,
                "kubelet_outbox_dispatcher",
                async move {
                    if let Err(err) = self.run(supervisor_for_run, cancel).await {
                        tracing::warn!(error = %err, "outbox dispatcher stopped with error");
                    }
                },
            )
            .await
    }

    pub async fn run(
        self,
        supervisor: Arc<TaskSupervisor>,
        cancel: CancellationToken,
    ) -> Result<()> {
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            // bug-grpc: the dispatcher must NEVER exit on a transient
            // error — a dead dispatcher means the worker silently stops
            // reporting pod status (the 10-minute "stable cluster"
            // stall). A node.db blip, a slow-RPC lease race, or any
            // other transient failure is logged and backed off, then the
            // loop continues. Only `cancel` ends the task.
            match self.dispatch_due_once(now_ms()).await {
                Ok(DispatchOutcome::Dispatched) => continue,
                Ok(DispatchOutcome::Idle { next_wake_ms }) => {
                    let sleep_until = next_wake_ms
                        .map(instant_for_epoch_ms)
                        .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(3600));
                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(()),
                        _ = self.notify.notified() => {},
                        result = supervisor.sleep_until("kubelet_outbox_next_due", sleep_until) => {
                            result?;
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "outbox dispatch iteration failed; backing off, NOT exiting"
                    );
                    self.dispatch_errors_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(()),
                        _ = self.notify.notified() => {},
                        result = supervisor.sleep(
                            "kubelet_outbox_error_backoff",
                            Duration::from_millis(DISPATCH_ERROR_BACKOFF_MS),
                        ) => {
                            result?;
                        }
                    }
                }
            }
        }
    }

    pub async fn dispatch_due_once(&self, now_ms: i64) -> Result<DispatchOutcome> {
        self.node_db.requeue_expired_outbox_leases(now_ms).await?;

        // Claim a window of due rows. In single mode the window is 1; in
        // batch mode it is `batch_size`. Either way the claimed rows are
        // dispatched concurrently (pipelined) so the worker keeps
        // multiple WAN `apply_outbox` round-trips in flight rather than
        // one row per RTT.
        let lease_token = uuid::Uuid::new_v4().to_string();
        let rows = if self.batch_mode {
            self.node_db
                .claim_due_outbox_batch(now_ms, self.batch_size, self.lease_ms, &lease_token)
                .await?
        } else {
            self.node_db
                .claim_next_due_outbox(now_ms, self.lease_ms, &lease_token)
                .await?
                .into_iter()
                .collect()
        };

        if rows.is_empty() {
            return Ok(DispatchOutcome::Idle {
                next_wake_ms: self.node_db.next_outbox_wake_ms(now_ms).await?,
            });
        }

        tracing::info!(
            target: "klights::outbox_dispatch",
            claimed = rows.len(),
            "outbox dispatch: claimed due rows for dispatch"
        );

        self.dispatch_rows_pipelined(rows, now_ms).await;
        let _ = self.persist_dispatch_counters().await;
        Ok(DispatchOutcome::Dispatched)
    }

    /// bug-grpc: dispatch a batch of claimed rows concurrently with a
    /// bounded in-flight window. Each row's WAN `apply_outbox` and its
    /// node.db effects are handled independently by `process_claimed_row`,
    /// so a slow or failing row never stalls the others. Per-subject FIFO
    /// is preserved by the claim (at most one row per subject per batch),
    /// and cross-subject commands are idempotent + rv-guarded, so
    /// concurrent application is safe.
    async fn dispatch_rows_pipelined(&self, rows: Vec<OutboxRow>, now_ms: i64) {
        use futures::stream::{FuturesUnordered, StreamExt as _};

        let window = self.batch_size.max(DEFAULT_DISPATCH_INFLIGHT).max(1);
        let mut rows = rows.into_iter();
        let mut in_flight = FuturesUnordered::new();

        // Prime the window.
        for _ in 0..window {
            match rows.next() {
                Some(row) => in_flight.push(self.process_claimed_row(row, now_ms)),
                None => break,
            }
        }
        // Drain, refilling as each row completes to keep the window full
        // without ever exceeding it.
        while in_flight.next().await.is_some() {
            if let Some(row) = rows.next() {
                in_flight.push(self.process_claimed_row(row, now_ms));
            }
        }
    }

    /// bug-grpc: apply one claimed row end-to-end. Infallible at the
    /// dispatch level — every error is logged and made non-fatal so the
    /// dispatcher loop never exits:
    /// - a missing/stale lease token or a lost `complete_outbox` race is
    ///   warned and skipped; `requeue_expired_outbox_leases` re-claims it.
    /// - a transient apply error backs the row off; a terminal one drops
    ///   it; max attempts dead-letters it.
    ///
    /// Shared by single and batch dispatch (DRY): the only difference
    /// between the modes is the claim window size.
    async fn process_claimed_row(&self, row: OutboxRow, now_ms: i64) {
        let Some(lease_token) = row.lease_token.as_deref() else {
            tracing::warn!(
                outbox_id = row.id,
                "claimed outbox row has no lease token; skipping (will be requeued)"
            );
            return;
        };
        let operation = match OutboxOperation::try_from(row.operation.as_str()) {
            Ok(operation) => operation,
            Err(err) => {
                tracing::warn!(
                    idempotency_key = %row.idempotency_key,
                    error = %err,
                    "unknown outbox operation, completing as terminal"
                );
                self.dispatch_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.complete_row(row.id, lease_token, &row.idempotency_key)
                    .await;
                return;
            }
        };

        let records_checkpoint =
            outbox_operation_records_pod_status_checkpoint(operation) && !row.pod_uid.is_empty();
        if records_checkpoint {
            tracing::info!(
                target: "klights::outbox_dispatch",
                idempotency_key = %row.idempotency_key,
                pod_uid = %row.pod_uid,
                attempt = row.attempt,
                "outbox dispatch: claimed pod-status row"
            );
        }
        let dispatch_start = std::time::Instant::now();
        let applied = self
            .client
            .apply_outbox(
                &row.idempotency_key,
                operation,
                Bytes::from(row.payload_proto.clone()),
            )
            .await;
        let elapsed_ms = dispatch_start.elapsed().as_millis() as u64;
        if records_checkpoint {
            tracing::info!(
                target: "klights::outbox_dispatch",
                idempotency_key = %row.idempotency_key,
                pod_uid = %row.pod_uid,
                attempt = row.attempt,
                elapsed_ms,
                resolved = !matches!(applied, Err(OutboxApplyError::Retryable(_))),
                "outbox dispatch: pod-status row apply_outbox resolved"
            );
        }
        match applied {
            Ok(result) => {
                self.dispatch_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if records_checkpoint
                    && let Err(err) = self
                        .mark_pod_status_checkpoint_applied_result(&row.pod_uid, &result, now_ms)
                        .await
                {
                    tracing::warn!(pod_uid = %row.pod_uid, error = %err, "mark checkpoint applied failed");
                }
                self.complete_row(row.id, lease_token, &row.idempotency_key)
                    .await;
            }
            Err(OutboxApplyError::Retryable(err)) => {
                self.dispatch_errors_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if row.attempt + 1 >= MAX_OUTBOX_ATTEMPTS {
                    tracing::warn!(
                        idempotency_key = %row.idempotency_key,
                        attempts = %(row.attempt + 1),
                        "outbox row exceeded max attempts, moving to dead letter"
                    );
                    if let Err(err) = self
                        .node_db
                        .move_outbox_to_dead_letter_if_max_attempts(
                            &row.idempotency_key,
                            MAX_OUTBOX_ATTEMPTS,
                        )
                        .await
                    {
                        tracing::warn!(idempotency_key = %row.idempotency_key, error = %err, "dead-letter move failed");
                    }
                    if records_checkpoint
                        && let Err(err) = self
                            .node_db
                            .delete_pod_status_checkpoint(&row.pod_uid)
                            .await
                    {
                        tracing::warn!(pod_uid = %row.pod_uid, error = %err, "delete checkpoint failed");
                    }
                } else {
                    let backoff_until_ms = now_ms.saturating_add(backoff_ms(row.attempt));
                    if let Err(err) = self
                        .node_db
                        .mark_outbox_attempt_failed(row.id, lease_token, backoff_until_ms, &err)
                        .await
                    {
                        tracing::warn!(outbox_id = row.id, error = %err, "mark outbox attempt failed");
                    }
                }
            }
            // All remaining `OutboxApplyError` variants (NotFound,
            // UidMismatch, ConflictTerminal) are terminal: drop the row.
            Err(err) => {
                debug_assert!(err.is_terminal());
                self.dispatch_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if records_checkpoint
                    && let Err(err) = self
                        .node_db
                        .delete_pod_status_checkpoint(&row.pod_uid)
                        .await
                {
                    tracing::warn!(pod_uid = %row.pod_uid, error = %err, "delete checkpoint failed");
                }
                tracing::debug!(
                    idempotency_key = %row.idempotency_key,
                    error = %err,
                    "dropping terminal outbox row"
                );
                self.complete_row(row.id, lease_token, &row.idempotency_key)
                    .await;
            }
        }
    }

    /// bug-grpc: complete a row, treating a lost lease race (0 rows
    /// changed / node.db error) as non-fatal — the row stays
    /// claimed-expired and `requeue_expired_outbox_leases` re-handles it.
    async fn complete_row(&self, id: i64, lease_token: &str, idempotency_key: &str) {
        match self.node_db.complete_outbox(id, lease_token).await {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(
                    outbox_id = id,
                    idempotency_key = %idempotency_key,
                    "complete_outbox found no matching lease (lease race); will be requeued"
                );
            }
            Err(err) => {
                tracing::warn!(
                    outbox_id = id,
                    idempotency_key = %idempotency_key,
                    error = %err,
                    "complete_outbox failed; will be requeued"
                );
            }
        }
    }

    async fn persist_dispatch_counters(&self) {
        let total = self
            .dispatch_total
            .load(std::sync::atomic::Ordering::Relaxed);
        let errors = self
            .dispatch_errors_total
            .load(std::sync::atomic::Ordering::Relaxed);
        let _ = self
            .node_db
            .set_node_meta("outbox_dispatch_total", &total.to_string())
            .await;
        let _ = self
            .node_db
            .set_node_meta("outbox_dispatch_errors_total", &errors.to_string())
            .await;
    }

    async fn mark_pod_status_checkpoint_applied_result(
        &self,
        pod_uid: &str,
        result: &OutboxApplyResult,
        updated_ms: i64,
    ) -> Result<()> {
        match result {
            OutboxApplyResult::Applied { applied_rv }
            | OutboxApplyResult::AlreadyApplied {
                applied_rv: Some(applied_rv),
            } => {
                self.node_db
                    .mark_pod_status_checkpoint_applied(pod_uid, *applied_rv, updated_ms)
                    .await
            }
            OutboxApplyResult::AlreadyApplied { applied_rv: None } => Ok(()),
        }
    }
}

fn backoff_ms(attempt: i64) -> i64 {
    5_000_i64
        .saturating_mul(attempt.saturating_add(1).max(1))
        .min(MAX_BACKOFF_MS)
}

fn outbox_operation_records_pod_status_checkpoint(operation: OutboxOperation) -> bool {
    matches!(
        operation,
        OutboxOperation::PodStatus
            | OutboxOperation::RuntimeReconcile
            | OutboxOperation::ProbeReadiness
            | OutboxOperation::DeadlineExceeded
            | OutboxOperation::ContainerStatusSnapshot
            | OutboxOperation::EphemeralContainerStatuses
    )
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn instant_for_epoch_ms(epoch_ms: i64) -> tokio::time::Instant {
    let now_epoch = now_ms();
    if epoch_ms <= now_epoch {
        tokio::time::Instant::now()
    } else {
        tokio::time::Instant::now()
            + Duration::from_millis(epoch_ms.saturating_sub(now_epoch) as u64)
    }
}

#[cfg(test)]
mod tests {
    mod batch_tests;
    mod dead_letter_tests;
    use std::sync::Arc;

    use async_trait::async_trait;
    use bytes::Bytes;
    use std::collections::HashSet;
    use tokio::sync::Mutex;

    use crate::datastore::ResourcePreconditions;
    use crate::datastore::backend_kind::BackendKind;
    use crate::datastore::command::StorageCommand;
    use crate::datastore::node_local::{NodeLocalHandle, selector};
    use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};

    use super::{
        DispatchOutcome, Outbox, OutboxApplyClient, OutboxApplyError, OutboxApplyResult,
        OutboxCommand, OutboxDispatcher, OutboxSubject,
    };

    fn supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
    }

    async fn node_db() -> NodeLocalHandle {
        selector::open_node_local(
            BackendKind::Sqlite,
            None,
            supervisor(),
            None,
            "sqlite:outbox-test",
        )
        .await
        .expect("open node-local test db")
    }

    #[tokio::test]
    async fn outbox_runtime_observation_checkpoint_round_trips_by_uid() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db);

        outbox
            .record_runtime_observation_checkpoint(
                "uid-runtime-checkpoint",
                vec!["ctr-a".to_string(), "ctr-b".to_string()],
                2,
                1234,
            )
            .await
            .expect("record runtime observation checkpoint");

        let loaded = outbox
            .get_runtime_observation_checkpoint("uid-runtime-checkpoint")
            .await
            .expect("load runtime observation checkpoint")
            .expect("checkpoint exists");
        assert_eq!(loaded.pod_uid, "uid-runtime-checkpoint");
        assert_eq!(
            loaded.container_ids,
            vec!["ctr-a".to_string(), "ctr-b".to_string()]
        );
        assert_eq!(loaded.generation, 2);
        assert_eq!(loaded.updated_ms, 1234);

        outbox
            .delete_runtime_observation_checkpoint("uid-runtime-checkpoint")
            .await
            .expect("delete runtime observation checkpoint");
        assert!(
            outbox
                .get_runtime_observation_checkpoint("uid-runtime-checkpoint")
                .await
                .expect("load after delete")
                .is_none()
        );
    }

    /// A worker restart resets the in-memory stamp allocator, and the host
    /// wall clock can step backward across that restart (NTP correction / VM
    /// skew). The leader drops a status whose stamp regressed, so the stamp
    /// MUST stay strictly monotonic across restart regardless of the clock.
    /// The shared node-local handle plays the role of node.db surviving the
    /// restart; the second `Outbox` is the post-restart process.
    #[tokio::test]
    async fn status_stamp_stays_monotonic_across_restart_under_clock_regression() {
        let handle = node_db().await;

        let outbox1 = Outbox::with_notify(handle.clone(), Arc::new(tokio::sync::Notify::new()));
        let s1 = outbox1
            .next_status_stamp_with_clock(1_000_000)
            .await
            .unwrap();
        let s2 = outbox1
            .next_status_stamp_with_clock(2_000_000)
            .await
            .unwrap();
        assert!(s2 > s1, "stamps must increase while issuing: {s1} -> {s2}");

        // Restart: brand-new in-memory allocator over the SAME node-local store,
        // with a wall clock that has stepped backward below the last stamp.
        let outbox2 = Outbox::with_notify(handle.clone(), Arc::new(tokio::sync::Notify::new()));
        let s3 = outbox2.next_status_stamp_with_clock(500_000).await.unwrap();
        assert!(
            s3 > s2,
            "stamp must stay strictly monotonic across restart even when the clock regresses: last={s2} after_restart={s3}"
        );

        // And it must keep advancing after the restart too.
        let s4 = outbox2.next_status_stamp_with_clock(500_001).await.unwrap();
        assert!(
            s4 > s3,
            "post-restart stamps must keep increasing: {s3} -> {s4}"
        );
    }

    fn pod_status_command(namespace: &str, name: &str, uid: &str) -> StorageCommand {
        StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(namespace.to_string()),
            name: name.to_string(),
            status: serde_json::json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some(uid.to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        }
    }

    fn lease_renew_command(node_name: &str, uid: &str) -> StorageCommand {
        StorageCommand::UpdateResource {
            api_version: "coordination.k8s.io/v1".to_string(),
            kind: "Lease".to_string(),
            namespace: Some("kube-node-lease".to_string()),
            name: node_name.to_string(),
            data: serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {
                    "namespace": "kube-node-lease",
                    "name": node_name,
                    "uid": uid
                },
                "spec": {
                    "holderIdentity": node_name,
                    "leaseDurationSeconds": 50,
                    "renewTime": "2026-05-25T13:15:21.000000Z"
                }
            }),
            expected_rv: 1,
            preconditions: ResourcePreconditions {
                uid: Some(uid.to_string()),
                resource_version: Some(1),
            },
        }
    }

    #[test]
    fn payload_round_trips_storage_command_as_protobuf() {
        let payload = OutboxPayload::from_command(pod_status_command("default", "web", "uid-1"));

        let bytes = payload.encode_protobuf().expect("encode payload");
        let decoded = OutboxPayload::decode_protobuf(&bytes).expect("decode payload");

        assert_eq!(decoded, payload);
    }

    #[derive(Default)]
    struct FakeApplyClient {
        calls: Mutex<Vec<String>>,
        responses: Mutex<Vec<Result<OutboxApplyResult, OutboxApplyError>>>,
    }

    impl FakeApplyClient {
        async fn push_response(&self, response: Result<OutboxApplyResult, OutboxApplyError>) {
            self.responses.lock().await.push(response);
        }

        async fn calls(&self) -> Vec<String> {
            self.calls.lock().await.clone()
        }
    }

    #[async_trait]
    impl OutboxApplyClient for FakeApplyClient {
        async fn apply_outbox(
            &self,
            idempotency_key: &str,
            _operation: OutboxOperation,
            _payload: Bytes,
        ) -> Result<OutboxApplyResult, OutboxApplyError> {
            self.calls.lock().await.push(idempotency_key.to_string());
            self.responses
                .lock()
                .await
                .pop()
                .unwrap_or(Ok(OutboxApplyResult::Applied { applied_rv: 1 }))
        }
    }

    #[derive(Default)]
    struct IdempotentApplyClient {
        calls: Mutex<Vec<String>>,
        applied: Mutex<HashSet<String>>,
    }

    impl IdempotentApplyClient {
        async fn calls(&self) -> Vec<String> {
            self.calls.lock().await.clone()
        }

        async fn applied_keys(&self) -> HashSet<String> {
            self.applied.lock().await.clone()
        }
    }

    #[async_trait]
    impl OutboxApplyClient for IdempotentApplyClient {
        async fn apply_outbox(
            &self,
            idempotency_key: &str,
            _operation: OutboxOperation,
            _payload: Bytes,
        ) -> Result<OutboxApplyResult, OutboxApplyError> {
            self.calls.lock().await.push(idempotency_key.to_string());
            let mut applied = self.applied.lock().await;
            if applied.insert(idempotency_key.to_string()) {
                Ok(OutboxApplyResult::Applied {
                    applied_rv: applied.len() as i64,
                })
            } else {
                Ok(OutboxApplyResult::AlreadyApplied {
                    applied_rv: Some(applied.len() as i64),
                })
            }
        }
    }

    /// bug-grpc: records the maximum number of concurrently in-flight
    /// `apply_outbox` calls, sleeping briefly so overlapping calls are
    /// observable. Used to prove pipelined dispatch keeps > 1 RPC in
    /// flight (bounded by the batch window).
    #[derive(Default)]
    struct InFlightTrackingClient {
        current: std::sync::atomic::AtomicUsize,
        max: std::sync::atomic::AtomicUsize,
    }

    impl InFlightTrackingClient {
        fn max_in_flight(&self) -> usize {
            self.max.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl OutboxApplyClient for InFlightTrackingClient {
        async fn apply_outbox(
            &self,
            _idempotency_key: &str,
            _operation: OutboxOperation,
            _payload: Bytes,
        ) -> Result<OutboxApplyResult, OutboxApplyError> {
            use std::sync::atomic::Ordering;
            let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
            self.max.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            self.current.fetch_sub(1, Ordering::SeqCst);
            Ok(OutboxApplyResult::Applied { applied_rv: 1 })
        }
    }

    #[tokio::test]
    async fn dispatcher_survives_transient_apply_error() {
        // bug-grpc: a transient (Retryable) apply error must NOT propagate
        // out of `dispatch_due_once` (which would kill the run loop). The
        // row is backed off and redelivered on the next due pass.
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        // Stack pops LIFO: this Retryable is returned first; the default
        // Ok is returned on the redispatch.
        client
            .push_response(Err(OutboxApplyError::Retryable("transient".to_string())))
            .await;
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());

        outbox
            .enqueue_command(OutboxCommand::new(
                "transient-key",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-1",
                    Some("default".to_string()),
                    "web",
                    Some("uid-1".to_string()),
                ),
                "uid-1",
                pod_status_command("default", "web", "uid-1"),
                10,
            ))
            .await
            .expect("enqueue");

        // First pass: apply fails Retryable -> must be reported as a
        // (non-fatal) Dispatched, not an Err that would crash run().
        assert_eq!(
            dispatcher
                .dispatch_due_once(20)
                .await
                .expect("transient apply error must not propagate"),
            DispatchOutcome::Dispatched
        );

        // The row is backed off; advance past the backoff and redeliver.
        let after_backoff = 20 + super::backoff_ms(0) + 1;
        assert_eq!(
            dispatcher
                .dispatch_due_once(after_backoff)
                .await
                .expect("redispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(client.calls().await.len(), 2, "row must be retried once");
        assert!(
            node_db
                .claim_next_due_outbox(after_backoff + 1, 1_000, "check-empty")
                .await
                .expect("claim")
                .is_none(),
            "row must be completed after the successful retry"
        );
    }

    #[tokio::test]
    async fn dispatcher_survives_complete_outbox_race() {
        // bug-grpc: when a slow WAN apply outlives its claim lease, the
        // post-RPC complete races on a stale token (complete returns
        // false). This must be non-fatal — the row stays claimable and is
        // redelivered (the leader replies AlreadyApplied), then completes.
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(IdempotentApplyClient::default());
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());

        outbox
            .enqueue_command(OutboxCommand::new(
                "race-key",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-1",
                    Some("default".to_string()),
                    "web",
                    Some("uid-1".to_string()),
                ),
                "uid-1",
                pod_status_command("default", "web", "uid-1"),
                10,
            ))
            .await
            .expect("enqueue");

        // Simulate an in-flight slow apply whose lease then expires:
        // claim with a short lease token, then requeue (clears the lease).
        let row = node_db
            .claim_next_due_outbox(20, 5, "stale-token")
            .await
            .expect("claim")
            .expect("a due row");
        node_db
            .requeue_expired_outbox_leases(100)
            .await
            .expect("requeue");
        // The stale-token complete now finds no matching lease — the race
        // is detected and is non-fatal (does not lose the row).
        assert!(
            !node_db
                .complete_outbox(row.id, "stale-token")
                .await
                .expect("complete"),
            "stale-token complete must report a lost lease race, not error"
        );

        // The dispatcher re-claims and completes the surviving row.
        assert_eq!(
            dispatcher
                .dispatch_due_once(200)
                .await
                .expect("redispatch after race"),
            DispatchOutcome::Dispatched
        );
        assert!(
            node_db
                .claim_next_due_outbox(300, 1_000, "check-empty")
                .await
                .expect("claim")
                .is_none(),
            "row must be completed after the lease race recovery"
        );
    }

    #[tokio::test]
    async fn pipelined_dispatch_keeps_multiple_in_flight() {
        // bug-grpc: batch dispatch must keep multiple `apply_outbox` RPCs
        // in flight concurrently (pipelined), bounded by the batch window.
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(InFlightTrackingClient::default());
        let batch = 4usize;
        let dispatcher =
            OutboxDispatcher::batch_mode_for_tests(node_db.clone(), client.clone(), batch);

        // Enqueue one row each for distinct subjects so the batch claims
        // them all (per-subject FIFO claims at most one per subject).
        for i in 0..batch {
            let pod = format!("pod-{i}");
            let uid = format!("uid-{i}");
            outbox
                .enqueue_command(OutboxCommand::new(
                    format!("inflight-key-{i}"),
                    OutboxOperation::PodStatus,
                    OutboxSubject::new(
                        format!("v1/Pod/default/{pod}/{uid}"),
                        Some("default".to_string()),
                        pod.clone(),
                        Some(uid.clone()),
                    ),
                    &uid,
                    pod_status_command("default", &pod, &uid),
                    10,
                ))
                .await
                .expect("enqueue");
        }

        assert_eq!(
            dispatcher.dispatch_due_once(20).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );

        let max = client.max_in_flight();
        assert!(
            max > 1,
            "dispatch must pipeline multiple RPCs concurrently, saw max in-flight = {max}"
        );
        assert!(
            max <= batch,
            "in-flight window must not exceed the batch size, saw {max} > {batch}"
        );
    }

    #[tokio::test]
    async fn dispatcher_delivers_due_rows_in_subject_fifo_order() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());

        outbox
            .enqueue_command(OutboxCommand::new(
                "key-1",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-1",
                    Some("default".to_string()),
                    "web",
                    Some("uid-1".to_string()),
                ),
                "uid-1",
                pod_status_command("default", "web", "uid-1"),
                10,
            ))
            .await
            .expect("enqueue first");
        outbox
            .enqueue_command(OutboxCommand::new(
                "key-2",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-1",
                    Some("default".to_string()),
                    "web",
                    Some("uid-1".to_string()),
                ),
                "uid-1",
                pod_status_command("default", "web", "uid-1"),
                10,
            ))
            .await
            .expect("enqueue second");

        assert_eq!(
            dispatcher.dispatch_due_once(10).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(
            dispatcher.dispatch_due_once(10).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(client.calls().await, vec!["key-1", "key-2"]);
        assert!(
            node_db
                .claim_next_due_outbox(10, 1_000, "assert-empty")
                .await
                .expect("claim after drain")
                .is_none()
        );
    }

    #[tokio::test]
    async fn dispatcher_prioritizes_lease_renew_over_older_pod_status_rows() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        let dispatcher = OutboxDispatcher::for_tests(node_db, client.clone());

        for i in 0..32 {
            let key = format!("pod-status-{i:02}");
            let pod_name = format!("web-{i:02}");
            let pod_uid = format!("pod-uid-{i:02}");
            outbox
                .enqueue_command(OutboxCommand::new(
                    &key,
                    OutboxOperation::PodStatus,
                    OutboxSubject::new(
                        format!("v1/Pod/default/{pod_name}/{pod_uid}"),
                        Some("default".to_string()),
                        pod_name.clone(),
                        Some(pod_uid.clone()),
                    ),
                    &pod_uid,
                    pod_status_command("default", &pod_name, &pod_uid),
                    1_000 + i,
                ))
                .await
                .expect("enqueue pod status");
        }
        outbox
            .enqueue_command(OutboxCommand::new(
                "lease-renew",
                OutboxOperation::LeaseRenew,
                OutboxSubject::new(
                    "coordination.k8s.io/v1/Lease/kube-node-lease/mn-leader/lease-uid",
                    Some("kube-node-lease".to_string()),
                    "mn-leader",
                    Some("lease-uid".to_string()),
                ),
                "",
                lease_renew_command("mn-leader", "lease-uid"),
                1_100,
            ))
            .await
            .expect("enqueue lease renew");

        assert_eq!(
            dispatcher.dispatch_due_once(1_100).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(client.calls().await, vec!["lease-renew"]);
    }

    #[tokio::test]
    async fn expired_lease_is_reclaimed_after_restart() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());

        outbox
            .enqueue_command(OutboxCommand::new(
                "key-lease",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-1",
                    Some("default".to_string()),
                    "web",
                    Some("uid-1".to_string()),
                ),
                "uid-1",
                pod_status_command("default", "web", "uid-1"),
                100,
            ))
            .await
            .expect("enqueue");
        let claimed = node_db
            .claim_next_due_outbox(100, 50, "dead-dispatcher")
            .await
            .expect("initial claim")
            .expect("row claimed");
        assert_eq!(claimed.idempotency_key, "key-lease");

        assert_eq!(
            dispatcher.dispatch_due_once(120).await.expect("dispatch"),
            DispatchOutcome::Idle {
                next_wake_ms: Some(150)
            }
        );
        assert_eq!(
            dispatcher.dispatch_due_once(151).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(client.calls().await, vec!["key-lease"]);
    }

    #[tokio::test]
    async fn crash_recovery_replays_expired_leases_without_duplicate_effects() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(IdempotentApplyClient::default());
        for i in 0..50 {
            let key = format!("pod-status-{i:02}");
            let pod_name = format!("web-{i:02}");
            let pod_uid = format!("uid-{i:02}");
            let subject_key = format!("v1/Pod/default/{pod_name}/{pod_uid}");
            outbox
                .enqueue_command(OutboxCommand::new(
                    &key,
                    OutboxOperation::PodStatus,
                    OutboxSubject::new(
                        subject_key,
                        Some("default".to_string()),
                        pod_name.clone(),
                        Some(pod_uid.clone()),
                    ),
                    &pod_uid,
                    pod_status_command("default", &pod_name, &pod_uid),
                    1_000 + i,
                ))
                .await
                .expect("enqueue pod status");
        }

        for _ in 0..10 {
            let row = node_db
                .claim_next_due_outbox(2_000, 100, "crashed-dispatcher")
                .await
                .expect("claim")
                .expect("row");
            client
                .apply_outbox(
                    &row.idempotency_key,
                    OutboxOperation::try_from(row.operation.as_str()).expect("operation"),
                    Bytes::from(row.payload_proto),
                )
                .await
                .expect("simulate leader effect before crash");
        }

        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());
        let mut now = 2_101;
        loop {
            match dispatcher.dispatch_due_once(now).await.expect("dispatch") {
                DispatchOutcome::Dispatched => {
                    now += 1;
                }
                DispatchOutcome::Idle { next_wake_ms: None } => break,
                DispatchOutcome::Idle {
                    next_wake_ms: Some(next),
                } => {
                    now = next.max(now + 1);
                }
            }
        }

        assert_eq!(client.applied_keys().await.len(), 50);
        assert_eq!(client.calls().await.len(), 60);
        assert!(
            node_db
                .claim_next_due_outbox(now, 1_000, "assert-empty")
                .await
                .expect("claim after drain")
                .is_none()
        );
    }

    #[tokio::test]
    async fn retryable_error_requeues_with_backoff_and_terminal_uid_mismatch_completes() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());

        client
            .push_response(Err(OutboxApplyError::Retryable("leader down".into())))
            .await;
        outbox
            .enqueue_command(OutboxCommand::new(
                "key-retry",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-1",
                    Some("default".to_string()),
                    "web",
                    Some("uid-1".to_string()),
                ),
                "uid-1",
                pod_status_command("default", "web", "uid-1"),
                1_000,
            ))
            .await
            .expect("enqueue retry row");
        assert_eq!(
            dispatcher.dispatch_due_once(1_000).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(
            dispatcher.dispatch_due_once(1_100).await.expect("dispatch"),
            DispatchOutcome::Idle {
                next_wake_ms: Some(6_000)
            }
        );

        client
            .push_response(Err(OutboxApplyError::UidMismatch {
                expected: "uid-1".into(),
                actual: "uid-2".into(),
            }))
            .await;
        assert_eq!(
            dispatcher.dispatch_due_once(6_000).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert!(
            node_db
                .claim_next_due_outbox(6_000, 1_000, "assert-empty")
                .await
                .expect("claim after terminal drop")
                .is_none()
        );
    }

    #[test]
    fn retry_backoff_is_linear_five_seconds_until_sixty_seconds() {
        let cases = [
            (0, 5_000),
            (1, 10_000),
            (2, 15_000),
            (3, 20_000),
            (4, 25_000),
            (5, 30_000),
            (6, 35_000),
            (7, 40_000),
            (8, 45_000),
            (9, 50_000),
            (10, 55_000),
            (11, 60_000),
            (12, 60_000),
            (100, 60_000),
        ];
        for (attempt, expected) in cases {
            assert_eq!(
                super::backoff_ms(attempt),
                expected,
                "attempt {attempt} should back off linearly by 5s and cap at 60s"
            );
        }
    }

    #[tokio::test]
    async fn applied_checkpoint_marker_does_not_drop_unmaterialized_pod_ip_status() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        node_db
            .upsert_pod_status_checkpoint(
                "uid-checkpoint-race",
                "default",
                "checkpoint-race",
                10,
                serde_json::json!({
                    "phase": "Pending",
                    "podIP": "10.50.5.2",
                    "podIPs": [{"ip": "10.50.5.2"}],
                    "hostIP": "10.99.0.15",
                    "hostIPs": [{"ip": "10.99.0.15"}]
                }),
                200,
            )
            .await
            .expect("record newer checkpoint");
        node_db
            .mark_pod_status_checkpoint_applied("uid-checkpoint-race", 12, 300)
            .await
            .expect("older outbox row marked applied");
        let live = crate::datastore::Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "checkpoint-race".to_string(),
            uid: "uid-checkpoint-race".to_string(),
            resource_version: 20,
            data: Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "checkpoint-race",
                    "uid": "uid-checkpoint-race",
                    "resourceVersion": "20"
                },
                "spec": {
                    "nodeName": "mn-controlplane3",
                    "containers": [{"name": "e2e", "image": "registry.k8s.io/conformance:v1.34.6"}]
                },
                "status": {"phase": "Pending"}
            })),
        };

        let merged = outbox
            .merge_pod_status_checkpoint(live)
            .await
            .expect("merge checkpoint");

        assert_eq!(
            merged
                .data
                .pointer("/status/podIP")
                .and_then(|value| value.as_str()),
            Some("10.50.5.2"),
            "checkpoint must survive until its status fields are visible in the live Pod"
        );
        assert!(
            node_db
                .get_pod_status_checkpoint("uid-checkpoint-race")
                .await
                .expect("read checkpoint")
                .is_some(),
            "unmaterialized checkpoint should remain for later local reads"
        );
    }

    #[tokio::test]
    async fn stale_pod_status_outbox_does_not_block_actor_finalize_delete() {
        let cluster_db = crate::datastore::test_support::in_memory().await;
        let created = cluster_db
            .create_resource(
                "v1",
                "Pod",
                Some("default"),
                "deadline-web",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "namespace": "default",
                        "name": "deadline-web",
                        "uid": "uid-deadline-web"
                    },
                    "spec": {
                        "nodeName": "worker-a",
                        "containers": [{"name": "app", "image": "nginx"}]
                    },
                    "status": {"phase": "Running"}
                }),
            )
            .await
            .expect("create pod");

        let mut terminating = std::sync::Arc::unwrap_or_clone(created.data);
        terminating["metadata"]["deletionTimestamp"] = serde_json::json!("2026-05-24T18:00:00Z");
        terminating["metadata"]["deletionGracePeriodSeconds"] = serde_json::json!(0);
        cluster_db
            .update_resource(
                "v1",
                "Pod",
                Some("default"),
                "deadline-web",
                terminating,
                created.resource_version,
            )
            .await
            .expect("mark pod terminating");

        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(crate::control_plane::client::local::LocalApiClient::new(
            Arc::new(cluster_db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        ));
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client);
        let subject = "v1/Pod/default/deadline-web/uid-deadline-web";
        let stale_status = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "deadline-web".to_string(),
            status: serde_json::json!({
                "phase": "Failed",
                "reason": "DeadlineExceeded",
                "message": "Pod was active on the node longer than the specified deadline (5s)"
            }),
            expected_rv: Some(created.resource_version),
            preconditions: ResourcePreconditions {
                uid: Some("uid-deadline-web".to_string()),
                resource_version: Some(created.resource_version),
            },
            observed_status_stamp: None,
        };
        outbox
            .enqueue_command(OutboxCommand::new(
                "deadline-web-stale-deadline",
                OutboxOperation::DeadlineExceeded,
                OutboxSubject::new(
                    subject,
                    Some("default".to_string()),
                    "deadline-web",
                    Some("uid-deadline-web".to_string()),
                ),
                "uid-deadline-web",
                stale_status,
                1_000,
            ))
            .await
            .expect("enqueue stale status");
        outbox
            .enqueue_command(OutboxCommand::new(
                "deadline-web-actor-finalize-delete",
                OutboxOperation::PodMetadata,
                OutboxSubject::new(
                    subject,
                    Some("default".to_string()),
                    "deadline-web",
                    Some("uid-deadline-web".to_string()),
                ),
                "uid-deadline-web",
                StorageCommand::DeleteResource {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "deadline-web".to_string(),
                    preconditions: ResourcePreconditions {
                        uid: Some("uid-deadline-web".to_string()),
                        resource_version: None,
                    },
                },
                1_001,
            ))
            .await
            .expect("enqueue actor finalize delete");

        assert_eq!(
            dispatcher.dispatch_due_once(1_001).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(
            dispatcher.dispatch_due_once(1_002).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert!(
            cluster_db
                .get_resource("v1", "Pod", Some("default"), "deadline-web")
                .await
                .expect("read pod")
                .is_none(),
            "stale status conflicts must not block the actor-owned delete row"
        );
    }

    #[tokio::test]
    async fn uid_mismatch_drops_event_no_retry() {
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let client = Arc::new(FakeApplyClient::default());
        let dispatcher = OutboxDispatcher::for_tests(node_db.clone(), client.clone());

        client
            .push_response(Err(OutboxApplyError::UidMismatch {
                expected: "uid-1".into(),
                actual: "uid-2".into(),
            }))
            .await;
        outbox
            .enqueue_command(OutboxCommand::new(
                "key-uid-mismatch",
                OutboxOperation::PodStatus,
                OutboxSubject::new(
                    "v1/Pod/default/web/uid-1",
                    Some("default".to_string()),
                    "web",
                    Some("uid-1".to_string()),
                ),
                "uid-1",
                pod_status_command("default", "web", "uid-1"),
                1_000,
            ))
            .await
            .expect("enqueue");

        assert_eq!(
            dispatcher.dispatch_due_once(1_000).await.expect("dispatch"),
            DispatchOutcome::Dispatched
        );
        assert_eq!(
            dispatcher.dispatch_due_once(1_000).await.expect("dispatch"),
            DispatchOutcome::Idle { next_wake_ms: None }
        );
        assert_eq!(client.calls().await, vec!["key-uid-mismatch"]);
    }

    #[tokio::test]
    async fn node_status_and_event_writes_enqueue_outbox_rows() {
        let db = Arc::new(
            crate::datastore::sqlite::Datastore::new_in_memory()
                .await
                .expect("open sqlite datastore"),
        );
        let node_db = node_db().await;
        let outbox = Outbox::new(node_db.clone());
        let existing_node = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "node-a"},
            "status": {}
        });
        db.create_resource("v1", "Node", None, "node-a", existing_node)
            .await
            .expect("seed existing node");

        crate::kubelet::node::register_node_with_outbox(
            db.as_ref(),
            &outbox,
            "node-a",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Worker {
                leader_endpoints: vec!["https://leader:7979".to_string()],
                token: Some("token".to_string()),
                skip_ca: false,
            },
            None,
            None,
        )
        .await
        .expect("enqueue node status");
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": "web",
                "uid": "pod-uid-1"
            }
        });
        crate::kubelet::events::emit_pod_event_with_outbox(
            db.as_ref(),
            Some(&outbox),
            crate::kubelet::events::PodEventRecord {
                pod: &pod,
                reason: "Started",
                message: "Started container app",
                event_type: "Normal",
                reporting_component: "klights-kubelet",
                reporting_instance: "node-a",
            },
        )
        .await
        .expect("enqueue event");

        let mut operations = Vec::new();
        while let Some(row) = node_db
            .claim_next_due_outbox(i64::MAX / 2, 1_000, "inspect")
            .await
            .expect("claim")
        {
            operations.push(row.operation);
            node_db
                .complete_outbox(row.id, row.lease_token.as_deref().expect("lease token"))
                .await
                .expect("complete");
        }

        assert_eq!(operations, vec!["NodeStatus", "EventCreate"]);
        assert!(
            db.list_resources(
                "v1",
                "Event",
                Some("default"),
                crate::datastore::ResourceListQuery::all()
            )
            .await
            .expect("list events")
            .items
            .is_empty()
        );
    }
}
