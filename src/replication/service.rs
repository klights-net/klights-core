//! Leader-side replication service (2A-4).
//!
//! Exposes a supervised internal service that can accept replica connections
//! and stream `StorageCommand + CommandMeta` entries. At this stage, the
//! service starts idle and does not stream commands yet — that wiring
//! happens in 2A-5/2A-6.
//!
//! ## Design invariants
//! - Idle-silent when no replicas connect (zero CPU).
//! - All tasks spawned through `TaskSupervisor`.
//! - No direct `tokio::spawn`, sleeps, or intervals.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use anyhow::anyhow;
use klights_node_api::{
    BoundedByteStream, ByteStreamBounds, ByteStreamError, ByteStreamFuture, ExecSetupError,
    NodeExec, NodeExecFrame, NodeExecFuture, NodeExecRequest, NodeExecSession, NodeExecSyncRequest,
    NodeExecSyncResult, NodeLog, NodeLogEvent, NodeLogFuture, NodeLogRequest, NodeLogResult,
    NodeLogSetupError, NodeMetrics, NodeMetricsError, NodeMetricsFuture, NodeMetricsRequest,
    NodeMetricsResult,
};
use tokio::sync::{Mutex, RwLock, broadcast, mpsc, oneshot, watch};

use super::protocol::{
    FollowerControlMessage, JoinRequest, JoinResponse, MetadataResponse, ReplicationEntry,
    RoutedNodeExecFrame, RoutedNodeExecRequest, RoutedNodeExecSyncRequest,
    RoutedNodeExecSyncResponse, RoutedNodeLogEvent, RoutedNodeLogRequest, RoutedNodeMetricsRequest,
    RoutedNodeMetricsResponse,
};

#[cfg(test)]
use crate::datastore::backend::DatastoreBackend;
use crate::replication::grpc::fanout::FanoutPool;
use klights_leader_api::NetworkDataplane;
use klights_supervisor::{TaskCategory, TaskSupervisor};

const STREAM_FOLLOWER_QUEUE_CAPACITY: usize = 1024;
const FOLLOWER_CONTROL_QUEUE_CAPACITY: usize = 64;
const FANOUT_BATCH_SIZE: usize = 64;
const NODE_EXEC_SYNC_TIMEOUT: Duration = Duration::from_secs(300);
const NODE_METRICS_TIMEOUT: Duration = Duration::from_secs(15);
const NODE_EXEC_STREAM_FRAME_QUEUE_CAPACITY: usize = 128;
const POD_LOG_STREAM_FRAME_QUEUE_CAPACITY: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeOperationKind {
    ExecSync,
    ExecStream,
    Log,
    Metrics,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FollowerCompletionContext<'a> {
    node_name: &'a str,
    follower_session: u64,
    kind: NodeOperationKind,
}

impl<'a> FollowerCompletionContext<'a> {
    pub(crate) const fn new(
        node_name: &'a str,
        follower_session: u64,
        kind: NodeOperationKind,
    ) -> Self {
        Self {
            node_name,
            follower_session,
            kind,
        }
    }
}

struct PendingNodeOperation<T> {
    node_name: String,
    follower_session: u64,
    kind: NodeOperationKind,
    generation: u64,
    sink: T,
}

type PendingNodeExecStreams =
    Arc<Mutex<HashMap<String, PendingNodeOperation<mpsc::Sender<RoutedNodeExecFrame>>>>>;
type PendingNodeLogStreams =
    Arc<Mutex<HashMap<String, PendingNodeOperation<mpsc::Sender<RoutedNodeLogEvent>>>>>;
type PendingNodeExecSync = Mutex<
    HashMap<
        String,
        PendingNodeOperation<oneshot::Sender<Result<NodeExecSyncResult, ExecSetupError>>>,
    >,
>;
type PendingNodeLogSync = Mutex<
    HashMap<
        String,
        PendingNodeOperation<oneshot::Sender<Result<NodeLogResult, NodeLogSetupError>>>,
    >,
>;
type PendingNodeMetrics = Mutex<
    HashMap<
        String,
        PendingNodeOperation<oneshot::Sender<Result<NodeMetricsResult, NodeMetricsError>>>,
    >,
>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FollowerMetrics {
    pub follower_count: usize,
    pub max_lag: i64,
    pub followers: Vec<FollowerStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FollowerStatus {
    pub node_name: String,
    pub applied_rv: i64,
    pub lag: i64,
    pub mode: String,
    pub encryption: String,
    pub public_key: Option<String>,
}

#[derive(Clone, Debug)]
struct FollowerState {
    metadata: NetworkDataplane,
    applied_rv: i64,
    control_tx: mpsc::Sender<FollowerControlMessage>,
    session_id: u64,
}

/// Leader-side replication service.
///
/// Holds a sender end of a watch channel that receives every
/// `ReplicationEntry` applied by the leader. Connected replicas
/// subscribe to this channel to receive a live command stream.
pub struct ReplicationService {
    /// Watch sender: every new command applied by the leader is sent here.
    entry_tx: watch::Sender<Option<ReplicationEntry>>,
    /// Loss-aware ordered stream for connected replicas.
    stream_tx: broadcast::Sender<ReplicationEntry>,
    /// Current replication position (resource version).
    current_rv: AtomicI64,
    /// Test-only datastore access for token fixture setup.
    #[cfg(test)]
    db: Option<Arc<dyn DatastoreBackend>>,
    /// Canonical cluster metadata observation port.
    metadata: Arc<dyn klights_cluster_store::ClusterMetadataRead>,
    /// Bootstrap-token admission port shared by worker and control-plane joins.
    bootstrap_tokens: Arc<dyn klights_leader_api::BootstrapTokenValidation>,
    /// Task supervisor for all spawned tasks.
    supervisor: Arc<TaskSupervisor>,
    next_follower_session: AtomicU64,
    next_pending_generation: AtomicU64,
    followers: RwLock<HashMap<String, FollowerState>>,
    pending_node_exec: PendingNodeExecSync,
    pending_node_exec_streams: PendingNodeExecStreams,
    pending_pod_log: PendingNodeLogSync,
    pending_pod_log_streams: PendingNodeLogStreams,
    pending_node_metrics: PendingNodeMetrics,
    pod_log_timeout: Duration,
    fanout_pool: Mutex<FanoutPool<ReplicationEntry>>,
    fanout_started: AtomicBool,
    observed_peer_endpoints: RwLock<HashMap<String, String>>,
}

struct ReplicationNodeExecSession {
    request_id: String,
    generation: u64,
    control_tx: mpsc::Sender<FollowerControlMessage>,
    inbound_rx: Mutex<mpsc::Receiver<RoutedNodeExecFrame>>,
    pending: PendingNodeExecStreams,
    cancelled: AtomicBool,
}

impl BoundedByteStream for ReplicationNodeExecSession {
    type Frame = NodeExecFrame;

    fn bounds(&self) -> ByteStreamBounds {
        ByteStreamBounds::try_new(
            NODE_EXEC_STREAM_FRAME_QUEUE_CAPACITY,
            NODE_EXEC_STREAM_FRAME_QUEUE_CAPACITY,
        )
        .expect("exec stream capacities are non-zero constants")
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn send_frame(&self, frame: NodeExecFrame) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            if self.is_cancelled() {
                return Err(ByteStreamError::cancelled());
            }
            self.control_tx
                .send(FollowerControlMessage::NodeExecFrame(RoutedNodeExecFrame {
                    request_id: self.request_id.clone(),
                    frame,
                }))
                .await
                .map_err(|err| {
                    ByteStreamError::closed(format!(
                        "node exec stream control channel closed: {err}"
                    ))
                })
        })
    }

    fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NodeExecFrame>> {
        Box::pin(async move {
            if self.is_cancelled() {
                return Err(ByteStreamError::cancelled());
            }
            match self.inbound_rx.lock().await.recv().await {
                Some(routed) => {
                    if routed.frame.is_terminal() {
                        self.cancelled.store(true, Ordering::Release);
                    }
                    Ok(Some(routed.frame))
                }
                None => {
                    self.cancelled.store(true, Ordering::Release);
                    Ok(None)
                }
            }
        })
    }

    fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            if !self.cancelled.swap(true, Ordering::AcqRel) {
                self.inbound_rx.get_mut().close();
                let mut pending = self.pending.lock().await;
                if pending
                    .get(&self.request_id)
                    .is_some_and(|entry| entry.generation == self.generation)
                {
                    pending.remove(&self.request_id);
                }
            }
            Ok(())
        })
    }
}

impl Drop for ReplicationNodeExecSession {
    fn drop(&mut self) {
        // Best-effort cleanup: the session might be dropped in a sync context
        // (e.g. during stack unwind). Use try_lock to avoid blocking.
        if let Ok(mut pending) = self.pending.try_lock()
            && pending
                .get(&self.request_id)
                .is_some_and(|entry| entry.generation == self.generation)
        {
            pending.remove(&self.request_id);
        }
    }
}

struct ReplicationNodeLogStream {
    request_id: String,
    generation: u64,
    inbound_rx: Mutex<mpsc::Receiver<RoutedNodeLogEvent>>,
    pending: PendingNodeLogStreams,
    cancelled: AtomicBool,
}

impl BoundedByteStream for ReplicationNodeLogStream {
    type Frame = NodeLogEvent;

    fn bounds(&self) -> ByteStreamBounds {
        ByteStreamBounds::try_new(
            POD_LOG_STREAM_FRAME_QUEUE_CAPACITY,
            POD_LOG_STREAM_FRAME_QUEUE_CAPACITY,
        )
        .expect("log stream capacities are non-zero constants")
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn send_frame(&self, _frame: NodeLogEvent) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            Err(ByteStreamError::closed(
                "replication node log stream is receive-only",
            ))
        })
    }

    fn recv_frame(&self) -> ByteStreamFuture<'_, Option<NodeLogEvent>> {
        Box::pin(async move {
            if self.is_cancelled() {
                return Err(ByteStreamError::cancelled());
            }
            match self.inbound_rx.lock().await.recv().await {
                Some(routed) => {
                    let terminal = routed.event.is_terminal();
                    if terminal {
                        self.cancelled.store(true, Ordering::Release);
                    }
                    Ok(Some(routed.event))
                }
                None => {
                    self.cancelled.store(true, Ordering::Release);
                    Ok(None)
                }
            }
        })
    }

    fn cancel(&mut self) -> ByteStreamFuture<'_, ()> {
        Box::pin(async move {
            if !self.cancelled.swap(true, Ordering::AcqRel) {
                self.inbound_rx.get_mut().close();
                let mut pending = self.pending.lock().await;
                if pending
                    .get(&self.request_id)
                    .is_some_and(|entry| entry.generation == self.generation)
                {
                    pending.remove(&self.request_id);
                }
            }
            Ok(())
        })
    }
}

impl Drop for ReplicationNodeLogStream {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.pending.try_lock()
            && pending
                .get(&self.request_id)
                .is_some_and(|entry| entry.generation == self.generation)
        {
            pending.remove(&self.request_id);
        }
    }
}

impl ReplicationService {
    /// Create a new idle replication service.
    ///
    /// The service is idle-silent until a replica connects.
    /// No background tasks are spawned at creation time.
    #[cfg(test)]
    pub fn new(db: Arc<dyn DatastoreBackend>, supervisor: Arc<TaskSupervisor>) -> Self {
        let metadata = Arc::new(
            crate::datastore::cluster_store_adapter::DatastoreClusterMetadataRead::new(db.clone()),
        );
        let bootstrap_tokens = Arc::new(
            crate::bootstrap::bootstrap_token::DatastoreBootstrapTokenValidation::new(db.clone()),
        );
        let mut service = Self::new_with_ports(metadata, bootstrap_tokens, supervisor);
        service.db = Some(db);
        service
    }

    #[cfg(test)]
    pub fn new_with_containerd_namespace(
        db: Arc<dyn DatastoreBackend>,
        supervisor: Arc<TaskSupervisor>,
        containerd_namespace: String,
    ) -> Self {
        let metadata = Arc::new(
            crate::datastore::cluster_store_adapter::DatastoreClusterMetadataRead::new(db.clone()),
        );
        let bootstrap_tokens = Arc::new(
            crate::bootstrap::bootstrap_token::DatastoreBootstrapTokenValidation::new(db.clone()),
        );
        let _ = containerd_namespace;
        let mut service = Self::new_with_ports(metadata, bootstrap_tokens, supervisor);
        service.db = Some(db);
        service
    }

    pub(crate) fn new_with_ports(
        metadata: Arc<dyn klights_cluster_store::ClusterMetadataRead>,
        bootstrap_tokens: Arc<dyn klights_leader_api::BootstrapTokenValidation>,
        supervisor: Arc<TaskSupervisor>,
    ) -> Self {
        let (entry_tx, _) = watch::channel(None);
        let (stream_tx, _) = broadcast::channel(1024);
        Self {
            entry_tx,
            stream_tx,
            current_rv: AtomicI64::new(0),
            #[cfg(test)]
            db: None,
            metadata,
            bootstrap_tokens,
            supervisor,
            next_follower_session: AtomicU64::new(1),
            next_pending_generation: AtomicU64::new(1),
            followers: RwLock::new(HashMap::new()),
            pending_node_exec: Mutex::new(HashMap::new()),
            pending_node_exec_streams: Arc::new(Mutex::new(HashMap::new())),
            pending_pod_log: Mutex::new(HashMap::new()),
            pending_pod_log_streams: Arc::new(Mutex::new(HashMap::new())),
            pending_node_metrics: Mutex::new(HashMap::new()),
            pod_log_timeout: Duration::from_secs(30),
            fanout_pool: Mutex::new(FanoutPool::new(FANOUT_BATCH_SIZE)),
            fanout_started: AtomicBool::new(false),
            observed_peer_endpoints: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn task_supervisor(&self) -> Arc<TaskSupervisor> {
        self.supervisor.clone()
    }

    fn next_operation_generation(&self) -> u64 {
        self.next_pending_generation.fetch_add(1, Ordering::Relaxed)
    }

    async fn follower_route(
        &self,
        node_name: &str,
    ) -> Option<(mpsc::Sender<FollowerControlMessage>, u64)> {
        self.followers
            .read()
            .await
            .get(node_name)
            .map(|state| (state.control_tx.clone(), state.session_id))
    }

    fn validate_completion<T>(
        request_id: &str,
        pending: &PendingNodeOperation<T>,
        context: FollowerCompletionContext<'_>,
        expected_kind: NodeOperationKind,
    ) -> Result<()> {
        if context.kind != expected_kind {
            return Err(anyhow!(
                "node completion kind mismatch for '{request_id}': expected {expected_kind:?}, got {:?}",
                context.kind
            ));
        }
        if pending.kind != expected_kind
            || pending.node_name != context.node_name
            || pending.follower_session != context.follower_session
        {
            return Err(anyhow!(
                "unauthenticated node completion correlation for '{request_id}'"
            ));
        }
        Ok(())
    }

    pub async fn record_observed_peer_endpoint(&self, node_name: &str, endpoint: String) {
        let node_name = node_name.trim();
        let endpoint = endpoint.trim();
        if node_name.is_empty() || endpoint.is_empty() {
            return;
        }
        self.observed_peer_endpoints
            .write()
            .await
            .insert(node_name.to_string(), endpoint.to_string());
    }

    pub async fn observed_peer_endpoint(&self, node_name: &str) -> Option<String> {
        self.observed_peer_endpoints
            .read()
            .await
            .get(node_name.trim())
            .cloned()
    }

    /// Notify the service that a new command has been applied.
    /// This is called after each successful write on the leader.
    pub fn notify_entry(&self, entry: ReplicationEntry) {
        let rv = entry.meta.resource_version;
        let mut current = self.current_rv.load(Ordering::Acquire);
        loop {
            if rv <= current {
                break;
            }
            match self
                .current_rv
                .compare_exchange(current, rv, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        let _ = self.stream_tx.send(entry.clone());
        self.entry_tx.send_replace(Some(entry));
    }

    /// Handle a join request from a connecting node.
    ///
    /// Validates the Kubernetes-style bootstrap token and returns accepted/rejected.
    pub async fn handle_join(&self, req: JoinRequest) -> JoinResponse {
        let validation = klights_leader_api::BootstrapTokenValidationRequest::try_new(
            req.token.clone(),
            klights_leader_api::BootstrapTokenScope::Worker,
        );
        if let Err(err) = match validation {
            Ok(request) => {
                self.bootstrap_tokens
                    .validate_bootstrap_token(request)
                    .await
            }
            Err(error) => Err(error),
        } {
            tracing::warn!(node = %req.node_name, error = %err, "join rejected: invalid bootstrap token");
            return JoinResponse::Rejected {
                reason: err.to_string(),
            };
        }

        self.handle_authenticated_join(req).await
    }

    pub(crate) async fn validate_controlplane_bootstrap_token(
        &self,
        token: &str,
    ) -> Result<(), klights_leader_api::BootstrapTokenValidationError> {
        let request = klights_leader_api::BootstrapTokenValidationRequest::try_new(
            token,
            klights_leader_api::BootstrapTokenScope::Controlplane,
        )?;
        self.bootstrap_tokens
            .validate_bootstrap_token(request)
            .await
    }

    /// Handle a join request already authenticated by another mechanism.
    pub async fn handle_authenticated_join(&self, req: JoinRequest) -> JoinResponse {
        // Read cluster metadata for the response
        let metadata = match self.metadata.read_cluster_metadata().await {
            Ok(observation) => observation.into_parts().0,
            Err(e) => {
                tracing::warn!("join rejected: failed to read metadata: {}", e);
                return JoinResponse::Rejected {
                    reason: "leader metadata error".into(),
                };
            }
        };

        tracing::info!(
            node = %req.node_name,
            role = ?req.role,
            cluster_id = %metadata.cluster_id,
            "accepted join request"
        );

        JoinResponse::Accepted {
            cluster_id: metadata.cluster_id,
            leader_epoch: metadata.leader_epoch,
            current_rv: metadata.current_rv,
        }
    }

    /// Handle a metadata request.
    pub async fn handle_metadata(&self) -> MetadataResponse {
        match self.metadata.read_cluster_metadata().await {
            Ok(observation) => {
                let m = observation.into_parts().0;
                let mut metadata = MetadataResponse::from(m);
                // T3: `current_log_apply_index` always returns 0.
                // The raft `last_applied` is the authoritative index.
                metadata.current_log_index = 0;
                metadata
            }
            Err(e) => {
                tracing::warn!("metadata request failed: {}", e);
                MetadataResponse {
                    cluster_id: String::new(),
                    leader_epoch: 0,
                    current_rv: 0,
                    current_log_index: 0,
                    supported_features: crate::replication::protocol::LOCAL_SUPPORTED_FEATURES,
                }
            }
        }
    }

    /// Subscribe to the entry watch channel.
    /// Returns a receiver that yields `Option<ReplicationEntry>`.
    pub fn subscribe_entries(&self) -> watch::Receiver<Option<ReplicationEntry>> {
        self.entry_tx.subscribe()
    }

    pub fn subscribe_stream_entries(&self) -> broadcast::Receiver<ReplicationEntry> {
        self.stream_tx.subscribe()
    }

    pub async fn register_stream_follower(
        self: &Arc<Self>,
        node_name: String,
        session_id: u64,
    ) -> Result<mpsc::Receiver<ReplicationEntry>> {
        self.ensure_fanout_worker().await?;
        let (tx, rx) = mpsc::channel(STREAM_FOLLOWER_QUEUE_CAPACITY);
        self.fanout_pool
            .lock()
            .await
            .add_follower(node_name, session_id, tx);
        Ok(rx)
    }

    async fn ensure_fanout_worker(self: &Arc<Self>) -> Result<()> {
        if self
            .fanout_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        let service = Arc::clone(self);
        let entries = self.stream_tx.subscribe();
        if let Err(err) = self
            .supervisor
            .spawn_async(
                TaskCategory::Network,
                "replication_grpc_fanout",
                async move {
                    service.run_fanout_worker(entries).await;
                },
            )
            .await
        {
            self.fanout_started.store(false, Ordering::Release);
            return Err(err.into());
        }
        Ok(())
    }

    async fn run_fanout_worker(
        self: Arc<Self>,
        mut entries: broadcast::Receiver<ReplicationEntry>,
    ) {
        loop {
            match entries.recv().await {
                Ok(entry) => {
                    let disconnected = self.fanout_pool.lock().await.publish(entry);
                    for (node_name, fanout_session) in disconnected {
                        self.unregister_follower(&node_name, fanout_session).await;
                        tracing::debug!(
                            node = %node_name,
                            "replication follower disconnected from gRPC fanout"
                        );
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(
                        skipped,
                        "replication gRPC fanout lagged behind leader stream"
                    );
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
        self.fanout_started.store(false, Ordering::Release);
    }

    /// Get the current replication position (resource version).
    pub fn current_position(&self) -> i64 {
        self.current_rv.load(Ordering::Acquire)
    }

    pub(crate) async fn register_follower<M>(
        &self,
        metadata: M,
    ) -> (mpsc::Receiver<FollowerControlMessage>, u64)
    where
        M: crate::control_plane::client::IntoFocusedDataplane,
    {
        let metadata = metadata
            .into_focused_dataplane()
            .expect("validated follower dataplane metadata converts losslessly");
        let node_name = metadata.node_name().to_string();
        let session_id = self.next_follower_session.fetch_add(1, Ordering::Relaxed);
        let (control_tx, control_rx) = mpsc::channel(FOLLOWER_CONTROL_QUEUE_CAPACITY);
        self.followers.write().await.insert(
            node_name,
            FollowerState {
                metadata,
                applied_rv: 0,
                control_tx,
                session_id,
            },
        );
        (control_rx, session_id)
    }

    pub async fn update_follower_ack(&self, node_name: &str, applied_rv: i64) {
        if let Some(follower) = self.followers.write().await.get_mut(node_name) {
            follower.applied_rv = follower.applied_rv.max(applied_rv);
        }
    }

    /// Unregister a follower iff the stored session still matches `session_id`.
    ///
    /// Callers that hold a stale session (e.g. a reconnected node whose old
    /// stream just noticed `control_rx` closing) must not remove the active
    /// replacement follower.
    ///
    /// Also sweeps all request/stream pending maps (node exec sync, node exec
    /// streams, pod log, pod log streams, node metrics) and completes every in-flight request or
    /// stream session targeted at the disconnected node so callers do not
    /// block until timeout.
    pub async fn unregister_follower(&self, node_name: &str, session_id: u64) {
        let mut followers = self.followers.write().await;
        let should_remove = followers
            .get(node_name)
            .is_some_and(|state| state.session_id == session_id);
        if should_remove {
            followers.remove(node_name);
        }
        // Only sweep if the follower was actually removed (session matched).
        // A stale unregister (from a reconnected follower's old stream)
        // must not affect in-flight requests for the new session.
        if !should_remove {
            return;
        }

        let disconnected_err = format!("follower '{node_name}' disconnected");

        // Sweep pending node exec sync requests.
        {
            let mut pending = self.pending_node_exec.lock().await;
            let stale: Vec<String> = pending
                .iter()
                .filter(|(_, entry)| {
                    entry.node_name == node_name && entry.follower_session == session_id
                })
                .map(|(id, _)| id.clone())
                .collect();
            for id in &stale {
                if let Some(entry) = pending.remove(id) {
                    let _ = entry
                        .sink
                        .send(Err(ExecSetupError::unavailable(disconnected_err.clone())));
                }
            }
        }

        // Sweep pending pod log requests.
        {
            let mut pending = self.pending_pod_log.lock().await;
            let stale: Vec<String> = pending
                .iter()
                .filter(|(_, entry)| {
                    entry.node_name == node_name && entry.follower_session == session_id
                })
                .map(|(id, _)| id.clone())
                .collect();
            for id in &stale {
                if let Some(entry) = pending.remove(id) {
                    let _ = entry.sink.send(Err(NodeLogSetupError::unavailable(
                        disconnected_err.clone(),
                    )));
                }
            }
        }

        // Sweep pending node metrics requests.
        {
            let mut pending = self.pending_node_metrics.lock().await;
            let stale: Vec<String> = pending
                .iter()
                .filter(|(_, entry)| {
                    entry.node_name == node_name && entry.follower_session == session_id
                })
                .map(|(id, _)| id.clone())
                .collect();
            for id in &stale {
                if let Some(entry) = pending.remove(id) {
                    let _ = entry
                        .sink
                        .send(Err(NodeMetricsError::unavailable(disconnected_err.clone())));
                }
            }
        }

        // Sweep pending node exec streams: drop the sender which causes the
        // stream session's receiver to return None on next recv().
        {
            let mut pending = self.pending_node_exec_streams.lock().await;
            let stale: Vec<String> = pending
                .iter()
                .filter(|(_, entry)| {
                    entry.node_name == node_name && entry.follower_session == session_id
                })
                .map(|(id, _)| id.clone())
                .collect();
            for id in &stale {
                pending.remove(id);
            }
        }

        // Sweep pending pod log streams.
        {
            let mut pending = self.pending_pod_log_streams.lock().await;
            let stale: Vec<String> = pending
                .iter()
                .filter(|(_, entry)| {
                    entry.node_name == node_name && entry.follower_session == session_id
                })
                .map(|(id, _)| id.clone())
                .collect();
            for id in &stale {
                pending.remove(id);
            }
        }
    }

    async fn request_node_exec_sync(
        &self,
        request_id: String,
        request: NodeExecSyncRequest,
    ) -> Result<NodeExecSyncResult, ExecSetupError> {
        let node_name = request.target().node_name().to_string();
        let (control_tx, follower_session) =
            self.follower_route(&node_name).await.ok_or_else(|| {
                ExecSetupError::unavailable(format!("node '{node_name}' is not connected for exec"))
            })?;
        let generation = self.next_operation_generation();

        let (response_tx, response_rx) = oneshot::channel();
        {
            let mut pending = self.pending_node_exec.lock().await;
            match pending.entry(request_id.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(PendingNodeOperation {
                        node_name: node_name.clone(),
                        follower_session,
                        kind: NodeOperationKind::ExecSync,
                        generation,
                        sink: response_tx,
                    });
                }
                std::collections::hash_map::Entry::Occupied(_) => {
                    return Err(ExecSetupError::duplicate_session(format!(
                        "duplicate node exec request id '{request_id}'"
                    )));
                }
            }
        }

        if let Err(err) = control_tx
            .send(FollowerControlMessage::NodeExecSync(
                RoutedNodeExecSyncRequest {
                    request_id: request_id.clone(),
                    request,
                },
            ))
            .await
        {
            let mut pending = self.pending_node_exec.lock().await;
            if pending
                .get(&request_id)
                .is_some_and(|entry| entry.generation == generation)
            {
                pending.remove(&request_id);
            }
            return Err(ExecSetupError::unavailable(format!(
                "node '{node_name}' exec stream is closed: {err}"
            )));
        }

        match self
            .supervisor
            .timeout(
                "node_exec_sync_response_timeout",
                NODE_EXEC_SYNC_TIMEOUT,
                response_rx,
            )
            .await
            .map_err(|err| {
                ExecSetupError::unavailable(format!("wait for node exec response: {err}"))
            })? {
            Ok(Ok(response)) => response,
            Ok(Err(_closed)) => Err(ExecSetupError::unavailable(format!(
                "node '{node_name}' exec response channel closed"
            ))),
            Err(_elapsed) => {
                let mut pending = self.pending_node_exec.lock().await;
                if pending
                    .get(&request_id)
                    .is_some_and(|entry| entry.generation == generation)
                {
                    pending.remove(&request_id);
                }
                Err(ExecSetupError::timeout(format!(
                    "node '{node_name}' exec response timed out after {:?}",
                    NODE_EXEC_SYNC_TIMEOUT
                )))
            }
        }
    }

    pub(crate) async fn complete_node_exec_sync(
        &self,
        context: FollowerCompletionContext<'_>,
        response: RoutedNodeExecSyncResponse,
    ) -> Result<()> {
        let mut pending = self.pending_node_exec.lock().await;
        let Some(entry) = pending.get(&response.request_id) else {
            return Err(anyhow!(
                "unknown node exec response id '{}'",
                response.request_id
            ));
        };
        Self::validate_completion(
            &response.request_id,
            entry,
            context,
            NodeOperationKind::ExecSync,
        )?;
        let entry = pending
            .remove(&response.request_id)
            .expect("validated pending exec response remains present");
        let _ = entry.sink.send(Ok(response.result));
        Ok(())
    }

    async fn request_node_metrics(
        &self,
        request_id: String,
        request: NodeMetricsRequest,
    ) -> Result<NodeMetricsResult, NodeMetricsError> {
        let node_name = request.target().node_name().to_string();
        let (control_tx, follower_session) =
            self.follower_route(&node_name).await.ok_or_else(|| {
                NodeMetricsError::unavailable(format!(
                    "node '{node_name}' is not connected for metrics"
                ))
            })?;
        let generation = self.next_operation_generation();

        let (response_tx, response_rx) = oneshot::channel();
        {
            let mut pending = self.pending_node_metrics.lock().await;
            if pending.contains_key(&request_id) {
                return Err(NodeMetricsError::duplicate_request(format!(
                    "duplicate node metrics request id '{request_id}'"
                )));
            }
            pending.insert(
                request_id.clone(),
                PendingNodeOperation {
                    node_name: node_name.clone(),
                    follower_session,
                    kind: NodeOperationKind::Metrics,
                    generation,
                    sink: response_tx,
                },
            );
        }

        if let Err(err) = control_tx
            .send(FollowerControlMessage::NodeMetrics(
                RoutedNodeMetricsRequest {
                    request_id: request_id.clone(),
                    request,
                },
            ))
            .await
        {
            let mut pending = self.pending_node_metrics.lock().await;
            if pending
                .get(&request_id)
                .is_some_and(|entry| entry.generation == generation)
            {
                pending.remove(&request_id);
            }
            return Err(NodeMetricsError::unavailable(format!(
                "node '{node_name}' metrics stream is closed: {err}"
            )));
        }

        match self
            .supervisor
            .timeout(
                "node_metrics_response_timeout",
                NODE_METRICS_TIMEOUT,
                response_rx,
            )
            .await
            .map_err(|error| {
                NodeMetricsError::unavailable(format!("wait for node metrics response: {error}"))
            })? {
            Ok(Ok(response)) => response,
            Ok(Err(_closed)) => Err(NodeMetricsError::closed(format!(
                "node '{node_name}' metrics response channel closed"
            ))),
            Err(_elapsed) => {
                let mut pending = self.pending_node_metrics.lock().await;
                if pending
                    .get(&request_id)
                    .is_some_and(|entry| entry.generation == generation)
                {
                    pending.remove(&request_id);
                }
                Err(NodeMetricsError::timeout(format!(
                    "node '{node_name}' metrics response timed out after {:?}",
                    NODE_METRICS_TIMEOUT
                )))
            }
        }
    }

    pub(crate) async fn complete_node_metrics(
        &self,
        context: FollowerCompletionContext<'_>,
        response: RoutedNodeMetricsResponse,
    ) -> Result<()> {
        let mut pending = self.pending_node_metrics.lock().await;
        let Some(entry) = pending.get(&response.request_id) else {
            return Err(anyhow!(
                "unknown node metrics response id '{}'",
                response.request_id
            ));
        };
        Self::validate_completion(
            &response.request_id,
            entry,
            context,
            NodeOperationKind::Metrics,
        )?;
        if response.node_name != context.node_name {
            return Err(anyhow!(
                "node metrics payload source mismatch for '{}': authenticated '{}', payload '{}'",
                response.request_id,
                context.node_name,
                response.node_name
            ));
        }
        if let Ok(result) = &response.result
            && result.target().node_name() != context.node_name
        {
            return Err(anyhow!(
                "node metrics result target mismatch for '{}'",
                response.request_id
            ));
        }
        let entry = pending
            .remove(&response.request_id)
            .expect("validated pending metrics response remains present");
        let _ = entry.sink.send(response.result);
        Ok(())
    }

    async fn open_node_exec_stream(
        &self,
        request_id: String,
        request: NodeExecRequest,
    ) -> Result<ReplicationNodeExecSession, ExecSetupError> {
        let node_name = request.target().node_name().to_string();
        let (control_tx, follower_session) =
            self.follower_route(&node_name).await.ok_or_else(|| {
                ExecSetupError::unavailable(format!("node '{node_name}' is not connected for exec"))
            })?;
        let generation = self.next_operation_generation();

        let (frame_tx, frame_rx) = mpsc::channel(NODE_EXEC_STREAM_FRAME_QUEUE_CAPACITY);
        {
            let mut pending = self.pending_node_exec_streams.lock().await;
            match pending.entry(request_id.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(PendingNodeOperation {
                        node_name: node_name.clone(),
                        follower_session,
                        kind: NodeOperationKind::ExecStream,
                        generation,
                        sink: frame_tx,
                    });
                }
                std::collections::hash_map::Entry::Occupied(_) => {
                    return Err(ExecSetupError::duplicate_session(format!(
                        "duplicate node exec stream id '{request_id}'"
                    )));
                }
            }
        }

        if let Err(err) = control_tx
            .send(FollowerControlMessage::NodeExec(RoutedNodeExecRequest {
                request_id: request_id.clone(),
                request,
            }))
            .await
        {
            let mut pending = self.pending_node_exec_streams.lock().await;
            if pending
                .get(&request_id)
                .is_some_and(|entry| entry.generation == generation)
            {
                pending.remove(&request_id);
            }
            return Err(ExecSetupError::unavailable(format!(
                "node '{node_name}' exec stream is closed before stream start: {err}"
            )));
        }

        Ok(ReplicationNodeExecSession {
            request_id,
            generation,
            control_tx,
            inbound_rx: Mutex::new(frame_rx),
            pending: self.pending_node_exec_streams.clone(),
            cancelled: AtomicBool::new(false),
        })
    }

    pub(crate) async fn complete_node_exec_stream_frame(
        &self,
        context: FollowerCompletionContext<'_>,
        frame: RoutedNodeExecFrame,
    ) -> Result<()> {
        let request_id = frame.request_id.clone();
        let (sender, generation) = {
            let pending = self.pending_node_exec_streams.lock().await;
            let Some(entry) = pending.get(&request_id) else {
                return Err(anyhow!("unknown node exec stream id '{request_id}'"));
            };
            Self::validate_completion(&request_id, entry, context, NodeOperationKind::ExecStream)?;
            (entry.sink.clone(), entry.generation)
        };

        let should_close = frame.frame.is_terminal();
        if sender.send(frame).await.is_err() {
            let mut pending = self.pending_node_exec_streams.lock().await;
            if pending
                .get(&request_id)
                .is_some_and(|entry| entry.generation == generation)
            {
                pending.remove(&request_id);
            }
            return Err(anyhow!(
                "node exec stream receiver closed for '{request_id}'"
            ));
        }
        if should_close {
            let mut pending = self.pending_node_exec_streams.lock().await;
            if pending
                .get(&request_id)
                .is_some_and(|entry| entry.generation == generation)
            {
                pending.remove(&request_id);
            }
        }
        Ok(())
    }

    async fn request_node_log(
        &self,
        request_id: String,
        request: NodeLogRequest,
    ) -> Result<NodeLogResult, NodeLogSetupError> {
        let node_name = request.target().node_name().to_string();
        let (control_tx, follower_session) =
            self.follower_route(&node_name).await.ok_or_else(|| {
                NodeLogSetupError::unavailable(format!(
                    "node '{node_name}' is not connected for pod log"
                ))
            })?;
        let generation = self.next_operation_generation();

        let (response_tx, response_rx) = oneshot::channel();
        {
            let mut pending = self.pending_pod_log.lock().await;
            match pending.entry(request_id.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(PendingNodeOperation {
                        node_name: node_name.clone(),
                        follower_session,
                        kind: NodeOperationKind::Log,
                        generation,
                        sink: response_tx,
                    });
                }
                std::collections::hash_map::Entry::Occupied(_) => {
                    return Err(NodeLogSetupError::duplicate_stream(format!(
                        "duplicate pod log request id '{request_id}'"
                    )));
                }
            }
        }

        if let Err(err) = control_tx
            .send(FollowerControlMessage::PodLog(RoutedNodeLogRequest {
                request_id: request_id.clone(),
                follow: false,
                request,
            }))
            .await
        {
            let mut pending = self.pending_pod_log.lock().await;
            if pending
                .get(&request_id)
                .is_some_and(|entry| entry.generation == generation)
            {
                pending.remove(&request_id);
            }
            return Err(NodeLogSetupError::unavailable(format!(
                "node '{node_name}' pod log stream is closed: {err}"
            )));
        }

        match self
            .supervisor
            .timeout(
                "pod_log_response_timeout",
                self.pod_log_timeout,
                response_rx,
            )
            .await
            .map_err(|err| {
                NodeLogSetupError::unavailable(format!("wait for node pod log response: {err}"))
            })? {
            Ok(Ok(response)) => response,
            Ok(Err(_closed)) => Err(NodeLogSetupError::unavailable(format!(
                "node '{node_name}' pod log response channel closed"
            ))),
            Err(_elapsed) => {
                let mut pending = self.pending_pod_log.lock().await;
                if pending
                    .get(&request_id)
                    .is_some_and(|entry| entry.generation == generation)
                {
                    pending.remove(&request_id);
                }
                Err(NodeLogSetupError::timeout(format!(
                    "node '{node_name}' pod log response timed out after {:?}",
                    self.pod_log_timeout
                )))
            }
        }
    }

    async fn open_node_log_stream(
        &self,
        request_id: String,
        request: NodeLogRequest,
    ) -> Result<ReplicationNodeLogStream, NodeLogSetupError> {
        let node_name = request.target().node_name().to_string();
        let (control_tx, follower_session) =
            self.follower_route(&node_name).await.ok_or_else(|| {
                NodeLogSetupError::unavailable(format!(
                    "node '{node_name}' is not connected for pod log"
                ))
            })?;
        let generation = self.next_operation_generation();

        let (frame_tx, frame_rx) = mpsc::channel(POD_LOG_STREAM_FRAME_QUEUE_CAPACITY);
        {
            let mut pending = self.pending_pod_log_streams.lock().await;
            match pending.entry(request_id.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(PendingNodeOperation {
                        node_name: node_name.clone(),
                        follower_session,
                        kind: NodeOperationKind::Log,
                        generation,
                        sink: frame_tx,
                    });
                }
                std::collections::hash_map::Entry::Occupied(_) => {
                    return Err(NodeLogSetupError::duplicate_stream(format!(
                        "duplicate pod log stream id '{request_id}'"
                    )));
                }
            }
        }

        if let Err(err) = control_tx
            .send(FollowerControlMessage::PodLog(RoutedNodeLogRequest {
                request_id: request_id.clone(),
                follow: true,
                request,
            }))
            .await
        {
            let mut pending = self.pending_pod_log_streams.lock().await;
            if pending
                .get(&request_id)
                .is_some_and(|entry| entry.generation == generation)
            {
                pending.remove(&request_id);
            }
            return Err(NodeLogSetupError::unavailable(format!(
                "node '{node_name}' pod log stream is closed before stream start: {err}"
            )));
        }

        Ok(ReplicationNodeLogStream {
            request_id,
            generation,
            inbound_rx: Mutex::new(frame_rx),
            pending: self.pending_pod_log_streams.clone(),
            cancelled: AtomicBool::new(false),
        })
    }

    pub(crate) async fn complete_node_log_event(
        &self,
        context: FollowerCompletionContext<'_>,
        routed: RoutedNodeLogEvent,
    ) -> Result<()> {
        {
            let mut pending = self.pending_pod_log.lock().await;
            if let Some(entry) = pending.get(&routed.request_id) {
                Self::validate_completion(
                    &routed.request_id,
                    entry,
                    context,
                    NodeOperationKind::Log,
                )?;
                let entry = pending
                    .remove(&routed.request_id)
                    .expect("validated pending log response remains present");
                let (content, terminal_error, _) = routed.event.into_parts();
                let result = match terminal_error {
                    Some(error) => NodeLogResult::failed(content, error),
                    None => NodeLogResult::success(content),
                };
                let _ = entry.sink.send(Ok(result));
                return Ok(());
            }
        }

        let request_id = routed.request_id.clone();
        let should_close = routed.event.is_terminal();
        let (sender, generation) = {
            let pending = self.pending_pod_log_streams.lock().await;
            let Some(entry) = pending.get(&request_id) else {
                return Err(anyhow!("unknown pod log response id '{request_id}'"));
            };
            Self::validate_completion(&request_id, entry, context, NodeOperationKind::Log)?;
            (entry.sink.clone(), entry.generation)
        };
        if sender.send(routed).await.is_err() {
            let mut pending = self.pending_pod_log_streams.lock().await;
            if pending
                .get(&request_id)
                .is_some_and(|entry| entry.generation == generation)
            {
                pending.remove(&request_id);
            }
            return Err(anyhow!("pod log stream receiver closed for '{request_id}'"));
        }
        if should_close {
            let mut pending = self.pending_pod_log_streams.lock().await;
            if pending
                .get(&request_id)
                .is_some_and(|entry| entry.generation == generation)
            {
                pending.remove(&request_id);
            }
        }
        Ok(())
    }

    pub async fn follower_metrics(&self) -> FollowerMetrics {
        let current_rv = self.current_position();
        let followers = self.followers.read().await;
        let mut statuses: Vec<FollowerStatus> = followers
            .values()
            .map(|state| {
                let lag = current_rv.saturating_sub(state.applied_rv).max(0);
                FollowerStatus {
                    node_name: state.metadata.node_name().to_string(),
                    applied_rv: state.applied_rv,
                    lag,
                    mode: match state.metadata.mode() {
                        klights_leader_api::NetworkNodeMode::Root => "root",
                        klights_leader_api::NetworkNodeMode::Rootless => "rootless",
                    }
                    .to_string(),
                    encryption: match state.metadata.encryption() {
                        klights_leader_api::DataplaneEncryption::WireGuard => "enabled",
                        klights_leader_api::DataplaneEncryption::Direct => "disabled",
                    }
                    .to_string(),
                    public_key: state.metadata.public_key().map(str::to_string),
                }
            })
            .collect();
        statuses.sort_by(|a, b| a.node_name.cmp(&b.node_name));
        FollowerMetrics {
            follower_count: statuses.len(),
            max_lag: statuses.iter().map(|status| status.lag).max().unwrap_or(0),
            followers: statuses,
        }
    }
}

impl NodeExec for ReplicationService {
    fn exec_sync(&self, request: NodeExecSyncRequest) -> NodeExecFuture<'_, NodeExecSyncResult> {
        Box::pin(async move {
            let request_id = crate::replication::new_command_id().to_string();
            self.request_node_exec_sync(request_id, request).await
        })
    }

    fn open_exec(&self, request: NodeExecRequest) -> NodeExecFuture<'_, Box<dyn NodeExecSession>> {
        Box::pin(async move {
            let request_id = crate::replication::new_command_id().to_string();
            let session = self.open_node_exec_stream(request_id, request).await?;
            Ok(Box::new(session) as Box<dyn NodeExecSession>)
        })
    }
}

impl NodeLog for ReplicationService {
    fn read_logs(&self, request: NodeLogRequest) -> NodeLogFuture<'_, NodeLogResult> {
        Box::pin(async move {
            let request_id = crate::replication::new_command_id().to_string();
            self.request_node_log(request_id, request).await
        })
    }

    fn open_logs(
        &self,
        request: NodeLogRequest,
    ) -> NodeLogFuture<'_, Box<dyn BoundedByteStream<Frame = NodeLogEvent>>> {
        Box::pin(async move {
            let request_id = crate::replication::new_command_id().to_string();
            let stream = self.open_node_log_stream(request_id, request).await?;
            Ok(Box::new(stream) as Box<dyn BoundedByteStream<Frame = NodeLogEvent>>)
        })
    }
}

impl NodeMetrics for ReplicationService {
    fn collect_metrics(
        &self,
        request: NodeMetricsRequest,
    ) -> NodeMetricsFuture<'_, NodeMetricsResult> {
        Box::pin(async move {
            let request_id = crate::replication::new_command_id().to_string();
            self.request_node_metrics(request_id, request).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::command::{
        COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand,
    };
    use klights_node_api::ExecStreamChannel;
    use klights_supervisor::TaskCategoryConfig;
    use serde_json::json;

    async fn test_service() -> ReplicationService {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));

        // Initialize cluster metadata (required for join validation)
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();

        ReplicationService::new(db, supervisor)
    }

    async fn create_scoped_token_for_test(
        db: &dyn crate::datastore::backend::DatastoreBackend,
        token: &str,
        scope: crate::bootstrap::bootstrap_token::BootstrapTokenScope,
    ) {
        crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_for_test(
            db, scope, token,
        )
        .await
        .unwrap();
    }

    fn sample_node_log_request(
        node_name: &str,
        follow: bool,
        tail_lines: Option<usize>,
    ) -> NodeLogRequest {
        NodeLogRequest::new(
            klights_node_api::NodeLogTarget::try_new(
                node_name,
                "sonobuoy",
                "sonobuoy-e2e-job",
                "pod-uid",
                "e2e",
            )
            .unwrap(),
            klights_node_api::NodeLogOptions::new(
                follow.then(|| "true".to_string()),
                tail_lines,
                None,
                None,
                None,
                None,
                None,
            ),
        )
    }

    fn sample_node_exec_request(node_name: &str) -> NodeExecRequest {
        let target = klights_node_api::NodeExecTarget::try_new(
            node_name,
            "default",
            "test-pod",
            "containerd://abc",
        )
        .unwrap();
        NodeExecRequest::exec(
            target,
            vec!["sh".to_string()],
            klights_node_api::ExecStreamOptions::new(false, true, false, false),
        )
    }

    fn sample_entry(rv: i64) -> ReplicationEntry {
        ReplicationEntry {
            command: StorageCommand::CreateResource {
                api_version: "v1".into(),
                kind: "ConfigMap".into(),
                namespace: Some("default".into()),
                name: "test".into(),
                data: json!({"metadata": {"name": "test"}}),
            },
            meta: CommandMeta {
                command_id: CommandId(format!("replication-service-sample-{rv}")),
                codec_version: COMMAND_CODEC_VERSION,
                resource_version: rv,
                uid: None,
                timestamp_ms: 0,
                authoring_node: "test".into(),
            },
        }
    }

    #[tokio::test]
    async fn service_starts_idle_without_error() {
        let service = test_service().await;
        assert_eq!(service.current_position(), 0);
    }

    #[tokio::test]
    async fn notify_entry_updates_position() {
        let service = test_service().await;
        service.notify_entry(sample_entry(42));
        assert_eq!(service.current_position(), 42);
    }

    #[tokio::test]
    async fn subscribe_receives_entries() {
        let service = test_service().await;
        let mut rx = service.subscribe_entries();

        service.notify_entry(sample_entry(1));

        // Watch channel should have the latest value
        assert!(rx.changed().await.is_ok());
        let entry = rx.borrow().clone();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().meta.resource_version, 1);
    }

    #[tokio::test]
    async fn stream_subscription_receives_every_entry() {
        let service = test_service().await;
        let mut rx = service.subscribe_stream_entries();

        service.notify_entry(sample_entry(1));
        service.notify_entry(sample_entry(2));

        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();
        assert_eq!(first.meta.resource_version, 1);
        assert_eq!(second.meta.resource_version, 2);
    }

    #[tokio::test]
    async fn stream_subscription_receives_out_of_order_older_entry() {
        let service = test_service().await;
        let mut rx = service.subscribe_stream_entries();

        service.notify_entry(sample_entry(2));
        service.notify_entry(sample_entry(1));

        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();
        assert_eq!(first.meta.resource_version, 2);
        assert_eq!(second.meta.resource_version, 1);
        assert_eq!(service.current_position(), 2);
    }

    #[tokio::test]
    async fn fanout_stream_follower_receives_live_entries_without_using_broadcast_directly() {
        let service = Arc::new(test_service().await);
        let mut follower = service
            .register_stream_follower("replica-1".to_string(), 1)
            .await
            .unwrap();

        service.notify_entry(sample_entry(1));
        service.notify_entry(sample_entry(2));

        let first = follower.recv().await.unwrap();
        let second = follower.recv().await.unwrap();
        assert_eq!(first.meta.resource_version, 1);
        assert_eq!(second.meta.resource_version, 2);
    }

    #[tokio::test]
    async fn fanout_stream_replaces_existing_node_sender_on_rejoin() {
        let service = Arc::new(test_service().await);
        let metadata_a = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "replica-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Enabled,
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            Some("127.0.0.1".to_string()),
            Some(51_820),
        )
        .unwrap();
        let metadata_b = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "replica-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Enabled,
            Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=".to_string()),
            Some("127.0.0.1".to_string()),
            Some(51_821),
        )
        .unwrap();

        let (_control_a, session_a) = service.register_follower(metadata_a).await;
        let mut old_stream = service
            .register_stream_follower("replica-1".to_string(), session_a)
            .await
            .unwrap();
        let (_control_b, session_b) = service.register_follower(metadata_b.clone()).await;
        let mut new_stream = service
            .register_stream_follower("replica-1".to_string(), session_b)
            .await
            .unwrap();

        assert!(matches!(
            old_stream.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ));

        service.notify_entry(sample_entry(3));
        assert_eq!(new_stream.recv().await.unwrap().meta.resource_version, 3);

        let metrics = service.follower_metrics().await;
        let expected_key = metadata_b.public_key.as_ref().map(ToString::to_string);
        assert_eq!(
            metrics.followers[0].public_key.as_deref(),
            expected_key.as_deref()
        );
    }

    #[tokio::test]
    async fn fanout_delivers_to_500_followers_without_head_of_line_blocking() {
        let service = Arc::new(test_service().await);
        let mut followers = Vec::new();
        for idx in 0..500 {
            followers.push(
                service
                    .register_stream_follower(format!("replica-{idx}"), idx as u64)
                    .await
                    .unwrap(),
            );
        }

        service.notify_entry(sample_entry(500));

        for follower in &mut followers {
            let entry = tokio::time::timeout(std::time::Duration::from_secs(1), follower.recv())
                .await
                .expect("fanout receiver timed out")
                .expect("fanout sender should stay connected");
            assert_eq!(entry.meta.resource_version, 500);
        }
    }

    #[tokio::test]
    async fn pod_log_follow_stream_routes_chunks_until_terminal_frame() {
        let service = Arc::new(test_service().await);
        let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (mut follower_rx, follower_session) = service.register_follower(metadata).await;

        let session = service
            .open_node_log_stream(
                "log-stream-1".to_string(),
                sample_node_log_request("worker-1", true, Some(200)),
            )
            .await
            .unwrap();

        let Some(FollowerControlMessage::PodLog(request)) = follower_rx.recv().await else {
            panic!("expected pod log follow request");
        };
        assert_eq!(request.request_id, "log-stream-1");
        assert!(request.follow);
        assert_eq!(request.request.options().tail_lines(), Some(200));

        service
            .complete_node_log_event(
                FollowerCompletionContext::new(
                    "worker-1",
                    follower_session,
                    NodeOperationKind::Log,
                ),
                RoutedNodeLogEvent {
                    request_id: "log-stream-1".to_string(),
                    event: NodeLogEvent::data(b"first\n".to_vec()),
                },
            )
            .await
            .unwrap();
        service
            .complete_node_log_event(
                FollowerCompletionContext::new(
                    "worker-1",
                    follower_session,
                    NodeOperationKind::Log,
                ),
                RoutedNodeLogEvent {
                    request_id: "log-stream-1".to_string(),
                    event: NodeLogEvent::data(b"second\n".to_vec()),
                },
            )
            .await
            .unwrap();
        service
            .complete_node_log_event(
                FollowerCompletionContext::new(
                    "worker-1",
                    follower_session,
                    NodeOperationKind::Log,
                ),
                RoutedNodeLogEvent {
                    request_id: "log-stream-1".to_string(),
                    event: NodeLogEvent::terminal(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            session.recv_frame().await.unwrap().unwrap().content(),
            b"first\n"
        );
        assert_eq!(
            session.recv_frame().await.unwrap().unwrap().content(),
            b"second\n"
        );
        let terminal = session.recv_frame().await.unwrap().unwrap();
        assert!(terminal.is_terminal());
    }

    #[tokio::test]
    async fn handle_join_accepts_valid_token() {
        let service = test_service().await;
        let token = crate::bootstrap::bootstrap_token::ensure_default_bootstrap_token(
            service.db.as_deref().expect("test datastore"),
            std::time::Duration::from_secs(3600),
        )
        .await
        .unwrap();

        let req = JoinRequest {
            token,
            node_name: "worker-1".into(),
            role: crate::replication::protocol::JoinRole::Worker,
        };

        let resp = service.handle_join(req).await;
        match resp {
            JoinResponse::Accepted { cluster_id, .. } => {
                assert!(!cluster_id.is_empty());
            }
            JoinResponse::Rejected { reason } => {
                panic!("expected accepted, got rejected: {reason}");
            }
        }
    }

    #[tokio::test]
    async fn handle_authenticated_join_does_not_send_service_account_signer_to_worker() {
        let db = Arc::new(crate::datastore::test_support::in_memory().await);
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(db.as_ref())
            .await
            .unwrap();
        let namespace_dir = tempfile::tempdir().unwrap();
        let namespace = namespace_dir.path().to_string_lossy().to_string();
        crate::auth::persist_service_account_signing_key(&namespace, "signing-key", &supervisor)
            .await
            .unwrap();
        let service = ReplicationService::new_with_containerd_namespace(db, supervisor, namespace);

        let worker_resp = service
            .handle_authenticated_join(JoinRequest {
                token: "token".into(),
                node_name: "worker-1".into(),
                role: crate::replication::protocol::JoinRole::Worker,
            })
            .await;
        assert!(
            matches!(worker_resp, JoinResponse::Accepted { .. }),
            "expected accepted worker join"
        );
        let json = serde_json::to_string(&worker_resp).unwrap();
        assert!(!json.contains("service_account_signing_key_pem"));
    }

    #[tokio::test]
    async fn handle_join_rejects_controlplane_token_for_worker_join() {
        let service = test_service().await;
        create_scoped_token_for_test(
            service.db.as_deref().expect("test datastore"),
            "abcdef.0123456789abcdef",
            crate::bootstrap::bootstrap_token::BootstrapTokenScope::Controlplane,
        )
        .await;

        let req = JoinRequest {
            token: "abcdef.0123456789abcdef".into(),
            node_name: "worker-1".into(),
            role: crate::replication::protocol::JoinRole::Worker,
        };

        let resp = service.handle_join(req).await;
        match resp {
            JoinResponse::Rejected { reason } => {
                assert!(reason.contains("worker bootstrap token"), "{reason}");
            }
            JoinResponse::Accepted { .. } => {
                panic!("worker join must reject a controlplane bootstrap token");
            }
        }
    }

    #[tokio::test]
    async fn handle_join_rejects_invalid_token() {
        let service = test_service().await;

        let req = JoinRequest {
            token: "wrong-token".into(),
            node_name: "worker-1".into(),
            role: crate::replication::protocol::JoinRole::Worker,
        };

        let resp = service.handle_join(req).await;
        match resp {
            JoinResponse::Rejected { reason } => {
                assert!(reason.contains("bootstrap token"));
            }
            JoinResponse::Accepted { .. } => {
                panic!("expected rejected for bad token");
            }
        }
    }

    #[tokio::test]
    async fn handle_join_rejects_expired_bootstrap_token() {
        let service = test_service().await;
        crate::bootstrap::bootstrap_token::create_scoped_bootstrap_token_secret_with_ttl_for_test(
            service.db.as_deref().expect("test datastore"),
            crate::bootstrap::bootstrap_token::BootstrapTokenScope::Worker,
            "abcdef.0123456789abcdef",
            std::time::Duration::from_secs(0),
        )
        .await
        .unwrap();

        let req = JoinRequest {
            token: "abcdef.0123456789abcdef".into(),
            node_name: "worker-1".into(),
            role: crate::replication::protocol::JoinRole::Worker,
        };

        let resp = service.handle_join(req).await;
        match resp {
            JoinResponse::Rejected { reason } => {
                assert!(reason.contains("expired"));
            }
            JoinResponse::Accepted { .. } => {
                panic!("expected rejected for expired bootstrap token");
            }
        }
    }

    #[tokio::test]
    async fn handle_metadata_returns_values() {
        let service = test_service().await;
        let resp = service.handle_metadata().await;
        assert!(!resp.cluster_id.is_empty());
        assert_eq!(resp.leader_epoch, 0);
        assert_eq!(resp.current_log_index, 0);
    }

    #[tokio::test]
    async fn service_no_replica_connection_required() {
        // The service starts and is fully functional without any replica.
        let service = test_service().await;
        // Just verify we can create and use it
        assert_eq!(service.current_position(), 0);
        service.notify_entry(sample_entry(5));
        assert_eq!(service.current_position(), 5);
    }

    #[tokio::test]
    async fn follower_metrics_track_ack_lag_and_disconnect() {
        let service = test_service().await;
        service.notify_entry(sample_entry(10));
        let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "replica-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();

        let (_control_rx, session_id) = service.register_follower(metadata).await;
        service.update_follower_ack("replica-1", 7).await;

        let metrics = service.follower_metrics().await;
        assert_eq!(metrics.follower_count, 1);
        assert_eq!(metrics.max_lag, 3);
        assert_eq!(metrics.followers[0].node_name, "replica-1");

        service.unregister_follower("replica-1", session_id).await;
        assert_eq!(service.follower_metrics().await.follower_count, 0);
    }

    /// Old-session unregister must never remove a reconnected follower.
    #[tokio::test]
    async fn reconnect_race_old_session_unregister_must_not_remove_new_follower() {
        let service = test_service().await;
        let metadata_a = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "replica-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Enabled,
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            Some("127.0.0.1".to_string()),
            Some(51_820),
        )
        .unwrap();
        let metadata_b = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "replica-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Enabled,
            Some("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=".to_string()),
            Some("127.0.0.1".to_string()),
            Some(51_821),
        )
        .unwrap();

        let (_control_rx_a, session_a) = service.register_follower(metadata_a).await;

        // Reconnect — this must invalidate session_a's control channel and
        // assign a new session.
        let (_control_rx_b, session_b) = service.register_follower(metadata_b.clone()).await;
        assert_ne!(
            session_a, session_b,
            "reconnect must produce a new session id"
        );

        // The old stream observes control_rx_a closed, breaks out of its loop,
        // and calls unregister_follower with the stale session_a.
        service.unregister_follower("replica-1", session_a).await;

        // The new follower (session_b) must still be registered.
        let metrics = service.follower_metrics().await;
        assert_eq!(
            metrics.follower_count, 1,
            "new follower must survive old-session unregister"
        );
        let expected_key = metadata_b.public_key.as_ref().map(ToString::to_string);
        assert_eq!(
            metrics.followers[0].public_key.as_deref(),
            expected_key.as_deref(),
            "surviving follower must be the reconnected session"
        );

        // A legitimate unregister with the current session must still work.
        service.unregister_follower("replica-1", session_b).await;
        assert_eq!(service.follower_metrics().await.follower_count, 0);
    }

    /// When the replication node-exec session is dropped without cancellation, the
    /// pending entry must be removed by the Drop impl.
    #[tokio::test]
    async fn node_exec_stream_session_drop_clears_pending_entry() {
        let service = Arc::new(test_service().await);
        let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (_control_rx, _session_id) = service.register_follower(metadata).await;

        let session = service
            .open_node_exec_stream(
                "drop-test-1".to_string(),
                sample_node_exec_request("worker-1"),
            )
            .await
            .unwrap();

        // Verify the session was registered.
        {
            let pending = service.pending_node_exec_streams.lock().await;
            assert!(
                pending.contains_key("drop-test-1"),
                "pending entry must exist before drop"
            );
        }

        // Drop the session without calling close().
        drop(session);

        // The pending entry must be gone.
        let pending = service.pending_node_exec_streams.lock().await;
        assert!(
            !pending.contains_key("drop-test-1"),
            "pending entry must be removed on drop"
        );
    }

    /// Same drop-safety for the replication node-log stream.
    #[tokio::test]
    async fn pod_log_stream_session_drop_clears_pending_entry() {
        let service = Arc::new(test_service().await);
        let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (_control_rx, _session_id) = service.register_follower(metadata).await;

        let session = service
            .open_node_log_stream(
                "log-drop-1".to_string(),
                sample_node_log_request("worker-1", true, None),
            )
            .await
            .unwrap();

        {
            let pending = service.pending_pod_log_streams.lock().await;
            assert!(
                pending.contains_key("log-drop-1"),
                "pending pod log entry must exist before drop"
            );
        }

        drop(session);

        let pending = service.pending_pod_log_streams.lock().await;
        assert!(
            !pending.contains_key("log-drop-1"),
            "pending pod log entry must be removed on drop"
        );
    }

    /// When a follower disconnects, unregister_follower must sweep the pending
    /// maps and complete every in-flight request/stream targeted at that node.
    /// Without this, callers block until timeout.
    #[tokio::test]
    async fn unregister_follower_completes_pending_requests() {
        let service = test_service().await;

        // Register a follower.
        let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "test-node".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Enabled,
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            Some("127.0.0.1".to_string()),
            Some(51_820),
        )
        .unwrap();
        let (_control_rx, session_id) = service.register_follower(metadata).await;

        // Manually insert a pending node-exec sync request for this node.
        let (exec_tx, mut exec_rx) = tokio::sync::oneshot::channel();
        service.pending_node_exec.lock().await.insert(
            "exec-req-1".to_string(),
            PendingNodeOperation {
                node_name: "test-node".to_string(),
                follower_session: session_id,
                kind: NodeOperationKind::ExecSync,
                generation: 1,
                sink: exec_tx,
            },
        );

        // Manually insert a pending pod-log request for this node.
        let (log_tx, mut log_rx) = tokio::sync::oneshot::channel();
        service.pending_pod_log.lock().await.insert(
            "log-req-1".to_string(),
            PendingNodeOperation {
                node_name: "test-node".to_string(),
                follower_session: session_id,
                kind: NodeOperationKind::Log,
                generation: 2,
                sink: log_tx,
            },
        );

        // Manually insert a pending node metrics request for this node.
        let (metrics_tx, mut metrics_rx) = tokio::sync::oneshot::channel();
        service.pending_node_metrics.lock().await.insert(
            "metrics-req-1".to_string(),
            PendingNodeOperation {
                node_name: "test-node".to_string(),
                follower_session: session_id,
                kind: NodeOperationKind::Metrics,
                generation: 3,
                sink: metrics_tx,
            },
        );

        // Also register a request for a DIFFERENT node — it must survive.
        let (other_tx, mut other_rx) = tokio::sync::oneshot::channel();
        service.pending_node_exec.lock().await.insert(
            "exec-req-2".to_string(),
            PendingNodeOperation {
                node_name: "other-node".to_string(),
                follower_session: 999,
                kind: NodeOperationKind::ExecSync,
                generation: 4,
                sink: other_tx,
            },
        );

        // Unregister the follower for test-node.
        service.unregister_follower("test-node", session_id).await;

        // The pending requests for test-node MUST be completed with an error.
        let exec_result = exec_rx.try_recv().expect("exec oneshot must be resolved");
        assert!(
            exec_result.is_err(),
            "pending exec must fail on follower disconnect"
        );
        let exec_err = exec_result.unwrap_err().to_string();
        assert!(
            exec_err.contains("test-node"),
            "exec error must mention the disconnected node: {exec_err}"
        );

        let log_result = log_rx.try_recv().expect("pod log oneshot must be resolved");
        assert!(
            log_result.is_err(),
            "pending pod log must fail on follower disconnect"
        );
        assert!(
            log_result.unwrap_err().to_string().contains("test-node"),
            "pod log error must mention the disconnected node"
        );
        let metrics_result = metrics_rx
            .try_recv()
            .expect("node metrics oneshot must be resolved");
        assert!(
            metrics_result.is_err(),
            "pending node metrics must fail on follower disconnect"
        );
        assert!(
            metrics_result
                .unwrap_err()
                .to_string()
                .contains("test-node"),
            "node metrics error must mention the disconnected node"
        );

        // The request for other-node must NOT be affected.
        assert!(
            other_rx.try_recv().is_err(),
            "other-node request must survive unregister of a different follower"
        );
        assert!(
            service
                .pending_node_exec
                .lock()
                .await
                .contains_key("exec-req-2"),
            "other-node pending entry must not be removed"
        );

        // The pending maps must NOT contain the test-node entries anymore.
        assert!(
            !service
                .pending_node_exec
                .lock()
                .await
                .contains_key("exec-req-1"),
            "test-node exec entry must be removed"
        );
        assert!(
            !service
                .pending_pod_log
                .lock()
                .await
                .contains_key("log-req-1"),
            "test-node log entry must be removed"
        );
        assert!(
            !service
                .pending_node_metrics
                .lock()
                .await
                .contains_key("metrics-req-1"),
            "test-node metrics entry must be removed"
        );
    }

    #[tokio::test]
    async fn request_node_metrics_sends_control_message_and_completes_response() {
        let service = Arc::new(test_service().await);
        let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (mut control_rx, session_id) = service.register_follower(metadata).await;

        let service_for_request = service.clone();
        let request_task = tokio::spawn(async move {
            service_for_request
                .request_node_metrics(
                    "metrics-1".to_string(),
                    NodeMetricsRequest::new(
                        klights_node_api::NodeMetricsTarget::try_new("worker-1").unwrap(),
                        Vec::new(),
                    ),
                )
                .await
                .unwrap()
        });

        let Some(FollowerControlMessage::NodeMetrics(request)) = control_rx.recv().await else {
            panic!("expected node metrics request");
        };
        assert_eq!(request.request_id, "metrics-1");
        assert_eq!(request.request.target().node_name(), "worker-1");

        service
            .complete_node_metrics(
                FollowerCompletionContext::new("worker-1", session_id, NodeOperationKind::Metrics),
                RoutedNodeMetricsResponse {
                    request_id: request.request_id,
                    node_name: "worker-1".to_string(),
                    result: Ok(NodeMetricsResult::new(
                        request.request.target().clone(),
                        None,
                        Vec::new(),
                    )),
                },
            )
            .await
            .unwrap();

        let response = request_task.await.unwrap();
        assert_eq!(response.target().node_name(), "worker-1");
        assert!(response.node().is_none());
    }

    #[tokio::test]
    async fn duplicate_node_metrics_correlation_does_not_replace_original_waiter() {
        let service = Arc::new(test_service().await);
        let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (_control_rx, session_id) = service.register_follower(metadata).await;
        let (original_tx, original_rx) = tokio::sync::oneshot::channel();
        service.pending_node_metrics.lock().await.insert(
            "metrics-duplicate".to_string(),
            PendingNodeOperation {
                node_name: "worker-1".to_string(),
                follower_session: session_id,
                kind: NodeOperationKind::Metrics,
                generation: service.next_operation_generation(),
                sink: original_tx,
            },
        );

        let error = service
            .request_node_metrics(
                "metrics-duplicate".to_string(),
                NodeMetricsRequest::new(
                    klights_node_api::NodeMetricsTarget::try_new("worker-1").unwrap(),
                    Vec::new(),
                ),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, NodeMetricsError::DuplicateRequest { .. }));

        let expected = NodeMetricsResult::new(
            klights_node_api::NodeMetricsTarget::try_new("worker-1").unwrap(),
            None,
            Vec::new(),
        );
        service
            .complete_node_metrics(
                FollowerCompletionContext::new("worker-1", session_id, NodeOperationKind::Metrics),
                RoutedNodeMetricsResponse {
                    request_id: "metrics-duplicate".to_string(),
                    node_name: "worker-1".to_string(),
                    result: Ok(expected.clone()),
                },
            )
            .await
            .unwrap();
        assert_eq!(original_rx.await.unwrap().unwrap(), expected);
    }

    #[tokio::test]
    async fn authenticated_completion_mismatch_never_consumes_metrics_waiter() {
        let service = Arc::new(test_service().await);
        let worker = |name: &str| {
            crate::networking::wireguard::DataplanePeerMetadata::try_new(
                name.to_string(),
                crate::networking::wireguard::DataplaneMode::Root,
                crate::networking::wireguard::DataplaneEncryption::Disabled,
                None,
                Some("127.0.0.1".to_string()),
                None,
            )
            .unwrap()
        };
        let (mut worker_one_rx, worker_one_session) =
            service.register_follower(worker("worker-1")).await;
        let (_worker_two_rx, worker_two_session) =
            service.register_follower(worker("worker-2")).await;

        let request_service = service.clone();
        let request = tokio::spawn(async move {
            request_service
                .request_node_metrics(
                    "shared-metrics-id".to_string(),
                    NodeMetricsRequest::new(
                        klights_node_api::NodeMetricsTarget::try_new("worker-1").unwrap(),
                        Vec::new(),
                    ),
                )
                .await
        });
        let Some(FollowerControlMessage::NodeMetrics(routed)) = worker_one_rx.recv().await else {
            panic!("expected metrics request");
        };
        let response = || RoutedNodeMetricsResponse {
            request_id: routed.request_id.clone(),
            node_name: "worker-1".to_string(),
            result: Ok(NodeMetricsResult::new(
                klights_node_api::NodeMetricsTarget::try_new("worker-1").unwrap(),
                None,
                Vec::new(),
            )),
        };

        for context in [
            FollowerCompletionContext::new(
                "worker-2",
                worker_two_session,
                NodeOperationKind::Metrics,
            ),
            FollowerCompletionContext::new(
                "worker-1",
                worker_one_session,
                NodeOperationKind::ExecSync,
            ),
        ] {
            assert!(
                service
                    .complete_node_metrics(context, response())
                    .await
                    .is_err()
            );
            assert!(
                service
                    .pending_node_metrics
                    .lock()
                    .await
                    .contains_key("shared-metrics-id")
            );
        }

        let mismatched_payload = RoutedNodeMetricsResponse {
            node_name: "worker-2".to_string(),
            ..response()
        };
        assert!(
            service
                .complete_node_metrics(
                    FollowerCompletionContext::new(
                        "worker-1",
                        worker_one_session,
                        NodeOperationKind::Metrics,
                    ),
                    mismatched_payload,
                )
                .await
                .is_err()
        );
        assert!(
            service
                .pending_node_metrics
                .lock()
                .await
                .contains_key("shared-metrics-id")
        );

        service
            .complete_node_metrics(
                FollowerCompletionContext::new(
                    "worker-1",
                    worker_one_session,
                    NodeOperationKind::Metrics,
                ),
                response(),
            )
            .await
            .unwrap();
        assert_eq!(
            request.await.unwrap().unwrap().target().node_name(),
            "worker-1"
        );
    }

    #[tokio::test]
    async fn stale_exec_stream_completion_cannot_remove_reused_request_id() {
        let service = Arc::new(test_service().await);
        let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (mut control_rx, follower_session) = service.register_follower(metadata).await;
        let old_session = service
            .open_node_exec_stream(
                "reused-exec-id".to_string(),
                sample_node_exec_request("worker-1"),
            )
            .await
            .unwrap();
        assert!(matches!(
            control_rx.recv().await,
            Some(FollowerControlMessage::NodeExec(_))
        ));
        let context = FollowerCompletionContext::new(
            "worker-1",
            follower_session,
            NodeOperationKind::ExecStream,
        );
        for _ in 0..NODE_EXEC_STREAM_FRAME_QUEUE_CAPACITY {
            service
                .complete_node_exec_stream_frame(
                    context,
                    RoutedNodeExecFrame {
                        request_id: "reused-exec-id".to_string(),
                        frame: NodeExecFrame::new(ExecStreamChannel::Stdout, vec![1], false),
                    },
                )
                .await
                .unwrap();
        }
        let blocked_service = service.clone();
        let blocked = tokio::spawn(async move {
            blocked_service
                .complete_node_exec_stream_frame(
                    context,
                    RoutedNodeExecFrame {
                        request_id: "reused-exec-id".to_string(),
                        frame: NodeExecFrame::new(ExecStreamChannel::Error, Vec::new(), true),
                    },
                )
                .await
        });
        tokio::task::yield_now().await;
        drop(old_session);
        let replacement = service
            .open_node_exec_stream(
                "reused-exec-id".to_string(),
                sample_node_exec_request("worker-1"),
            )
            .await
            .unwrap();
        assert!(matches!(
            control_rx.recv().await,
            Some(FollowerControlMessage::NodeExec(_))
        ));
        assert!(blocked.await.unwrap().is_err());

        service
            .complete_node_exec_stream_frame(
                context,
                RoutedNodeExecFrame {
                    request_id: "reused-exec-id".to_string(),
                    frame: NodeExecFrame::new(ExecStreamChannel::Stdout, b"new".to_vec(), false),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            replacement.recv_frame().await.unwrap().unwrap().data(),
            b"new"
        );
    }

    #[tokio::test]
    async fn unregister_follower_closes_pending_node_exec_stream_immediately() {
        let service = Arc::new(test_service().await);
        let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (mut control_rx, session_id) = service.register_follower(metadata).await;

        let session = service
            .open_node_exec_stream(
                "exec-stream-disconnect-1".to_string(),
                sample_node_exec_request("worker-1"),
            )
            .await
            .unwrap();

        let routed = control_rx
            .recv()
            .await
            .expect("control request must be routed");
        assert!(matches!(
            routed,
            FollowerControlMessage::NodeExec(request)
                if request.request_id == "exec-stream-disconnect-1"
        ));

        service.unregister_follower("worker-1", session_id).await;

        let closed =
            tokio::time::timeout(std::time::Duration::from_millis(100), session.recv_frame())
                .await
                .expect("stream recv must resolve immediately after follower disconnect")
                .unwrap();
        assert!(
            closed.is_none(),
            "disconnect must close the exec stream receiver"
        );

        assert!(
            !service
                .pending_node_exec_streams
                .lock()
                .await
                .contains_key("exec-stream-disconnect-1"),
            "pending exec stream entry must be swept"
        );
    }

    #[tokio::test]
    async fn unregister_follower_closes_pending_pod_log_stream_immediately() {
        let service = Arc::new(test_service().await);
        let metadata = crate::networking::wireguard::DataplanePeerMetadata::try_new(
            "worker-1".to_string(),
            crate::networking::wireguard::DataplaneMode::Root,
            crate::networking::wireguard::DataplaneEncryption::Disabled,
            None,
            Some("127.0.0.1".to_string()),
            None,
        )
        .unwrap();
        let (mut control_rx, session_id) = service.register_follower(metadata).await;

        let session = service
            .open_node_log_stream(
                "pod-log-stream-disconnect-1".to_string(),
                sample_node_log_request("worker-1", true, None),
            )
            .await
            .unwrap();

        let routed = control_rx
            .recv()
            .await
            .expect("control request must be routed");
        assert!(matches!(
            routed,
            FollowerControlMessage::PodLog(request)
                if request.request_id == "pod-log-stream-disconnect-1"
        ));

        service.unregister_follower("worker-1", session_id).await;

        let closed =
            tokio::time::timeout(std::time::Duration::from_millis(100), session.recv_frame())
                .await
                .expect("stream recv must resolve immediately after follower disconnect")
                .unwrap();
        assert!(
            closed.is_none(),
            "disconnect must close the pod log stream receiver"
        );

        assert!(
            !service
                .pending_pod_log_streams
                .lock()
                .await
                .contains_key("pod-log-stream-disconnect-1"),
            "pending pod log stream entry must be swept"
        );
    }
}
