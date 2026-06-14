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
use anyhow::{Context, anyhow};
use tokio::sync::{Mutex, RwLock, broadcast, mpsc, oneshot, watch};

use super::protocol::{
    FollowerControlMessage, JoinRequest, JoinResponse, MetadataResponse, NodeExecRequest,
    NodeExecStreamFrame, NodeExecSyncRequest, NodeExecSyncResponse, PodLogRequest, PodLogResponse,
    ReplicationEntry,
};

use crate::datastore::backend::DatastoreBackend;
use crate::networking::wireguard::DataplanePeerMetadata;
use crate::replication::grpc::fanout::FanoutPool;
use crate::task_supervisor::{TaskCategory, TaskSupervisor};

const STREAM_FOLLOWER_QUEUE_CAPACITY: usize = 1024;
const FOLLOWER_CONTROL_QUEUE_CAPACITY: usize = 64;
const FANOUT_BATCH_SIZE: usize = 64;
const NODE_EXEC_SYNC_TIMEOUT: Duration = Duration::from_secs(300);
const NODE_EXEC_STREAM_FRAME_QUEUE_CAPACITY: usize = 128;
const POD_LOG_STREAM_FRAME_QUEUE_CAPACITY: usize = 128;

type PendingNodeExecStreams =
    Arc<Mutex<HashMap<String, (String, mpsc::Sender<NodeExecStreamFrame>)>>>;
type PendingPodLogStreams = Arc<Mutex<HashMap<String, (String, mpsc::Sender<PodLogResponse>)>>>;
type PendingNodeExecSync =
    Mutex<HashMap<String, (String, oneshot::Sender<Result<NodeExecSyncResponse>>)>>;
type PendingPodLogSync = Mutex<HashMap<String, (String, oneshot::Sender<Result<PodLogResponse>>)>>;

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
    metadata: DataplanePeerMetadata,
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
    /// Datastore reference for metadata/token validation.
    db: Arc<dyn DatastoreBackend>,
    /// Task supervisor for all spawned tasks.
    supervisor: Arc<TaskSupervisor>,
    next_follower_session: AtomicU64,
    followers: RwLock<HashMap<String, FollowerState>>,
    pending_node_exec: PendingNodeExecSync,
    pending_node_exec_streams: PendingNodeExecStreams,
    pending_pod_log: PendingPodLogSync,
    pending_pod_log_streams: PendingPodLogStreams,
    pod_log_timeout: Duration,
    fanout_pool: Mutex<FanoutPool<ReplicationEntry>>,
    fanout_started: AtomicBool,
    observed_peer_endpoints: RwLock<HashMap<String, String>>,
}

pub struct NodeExecStreamSession {
    request_id: String,
    control_tx: mpsc::Sender<FollowerControlMessage>,
    inbound_rx: mpsc::Receiver<NodeExecStreamFrame>,
    pending: PendingNodeExecStreams,
}

impl NodeExecStreamSession {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub async fn send_frame(&self, mut frame: NodeExecStreamFrame) -> Result<()> {
        if frame.request_id.trim().is_empty() {
            frame.request_id = self.request_id.clone();
        }
        self.control_tx
            .send(FollowerControlMessage::NodeExecFrame(frame))
            .await
            .map_err(|err| anyhow!("node exec stream control channel closed: {err}"))
    }

    pub async fn recv_frame(&mut self) -> Result<Option<NodeExecStreamFrame>> {
        Ok(self.inbound_rx.recv().await)
    }

    pub async fn close(&self) {
        self.pending.lock().await.remove(&self.request_id);
    }
}

impl Drop for NodeExecStreamSession {
    fn drop(&mut self) {
        // Best-effort cleanup: the session might be dropped in a sync context
        // (e.g. during stack unwind). Use try_lock to avoid blocking.
        if let Ok(mut pending) = self.pending.try_lock() {
            pending.remove(&self.request_id);
        }
    }
}

pub struct PodLogStreamSession {
    request_id: String,
    inbound_rx: mpsc::Receiver<PodLogResponse>,
    pending: PendingPodLogStreams,
}

impl PodLogStreamSession {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub async fn recv_response(&mut self) -> Result<Option<PodLogResponse>> {
        Ok(self.inbound_rx.recv().await)
    }

    pub async fn close(&self) {
        self.pending.lock().await.remove(&self.request_id);
    }
}

impl Drop for PodLogStreamSession {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.pending.try_lock() {
            pending.remove(&self.request_id);
        }
    }
}

impl ReplicationService {
    /// Create a new idle replication service.
    ///
    /// The service is idle-silent until a replica connects.
    /// No background tasks are spawned at creation time.
    pub fn new(db: Arc<dyn DatastoreBackend>, supervisor: Arc<TaskSupervisor>) -> Self {
        Self::new_with_containerd_namespace(db, supervisor, crate::paths::runtime_namespace())
    }

    pub fn new_with_containerd_namespace(
        db: Arc<dyn DatastoreBackend>,
        supervisor: Arc<TaskSupervisor>,
        _containerd_namespace: String,
    ) -> Self {
        let (entry_tx, _) = watch::channel(None);
        let (stream_tx, _) = broadcast::channel(1024);
        Self {
            entry_tx,
            stream_tx,
            current_rv: AtomicI64::new(0),
            db,
            supervisor,
            next_follower_session: AtomicU64::new(1),
            followers: RwLock::new(HashMap::new()),
            pending_node_exec: Mutex::new(HashMap::new()),
            pending_node_exec_streams: Arc::new(Mutex::new(HashMap::new())),
            pending_pod_log: Mutex::new(HashMap::new()),
            pending_pod_log_streams: Arc::new(Mutex::new(HashMap::new())),
            pod_log_timeout: Duration::from_secs(30),
            fanout_pool: Mutex::new(FanoutPool::new(FANOUT_BATCH_SIZE)),
            fanout_started: AtomicBool::new(false),
            observed_peer_endpoints: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn task_supervisor(&self) -> Arc<TaskSupervisor> {
        self.supervisor.clone()
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
        if let Err(err) = crate::bootstrap::bootstrap_token::validate_bootstrap_token_for_scope(
            self.db.as_ref(),
            &req.token,
            crate::bootstrap::bootstrap_token::BootstrapTokenScope::Worker,
        )
        .await
        {
            tracing::warn!(node = %req.node_name, error = %err, "join rejected: invalid bootstrap token");
            return JoinResponse::Rejected {
                reason: err.to_string(),
            };
        }

        self.handle_authenticated_join(req).await
    }

    /// Handle a join request already authenticated by another mechanism.
    pub async fn handle_authenticated_join(&self, req: JoinRequest) -> JoinResponse {
        // Read cluster metadata for the response
        let metadata =
            match crate::bootstrap::cluster_meta::read_cluster_metadata(self.db.as_ref()).await {
                Ok(m) => m,
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
        match crate::bootstrap::cluster_meta::read_cluster_metadata(self.db.as_ref()).await {
            Ok(m) => {
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
                }
            }
        }
    }

    pub async fn handle_cluster_membership(
        &self,
    ) -> crate::control_plane::client::membership::ClusterMembership {
        match crate::bootstrap::cluster_meta::read_cluster_membership(self.db.as_ref()).await {
            Ok(membership) => membership,
            Err(e) => {
                tracing::warn!("cluster membership request failed: {}", e);
                crate::control_plane::client::membership::ClusterMembership {
                    cluster_id: String::new(),
                    voters: Vec::new(),
                    term: 0,
                    leader_hint: None,
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
            return Err(err);
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

    pub async fn register_follower(
        &self,
        metadata: DataplanePeerMetadata,
    ) -> (mpsc::Receiver<FollowerControlMessage>, u64) {
        let node_name = metadata.node_name.clone();
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
    /// Also sweeps all four pending maps (node exec sync, node exec streams,
    /// pod log, pod log streams) and completes every in-flight request or
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
                .filter(|(_, (n, _))| n.as_str() == node_name)
                .map(|(id, _)| id.clone())
                .collect();
            for id in &stale {
                if let Some((_, tx)) = pending.remove(id) {
                    let _ = tx.send(Err(anyhow::anyhow!("{disconnected_err}")));
                }
            }
        }

        // Sweep pending pod log requests.
        {
            let mut pending = self.pending_pod_log.lock().await;
            let stale: Vec<String> = pending
                .iter()
                .filter(|(_, (n, _))| n.as_str() == node_name)
                .map(|(id, _)| id.clone())
                .collect();
            for id in &stale {
                if let Some((_, tx)) = pending.remove(id) {
                    let _ = tx.send(Err(anyhow::anyhow!("{disconnected_err}")));
                }
            }
        }

        // Sweep pending node exec streams: drop the sender which causes the
        // stream session's receiver to return None on next recv().
        {
            let mut pending = self.pending_node_exec_streams.lock().await;
            let stale: Vec<String> = pending
                .iter()
                .filter(|(_, (n, _))| n.as_str() == node_name)
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
                .filter(|(_, (n, _))| n.as_str() == node_name)
                .map(|(id, _)| id.clone())
                .collect();
            for id in &stale {
                pending.remove(id);
            }
        }
    }

    pub async fn request_node_exec_sync(
        &self,
        mut request: NodeExecSyncRequest,
    ) -> Result<NodeExecSyncResponse> {
        if request.request_id.trim().is_empty() {
            request.request_id = crate::datastore::command::CommandId::new().to_string();
        }
        let request_id = request.request_id.clone();
        let node_name = request.node_name.clone();
        let control_tx = {
            let followers = self.followers.read().await;
            followers
                .get(&node_name)
                .map(|state| state.control_tx.clone())
                .ok_or_else(|| anyhow!("node '{node_name}' is not connected for exec"))?
        };

        let (response_tx, response_rx) = oneshot::channel();
        {
            let mut pending = self.pending_node_exec.lock().await;
            if pending
                .insert(request_id.clone(), (node_name.clone(), response_tx))
                .is_some()
            {
                return Err(anyhow!("duplicate node exec request id '{request_id}'"));
            }
        }

        if let Err(err) = control_tx
            .send(FollowerControlMessage::NodeExecSync(request))
            .await
        {
            self.pending_node_exec.lock().await.remove(&request_id);
            return Err(anyhow!("node '{node_name}' exec stream is closed: {err}"));
        }

        match self
            .supervisor
            .timeout(
                "node_exec_sync_response_timeout",
                NODE_EXEC_SYNC_TIMEOUT,
                response_rx,
            )
            .await
            .context("wait for node exec response")?
        {
            Ok(Ok(response)) => response,
            Ok(Err(_closed)) => Err(anyhow!("node '{node_name}' exec response channel closed")),
            Err(_elapsed) => {
                self.pending_node_exec.lock().await.remove(&request_id);
                Err(anyhow!(
                    "node '{node_name}' exec response timed out after {:?}",
                    NODE_EXEC_SYNC_TIMEOUT
                ))
            }
        }
    }

    pub async fn complete_node_exec_sync(&self, response: NodeExecSyncResponse) -> Result<()> {
        let Some((_node_name, waiter)) = self
            .pending_node_exec
            .lock()
            .await
            .remove(&response.request_id)
        else {
            return Err(anyhow!(
                "unknown node exec response id '{}'",
                response.request_id
            ));
        };
        let _ = waiter.send(Ok(response));
        Ok(())
    }

    pub async fn open_node_exec_stream(
        &self,
        mut request: NodeExecRequest,
    ) -> Result<NodeExecStreamSession> {
        if request.request_id.trim().is_empty() {
            request.request_id = crate::datastore::command::CommandId::new().to_string();
        }
        let request_id = request.request_id.clone();
        let node_name = request.node_name.clone();
        let control_tx = {
            let followers = self.followers.read().await;
            followers
                .get(&node_name)
                .map(|state| state.control_tx.clone())
                .ok_or_else(|| anyhow!("node '{node_name}' is not connected for exec"))?
        };

        let (frame_tx, frame_rx) = mpsc::channel(NODE_EXEC_STREAM_FRAME_QUEUE_CAPACITY);
        {
            let mut pending = self.pending_node_exec_streams.lock().await;
            if pending
                .insert(request_id.clone(), (node_name.clone(), frame_tx))
                .is_some()
            {
                return Err(anyhow!("duplicate node exec stream id '{request_id}'"));
            }
        }

        if let Err(err) = control_tx
            .send(FollowerControlMessage::NodeExec(request))
            .await
        {
            self.pending_node_exec_streams
                .lock()
                .await
                .remove(&request_id);
            return Err(anyhow!(
                "node '{node_name}' exec stream is closed before stream start: {err}"
            ));
        }

        Ok(NodeExecStreamSession {
            request_id,
            control_tx,
            inbound_rx: frame_rx,
            pending: self.pending_node_exec_streams.clone(),
        })
    }

    pub async fn complete_node_exec_stream_frame(&self, frame: NodeExecStreamFrame) -> Result<()> {
        let request_id = frame.request_id.clone();
        let sender = {
            let pending = self.pending_node_exec_streams.lock().await;
            pending.get(&request_id).map(|(_, s)| s.clone())
        };
        let Some(sender) = sender else {
            return Err(anyhow!("unknown node exec stream id '{request_id}'"));
        };

        let should_close = super::protocol::node_exec_error_frame_is_terminal(&frame);
        if sender.send(frame).await.is_err() {
            self.pending_node_exec_streams
                .lock()
                .await
                .remove(&request_id);
            return Err(anyhow!(
                "node exec stream receiver closed for '{request_id}'"
            ));
        }
        if should_close {
            self.pending_node_exec_streams
                .lock()
                .await
                .remove(&request_id);
        }
        Ok(())
    }

    pub async fn request_pod_log(&self, mut request: PodLogRequest) -> Result<PodLogResponse> {
        if request.request_id.trim().is_empty() {
            request.request_id = crate::datastore::command::CommandId::new().to_string();
        }
        request.follow = None;
        let request_id = request.request_id.clone();
        let node_name = request.node_name.clone();
        let control_tx = {
            let followers = self.followers.read().await;
            followers
                .get(&node_name)
                .map(|state| state.control_tx.clone())
                .ok_or_else(|| anyhow!("node '{node_name}' is not connected for pod log"))?
        };

        let (response_tx, response_rx) = oneshot::channel();
        {
            let mut pending = self.pending_pod_log.lock().await;
            if pending
                .insert(request_id.clone(), (node_name.clone(), response_tx))
                .is_some()
            {
                return Err(anyhow!("duplicate pod log request id '{request_id}'"));
            }
        }

        if let Err(err) = control_tx
            .send(FollowerControlMessage::PodLog(request))
            .await
        {
            self.pending_pod_log.lock().await.remove(&request_id);
            return Err(anyhow!(
                "node '{node_name}' pod log stream is closed: {err}"
            ));
        }

        match self
            .supervisor
            .timeout(
                "pod_log_response_timeout",
                self.pod_log_timeout,
                response_rx,
            )
            .await
            .context("wait for node pod log response")?
        {
            Ok(Ok(response)) => response,
            Ok(Err(_closed)) => Err(anyhow!(
                "node '{node_name}' pod log response channel closed"
            )),
            Err(_elapsed) => {
                self.pending_pod_log.lock().await.remove(&request_id);
                Err(anyhow!(
                    "node '{node_name}' pod log response timed out after {:?}",
                    self.pod_log_timeout
                ))
            }
        }
    }

    pub async fn request_pod_log_stream(
        &self,
        mut request: PodLogRequest,
    ) -> Result<PodLogStreamSession> {
        if request.request_id.trim().is_empty() {
            request.request_id = crate::datastore::command::CommandId::new().to_string();
        }
        request.follow = Some("true".to_string());
        let request_id = request.request_id.clone();
        let node_name = request.node_name.clone();
        let control_tx = {
            let followers = self.followers.read().await;
            followers
                .get(&node_name)
                .map(|state| state.control_tx.clone())
                .ok_or_else(|| anyhow!("node '{node_name}' is not connected for pod log"))?
        };

        let (frame_tx, frame_rx) = mpsc::channel(POD_LOG_STREAM_FRAME_QUEUE_CAPACITY);
        {
            let mut pending = self.pending_pod_log_streams.lock().await;
            if pending
                .insert(request_id.clone(), (node_name.clone(), frame_tx))
                .is_some()
            {
                return Err(anyhow!("duplicate pod log stream id '{request_id}'"));
            }
        }

        if let Err(err) = control_tx
            .send(FollowerControlMessage::PodLog(request))
            .await
        {
            self.pending_pod_log_streams
                .lock()
                .await
                .remove(&request_id);
            return Err(anyhow!(
                "node '{node_name}' pod log stream is closed before stream start: {err}"
            ));
        }

        Ok(PodLogStreamSession {
            request_id,
            inbound_rx: frame_rx,
            pending: self.pending_pod_log_streams.clone(),
        })
    }

    pub async fn complete_pod_log(&self, response: PodLogResponse) -> Result<()> {
        if let Some((_node_name, waiter)) = self
            .pending_pod_log
            .lock()
            .await
            .remove(&response.request_id)
        {
            let _ = waiter.send(Ok(response));
            return Ok(());
        }

        let request_id = response.request_id.clone();
        let sender = {
            let pending = self.pending_pod_log_streams.lock().await;
            pending.get(&request_id).map(|(_, s)| s.clone())
        };
        let Some(sender) = sender else {
            return Err(anyhow!("unknown pod log response id '{request_id}'"));
        };
        let should_close = response.fin || response.error.is_some();
        if sender.send(response).await.is_err() {
            self.pending_pod_log_streams
                .lock()
                .await
                .remove(&request_id);
            return Err(anyhow!("pod log stream receiver closed for '{request_id}'"));
        }
        if should_close {
            self.pending_pod_log_streams
                .lock()
                .await
                .remove(&request_id);
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
                    node_name: state.metadata.node_name.clone(),
                    applied_rv: state.applied_rv,
                    lag,
                    mode: state.metadata.mode.as_str().to_string(),
                    encryption: state.metadata.encryption.as_str().to_string(),
                    public_key: state.metadata.public_key.as_ref().map(ToString::to_string),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::command::{
        COMMAND_CODEC_VERSION, CommandId, CommandMeta, StorageCommand,
    };
    use crate::task_supervisor::TaskCategoryConfig;
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
                command_id: CommandId::new(),
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
        let (mut follower_rx, _follower_session) = service.register_follower(metadata).await;

        let mut session = service
            .request_pod_log_stream(PodLogRequest {
                request_id: "log-stream-1".to_string(),
                node_name: "worker-1".to_string(),
                namespace: "sonobuoy".to_string(),
                pod_name: "sonobuoy-e2e-job".to_string(),
                pod_uid: "pod-uid".to_string(),
                container_name: "e2e".to_string(),
                follow: Some("true".to_string()),
                tail_lines: Some("200".to_string()),
                timestamps: None,
                since_time: None,
                since_seconds: None,
                limit_bytes: None,
                previous: None,
            })
            .await
            .unwrap();

        let Some(FollowerControlMessage::PodLog(request)) = follower_rx.recv().await else {
            panic!("expected pod log follow request");
        };
        assert_eq!(request.request_id, "log-stream-1");
        assert_eq!(request.follow.as_deref(), Some("true"));
        assert_eq!(request.tail_lines.as_deref(), Some("200"));

        service
            .complete_pod_log(PodLogResponse {
                request_id: "log-stream-1".to_string(),
                log_content: b"first\n".to_vec(),
                error: None,
                fin: false,
            })
            .await
            .unwrap();
        service
            .complete_pod_log(PodLogResponse {
                request_id: "log-stream-1".to_string(),
                log_content: b"second\n".to_vec(),
                error: None,
                fin: false,
            })
            .await
            .unwrap();
        service
            .complete_pod_log(PodLogResponse {
                request_id: "log-stream-1".to_string(),
                log_content: Vec::new(),
                error: None,
                fin: true,
            })
            .await
            .unwrap();

        assert_eq!(
            session.recv_response().await.unwrap().unwrap().log_content,
            b"first\n"
        );
        assert_eq!(
            session.recv_response().await.unwrap().unwrap().log_content,
            b"second\n"
        );
        let terminal = session.recv_response().await.unwrap().unwrap();
        assert!(terminal.fin);
        assert!(session.recv_response().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn handle_join_accepts_valid_token() {
        let service = test_service().await;
        let token = crate::bootstrap::bootstrap_token::ensure_default_bootstrap_token(
            service.db.as_ref(),
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
            service.db.as_ref(),
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
            service.db.as_ref(),
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

    /// When a NodeExecStreamSession is dropped without calling close(), the
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
            .open_node_exec_stream(NodeExecRequest {
                request_id: "drop-test-1".to_string(),
                node_name: "worker-1".to_string(),
                namespace: "default".to_string(),
                pod_name: "test-pod".to_string(),
                container_id: "containerd://abc".to_string(),
                command: vec!["sh".to_string()],
                tty: false,
                stdin: false,
                stdout: true,
                stderr: false,
            })
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

    /// Same drop-safety for PodLogStreamSession.
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
            .request_pod_log_stream(PodLogRequest {
                request_id: "log-drop-1".to_string(),
                node_name: "worker-1".to_string(),
                namespace: "default".to_string(),
                pod_name: "test-pod".to_string(),
                pod_uid: "uid-1".to_string(),
                container_name: "app".to_string(),
                follow: None,
                tail_lines: None,
                timestamps: None,
                since_time: None,
                since_seconds: None,
                limit_bytes: None,
                previous: None,
            })
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
        service
            .pending_node_exec
            .lock()
            .await
            .insert("exec-req-1".to_string(), ("test-node".to_string(), exec_tx));

        // Manually insert a pending pod-log request for this node.
        let (log_tx, mut log_rx) = tokio::sync::oneshot::channel();
        service
            .pending_pod_log
            .lock()
            .await
            .insert("log-req-1".to_string(), ("test-node".to_string(), log_tx));

        // Also register a request for a DIFFERENT node — it must survive.
        let (other_tx, mut other_rx) = tokio::sync::oneshot::channel();
        service.pending_node_exec.lock().await.insert(
            "exec-req-2".to_string(),
            ("other-node".to_string(), other_tx),
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

        let mut session = service
            .open_node_exec_stream(NodeExecRequest {
                request_id: "exec-stream-disconnect-1".to_string(),
                node_name: "worker-1".to_string(),
                namespace: "default".to_string(),
                pod_name: "test-pod".to_string(),
                container_id: "containerd://abc".to_string(),
                command: vec!["sh".to_string()],
                tty: false,
                stdin: false,
                stdout: true,
                stderr: false,
            })
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

        let mut session = service
            .request_pod_log_stream(PodLogRequest {
                request_id: "pod-log-stream-disconnect-1".to_string(),
                node_name: "worker-1".to_string(),
                namespace: "default".to_string(),
                pod_name: "test-pod".to_string(),
                pod_uid: "uid-1".to_string(),
                container_name: "app".to_string(),
                follow: None,
                tail_lines: None,
                timestamps: None,
                since_time: None,
                since_seconds: None,
                limit_bytes: None,
                previous: None,
            })
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

        let closed = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            session.recv_response(),
        )
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
