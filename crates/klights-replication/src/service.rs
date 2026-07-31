//! Leader-side authenticated node-control service.
//!
//! Accepts worker/control-plane sessions and multiplexes focused exec, log,
//! metrics, join, and metadata operations. Committed Raft entries travel
//! through OpenRaft's peer RPC transport, not this node-control stream.
//!
//! ## Design invariants
//! - Idle-silent when no replicas connect (zero CPU).
//! - All tasks spawned through `TaskSupervisor`.
//! - No direct `tokio::spawn`, sleeps, or intervals.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use anyhow::anyhow;
use klights_leader_api::{JoinRequest, JoinResponse, MetadataResponse};
use klights_node_api::{
    BoundedByteStream, ByteStreamBounds, ByteStreamError, ByteStreamFuture, ExecSetupError,
    FollowerControlMessage, NodeExecFrame, NodeExecRequest, NodeExecSession, NodeExecSyncRequest,
    NodeExecSyncResult, NodeLogEvent, NodeLogRequest, NodeLogResult, NodeLogSetupError,
    NodeMetricsError, NodeMetricsRequest, NodeMetricsResult, RoutedNodeExecFrame,
    RoutedNodeExecRequest, RoutedNodeExecSyncRequest, RoutedNodeExecSyncResponse,
    RoutedNodeLogEvent, RoutedNodeLogRequest, RoutedNodeMetricsRequest, RoutedNodeMetricsResponse,
};
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};

use klights_node_api::{FollowerCompletionContext, NodeOperationKind};

use klights_leader_api::NetworkDataplane;
use klights_supervisor::TaskSupervisor;

const FOLLOWER_CONTROL_QUEUE_CAPACITY: usize = 64;
const NODE_EXEC_SYNC_TIMEOUT: Duration = Duration::from_secs(300);
const NODE_METRICS_TIMEOUT: Duration = Duration::from_secs(15);
const NODE_EXEC_STREAM_FRAME_QUEUE_CAPACITY: usize = 128;
const POD_LOG_STREAM_FRAME_QUEUE_CAPACITY: usize = 128;

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

impl klights_leader_api::LeaderFollowerDiagnostics for ReplicationService {
    fn follower_diagnostics(&self) -> klights_leader_api::FollowerDiagnosticsFuture<'_> {
        Box::pin(async move {
            let metrics = self.follower_metrics().await;
            klights_leader_api::FollowerDiagnostics {
                follower_count: metrics.follower_count,
                max_lag: metrics.max_lag,
                followers: metrics
                    .followers
                    .into_iter()
                    .map(|follower| klights_leader_api::FollowerDiagnostic {
                        node_name: follower.node_name,
                        applied_resource_version: follower.applied_rv,
                        lag: follower.lag,
                        mode: follower.mode,
                        encryption: follower.encryption,
                        public_key: follower.public_key,
                    })
                    .collect(),
            }
        })
    }
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
/// Owns authenticated worker/control-plane sessions and the focused node
/// operation channels multiplexed over the leader control stream.
pub struct ReplicationService {
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
    pub fn new_with_ports(
        metadata: Arc<dyn klights_cluster_store::ClusterMetadataRead>,
        bootstrap_tokens: Arc<dyn klights_leader_api::BootstrapTokenValidation>,
        supervisor: Arc<TaskSupervisor>,
    ) -> Self {
        Self {
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
            observed_peer_endpoints: RwLock::new(HashMap::new()),
        }
    }

    pub fn task_supervisor(&self) -> Arc<TaskSupervisor> {
        self.supervisor.clone()
    }

    pub async fn open_node_exec_with_command_id(
        &self,
        request_id: klights_cluster_core::CommandId,
        request: NodeExecRequest,
    ) -> Result<Box<dyn NodeExecSession>, ExecSetupError> {
        Ok(Box::new(
            self.open_node_exec_stream(request_id.to_string(), request)
                .await?,
        ))
    }

    pub async fn open_node_logs_with_command_id(
        &self,
        request_id: klights_cluster_core::CommandId,
        request: NodeLogRequest,
    ) -> Result<Box<dyn BoundedByteStream<Frame = NodeLogEvent>>, NodeLogSetupError> {
        Ok(Box::new(
            self.open_node_log_stream(request_id.to_string(), request)
                .await?,
        ))
    }

    pub async fn collect_node_metrics_with_command_id(
        &self,
        request_id: klights_cluster_core::CommandId,
        request: NodeMetricsRequest,
    ) -> Result<NodeMetricsResult, NodeMetricsError> {
        self.request_node_metrics(request_id.to_string(), request)
            .await
    }

    pub async fn execute_node_sync_with_command_id(
        &self,
        request_id: klights_cluster_core::CommandId,
        request: NodeExecSyncRequest,
    ) -> Result<NodeExecSyncResult, ExecSetupError> {
        self.request_node_exec_sync(request_id.to_string(), request)
            .await
    }

    pub async fn read_node_logs_with_command_id(
        &self,
        request_id: klights_cluster_core::CommandId,
        request: NodeLogRequest,
    ) -> Result<NodeLogResult, NodeLogSetupError> {
        self.request_node_log(request_id.to_string(), request).await
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

    pub async fn validate_controlplane_bootstrap_token(
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
                // The raft `last_applied` is the authoritative log index.
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
                    command_codec_version: klights_cluster_core::COMMAND_CODEC_VERSION,
                }
            }
        }
    }

    pub async fn register_follower(
        &self,
        metadata: NetworkDataplane,
    ) -> (mpsc::Receiver<FollowerControlMessage>, u64) {
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

    pub async fn complete_node_exec_sync(
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

    pub async fn complete_node_metrics(
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

    pub async fn complete_node_exec_stream_frame(
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

    pub async fn complete_node_log_event(
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
        let current_rv = self
            .metadata
            .read_cluster_metadata()
            .await
            .map(|observation| observation.into_parts().0.current_rv)
            .unwrap_or_default();
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
