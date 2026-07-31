//! Root composition adapter for the reusable leader RPC runtime ports.

use std::sync::Arc;

use klights_cluster_core::{CommandId, ReplicationEntry};
use klights_leader_api::{JoinRequest, JoinResponse, MetadataResponse};
use klights_leader_rpc::server::{
    GrpcBootstrapRuntime, GrpcFollowerCompletionRuntime, GrpcFollowerSessionRuntime,
    GrpcMetadataRuntime, GrpcRuntimeError, GrpcRuntimeSupervision,
};
use klights_node_api::{
    BoundedByteStream, FollowerCompletionContext, FollowerControlMessage, NodeExec, NodeExecFuture,
    NodeExecRequest, NodeExecSession, NodeExecSyncRequest, NodeExecSyncResult, NodeLog,
    NodeLogEvent, NodeLogFuture, NodeLogRequest, NodeLogResult, NodeMetrics, NodeMetricsFuture,
    NodeMetricsRequest, NodeMetricsResult, RoutedNodeExecFrame, RoutedNodeExecSyncResponse,
    RoutedNodeLogEvent, RoutedNodeMetricsResponse,
};
use klights_replication::ReplicationService;

fn new_command_id() -> klights_cluster_core::CommandId {
    CommandId(uuid::Uuid::new_v4().to_string())
}

/// Embedded replication application adapter for the reusable authenticated
/// gRPC transport contracts.
pub(crate) struct GrpcReplicationRuntimeAdapter {
    service: Arc<ReplicationService>,
}

impl GrpcReplicationRuntimeAdapter {
    pub(crate) fn new(service: Arc<ReplicationService>) -> Arc<Self> {
        Arc::new(Self { service })
    }
}

impl NodeExec for GrpcReplicationRuntimeAdapter {
    fn exec_sync(&self, request: NodeExecSyncRequest) -> NodeExecFuture<'_, NodeExecSyncResult> {
        Box::pin(async move {
            self.service
                .execute_node_sync_with_command_id(new_command_id(), request)
                .await
        })
    }

    fn open_exec(&self, request: NodeExecRequest) -> NodeExecFuture<'_, Box<dyn NodeExecSession>> {
        Box::pin(async move {
            self.service
                .open_node_exec_with_command_id(new_command_id(), request)
                .await
        })
    }
}

impl NodeLog for GrpcReplicationRuntimeAdapter {
    fn read_logs(&self, request: NodeLogRequest) -> NodeLogFuture<'_, NodeLogResult> {
        Box::pin(async move {
            self.service
                .read_node_logs_with_command_id(new_command_id(), request)
                .await
        })
    }

    fn open_logs(
        &self,
        request: NodeLogRequest,
    ) -> NodeLogFuture<'_, Box<dyn BoundedByteStream<Frame = NodeLogEvent>>> {
        Box::pin(async move {
            self.service
                .open_node_logs_with_command_id(new_command_id(), request)
                .await
        })
    }
}

impl NodeMetrics for GrpcReplicationRuntimeAdapter {
    fn collect_metrics(
        &self,
        request: NodeMetricsRequest,
    ) -> NodeMetricsFuture<'_, NodeMetricsResult> {
        Box::pin(async move {
            self.service
                .collect_node_metrics_with_command_id(new_command_id(), request)
                .await
        })
    }
}

impl GrpcRuntimeSupervision for GrpcReplicationRuntimeAdapter {
    fn task_supervisor(&self) -> Arc<klights_supervisor::TaskSupervisor> {
        self.service.task_supervisor()
    }
}

#[async_trait::async_trait]
impl GrpcBootstrapRuntime for GrpcReplicationRuntimeAdapter {
    async fn validate_controlplane_bootstrap_token(
        &self,
        token: &str,
    ) -> Result<(), klights_leader_api::BootstrapTokenValidationError> {
        self.service
            .validate_controlplane_bootstrap_token(token)
            .await
    }

    async fn handle_authenticated_join(&self, request: JoinRequest) -> JoinResponse {
        self.service.handle_authenticated_join(request).await
    }
}

#[async_trait::async_trait]
impl GrpcFollowerSessionRuntime for GrpcReplicationRuntimeAdapter {
    async fn register_follower(
        &self,
        dataplane: klights_leader_api::NetworkDataplane,
    ) -> (tokio::sync::mpsc::Receiver<FollowerControlMessage>, u64) {
        self.service.register_follower(dataplane).await
    }

    async fn register_stream_follower(
        &self,
        node_name: String,
        session_id: u64,
    ) -> Result<tokio::sync::mpsc::Receiver<ReplicationEntry>, GrpcRuntimeError> {
        self.service
            .register_stream_follower(node_name, session_id)
            .await
            .map_err(|error| {
                GrpcRuntimeError::unavailable("register follower stream", error.to_string())
            })
    }

    async fn update_follower_ack(&self, node_name: &str, applied_rv: i64) {
        self.service
            .update_follower_ack(node_name, applied_rv)
            .await;
    }

    async fn unregister_follower(&self, node_name: &str, session_id: u64) {
        self.service
            .unregister_follower(node_name, session_id)
            .await;
    }
}

#[async_trait::async_trait]
impl GrpcFollowerCompletionRuntime for GrpcReplicationRuntimeAdapter {
    async fn complete_node_exec_sync(
        &self,
        context: FollowerCompletionContext<'_>,
        response: RoutedNodeExecSyncResponse,
    ) -> Result<(), GrpcRuntimeError> {
        self.service
            .complete_node_exec_sync(context, response)
            .await
            .map_err(|error| {
                GrpcRuntimeError::unavailable("complete node exec sync", error.to_string())
            })
    }

    async fn complete_node_log_event(
        &self,
        context: FollowerCompletionContext<'_>,
        response: RoutedNodeLogEvent,
    ) -> Result<(), GrpcRuntimeError> {
        self.service
            .complete_node_log_event(context, response)
            .await
            .map_err(|error| {
                GrpcRuntimeError::unavailable("complete node log event", error.to_string())
            })
    }

    async fn complete_node_metrics(
        &self,
        context: FollowerCompletionContext<'_>,
        response: RoutedNodeMetricsResponse,
    ) -> Result<(), GrpcRuntimeError> {
        self.service
            .complete_node_metrics(context, response)
            .await
            .map_err(|error| {
                GrpcRuntimeError::unavailable("complete node metrics", error.to_string())
            })
    }

    async fn complete_node_exec_stream_frame(
        &self,
        context: FollowerCompletionContext<'_>,
        response: RoutedNodeExecFrame,
    ) -> Result<(), GrpcRuntimeError> {
        self.service
            .complete_node_exec_stream_frame(context, response)
            .await
            .map_err(|error| {
                GrpcRuntimeError::unavailable("complete node exec stream frame", error.to_string())
            })
    }
}

#[async_trait::async_trait]
impl GrpcMetadataRuntime for GrpcReplicationRuntimeAdapter {
    async fn handle_metadata(&self) -> MetadataResponse {
        self.service.handle_metadata().await
    }

    async fn record_observed_peer_endpoint(&self, node_name: &str, endpoint: String) {
        self.service
            .record_observed_peer_endpoint(node_name, endpoint)
            .await;
    }

    async fn observed_peer_endpoint(&self, node_name: &str) -> Option<String> {
        self.service.observed_peer_endpoint(node_name).await
    }
}
