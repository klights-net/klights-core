use std::sync::Arc;

use klights_kubelet::node_outbox::payload::OutboxOperationExt as _;
use klights_node_api::{
    NodeAdminError, NodeAdminFuture, NodeDeadLetter, NodeDeadLetterAdmin, NodeOutboxDiagnostics,
    NodeOutboxStatus,
};
use klights_node_store::{DeadLetterKey, DeadLetterReplayRequest, DeadLetterStore};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct RootNodeAdmin {
    dead_letters: Arc<dyn DeadLetterStore>,
    outbox_notify: Arc<Notify>,
}

impl RootNodeAdmin {
    fn new(dead_letters: Arc<dyn DeadLetterStore>, outbox_notify: Arc<Notify>) -> Arc<Self> {
        Arc::new(Self {
            dead_letters,
            outbox_notify,
        })
    }
}

impl NodeOutboxDiagnostics for RootNodeAdmin {
    fn outbox_status(&self) -> NodeAdminFuture<'_, NodeOutboxStatus> {
        Box::pin(async move {
            let stats = self
                .dead_letters
                .outbox_stats()
                .await
                .map_err(|error| NodeAdminError::unavailable(error.to_string()))?;
            Ok(NodeOutboxStatus {
                pending: stats.pending(),
                oldest_age_seconds: stats.oldest_age_seconds(),
                dispatch_total: stats.dispatch_total() as u64,
                dispatch_errors_total: stats.dispatch_errors_total() as u64,
                dead_letter_total: stats.dead_letter_count(),
            })
        })
    }
}

impl NodeDeadLetterAdmin for RootNodeAdmin {
    fn list_dead_letters(&self) -> NodeAdminFuture<'_, Vec<NodeDeadLetter>> {
        Box::pin(async move {
            let entries = self
                .dead_letters
                .list_dead_letter()
                .await
                .map_err(|error| NodeAdminError::unavailable(error.to_string()))?;
            Ok(entries
                .into_iter()
                .map(|entry| {
                    let resource = entry.subject().resource();
                    NodeDeadLetter {
                        id: entry.id(),
                        original_id: entry.original_id(),
                        client_id: entry.client_id().to_string(),
                        idempotency_key: entry.idempotency_key().to_string(),
                        enqueued_ms: entry.enqueued_ms(),
                        subject_key: entry.subject().subject_key().to_string(),
                        subject_api_version: resource.api_version.clone(),
                        subject_kind: resource.kind.clone(),
                        subject_namespace: resource.namespace.clone(),
                        subject_name: resource.name.clone(),
                        subject_uid: entry.subject().subject_uid().map(str::to_string),
                        pod_uid: entry.subject().pod_uid().to_string(),
                        operation: entry.operation().to_string(),
                        stream_id: entry.sequence().stream_id(),
                        stream_seq: entry.sequence().stream_seq(),
                        payload_proto: entry.payload().to_vec(),
                        attempts: entry.attempts(),
                        last_error: entry.last_error().to_string(),
                        moved_at_ms: entry.moved_at_ms(),
                    }
                })
                .collect())
        })
    }

    fn replay_dead_letter(&self, id: i64) -> NodeAdminFuture<'_, bool> {
        Box::pin(async move {
            let key = DeadLetterKey::try_new(id)
                .map_err(|error| NodeAdminError::unavailable(error.to_string()))?;
            let Some(row) = self
                .dead_letters
                .get_dead_letter(key)
                .await
                .map_err(|error| NodeAdminError::unavailable(error.to_string()))?
            else {
                return Ok(false);
            };
            let operation =
                klights_kubelet::node_outbox::payload::OutboxOperation::try_from(row.operation())
                    .map_err(|error| NodeAdminError::unavailable(error.to_string()))?;
            let payload = klights_leader_rpc::storage_wire_codec::decode_outbox_payload_protobuf(
                row.payload(),
            )
            .map_err(|error| NodeAdminError::unavailable(error.to_string()))?;
            let classification = operation
                .classification(payload.command())
                .map_err(|error| NodeAdminError::unavailable(error.to_string()))?;
            let replayed = self
                .dead_letters
                .replay_dead_letter(DeadLetterReplayRequest::new(key, classification))
                .await
                .map_err(|error| NodeAdminError::unavailable(error.to_string()))?;
            if replayed {
                self.outbox_notify.notify_one();
            }
            Ok(replayed)
        })
    }

    fn delete_dead_letter(&self, id: i64) -> NodeAdminFuture<'_, bool> {
        Box::pin(async move {
            let key = DeadLetterKey::try_new(id)
                .map_err(|error| NodeAdminError::unavailable(error.to_string()))?;
            self.dead_letters
                .delete_dead_letter(key)
                .await
                .map_err(|error| NodeAdminError::unavailable(error.to_string()))
        })
    }
}

pub async fn start_node_admin(
    dead_letters: Arc<dyn DeadLetterStore>,
    outbox_notify: Arc<Notify>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    cancel: CancellationToken,
) -> anyhow::Result<klights_supervisor::SupervisedJoinHandle<()>> {
    let port = std::env::var("KLIGHTS_NODE_ADMIN_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(7781);
    let adapter = RootNodeAdmin::new(dead_letters, outbox_notify);
    klights_apiserver::start_node_admin(
        klights_apiserver::NodeAdminEndpointInputs::new(adapter.clone(), adapter),
        port,
        supervisor,
        async move { cancel.cancelled().await },
    )
    .await
}

#[cfg(test)]
mod legacy_tests;
