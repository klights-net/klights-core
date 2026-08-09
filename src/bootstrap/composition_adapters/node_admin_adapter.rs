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

pub(crate) async fn start_node_admin(
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
mod tests {
    use super::*;

    use crate::bootstrap::node_store::NodeLocalStores;
    use crate::bootstrap::node_store::{open_node_local, open_node_local_with_sqlite};
    use crate::datastore::backend_kind::BackendKind;
    use crate::datastore::node_local::DeadLetterTestInsert;
    use crate::datastore::node_local::{LegacyDeliveryTestStore as _, OutboxInsert};
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

    fn supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
    }

    fn pod_status_classification() -> klights_node_store::OutboxClassification {
        klights_node_store::OutboxClassification::try_new(
            klights_node_store::OutboxPriority::Workload,
            klights_node_store::OutboxSupersedability::PodStatus,
            klights_node_store::TerminalDeleteClassification::NotTerminalDelete,
            klights_node_store::OutboxSequencePolicy::PerSubject,
        )
        .expect("valid Pod status classification")
    }

    fn pod_status_payload() -> Vec<u8> {
        crate::bootstrap::composition_tests::support::OutboxPayload::from_command(
            klights_cluster_core::StorageCommand::UpdateStatus {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "web".to_string(),
                status: serde_json::json!({"phase": "Running"}),
                expected_rv: None,
                preconditions: klights_cluster_core::ResourcePreconditions::uid("uid-1"),
                observed_status_stamp: Some(1),
            },
        )
        .encode_protobuf()
        .expect("encode test Pod status outbox envelope")
    }

    async fn node_db(connection_key: &'static str) -> NodeLocalStores {
        open_node_local(
            BackendKind::Sqlite,
            None,
            supervisor(),
            None,
            connection_key,
        )
        .await
        .expect("open node-local test db")
    }

    async fn node_db_with_dead_letter() -> (NodeLocalStores, i64) {
        let ndb = node_db("sqlite:node-admin-adapter-test").await;
        ndb.legacy_enqueue_outbox(OutboxInsert {
            idempotency_key: "node-admin-dl-key".to_string(),
            enqueued_ms: 1000,
            subject_key: "v1/Pod/default/web/uid-1".to_string(),
            subject_api_version: "v1".to_string(),
            subject_kind: "Pod".to_string(),
            subject_namespace: Some("default".to_string()),
            subject_name: "web".to_string(),
            subject_uid: Some("uid-1".to_string()),
            pod_uid: "uid-1".to_string(),
            operation: "PodStatus".to_string(),
            payload_proto: pod_status_payload(),
            next_due_ms: 1000,
            classification: pod_status_classification(),
        })
        .await
        .expect("enqueue for dead letter");
        ndb.legacy_move_outbox_to_dead_letter_if_max_attempts("node-admin-dl-key", 0)
            .await
            .expect("move to dead letter");
        let id = ndb
            .legacy_list_dead_letter()
            .await
            .expect("list dead letter")
            .first()
            .expect("dead letter row")
            .id;
        (ndb, id)
    }

    async fn node_db_with_unassigned_dead_letter() -> (NodeLocalStores, i64) {
        let (ndb, sqlite) = open_node_local_with_sqlite(
            BackendKind::Sqlite,
            None,
            supervisor(),
            None,
            "sqlite:node-admin-unassigned-dead-letter-adapter-test",
        )
        .await
        .expect("open node-local test db");
        sqlite
            .expect("SQLite backend")
            .insert_dead_letter_test_only(DeadLetterTestInsert {
                idempotency_key: "node-admin-unassigned-dl-key",
                operation: "PodStatus",
                subject_key: "v1/Pod/default/web/uid-1",
                subject_api_version: "v1",
                subject_kind: "Pod",
                subject_namespace: Some("default"),
                subject_name: "web",
                subject_uid: Some("uid-1"),
                pod_uid: "uid-1",
                payload_proto: &[1, 2, 3],
                attempts: 720,
                last_error: "max attempts",
                moved_at_ms: 2_000,
            })
            .await
            .expect("insert unassigned dead letter");
        let id = ndb
            .legacy_list_dead_letter()
            .await
            .expect("list dead letter")
            .first()
            .expect("dead letter row")
            .id;
        (ndb, id)
    }

    fn adapter(node_db: &NodeLocalStores, notify: Arc<Notify>) -> Arc<RootNodeAdmin> {
        RootNodeAdmin::new(node_db.dead_letters(), notify)
    }

    #[tokio::test]
    async fn outbox_status_adapter_returns_persisted_metrics() {
        let ndb = node_db("sqlite:node-admin-status-adapter-test").await;
        ndb.identity()
            .set_node_meta("outbox_dispatch_total", "42")
            .await
            .expect("write dispatch counter");
        ndb.identity()
            .set_node_meta("outbox_dispatch_errors_total", "7")
            .await
            .expect("write error counter");
        ndb.legacy_enqueue_outbox(OutboxInsert {
            idempotency_key: "status-test-key".to_string(),
            enqueued_ms: 1000,
            subject_key: "v1/Pod/default/web/uid-1".to_string(),
            subject_api_version: "v1".to_string(),
            subject_kind: "Pod".to_string(),
            subject_namespace: Some("default".to_string()),
            subject_name: "web".to_string(),
            subject_uid: Some("uid-1".to_string()),
            pod_uid: "uid-1".to_string(),
            operation: "PodStatus".to_string(),
            payload_proto: Vec::new(),
            next_due_ms: 1000,
            classification: pod_status_classification(),
        })
        .await
        .expect("enqueue");

        let status = adapter(&ndb, Arc::new(Notify::new()))
            .outbox_status()
            .await
            .expect("read status through root adapter");
        assert_eq!(status.pending, 1);
        assert_eq!(status.dispatch_total, 42);
        assert_eq!(status.dispatch_errors_total, 7);
        assert!(status.dead_letter_total >= 0);
    }

    #[tokio::test]
    async fn dead_letter_list_adapter_preserves_row_identity() {
        let (ndb, _) = node_db_with_dead_letter().await;
        let rows = adapter(&ndb, Arc::new(Notify::new()))
            .list_dead_letters()
            .await
            .expect("list through root adapter");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].idempotency_key, "node-admin-dl-key");
        assert_eq!(rows[0].subject_uid.as_deref(), Some("uid-1"));
    }

    #[tokio::test]
    async fn dead_letter_replay_adapter_requeues_and_wakes_dispatcher() {
        let (ndb, id) = node_db_with_dead_letter().await;
        let notify = Arc::new(Notify::new());
        assert!(
            adapter(&ndb, notify.clone())
                .replay_dead_letter(id)
                .await
                .expect("replay through root adapter")
        );
        tokio::time::timeout(std::time::Duration::from_millis(50), notify.notified())
            .await
            .expect("successful replay must wake the idle dispatcher");
        assert!(
            ndb.legacy_list_dead_letter()
                .await
                .expect("list dead letter")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn dead_letter_delete_adapter_removes_unassigned_row() {
        let (ndb, id) = node_db_with_unassigned_dead_letter().await;
        assert!(
            adapter(&ndb, Arc::new(Notify::new()))
                .delete_dead_letter(id)
                .await
                .expect("delete through root adapter")
        );
        assert!(
            ndb.legacy_list_dead_letter()
                .await
                .expect("list dead letter")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn dead_letter_replay_adapter_reports_missing_row() {
        let ndb = node_db("sqlite:node-admin-missing-adapter-test").await;
        assert!(
            !adapter(&ndb, Arc::new(Notify::new()))
                .replay_dead_letter(99_999)
                .await
                .expect("missing replay through root adapter")
        );
    }
}
