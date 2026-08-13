//! Focused SQLite fixtures for cross-crate node-delivery tests.

mod legacy_delivery;
mod runtime_work;
mod types;

pub use legacy_delivery::LegacyDeliveryTestStore;
pub use runtime_work::RuntimeWorkTestStore;
pub use types::{
    DeadLetterRow, DeadLetterTestInsert, OutboxInsert, OutboxRow, OutboxStats, PodStatusCheckpoint,
};

use std::sync::Arc;

use klights_node_store::{
    DeadLetterStore, OutboxDispatcherStore, OutboxProducerStore, OutboxStatusStampStore,
    PodStatusCheckpointStore, RuntimeObservationCheckpointStore,
};

#[derive(Clone)]
pub struct NodeDeliveryTestStore {
    delivery: Arc<crate::delivery::SqliteDeliveryStore>,
    identity: Arc<crate::SqliteNodeIdentity>,
    executor: klights_supervisor::DbExecutor,
}

impl NodeDeliveryTestStore {
    pub async fn open(
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
        connection_key: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let executor =
            crate::open::open_with_opts(crate::open::in_memory_opts(), supervisor, connection_key)
                .await?;
        Ok(Self {
            delivery: Arc::new(crate::delivery::SqliteDeliveryStore::new(
                executor.clone(),
                Arc::new(klights_supervisor::SystemWallClock),
            )),
            identity: Arc::new(crate::SqliteNodeIdentity::new(executor.clone())),
            executor,
        })
    }

    pub fn outbox_producer(&self) -> Arc<dyn OutboxProducerStore> {
        self.delivery.clone()
    }

    pub fn outbox_dispatcher(&self) -> Arc<dyn OutboxDispatcherStore> {
        self.delivery.clone()
    }

    pub fn outbox_status_stamps(&self) -> Arc<dyn OutboxStatusStampStore> {
        self.delivery.clone()
    }

    pub fn dead_letters(&self) -> Arc<dyn DeadLetterStore> {
        self.delivery.clone()
    }

    pub fn pod_status_checkpoints(&self) -> Arc<dyn PodStatusCheckpointStore> {
        self.delivery.clone()
    }

    pub fn runtime_observation_checkpoints(&self) -> Arc<dyn RuntimeObservationCheckpointStore> {
        self.delivery.clone()
    }

    pub async fn node_meta(&self, key: &str) -> anyhow::Result<Option<String>> {
        use klights_node_store::NodeIdentity as _;
        self.identity
            .get_node_meta(key)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))
    }

    pub async fn outbox_stream_position_for_test(
        &self,
        idempotency_key: &str,
    ) -> anyhow::Result<Option<(i64, i64)>> {
        use rusqlite::OptionalExtension as _;
        let idempotency_key = idempotency_key.to_string();
        self.executor
            .call_raw("node-test:outbox-stream-position", move |conn| {
                conn.query_row(
                    "SELECT stream_id, stream_seq FROM outbox WHERE idempotency_key = ?1",
                    [idempotency_key],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(klights_supervisor::DbError::from)
            })
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn set_outbox_operation_for_test(
        &self,
        idempotency_key: &str,
        operation: &str,
    ) -> anyhow::Result<()> {
        let idempotency_key = idempotency_key.to_string();
        let operation = operation.to_string();
        self.executor
            .call_raw("node-test:set-outbox-operation", move |conn| {
                conn.execute_batch("PRAGMA ignore_check_constraints = ON")?;
                let changed = conn.execute(
                    "UPDATE outbox SET operation = ?2 WHERE idempotency_key = ?1",
                    rusqlite::params![idempotency_key, operation],
                )?;
                conn.execute_batch("PRAGMA ignore_check_constraints = OFF")?;
                if changed != 1 {
                    return Err(klights_supervisor::DbError::Application(Box::new(
                        std::io::Error::other(format!(
                            "test operation mutation changed {changed} rows"
                        )),
                    )));
                }
                Ok(())
            })
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn outbox_operation_for_test(
        &self,
        idempotency_key: &str,
    ) -> anyhow::Result<Option<String>> {
        use rusqlite::OptionalExtension as _;
        let idempotency_key = idempotency_key.to_string();
        self.executor
            .call_raw("node-test:outbox-operation", move |conn| {
                conn.query_row(
                    "SELECT operation FROM outbox WHERE idempotency_key = ?1",
                    [idempotency_key],
                    |row| row.get(0),
                )
                .optional()
                .map_err(klights_supervisor::DbError::from)
            })
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn insert_dead_letter_test_only(
        &self,
        row: DeadLetterTestInsert<'_>,
    ) -> anyhow::Result<()> {
        let row = (
            row.idempotency_key.to_string(),
            row.operation.to_string(),
            row.subject_key.to_string(),
            row.subject_api_version.to_string(),
            row.subject_kind.to_string(),
            row.subject_namespace.map(str::to_string),
            row.subject_name.to_string(),
            row.subject_uid.map(str::to_string),
            row.pod_uid.to_string(),
            row.payload_proto.to_vec(),
            row.attempts,
            row.last_error.to_string(),
            row.moved_at_ms,
        );
        self.executor
            .call_raw("node-test:insert-dead-letter", move |conn| {
                conn.execute(
                    "INSERT INTO outbox_dead_letter (original_id, client_id, idempotency_key, enqueued_ms, subject_key, subject_api_version, subject_kind, subject_namespace, subject_name, subject_uid, pod_uid, operation, stream_id, stream_seq, payload_proto, attempts, last_error, moved_at_ms) VALUES (0, '', ?1, 0, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, 0, ?10, ?11, ?12, ?13)",
                    rusqlite::params![row.0, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.1, row.9, row.10, row.11, row.12],
                )?;
                Ok(())
            })
            .await
            .map_err(anyhow::Error::from)
    }
}

impl std::ops::Deref for NodeDeliveryTestStore {
    type Target = crate::delivery::SqliteDeliveryStore;

    fn deref(&self) -> &Self::Target {
        &self.delivery
    }
}
