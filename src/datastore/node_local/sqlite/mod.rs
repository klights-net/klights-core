mod queries;

use anyhow::{Result, anyhow};
use klights_node_datastore::{
    SqliteNodeIdentity, SqliteNodeNetworkStateStore, SqliteRaftDurability,
    delivery::SqliteDeliveryStore,
};
use rusqlite::OptionalExtension;
use serde_json::Value;
use tokio::sync::broadcast;

use super::types::{
    PodSlotAdmissionEvent, PodSlotAdmissionResult, PodSlotAdmissionState, PodSlotClearResult,
    PodSlotMutationResult, PodWorkqueueEntry, PodWorkqueueKind,
};
use klights_supervisor::DbExecutor;
#[cfg(test)]
use sha2::{Digest, Sha256};
const POD_SLOT_ADMISSION_CHANNEL_BOUND: usize = 4_096;

#[derive(Clone)]
pub struct SqliteNodeLocalDb {
    executor: DbExecutor,
    identity: std::sync::Arc<SqliteNodeIdentity>,
    raft_persistence: std::sync::Arc<SqliteRaftDurability>,
    delivery: std::sync::Arc<SqliteDeliveryStore>,
    network_state: std::sync::Arc<SqliteNodeNetworkStateStore>,
    pod_slot_admission_tx: broadcast::Sender<PodSlotAdmissionEvent>,
    wall_clock: std::sync::Arc<dyn klights_supervisor::WallClock>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodRuntimeRow {
    pub pod_uid: String,
    pub namespace: String,
    pub pod_name: String,
    pub node_name: String,
    pub sandbox_id: Option<String>,
    pub cgroup_path: Option<String>,
    pub created_ms: i64,
    pub started_ms: Option<i64>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub struct PodStatusCheckpoint {
    pub pod_uid: String,
    pub namespace: String,
    pub pod_name: String,
    pub base_rv: i64,
    pub applied_rv: Option<i64>,
    pub status: Value,
    pub updated_ms: i64,
}

/// Node-local snapshot of runtime reconcile observations.
///
/// Mirrors `RuntimeReconcileObservations` (kubelet/pod_runtime/observations.rs)
/// but persisted to node.db so CRI events observed for a Pod UID survive an
/// actor or worker restart when CRI/containerd may have already dropped the
/// short-lived container details. UID-bound and node-local only; never
/// replicated through cluster.db or raft.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeObservationCheckpoint {
    pub pod_uid: String,
    pub container_ids: Vec<String>,
    pub generation: u64,
    pub updated_ms: i64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxInsert {
    pub idempotency_key: String,
    pub enqueued_ms: i64,
    pub subject_key: String,
    pub subject_api_version: String,
    pub subject_kind: String,
    pub subject_namespace: Option<String>,
    pub subject_name: String,
    pub subject_uid: Option<String>,
    pub pod_uid: String,
    pub operation: String,
    pub classification: klights_node_store::OutboxClassification,
    pub payload_proto: Vec<u8>,
    pub next_due_ms: i64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxRow {
    pub id: i64,
    pub client_id: String,
    pub idempotency_key: String,
    pub enqueued_ms: i64,
    pub subject_key: String,
    pub subject_api_version: String,
    pub subject_kind: String,
    pub subject_namespace: Option<String>,
    pub subject_name: String,
    pub subject_uid: Option<String>,
    pub pod_uid: String,
    pub operation: String,
    pub priority_class: i64,
    pub supersedable_pod_status: bool,
    pub is_terminal_pod_delete: bool,
    pub stream_id: i64,
    pub stream_seq: i64,
    pub payload_proto: Vec<u8>,
    pub attempt: i64,
    pub next_due_ms: i64,
    pub leased_until_ms: i64,
    pub lease_token: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeStateRow {
    pub pod_uid: String,
    pub container_name: String,
    pub probe_kind: String,
    pub last_result_ms: Option<i64>,
    pub last_success: Option<bool>,
    pub consecutive_fail: i64,
    pub next_eligible_ms: i64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationCheckpoint {
    pub last_applied_rv: i64,
    pub leader_epoch: i64,
    pub cluster_id: String,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DeadLetterRow {
    pub id: i64,
    pub original_id: i64,
    pub client_id: String,
    pub idempotency_key: String,
    pub enqueued_ms: i64,
    pub subject_key: String,
    pub subject_api_version: String,
    pub subject_kind: String,
    pub subject_namespace: Option<String>,
    pub subject_name: String,
    pub subject_uid: Option<String>,
    pub pod_uid: String,
    pub operation: String,
    pub stream_id: i64,
    pub stream_seq: i64,
    pub payload_proto: Vec<u8>,
    pub attempts: i64,
    pub last_error: String,
    pub moved_at_ms: i64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterTestInsert<'a> {
    pub idempotency_key: &'a str,
    pub operation: &'a str,
    pub subject_key: &'a str,
    pub subject_api_version: &'a str,
    pub subject_kind: &'a str,
    pub subject_namespace: Option<&'a str>,
    pub subject_name: &'a str,
    pub subject_uid: Option<&'a str>,
    pub pod_uid: &'a str,
    pub payload_proto: &'a [u8],
    pub attempts: i64,
    pub last_error: &'a str,
    pub moved_at_ms: i64,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct OutboxStats {
    pub pending: i64,
    pub oldest_age_seconds: f64,
    pub dead_letter_count: i64,
    pub dispatch_total: i64,
    pub dispatch_errors_total: i64,
}

impl SqliteNodeLocalDb {
    #[cfg(test)]
    pub fn from_executor(executor: DbExecutor) -> Result<Self> {
        Self::from_executor_with_clock(
            executor,
            std::sync::Arc::new(klights_supervisor::SystemWallClock),
        )
    }

    pub fn from_executor_with_clock(
        executor: DbExecutor,
        wall_clock: std::sync::Arc<dyn klights_supervisor::WallClock>,
    ) -> Result<Self> {
        let (pod_slot_admission_tx, _) = broadcast::channel(POD_SLOT_ADMISSION_CHANNEL_BOUND);
        Ok(Self {
            identity: std::sync::Arc::new(SqliteNodeIdentity::new(executor.clone())),
            raft_persistence: std::sync::Arc::new(SqliteRaftDurability::new(executor.clone())),
            delivery: std::sync::Arc::new(SqliteDeliveryStore::new(
                executor.clone(),
                wall_clock.clone(),
            )),
            network_state: std::sync::Arc::new(SqliteNodeNetworkStateStore::new(
                executor.clone(),
                wall_clock.clone(),
            )),
            executor,
            pod_slot_admission_tx,
            wall_clock,
        })
    }

    pub(crate) fn raft_persistence(&self) -> std::sync::Arc<SqliteRaftDurability> {
        self.raft_persistence.clone()
    }

    pub(crate) fn raft_persistence_ref(&self) -> &SqliteRaftDurability {
        &self.raft_persistence
    }

    pub(crate) fn identity_ref(&self) -> &SqliteNodeIdentity {
        &self.identity
    }

    pub(crate) fn delivery_ref(&self) -> &SqliteDeliveryStore {
        &self.delivery
    }

    pub(crate) fn network_state_ref(&self) -> &SqliteNodeNetworkStateStore {
        &self.network_state
    }

    fn now_ms(&self) -> i64 {
        self.wall_clock
            .now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64
    }

    pub async fn db_call<T, F>(&self, query_name: &'static str, f: F) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<T> + Send + 'static,
    {
        self.executor.call_raw(query_name, f).await
    }

    #[cfg(test)]
    pub async fn outbox_stream_position_for_test(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<(i64, i64)>> {
        let idempotency_key = idempotency_key.to_string();
        self.db_call("node_local:outbox_stream_position_test", move |conn| {
            conn.query_row(
                "SELECT stream_id, stream_seq FROM outbox WHERE idempotency_key = ?1",
                [idempotency_key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|error| anyhow!("outbox stream-position test read failed: {error}"))
    }

    #[cfg(test)]
    pub async fn clear_outbox_stream_identity_for_test(&self, idempotency_key: &str) -> Result<()> {
        let idempotency_key = idempotency_key.to_string();
        self.db_call("node_local:outbox_legacy_stream_test", move |conn| {
            let tx = conn.transaction()?;
            let changed = tx.execute(
                "UPDATE outbox SET client_id = '', stream_id = 0, stream_seq = 0 \
                 WHERE idempotency_key = ?1",
                [&idempotency_key],
            )?;
            if changed != 1 {
                return Err(tokio_rusqlite::Error::Other(Box::new(
                    std::io::Error::other(format!(
                        "test legacy identity mutation changed {changed} rows instead of exactly one"
                    )),
                )));
            }
            tx.execute("DELETE FROM outbox_stream_sequences", [])?;
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|error| anyhow!("outbox legacy stream test mutation failed: {error}"))
    }

    #[cfg(test)]
    pub async fn clear_all_outbox_stream_identity_for_test(&self) -> Result<()> {
        self.db_call("node_local:outbox_legacy_stream_all_test", move |conn| {
            let tx = conn.transaction()?;
            tx.execute(
                "UPDATE outbox SET client_id = '', stream_id = 0, stream_seq = 0 \
                 WHERE operation != 'LeaseRenew'",
                [],
            )?;
            tx.execute(
                "UPDATE outbox_dead_letter SET client_id = '', stream_id = 0, stream_seq = 0 \
                 WHERE operation != 'LeaseRenew'",
                [],
            )?;
            tx.execute("DELETE FROM outbox_stream_sequences", [])?;
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|error| anyhow!("outbox legacy stream test mutation failed: {error}"))
    }

    #[cfg(test)]
    pub async fn set_outbox_operation_for_test(
        &self,
        idempotency_key: &str,
        operation: &str,
    ) -> Result<()> {
        let idempotency_key = idempotency_key.to_string();
        let operation = operation.to_string();
        self.db_call("node_local:outbox_operation_test_update", move |conn| {
            conn.execute_batch("PRAGMA ignore_check_constraints = ON")?;
            let update = conn.execute(
                "UPDATE outbox SET operation = ?2 WHERE idempotency_key = ?1",
                rusqlite::params![idempotency_key, operation],
            );
            conn.execute_batch("PRAGMA ignore_check_constraints = OFF")?;
            let changed = update?;
            if changed != 1 {
                return Err(tokio_rusqlite::Error::Other(Box::new(
                    std::io::Error::other(format!(
                        "test operation-only mutation changed {changed} rows instead of exactly one"
                    )),
                )));
            }
            Ok(())
        })
        .await
        .map_err(|error| anyhow!("outbox operation test update failed: {error}"))
    }

    #[cfg(test)]
    pub async fn outbox_operation_for_test(&self, idempotency_key: &str) -> Result<Option<String>> {
        let idempotency_key = idempotency_key.to_string();
        self.db_call("node_local:outbox_operation_test_read", move |conn| {
            conn.query_row(
                "SELECT operation FROM outbox WHERE idempotency_key = ?1",
                [idempotency_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|error| anyhow!("outbox operation test read failed: {error}"))
    }

    #[cfg(test)]
    pub async fn set_outbox_attempt_for_test(
        &self,
        idempotency_key: &str,
        attempt: i64,
    ) -> Result<()> {
        let idempotency_key = idempotency_key.to_string();
        self.db_call("node_local:outbox_attempt_test_update", move |conn| {
            let changed = conn.execute(
                "UPDATE outbox SET attempt = ?2 WHERE idempotency_key = ?1",
                rusqlite::params![idempotency_key, attempt],
            )?;
            if changed != 1 {
                return Err(tokio_rusqlite::Error::Other(Box::new(
                    std::io::Error::other(format!(
                        "test attempt mutation changed {changed} rows instead of exactly one"
                    )),
                )));
            }
            Ok(())
        })
        .await
        .map_err(|error| anyhow!("outbox attempt test update failed: {error}"))
    }

    #[cfg(test)]
    pub async fn insert_dead_letter_test_only(&self, row: DeadLetterTestInsert<'_>) -> Result<()> {
        let idempotency_key = row.idempotency_key.to_string();
        let operation = row.operation.to_string();
        let subject_key = row.subject_key.to_string();
        let subject_api_version = row.subject_api_version.to_string();
        let subject_kind = row.subject_kind.to_string();
        let subject_namespace = row.subject_namespace.map(str::to_string);
        let subject_name = row.subject_name.to_string();
        let subject_uid = row.subject_uid.map(str::to_string);
        let pod_uid = row.pod_uid.to_string();
        let payload_proto = row.payload_proto.to_vec();
        let attempts = row.attempts;
        let last_error = row.last_error.to_string();
        let moved_at_ms = row.moved_at_ms;
        self.db_call("node_local:dead_letter_test_insert", move |conn| {
            conn.execute(
                "INSERT INTO outbox_dead_letter \
                 (original_id, client_id, idempotency_key, enqueued_ms, subject_key, \
                  subject_api_version, subject_kind, subject_namespace, subject_name, subject_uid, \
                  pod_uid, operation, stream_id, stream_seq, payload_proto, attempts, last_error, \
                  moved_at_ms) VALUES \
                 (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
                rusqlite::params![
                    0_i64,
                    "",
                    idempotency_key,
                    0_i64,
                    subject_key,
                    subject_api_version,
                    subject_kind,
                    subject_namespace,
                    subject_name,
                    subject_uid,
                    pod_uid,
                    operation,
                    0_i64,
                    0_i64,
                    payload_proto,
                    attempts,
                    last_error,
                    moved_at_ms,
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|error| anyhow!("dead letter test insert failed: {error}"))
    }

    pub fn subscribe_pod_slot_admissions(&self) -> broadcast::Receiver<PodSlotAdmissionEvent> {
        self.pod_slot_admission_tx.subscribe()
    }

    pub async fn admit_pod_runtime(
        &self,
        pod_uid: &str,
        namespace: &str,
        pod_name: &str,
        node_name: &str,
    ) -> Result<()> {
        let pod_uid = pod_uid.to_string();
        let namespace = namespace.to_string();
        let pod_name = pod_name.to_string();
        let node_name = node_name.to_string();
        let now = self.now_ms();
        self.db_call("node_local:pod_runtime_admit", move |conn| {
            let updated = conn.execute(
                queries::POD_RUNTIME_ADMIT,
                rusqlite::params![pod_uid, namespace, pod_name, node_name, now],
            )?;
            if updated != 1 {
                return Err(rusqlite::Error::QueryReturnedNoRows.into());
            }
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod_runtime admit failed: {e}"))
    }

    pub async fn pod_slot_try_admit(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<PodSlotAdmissionResult> {
        let namespace = namespace.to_string();
        let pod_name = pod_name.to_string();
        let pod_uid = pod_uid.to_string();
        let node_name = node_name.to_string();
        let event_namespace = namespace.clone();
        let event_pod_name = pod_name.clone();
        let event_pod_uid = pod_uid.clone();
        let now = self.now_ms();
        let (result, event) = self
            .db_call("node_local:pod_slot_try_admit", move |conn| {
                let tx = conn.transaction()?;
                let existing = read_pod_slot(&tx, &namespace, &pod_name)?;
                let (result, event) = match existing {
                    None => {
                        let rv = next_pod_slot_resource_version(&tx)?;
                        tx.execute(
                            queries::POD_SLOT_ADMISSION_INSERT,
                            rusqlite::params![
                                namespace,
                                pod_name,
                                pod_uid,
                                node_name,
                                PodSlotAdmissionState::Admitted.as_str(),
                                rv,
                                now,
                            ],
                        )?;
                        (
                            PodSlotAdmissionResult::Admitted {
                                resource_version: rv,
                            },
                            Some(PodSlotAdmissionEvent::Changed {
                                namespace: event_namespace,
                                pod_name: event_pod_name,
                                pod_uid: event_pod_uid,
                                state: PodSlotAdmissionState::Admitted,
                                resource_version: rv,
                            }),
                        )
                    }
                    Some(row) if row.pod_uid == pod_uid => {
                        if row.state == PodSlotAdmissionState::Admitted
                            && row.node_name == node_name
                        {
                            (
                                PodSlotAdmissionResult::Admitted {
                                    resource_version: row.resource_version,
                                },
                                None,
                            )
                        } else {
                            let rv = next_pod_slot_resource_version(&tx)?;
                            tx.execute(
                                queries::POD_SLOT_ADMISSION_UPDATE,
                                rusqlite::params![
                                    namespace,
                                    pod_name,
                                    pod_uid,
                                    node_name,
                                    PodSlotAdmissionState::Admitted.as_str(),
                                    rv,
                                    now,
                                ],
                            )?;
                            (
                                PodSlotAdmissionResult::Admitted {
                                    resource_version: rv,
                                },
                                Some(PodSlotAdmissionEvent::Changed {
                                    namespace: event_namespace,
                                    pod_name: event_pod_name,
                                    pod_uid: event_pod_uid,
                                    state: PodSlotAdmissionState::Admitted,
                                    resource_version: rv,
                                }),
                            )
                        }
                    }
                    Some(row) => (
                        PodSlotAdmissionResult::Blocked {
                            blocking_uid: row.pod_uid,
                            blocking_node: row.node_name,
                            state: row.state,
                            resource_version: row.resource_version,
                        },
                        None,
                    ),
                };
                tx.commit()?;
                Ok((result, event))
            })
            .await
            .map_err(|e| anyhow!("pod slot admission failed: {e}"))?;
        if let Some(event) = event {
            let _ = self.pod_slot_admission_tx.send(event);
        }
        Ok(result)
    }

    pub async fn pod_slot_mark_terminating(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        node_name: &str,
    ) -> Result<PodSlotMutationResult> {
        let namespace = namespace.to_string();
        let pod_name = pod_name.to_string();
        let pod_uid = pod_uid.to_string();
        let node_name = node_name.to_string();
        let event_namespace = namespace.clone();
        let event_pod_name = pod_name.clone();
        let event_pod_uid = pod_uid.clone();
        let now = self.now_ms();
        let (result, event) = self
            .db_call("node_local:pod_slot_mark_terminating", move |conn| {
                let tx = conn.transaction()?;
                let existing = read_pod_slot(&tx, &namespace, &pod_name)?;
                let (result, event) = match existing {
                    Some(row) if row.pod_uid != pod_uid => {
                        return Err(tokio_rusqlite::Error::Other(Box::new(
                            std::io::Error::other("pod slot admission UID precondition failed"),
                        )));
                    }
                    Some(row)
                        if row.state == PodSlotAdmissionState::Terminating
                            && row.node_name == node_name =>
                    {
                        (
                            PodSlotMutationResult::Unchanged {
                                resource_version: row.resource_version,
                            },
                            None,
                        )
                    }
                    Some(_) => {
                        let rv = next_pod_slot_resource_version(&tx)?;
                        tx.execute(
                            queries::POD_SLOT_ADMISSION_UPDATE,
                            rusqlite::params![
                                namespace,
                                pod_name,
                                pod_uid,
                                node_name,
                                PodSlotAdmissionState::Terminating.as_str(),
                                rv,
                                now,
                            ],
                        )?;
                        (
                            PodSlotMutationResult::Changed {
                                resource_version: rv,
                            },
                            Some(PodSlotAdmissionEvent::Changed {
                                namespace: event_namespace,
                                pod_name: event_pod_name,
                                pod_uid: event_pod_uid,
                                state: PodSlotAdmissionState::Terminating,
                                resource_version: rv,
                            }),
                        )
                    }
                    None => {
                        let rv = next_pod_slot_resource_version(&tx)?;
                        tx.execute(
                            queries::POD_SLOT_ADMISSION_INSERT,
                            rusqlite::params![
                                namespace,
                                pod_name,
                                pod_uid,
                                node_name,
                                PodSlotAdmissionState::Terminating.as_str(),
                                rv,
                                now,
                            ],
                        )?;
                        (
                            PodSlotMutationResult::Changed {
                                resource_version: rv,
                            },
                            Some(PodSlotAdmissionEvent::Changed {
                                namespace: event_namespace,
                                pod_name: event_pod_name,
                                pod_uid: event_pod_uid,
                                state: PodSlotAdmissionState::Terminating,
                                resource_version: rv,
                            }),
                        )
                    }
                };
                tx.commit()?;
                Ok((result, event))
            })
            .await
            .map_err(|e| anyhow!("pod slot terminating transition failed: {e}"))?;
        if let Some(event) = event {
            let _ = self.pod_slot_admission_tx.send(event);
        }
        Ok(result)
    }

    pub async fn pod_slot_clear_if_uid(
        &self,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
    ) -> Result<PodSlotClearResult> {
        let namespace = namespace.to_string();
        let pod_name = pod_name.to_string();
        let pod_uid = pod_uid.to_string();
        let event_namespace = namespace.clone();
        let event_pod_name = pod_name.clone();
        let event_pod_uid = pod_uid.clone();
        let (result, event) = self
            .db_call("node_local:pod_slot_clear_if_uid", move |conn| {
                let tx = conn.transaction()?;
                let Some(row) = read_pod_slot(&tx, &namespace, &pod_name)? else {
                    tx.commit()?;
                    return Ok((PodSlotClearResult::NotFound, None));
                };
                if row.pod_uid != pod_uid {
                    tx.commit()?;
                    return Ok((
                        PodSlotClearResult::UidMismatch {
                            blocking_uid: row.pod_uid,
                            blocking_node: row.node_name,
                            state: row.state,
                            resource_version: row.resource_version,
                        },
                        None,
                    ));
                }
                let rv = next_pod_slot_resource_version(&tx)?;
                tx.execute(
                    queries::POD_SLOT_ADMISSION_DELETE_IF_UID,
                    rusqlite::params![namespace, pod_name, pod_uid],
                )?;
                tx.commit()?;
                Ok((
                    PodSlotClearResult::Cleared {
                        resource_version: rv,
                    },
                    Some(PodSlotAdmissionEvent::Cleared {
                        namespace: event_namespace,
                        pod_name: event_pod_name,
                        pod_uid: event_pod_uid,
                        resource_version: rv,
                    }),
                ))
            })
            .await
            .map_err(|e| anyhow!("pod slot clear failed: {e}"))?;
        if let Some(event) = event {
            let _ = self.pod_slot_admission_tx.send(event);
        }
        Ok(result)
    }

    pub async fn record_owned_sandbox(
        &self,
        pod_uid: &str,
        namespace: &str,
        pod_name: &str,
        node_name: &str,
        sandbox_id: &str,
        created_ms: i64,
    ) -> std::result::Result<(), super::PodRuntimeOwnershipError> {
        let pod_uid = pod_uid.to_string();
        let conflict_pod_uid = pod_uid.clone();
        let namespace = namespace.to_string();
        let pod_name = pod_name.to_string();
        let node_name = node_name.to_string();
        let sandbox_id = sandbox_id.to_string();
        let existing = self
            .db_call("node_local:pod_runtime_record_owned_sandbox", move |conn| {
                let tx = conn.transaction()?;
                let updated = tx.execute(
                    queries::POD_RUNTIME_RECORD_OWNED_SANDBOX,
                    rusqlite::params![
                        pod_uid, namespace, pod_name, node_name, sandbox_id, created_ms
                    ],
                )?;
                if updated == 1 {
                    tx.commit()?;
                    return Ok(None);
                }
                let existing = tx
                    .query_row(
                        queries::POD_RUNTIME_OWNERSHIP_GET_UID,
                        rusqlite::params![pod_uid],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, String>(2)?,
                                row.get::<_, Option<String>>(3)?,
                            ))
                        },
                    )
                    .optional()?;
                tx.commit()?;
                Ok(existing)
            })
            .await
            .map_err(|error| super::PodRuntimeOwnershipError::Persistence {
                message: format!("pod_runtime record owned sandbox failed: {error}"),
            })?;
        match existing {
            None => Ok(()),
            Some((
                existing_namespace,
                existing_pod_name,
                existing_node_name,
                existing_sandbox_id,
            )) => Err(super::PodRuntimeOwnershipError::Conflict {
                pod_uid: conflict_pod_uid,
                existing_namespace,
                existing_pod_name,
                existing_node_name,
                existing_sandbox_id,
            }),
        }
    }

    pub async fn record_cgroup(&self, pod_uid: &str, cgroup_path: &str) -> Result<()> {
        let pod_uid = pod_uid.to_string();
        let cgroup_path = cgroup_path.to_string();
        self.db_call("node_local:pod_runtime_record_cgroup", move |conn| {
            conn.execute(
                queries::POD_RUNTIME_RECORD_CGROUP,
                rusqlite::params![pod_uid, cgroup_path],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod_runtime record cgroup failed: {e}"))
    }

    pub async fn delete_pod_runtime_for_uid(&self, pod_uid: &str) -> Result<()> {
        let pod_uid = pod_uid.to_string();
        self.db_call("node_local:pod_runtime_delete", move |conn| {
            conn.execute(queries::POD_RUNTIME_DELETE_UID, [pod_uid])?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod_runtime delete failed: {e}"))
    }

    pub async fn get_pod_runtime(&self, pod_uid: &str) -> Result<Option<PodRuntimeRow>> {
        let pod_uid = pod_uid.to_string();
        self.db_call("node_local:pod_runtime_get", move |conn| {
            conn.query_row(queries::POD_RUNTIME_GET_UID, [pod_uid], row_to_pod_runtime)
                .optional()
                .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("pod_runtime get failed: {e}"))
    }

    pub async fn list_pod_runtime(&self) -> Result<Vec<PodRuntimeRow>> {
        self.db_call("node_local:pod_runtime_list", move |conn| {
            let mut stmt = conn.prepare(queries::POD_RUNTIME_LIST)?;
            let rows = stmt
                .query_map([], row_to_pod_runtime)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("pod_runtime list failed: {e}"))
    }

    pub async fn list_pod_runtime_by_namespace(
        &self,
        namespace: &str,
    ) -> Result<Vec<PodRuntimeRow>> {
        let namespace = namespace.to_string();
        self.db_call("node_local:pod_runtime_list_ns", move |conn| {
            let mut stmt = conn.prepare(queries::POD_RUNTIME_LIST_NS)?;
            let rows = stmt
                .query_map([namespace], row_to_pod_runtime)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("pod_runtime list namespace failed: {e}"))
    }

    pub async fn record_probe_result(
        &self,
        pod_uid: &str,
        container_name: &str,
        probe_kind: &str,
        success: bool,
        ts_ms: i64,
    ) -> Result<()> {
        let pod_uid = pod_uid.to_string();
        let container_name = container_name.to_string();
        let probe_kind = probe_kind.to_string();
        let success_int = if success { 1 } else { 0 };
        self.db_call("node_local:probe_record", move |conn| {
            conn.execute(
                queries::PROBE_STATE_UPSERT,
                rusqlite::params![pod_uid, container_name, probe_kind, ts_ms, success_int],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("probe_state record failed: {e}"))
    }

    pub async fn get_probe_state(
        &self,
        pod_uid: &str,
        container_name: &str,
        probe_kind: &str,
    ) -> Result<Option<ProbeStateRow>> {
        let pod_uid = pod_uid.to_string();
        let container_name = container_name.to_string();
        let probe_kind = probe_kind.to_string();
        self.db_call("node_local:probe_get", move |conn| {
            conn.query_row(
                queries::PROBE_STATE_GET,
                rusqlite::params![pod_uid, container_name, probe_kind],
                |row| {
                    Ok(ProbeStateRow {
                        pod_uid: row.get(0)?,
                        container_name: row.get(1)?,
                        probe_kind: row.get(2)?,
                        last_result_ms: row.get(3)?,
                        last_success: row.get::<_, Option<i64>>(4)?.map(|v| v != 0),
                        consecutive_fail: row.get(5)?,
                        next_eligible_ms: row.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("probe_state get failed: {e}"))
    }

    // T3: `append_log_apply_entry`, `list_log_apply_entries_after`,
    // `load_log_apply_checkpoint`, `save_log_apply_checkpoint` removed.
    // These were consumed only by the BackupApplier (deleted in T1.6).
    // The `raft_log_entries` table (used by openraft's RaftLogStorage)
    // is the sole durable log.

    pub async fn current_log_apply_index(&self) -> Result<i64> {
        // T3: returns 0 since the `log_apply_entries` table is gone.
        // The raft `last_applied` index is the authoritative source.
        Ok(0)
    }

    #[cfg(test)]
    pub async fn table_names_for_test(&self) -> Result<Vec<String>> {
        self.db_call("node_local:test_table_names", move |conn| {
            let rows = conn
                .prepare(
                    "SELECT name FROM sqlite_master \
                     WHERE type='table' AND name NOT LIKE 'sqlite_%' \
                     ORDER BY name",
                )?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .map_err(|e| anyhow!("test table names failed: {e}"))
    }

    #[cfg(test)]
    pub async fn table_has_not_null_column_for_test(
        &self,
        table: &str,
        column: &str,
    ) -> Result<bool> {
        let table = table.to_string();
        let column = column.to_string();
        self.db_call("node_local:test_not_null_column", move |conn| {
            let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_ident(&table)))?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let name: String = row.get(1)?;
                let ty: String = row.get(2)?;
                let not_null: i64 = row.get(3)?;
                if name == column {
                    return Ok(not_null == 1 && ty.eq_ignore_ascii_case("TEXT"));
                }
            }
            Ok(false)
        })
        .await
        .map_err(|e| anyhow!("test column check failed: {e}"))
    }

    #[cfg(test)]
    pub async fn schema_contains_full_resource_body_column_for_test(&self) -> Result<bool> {
        self.db_call("node_local:test_body_column", move |conn| {
            let tables = conn
                .prepare(
                    "SELECT name FROM sqlite_master \
                     WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                )?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for table in tables {
                let mut stmt =
                    conn.prepare(&format!("PRAGMA table_info({})", quote_ident(&table)))?;
                let mut rows = stmt.query([])?;
                while let Some(row) = rows.next()? {
                    let name: String = row.get(1)?;
                    let ty: String = row.get(2)?;
                    if name == "data" && ty.eq_ignore_ascii_case("BLOB") {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        })
        .await
        .map_err(|e| anyhow!("test body column check failed: {e}"))
    }
}

fn row_to_pod_runtime(row: &rusqlite::Row<'_>) -> rusqlite::Result<PodRuntimeRow> {
    Ok(PodRuntimeRow {
        pod_uid: row.get(0)?,
        namespace: row.get(1)?,
        pod_name: row.get(2)?,
        node_name: row.get(3)?,
        sandbox_id: row.get(4)?,
        cgroup_path: row.get(5)?,
        created_ms: row.get(6)?,
        started_ms: row.get(7)?,
    })
}

struct PodSlotRow {
    pod_uid: String,
    node_name: String,
    state: PodSlotAdmissionState,
    resource_version: i64,
}

fn read_pod_slot(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    pod_name: &str,
) -> rusqlite::Result<Option<PodSlotRow>> {
    tx.query_row(
        queries::POD_SLOT_ADMISSION_SELECT,
        rusqlite::params![namespace, pod_name],
        |row| {
            let state_text: String = row.get(2)?;
            let state = PodSlotAdmissionState::parse(&state_text).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::other(err.to_string())),
                )
            })?;
            Ok(PodSlotRow {
                pod_uid: row.get(0)?,
                node_name: row.get(1)?,
                state,
                resource_version: row.get(3)?,
            })
        },
    )
    .optional()
}

fn next_pod_slot_resource_version(tx: &rusqlite::Transaction<'_>) -> rusqlite::Result<i64> {
    let current = tx
        .query_row(queries::POD_SLOT_RV_SELECT, [], |row| {
            row.get::<_, String>(0)
        })
        .optional()?
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let next = current.saturating_add(1);
    tx.execute(queries::POD_SLOT_RV_UPSERT, [next.to_string()])?;
    Ok(next)
}

#[cfg(test)]
fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

impl SqliteNodeLocalDb {
    pub async fn enqueue_workqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &klights_types::PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
        let kind = kind.as_str().to_string();
        let namespace = pod.namespace.clone();
        let name = pod.name.clone();
        let uid = pod.uid.clone();
        let payload = serde_json::to_vec(&payload)?;
        let last_error = last_error.map(str::to_string);
        let now = self.now_ms();
        let floor = now.saturating_add(min_delay_ms.max(0));
        self.db_call("node_local:workqueue_enqueue", move |conn| {
            let tail_other: i64 = conn.query_row(
                "SELECT COALESCE(MAX(next_due_ms), 0) FROM pod_workqueue \
                 WHERE NOT (kind = ?1 AND namespace = ?2 AND pod_name = ?3 AND pod_uid = ?4)",
                rusqlite::params![kind, namespace, name, uid],
                |row| row.get(0),
            )?;
            let next_due_ms = floor.max(tail_other.saturating_add(1));
            conn.execute(
                "INSERT INTO pod_workqueue \
                 (kind, namespace, pod_name, pod_uid, payload, attempt_count, next_due_ms, last_error, enqueued_ms) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
                 ON CONFLICT(kind, namespace, pod_name, pod_uid) DO UPDATE SET \
                   payload = excluded.payload, \
                   attempt_count = excluded.attempt_count, \
                   next_due_ms = excluded.next_due_ms, \
                   last_error = excluded.last_error, \
                   enqueued_ms = excluded.enqueued_ms",
                rusqlite::params![
                    kind,
                    namespace,
                    name,
                    uid,
                    payload,
                    attempt_count,
                    next_due_ms,
                    last_error,
                    now
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod_workqueue enqueue failed: {e}"))
    }

    pub async fn peek_workqueue_next_due(&self) -> Result<Option<i64>> {
        self.db_call("node_local:workqueue_peek", move |conn| {
            conn.query_row("SELECT MIN(next_due_ms) FROM pod_workqueue", [], |row| {
                row.get::<_, Option<i64>>(0)
            })
            .optional()
            .map(|v| v.flatten())
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow!("pod_workqueue peek failed: {e}"))
    }

    pub async fn claim_workqueue_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>> {
        self.db_call("node_local:workqueue_claim", move |conn| {
            let tx = conn.transaction()?;
            let row = tx
                .query_row(
                    "SELECT id, kind, namespace, pod_name, pod_uid, payload, attempt_count, next_due_ms \
                     FROM pod_workqueue WHERE next_due_ms <= ?1 ORDER BY next_due_ms ASC, id ASC LIMIT 1",
                    [now_ms],
                    |row| {
                        let kind_raw: String = row.get(1)?;
                        let kind = PodWorkqueueKind::parse(&kind_raw).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                rusqlite::types::Type::Text,
                                Box::new(std::io::Error::other(e.to_string())),
                            )
                        })?;
                        let payload: Vec<u8> = row.get(5)?;
                        let payload = serde_json::from_slice::<Value>(&payload).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                5,
                                rusqlite::types::Type::Blob,
                                Box::new(e),
                            )
                        })?;
                        Ok(PodWorkqueueEntry {
                            id: row.get(0)?,
                            kind,
                            namespace: row.get(2)?,
                            name: row.get(3)?,
                            uid: row.get(4)?,
                            payload,
                            attempt_count: row.get(6)?,
                            next_attempt_at_ms: row.get(7)?,
                        })
                    },
                )
                .optional()?;
            if let Some(ref claimed) = row {
                tx.execute("DELETE FROM pod_workqueue WHERE id = ?1", [claimed.id])?;
            }
            tx.commit()?;
            Ok(row)
        })
        .await
        .map_err(|e| anyhow!("pod_workqueue claim failed: {e}"))
    }

    pub async fn complete_workqueue(&self, id: i64) -> Result<()> {
        self.db_call("node_local:workqueue_complete", move |conn| {
            conn.execute("DELETE FROM pod_workqueue WHERE id = ?1", [id])?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("pod_workqueue complete failed: {e}"))
    }
}

#[cfg(test)]
pub fn outbox_stream_id(subject_key: &str) -> i64 {
    let digest = Sha256::digest(subject_key.as_bytes());
    let mut shard_bytes = [0_u8; 8];
    shard_bytes.copy_from_slice(&digest[..8]);
    let value = u64::from_be_bytes(shard_bytes);
    let stream_id = (value & i64::MAX as u64) as i64;
    if stream_id == 0 { 1 } else { stream_id }
}
