//! SQLite persistence for node-local Pod runtime, probe, workqueue, and slot state.
//!
//! This module is passive persistence only. Runtime actors, probe scheduling,
//! workqueue retry policy, timers, and volume/filesystem behavior remain with
//! their feature owners.

use std::sync::Arc;

use klights_node_store::{
    DueTimeMs, ObservedPodVersion, OwnedPodSandbox, PodRuntimeAdmission, PodRuntimeCgroup,
    PodRuntimeRecord, PodRuntimeStore, PodSlotAdmissionEvent, PodSlotAdmissionEventSource,
    PodSlotAdmissionRequest, PodSlotAdmissionResult, PodSlotAdmissionState, PodSlotAdmissionStore,
    PodSlotClearResult, PodSlotEventSubscription, PodSlotMutationResult, PodWorkIdentity,
    PodWorkqueueEnqueue, PodWorkqueueEntry, PodWorkqueueKind, PodWorkqueueStore, ProbeKey,
    ProbeResult, ProbeState, ProbeStateStore, RuntimeNamespace, RuntimePodUid, RuntimeWorkError,
    RuntimeWorkFuture,
};
use klights_supervisor::{DbExecutor, WallClock};
use klights_types::PodIdentity;
use rusqlite::OptionalExtension;
use tokio::sync::broadcast;

const POD_SLOT_ADMISSION_CHANNEL_BOUND: usize = 4_096;

const POD_RUNTIME_ADMIT: &str = "INSERT INTO pod_runtime \
     (pod_uid, namespace, pod_name, node_name, created_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5) \
     ON CONFLICT(pod_uid) DO UPDATE SET \
       namespace = excluded.namespace, \
       pod_name = excluded.pod_name, \
       node_name = excluded.node_name \
     WHERE pod_runtime.namespace = excluded.namespace \
       AND pod_runtime.pod_name = excluded.pod_name \
       AND pod_runtime.node_name = excluded.node_name";
const POD_RUNTIME_OWNERSHIP_GET_UID: &str = "SELECT namespace, pod_name, node_name, \
     sandbox_id FROM pod_runtime WHERE pod_uid = ?1";
const POD_RUNTIME_RECORD_OWNED_SANDBOX: &str = "INSERT INTO pod_runtime \
     (pod_uid, namespace, pod_name, node_name, sandbox_id, created_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
     ON CONFLICT(pod_uid) DO UPDATE SET sandbox_id = excluded.sandbox_id \
     WHERE pod_runtime.namespace = excluded.namespace \
       AND pod_runtime.pod_name = excluded.pod_name \
       AND pod_runtime.node_name = excluded.node_name \
       AND (pod_runtime.sandbox_id IS NULL \
            OR pod_runtime.sandbox_id = excluded.sandbox_id)";
const POD_RUNTIME_RECORD_CGROUP: &str = "UPDATE pod_runtime \
     SET cgroup_path = ?2 WHERE pod_uid = ?1";
const POD_RUNTIME_DELETE_UID: &str = "DELETE FROM pod_runtime WHERE pod_uid = ?1";
const POD_RUNTIME_GET_UID: &str = "SELECT pod_uid, namespace, pod_name, node_name, \
     sandbox_id, cgroup_path, created_ms, started_ms FROM pod_runtime WHERE pod_uid = ?1";
const POD_RUNTIME_LIST: &str = "SELECT pod_uid, namespace, pod_name, node_name, \
     sandbox_id, cgroup_path, created_ms, started_ms FROM pod_runtime ORDER BY pod_uid";
const POD_RUNTIME_LIST_NS: &str = "SELECT pod_uid, namespace, pod_name, node_name, \
     sandbox_id, cgroup_path, created_ms, started_ms FROM pod_runtime WHERE namespace = ?1 ORDER BY pod_uid";

const POD_SLOT_ADMISSION_SELECT: &str = "SELECT pod_uid, node_name, state, updated_rv \
     FROM pod_slot_admissions WHERE namespace = ?1 AND pod_name = ?2";
const POD_SLOT_ADMISSION_INSERT: &str = "INSERT INTO pod_slot_admissions \
     (namespace, pod_name, pod_uid, node_name, state, updated_rv, updated_at_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";
const POD_SLOT_ADMISSION_UPDATE: &str = "UPDATE pod_slot_admissions \
     SET pod_uid = ?3, node_name = ?4, state = ?5, updated_rv = ?6, updated_at_ms = ?7 \
     WHERE namespace = ?1 AND pod_name = ?2";
const POD_SLOT_ADMISSION_DELETE_IF_UID: &str = "DELETE FROM pod_slot_admissions \
     WHERE namespace = ?1 AND pod_name = ?2 AND pod_uid = ?3";
const POD_SLOT_RV_SELECT: &str =
    "SELECT value FROM _node_meta WHERE key = 'pod_slot_resource_version'";
const POD_SLOT_RV_UPSERT: &str = "INSERT INTO _node_meta (key, value) \
     VALUES ('pod_slot_resource_version', ?1) \
     ON CONFLICT(key) DO UPDATE SET value = excluded.value";

const PROBE_STATE_UPSERT: &str = "INSERT INTO probe_state \
     (pod_uid, container_name, probe_kind, last_result_ms, last_success, consecutive_fail, next_eligible_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, CASE WHEN ?5 = 1 THEN 0 ELSE 1 END, ?4) \
     ON CONFLICT(pod_uid, container_name, probe_kind) DO UPDATE SET \
       last_result_ms = excluded.last_result_ms, \
       last_success = excluded.last_success, \
       consecutive_fail = CASE WHEN excluded.last_success = 1 THEN 0 ELSE probe_state.consecutive_fail + 1 END, \
       next_eligible_ms = excluded.next_eligible_ms";
const PROBE_STATE_GET: &str = "SELECT pod_uid, container_name, probe_kind, \
     last_result_ms, last_success, consecutive_fail, next_eligible_ms \
     FROM probe_state WHERE pod_uid = ?1 AND container_name = ?2 AND probe_kind = ?3";

/// Passive SQLite implementation of the focused runtime-work persistence ports.
#[derive(Clone)]
pub struct SqliteRuntimeWorkStore {
    executor: DbExecutor,
    wall_clock: Arc<dyn WallClock>,
    pod_slot_admission_tx: broadcast::Sender<PodSlotAdmissionEvent>,
}

impl SqliteRuntimeWorkStore {
    pub fn new(executor: DbExecutor, wall_clock: Arc<dyn WallClock>) -> Self {
        let (pod_slot_admission_tx, _) = broadcast::channel(POD_SLOT_ADMISSION_CHANNEL_BOUND);
        Self {
            executor,
            wall_clock,
            pod_slot_admission_tx,
        }
    }

    fn now_ms(&self) -> i64 {
        self.wall_clock
            .now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64
    }

    async fn db_call<T, F>(&self, query_name: &'static str, call: F) -> tokio_rusqlite::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut rusqlite::Connection) -> tokio_rusqlite::Result<T> + Send + 'static,
    {
        self.executor.call_raw(query_name, call).await
    }
}

impl PodRuntimeStore for SqliteRuntimeWorkStore {
    fn admit_pod_runtime(&self, admission: PodRuntimeAdmission) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async move {
            let (pod, node_name) = admission.into_parts();
            let now = self.now_ms();
            self.db_call("node_local:pod_runtime_admit", move |conn| {
                let updated = conn.execute(
                    POD_RUNTIME_ADMIT,
                    rusqlite::params![pod.uid, pod.namespace, pod.name, node_name, now],
                )?;
                if updated != 1 {
                    return Err(rusqlite::Error::QueryReturnedNoRows.into());
                }
                Ok(())
            })
            .await
            .map_err(|error| persistence_error("pod_runtime admit", error))
        })
    }

    fn record_owned_sandbox(&self, sandbox: OwnedPodSandbox) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async move {
            let (pod, node_name, sandbox_id, created_ms) = sandbox.into_parts();
            let conflict_uid = pod.uid.clone();
            let existing = self
                .db_call("node_local:pod_runtime_record_owned_sandbox", move |conn| {
                    let transaction = conn.transaction()?;
                    let updated = transaction.execute(
                        POD_RUNTIME_RECORD_OWNED_SANDBOX,
                        rusqlite::params![
                            pod.uid,
                            pod.namespace,
                            pod.name,
                            node_name,
                            sandbox_id,
                            created_ms,
                        ],
                    )?;
                    if updated == 1 {
                        transaction.commit()?;
                        return Ok(None);
                    }
                    let existing = transaction
                        .query_row(
                            POD_RUNTIME_OWNERSHIP_GET_UID,
                            rusqlite::params![pod.uid],
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
                    transaction.commit()?;
                    Ok(existing)
                })
                .await
                .map_err(|error| persistence_error("pod_runtime record owned sandbox", error))?;
            match existing {
                None => Ok(()),
                Some((namespace, pod_name, node_name, sandbox_id)) => {
                    Err(RuntimeWorkError::ownership_conflict(
                        conflict_uid,
                        namespace,
                        pod_name,
                        node_name,
                        sandbox_id,
                    ))
                }
            }
        })
    }

    fn record_cgroup(&self, cgroup: PodRuntimeCgroup) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async move {
            let (pod_uid, cgroup_path) = cgroup.into_parts();
            self.db_call("node_local:pod_runtime_record_cgroup", move |conn| {
                conn.execute(
                    POD_RUNTIME_RECORD_CGROUP,
                    rusqlite::params![pod_uid, cgroup_path],
                )?;
                Ok(())
            })
            .await
            .map_err(|error| persistence_error("pod_runtime record cgroup", error))
        })
    }

    fn delete_pod_runtime_for_uid(&self, pod_uid: RuntimePodUid) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async move {
            let pod_uid = pod_uid.into_inner();
            self.db_call("node_local:pod_runtime_delete", move |conn| {
                conn.execute(POD_RUNTIME_DELETE_UID, [pod_uid])?;
                Ok(())
            })
            .await
            .map_err(|error| persistence_error("pod_runtime delete", error))
        })
    }

    fn get_pod_runtime(
        &self,
        pod_uid: RuntimePodUid,
    ) -> RuntimeWorkFuture<'_, Option<PodRuntimeRecord>> {
        Box::pin(async move {
            let pod_uid = pod_uid.into_inner();
            let row = self
                .db_call("node_local:pod_runtime_get", move |conn| {
                    conn.query_row(POD_RUNTIME_GET_UID, [pod_uid], runtime_row)
                        .optional()
                        .map_err(tokio_rusqlite::Error::from)
                })
                .await
                .map_err(|error| persistence_error("pod_runtime get", error))?;
            row.map(runtime_record).transpose()
        })
    }

    fn list_pod_runtime(&self) -> RuntimeWorkFuture<'_, Vec<PodRuntimeRecord>> {
        Box::pin(async move {
            let rows = self
                .db_call("node_local:pod_runtime_list", move |conn| {
                    conn.prepare(POD_RUNTIME_LIST)?
                        .query_map([], runtime_row)?
                        .collect::<rusqlite::Result<Vec<_>>>()
                        .map_err(tokio_rusqlite::Error::from)
                })
                .await
                .map_err(|error| persistence_error("pod_runtime list", error))?;
            rows.into_iter().map(runtime_record).collect()
        })
    }

    fn list_pod_runtime_by_namespace(
        &self,
        namespace: RuntimeNamespace,
    ) -> RuntimeWorkFuture<'_, Vec<PodRuntimeRecord>> {
        Box::pin(async move {
            let namespace = namespace.into_inner();
            let rows = self
                .db_call("node_local:pod_runtime_list_ns", move |conn| {
                    conn.prepare(POD_RUNTIME_LIST_NS)?
                        .query_map([namespace], runtime_row)?
                        .collect::<rusqlite::Result<Vec<_>>>()
                        .map_err(tokio_rusqlite::Error::from)
                })
                .await
                .map_err(|error| persistence_error("pod_runtime list namespace", error))?;
            rows.into_iter().map(runtime_record).collect()
        })
    }
}

impl ProbeStateStore for SqliteRuntimeWorkStore {
    fn record_probe_result(&self, result: ProbeResult) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async move {
            let (key, success, result_ms) = result.into_parts();
            let (pod_uid, container_name, probe_kind) = key.into_parts();
            let success = i64::from(success);
            self.db_call("node_local:probe_record", move |conn| {
                conn.execute(
                    PROBE_STATE_UPSERT,
                    rusqlite::params![pod_uid, container_name, probe_kind, result_ms, success],
                )?;
                Ok(())
            })
            .await
            .map_err(|error| persistence_error("probe_state record", error))
        })
    }

    fn get_probe_state(&self, key: ProbeKey) -> RuntimeWorkFuture<'_, Option<ProbeState>> {
        Box::pin(async move {
            let (pod_uid, container_name, probe_kind) = key.into_parts();
            let row = self
                .db_call("node_local:probe_get", move |conn| {
                    conn.query_row(
                        PROBE_STATE_GET,
                        rusqlite::params![pod_uid, container_name, probe_kind],
                        |row| {
                            Ok(ProbeRow {
                                pod_uid: row.get(0)?,
                                container_name: row.get(1)?,
                                probe_kind: row.get(2)?,
                                last_result_ms: row.get(3)?,
                                last_success: row.get::<_, Option<i64>>(4)?.map(|value| value != 0),
                                consecutive_failures: row.get(5)?,
                                next_eligible_ms: row.get(6)?,
                            })
                        },
                    )
                    .optional()
                    .map_err(tokio_rusqlite::Error::from)
                })
                .await
                .map_err(|error| persistence_error("probe_state get", error))?;
            row.map(probe_state).transpose()
        })
    }
}

impl PodWorkqueueStore for SqliteRuntimeWorkStore {
    fn enqueue_work(&self, entry: PodWorkqueueEnqueue) -> RuntimeWorkFuture<'_, ()> {
        Box::pin(async move {
            let (identity, payload, attempt_count, minimum_delay_ms, last_error) =
                entry.into_parts();
            let (kind, pod) = identity.into_persisted();
            let kind = workqueue_kind(kind);
            let now = self.now_ms();
            let floor = now.saturating_add(minimum_delay_ms);
            self.db_call("node_local:workqueue_enqueue", move |conn| {
                let tail_other: i64 = conn.query_row(
                    "SELECT COALESCE(MAX(next_due_ms), 0) FROM pod_workqueue \
                     WHERE NOT (kind = ?1 AND namespace = ?2 AND pod_name = ?3 AND pod_uid = ?4)",
                    rusqlite::params![kind, pod.namespace, pod.name, pod.uid],
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
                        pod.namespace,
                        pod.name,
                        pod.uid,
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
            .map_err(|error| persistence_error("pod_workqueue enqueue", error))
        })
    }

    fn peek_next_due_ms(&self) -> RuntimeWorkFuture<'_, Option<i64>> {
        Box::pin(async move {
            self.db_call("node_local:workqueue_peek", move |conn| {
                conn.query_row("SELECT MIN(next_due_ms) FROM pod_workqueue", [], |row| {
                    row.get::<_, Option<i64>>(0)
                })
                .optional()
                .map(|value| value.flatten())
                .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .map_err(|error| persistence_error("pod_workqueue peek", error))
        })
    }

    fn claim_due_work(&self, now: DueTimeMs) -> RuntimeWorkFuture<'_, Option<PodWorkqueueEntry>> {
        Box::pin(async move {
            self.db_call("node_local:workqueue_claim", move |conn| {
                let transaction = conn.transaction()?;
                let row = transaction
                    .query_row(
                        "SELECT id, kind, namespace, pod_name, pod_uid, payload, attempt_count, next_due_ms \
                         FROM pod_workqueue WHERE next_due_ms <= ?1 ORDER BY next_due_ms ASC, id ASC LIMIT 1",
                        [now.get()],
                        |row| {
                            Ok(WorkqueueRow {
                                id: row.get(0)?,
                                kind: row.get(1)?,
                                namespace: row.get(2)?,
                                pod_name: row.get(3)?,
                                pod_uid: row.get(4)?,
                                payload: row.get(5)?,
                                attempt_count: row.get(6)?,
                                next_due_ms: row.get(7)?,
                            })
                        },
                    )
                    .optional()?;
                let Some(row) = row else {
                    transaction.commit()?;
                    return Ok(Ok(None));
                };
                let id = row.id;
                let entry = match workqueue_entry(row) {
                    Ok(entry) => entry,
                    Err(error) => return Ok(Err(error)),
                };
                transaction.execute("DELETE FROM pod_workqueue WHERE id = ?1", [id])?;
                transaction.commit()?;
                Ok(Ok(Some(entry)))
            })
            .await
            .map_err(|error| persistence_error("pod_workqueue claim", error))?
        })
    }
}

impl PodSlotAdmissionStore for SqliteRuntimeWorkStore {
    fn try_admit(
        &self,
        request: PodSlotAdmissionRequest,
    ) -> RuntimeWorkFuture<'_, PodSlotAdmissionResult> {
        Box::pin(async move {
            let (pod, node_name) = request.into_parts();
            let event_pod = pod.clone();
            let now = self.now_ms();
            let (result, event) = self
                .db_call("node_local:pod_slot_try_admit", move |conn| {
                    let transaction = conn.transaction()?;
                    let existing = read_pod_slot(&transaction, &pod.namespace, &pod.name)?;
                    let (result, event) = match existing {
                        None => {
                            let version = next_pod_slot_version(&transaction)?;
                            transaction.execute(
                                POD_SLOT_ADMISSION_INSERT,
                                rusqlite::params![
                                    pod.namespace,
                                    pod.name,
                                    pod.uid,
                                    node_name,
                                    slot_state(PodSlotAdmissionState::Admitted),
                                    version.get(),
                                    now,
                                ],
                            )?;
                            (
                                PodSlotAdmissionResult::Admitted {
                                    observed_pod_version: version,
                                },
                                Some(PodSlotAdmissionEvent::Changed {
                                    pod: event_pod,
                                    state: PodSlotAdmissionState::Admitted,
                                    observed_pod_version: version,
                                }),
                            )
                        }
                        Some(row) if row.pod_uid == pod.uid => {
                            if row.state == PodSlotAdmissionState::Admitted
                                && row.node_name == node_name
                            {
                                (
                                    PodSlotAdmissionResult::Admitted {
                                        observed_pod_version: row.version,
                                    },
                                    None,
                                )
                            } else {
                                let version = next_pod_slot_version(&transaction)?;
                                transaction.execute(
                                    POD_SLOT_ADMISSION_UPDATE,
                                    rusqlite::params![
                                        pod.namespace,
                                        pod.name,
                                        pod.uid,
                                        node_name,
                                        slot_state(PodSlotAdmissionState::Admitted),
                                        version.get(),
                                        now,
                                    ],
                                )?;
                                (
                                    PodSlotAdmissionResult::Admitted {
                                        observed_pod_version: version,
                                    },
                                    Some(PodSlotAdmissionEvent::Changed {
                                        pod: event_pod,
                                        state: PodSlotAdmissionState::Admitted,
                                        observed_pod_version: version,
                                    }),
                                )
                            }
                        }
                        Some(row) => (
                            PodSlotAdmissionResult::Blocked {
                                blocking_uid: row.pod_uid,
                                blocking_node: row.node_name,
                                state: row.state,
                                observed_pod_version: row.version,
                            },
                            None,
                        ),
                    };
                    transaction.commit()?;
                    Ok((result, event))
                })
                .await
                .map_err(|error| persistence_error("pod slot admission", error))?;
            if let Some(event) = event {
                let _ = self.pod_slot_admission_tx.send(event);
            }
            Ok(result)
        })
    }

    fn mark_terminating(
        &self,
        request: PodSlotAdmissionRequest,
    ) -> RuntimeWorkFuture<'_, PodSlotMutationResult> {
        Box::pin(async move {
            let (pod, node_name) = request.into_parts();
            let event_pod = pod.clone();
            let expected_uid = pod.uid.clone();
            let now = self.now_ms();
            let outcome = self
                .db_call("node_local:pod_slot_mark_terminating", move |conn| {
                    let transaction = conn.transaction()?;
                    let existing = read_pod_slot(&transaction, &pod.namespace, &pod.name)?;
                    let (result, event) = match existing {
                        Some(row) if row.pod_uid != pod.uid => {
                            transaction.commit()?;
                            return Ok(Err(row.pod_uid));
                        }
                        Some(row)
                            if row.state == PodSlotAdmissionState::Terminating
                                && row.node_name == node_name =>
                        {
                            (
                                PodSlotMutationResult::Unchanged {
                                    observed_pod_version: row.version,
                                },
                                None,
                            )
                        }
                        Some(_) => {
                            let version = next_pod_slot_version(&transaction)?;
                            transaction.execute(
                                POD_SLOT_ADMISSION_UPDATE,
                                rusqlite::params![
                                    pod.namespace,
                                    pod.name,
                                    pod.uid,
                                    node_name,
                                    slot_state(PodSlotAdmissionState::Terminating),
                                    version.get(),
                                    now,
                                ],
                            )?;
                            (
                                PodSlotMutationResult::Changed {
                                    observed_pod_version: version,
                                },
                                Some(PodSlotAdmissionEvent::Changed {
                                    pod: event_pod,
                                    state: PodSlotAdmissionState::Terminating,
                                    observed_pod_version: version,
                                }),
                            )
                        }
                        None => {
                            let version = next_pod_slot_version(&transaction)?;
                            transaction.execute(
                                POD_SLOT_ADMISSION_INSERT,
                                rusqlite::params![
                                    pod.namespace,
                                    pod.name,
                                    pod.uid,
                                    node_name,
                                    slot_state(PodSlotAdmissionState::Terminating),
                                    version.get(),
                                    now,
                                ],
                            )?;
                            (
                                PodSlotMutationResult::Changed {
                                    observed_pod_version: version,
                                },
                                Some(PodSlotAdmissionEvent::Changed {
                                    pod: event_pod,
                                    state: PodSlotAdmissionState::Terminating,
                                    observed_pod_version: version,
                                }),
                            )
                        }
                    };
                    transaction.commit()?;
                    Ok(Ok((result, event)))
                })
                .await
                .map_err(|error| persistence_error("pod slot terminating transition", error))?;
            let (result, event) = outcome
                .map_err(|actual_uid| RuntimeWorkError::uid_conflict(expected_uid, actual_uid))?;
            if let Some(event) = event {
                let _ = self.pod_slot_admission_tx.send(event);
            }
            Ok(result)
        })
    }

    fn clear_if_uid(
        &self,
        request: PodSlotAdmissionRequest,
    ) -> RuntimeWorkFuture<'_, PodSlotClearResult> {
        Box::pin(async move {
            let (pod, _node_name) = request.into_parts();
            let event_pod = pod.clone();
            let (result, event) = self
                .db_call("node_local:pod_slot_clear_if_uid", move |conn| {
                    let transaction = conn.transaction()?;
                    let Some(row) = read_pod_slot(&transaction, &pod.namespace, &pod.name)? else {
                        transaction.commit()?;
                        return Ok((PodSlotClearResult::NotFound, None));
                    };
                    if row.pod_uid != pod.uid {
                        transaction.commit()?;
                        return Ok((
                            PodSlotClearResult::UidMismatch {
                                blocking_uid: row.pod_uid,
                                blocking_node: row.node_name,
                                state: row.state,
                                observed_pod_version: row.version,
                            },
                            None,
                        ));
                    }
                    let version = next_pod_slot_version(&transaction)?;
                    transaction.execute(
                        POD_SLOT_ADMISSION_DELETE_IF_UID,
                        rusqlite::params![pod.namespace, pod.name, pod.uid],
                    )?;
                    transaction.commit()?;
                    Ok((
                        PodSlotClearResult::Cleared {
                            observed_pod_version: version,
                        },
                        Some(PodSlotAdmissionEvent::Cleared {
                            pod: event_pod,
                            observed_pod_version: version,
                        }),
                    ))
                })
                .await
                .map_err(|error| persistence_error("pod slot clear", error))?;
            if let Some(event) = event {
                let _ = self.pod_slot_admission_tx.send(event);
            }
            Ok(result)
        })
    }
}

impl PodSlotAdmissionEventSource for SqliteRuntimeWorkStore {
    fn subscribe(&self) -> Box<dyn PodSlotEventSubscription> {
        Box::new(SqlitePodSlotEventSubscription {
            receiver: self.pod_slot_admission_tx.subscribe(),
        })
    }
}

struct SqlitePodSlotEventSubscription {
    receiver: broadcast::Receiver<PodSlotAdmissionEvent>,
}

impl PodSlotEventSubscription for SqlitePodSlotEventSubscription {
    fn next_event(&mut self) -> RuntimeWorkFuture<'_, Option<PodSlotAdmissionEvent>> {
        Box::pin(async move {
            match self.receiver.recv().await {
                Ok(event) => Ok(Some(event)),
                Err(broadcast::error::RecvError::Closed) => Ok(None),
                Err(error) => Err(RuntimeWorkError::retryable(error.to_string())),
            }
        })
    }
}

fn persistence_error(context: &str, error: impl std::fmt::Display) -> RuntimeWorkError {
    RuntimeWorkError::persistence_failed(format!("{context} failed: {error}"))
}

struct RuntimeRow {
    pod_uid: String,
    namespace: String,
    pod_name: String,
    node_name: String,
    sandbox_id: Option<String>,
    cgroup_path: Option<String>,
    created_ms: i64,
    started_ms: Option<i64>,
}

fn runtime_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RuntimeRow> {
    Ok(RuntimeRow {
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

fn runtime_record(row: RuntimeRow) -> Result<PodRuntimeRecord, RuntimeWorkError> {
    PodRuntimeRecord::try_new(
        PodIdentity::new(&row.namespace, &row.pod_name, &row.pod_uid),
        row.node_name,
        row.sandbox_id,
        row.cgroup_path,
        row.created_ms,
        row.started_ms,
    )
    .map_err(|error| RuntimeWorkError::corrupt_data(error.to_string()))
}

struct ProbeRow {
    pod_uid: String,
    container_name: String,
    probe_kind: String,
    last_result_ms: Option<i64>,
    last_success: Option<bool>,
    consecutive_failures: i64,
    next_eligible_ms: i64,
}

fn probe_state(row: ProbeRow) -> Result<ProbeState, RuntimeWorkError> {
    ProbeState::try_new(
        ProbeKey::try_new(row.pod_uid, row.container_name, row.probe_kind)
            .map_err(|error| RuntimeWorkError::corrupt_data(error.to_string()))?,
        row.last_result_ms,
        row.last_success,
        row.consecutive_failures,
        row.next_eligible_ms,
    )
    .map_err(|error| RuntimeWorkError::corrupt_data(error.to_string()))
}

fn workqueue_kind(kind: PodWorkqueueKind) -> &'static str {
    match kind {
        PodWorkqueueKind::Pod => "pod",
        PodWorkqueueKind::Namespace => "namespace",
    }
}

fn parse_workqueue_kind(kind: &str) -> Result<PodWorkqueueKind, RuntimeWorkError> {
    match kind {
        "pod" => Ok(PodWorkqueueKind::Pod),
        "namespace" => Ok(PodWorkqueueKind::Namespace),
        other => Err(RuntimeWorkError::corrupt_data(format!(
            "invalid pod_workqueue kind {other:?}"
        ))),
    }
}

struct WorkqueueRow {
    id: i64,
    kind: String,
    namespace: String,
    pod_name: String,
    pod_uid: String,
    payload: Vec<u8>,
    attempt_count: i64,
    next_due_ms: i64,
}

fn workqueue_entry(row: WorkqueueRow) -> Result<PodWorkqueueEntry, RuntimeWorkError> {
    let kind = parse_workqueue_kind(&row.kind)?;
    let identity = PodWorkIdentity::try_from_persisted(
        kind,
        PodIdentity::new(&row.namespace, &row.pod_name, &row.pod_uid),
    )
    .map_err(|error| RuntimeWorkError::corrupt_data(error.to_string()))?;
    PodWorkqueueEntry::try_new(
        row.id,
        identity,
        row.payload,
        row.attempt_count,
        row.next_due_ms,
    )
    .map_err(|error| RuntimeWorkError::corrupt_data(error.to_string()))
}

struct PodSlotRow {
    pod_uid: String,
    node_name: String,
    state: PodSlotAdmissionState,
    version: ObservedPodVersion,
}

fn read_pod_slot(
    transaction: &rusqlite::Transaction<'_>,
    namespace: &str,
    pod_name: &str,
) -> tokio_rusqlite::Result<Option<PodSlotRow>> {
    let row = transaction
        .query_row(
            POD_SLOT_ADMISSION_SELECT,
            rusqlite::params![namespace, pod_name],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .optional()?;
    row.map(|(pod_uid, node_name, state, version)| {
        Ok(PodSlotRow {
            pod_uid,
            node_name,
            state: parse_slot_state(&state)?,
            version: ObservedPodVersion::try_new(version)
                .map_err(|error| corrupt_db_error(error.to_string()))?,
        })
    })
    .transpose()
}

fn slot_state(state: PodSlotAdmissionState) -> &'static str {
    match state {
        PodSlotAdmissionState::Admitted => "Admitted",
        PodSlotAdmissionState::Terminating => "Terminating",
    }
}

fn parse_slot_state(value: &str) -> tokio_rusqlite::Result<PodSlotAdmissionState> {
    match value {
        "Admitted" => Ok(PodSlotAdmissionState::Admitted),
        "Terminating" => Ok(PodSlotAdmissionState::Terminating),
        other => Err(corrupt_db_error(format!(
            "invalid pod slot admission state {other:?}"
        ))),
    }
}

fn next_pod_slot_version(
    transaction: &rusqlite::Transaction<'_>,
) -> tokio_rusqlite::Result<ObservedPodVersion> {
    let current = transaction
        .query_row(POD_SLOT_RV_SELECT, [], |row| row.get::<_, String>(0))
        .optional()?
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let next = current.saturating_add(1);
    transaction.execute(POD_SLOT_RV_UPSERT, [next.to_string()])?;
    ObservedPodVersion::try_new(next).map_err(|error| corrupt_db_error(error.to_string()))
}

fn corrupt_db_error(message: String) -> tokio_rusqlite::Error {
    tokio_rusqlite::Error::Other(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    )))
}
