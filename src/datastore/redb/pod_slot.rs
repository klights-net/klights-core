//! `RedbPodSlotStore` — pod slot admission, termination, and cleanup.

use std::sync::Arc;

use ::redb::ReadableTable;
use anyhow::{Result, anyhow};
use serde_json::Value;
use tokio::sync::broadcast;

use crate::datastore::redb::accessor::RedbAccessor;
use crate::datastore::redb::helpers;
use crate::datastore::redb::tables;
use crate::datastore::types::*;

fn pod_slot_key(ns: &str, pod: &str) -> String {
    format!("{}/{}", ns, pod)
}

fn pod_slot_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn pod_slot_row_to_result(row: &Value) -> Result<PodSlotAdmissionResult> {
    let state = row
        .get("state")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("pod slot row missing state"))
        .and_then(PodSlotAdmissionState::parse)?;
    Ok(PodSlotAdmissionResult::Blocked {
        blocking_uid: row
            .get("pod_uid")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string(),
        blocking_node: row
            .get("node_name")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string(),
        state,
        resource_version: row
            .get("updated_rv")
            .and_then(|value| value.as_i64())
            .unwrap_or_default(),
    })
}

pub struct RedbPodSlotStore {
    accessor: Arc<RedbAccessor>,
    admission_tx: broadcast::Sender<PodSlotAdmissionEvent>,
}

impl RedbPodSlotStore {
    pub fn new(
        accessor: Arc<RedbAccessor>,
        admission_tx: broadcast::Sender<PodSlotAdmissionEvent>,
    ) -> Self {
        Self {
            accessor,
            admission_tx,
        }
    }

    async fn db_call<T, F>(&self, label: &str, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&::redb::Database) -> Result<T> + Send + 'static,
    {
        self.accessor.call(label, f).await
    }

    pub async fn try_admit(
        &self,
        ns: &str,
        pod: &str,
        uid: &str,
        node: &str,
    ) -> Result<PodSlotAdmissionResult> {
        let ns = ns.to_string();
        let pod = pod.to_string();
        let uid = uid.to_string();
        let node = node.to_string();
        let event_ns = ns.clone();
        let event_pod = pod.clone();
        let event_uid = uid.clone();
        let (result, event) = self
            .db_call("pod_slot_try_admit_impl", move |db| {
                let w = db.begin_write()?;
                let key = pod_slot_key(&ns, &pod);
                let existing_value = {
                    let table = w.open_table(tables::POD_SLOT_ADMISSIONS)?;
                    table.get(key.as_str())?.map(|row| row.value().to_vec())
                };
                if let Some(bytes) = existing_value {
                    let row: Value = serde_json::from_slice(&bytes).unwrap_or_default();
                    let current_uid = row
                        .get("pod_uid")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default();
                    let current_node = row
                        .get("node_name")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default();
                    let current_state = row
                        .get("state")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default();
                    let current_rv = row
                        .get("updated_rv")
                        .and_then(|value| value.as_i64())
                        .unwrap_or_default();
                    if current_uid != uid {
                        let result = pod_slot_row_to_result(&row)?;
                        w.commit()?;
                        return Ok((result, None));
                    }
                    if current_node == node
                        && current_state == PodSlotAdmissionState::Admitted.as_str()
                    {
                        w.commit()?;
                        return Ok((
                            PodSlotAdmissionResult::Admitted {
                                resource_version: current_rv,
                            },
                            None,
                        ));
                    }
                }

                let rv = helpers::incr_rv(&w)?;
                let value = serde_json::json!({
                    "pod_uid": uid,
                    "node_name": node,
                    "state": PodSlotAdmissionState::Admitted.as_str(),
                    "updated_rv": rv,
                    "updated_at_ms": pod_slot_now_ms(),
                });
                {
                    let mut table = w.open_table(tables::POD_SLOT_ADMISSIONS)?;
                    table.insert(key.as_str(), serde_json::to_vec(&value)?.as_slice())?;
                }
                w.commit()?;
                Ok((
                    PodSlotAdmissionResult::Admitted {
                        resource_version: rv,
                    },
                    Some(PodSlotAdmissionEvent::Changed {
                        namespace: event_ns,
                        pod_name: event_pod,
                        pod_uid: event_uid,
                        state: PodSlotAdmissionState::Admitted,
                        resource_version: rv,
                    }),
                ))
            })
            .await?;
        if let Some(event) = event {
            let _ = self.admission_tx.send(event);
        }
        Ok(result)
    }

    pub async fn mark_terminating(&self, ns: &str, pod: &str, uid: &str, node: &str) -> Result<()> {
        let ns = ns.to_string();
        let pod = pod.to_string();
        let uid = uid.to_string();
        let node = node.to_string();
        let event_ns = ns.clone();
        let event_pod = pod.clone();
        let event_uid = uid.clone();
        let event = self
            .db_call("pod_slot_mark_terminating_impl", move |db| {
                let w = db.begin_write()?;
                let key = pod_slot_key(&ns, &pod);
                let existing_value = {
                    let table = w.open_table(tables::POD_SLOT_ADMISSIONS)?;
                    table.get(key.as_str())?.map(|row| row.value().to_vec())
                };
                if let Some(bytes) = existing_value {
                    let row: Value = serde_json::from_slice(&bytes).unwrap_or_default();
                    let current_uid = row
                        .get("pod_uid")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default();
                    let current_node = row
                        .get("node_name")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default();
                    let current_state = row
                        .get("state")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default();
                    if current_uid != uid {
                        return Err(crate::datastore::errors::DatastoreError::conflict(
                            "pod slot admission UID precondition failed",
                        )
                        .into());
                    }
                    if current_node == node
                        && current_state == PodSlotAdmissionState::Terminating.as_str()
                    {
                        w.commit()?;
                        return Ok(None);
                    }
                }

                let rv = helpers::incr_rv(&w)?;
                let value = serde_json::json!({
                    "pod_uid": uid,
                    "node_name": node,
                    "state": PodSlotAdmissionState::Terminating.as_str(),
                    "updated_rv": rv,
                    "updated_at_ms": pod_slot_now_ms(),
                });
                {
                    let mut table = w.open_table(tables::POD_SLOT_ADMISSIONS)?;
                    table.insert(key.as_str(), serde_json::to_vec(&value)?.as_slice())?;
                }
                w.commit()?;
                Ok(Some(PodSlotAdmissionEvent::Changed {
                    namespace: event_ns,
                    pod_name: event_pod,
                    pod_uid: event_uid,
                    state: PodSlotAdmissionState::Terminating,
                    resource_version: rv,
                }))
            })
            .await?;
        if let Some(event) = event {
            let _ = self.admission_tx.send(event);
        }
        Ok(())
    }

    pub async fn clear_if_uid(&self, ns: &str, pod: &str, uid: &str, _node: &str) -> Result<()> {
        let ns = ns.to_string();
        let pod = pod.to_string();
        let uid = uid.to_string();
        let event_ns = ns.clone();
        let event_pod = pod.clone();
        let event_uid = uid.clone();
        let event = self
            .db_call("pod_slot_clear_if_uid_impl", move |db| {
                let w = db.begin_write()?;
                let key = pod_slot_key(&ns, &pod);
                let existing_value = {
                    let table = w.open_table(tables::POD_SLOT_ADMISSIONS)?;
                    table.get(key.as_str())?.map(|row| row.value().to_vec())
                };
                let Some(bytes) = existing_value else {
                    w.commit()?;
                    return Ok(None);
                };
                let row: Value = serde_json::from_slice(&bytes).unwrap_or_default();
                let current_uid = row
                    .get("pod_uid")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();
                if current_uid != uid {
                    w.commit()?;
                    return Ok(None);
                }
                let rv = helpers::incr_rv(&w)?;
                {
                    let mut table = w.open_table(tables::POD_SLOT_ADMISSIONS)?;
                    table.remove(key.as_str())?;
                }
                w.commit()?;
                Ok(Some(PodSlotAdmissionEvent::Cleared {
                    namespace: event_ns,
                    pod_name: event_pod,
                    pod_uid: event_uid,
                    resource_version: rv,
                }))
            })
            .await?;
        if let Some(event) = event {
            let _ = self.admission_tx.send(event);
        }
        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PodSlotAdmissionEvent> {
        self.admission_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::broadcast;

    use crate::datastore::redb::accessor::RedbAccessor;
    use crate::datastore::redb::open_boundary;
    use crate::datastore::types::PodSlotAdmissionEvent;
    use crate::task_supervisor::TaskSupervisor;

    use super::*;

    fn store() -> (RedbPodSlotStore, broadcast::Receiver<PodSlotAdmissionEvent>) {
        let db = open_boundary::open_in_memory_blocking().unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        let (tx, rx) = broadcast::channel(256);
        (RedbPodSlotStore::new(accessor, tx), rx)
    }

    #[tokio::test]
    async fn admission_emits_changed_event() {
        let (s, mut rx) = store();
        s.try_admit("ns", "pod", "uid", "node").await.unwrap();
        let ev = rx.recv().await.unwrap();
        assert!(
            matches!(ev, PodSlotAdmissionEvent::Changed { ref state, .. } if *state == PodSlotAdmissionState::Admitted)
        );
    }

    #[tokio::test]
    async fn mark_terminating_emits_event() {
        let (s, mut rx) = store();
        s.try_admit("ns", "pod", "uid", "node").await.unwrap();
        let _ = rx.recv().await; // consume Admitted
        s.mark_terminating("ns", "pod", "uid", "node")
            .await
            .unwrap();
        let ev = rx.recv().await.unwrap();
        assert!(
            matches!(ev, PodSlotAdmissionEvent::Changed { ref state, .. } if *state == PodSlotAdmissionState::Terminating)
        );
    }

    #[tokio::test]
    async fn clear_emits_cleared_event() {
        let (s, mut rx) = store();
        s.try_admit("ns", "pod", "uid", "node").await.unwrap();
        let _ = rx.recv().await;
        s.clear_if_uid("ns", "pod", "uid", "node").await.unwrap();
        let ev = rx.recv().await.unwrap();
        assert!(matches!(ev, PodSlotAdmissionEvent::Cleared { .. }));
    }
}
