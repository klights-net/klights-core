//! `RedbWorkqueueStore` — pod workqueue enqueue/claim/complete/dead-letter.

use std::sync::Arc;

use ::redb::{ReadableDatabase, ReadableTable};
use anyhow::Result;
use serde_json::Value;

use crate::datastore::redb::accessor::RedbAccessor;
use crate::datastore::redb::helpers;
use crate::datastore::redb::tables;
use crate::datastore::types::*;
use crate::pod_identity::PodIdentity;

pub struct RedbWorkqueueStore {
    accessor: Arc<RedbAccessor>,
}

impl RedbWorkqueueStore {
    pub fn new(accessor: Arc<RedbAccessor>) -> Self {
        Self { accessor }
    }

    async fn db_call<T, F>(&self, label: &str, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&::redb::Database) -> Result<T> + Send + 'static,
    {
        self.accessor.call(label, f).await
    }

    pub async fn enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
        let name_owned = pod.name.clone();
        let namespace_owned = pod.namespace.clone();
        let uid_owned = pod.uid.clone();
        let last_error_owned = last_error.map(|s| s.to_string());
        self.db_call("workqueue_enqueue_impl", move |db| {
            let name: &str = &name_owned;
            let namespace: &str = &namespace_owned;
            let uid: &str = &uid_owned;
            let last_error: Option<&str> = last_error_owned.as_deref();
            let now = helpers::now_ms();
            let floor = now.saturating_add(min_delay_ms.max(0));
            let w = db.begin_write()?;

            let tail_other: i64 = {
                let t = w.open_table(tables::POD_WORKQUEUE)?;
                let mut max_due = 0i64;
                for e in t.iter()? {
                    let (_, val) = e?;
                    let v: Value = serde_json::from_slice(val.value()).unwrap_or_default();
                    if v.get("kind").and_then(|s| s.as_str()) == Some(kind.as_str())
                        && v.get("namespace").and_then(|s| s.as_str()) == Some(namespace)
                        && v.get("name").and_then(|s| s.as_str()) == Some(name)
                        && v.get("uid").and_then(|s| s.as_str()) == Some(uid)
                    {
                        let due = v
                            .get("next_attempt_at_ms")
                            .and_then(|x| x.as_i64())
                            .unwrap_or(0);
                        max_due = max_due.max(due);
                    }
                }
                max_due
            };
            let next_attempt_at_ms = floor.max(tail_other.saturating_add(1));

            {
                let mut t = w.open_table(tables::POD_WORKQUEUE)?;
                let mut keys_to_remove = Vec::new();
                for e in t.iter()? {
                    let (k, val) = e?;
                    let v: Value = serde_json::from_slice(val.value()).unwrap_or_default();
                    if v.get("kind").and_then(|s| s.as_str()) == Some(kind.as_str())
                        && v.get("namespace").and_then(|s| s.as_str()) == Some(namespace)
                        && v.get("name").and_then(|s| s.as_str()) == Some(name)
                        && v.get("uid").and_then(|s| s.as_str()) == Some(uid)
                    {
                        keys_to_remove.push(k.value());
                    }
                }
                for k in keys_to_remove {
                    t.remove(k)?;
                }
            }

            let id = {
                let mut meta = w.open_table(tables::META)?;
                let wq_id = meta
                    .get("wq_id")?
                    .map(|g| {
                        std::str::from_utf8(g.value())
                            .unwrap_or("0")
                            .parse::<i64>()
                            .unwrap_or(0)
                    })
                    .unwrap_or(0)
                    + 1;
                meta.insert("wq_id", wq_id.to_string().as_bytes())?;
                wq_id
            };

            {
                let mut t = w.open_table(tables::POD_WORKQUEUE)?;
                let v = serde_json::json!({
                    "kind": kind.as_str(),
                    "namespace": namespace,
                    "name": name,
                    "uid": uid,
                    "payload": payload,
                    "attempt_count": attempt_count,
                    "next_attempt_at_ms": next_attempt_at_ms,
                    "last_error": last_error,
                    "created_at_ms": now,
                });
                t.insert(id as u64, serde_json::to_vec(&v)?.as_slice())?;
            }
            w.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn peek_next_due(&self) -> Result<Option<i64>> {
        self.db_call("workqueue_peek_next_due_impl", move |db| {
            let r = db.begin_read()?;
            let t = r.open_table(tables::POD_WORKQUEUE)?;
            let now = helpers::now_ms();
            for e in t.iter()? {
                let (_, val) = e?;
                let v: Value = serde_json::from_slice(val.value()).unwrap_or_default();
                let due = v
                    .get("next_attempt_at_ms")
                    .and_then(|x| x.as_i64())
                    .unwrap_or(i64::MAX);
                if due <= now {
                    return Ok(Some(due));
                }
            }
            Ok(None)
        })
        .await
    }

    pub async fn claim_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>> {
        self.db_call("workqueue_claim_due_impl", move |db| {
            let w = db.begin_write()?;
            let mut claimed: Option<PodWorkqueueEntry> = None;
            let mut claimed_key: Option<u64> = None;
            {
                let t = w.open_table(tables::POD_WORKQUEUE)?;
                for e in t.iter()? {
                    let (k, val) = e?;
                    let v: Value = serde_json::from_slice(val.value()).unwrap_or_default();
                    let due = v
                        .get("next_attempt_at_ms")
                        .and_then(|x| x.as_i64())
                        .unwrap_or(i64::MAX);
                    if due <= now_ms {
                        let kind_str = v.get("kind").and_then(|s| s.as_str()).unwrap_or("pod");
                        let kind =
                            PodWorkqueueKind::parse(kind_str).unwrap_or(PodWorkqueueKind::Pod);
                        claimed = Some(PodWorkqueueEntry {
                            id: k.value() as i64,
                            kind,
                            namespace: v
                                .get("namespace")
                                .and_then(|s| s.as_str())
                                .unwrap_or("")
                                .to_string(),
                            name: v
                                .get("name")
                                .and_then(|s| s.as_str())
                                .unwrap_or("")
                                .to_string(),
                            uid: v
                                .get("uid")
                                .and_then(|s| s.as_str())
                                .unwrap_or("")
                                .to_string(),
                            payload: v.get("payload").cloned().unwrap_or(Value::Null),
                            attempt_count: v
                                .get("attempt_count")
                                .and_then(|x| x.as_i64())
                                .unwrap_or(0),
                            next_attempt_at_ms: due,
                        });
                        claimed_key = Some(k.value());
                        break;
                    }
                }
            }
            if let Some(key) = claimed_key {
                let mut t = w.open_table(tables::POD_WORKQUEUE)?;
                t.remove(key)?;
            }
            w.commit()?;
            Ok(claimed)
        })
        .await
    }

    pub async fn complete(&self, id: i64) -> Result<()> {
        self.db_call("workqueue_complete_impl", move |db| {
            let w = db.begin_write()?;
            {
                let mut t = w.open_table(tables::POD_WORKQUEUE)?;
                t.remove(id as u64)?;
            }
            w.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn record_failure(
        &self,
        row: PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> Result<()> {
        let pod = PodIdentity::new(&row.namespace, &row.name, &row.uid);
        self.enqueue(
            row.kind,
            &pod,
            row.payload,
            row.attempt_count.saturating_add(1),
            min_delay_ms,
            Some(error),
        )
        .await
    }

    pub async fn dead_letter(&self, id: i64, _error: &str) -> Result<()> {
        self.complete(id).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use crate::datastore::redb::accessor::RedbAccessor;
    use crate::datastore::redb::open_boundary;
    use crate::task_supervisor::TaskSupervisor;

    use super::*;

    fn store() -> RedbWorkqueueStore {
        let db = open_boundary::open_in_memory_blocking().unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        RedbWorkqueueStore::new(accessor)
    }

    #[tokio::test]
    async fn enqueue_claim_complete_flow() {
        let s = store();
        let pod = PodIdentity::new("ns", "name", "uid");
        s.enqueue(PodWorkqueueKind::Pod, &pod, json!({}), 0, 0, None)
            .await
            .unwrap();
        let due = s.peek_next_due().await.unwrap();
        assert!(due.is_some());
        let entry = s.claim_due(due.unwrap() + 1000).await.unwrap();
        assert!(entry.is_some());
        let e = entry.unwrap();
        assert_eq!(e.namespace, "ns");
        assert_eq!(e.name, "name");
        s.complete(e.id).await.unwrap();
        assert!(s.peek_next_due().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn enqueue_with_delay_not_due_yet() {
        let s = store();
        let pod = PodIdentity::new("ns", "d", "u");
        s.enqueue(PodWorkqueueKind::Pod, &pod, json!({}), 0, 1_000_000, None)
            .await
            .unwrap();
        assert!(s.peek_next_due().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn record_failure_increments_attempt_count() {
        let s = store();
        let pod = PodIdentity::new("ns", "f", "u");
        s.enqueue(PodWorkqueueKind::Pod, &pod, json!({}), 0, 0, None)
            .await
            .unwrap();
        let entry = s.claim_due(i64::MAX).await.unwrap().unwrap();
        s.record_failure(entry, 0, "boom").await.unwrap();
        let retry = s.claim_due(i64::MAX).await.unwrap().unwrap();
        assert_eq!(retry.attempt_count, 1);
    }

    #[tokio::test]
    async fn upsert_removes_prior_entry_same_identity() {
        let s = store();
        let pod = PodIdentity::new("ns", "u", "uid");
        s.enqueue(PodWorkqueueKind::Pod, &pod, json!({}), 0, 0, None)
            .await
            .unwrap();
        s.enqueue(PodWorkqueueKind::Pod, &pod, json!({}), 0, 0, None)
            .await
            .unwrap();
        // Should only have one entry — claim it, then peek should be empty.
        let entry = s.claim_due(i64::MAX).await.unwrap().unwrap();
        s.complete(entry.id).await.unwrap();
        assert!(s.peek_next_due().await.unwrap().is_none());
    }
}
