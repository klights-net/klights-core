//! `RedbWatchStore` — watch event history, catch-up, and GC.

use std::sync::Arc;

use ::redb::{ReadableDatabase, ReadableTable};
use anyhow::Result;
use serde_json::Value;

use crate::datastore::redb::accessor::RedbAccessor;
use crate::datastore::redb::tables;
use crate::datastore::types::*;

pub struct RedbWatchStore {
    pub accessor: Arc<RedbAccessor>,
}

impl RedbWatchStore {
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

    pub async fn watch_list(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let targets_owned = targets.to_vec();
        self.db_call("watch_list", move |db| {
            let targets: &[WatchTarget] = &targets_owned;
            let r = db.begin_read()?;
            let tbl = r.open_table(tables::WATCH_EVENTS)?;
            let mut result = Vec::new();
            let start = (since_rv + 1).max(0) as u64;
            for e in tbl.range(start..)? {
                let (rv_guard, event_ref) = e?;
                let rv = rv_guard.value() as i64;
                let body = event_ref.value().to_vec();
                let event: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let ev_av = event
                    .get("apiVersion")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let ev_kind = event.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                let ev_data = event.get("data").cloned().unwrap_or(Value::Null);
                let ev_type = event
                    .get("eventType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let ev_ns = event.get("namespace").and_then(|v| v.as_str());
                let ev_name = event.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let matches = targets
                    .iter()
                    .any(|t| t.api_version == ev_av && t.kind == ev_kind);
                if matches {
                    result.push(CatchUpResource {
                        resource: Resource {
                            id: 0,
                            api_version: ev_av.to_string(),
                            kind: ev_kind.to_string(),
                            namespace: ev_ns.map(|s| s.to_string()),
                            name: ev_name.to_string(),
                            uid: Resource::uid_from_data(&ev_data),
                            resource_version: rv,
                            data: std::sync::Arc::new(ev_data),
                        },
                        event_type: std::borrow::Cow::Owned(ev_type.to_string()),
                    });
                }
            }
            Ok(result)
        })
        .await
    }

    pub async fn watch_list_deleted_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        self.db_call("watch_list_deleted_since", move |db| {
            let r = db.begin_read()?;
            let tbl = r.open_table(tables::WATCH_EVENTS)?;
            let mut result = Vec::new();
            let start = (since_rv + 1).max(0) as u64;
            for e in tbl.range(start..)? {
                let (rv_guard, event_ref) = e?;
                let rv = rv_guard.value() as i64;
                let body = event_ref.value().to_vec();
                let event: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let ev_type = event
                    .get("eventType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if ev_type != "DELETED" {
                    continue;
                }
                let ev_av = event
                    .get("apiVersion")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let ev_kind = event.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                let ev_data = event.get("data").cloned().unwrap_or(Value::Null);
                let ev_ns = event.get("namespace").and_then(|v| v.as_str());
                let ev_name = event.get("name").and_then(|v| v.as_str()).unwrap_or("");
                result.push(CatchUpResource {
                    resource: Resource {
                        id: 0,
                        api_version: ev_av.to_string(),
                        kind: ev_kind.to_string(),
                        namespace: ev_ns.map(str::to_string),
                        name: ev_name.to_string(),
                        uid: Resource::uid_from_data(&ev_data),
                        resource_version: rv,
                        data: std::sync::Arc::new(ev_data),
                    },
                    event_type: std::borrow::Cow::Borrowed("DELETED"),
                });
            }
            Ok(result)
        })
        .await
    }

    pub async fn watch_list_all_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        self.db_call("watch_list_all_since", move |db| {
            let r = db.begin_read()?;
            let tbl = r.open_table(tables::WATCH_EVENTS)?;
            let mut result = Vec::new();
            let start = (since_rv + 1).max(0) as u64;
            for e in tbl.range(start..)? {
                let (rv_guard, event_ref) = e?;
                let rv = rv_guard.value() as i64;
                let body = event_ref.value().to_vec();
                let event: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let ev_av = event
                    .get("apiVersion")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let ev_kind = event.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                let ev_data = event.get("data").cloned().unwrap_or(Value::Null);
                let ev_type = event
                    .get("eventType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let ev_ns = event.get("namespace").and_then(|v| v.as_str());
                let ev_name = event.get("name").and_then(|v| v.as_str()).unwrap_or("");
                result.push(CatchUpResource {
                    resource: Resource {
                        id: 0,
                        api_version: ev_av.to_string(),
                        kind: ev_kind.to_string(),
                        namespace: ev_ns.map(str::to_string),
                        name: ev_name.to_string(),
                        uid: Resource::uid_from_data(&ev_data),
                        resource_version: rv,
                        data: std::sync::Arc::new(ev_data),
                    },
                    event_type: std::borrow::Cow::Owned(ev_type.to_string()),
                });
            }
            Ok(result)
        })
        .await
    }

    pub async fn modified_since(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let scope = if ns.is_some() {
            WatchTargetScope::Namespaced(ns.map(|s| s.to_string()))
        } else {
            WatchTargetScope::Cluster
        };
        let targets = vec![WatchTarget {
            api_version: av.to_string(),
            kind: kind.to_string(),
            scope,
        }];
        self.watch_list(&targets, since_rv).await
    }

    pub async fn gc_watch(&self, max_rows: i64, _batch_cap: i64) -> Result<usize> {
        self.db_call("gc_watch", move |db| {
            let w = db.begin_write()?;
            let count: usize = {
                let tbl = w.open_table(tables::WATCH_EVENTS)?;
                tbl.iter()?.count()
            };
            if count <= max_rows as usize {
                w.commit()?;
                return Ok(0);
            }
            let to_remove = count - max_rows as usize;
            let keys_to_remove: Vec<u64> = {
                let tbl = w.open_table(tables::WATCH_EVENTS)?;
                let mut keys = Vec::new();
                for e in tbl.iter()? {
                    let (k, _) = e?;
                    keys.push(k.value());
                    if keys.len() >= to_remove {
                        break;
                    }
                }
                keys
            };
            let removed = {
                let mut tbl2 = w.open_table(tables::WATCH_EVENTS)?;
                let n = keys_to_remove.len();
                for k in &keys_to_remove {
                    tbl2.remove(*k)?;
                }
                n
            };
            w.commit()?;
            Ok(removed)
        })
        .await
    }

    pub async fn gc_watch_prunable_count(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        self.db_call("gc_watch_prunable_count", move |db| {
            let r = db.begin_read()?;
            let tbl = r.open_table(tables::WATCH_EVENTS)?;
            let count = tbl.iter()?.count();
            if count <= max_rows as usize {
                return Ok(0);
            }
            Ok((count - max_rows as usize).min(batch_cap as usize))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::datastore::redb::accessor::RedbAccessor;
    use crate::datastore::redb::helpers;
    use crate::datastore::redb::open_boundary;
    use crate::task_supervisor::TaskSupervisor;

    use super::*;

    fn store() -> RedbWatchStore {
        let db = open_boundary::open_in_memory_blocking().unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        RedbWatchStore::new(accessor)
    }

    fn insert_watch_event(
        s: &RedbWatchStore,
        rv: i64,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
        event_type: &str,
    ) {
        let ev = serde_json::json!({"apiVersion":av,"kind":kind,"namespace":ns,"name":name,"eventType":event_type,"data":{}});
        let db = s.accessor.db().unwrap();
        let w = db.begin_write().unwrap();
        helpers::watch_insert(&w, rv, &ev).unwrap();
        w.commit().unwrap();
    }

    #[tokio::test]
    async fn watch_list_filters_by_target() {
        let s = store();
        insert_watch_event(&s, 1, "v1", "Pod", Some("ns"), "p", "ADDED");
        insert_watch_event(&s, 2, "v1", "ConfigMap", Some("ns"), "cm", "ADDED");

        let targets = vec![WatchTarget {
            api_version: "v1".into(),
            kind: "Pod".into(),
            scope: WatchTargetScope::Namespaced(Some("ns".into())),
        }];
        let results = s.watch_list(&targets, 0).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].resource.name, "p");
    }

    #[tokio::test]
    async fn watch_list_deleted_only_returns_deleted() {
        let s = store();
        insert_watch_event(&s, 1, "v1", "Pod", Some("ns"), "p", "ADDED");
        insert_watch_event(&s, 2, "v1", "Pod", Some("ns"), "p", "DELETED");
        insert_watch_event(&s, 3, "v1", "Pod", Some("ns"), "q", "MODIFIED");

        let results = s.watch_list_deleted_since(0).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].resource.name, "p");
    }

    #[tokio::test]
    async fn watch_list_respects_since_rv() {
        let s = store();
        insert_watch_event(&s, 1, "v1", "Pod", Some("ns"), "old", "ADDED");
        insert_watch_event(&s, 2, "v1", "Pod", Some("ns"), "new", "ADDED");

        let results = s.watch_list_all_since(1).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].resource.name, "new");
    }

    #[tokio::test]
    async fn gc_watch_trims_oldest() {
        let s = store();
        for i in 1..=10 {
            insert_watch_event(&s, i, "v1", "Pod", Some("ns"), &format!("p{i}"), "ADDED");
        }
        let removed = s.gc_watch(3, 100).await.unwrap();
        assert_eq!(removed, 7);
        // After GC, only 3 remain
        let all = s.watch_list_all_since(0).await.unwrap();
        assert_eq!(all.len(), 3);
    }
}
