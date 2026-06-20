//! `RedbWatchStore` — watch event history, catch-up, and GC.

use std::{collections::BTreeMap, sync::Arc};

use ::redb::{ReadableDatabase, ReadableTable};
use anyhow::Result;
use serde_json::Value;

use crate::datastore::redb::accessor::RedbAccessor;
use crate::datastore::redb::tables;
use crate::datastore::types::*;

const CLUSTER_NAMESPACE_KEY: &str = "#cluster";

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
            Self::watch_list_in_read(&r, targets, since_rv)
        })
        .await
    }

    pub async fn watch_list_checked(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<WatchReplayRead> {
        if targets.is_empty() {
            return Ok(WatchReplayRead::Events(Vec::new()));
        }

        let targets_owned = targets.to_vec();
        self.db_call("watch_list_checked", move |db| {
            let targets: &[WatchTarget] = &targets_owned;
            let r = db.begin_read()?;
            if since_rv > 0 {
                for target in targets {
                    if let Some(floor_rv) = target_floor(&r, target)?
                        && since_rv < floor_rv
                    {
                        return Ok(WatchReplayRead::Expired);
                    }
                }
            }
            Self::watch_list_in_read(&r, targets, since_rv).map(WatchReplayRead::Events)
        })
        .await
    }

    pub async fn watch_list_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead> {
        match self.watch_list_checked(targets, since_rv).await? {
            WatchReplayRead::Events(mut events) => {
                events.truncate(limit.get());
                Ok(WatchReplayRead::Events(events))
            }
            WatchReplayRead::Expired => Ok(WatchReplayRead::Expired),
        }
    }

    fn watch_list_in_read(
        read_txn: &::redb::ReadTransaction,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let tbl = read_txn.open_table(tables::WATCH_EVENTS)?;
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
            let ev_ns = event.get("namespace").and_then(|v| v.as_str());
            if !targets
                .iter()
                .any(|target| watch_event_matches_target(target, ev_av, ev_kind, ev_ns))
            {
                continue;
            }

            let ev_data = event.get("data").cloned().unwrap_or(Value::Null);
            let ev_type = event
                .get("eventType")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let ev_name = event.get("name").and_then(|v| v.as_str()).unwrap_or("");
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
        Ok(result)
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

    pub async fn gc_watch(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
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
            let to_remove = (count - max_rows as usize).min(batch_cap as usize);
            let (keys_to_remove, floor_updates): (Vec<u64>, BTreeMap<Vec<u8>, u64>) = {
                let tbl = w.open_table(tables::WATCH_EVENTS)?;
                let mut keys = Vec::new();
                let mut floor_updates: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
                for e in tbl.iter()? {
                    let (k, event_ref) = e?;
                    let rv = k.value();
                    let body = event_ref.value().to_vec();
                    let event: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    if let Some(key) = floor_key_for_event(&event) {
                        floor_updates
                            .entry(key)
                            .and_modify(|floor| *floor = (*floor).max(rv))
                            .or_insert(rv);
                    }
                    keys.push(rv);
                    if keys.len() >= to_remove {
                        break;
                    }
                }
                (keys, floor_updates)
            };
            {
                let mut floors = w.open_table(tables::WATCH_REPLAY_FLOORS)?;
                for (key, floor_rv) in floor_updates {
                    let existing = floors.get(key.as_slice())?.map(|guard| guard.value());
                    if existing.is_none_or(|current| floor_rv > current) {
                        floors.insert(key.as_slice(), floor_rv)?;
                    }
                }
            }
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

fn watch_event_matches_target(
    target: &WatchTarget,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
) -> bool {
    if target.api_version != api_version || target.kind != kind {
        return false;
    }
    match &target.scope {
        WatchTargetScope::Cluster => namespace.is_none(),
        WatchTargetScope::Namespaced(Some(want)) => namespace == Some(want.as_str()),
        WatchTargetScope::Namespaced(None) => namespace.is_some(),
    }
}

fn target_floor(read_txn: &::redb::ReadTransaction, target: &WatchTarget) -> Result<Option<i64>> {
    match &target.scope {
        WatchTargetScope::Cluster => read_floor(
            read_txn,
            &target.api_version,
            &target.kind,
            CLUSTER_NAMESPACE_KEY,
        ),
        WatchTargetScope::Namespaced(Some(namespace)) => {
            read_floor(read_txn, &target.api_version, &target.kind, namespace)
        }
        WatchTargetScope::Namespaced(None) => {
            read_namespaced_all_floor(read_txn, &target.api_version, &target.kind)
        }
    }
}

fn read_floor(
    read_txn: &::redb::ReadTransaction,
    api_version: &str,
    kind: &str,
    namespace_key: &str,
) -> Result<Option<i64>> {
    let floors = read_txn.open_table(tables::WATCH_REPLAY_FLOORS)?;
    let key = floor_key(api_version, kind, namespace_key);
    Ok(floors
        .get(key.as_slice())?
        .map(|floor| floor.value() as i64))
}

fn read_namespaced_all_floor(
    read_txn: &::redb::ReadTransaction,
    api_version: &str,
    kind: &str,
) -> Result<Option<i64>> {
    let floors = read_txn.open_table(tables::WATCH_REPLAY_FLOORS)?;
    let prefix = floor_key_prefix(api_version, kind);
    let mut floor = None;
    for entry in floors.iter()? {
        let (key, value) = entry?;
        let key = key.value();
        let namespace_key = match key.strip_prefix(prefix.as_slice()) {
            Some(namespace_key) if namespace_key != CLUSTER_NAMESPACE_KEY.as_bytes() => {
                namespace_key
            }
            _ => continue,
        };
        if namespace_key.is_empty() {
            continue;
        }
        let rv = value.value() as i64;
        floor = Some(floor.map_or(rv, |current: i64| current.max(rv)));
    }
    Ok(floor)
}

fn floor_key_for_event(event: &Value) -> Option<Vec<u8>> {
    let api_version = event.get("apiVersion").and_then(|value| value.as_str())?;
    let kind = event.get("kind").and_then(|value| value.as_str())?;
    let namespace_key = event
        .get("namespace")
        .and_then(|value| value.as_str())
        .unwrap_or(CLUSTER_NAMESPACE_KEY);
    Some(floor_key(api_version, kind, namespace_key))
}

fn floor_key(api_version: &str, kind: &str, namespace_key: &str) -> Vec<u8> {
    let mut key = floor_key_prefix(api_version, kind);
    key.extend_from_slice(namespace_key.as_bytes());
    key
}

fn floor_key_prefix(api_version: &str, kind: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(api_version.len() + kind.len() + 2);
    key.extend_from_slice(api_version.as_bytes());
    key.push(0);
    key.extend_from_slice(kind.as_bytes());
    key.push(0);
    key
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
