//! Historical-snapshot reconstruction for LIST `resourceVersionMatch=Exact`
//! and consistent paginated continuations.
//!
//! Reconstructs the set of resources of a `(api_version, kind, namespace)` as
//! they existed at a past `resourceVersion` by combining the live rows
//! (unchanged since that rv) with the durable `watch_events` history (the
//! latest event at-or-before the requested rv for every key that changed
//! afterwards). When the requested rv predates the retained window we cannot
//! rebuild a faithful snapshot, so the caller is told to answer `410 Gone`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::Result;
use rusqlite::{OptionalExtension, ToSql};
use serde_json::Value;

use super::super::queries;
use super::super::scope::use_namespaced_table;
use super::*;
use crate::datastore::types::{Resource, ResourceList, ResourceListQuery, SnapshotAtRv};
use crate::label_selector::LabelSelector;

/// Per-key history facts derived from `watch_events`, relative to the requested
/// snapshot rv `N`.
#[derive(Default)]
struct NameHistory {
    /// `(rv, event_type)` of the latest event with `rv <= N`, if any.
    latest_le_n: Option<(i64, String)>,
    /// Event type of the earliest event with `rv > N`, if any.
    earliest_gt_n_type: Option<String>,
}

/// Closure result: the reconstruction either defers to the live list, reports
/// the rv as unreconstructable, or yields the raw (unfiltered, unpaginated)
/// snapshot items sorted by name.
enum RawSnapshot {
    Current,
    Expired,
    Items(Vec<Resource>),
}

fn json_from_bytes(bytes: Vec<u8>) -> rusqlite::Result<Value> {
    serde_json::from_slice(&bytes).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))
}

fn resource_from_event(
    api_version: &str,
    kind: &str,
    name: &str,
    rv: i64,
    data: Arc<Value>,
    namespaced: bool,
) -> Resource {
    let namespace = if namespaced {
        data.pointer("/metadata/namespace")
            .and_then(|v| v.as_str())
            .map(str::to_string)
    } else {
        None
    };
    Resource {
        id: 0,
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace,
        name: name.to_string(),
        uid: Resource::uid_from_data(&data),
        resource_version: rv,
        data,
    }
}

impl Datastore {
    pub async fn snapshot_resources_at_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
        snapshot_rv: i64,
    ) -> Result<SnapshotAtRv> {
        let av = api_version.to_string();
        let k = kind.to_string();
        let ns_owned = namespace.map(str::to_string);
        let namespaced = use_namespaced_table(api_version, kind, &namespace);
        // Namespaces are cluster-scoped but persist in their own `namespaces`
        // table rather than the generic cluster_resources table.
        let is_namespace = api_version == "v1" && kind == "Namespace";
        let n = snapshot_rv;

        let raw = self
            .db_call("snapshot_resources_at_rv", move |conn| {
                let tx = conn.transaction()?;

                let current_rv = Self::current_resource_version_in_tx(&tx)?;
                if n >= current_rv {
                    return Ok(RawSnapshot::Current);
                }

                // The window must retain every event with rv > N (so we see all
                // post-N changes) plus enough <= N history to rebuild changed
                // keys. Mirror the watch 410 floor: earliest retained <= N + 1.
                let earliest: Option<i64> = tx
                    .query_row(queries::WATCH_EVENTS_MIN_RV, [], |r| r.get(0))
                    .optional()?;
                match earliest {
                    Some(e) if n + 1 >= e => {}
                    _ => return Ok(RawSnapshot::Expired),
                }

                // 1. Live rows for the target (with created_rv to tell apart
                //    "existed at N" from "created after N"). Namespaces live in
                //    a dedicated table without a created_rv column, so their
                //    existence at N is derived from watch_events history instead
                //    (created_rv = None).
                let mut current: HashMap<String, (i64, Option<i64>, Resource)> = HashMap::new();
                if is_namespace {
                    let mut stmt = tx.prepare(
                        "SELECT name, resource_version, uid, data FROM namespaces",
                    )?;
                    let rows = stmt.query_map([], |row| {
                        let name: String = row.get(0)?;
                        let rv: i64 = row.get(1)?;
                        let uid: String = row.get(2)?;
                        let data = Arc::new(json_from_bytes(row.get(3)?)?);
                        Ok((
                            name.clone(),
                            rv,
                            Resource {
                                id: 0,
                                api_version: av.clone(),
                                kind: k.clone(),
                                namespace: None,
                                name,
                                uid,
                                resource_version: rv,
                                data,
                            },
                        ))
                    })?;
                    for row in rows {
                        let (name, rv, res) = row?;
                        current.insert(name, (rv, None, res));
                    }
                } else if namespaced {
                    let mut sql = "SELECT name, namespace, resource_version, created_rv, uid, data \
                         FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2"
                        .to_string();
                    let mut params: Vec<Box<dyn ToSql>> =
                        vec![Box::new(av.clone()), Box::new(k.clone())];
                    if let Some(ns) = &ns_owned {
                        sql.push_str(" AND namespace = ?3");
                        params.push(Box::new(ns.clone()));
                    }
                    let pref: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();
                    let mut stmt = tx.prepare(&sql)?;
                    let rows = stmt.query_map(&pref[..], |row| {
                        let name: String = row.get(0)?;
                        let namespace: String = row.get(1)?;
                        let rv: i64 = row.get(2)?;
                        let created_rv: i64 = row.get(3)?;
                        let uid: String = row.get(4)?;
                        let data = Arc::new(json_from_bytes(row.get(5)?)?);
                        Ok((
                            name.clone(),
                            rv,
                            created_rv,
                            Resource {
                                id: 0,
                                api_version: av.clone(),
                                kind: k.clone(),
                                namespace: Some(namespace),
                                name,
                                uid,
                                resource_version: rv,
                                data,
                            },
                        ))
                    })?;
                    for row in rows {
                        let (name, rv, created_rv, res) = row?;
                        current.insert(name, (rv, Some(created_rv), res));
                    }
                } else {
                    let mut stmt = tx.prepare(
                        "SELECT name, resource_version, created_rv, uid, data \
                         FROM cluster_resources WHERE api_version = ?1 AND kind = ?2",
                    )?;
                    let rows = stmt.query_map(rusqlite::params![av, k], |row| {
                        let name: String = row.get(0)?;
                        let rv: i64 = row.get(1)?;
                        let created_rv: i64 = row.get(2)?;
                        let uid: String = row.get(3)?;
                        let data = Arc::new(json_from_bytes(row.get(4)?)?);
                        Ok((
                            name.clone(),
                            rv,
                            created_rv,
                            Resource {
                                id: 0,
                                api_version: av.clone(),
                                kind: k.clone(),
                                namespace: None,
                                name,
                                uid,
                                resource_version: rv,
                                data,
                            },
                        ))
                    })?;
                    for row in rows {
                        let (name, rv, created_rv, res) = row?;
                        current.insert(name, (rv, Some(created_rv), res));
                    }
                }

                // 2. Per-key history facts from watch_events (structure only —
                //    object bytes are fetched lazily for the keys we need).
                let mut histories: HashMap<String, NameHistory> = HashMap::new();
                {
                    let mut sql = "SELECT name, resource_version, event_type FROM watch_events \
                         WHERE api_version = ?1 AND kind = ?2"
                        .to_string();
                    let mut params: Vec<Box<dyn ToSql>> =
                        vec![Box::new(av.clone()), Box::new(k.clone())];
                    if namespaced {
                        if let Some(ns) = &ns_owned {
                            sql.push_str(" AND namespace = ?3");
                            params.push(Box::new(ns.clone()));
                        } else {
                            sql.push_str(" AND namespace IS NOT NULL");
                        }
                    } else {
                        sql.push_str(" AND namespace IS NULL");
                    }
                    sql.push_str(" ORDER BY name, resource_version");
                    let pref: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();
                    let mut stmt = tx.prepare(&sql)?;
                    let rows = stmt.query_map(&pref[..], |row| {
                        let name: String = row.get(0)?;
                        let rv: i64 = row.get(1)?;
                        let etype: String = row.get(2)?;
                        Ok((name, rv, etype))
                    })?;
                    for row in rows {
                        let (name, rv, etype) = row?;
                        let h = histories.entry(name).or_default();
                        if rv <= n {
                            // Ascending order: the last <= N seen wins.
                            h.latest_le_n = Some((rv, etype));
                        } else if h.earliest_gt_n_type.is_none() {
                            h.earliest_gt_n_type = Some(etype);
                        }
                    }
                }

                // 3. Decide each key's state at N.
                let mut result: BTreeMap<String, Resource> = BTreeMap::new();
                let mut to_apply: Vec<(String, i64)> = Vec::new();
                let mut expired = false;

                for (name, (rv, created_rv, res)) in &current {
                    if *rv <= n {
                        // Unchanged since rv <= N: live row is the state at N.
                        result.insert(name.clone(), res.clone());
                        continue;
                    }
                    // Changed after N — rebuild from the latest event <= N.
                    match histories.get(name).and_then(|h| h.latest_le_n.as_ref()) {
                        Some((le_rv, etype)) if etype != "DELETED" => {
                            to_apply.push((name.clone(), *le_rv));
                        }
                        Some(_) => { /* deleted at/before N, re-created after: absent */ }
                        None => {
                            // Did this key exist at N? The live row changed after
                            // N (rv > N), so every one of its post-N events is
                            // retained (window floor <= N+1) and earliest_gt_n is
                            // populated. With a created_rv column we trust it;
                            // otherwise (namespaces) the earliest retained change
                            // being its creation (ADDED) means it was born after N.
                            let existed_at_n = match created_rv {
                                Some(crv) => *crv <= n,
                                None => {
                                    histories
                                        .get(name)
                                        .and_then(|h| h.earliest_gt_n_type.as_deref())
                                        != Some("ADDED")
                                }
                            };
                            if existed_at_n {
                                // Existed at N but its pre-N history was compacted.
                                expired = true;
                            }
                            // else: created after N → absent at N.
                        }
                    }
                }

                for (name, h) in &histories {
                    if current.contains_key(name) {
                        continue;
                    }
                    // Key absent from live rows → deleted after N (or never lived
                    // past the window).
                    match h.latest_le_n.as_ref() {
                        Some((le_rv, etype)) if etype != "DELETED" => {
                            to_apply.push((name.clone(), *le_rv));
                        }
                        Some(_) => { /* deleted at/before N: absent */ }
                        None => {
                            if h.earliest_gt_n_type.as_deref() != Some("ADDED") {
                                // Earliest retained change is a modify/delete, so
                                // the key existed at N but pre-N state is gone.
                                expired = true;
                            }
                            // else: first retained event is its creation (> N) →
                            // absent at N.
                        }
                    }
                }

                if expired {
                    return Ok(RawSnapshot::Expired);
                }

                // 4. Fetch object bytes for the rebuilt keys (one row per rv —
                //    resource_version is UNIQUE in watch_events).
                if !to_apply.is_empty() {
                    let rvs: Vec<i64> = to_apply.iter().map(|(_, rv)| *rv).collect();
                    let placeholders = (0..rvs.len())
                        .map(|i| format!("?{}", i + 1))
                        .collect::<Vec<_>>()
                        .join(",");
                    let sql = format!(
                        "SELECT resource_version, data FROM watch_events WHERE resource_version IN ({placeholders})"
                    );
                    let pref: Vec<&dyn ToSql> =
                        rvs.iter().map(|rv| rv as &dyn ToSql).collect();
                    let mut stmt = tx.prepare(&sql)?;
                    let rows = stmt.query_map(&pref[..], |row| {
                        let rv: i64 = row.get(0)?;
                        let data = Arc::new(json_from_bytes(row.get(1)?)?);
                        Ok((rv, data))
                    })?;
                    let mut data_by_rv: HashMap<i64, Arc<Value>> = HashMap::new();
                    for row in rows {
                        let (rv, data) = row?;
                        data_by_rv.insert(rv, data);
                    }
                    for (name, le_rv) in &to_apply {
                        if let Some(data) = data_by_rv.get(le_rv) {
                            result.insert(
                                name.clone(),
                                resource_from_event(&av, &k, name, *le_rv, data.clone(), namespaced),
                            );
                        }
                    }
                }

                Ok(RawSnapshot::Items(result.into_values().collect()))
            })
            .await?;

        let items = match raw {
            RawSnapshot::Current => return Ok(SnapshotAtRv::Current),
            RawSnapshot::Expired => return Ok(SnapshotAtRv::Expired),
            RawSnapshot::Items(items) => items,
        };

        Ok(SnapshotAtRv::List(paginate_snapshot(
            items,
            query,
            snapshot_rv,
        )?))
    }
}

/// Apply label/field selectors and keyset pagination to the reconstructed
/// (name-sorted) snapshot, reusing the same matchers as the live list/watch
/// paths so Exact and live LISTs agree.
fn paginate_snapshot(
    items: Vec<Resource>,
    query: ResourceListQuery<'_>,
    snapshot_rv: i64,
) -> Result<ResourceList> {
    let parsed_label = match query.label_selector.filter(|s| !s.trim().is_empty()) {
        Some(s) => Some(LabelSelector::parse(s)?),
        None => None,
    };
    let field = query.field_selector.filter(|s| !s.trim().is_empty());

    let mut filtered: Vec<Resource> = items
        .into_iter()
        .filter(|r| {
            parsed_label
                .as_ref()
                .is_none_or(|sel| sel.matches_resource(&r.data))
                && crate::watch::value_matches_field_selector(&r.data, field)
        })
        .collect();

    // Keyset continuation (items are already sorted by name ascending).
    if let Some(cont) = query.continue_token.filter(|t| !t.is_empty()) {
        filtered.retain(|r| r.name.as_str() > cont);
    }

    let total = filtered.len() as i64;
    let (page, continue_token, remaining_item_count) = match query.limit.filter(|l| *l > 0) {
        Some(limit) if total > limit => {
            let page: Vec<Resource> = filtered.into_iter().take(limit as usize).collect();
            let last = page.last().map(|r| r.name.clone());
            (page, last, Some(total - limit))
        }
        _ => (filtered, None, None),
    };

    Ok(ResourceList {
        items: page,
        resource_version: snapshot_rv,
        continue_token,
        remaining_item_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::sqlite::Datastore;
    use serde_json::json;

    async fn put(db: &Datastore, name: &str, val: &str) -> i64 {
        let r = db
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                name,
                json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": name, "namespace": "default"},
                    "data": {"k": val}
                }),
            )
            .await
            .unwrap();
        r.resource_version
    }

    fn sorted_names(list: &ResourceList) -> Vec<String> {
        let mut v: Vec<String> = list.items.iter().map(|r| r.name.clone()).collect();
        v.sort();
        v
    }

    #[tokio::test]
    async fn snapshot_reconstructs_state_at_past_rv() {
        let db = Datastore::new_in_memory().await.unwrap();
        put(&db, "a", "old").await;
        let rb = put(&db, "b", "bee").await; // snapshot point: {a:old, b:bee}

        // Mutations after the snapshot point must not leak into the snapshot.
        let cur_a = db
            .get_resource("v1", "ConfigMap", Some("default"), "a")
            .await
            .unwrap()
            .unwrap();
        db.update_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "a",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "a", "namespace": "default"},
                "data": {"k": "new"}
            }),
            cur_a.resource_version,
        )
        .await
        .unwrap();
        db.delete_resource("v1", "ConfigMap", Some("default"), "b")
            .await
            .unwrap();
        put(&db, "c", "see").await;

        let snap = db
            .snapshot_resources_at_rv(
                "v1",
                "ConfigMap",
                Some("default"),
                ResourceListQuery::all(),
                rb,
            )
            .await
            .unwrap();
        let list = match snap {
            SnapshotAtRv::List(l) => l,
            other => panic!("expected List, got {other:?}"),
        };
        assert_eq!(
            sorted_names(&list),
            vec!["a".to_string(), "b".to_string()],
            "snapshot at rb must contain a (deleted-after view) and b, not c"
        );
        assert_eq!(list.resource_version, rb);
        let a = list.items.iter().find(|r| r.name == "a").unwrap();
        assert_eq!(
            a.data.pointer("/data/k").and_then(|v| v.as_str()),
            Some("old"),
            "a must show its pre-update value at the snapshot rv"
        );
    }

    #[tokio::test]
    async fn snapshot_at_or_after_current_defers_to_live() {
        let db = Datastore::new_in_memory().await.unwrap();
        put(&db, "a", "x").await;
        let cur = db.get_current_resource_version().await.unwrap();
        let snap = db
            .snapshot_resources_at_rv(
                "v1",
                "ConfigMap",
                Some("default"),
                ResourceListQuery::all(),
                cur,
            )
            .await
            .unwrap();
        assert!(matches!(snap, SnapshotAtRv::Current));
    }

    #[tokio::test]
    async fn snapshot_below_retained_window_is_expired() {
        let db = Datastore::new_in_memory().await.unwrap();
        let ra = put(&db, "a", "x").await;
        for i in 0..5 {
            put(&db, &format!("p{i}"), "y").await;
        }
        // Prune the window to the single most recent event so `ra` drops out.
        db.gc_watch_events(1, 1000).await.unwrap();
        let snap = db
            .snapshot_resources_at_rv(
                "v1",
                "ConfigMap",
                Some("default"),
                ResourceListQuery::all(),
                ra,
            )
            .await
            .unwrap();
        assert!(
            matches!(snap, SnapshotAtRv::Expired),
            "an rv below the retained window must be Expired"
        );
    }

    #[tokio::test]
    async fn snapshot_applies_selectors_and_pagination() {
        let db = Datastore::new_in_memory().await.unwrap();
        for name in ["a", "b", "c"] {
            put(&db, name, "v").await;
        }
        let rv = put(&db, "d", "v").await; // snapshot over {a,b,c,d}
        put(&db, "e", "v").await; // after snapshot — excluded

        // Page 1: limit 2 over the historical set.
        let snap = db
            .snapshot_resources_at_rv(
                "v1",
                "ConfigMap",
                Some("default"),
                ResourceListQuery::new(None, None, Some(2), None),
                rv,
            )
            .await
            .unwrap();
        let list = match snap {
            SnapshotAtRv::List(l) => l,
            other => panic!("expected List, got {other:?}"),
        };
        assert_eq!(sorted_names(&list), vec!["a".to_string(), "b".to_string()]);
        assert_eq!(list.continue_token.as_deref(), Some("b"));
        assert_eq!(list.remaining_item_count, Some(2));

        // Page 2: continue after "b".
        let snap = db
            .snapshot_resources_at_rv(
                "v1",
                "ConfigMap",
                Some("default"),
                ResourceListQuery::new(None, None, Some(2), Some("b")),
                rv,
            )
            .await
            .unwrap();
        let list = match snap {
            SnapshotAtRv::List(l) => l,
            other => panic!("expected List, got {other:?}"),
        };
        assert_eq!(
            sorted_names(&list),
            vec!["c".to_string(), "d".to_string()],
            "page 2 must contain c,d from the historical set (not the later e)"
        );
        assert_eq!(list.continue_token, None);
    }

    async fn put_ns(db: &Datastore, name: &str, label: &str) -> i64 {
        let r = db
            .create_namespace(
                name,
                json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": name, "labels": {"k": label}}
                }),
            )
            .await
            .unwrap();
        r.resource_version
    }

    /// Namespaces persist in their own table (no created_rv column), so their
    /// snapshot reconstruction must read that table for live rows and derive
    /// existence-at-N from watch_events history. This mirrors
    /// `snapshot_reconstructs_state_at_past_rv` for the Namespace kind.
    #[tokio::test]
    async fn snapshot_reconstructs_namespace_state_at_past_rv() {
        let db = Datastore::new_in_memory().await.unwrap();
        put_ns(&db, "a", "old").await;
        let rb = put_ns(&db, "b", "bee").await; // snapshot point: {a:old, b}

        // Mutations after the snapshot point must not leak into the snapshot.
        let cur_a = db.get_namespace("a").await.unwrap().unwrap();
        db.update_namespace(
            "a",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "a", "labels": {"k": "new"}}
            }),
            cur_a.resource_version,
        )
        .await
        .unwrap();
        db.delete_namespace("b").await.unwrap();
        put_ns(&db, "c", "see").await;

        let snap = db
            .snapshot_resources_at_rv("v1", "Namespace", None, ResourceListQuery::all(), rb)
            .await
            .unwrap();
        let list = match snap {
            SnapshotAtRv::List(l) => l,
            other => panic!("expected List, got {other:?}"),
        };
        assert_eq!(
            sorted_names(&list),
            vec!["a".to_string(), "b".to_string()],
            "namespace snapshot at rb must contain a and b, not the later c"
        );
        assert_eq!(list.resource_version, rb);
        let a = list.items.iter().find(|r| r.name == "a").unwrap();
        assert_eq!(
            a.data
                .pointer("/metadata/labels/k")
                .and_then(|v| v.as_str()),
            Some("old"),
            "namespace a must show its pre-update value at the snapshot rv"
        );
    }

    /// A namespace created entirely after the snapshot rv must be absent (not
    /// erroneously treated as expired) even though the namespaces table has no
    /// created_rv column — the earliest-retained ADDED event proves it was born
    /// after N.
    #[tokio::test]
    async fn snapshot_namespace_created_after_rv_is_absent() {
        let db = Datastore::new_in_memory().await.unwrap();
        let rb = put_ns(&db, "a", "old").await; // snapshot point: {a}
        put_ns(&db, "z", "new").await; // created after the snapshot

        let snap = db
            .snapshot_resources_at_rv("v1", "Namespace", None, ResourceListQuery::all(), rb)
            .await
            .unwrap();
        let list = match snap {
            SnapshotAtRv::List(l) => l,
            other => panic!("expected List, got {other:?}"),
        };
        assert_eq!(
            sorted_names(&list),
            vec!["a".to_string()],
            "namespace z created after N must be absent from the snapshot, not expired"
        );
    }
}
