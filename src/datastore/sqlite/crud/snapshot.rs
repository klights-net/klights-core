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
use super::super::read_store::SqliteReadStore;
use super::super::scope::use_namespaced_table;
use klights_cluster_core::Resource;
use klights_cluster_store::{
    ResourceCollectionKey, ResourceListPage, ResourceListQuery, ResourceListSnapshot,
};
use klights_types::LabelSelector;

/// Per-key history facts derived from `watch_events`, relative to the requested
/// snapshot rv `N`.
#[derive(Default)]
struct NameHistory {
    /// `(rv, event_type)` of the latest event with `rv <= N`, if any.
    latest_le_n: Option<(i64, String)>,
    /// Event type of the earliest event with `rv > N`, if any.
    earliest_gt_n_type: Option<String>,
}

#[derive(Clone, Debug)]
struct SnapshotKey {
    name: String,
}

impl SnapshotKey {
    fn new(_namespace: Option<String>, name: String) -> Self {
        Self { name }
    }
}

impl PartialEq for SnapshotKey {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for SnapshotKey {}

impl std::hash::Hash for SnapshotKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.name, state);
    }
}

/// Closure result: the reconstruction either defers to the live list, reports
/// the rv as unreconstructable, or yields the raw (unfiltered, unpaginated)
/// snapshot items sorted by name.
enum RawSnapshot {
    Current,
    Expired,
    Items(Vec<Resource>, klights_cluster_core::WatchReplayPosition),
}

pub(in crate::datastore::sqlite) enum ExactSnapshotRead {
    Current,
    Expired,
    List(ResourceListPage),
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

impl SqliteReadStore {
    pub(in crate::datastore::sqlite) async fn snapshot_resources_at_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery,
        snapshot_rv: i64,
    ) -> Result<ExactSnapshotRead> {
        let av = api_version.to_string();
        let k = kind.to_string();
        let ns_owned = namespace.map(str::to_string);
        let namespaced = use_namespaced_table(api_version, kind, &namespace);
        // Namespaces are cluster-scoped but persist in their own `namespaces`
        // table rather than the generic cluster_resources table.
        let is_namespace = api_version == "v1" && kind == "Namespace";
        let n = snapshot_rv;

        let raw = self
            .read_db_call("snapshot_resources_at_rv", move |conn| {
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
                let mut current: HashMap<SnapshotKey, (i64, Option<i64>, Resource)> =
                    HashMap::new();
                if is_namespace {
                    let mut stmt =
                        tx.prepare("SELECT name, resource_version, uid, data FROM namespaces")?;
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
                        current.insert(SnapshotKey::new(None, name), (rv, None, res));
                    }
                } else if namespaced {
                    let mut sql =
                        "SELECT name, namespace, resource_version, created_rv, uid, data \
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
                        current.insert(
                            SnapshotKey::new(res.namespace.clone(), name),
                            (rv, Some(created_rv), res),
                        );
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
                        current.insert(SnapshotKey::new(None, name), (rv, Some(created_rv), res));
                    }
                }

                // 2. Per-key history facts from watch_events (structure only —
                //    object bytes are fetched lazily for the keys we need).
                let mut histories: HashMap<SnapshotKey, NameHistory> = HashMap::new();
                {
                    let mut sql =
                        "SELECT namespace, name, resource_version, event_type FROM watch_events \
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
                        let namespace: Option<String> = row.get(0)?;
                        let name: String = row.get(1)?;
                        let rv: i64 = row.get(2)?;
                        let event_type: String = row.get(3)?;
                        Ok((SnapshotKey::new(namespace, name), rv, event_type))
                    })?;
                    for row in rows {
                        let (key, rv, event_type) = row?;
                        let history = histories.entry(key).or_default();
                        if rv <= n {
                            // Ascending order: the last <= N seen wins.
                            history.latest_le_n = Some((rv, event_type));
                        } else if history.earliest_gt_n_type.is_none() {
                            history.earliest_gt_n_type = Some(event_type);
                        }
                    }
                }

                // 3. Decide each key's state at N.
                let mut result: BTreeMap<String, Resource> = BTreeMap::new();
                let mut to_apply: Vec<(SnapshotKey, i64)> = Vec::new();
                let mut expired = false;

                for (key, (rv, created_rv, res)) in &current {
                    if *rv <= n {
                        // Unchanged since rv <= N: live row is the state at N.
                        result.insert(key.name.clone(), res.clone());
                        continue;
                    }
                    // Changed after N — rebuild from the latest event <= N.
                    match histories
                        .get(key)
                        .and_then(|history| history.latest_le_n.as_ref())
                    {
                        Some((le_rv, etype)) if etype != "DELETED" => {
                            to_apply.push((key.clone(), *le_rv));
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
                                        .get(key)
                                        .and_then(|history| history.earliest_gt_n_type.as_deref())
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

                for (key, history) in &histories {
                    if current.contains_key(key) {
                        continue;
                    }
                    // Key absent from live rows → deleted after N (or never lived
                    // past the window).
                    match history.latest_le_n.as_ref() {
                        Some((le_rv, etype)) if etype != "DELETED" => {
                            to_apply.push((key.clone(), *le_rv));
                        }
                        Some(_) => { /* deleted at/before N: absent */ }
                        None => {
                            if history.earliest_gt_n_type.as_deref() != Some("ADDED") {
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

                // 4. Fetch object bytes for the rebuilt keys. A raft/etcd-style
                //    transaction may produce several object events at one RV, so
                //    historical bytes are keyed by (namespace, name, rv), not rv alone.
                if !to_apply.is_empty() {
                    let mut sql =
                        "SELECT namespace, name, resource_version, data FROM watch_events \
                         WHERE api_version = ?1 AND kind = ?2"
                            .to_string();
                    let mut params: Vec<Box<dyn ToSql>> =
                        vec![Box::new(av.clone()), Box::new(k.clone())];
                    if namespaced {
                        if let Some(ns) = &ns_owned {
                            sql.push_str(&format!(" AND namespace = ?{}", params.len() + 1));
                            params.push(Box::new(ns.clone()));
                        } else {
                            sql.push_str(" AND namespace IS NOT NULL");
                        }
                    } else {
                        sql.push_str(" AND namespace IS NULL");
                    }
                    sql.push_str(" AND (");
                    for (idx, (key, rv)) in to_apply.iter().enumerate() {
                        if idx > 0 {
                            sql.push_str(" OR ");
                        }
                        sql.push_str(&format!(
                            "(name = ?{} AND resource_version = ?{})",
                            params.len() + 1,
                            params.len() + 2
                        ));
                        params.push(Box::new(key.name.clone()));
                        params.push(Box::new(*rv));
                    }
                    sql.push(')');
                    let pref: Vec<&dyn ToSql> = params.iter().map(|p| p.as_ref()).collect();
                    let mut stmt = tx.prepare(&sql)?;
                    let rows = stmt.query_map(&pref[..], |row| {
                        let namespace: Option<String> = row.get(0)?;
                        let name: String = row.get(1)?;
                        let rv: i64 = row.get(2)?;
                        let data = Arc::new(json_from_bytes(row.get(3)?)?);
                        Ok((SnapshotKey::new(namespace, name), rv, data))
                    })?;
                    let mut data_by_key_rv: HashMap<(SnapshotKey, i64), Arc<Value>> =
                        HashMap::new();
                    for row in rows {
                        let (key, rv, data) = row?;
                        data_by_key_rv.insert((key, rv), data);
                    }
                    for (key, le_rv) in &to_apply {
                        if let Some(data) = data_by_key_rv.get(&(key.clone(), *le_rv)) {
                            result.insert(
                                key.name.clone(),
                                resource_from_event(
                                    &av,
                                    &k,
                                    &key.name,
                                    *le_rv,
                                    data.clone(),
                                    namespaced,
                                ),
                            );
                        }
                    }
                }

                let event_id = tx.query_row(
                    "SELECT COALESCE(MAX(id), 0) FROM watch_events WHERE resource_version <= ?1",
                    rusqlite::params![n],
                    |row| row.get(0),
                )?;
                let items = result.into_values().collect::<Vec<_>>();
                Ok(RawSnapshot::Items(
                    items,
                    klights_cluster_core::WatchReplayPosition {
                        resource_version: n,
                        event_id,
                        resource_version_filter_through_event_id: 0,
                    },
                ))
            })
            .await?;

        let (items, position) = match raw {
            RawSnapshot::Current => return Ok(ExactSnapshotRead::Current),
            RawSnapshot::Expired => return Ok(ExactSnapshotRead::Expired),
            RawSnapshot::Items(items, position) => (items, position),
        };

        Ok(ExactSnapshotRead::List(paginate_snapshot(
            items, &query, position,
        )?))
    }
}

/// Apply label/field selectors and keyset pagination to the reconstructed
/// (name-sorted) snapshot, reusing the same matchers as the live list/watch
/// paths so Exact and live LISTs agree.
fn paginate_snapshot(
    items: Vec<Resource>,
    query: &ResourceListQuery,
    position: klights_cluster_core::WatchReplayPosition,
) -> Result<ResourceListPage> {
    let parsed_label = match query
        .label_selector()
        .filter(|selector| !selector.trim().is_empty())
    {
        Some(s) => Some(LabelSelector::parse(s)?),
        None => None,
    };
    let parsed_field = query
        .field_selector()
        .filter(|selector| !selector.trim().is_empty())
        .map(klights_types::FieldSelector::parse)
        .transpose()?;

    let mut filtered: Vec<Resource> = items
        .into_iter()
        .filter(|r| {
            parsed_label
                .as_ref()
                .is_none_or(|sel| sel.matches_resource(&r.data))
                && parsed_field.as_ref().is_none_or(|selector| {
                    selector.matches_resource_with_identity(&r.api_version, &r.kind, &r.data)
                })
        })
        .collect();
    filtered.sort_by(|left, right| left.name.cmp(&right.name));

    if let Some(continuation) = query.continuation() {
        filtered.retain(|resource| resource.name.as_str() > continuation.after().name());
    }

    let total = filtered.len() as i64;
    let (page, has_more, remaining_item_count) = match query.limit() {
        Some(limit) if total > limit => {
            let page: Vec<Resource> = filtered
                .into_iter()
                .take(usize::try_from(limit).unwrap_or(usize::MAX))
                .collect();
            (page, true, Some(total - limit))
        }
        _ => (filtered, false, None),
    };
    let snapshot = ResourceListSnapshot::try_new(position)
        .map_err(|error| anyhow::anyhow!("invalid exact snapshot position: {error}"))?;
    let continuation = has_more
        .then(|| {
            page.last().map(|resource| {
                klights_cluster_store::ResourceContinuation::new(
                    ResourceCollectionKey::new(resource.namespace.clone(), resource.name.clone()),
                    snapshot,
                )
            })
        })
        .flatten();
    ResourceListPage::try_new(page, snapshot, continuation, remaining_item_count)
        .map_err(|error| anyhow::anyhow!("invalid exact snapshot page: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::sqlite::Datastore;
    use crate::datastore::{ResourceList, ResourceListQuery, SnapshotAtRv};
    use klights_cluster_store::{
        ResourceCollectionScope, ResourceListRead, ResourceListRequest, ResourceVersionMatch,
    };
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

    async fn put_in_namespace(db: &Datastore, namespace: &str, name: &str) -> i64 {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some(namespace),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": name, "namespace": namespace}
            }),
        )
        .await
        .unwrap()
        .resource_version
    }

    fn lower_identities(read: &ResourceListRead) -> Vec<(Option<String>, String)> {
        read.items()
            .iter()
            .map(|resource| (resource.namespace.clone(), resource.name.clone()))
            .collect()
    }

    #[tokio::test]
    async fn all_namespace_current_and_exact_preserve_legacy_name_page_oracle() {
        let db = Datastore::new_in_memory().await.unwrap();
        put_in_namespace(&db, "ns-a", "a").await;
        put_in_namespace(&db, "ns-a", "same").await;
        put_in_namespace(&db, "ns-b", "same").await;
        let exact_rv = put_in_namespace(&db, "ns-z", "z").await;

        // Force Exact down the historical reconstruction path without changing
        // the target collection.
        db.create_resource(
            "v1",
            "Secret",
            Some("ns-a"),
            "later",
            json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": "later", "namespace": "ns-a"}
            }),
        )
        .await
        .unwrap();

        // Independent copies of the pre-extraction SQLite semantics:
        // current all-namespace LIST scans ORDER BY name and retains ties;
        // Exact scans without ordering and materializes a name-keyed BTreeMap,
        // so equal names collapse to the last row observed.
        let (current_oracle, exact_oracle) = db
            .read_db_call("legacy-list-oracle", |connection| {
                let mut current_stmt = connection.prepare(
                    "SELECT namespace, name FROM namespaced_resources \
                     WHERE api_version = 'v1' AND kind = 'ConfigMap' ORDER BY name",
                )?;
                let current_oracle = current_stmt
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect::<rusqlite::Result<Vec<(Option<String>, String)>>>()?;

                let mut exact_stmt = connection.prepare(
                    "SELECT namespace, name FROM namespaced_resources \
                     WHERE api_version = 'v1' AND kind = 'ConfigMap'",
                )?;
                let mut exact_by_name = BTreeMap::new();
                for row in exact_stmt.query_map([], |row| {
                    Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?))
                })? {
                    let identity = row?;
                    exact_by_name.insert(identity.1.clone(), identity);
                }
                Ok((
                    current_oracle,
                    exact_by_name.into_values().collect::<Vec<_>>(),
                ))
            })
            .await
            .unwrap();
        assert_eq!(current_oracle.len(), 4);
        assert_eq!(
            exact_oracle.len(),
            3,
            "the legacy Exact name map collapses equal names across namespaces"
        );

        let store = db.focused_read_store();
        for (mode, oracle) in [
            (ResourceVersionMatch::Any, current_oracle),
            (ResourceVersionMatch::Exact(exact_rv), exact_oracle),
        ] {
            let first = klights_cluster_store::ClusterResourceRead::list_resources(
                store.as_ref(),
                ResourceListRequest::new(
                    "v1",
                    "ConfigMap",
                    ResourceCollectionScope::AllNamespaces,
                    klights_cluster_store::ResourceListQuery::try_new(
                        None,
                        None,
                        Some(2),
                        None,
                        mode,
                    )
                    .unwrap(),
                ),
            )
            .await
            .unwrap();
            assert_eq!(lower_identities(&first), oracle[..2]);
            let continuation = first
                .continuation()
                .cloned()
                .expect("legacy first page has a name continuation");
            assert_eq!(continuation.after().name(), oracle[1].1);

            let after = continuation.after().name().to_string();
            let second_expected = oracle
                .iter()
                .filter(|(_, name)| name > &after)
                .take(2)
                .cloned()
                .collect::<Vec<_>>();
            let second = klights_cluster_store::ClusterResourceRead::list_resources(
                store.as_ref(),
                ResourceListRequest::new(
                    "v1",
                    "ConfigMap",
                    ResourceCollectionScope::AllNamespaces,
                    klights_cluster_store::ResourceListQuery::try_new(
                        None,
                        None,
                        Some(2),
                        Some(continuation),
                        mode,
                    )
                    .unwrap(),
                ),
            )
            .await
            .unwrap();
            assert_eq!(lower_identities(&second), second_expected);
        }
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
