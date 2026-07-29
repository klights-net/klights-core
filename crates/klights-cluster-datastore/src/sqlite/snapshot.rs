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

use klights_cluster_core::Resource;
use klights_cluster_store::{
    ResourceCollectionKey, ResourceListPage, ResourceListQuery, ResourceListSnapshot,
};
use klights_types::LabelSelector;

use super::read_queries as queries;
use super::read_store::SqliteReadStore;
use super::scope::use_namespaced_table;

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

pub enum ExactSnapshotRead {
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
    pub async fn snapshot_resources_at_rv(
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
