//! Historical-snapshot reconstruction for LIST `resourceVersionMatch=Exact`
//! and consistent paginated continuations.
//!
//! Reconstructs the set of resources of a `(api_version, kind, namespace)` as
//! they existed at a past `resourceVersion` by combining the live rows
//! (unchanged since that rv) with the durable `watch_events` history (the
//! latest event at-or-before the requested rv for every key that changed
//! afterwards). When the requested rv predates the retained window we cannot
//! rebuild a faithful snapshot, so the caller is told to answer `410 Gone`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use std::sync::OnceLock;

use anyhow::Result;
use rusqlite::{OptionalExtension, ToSql};
use serde_json::Value;

use klights_cluster_core::{Resource, WatchReplayPosition};
use klights_cluster_store::{
    DurableWatchTarget, ResourceCollectionKey, ResourceCollectionScope, ResourceListPage,
    ResourceListQuery, ResourceListRead, ResourceListRequest, ResourceListSnapshot,
    ResourceReadError,
};
use klights_types::LabelSelector;

use super::read_queries as queries;
use super::read_store::SqliteReadStore;
use super::scope::use_namespaced_table;

/// A historical page deliberately reads a small number of identities per DB
/// turn.  The outer async state machine may make more turns to satisfy a
/// selector, but a serialized SQLite closure never grows with collection or
/// history size.
pub(crate) const HISTORICAL_CANDIDATE_CAP: usize = 64;
pub(crate) const HISTORICAL_IDENTITY_HISTORY_CAP: usize = 64;

const PHYSICAL_BOUND_PREFIX: &str = "physical-bound-";

static PHYSICAL_RESOURCE_DECODES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static PHYSICAL_EVENT_DECODES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static PHYSICAL_CANDIDATE_BATCH_MAX: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static PHYSICAL_HISTORY_BATCH_MAX: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhysicalBoundCounters {
    pub resource_decodes: u64,
    pub event_decodes: u64,
    pub candidate_batch_max: u64,
    pub history_batch_max: u64,
}

pub fn reset_physical_bound_counters_for_test() {
    use std::sync::atomic::Ordering;
    PHYSICAL_RESOURCE_DECODES.store(0, Ordering::Release);
    PHYSICAL_EVENT_DECODES.store(0, Ordering::Release);
    PHYSICAL_CANDIDATE_BATCH_MAX.store(0, Ordering::Release);
    PHYSICAL_HISTORY_BATCH_MAX.store(0, Ordering::Release);
}

pub fn physical_bound_counters_for_test() -> PhysicalBoundCounters {
    use std::sync::atomic::Ordering;
    PhysicalBoundCounters {
        resource_decodes: PHYSICAL_RESOURCE_DECODES.load(Ordering::Acquire),
        event_decodes: PHYSICAL_EVENT_DECODES.load(Ordering::Acquire),
        candidate_batch_max: PHYSICAL_CANDIDATE_BATCH_MAX.load(Ordering::Acquire),
        history_batch_max: PHYSICAL_HISTORY_BATCH_MAX.load(Ordering::Acquire),
    }
}

pub(crate) fn record_physical_resource_decode(name: &str) {
    if name.starts_with(PHYSICAL_BOUND_PREFIX) {
        PHYSICAL_RESOURCE_DECODES.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
}

fn record_physical_history_batch(name: &str, size: usize) {
    if name.starts_with(PHYSICAL_BOUND_PREFIX) {
        PHYSICAL_EVENT_DECODES.fetch_add(size as u64, std::sync::atomic::Ordering::AcqRel);
        PHYSICAL_HISTORY_BATCH_MAX.fetch_max(size as u64, std::sync::atomic::Ordering::AcqRel);
    }
}

fn record_physical_candidate_batch(candidates: &[ResourceCollectionKey]) {
    if candidates
        .iter()
        .all(|candidate| candidate.name().starts_with(PHYSICAL_BOUND_PREFIX))
    {
        PHYSICAL_CANDIDATE_BATCH_MAX
            .fetch_max(candidates.len() as u64, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Test-only deterministic seam between bounded candidate DB turns. It is
/// intentionally absent from production behavior and lets certification
/// compact history after one window has committed but before the next starts.
struct HistoricalWindowTestControl {
    pause_position: std::sync::Mutex<Option<WatchReplayPosition>>,
    reached: tokio::sync::Semaphore,
    resume: tokio::sync::Semaphore,
    candidate_windows: std::sync::atomic::AtomicU64,
    history_windows: std::sync::atomic::AtomicU64,
}

fn historical_window_test_control() -> &'static HistoricalWindowTestControl {
    static CONTROL: OnceLock<HistoricalWindowTestControl> = OnceLock::new();
    CONTROL.get_or_init(|| HistoricalWindowTestControl {
        pause_position: std::sync::Mutex::new(None),
        reached: tokio::sync::Semaphore::new(0),
        resume: tokio::sync::Semaphore::new(0),
        candidate_windows: std::sync::atomic::AtomicU64::new(0),
        history_windows: std::sync::atomic::AtomicU64::new(0),
    })
}

pub fn arm_historical_window_pause_for_test(position: WatchReplayPosition) {
    use std::sync::atomic::Ordering;
    let control = historical_window_test_control();
    *control
        .pause_position
        .lock()
        .expect("historical window pause mutex poisoned") = Some(position);
    control.candidate_windows.store(0, Ordering::Release);
    control.history_windows.store(0, Ordering::Release);
}

pub async fn wait_for_historical_window_pause_for_test() {
    historical_window_test_control()
        .reached
        .acquire()
        .await
        .unwrap()
        .forget();
}

pub fn resume_historical_window_pause_for_test() {
    historical_window_test_control().resume.add_permits(1);
}

pub fn historical_window_counts_for_test() -> (u64, u64) {
    use std::sync::atomic::Ordering;
    let control = historical_window_test_control();
    (
        control.candidate_windows.load(Ordering::Acquire),
        control.history_windows.load(Ordering::Acquire),
    )
}

/// Per-key history facts derived from `watch_events`, relative to the requested
/// snapshot rv `N`.
#[derive(Default)]
struct NameHistory {
    /// `(rv, event_type)` of the latest event with `rv <= N`, if any.
    latest_le_n: Option<(i64, String)>,
    /// Event type of the earliest event with `rv > N`, if any.
    earliest_gt_n_type: Option<String>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct SnapshotKey {
    namespace: Option<String>,
    name: String,
}

impl SnapshotKey {
    fn new(namespace: Option<String>, name: String) -> Self {
        Self { namespace, name }
    }
}

/// Closure result: the reconstruction either defers to the live list, reports
/// the rv as unreconstructable, or yields the raw (unfiltered, unpaginated)
/// snapshot items sorted by name.
enum RawSnapshot {
    Current,
    Expired { oldest_available: i64 },
    Items(Vec<Resource>, klights_cluster_core::WatchReplayPosition),
}

pub enum ExactSnapshotRead {
    Current,
    Expired { oldest_available: i64 },
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
        let all_namespaces = namespaced && ns_owned.is_none();
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
                let earliest_available = match earliest {
                    Some(earliest) if n + 1 >= earliest => earliest,
                    Some(oldest_available) => {
                        return Ok(RawSnapshot::Expired { oldest_available });
                    }
                    None => {
                        return Ok(RawSnapshot::Expired {
                            oldest_available: current_rv,
                        });
                    }
                };

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
                let mut result: BTreeMap<SnapshotKey, Resource> = BTreeMap::new();
                let mut to_apply: Vec<(SnapshotKey, i64)> = Vec::new();
                let mut expired = false;

                for (key, (rv, created_rv, res)) in &current {
                    if *rv <= n {
                        // Unchanged since rv <= N: live row is the state at N.
                        result.insert(key.clone(), res.clone());
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
                    return Ok(RawSnapshot::Expired {
                        oldest_available: earliest_available,
                    });
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
                                key.clone(),
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
            RawSnapshot::Expired { oldest_available } => {
                return Ok(ExactSnapshotRead::Expired { oldest_available });
            }
            RawSnapshot::Items(items, position) => (items, position),
        };

        Ok(ExactSnapshotRead::List(paginate_snapshot(
            items,
            &query,
            position,
            all_namespaces,
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
    all_namespaces: bool,
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
    filtered.sort_by(|left, right| {
        if all_namespaces {
            (
                left.namespace.as_deref().unwrap_or_default(),
                left.name.as_str(),
            )
                .cmp(&(
                    right.namespace.as_deref().unwrap_or_default(),
                    right.name.as_str(),
                ))
        } else {
            left.name.cmp(&right.name)
        }
    });

    if let Some(continuation) = query.continuation() {
        filtered.retain(|resource| {
            if all_namespaces && continuation.after().namespace().is_some() {
                (
                    resource.namespace.as_deref().unwrap_or_default(),
                    resource.name.as_str(),
                ) > (
                    continuation.after().namespace().unwrap_or_default(),
                    continuation.after().name(),
                )
            } else {
                resource.name.as_str() > continuation.after().name()
            }
        });
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

/// Bounded typed historical LIST.  This is intentionally separate from the
/// broad snapshot port: LIST has a public keyset cursor, so it can walk the
/// identity index a window at a time and reconstruct only identities that may
/// be emitted.  Watch/bootstrap callers still use the collection snapshot
/// port, where returning the complete membership is the contract.
pub(crate) async fn bounded_historical_list(
    store: &SqliteReadStore,
    request: ResourceListRequest,
    position: WatchReplayPosition,
) -> std::result::Result<ResourceListRead, ResourceReadError> {
    let labels = request
        .query()
        .label_selector()
        .filter(|value| !value.trim().is_empty())
        .map(LabelSelector::parse)
        .transpose()
        .map_err(|error| ResourceReadError::InvalidSelector {
            message: error.to_string(),
        })?;
    let fields = request
        .query()
        .field_selector()
        .filter(|value| !value.trim().is_empty())
        .map(klights_types::FieldSelector::parse)
        .transpose()
        .map_err(|error| ResourceReadError::InvalidSelector {
            message: error.to_string(),
        })?;
    let target = match request.scope() {
        ResourceCollectionScope::Cluster => {
            DurableWatchTarget::cluster(request.api_version(), request.kind())
        }
        ResourceCollectionScope::AllNamespaces => {
            DurableWatchTarget::namespaced(request.api_version(), request.kind())
        }
        ResourceCollectionScope::Namespace(namespace) => {
            DurableWatchTarget::namespaced_in_namespace(
                request.api_version(),
                request.kind(),
                namespace,
            )
        }
    };
    let all_namespaces = matches!(request.scope(), ResourceCollectionScope::AllNamespaces);
    let limit = request
        .query()
        .limit()
        .and_then(|value| usize::try_from(value).ok());
    let count_exact_remaining = limit.is_some() && labels.is_none() && fields.is_none();
    let probe = limit.map_or(usize::MAX, |value| value.saturating_add(1));
    let mut emitted = Vec::with_capacity(probe.min(HISTORICAL_CANDIDATE_CAP));
    let mut matched_count = 0_i64;
    let mut candidate_after = request.query().start_after().cloned();
    let mut more_candidates: bool;

    loop {
        let candidates = match historical_candidate_window(
            store,
            &request,
            &target,
            position,
            candidate_after.clone(),
        )
        .await?
        {
            HistoricalCandidateWindow::Items(candidates) => candidates,
            HistoricalCandidateWindow::Expired { oldest_available } => {
                return Ok(ResourceListRead::Expired {
                    requested: position.resource_version,
                    oldest_available,
                    replacement: request.query().continuation().map(|cursor| {
                        klights_cluster_store::ResourceListRecoveryContinuation::new(
                            cursor.after().clone(),
                        )
                    }),
                });
            }
        };
        record_physical_candidate_batch(&candidates);
        {
            use std::sync::atomic::Ordering;
            let control = historical_window_test_control();
            control.candidate_windows.fetch_add(1, Ordering::AcqRel);
            let should_pause = {
                let mut pause_position = control
                    .pause_position
                    .lock()
                    .expect("historical window pause mutex poisoned");
                if *pause_position == Some(position) {
                    *pause_position = None;
                    true
                } else {
                    false
                }
            };
            if should_pause {
                control.reached.add_permits(1);
                control.resume.acquire().await.unwrap().forget();
            }
        }
        if candidates.is_empty() {
            more_candidates = false;
            break;
        }
        more_candidates = candidates.len() == HISTORICAL_CANDIDATE_CAP;
        for candidate in candidates {
            candidate_after = Some(candidate.clone());
            match historical_identity_at_position(store, &request, &target, position, &candidate)
                .await?
            {
                HistoricalIdentity::Absent => continue,
                HistoricalIdentity::Resource(resource) => {
                    if labels
                        .as_ref()
                        .is_some_and(|selector| !selector.matches_resource(&resource.data))
                        || fields.as_ref().is_some_and(|selector| {
                            !selector.matches_resource_with_identity(
                                &resource.api_version,
                                &resource.kind,
                                &resource.data,
                            )
                        })
                    {
                        continue;
                    }
                    matched_count = matched_count.saturating_add(1);
                    if !count_exact_remaining || emitted.len() < limit.unwrap_or(usize::MAX) {
                        emitted.push(resource);
                    }
                    if !count_exact_remaining && emitted.len() == probe {
                        break;
                    }
                    continue;
                }
                HistoricalIdentity::Expired { oldest_available } => {
                    return Ok(ResourceListRead::Expired {
                        requested: position.resource_version,
                        oldest_available,
                        replacement: request.query().continuation().map(|cursor| {
                            klights_cluster_store::ResourceListRecoveryContinuation::new(
                                cursor.after().clone(),
                            )
                        }),
                    });
                }
            }
        }
        if (!count_exact_remaining && emitted.len() == probe) || !more_candidates {
            break;
        }
    }

    let has_more = if count_exact_remaining {
        limit.is_some_and(|value| matched_count > i64::try_from(value).unwrap_or(i64::MAX))
    } else {
        limit.is_some_and(|value| emitted.len() > value) || (more_candidates && limit.is_none())
    };
    if let Some(limit) = limit {
        emitted.truncate(limit);
    }
    let snapshot = ResourceListSnapshot::try_new(position)?;
    let continuation = has_more
        .then(|| {
            emitted.last().map(|resource| {
                klights_cluster_store::ResourceContinuation::new(
                    ResourceCollectionKey::new(resource.namespace.clone(), resource.name.clone()),
                    snapshot,
                )
            })
        })
        .flatten();
    // A selector may underfill an internal candidate window.  Its next public
    // token is still the last *emitted* identity; callers never see the
    // request-local scan cursor.
    let remaining_item_count = (count_exact_remaining && has_more)
        .then(|| {
            matched_count
                .checked_sub(i64::try_from(emitted.len()).unwrap_or(i64::MAX))
                .ok_or_else(|| ResourceReadError::CorruptData {
                    message: "historical LIST remaining item count underflowed".to_string(),
                })
        })
        .transpose()?;
    let _ = all_namespaces;
    Ok(ResourceListRead::Historical(ResourceListPage::try_new(
        emitted,
        snapshot,
        continuation,
        remaining_item_count,
    )?))
}

enum HistoricalCandidateWindow {
    Items(Vec<ResourceCollectionKey>),
    Expired { oldest_available: i64 },
}

enum HistoricalIdentity {
    Resource(Resource),
    Absent,
    Expired { oldest_available: i64 },
}

async fn historical_candidate_window(
    store: &SqliteReadStore,
    request: &ResourceListRequest,
    target: &DurableWatchTarget,
    position: WatchReplayPosition,
    after: Option<ResourceCollectionKey>,
) -> std::result::Result<HistoricalCandidateWindow, ResourceReadError> {
    let av = request.api_version().to_string();
    let kind = request.kind().to_string();
    let scope = request.scope().clone();
    let current_target = target.clone();
    let current_av = av.clone();
    let current_kind = kind.clone();
    let current_scope = scope.clone();
    let current_after = after.clone();
    let current = store
        .read_db_call(
            "cluster-read:historical-current-candidate-window",
            move |connection| {
                if let Some(oldest_available) =
                    historical_floor(connection, &current_target, position)?
                {
                    return Ok(HistoricalCandidateWindow::Expired { oldest_available });
                }
                let namespaces = current_av == "v1"
                    && current_kind == "Namespace"
                    && matches!(current_scope, ResourceCollectionScope::Cluster);
                let mut values: Vec<Box<dyn ToSql>> = Vec::new();
                let mut sql = if namespaces {
                    "SELECT NULL AS namespace, name FROM namespaces WHERE 1 = 1".to_string()
                } else {
                    values.push(Box::new(current_av));
                    values.push(Box::new(current_kind));
                    match &current_scope {
                        ResourceCollectionScope::Cluster => "SELECT NULL AS namespace, name FROM cluster_resources WHERE api_version = ?1 AND kind = ?2".to_string(),
                        ResourceCollectionScope::AllNamespaces => "SELECT namespace, name FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2 AND json_type(data, '$.metadata.namespace') = 'text'".to_string(),
                        ResourceCollectionScope::Namespace(namespace) => {
                            values.push(Box::new(namespace.clone()));
                            "SELECT namespace, name FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND json_extract(data, '$.metadata.namespace') = ?3".to_string()
                        }
                    }
                };
                append_lexical_after(&mut sql, &mut values, &current_scope, current_after.as_ref());
                sql.push_str(" ORDER BY COALESCE(namespace, ''), name");
                sql.push_str(&format!(" LIMIT ?{}", values.len() + 1));
                values.push(Box::new(HISTORICAL_CANDIDATE_CAP as i64));
                let refs = values
                    .iter()
                    .map(|value| value.as_ref())
                    .collect::<Vec<_>>();
                let mut statement = connection.prepare(&sql)?;
                let rows = statement.query_map(refs.as_slice(), |row| {
                    Ok(ResourceCollectionKey::new(
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, String>(1)?,
                    ))
                })?;
                Ok(HistoricalCandidateWindow::Items(
                    rows.collect::<rusqlite::Result<_>>()?,
                ))
            },
        )
        .await
        .map_err(|error| ResourceReadError::retryable(error.to_string()))?;
    let HistoricalCandidateWindow::Items(current) = current else {
        return Ok(current);
    };

    let mut touched = BTreeSet::new();
    let mut raw_cursor: Option<(ResourceCollectionKey, i64)> = None;
    let public_after = after.clone();
    loop {
        let history_target = target.clone();
        let history_av = av.clone();
        let history_kind = kind.clone();
        let history_scope = scope.clone();
        let history_after = public_after.clone();
        let cursor = raw_cursor.clone();
        let batch = store
            .read_db_call("cluster-read:historical-touched-candidate-window", move |connection| {
                if let Some(oldest_available) = historical_floor(connection, &history_target, position)? {
                    return Ok(HistoricalRawCandidateWindow::Expired { oldest_available });
                }
                let mut values: Vec<Box<dyn ToSql>> = vec![Box::new(history_av), Box::new(history_kind)];
                let mut sql = "SELECT namespace, name, id, resource_version FROM watch_events INDEXED BY idx_watch_events_identity_id_desc WHERE api_version = ?1 AND kind = ?2".to_string();
                append_history_scope(&mut sql, &mut values, &history_scope);
                if let Some((key, id)) = cursor.as_ref() {
                    append_raw_history_after(&mut sql, &mut values, &history_scope, key, *id);
                } else {
                    append_lexical_after(&mut sql, &mut values, &history_scope, history_after.as_ref());
                }
                // Keep this expression byte-for-byte aligned with
                // idx_watch_events_identity_id_desc so each bounded window is
                // an index walk rather than a collection-sized temp sort.
                sql.push_str(" ORDER BY COALESCE(namespace, '#cluster'), name, id DESC");
                sql.push_str(&format!(" LIMIT ?{}", values.len() + 1));
                values.push(Box::new(HISTORICAL_CANDIDATE_CAP as i64));
                let refs = values.iter().map(|value| value.as_ref()).collect::<Vec<_>>();
                let mut statement = connection.prepare(&sql)?;
                let rows = statement.query_map(refs.as_slice(), |row| {
                    Ok(HistoricalRawCandidate {
                        key: ResourceCollectionKey::new(row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?),
                        event_id: row.get(2)?,
                        resource_version: row.get(3)?,
                    })
                })?;
                Ok(HistoricalRawCandidateWindow::Items(rows.collect::<rusqlite::Result<_>>()?))
            })
            .await
            .map_err(|error| ResourceReadError::retryable(error.to_string()))?;
        let HistoricalRawCandidateWindow::Items(rows) = batch else {
            let HistoricalRawCandidateWindow::Expired { oldest_available } = batch else {
                unreachable!()
            };
            return Ok(HistoricalCandidateWindow::Expired { oldest_available });
        };
        let exhausted = rows.len() < HISTORICAL_CANDIDATE_CAP;
        for row in &rows {
            if !position.represents_event(row.event_id, row.resource_version) {
                touched.insert(row.key.clone());
            }
        }
        if let Some(last) = rows.last() {
            raw_cursor = Some((last.key.clone(), last.event_id));
        }
        if exhausted || touched.len() >= HISTORICAL_CANDIDATE_CAP {
            break;
        }
    }
    let merged = current.into_iter().chain(touched).collect::<BTreeSet<_>>();
    Ok(HistoricalCandidateWindow::Items(
        merged.into_iter().take(HISTORICAL_CANDIDATE_CAP).collect(),
    ))
}

struct HistoricalRawCandidate {
    key: ResourceCollectionKey,
    event_id: i64,
    resource_version: i64,
}

fn append_history_scope(
    sql: &mut String,
    values: &mut Vec<Box<dyn ToSql>>,
    scope: &ResourceCollectionScope,
) {
    match scope {
        ResourceCollectionScope::Cluster => sql.push_str(" AND namespace IS NULL"),
        ResourceCollectionScope::AllNamespaces => sql.push_str(" AND namespace IS NOT NULL"),
        ResourceCollectionScope::Namespace(namespace) => {
            sql.push_str(&format!(" AND namespace = ?{}", values.len() + 1));
            values.push(Box::new(namespace.clone()));
        }
    }
}

fn append_lexical_after(
    sql: &mut String,
    values: &mut Vec<Box<dyn ToSql>>,
    scope: &ResourceCollectionScope,
    after: Option<&ResourceCollectionKey>,
) {
    let Some(after) = after else { return };
    match scope {
        ResourceCollectionScope::AllNamespaces => {
            let namespace = after.namespace().unwrap_or_default().to_string();
            sql.push_str(&format!(
                " AND (COALESCE(namespace, '') > ?{} OR (COALESCE(namespace, '') = ?{} AND name > ?{}))",
                values.len() + 1,
                values.len() + 2,
                values.len() + 3
            ));
            values.push(Box::new(namespace.clone()));
            values.push(Box::new(namespace));
            values.push(Box::new(after.name().to_string()));
        }
        ResourceCollectionScope::Cluster | ResourceCollectionScope::Namespace(_) => {
            sql.push_str(&format!(" AND name > ?{}", values.len() + 1));
            values.push(Box::new(after.name().to_string()));
        }
    }
}

fn append_raw_history_after(
    sql: &mut String,
    values: &mut Vec<Box<dyn ToSql>>,
    scope: &ResourceCollectionScope,
    key: &ResourceCollectionKey,
    event_id: i64,
) {
    match scope {
        ResourceCollectionScope::AllNamespaces => {
            let namespace = key.namespace().unwrap_or("#cluster").to_string();
            sql.push_str(&format!(
                " AND (COALESCE(namespace, '#cluster') > ?{} OR (COALESCE(namespace, '#cluster') = ?{} AND (name > ?{} OR (name = ?{} AND id < ?{}))))",
                values.len() + 1,
                values.len() + 2,
                values.len() + 3,
                values.len() + 4,
                values.len() + 5
            ));
            values.push(Box::new(namespace.clone()));
            values.push(Box::new(namespace));
            values.push(Box::new(key.name().to_string()));
            values.push(Box::new(key.name().to_string()));
            values.push(Box::new(event_id));
        }
        ResourceCollectionScope::Cluster | ResourceCollectionScope::Namespace(_) => {
            sql.push_str(&format!(
                " AND (name > ?{} OR (name = ?{} AND id < ?{}))",
                values.len() + 1,
                values.len() + 2,
                values.len() + 3
            ));
            values.push(Box::new(key.name().to_string()));
            values.push(Box::new(key.name().to_string()));
            values.push(Box::new(event_id));
        }
    }
}

enum HistoricalRawCandidateWindow {
    Items(Vec<HistoricalRawCandidate>),
    Expired { oldest_available: i64 },
}

async fn historical_identity_at_position(
    store: &SqliteReadStore,
    request: &ResourceListRequest,
    target: &DurableWatchTarget,
    position: WatchReplayPosition,
    candidate: &ResourceCollectionKey,
) -> std::result::Result<HistoricalIdentity, ResourceReadError> {
    let av = request.api_version().to_string();
    let kind = request.kind().to_string();
    let namespace = candidate.namespace().map(str::to_string);
    let name = candidate.name().to_string();
    let target = target.clone();
    let mut before_id = i64::MAX;
    let mut initialized = false;
    let mut reconstructor = None;
    loop {
        let closure_av = av.clone();
        let closure_kind = kind.clone();
        let closure_namespace = namespace.clone();
        let closure_name = name.clone();
        let closure_target = target.clone();
        let batch = store
            .read_db_call(
                "cluster-read:historical-identity-history",
                move |connection| {
                    if let Some(oldest_available) =
                        historical_floor(connection, &closure_target, position)?
                    {
                        return Ok(HistoricalHistoryBatch::Expired { oldest_available });
                    }
                    let current = historical_current_resource(
                        connection,
                        &closure_av,
                        &closure_kind,
                        closure_namespace.as_deref(),
                        &closure_name,
                    )?;
                    let mut statement = connection.prepare(
                        "SELECT id, resource_version, event_type, data FROM watch_events \
                 WHERE api_version = ?1 AND kind = ?2 \
                   AND COALESCE(namespace, '#cluster') = COALESCE(?3, '#cluster') \
                   AND name = ?4 AND id < ?5 \
                 ORDER BY id DESC LIMIT ?6",
                    )?;
                    let rows = statement.query_map(
                        rusqlite::params![
                            closure_av,
                            closure_kind,
                            closure_namespace,
                            closure_name,
                            before_id,
                            HISTORICAL_IDENTITY_HISTORY_CAP as i64
                        ],
                        |row| {
                            let event_id: i64 = row.get(0)?;
                            let resource_version: i64 = row.get(1)?;
                            let event_type: String = row.get(2)?;
                            let data = json_from_bytes(row.get(3)?)?;
                            Ok((event_id, resource_version, event_type, data))
                        },
                    )?;
                    Ok(HistoricalHistoryBatch::Events {
                        current,
                        events: rows.collect::<rusqlite::Result<_>>()?,
                    })
                },
            )
            .await
            .map_err(|error| ResourceReadError::retryable(error.to_string()))?;
        historical_window_test_control()
            .history_windows
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let HistoricalHistoryBatch::Events {
            current: batch_current,
            events,
        } = batch
        else {
            let HistoricalHistoryBatch::Expired { oldest_available } = batch else {
                unreachable!()
            };
            return Ok(HistoricalIdentity::Expired { oldest_available });
        };
        record_physical_history_batch(&name, events.len());
        if !initialized {
            reconstructor = Some(crate::position_membership::MembershipReconstructor::new(
                batch_current.into_iter().collect(),
                position,
            ));
            initialized = true;
        }
        let short = events.len() < HISTORICAL_IDENTITY_HISTORY_CAP;
        if let Some(last) = events.last() {
            before_id = last.0;
        }
        let reconstructor = reconstructor
            .as_mut()
            .expect("initialized with first bounded history batch");
        for (event_id, resource_version, event_type, data) in events {
            reconstructor.observe(&crate::position_membership::MembershipHistoryEvent {
                event_id,
                event_type,
                resource: crate::position_membership::resource_from_history(
                    av.clone(),
                    kind.clone(),
                    namespace.clone(),
                    name.clone(),
                    resource_version,
                    data,
                ),
            });
        }
        if short || reconstructor.can_stop_before(before_id) {
            return match std::mem::replace(
                reconstructor,
                crate::position_membership::MembershipReconstructor::new(Vec::new(), position),
            )
            .finish()
            {
                crate::position_membership::ReconstructedMembership::Items(mut items) => Ok(items
                    .pop()
                    .map_or(HistoricalIdentity::Absent, HistoricalIdentity::Resource)),
                crate::position_membership::ReconstructedMembership::Expired => {
                    // The floor was available at every turn, but a particular
                    // identity has an incomplete predecessor chain.  Fail
                    // closed just like the collection reconstructor.
                    Ok(HistoricalIdentity::Expired {
                        oldest_available: position.resource_version,
                    })
                }
            };
        }
    }
}

enum HistoricalHistoryBatch {
    Events {
        current: Option<Resource>,
        events: Vec<(i64, i64, String, Value)>,
    },
    Expired {
        oldest_available: i64,
    },
}

fn historical_floor(
    connection: &rusqlite::Connection,
    target: &DurableWatchTarget,
    position: WatchReplayPosition,
) -> rusqlite::Result<Option<i64>> {
    let current_rv: i64 = connection.query_row(
        "SELECT CAST(value AS INTEGER) FROM metadata WHERE key = 'resource_version'",
        [],
        |row| row.get(0),
    )?;
    let current_event_id: i64 = connection.query_row(
        "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'watch_events'), 0)",
        [],
        |row| row.get(0),
    )?;
    if position.event_id > current_event_id
        || position.resource_version_filter_through_event_id > current_event_id
        || (position.event_id == 0
            && position.resource_version_filter_through_event_id == 0
            && position.resource_version > current_rv)
    {
        return Ok(Some(current_rv));
    }
    let boundaries = super::replay_floor::target_replay_boundaries(
        connection,
        target.api_version(),
        target.kind(),
        target.scope(),
    )?;
    if klights_cluster_store::ReplayRetentionBoundary::classify_all(
        boundaries.iter().copied(),
        position,
    ) != klights_cluster_store::ReplayAvailability::Expired
    {
        return Ok(None);
    }
    Ok(Some(
        boundaries
            .into_iter()
            .map(|boundary| match boundary {
                klights_cluster_store::ReplayRetentionBoundary::Exact(position) => {
                    position.resource_version
                }
                klights_cluster_store::ReplayRetentionBoundary::LegacyRvOnly {
                    resource_version,
                } => resource_version,
            })
            .max()
            .unwrap_or(position.resource_version),
    ))
}

fn historical_current_resource(
    connection: &rusqlite::Connection,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
) -> rusqlite::Result<Option<Resource>> {
    let (sql, params): (&str, Vec<Box<dyn ToSql>>) = if api_version == "v1" && kind == "Namespace" {
        (
            "SELECT resource_version, data FROM namespaces WHERE name = ?1",
            vec![Box::new(name.to_string())],
        )
    } else if namespace.is_some() {
        (
            "SELECT resource_version, data FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4",
            vec![
                Box::new(api_version.to_string()),
                Box::new(kind.to_string()),
                Box::new(namespace.unwrap().to_string()),
                Box::new(name.to_string()),
            ],
        )
    } else {
        (
            "SELECT resource_version, data FROM cluster_resources WHERE api_version = ?1 AND kind = ?2 AND name = ?3",
            vec![
                Box::new(api_version.to_string()),
                Box::new(kind.to_string()),
                Box::new(name.to_string()),
            ],
        )
    };
    let refs = params
        .iter()
        .map(|value| value.as_ref())
        .collect::<Vec<_>>();
    connection
        .query_row(sql, refs.as_slice(), |row| {
            Ok(crate::position_membership::resource_from_history(
                api_version.to_string(),
                kind.to_string(),
                namespace.map(str::to_string),
                name.to_string(),
                row.get(0)?,
                json_from_bytes(row.get(1)?)?,
            ))
        })
        .optional()
}
