//! Backend-private, root-independent Redb read algorithms.
//!
//! This is the mechanically movable Phase 10B implementation core. It owns
//! Redb-specific DTOs where the focused cluster-store contracts do not cover
//! legacy compatibility semantics.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::net::Ipv4Addr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::OnceLock;

use super::{RedbAccessor, tables};
use anyhow::{Result, anyhow};
use bytes::Bytes;
use klights_cluster_core::{PositionedWatchEvent, Resource, WatchReplayPosition};
use klights_cluster_store::{
    DataplaneEncryption, DataplaneMode, DataplanePeerMetadata, DurableRawWatchEvent,
    DurableReplayFloor, DurableReplayTarget, DurableWatchEvent, DurableWatchScope,
    DurableWatchTarget, PeerTopologyRequest, ResourceCollectionKey, ResourceCollectionScope,
    ResourceContinuation, ResourceListPage, ResourceListRequest, ResourceListSnapshot,
    ResourceReadError, StoredNodeSubnet,
};
use klights_types::{HostPortRange, NodeName, NodePeerMode, PodSubnet};
use redb::{ReadableDatabase, ReadableTable};
use serde::Deserialize;
use serde_json::{Value, value::RawValue};

use super::key_codec::{decode_resource_key, lex_next, resource_key, resource_prefix};
use super::replay_floor::LegacyReplayFloor;

const CLUSTER_NAMESPACE_KEY: &str = "#cluster";
const HISTORICAL_CANDIDATE_CAP: usize = 64;
const HISTORICAL_IDENTITY_HISTORY_CAP: usize = 64;
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

fn is_physical_bound_name(name: &str) -> bool {
    name.starts_with(PHYSICAL_BOUND_PREFIX)
}

struct HistoricalWindowTestControl {
    pause_once: std::sync::atomic::AtomicBool,
    pause_resource_version: std::sync::atomic::AtomicI64,
    pause_event_id: std::sync::atomic::AtomicI64,
    reached: tokio::sync::Semaphore,
    resume: tokio::sync::Semaphore,
    candidate_windows: std::sync::atomic::AtomicU64,
    history_windows: std::sync::atomic::AtomicU64,
}

fn historical_window_test_control() -> &'static HistoricalWindowTestControl {
    static CONTROL: OnceLock<HistoricalWindowTestControl> = OnceLock::new();
    CONTROL.get_or_init(|| HistoricalWindowTestControl {
        pause_once: std::sync::atomic::AtomicBool::new(false),
        pause_resource_version: std::sync::atomic::AtomicI64::new(0),
        pause_event_id: std::sync::atomic::AtomicI64::new(0),
        reached: tokio::sync::Semaphore::new(0),
        resume: tokio::sync::Semaphore::new(0),
        candidate_windows: std::sync::atomic::AtomicU64::new(0),
        history_windows: std::sync::atomic::AtomicU64::new(0),
    })
}

pub fn arm_historical_window_pause_for_test(position: WatchReplayPosition) {
    use std::sync::atomic::Ordering;
    let c = historical_window_test_control();
    c.pause_resource_version
        .store(position.resource_version, Ordering::Release);
    c.pause_event_id.store(position.event_id, Ordering::Release);
    c.pause_once.store(true, Ordering::Release);
    c.candidate_windows.store(0, Ordering::Release);
    c.history_windows.store(0, Ordering::Release);
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
    let c = historical_window_test_control();
    (
        c.candidate_windows.load(Ordering::Acquire),
        c.history_windows.load(Ordering::Acquire),
    )
}

#[derive(Clone)]
pub struct RedbReadCore {
    accessor: Arc<RedbAccessor>,
}

#[derive(Clone, Debug)]
pub enum RedbCollectionScope {
    LegacyAny,
    Cluster,
    AllNamespaces,
    Namespace(String),
}

#[derive(Clone, Debug, Default)]
pub struct RedbListQuery {
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub limit: Option<i64>,
    pub cursor: Option<ResourceCollectionKey>,
}

#[derive(Clone, Debug)]
pub struct RedbResourceList {
    pub items: Vec<Resource>,
    pub position: WatchReplayPosition,
    pub continuation: Option<ResourceCollectionKey>,
    pub remaining_item_count: Option<i64>,
}

#[derive(Clone, Debug)]
pub enum RedbCheckedWatchRead<T> {
    Events(Vec<T>),
    Expired,
}

#[derive(Clone, Debug)]
pub struct RedbPositionedWatchPage<T> {
    pub events: Vec<PositionedWatchEvent<T>>,
    pub next_position: WatchReplayPosition,
}

#[derive(Clone, Debug)]
pub enum RedbPositionedWatchRead<T> {
    Events(RedbPositionedWatchPage<T>),
    Expired,
}

#[derive(Clone, Debug)]
pub enum RedbSnapshotRead {
    Historical {
        items: Vec<Resource>,
        position: WatchReplayPosition,
    },
    Expired {
        oldest_available: i64,
    },
}

pub enum RedbHistoricalListRead {
    Page(ResourceListPage),
    Expired { oldest_available: i64 },
}

#[derive(Deserialize)]
struct StoredWatchEvent<'a> {
    #[serde(rename = "apiVersion")]
    api_version: Option<&'a str>,
    kind: Option<&'a str>,
    namespace: Option<&'a str>,
    name: Option<&'a str>,
    #[serde(rename = "eventType")]
    event_type: Option<&'a str>,
    #[serde(rename = "resourceVersion")]
    resource_version: Option<i64>,
    #[serde(borrow)]
    data: Option<&'a RawValue>,
}

impl RedbReadCore {
    pub fn new(accessor: Arc<RedbAccessor>) -> Self {
        Self { accessor }
    }

    async fn call<T, F>(&self, label: &'static str, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&redb::Database) -> Result<T> + Send + 'static,
    {
        self.accessor.call(label, operation).await
    }

    pub async fn allocator_position(&self) -> Result<WatchReplayPosition> {
        self.call("redb-read:allocator-position", |database| {
            let read = database.begin_read()?;
            replay_position_in_read(&read)
        })
        .await
    }

    pub async fn exact_resource_version_position(
        &self,
        resource_version: i64,
    ) -> Result<WatchReplayPosition> {
        self.call("redb-read:historical-exact-position", move |database| {
            let read = database.begin_read()?;
            let head = replay_position_in_read(&read)?;
            // Establish an exact RV boundary without reverse-scanning the
            // global watch log: through this fixed apply-order head, rows are
            // filtered by RV. A continuation carries this immutable boundary.
            Ok(WatchReplayPosition {
                resource_version,
                event_id: 0,
                resource_version_filter_through_event_id: head.event_id,
            })
        })
        .await
    }

    pub async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        let api_version = api_version.to_string();
        let kind = kind.to_string();
        let namespace = namespace.map(str::to_string);
        let name = name.to_string();
        self.call("redb-read:get-resource", move |database| {
            let read = database.begin_read()?;
            if api_version == "v1" && kind == "Namespace" && namespace.is_none() {
                let table = read.open_table(tables::NAMESPACES)?;
                return Ok(table
                    .get(name.as_str())?
                    .map(|body| namespace_from_body(&name, body.value())));
            }
            let key = resource_key(&api_version, &kind, namespace.as_deref(), &name);
            let table = read.open_table(if namespace.is_some() {
                tables::RES_NS
            } else {
                tables::RES_CLUSTER
            })?;
            Ok(table.get(key.as_slice())?.map(|value| {
                let (resource_version, body) = value.value();
                resource_from_body(
                    &api_version,
                    &kind,
                    namespace.clone(),
                    &name,
                    resource_version as i64,
                    body,
                )
            }))
        })
        .await
    }

    pub async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        scope: RedbCollectionScope,
        query: RedbListQuery,
    ) -> Result<RedbResourceList> {
        let api_version = api_version.to_string();
        let kind = kind.to_string();
        let labels = query
            .label_selector
            .as_deref()
            .map(klights_types::parse_label_selector)
            .transpose()?
            .unwrap_or_default();
        let fields = query
            .field_selector
            .as_deref()
            .map(klights_types::FieldSelector::parse)
            .transpose()?;
        let has_selectors = !labels.is_empty() || fields.is_some();
        let limit = query
            .limit
            .filter(|value| *value > 0)
            .and_then(|value| usize::try_from(value).ok());
        let cursor = query.cursor;
        self.call("redb-read:list-resources", move |database| {
            let read = database.begin_read()?;
            let position = replay_position_in_read(&read)?;
            if api_version == "v1"
                && kind == "Namespace"
                && matches!(scope, RedbCollectionScope::Cluster)
            {
                return list_namespaces_in_read(&read, labels, fields, limit, cursor, position);
            }
            if !has_selectors && !matches!(scope, RedbCollectionScope::LegacyAny) {
                return list_current_identity_page_in_read(
                    &read,
                    &api_version,
                    &kind,
                    &scope,
                    limit,
                    cursor,
                    position,
                );
            }
            if matches!(scope, RedbCollectionScope::AllNamespaces) {
                let table = read.open_table(tables::RES_NS)?;
                let prefix = resource_prefix(&api_version, &kind, None);
                let start = cursor
                    .as_ref()
                    .and_then(|cursor| {
                        cursor.namespace().and_then(|namespace| {
                            lex_next(&resource_key(
                                &api_version,
                                &kind,
                                Some(namespace),
                                cursor.name(),
                            ))
                        })
                    })
                    .unwrap_or_else(|| {
                        let mut start = prefix.clone();
                        start.push(0);
                        start
                    });
                let end = lex_next(&prefix).unwrap_or_else(|| {
                    let mut end = prefix;
                    end.push(0xff);
                    end
                });
                let mut items = Vec::new();
                for entry in table.range(start.as_slice()..end.as_slice())? {
                    if limit.is_some_and(|limit| items.len() > limit) {
                        break;
                    }
                    let (_, value) = entry?;
                    let (resource_version, body) = value.value();
                    let data: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
                    let resource =
                        resource_from_data(&api_version, &kind, resource_version as i64, data);
                    if labels.iter().all(|requirement| {
                        requirement.matches(
                            resource
                                .data
                                .pointer("/metadata/labels")
                                .and_then(Value::as_object),
                        )
                    }) && fields.as_ref().is_none_or(|selector| {
                        selector.matches_resource_with_identity(
                            &api_version,
                            &kind,
                            resource.data.as_ref(),
                        )
                    }) {
                        items.push(resource);
                    }
                }
                let has_more = limit.is_some_and(|limit| items.len() > limit);
                if let Some(limit) = limit {
                    items.truncate(limit);
                }
                let continuation = has_more.then(|| {
                    let item = items.last().expect("non-empty all-namespace page");
                    ResourceCollectionKey::new(item.namespace.clone(), item.name.clone())
                });
                return Ok(RedbResourceList {
                    items,
                    position,
                    continuation,
                    // The exact count is optional in Kubernetes. Computing it
                    // would require draining the rest of this ordered range,
                    // which violates a bounded page read.
                    remaining_item_count: None,
                });
            }

            let scans: Vec<(_, Option<String>)> = match &scope {
                RedbCollectionScope::LegacyAny => {
                    vec![(tables::RES_NS, None), (tables::RES_CLUSTER, None)]
                }
                RedbCollectionScope::Cluster => vec![(tables::RES_CLUSTER, None)],
                RedbCollectionScope::AllNamespaces => vec![(tables::RES_NS, None)],
                RedbCollectionScope::Namespace(namespace) => {
                    vec![(tables::RES_NS, Some(namespace.clone()))]
                }
            };
            let target = if has_selectors {
                limit.map_or(usize::MAX, |value| value.saturating_add(1))
            } else {
                usize::MAX
            };
            let mut items = Vec::with_capacity(target.min(4096));
            let mut has_more = false;

            for (table_definition, namespace_filter) in scans {
                let table = read.open_table(table_definition)?;
                let prefix = resource_prefix(&api_version, &kind, namespace_filter.as_deref());
                let start = cursor
                    .as_ref()
                    .and_then(|cursor| {
                        let cursor_namespace = match &scope {
                            RedbCollectionScope::AllNamespaces => cursor.namespace(),
                            RedbCollectionScope::Namespace(namespace) => Some(namespace.as_str()),
                            RedbCollectionScope::LegacyAny | RedbCollectionScope::Cluster => None,
                        };
                        lex_next(&resource_key(
                            &api_version,
                            &kind,
                            cursor_namespace,
                            cursor.name(),
                        ))
                    })
                    .unwrap_or_else(|| {
                        let mut start = prefix.clone();
                        start.push(0);
                        start
                    });
                let end = lex_next(&prefix).unwrap_or_else(|| {
                    let mut end = prefix;
                    end.push(0xff);
                    end
                });
                for entry in table.range(start.as_slice()..end.as_slice())? {
                    let (_key, value) = entry?;
                    let (resource_version, body) = value.value();
                    let data: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
                    let resource =
                        resource_from_data(&api_version, &kind, resource_version as i64, data);
                    if !labels.iter().all(|requirement| {
                        requirement.matches(
                            resource
                                .data
                                .pointer("/metadata/labels")
                                .and_then(Value::as_object),
                        )
                    }) || fields.as_ref().is_some_and(|selector| {
                        !selector.matches_resource_with_identity(
                            &api_version,
                            &kind,
                            resource.data.as_ref(),
                        )
                    }) {
                        continue;
                    }
                    if has_selectors {
                        if items.len() == target {
                            break;
                        }
                        items.push(resource);
                    } else {
                        items.push(resource);
                        if limit.is_some_and(|limit| items.len() > limit) {
                            has_more = true;
                            break;
                        }
                    }
                }
                if has_selectors && items.len() == target {
                    break;
                }
            }

            if has_selectors {
                has_more = limit.is_some_and(|limit| items.len() > limit);
                if let Some(limit) = limit {
                    items.truncate(limit);
                }
            } else if let Some(limit) = limit {
                // The selector-free loop deliberately reads one probe row to
                // establish continuation.  Never expose that probe as an
                // extra Kubernetes item.
                items.truncate(limit);
            }
            let continuation = has_more.then(|| {
                let item = items.last().expect("non-empty limited Redb page");
                ResourceCollectionKey::new(item.namespace.clone(), item.name.clone())
            });
            Ok(RedbResourceList {
                items,
                position,
                continuation,
                // See the all-namespace branch above: an exact remaining
                // count would force a collection-sized scan after the page.
                remaining_item_count: None,
            })
        })
        .await
    }

    /// Bounded historical LIST state machine. Candidate identities are walked
    /// from the derived identity index and each reverse-history DB turn is
    /// capped independently. The public cursor is only the last emitted key.
    pub async fn bounded_historical_list(
        &self,
        request: ResourceListRequest,
        position: WatchReplayPosition,
    ) -> std::result::Result<RedbHistoricalListRead, ResourceReadError> {
        let labels = request
            .query()
            .label_selector()
            .filter(|value| !value.trim().is_empty())
            .map(klights_types::parse_label_selector)
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
        let target =
            durable_target_for_scope(request.api_version(), request.kind(), request.scope());
        let limit = request
            .query()
            .limit()
            .and_then(|value| usize::try_from(value).ok());
        let probe = limit.map_or(usize::MAX, |value| value.saturating_add(1));
        let mut emitted = Vec::with_capacity(probe.min(HISTORICAL_CANDIDATE_CAP));
        let mut after = request.query().start_after().cloned();
        let mut more_candidates: bool;
        let mut instrument_test_windows = false;
        loop {
            let candidates = match self
                .historical_candidate_window(
                    request.api_version(),
                    request.kind(),
                    request.scope(),
                    &target,
                    position,
                    after.clone(),
                )
                .await?
            {
                RedbHistoricalCandidates::Items(items) => items,
                RedbHistoricalCandidates::Expired { oldest_available } => {
                    return Ok(RedbHistoricalListRead::Expired { oldest_available });
                }
            };
            if candidates
                .iter()
                .all(|candidate| is_physical_bound_name(candidate.name()))
            {
                PHYSICAL_CANDIDATE_BATCH_MAX
                    .fetch_max(candidates.len() as u64, std::sync::atomic::Ordering::AcqRel);
            }
            {
                use std::sync::atomic::Ordering;
                let control = historical_window_test_control();
                let pause_matches_position = control.pause_resource_version.load(Ordering::Acquire)
                    == position.resource_version
                    && control.pause_event_id.load(Ordering::Acquire) == position.event_id;
                if pause_matches_position
                    && control
                        .pause_once
                        .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                {
                    instrument_test_windows = true;
                    control.reached.add_permits(1);
                    control.resume.acquire().await.unwrap().forget();
                }
                if instrument_test_windows {
                    control.candidate_windows.fetch_add(1, Ordering::AcqRel);
                }
            }
            if candidates.is_empty() {
                more_candidates = false;
                break;
            }
            more_candidates = candidates.len() == HISTORICAL_CANDIDATE_CAP;
            for candidate in candidates {
                after = Some(candidate.clone());
                match self
                    .historical_identity_at_position(
                        request.api_version(),
                        request.kind(),
                        &target,
                        position,
                        &candidate,
                        instrument_test_windows,
                    )
                    .await?
                {
                    RedbHistoricalIdentity::Absent => continue,
                    RedbHistoricalIdentity::Expired { oldest_available } => {
                        return Ok(RedbHistoricalListRead::Expired { oldest_available });
                    }
                    RedbHistoricalIdentity::Resource(resource) => {
                        if !labels.as_ref().is_none_or(|requirements| {
                            requirements.iter().all(|requirement| {
                                requirement.matches(
                                    resource
                                        .data
                                        .pointer("/metadata/labels")
                                        .and_then(Value::as_object),
                                )
                            })
                        }) || fields.as_ref().is_some_and(|selector| {
                            !selector.matches_resource_with_identity(
                                &resource.api_version,
                                &resource.kind,
                                &resource.data,
                            )
                        }) {
                            continue;
                        }
                        emitted.push(resource);
                        if emitted.len() == probe {
                            break;
                        }
                    }
                }
            }
            if emitted.len() == probe || !more_candidates {
                break;
            }
        }
        let has_more = limit.is_some_and(|value| emitted.len() > value)
            || (limit.is_none() && more_candidates);
        if let Some(limit) = limit {
            emitted.truncate(limit);
        }
        let snapshot = ResourceListSnapshot::try_new(position)?;
        let continuation = has_more
            .then(|| {
                emitted.last().map(|resource| {
                    ResourceContinuation::new(
                        ResourceCollectionKey::new(
                            resource.namespace.clone(),
                            resource.name.clone(),
                        ),
                        snapshot,
                    )
                })
            })
            .flatten();
        Ok(RedbHistoricalListRead::Page(ResourceListPage::try_new(
            emitted,
            snapshot,
            continuation,
            None,
        )?))
    }

    async fn historical_candidate_window(
        &self,
        api_version: &str,
        kind: &str,
        scope: &ResourceCollectionScope,
        target: &DurableWatchTarget,
        position: WatchReplayPosition,
        after: Option<ResourceCollectionKey>,
    ) -> std::result::Result<RedbHistoricalCandidates, ResourceReadError> {
        let api_version = api_version.to_string();
        let kind = kind.to_string();
        let scope = scope.clone();
        let target = target.clone();
        self.call("redb-read:historical-candidate-window", move |database| {
            let read = database.begin_read()?;
            let current = replay_position_in_read(&read)?;
            if historical_position_unavailable(position, current) {
                return Ok(RedbHistoricalCandidates::Expired {
                    oldest_available: current.resource_version,
                });
            }
            if position_expired_for_targets(&read, std::slice::from_ref(&target), position)? {
                return Ok(RedbHistoricalCandidates::Expired {
                    oldest_available: oldest_available_for_targets(
                        &read,
                        std::slice::from_ref(&target),
                        position.resource_version,
                    )?,
                });
            }
            let (prefix, namespaced) = history_prefix(&api_version, &kind, &scope);
            let end = lex_next(&prefix).unwrap_or_else(|| vec![0xff]);
            let start = after
                .as_ref()
                .map(|key| {
                    history_identity_prefix(
                        &api_version,
                        &kind,
                        key.namespace(),
                        key.name(),
                        namespaced,
                    )
                })
                .and_then(|key| lex_next(&key))
                .unwrap_or(prefix);
            let history = read.open_table(tables::RESOURCE_HISTORY_BY_IDENTITY)?;
            let current = read.open_table(tables::RESOURCE_CURRENT_BY_IDENTITY)?;
            let events = read.open_table(tables::WATCH_EVENTS)?;
            // Merge two independently seekable lexical streams.  Advancing a
            // history identity skips every event for that identity before the
            // next seek, so churn cannot turn this into a global event scan.
            let mut history_start = start.clone();
            let mut current_start = start;
            let mut history_next = None;
            let mut current_next = None;
            let mut history_done = false;
            let mut current_done = false;
            let mut candidates = Vec::with_capacity(HISTORICAL_CANDIDATE_CAP);
            while candidates.len() < HISTORICAL_CANDIDATE_CAP {
                if history_next.is_none() && !history_done {
                    let Some(entry) = history
                        .range(history_start.as_slice()..end.as_slice())?
                        .next()
                    else {
                        history_done = true;
                        continue;
                    };
                    let (key, _) = entry?;
                    let encoded = key.value();
                    let Some((namespace, name, identity_end)) =
                        decode_history_identity(encoded, namespaced)
                    else {
                        return Err(anyhow!("malformed derived resource-history identity key"));
                    };
                    history_start =
                        lex_next(&encoded[..identity_end]).unwrap_or_else(|| end.clone());
                    // An identity is a historical candidate only when its
                    // latest retained mutation is not represented by P.  The
                    // event id is monotonic for an identity, so a represented
                    // latest mutation means every older retained mutation is
                    // already part of the positioned state.  Skipping it here
                    // prevents an empty page from walking every pre-P
                    // tombstone in retained history.
                    let identity_prefix = &encoded[..identity_end];
                    let identity_end_key = lex_next(identity_prefix).unwrap_or_else(|| end.clone());
                    let Some(latest) = history
                        .range(identity_prefix..identity_end_key.as_slice())?
                        .next_back()
                    else {
                        continue;
                    };
                    let (_, latest_event_id) = latest?;
                    let latest_event_id = latest_event_id.value();
                    let Some(stored) = events.get(latest_event_id)? else {
                        return Err(anyhow!(
                            "derived history index points at a missing watch event"
                        ));
                    };
                    let stored: StoredWatchEvent<'_> = serde_json::from_slice(stored.value())?;
                    let latest_resource_version = stored
                        .resource_version
                        .ok_or_else(|| anyhow!("watch event is missing resourceVersion"))?;
                    if position.represents_event(latest_event_id as i64, latest_resource_version) {
                        continue;
                    }
                    history_next = Some(ResourceCollectionKey::new(namespace, name));
                }
                if current_next.is_none() && !current_done {
                    let Some(entry) = current
                        .range(current_start.as_slice()..end.as_slice())?
                        .next()
                    else {
                        current_done = true;
                        continue;
                    };
                    let (key, _) = entry?;
                    let mut synthetic = key.value().to_vec();
                    synthetic.extend_from_slice(&0_u64.to_be_bytes());
                    let Some((namespace, name, _)) =
                        decode_history_identity(&synthetic, namespaced)
                    else {
                        return Err(anyhow!("malformed current resource identity key"));
                    };
                    current_start = lex_next(key.value()).unwrap_or_else(|| end.clone());
                    current_next = Some(ResourceCollectionKey::new(namespace, name));
                }
                match (&history_next, &current_next) {
                    (None, None) => break,
                    (Some(history), None) => {
                        candidates.push(history.clone());
                        history_next = None;
                    }
                    (None, Some(current)) => {
                        candidates.push(current.clone());
                        current_next = None;
                    }
                    (Some(history), Some(current)) => match history.cmp(current) {
                        std::cmp::Ordering::Less => {
                            candidates.push(history.clone());
                            history_next = None;
                        }
                        std::cmp::Ordering::Greater => {
                            candidates.push(current.clone());
                            current_next = None;
                        }
                        std::cmp::Ordering::Equal => {
                            candidates.push(history.clone());
                            history_next = None;
                            current_next = None;
                        }
                    },
                }
            }
            Ok(RedbHistoricalCandidates::Items(candidates))
        })
        .await
        .map_err(|error| ResourceReadError::retryable(error.to_string()))
    }

    async fn historical_identity_at_position(
        &self,
        api_version: &str,
        kind: &str,
        target: &DurableWatchTarget,
        position: WatchReplayPosition,
        candidate: &ResourceCollectionKey,
        instrument_test_windows: bool,
    ) -> std::result::Result<RedbHistoricalIdentity, ResourceReadError> {
        let api_version = api_version.to_string();
        let kind = kind.to_string();
        let namespace = candidate.namespace().map(str::to_string);
        let name = candidate.name().to_string();
        let target = target.clone();
        let namespaced = namespace.is_some();
        let identity =
            history_identity_prefix(&api_version, &kind, namespace.as_deref(), &name, namespaced);
        let identity_end = lex_next(&identity).unwrap_or_else(|| vec![0xff]);
        let mut before = identity_end;
        let mut initialized = false;
        let mut reconstructor = None;
        loop {
            let api_version = api_version.clone();
            let kind = kind.clone();
            let namespace = namespace.clone();
            let name = name.clone();
            let physical_bound_identity = is_physical_bound_name(&name);
            let closure_identity = identity.clone();
            let closure_before = before.clone();
            let target = target.clone();
            let batch = self
                .call("redb-read:historical-identity-history", move |database| {
                    let read = database.begin_read()?;
                    let current_position = replay_position_in_read(&read)?;
                    if historical_position_unavailable(position, current_position) {
                        return Ok(RedbHistoricalHistory::Expired {
                            oldest_available: current_position.resource_version,
                        });
                    }
                    if position_expired_for_targets(&read, std::slice::from_ref(&target), position)?
                    {
                        return Ok(RedbHistoricalHistory::Expired {
                            oldest_available: oldest_available_for_targets(
                                &read,
                                std::slice::from_ref(&target),
                                position.resource_version,
                            )?,
                        });
                    }
                    let current = read_current_identity(
                        &read,
                        &api_version,
                        &kind,
                        namespace.as_deref(),
                        &name,
                    )?;
                    let index = read.open_table(tables::RESOURCE_HISTORY_BY_IDENTITY)?;
                    let events_table = read.open_table(tables::WATCH_EVENTS)?;
                    let mut events = Vec::with_capacity(HISTORICAL_IDENTITY_HISTORY_CAP);
                    for entry in index
                        .range(closure_identity.as_slice()..closure_before.as_slice())?
                        .rev()
                    {
                        let (_, event_id) = entry?;
                        let id = event_id.value();
                        let Some(encoded) = events_table.get(id)? else {
                            return Err(anyhow!(
                                "derived history index points at a missing watch event"
                            ));
                        };
                        let stored: StoredWatchEvent<'_> = serde_json::from_slice(encoded.value())?;
                        let event = durable_event_from_stored(stored)?;
                        events.push(MembershipHistoryEvent {
                            event_id: id as i64,
                            event_type: event.event_type().to_string(),
                            resource: event.into_resource(),
                        });
                        if events.len() == HISTORICAL_IDENTITY_HISTORY_CAP {
                            break;
                        }
                    }
                    Ok(RedbHistoricalHistory::Events { current, events })
                })
                .await
                .map_err(|error| ResourceReadError::retryable(error.to_string()))?;
            let RedbHistoricalHistory::Events { current, events } = batch else {
                let RedbHistoricalHistory::Expired { oldest_available } = batch else {
                    unreachable!()
                };
                return Ok(RedbHistoricalIdentity::Expired { oldest_available });
            };
            if instrument_test_windows {
                historical_window_test_control()
                    .history_windows
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            }
            if !initialized {
                reconstructor = Some(MembershipReconstructor::new(
                    current.into_iter().collect(),
                    position,
                ));
                initialized = true;
            }
            let short = events.len() < HISTORICAL_IDENTITY_HISTORY_CAP;
            if physical_bound_identity {
                PHYSICAL_EVENT_DECODES
                    .fetch_add(events.len() as u64, std::sync::atomic::Ordering::AcqRel);
                PHYSICAL_HISTORY_BATCH_MAX
                    .fetch_max(events.len() as u64, std::sync::atomic::Ordering::AcqRel);
            }
            if let Some(last) = events.last() {
                let mut key = identity.clone();
                key.extend_from_slice(&(last.event_id as u64).to_be_bytes());
                before = key;
            }
            let reconstructor = reconstructor
                .as_mut()
                .expect("first history window initializes reverse state");
            for event in &events {
                reconstructor.observe(event);
            }
            if short
                || reconstructor.can_stop_before(events.last().map_or(0, |event| event.event_id))
            {
                return match std::mem::replace(
                    reconstructor,
                    MembershipReconstructor::new(Vec::new(), position),
                )
                .finish()
                {
                    ReconstructedMembership::Items(mut items) => Ok(items.pop().map_or(
                        RedbHistoricalIdentity::Absent,
                        RedbHistoricalIdentity::Resource,
                    )),
                    ReconstructedMembership::Expired => Ok(RedbHistoricalIdentity::Expired {
                        oldest_available: position.resource_version,
                    }),
                };
            }
        }
    }

    pub async fn list_resources_for_watch_targets(
        &self,
        targets: &[DurableWatchTarget],
        label_selector: Option<&str>,
    ) -> Result<(Vec<Resource>, WatchReplayPosition)> {
        let targets = targets.to_vec();
        let requirements = label_selector
            .map(klights_types::parse_label_selector)
            .transpose()?
            .unwrap_or_default();
        self.call("redb-read:list-watch-targets", move |database| {
            let read = database.begin_read()?;
            let mut items = read_current_targets(&read, &targets)?;
            items.retain(|resource| {
                requirements.iter().all(|requirement| {
                    requirement.matches(
                        resource
                            .data
                            .pointer("/metadata/labels")
                            .and_then(Value::as_object),
                    )
                })
            });
            sort_for_targets(&mut items, &targets);
            Ok((items, replay_position_in_read(&read)?))
        })
        .await
    }

    pub async fn list_resource_keys(
        &self,
        api_version: &str,
        kind: &str,
        namespaced: bool,
    ) -> Result<Vec<ResourceCollectionKey>> {
        let api_version = api_version.to_string();
        let kind = kind.to_string();
        self.call("redb-read:list-resource-keys", move |database| {
            let read = database.begin_read()?;
            let table = read.open_table(if namespaced {
                tables::RES_NS
            } else {
                tables::RES_CLUSTER
            })?;
            let mut keys = Vec::new();
            for entry in table.iter()? {
                let (key, _value) = entry?;
                let Some((entry_api_version, entry_kind, namespace, name)) =
                    decode_resource_key(key.value(), namespaced)
                else {
                    continue;
                };
                if entry_api_version == api_version && entry_kind == kind {
                    keys.push(ResourceCollectionKey::new(namespace, name));
                }
            }
            keys.sort();
            Ok(keys)
        })
        .await
    }

    pub async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        self.call("redb-read:list-cluster-resources", |database| {
            let read = database.begin_read()?;
            let table = read.open_table(tables::RES_CLUSTER)?;
            resources_from_table(&table, false, |_| true)
        })
        .await
    }

    pub async fn find_owned(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        let owner_uid = owner_uid.to_string();
        let namespace = namespace.map(str::to_string);
        self.call("redb-read:find-owned", move |database| {
            let mut prefix = owner_uid.as_bytes().to_vec();
            prefix.push(0);
            let end = lex_next(&prefix).unwrap_or_else(|| {
                let mut end = prefix.clone();
                end.push(0xff);
                end
            });
            let read = database.begin_read()?;
            let table = read.open_table(tables::RESOURCES_BY_OWNER)?;
            let mut items = Vec::new();
            for entry in table.range(prefix.as_slice()..end.as_slice())? {
                let (_, value) = entry?;
                let (resource_version, body) = value.value();
                let resource = untyped_resource(resource_version as i64, body);
                if namespace
                    .as_deref()
                    .is_none_or(|expected| resource.namespace.as_deref() == Some(expected))
                {
                    items.push(resource);
                }
            }
            Ok(items)
        })
        .await
    }

    pub async fn list_namespace_resources(
        &self,
        namespace: &str,
        kind: Option<&str>,
        excluding_kind: bool,
    ) -> Result<Vec<Resource>> {
        let namespace = namespace.to_string();
        let kind = kind.map(str::to_string);
        self.call("redb-read:list-namespace-content", move |database| {
            let read = database.begin_read()?;
            let table = read.open_table(tables::RES_NS)?;
            resources_from_table(&table, true, |resource| {
                resource.namespace.as_deref() == Some(namespace.as_str())
                    && kind
                        .as_deref()
                        .is_none_or(|expected| (resource.kind == expected) != excluding_kind)
            })
        })
        .await
    }

    pub async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        Ok(i64::try_from(
            self.list_namespace_resources(namespace, None, false)
                .await?
                .len(),
        )
        .unwrap_or(i64::MAX))
    }
}

fn list_namespaces_in_read(
    read: &redb::ReadTransaction,
    labels: Vec<klights_types::LabelRequirement>,
    fields: Option<klights_types::FieldSelector>,
    limit: Option<usize>,
    cursor: Option<ResourceCollectionKey>,
    position: WatchReplayPosition,
) -> Result<RedbResourceList> {
    let table = read.open_table(tables::NAMESPACES)?;
    let mut items = Vec::new();
    let probe = limit.map(|value| value.saturating_add(1));
    let mut include = |name: &str, body: &[u8]| {
        let resource = namespace_from_body(name, body);
        if labels.iter().all(|requirement| {
            requirement.matches(
                resource
                    .data
                    .pointer("/metadata/labels")
                    .and_then(Value::as_object),
            )
        }) && fields.as_ref().is_none_or(|selector| {
            selector.matches_resource_with_identity("v1", "Namespace", resource.data.as_ref())
        }) {
            items.push(resource);
        }
        probe.is_some_and(|probe| items.len() >= probe)
    };
    if let Some(cursor) = cursor.as_ref() {
        for entry in table.range(cursor.name()..)? {
            let (name, body) = entry?;
            if name.value() <= cursor.name() {
                continue;
            }
            if include(name.value(), body.value()) {
                break;
            }
        }
    } else {
        for entry in table.iter()? {
            let (name, body) = entry?;
            if include(name.value(), body.value()) {
                break;
            }
        }
    }
    let has_more = limit.is_some_and(|limit| items.len() > limit);
    if let Some(limit) = limit {
        items.truncate(limit);
    }
    let continuation = has_more.then(|| {
        ResourceCollectionKey::new(
            None::<String>,
            items.last().expect("non-empty namespace page").name.clone(),
        )
    });
    Ok(RedbResourceList {
        items,
        position,
        continuation,
        remaining_item_count: None,
    })
}

fn resources_from_table(
    table: &redb::ReadOnlyTable<&[u8], (u64, &[u8])>,
    namespaced: bool,
    mut include: impl FnMut(&Resource) -> bool,
) -> Result<Vec<Resource>> {
    let mut items = Vec::new();
    for entry in table.iter()? {
        let (key, value) = entry?;
        let (resource_version, body) = value.value();
        let Some((api_version, kind, namespace, name)) =
            decode_resource_key(key.value(), namespaced)
        else {
            continue;
        };
        let resource = resource_from_body(
            api_version,
            kind,
            namespace.map(str::to_string),
            name,
            resource_version as i64,
            body,
        );
        if include(&resource) {
            items.push(resource);
        }
    }
    Ok(items)
}

fn resource_from_body(
    api_version: &str,
    kind: &str,
    namespace: Option<impl Into<String>>,
    name: &str,
    resource_version: i64,
    body: &[u8],
) -> Resource {
    let data = serde_json::from_slice(body).unwrap_or(Value::Null);
    Resource {
        id: 0,
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace: namespace.map(Into::into),
        name: name.to_string(),
        uid: Resource::uid_from_data(&data),
        resource_version,
        data: Arc::new(data),
    }
}

fn namespace_from_body(name: &str, body: &[u8]) -> Resource {
    let mut resource = resource_from_body("v1", "Namespace", None::<String>, name, 0, body);
    resource.resource_version = resource
        .data
        .pointer("/metadata/resourceVersion")
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
        .unwrap_or_default();
    resource
}

fn resource_from_data(
    api_version: &str,
    kind: &str,
    resource_version: i64,
    data: Value,
) -> Resource {
    let namespace = data
        .pointer("/metadata/namespace")
        .and_then(Value::as_str)
        .map(str::to_string);
    let name = data
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if is_physical_bound_name(&name) {
        PHYSICAL_RESOURCE_DECODES.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
    Resource {
        id: 0,
        api_version: api_version.to_string(),
        kind: kind.to_string(),
        namespace,
        name,
        uid: Resource::uid_from_data(&data),
        resource_version,
        data: Arc::new(data),
    }
}

fn untyped_resource(resource_version: i64, body: &[u8]) -> Resource {
    let data: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let api_version = data
        .get("apiVersion")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let kind = data
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    resource_from_data(&api_version, &kind, resource_version, data)
}

fn replay_position_in_read(read: &redb::ReadTransaction) -> Result<WatchReplayPosition> {
    let metadata = read.open_table(tables::META)?;
    let parse = |key: &str| -> Result<i64> {
        let Some(value) = metadata.get(key)? else {
            return Ok(0);
        };
        let raw = std::str::from_utf8(value.value())
            .map_err(|error| anyhow!("invalid UTF-8 {key} metadata: {error}"))?;
        raw.parse::<i64>()
            .map_err(|_| anyhow!("invalid numeric {key} metadata {raw:?}"))
    };
    Ok(WatchReplayPosition {
        resource_version: parse("rv")?,
        event_id: parse("watch_event_id")?,
        resource_version_filter_through_event_id: 0,
    })
}

impl RedbReadCore {
    pub async fn watch_events_since(
        &self,
        targets: &[DurableWatchTarget],
        since_resource_version: i64,
    ) -> Result<Vec<DurableWatchEvent>> {
        let targets = targets.to_vec();
        self.call("redb-read:watch-events-since", move |database| {
            let read = database.begin_read()?;
            parsed_watch_events_in_read(&read, &targets, since_resource_version, None, None, None)
        })
        .await
    }

    pub async fn watch_events_since_checked(
        &self,
        targets: &[DurableWatchTarget],
        since_resource_version: i64,
        limit: Option<NonZeroUsize>,
    ) -> Result<RedbCheckedWatchRead<DurableWatchEvent>> {
        if targets.is_empty() {
            return Ok(RedbCheckedWatchRead::Events(Vec::new()));
        }
        let targets = targets.to_vec();
        self.call("redb-read:watch-events-since-checked", move |database| {
            let read = database.begin_read()?;
            if since_resource_version > 0
                && targets.iter().try_fold(false, |expired, target| {
                    Ok::<_, anyhow::Error>(
                        expired
                            || target_rv_floor(&read, target)?
                                .is_some_and(|floor| since_resource_version < floor),
                    )
                })?
            {
                return Ok(RedbCheckedWatchRead::Expired);
            }
            parsed_watch_events_in_read(
                &read,
                &targets,
                since_resource_version,
                None,
                None,
                limit.map(NonZeroUsize::get),
            )
            .map(RedbCheckedWatchRead::Events)
        })
        .await
    }

    pub async fn positioned_watch_events(
        &self,
        targets: &[DurableWatchTarget],
        position: WatchReplayPosition,
        limit: NonZeroUsize,
    ) -> Result<RedbPositionedWatchRead<DurableWatchEvent>> {
        let targets = targets.to_vec();
        self.call("redb-read:positioned-watch-events", move |database| {
            let read = database.begin_read()?;
            let current = replay_position_in_read(&read)?;
            if position.event_id > current.event_id
                || (position.event_id == 0 && position.resource_version > current.resource_version)
                || position_expired_for_targets(&read, &targets, position)?
            {
                return Ok(RedbPositionedWatchRead::Expired);
            }
            let table = read.open_table(tables::WATCH_EVENTS)?;
            let start = if position.event_id == 0 {
                0
            } else {
                position.event_id.saturating_add(1).max(0) as u64
            };
            let filter_through = if position.resource_version_filter_through_event_id > 0 {
                position.resource_version_filter_through_event_id
            } else if position.event_id == 0 {
                i64::MAX
            } else {
                0
            };
            let mut events = Vec::with_capacity(limit.get().min(4096));
            for entry in table.range(start..=current.event_id.max(0) as u64)? {
                let (event_id, encoded) = entry?;
                let stored: StoredWatchEvent<'_> = serde_json::from_slice(encoded.value())
                    .map_err(|error| anyhow!("malformed persisted watch event JSON: {error}"))?;
                let resource_version = stored.resource_version.unwrap_or_default();
                if event_id.value() as i64 <= filter_through
                    && resource_version <= position.resource_version
                {
                    continue;
                }
                if !targets.iter().any(|target| stored_matches(target, &stored)) {
                    continue;
                }
                events.push(PositionedWatchEvent {
                    position: WatchReplayPosition {
                        resource_version,
                        event_id: event_id.value() as i64,
                        resource_version_filter_through_event_id: 0,
                    },
                    event: durable_event_from_stored(stored)?,
                });
                if events.len() == limit.get() {
                    break;
                }
            }
            let next_position =
                WatchReplayPosition::after_page(position, &events, current.event_id, limit);
            Ok(RedbPositionedWatchRead::Events(RedbPositionedWatchPage {
                events,
                next_position,
            }))
        })
        .await
    }

    pub async fn raw_watch_events_since_checked(
        &self,
        targets: &[DurableWatchTarget],
        since_resource_version: i64,
        limit: NonZeroUsize,
    ) -> Result<RedbCheckedWatchRead<DurableRawWatchEvent>> {
        if targets.is_empty() {
            return Ok(RedbCheckedWatchRead::Events(Vec::new()));
        }
        let targets = targets.to_vec();
        self.call("redb-read:raw-watch-events-since", move |database| {
            let read = database.begin_read()?;
            if since_resource_version > 0
                && targets.iter().try_fold(false, |expired, target| {
                    Ok::<_, anyhow::Error>(
                        expired
                            || target_rv_floor(&read, target)?
                                .is_some_and(|floor| since_resource_version < floor),
                    )
                })?
            {
                return Ok(RedbCheckedWatchRead::Expired);
            }
            let table = read.open_table(tables::WATCH_EVENTS)?;
            let mut events = Vec::with_capacity(limit.get().min(4096));
            for entry in table.iter()? {
                let (_, encoded) = entry?;
                let Ok(stored) = serde_json::from_slice::<StoredWatchEvent<'_>>(encoded.value())
                else {
                    continue;
                };
                if stored.resource_version.unwrap_or_default() <= since_resource_version
                    || !targets.iter().any(|target| stored_matches(target, &stored))
                {
                    continue;
                }
                events.push(raw_event_from_stored(stored));
                if events.len() == limit.get() {
                    break;
                }
            }
            Ok(RedbCheckedWatchRead::Events(events))
        })
        .await
    }

    pub async fn positioned_raw_watch_events(
        &self,
        targets: &[DurableWatchTarget],
        position: WatchReplayPosition,
        limit: NonZeroUsize,
    ) -> Result<RedbPositionedWatchRead<DurableRawWatchEvent>> {
        let targets = targets.to_vec();
        self.call("redb-read:positioned-raw-watch-events", move |database| {
            let read = database.begin_read()?;
            let current = replay_position_in_read(&read)?;
            if position.event_id > current.event_id
                || (position.event_id == 0 && position.resource_version > current.resource_version)
                || position_expired_for_targets(&read, &targets, position)?
            {
                return Ok(RedbPositionedWatchRead::Expired);
            }
            let table = read.open_table(tables::WATCH_EVENTS)?;
            let start = if position.event_id == 0 {
                0
            } else {
                position.event_id.saturating_add(1).max(0) as u64
            };
            let filter_through = if position.resource_version_filter_through_event_id > 0 {
                position.resource_version_filter_through_event_id
            } else if position.event_id == 0 {
                i64::MAX
            } else {
                0
            };
            let mut events = Vec::with_capacity(limit.get().min(4096));
            for entry in table.range(start..=current.event_id.max(0) as u64)? {
                let (event_id, encoded) = entry?;
                let Ok(stored) = serde_json::from_slice::<StoredWatchEvent<'_>>(encoded.value())
                else {
                    continue;
                };
                let resource_version = stored.resource_version.unwrap_or_default();
                if (event_id.value() as i64) <= filter_through
                    && resource_version <= position.resource_version
                {
                    continue;
                }
                if !targets.iter().any(|target| stored_matches(target, &stored)) {
                    continue;
                }
                events.push(PositionedWatchEvent {
                    position: WatchReplayPosition {
                        resource_version,
                        event_id: event_id.value() as i64,
                        resource_version_filter_through_event_id: 0,
                    },
                    event: raw_event_from_stored(stored),
                });
                if events.len() == limit.get() {
                    break;
                }
            }
            let next_position =
                WatchReplayPosition::after_page(position, &events, current.event_id, limit);
            Ok(RedbPositionedWatchRead::Events(RedbPositionedWatchPage {
                events,
                next_position,
            }))
        })
        .await
    }

    pub async fn all_watch_events_since(
        &self,
        since_resource_version: i64,
        deleted_only: bool,
    ) -> Result<Vec<DurableWatchEvent>> {
        self.call("redb-read:all-watch-events-since", move |database| {
            let read = database.begin_read()?;
            parsed_watch_events_in_read(
                &read,
                &[],
                since_resource_version,
                deleted_only.then_some("DELETED"),
                None,
                None,
            )
        })
        .await
    }

    pub async fn all_watch_events_since_paged(
        &self,
        since_resource_version: i64,
        after_event_id: i64,
        through_event_id: Option<i64>,
        limit: NonZeroUsize,
    ) -> Result<Vec<(i64, DurableWatchEvent)>> {
        self.call("redb-read:all-watch-events-paged", move |database| {
            let read = database.begin_read()?;
            let table = read.open_table(tables::WATCH_EVENTS)?;
            let start = after_event_id.saturating_add(1).max(0) as u64;
            let mut events = Vec::with_capacity(limit.get().min(4096));
            for entry in table.range(start..)? {
                let (event_id, encoded) = entry?;
                let event_id = event_id.value() as i64;
                if through_event_id.is_some_and(|through| event_id > through) {
                    break;
                }
                let stored: StoredWatchEvent<'_> = serde_json::from_slice(encoded.value())
                    .unwrap_or(StoredWatchEvent {
                        api_version: None,
                        kind: None,
                        namespace: None,
                        name: None,
                        event_type: None,
                        resource_version: None,
                        data: None,
                    });
                if stored.resource_version.unwrap_or_default() <= since_resource_version {
                    continue;
                }
                events.push((event_id, durable_event_from_stored(stored)?));
                if events.len() == limit.get() {
                    break;
                }
            }
            Ok(events)
        })
        .await
    }

    pub async fn replay_floors(&self) -> Result<Vec<DurableReplayFloor>> {
        self.call("redb-read:replay-floors", |database| {
            let read = database.begin_read()?;
            let table = read.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
            let mut floors = Vec::new();
            for entry in table.iter()? {
                let (key, value) = entry?;
                if let Some(floor) = decode_durable_floor(key.value(), value.value())? {
                    floors.push(floor);
                }
            }
            floors.sort_by(|left, right| {
                replay_target_sort_key(left.target()).cmp(&replay_target_sort_key(right.target()))
            });
            Ok(floors)
        })
        .await
    }

    pub async fn replay_floors_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotReplayFloorCursor>,
        limit: NonZeroUsize,
    ) -> Result<Vec<DurableReplayFloor>> {
        if limit.get() > klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
            return Err(anyhow!(
                "watch replay-floor page limit {} exceeds {}",
                limit,
                klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE
            ));
        }
        let after = after.map(|cursor| match cursor.target() {
            klights_cluster_store::DurableReplayTarget::All => floor_key("*", "*", "*"),
            klights_cluster_store::DurableReplayTarget::Cluster { api_version, kind } => {
                floor_key(api_version, kind, CLUSTER_NAMESPACE_KEY)
            }
            klights_cluster_store::DurableReplayTarget::Namespaced {
                api_version,
                kind,
                namespace,
            } => floor_key(api_version, kind, namespace),
        });
        self.call("redb-read:replay-floors-paged", move |database| {
            let read = database.begin_read()?;
            let table = read.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
            let mut floors = Vec::with_capacity(limit.get());
            if let Some(after) = after {
                for entry in table.range(after.as_slice()..)? {
                    let (key, value) = entry?;
                    if key.value() <= after.as_slice() {
                        continue;
                    }
                    if let Some(floor) = decode_durable_floor(key.value(), value.value())? {
                        floors.push(floor);
                        if floors.len() == limit.get() {
                            break;
                        }
                    }
                }
            } else {
                for entry in table.iter()? {
                    let (key, value) = entry?;
                    if let Some(floor) = decode_durable_floor(key.value(), value.value())? {
                        floors.push(floor);
                        if floors.len() == limit.get() {
                            break;
                        }
                    }
                }
            }
            Ok(floors)
        })
        .await
    }
}

fn parsed_watch_events_in_read(
    read: &redb::ReadTransaction,
    targets: &[DurableWatchTarget],
    since_resource_version: i64,
    required_event_type: Option<&str>,
    start_event_id: Option<i64>,
    limit: Option<usize>,
) -> Result<Vec<DurableWatchEvent>> {
    let table = read.open_table(tables::WATCH_EVENTS)?;
    let mut events = Vec::with_capacity(limit.unwrap_or_default().min(4096));
    let start = start_event_id.unwrap_or_default().max(0) as u64;
    for entry in table.range(start..)? {
        let (_, encoded) = entry?;
        let stored: StoredWatchEvent<'_> =
            serde_json::from_slice(encoded.value()).unwrap_or(StoredWatchEvent {
                api_version: None,
                kind: None,
                namespace: None,
                name: None,
                event_type: None,
                resource_version: None,
                data: None,
            });
        if stored.resource_version.unwrap_or_default() <= since_resource_version
            || required_event_type.is_some_and(|required| stored.event_type != Some(required))
            || (!targets.is_empty()
                && !targets.iter().any(|target| stored_matches(target, &stored)))
        {
            continue;
        }
        events.push(durable_event_from_stored(stored)?);
        if limit.is_some_and(|limit| events.len() == limit) {
            break;
        }
    }
    Ok(events)
}

fn stored_matches(target: &DurableWatchTarget, stored: &StoredWatchEvent<'_>) -> bool {
    target.api_version() == stored.api_version.unwrap_or_default()
        && target.kind() == stored.kind.unwrap_or_default()
        && target_matches_namespace(target, stored.namespace)
}

fn target_matches_namespace(target: &DurableWatchTarget, namespace: Option<&str>) -> bool {
    match target.scope() {
        DurableWatchScope::Cluster => namespace.is_none(),
        DurableWatchScope::Namespaced(Some(expected)) => namespace == Some(expected),
        DurableWatchScope::Namespaced(None) => namespace.is_some(),
    }
}

fn durable_event_from_stored(stored: StoredWatchEvent<'_>) -> Result<DurableWatchEvent> {
    let data = stored
        .data
        .map(|raw| serde_json::from_str(raw.get()))
        .transpose()?
        .unwrap_or(Value::Null);
    let resource = Resource {
        id: 0,
        api_version: stored.api_version.unwrap_or_default().to_string(),
        kind: stored.kind.unwrap_or_default().to_string(),
        namespace: stored.namespace.map(str::to_string),
        name: stored.name.unwrap_or_default().to_string(),
        uid: Resource::uid_from_data(&data),
        resource_version: stored.resource_version.unwrap_or_default(),
        data: Arc::new(data),
    };
    Ok(DurableWatchEvent::new(
        stored.event_type.unwrap_or_default().to_string(),
        resource,
    ))
}

fn raw_event_from_stored(stored: StoredWatchEvent<'_>) -> DurableRawWatchEvent {
    DurableRawWatchEvent {
        api_version: stored.api_version.unwrap_or_default().to_string(),
        kind: stored.kind.unwrap_or_default().to_string(),
        namespace: stored.namespace.map(str::to_string),
        name: stored.name.unwrap_or_default().to_string(),
        resource_version: stored.resource_version.unwrap_or_default(),
        event_type: Cow::Owned(stored.event_type.unwrap_or_default().to_string()),
        object_json: stored.data.map_or_else(
            || Bytes::from_static(b"null"),
            |data| Bytes::copy_from_slice(data.get().as_bytes()),
        ),
    }
}

fn target_rv_floor(
    read: &redb::ReadTransaction,
    target: &DurableWatchTarget,
) -> Result<Option<i64>> {
    let table = read.open_table(tables::WATCH_REPLAY_FLOORS)?;
    let scoped = match target.scope() {
        DurableWatchScope::Cluster => table
            .get(floor_key(target.api_version(), target.kind(), CLUSTER_NAMESPACE_KEY).as_slice())?
            .map(|value| value.value() as i64),
        DurableWatchScope::Namespaced(Some(namespace)) => table
            .get(floor_key(target.api_version(), target.kind(), namespace).as_slice())?
            .map(|value| value.value() as i64),
        DurableWatchScope::Namespaced(None) => {
            namespaced_floor(read, target.api_version(), target.kind(), false)?
        }
    };
    Ok(LegacyReplayFloor::read(read)?
        .and_then(|legacy| legacy.merge_resource_version(scoped))
        .or(scoped))
}

/// Returns the strongest real resourceVersion retention boundary across the
/// collections participating in a positioned read. A cursor beyond an
/// uncompacted head reports that observed head, never a fabricated zero.
fn oldest_available_for_targets(
    read: &redb::ReadTransaction,
    targets: &[DurableWatchTarget],
    current_resource_version: i64,
) -> Result<i64> {
    let mut oldest = None;
    for target in targets {
        if let Some(floor) = target_rv_floor(read, target)? {
            oldest = Some(oldest.map_or(floor, |current: i64| current.max(floor)));
        }
    }
    Ok(oldest.unwrap_or(current_resource_version))
}

fn position_expired_for_targets(
    read: &redb::ReadTransaction,
    targets: &[DurableWatchTarget],
    position: WatchReplayPosition,
) -> Result<bool> {
    for target in targets {
        let floor = if position.event_id == 0 {
            target_rv_floor(read, target)?
        } else {
            let table = read.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
            let scoped = match target.scope() {
                DurableWatchScope::Cluster => table
                    .get(
                        floor_key(target.api_version(), target.kind(), CLUSTER_NAMESPACE_KEY)
                            .as_slice(),
                    )?
                    .and_then(|value| decode_position(value.value()).map(|(_, event_id)| event_id)),
                DurableWatchScope::Namespaced(Some(namespace)) => table
                    .get(floor_key(target.api_version(), target.kind(), namespace).as_slice())?
                    .and_then(|value| decode_position(value.value()).map(|(_, event_id)| event_id)),
                DurableWatchScope::Namespaced(None) => {
                    namespaced_floor(read, target.api_version(), target.kind(), true)?
                }
            };
            LegacyReplayFloor::read(read)?
                .and_then(|legacy| legacy.merge_event_id(scoped))
                .or(scoped)
        };
        let cursor = if position.event_id == 0 {
            position.resource_version
        } else {
            position.event_id
        };
        if floor.is_some_and(|floor| cursor < floor) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn namespaced_floor(
    read: &redb::ReadTransaction,
    api_version: &str,
    kind: &str,
    positioned: bool,
) -> Result<Option<i64>> {
    let prefix = floor_prefix(api_version, kind);
    let mut floor = None;
    if positioned {
        let table = read.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
        for entry in table.iter()? {
            let (key, value) = entry?;
            let Some(namespace) = key.value().strip_prefix(prefix.as_slice()) else {
                continue;
            };
            if namespace.is_empty() || namespace == CLUSTER_NAMESPACE_KEY.as_bytes() {
                continue;
            }
            if let Some((_, candidate)) = decode_position(value.value()) {
                floor = Some(floor.map_or(candidate, |current: i64| current.max(candidate)));
            }
        }
    } else {
        let table = read.open_table(tables::WATCH_REPLAY_FLOORS)?;
        for entry in table.iter()? {
            let (key, value) = entry?;
            let Some(namespace) = key.value().strip_prefix(prefix.as_slice()) else {
                continue;
            };
            if namespace.is_empty() || namespace == CLUSTER_NAMESPACE_KEY.as_bytes() {
                continue;
            }
            let candidate = value.value() as i64;
            floor = Some(floor.map_or(candidate, |current: i64| current.max(candidate)));
        }
    }
    Ok(floor)
}

fn floor_prefix(api_version: &str, kind: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(api_version.len() + kind.len() + 2);
    key.extend_from_slice(api_version.as_bytes());
    key.push(0);
    key.extend_from_slice(kind.as_bytes());
    key.push(0);
    key
}

fn floor_key(api_version: &str, kind: &str, namespace: &str) -> Vec<u8> {
    let mut key = floor_prefix(api_version, kind);
    key.extend_from_slice(namespace.as_bytes());
    key
}

fn replay_target_sort_key(target: &DurableReplayTarget) -> (&str, &str, &str) {
    match target {
        DurableReplayTarget::All => ("*", "*", "*"),
        DurableReplayTarget::Cluster { api_version, kind } => {
            (api_version, kind, CLUSTER_NAMESPACE_KEY)
        }
        DurableReplayTarget::Namespaced {
            api_version,
            kind,
            namespace,
        } => (api_version, kind, namespace),
    }
}

fn decode_position(encoded: &[u8]) -> Option<(i64, i64)> {
    (encoded.len() == 16).then(|| {
        (
            i64::try_from(u64::from_be_bytes(
                encoded[..8].try_into().expect("fixed floor prefix"),
            ))
            .unwrap_or(i64::MAX),
            i64::try_from(u64::from_be_bytes(
                encoded[8..].try_into().expect("fixed floor suffix"),
            ))
            .unwrap_or(i64::MAX),
        )
    })
}

fn decode_floor_key(key: &[u8]) -> Option<(String, String, String)> {
    let mut parts = key.splitn(3, |byte| *byte == 0);
    Some((
        String::from_utf8_lossy(parts.next()?).into_owned(),
        String::from_utf8_lossy(parts.next()?).into_owned(),
        String::from_utf8_lossy(parts.next()?).into_owned(),
    ))
}

fn decode_durable_floor(key: &[u8], value: &[u8]) -> Result<Option<DurableReplayFloor>> {
    let Some((api_version, kind, namespace)) = decode_floor_key(key) else {
        return Ok(None);
    };
    let Some((resource_version, event_id)) = decode_position(value) else {
        return Ok(None);
    };
    let floor = if api_version == "*" && kind == "*" && namespace == "*" {
        DurableReplayFloor::all(resource_version, event_id, true)
    } else if namespace == CLUSTER_NAMESPACE_KEY {
        DurableReplayFloor::cluster(api_version, kind, resource_version, event_id, true)
    } else {
        DurableReplayFloor::namespaced(
            api_version,
            kind,
            namespace,
            resource_version,
            event_id,
            true,
        )
    }
    .map_err(|error| anyhow!(error.to_string()))?;
    Ok(Some(floor))
}

impl RedbReadCore {
    pub async fn snapshot_at_position(
        &self,
        targets: &[DurableWatchTarget],
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        position: WatchReplayPosition,
    ) -> Result<RedbSnapshotRead> {
        let targets = targets.to_vec();
        let sort_targets = targets.clone();
        let labels = label_selector.map(str::to_string);
        let fields = field_selector.map(str::to_string);
        self.call("redb-read:snapshot-at-position", move |database| {
            let read = database.begin_read()?;
            let current_position = replay_position_in_read(&read)?;
            if position.event_id > current_position.event_id
                || position.resource_version_filter_through_event_id > current_position.event_id
                || (position.event_id == 0
                    && position.resource_version_filter_through_event_id == 0
                    && position.resource_version > current_position.resource_version)
            {
                return Ok(RedbSnapshotRead::Expired {
                    oldest_available: current_position.resource_version,
                });
            }
            let current = read_current_targets(&read, &targets)?;
            let raw = if cursor_covers_current(position, current_position) {
                ReconstructedMembership::Items(current)
            } else if position_expired_for_targets(&read, &targets, position)? {
                ReconstructedMembership::Expired
            } else {
                reconstruct_membership_in_read(&read, &targets, current, position)?
            };
            let ReconstructedMembership::Items(mut items) = raw else {
                return Ok(RedbSnapshotRead::Expired {
                    oldest_available: oldest_available_for_targets(
                        &read,
                        &targets,
                        current_position.resource_version,
                    )?,
                });
            };
            apply_selectors(&mut items, labels.as_deref(), fields.as_deref())?;
            sort_for_targets(&mut items, &sort_targets);
            Ok(RedbSnapshotRead::Historical { items, position })
        })
        .await
    }

    pub async fn get_node_subnet(&self, node_name: &str) -> Result<Option<StoredNodeSubnet>> {
        let node_name = node_name.to_string();
        self.call("redb-read:get-node-subnet", move |database| {
            let read = database.begin_read()?;
            let table = read.open_table(tables::NODE_SUBNETS)?;
            table
                .get(node_name.as_str())?
                .map(|value| parse_node_subnet(&node_name, value.value()))
                .transpose()
        })
        .await
    }

    pub async fn list_peer_subnets(
        &self,
        request: PeerTopologyRequest,
    ) -> Result<Vec<StoredNodeSubnet>> {
        let excluded = request
            .excluded_node_name()
            .map(|name| name.as_str().to_string());
        self.call("redb-read:list-peer-subnets", move |database| {
            let read = database.begin_read()?;
            let table = read.open_table(tables::NODE_SUBNETS)?;
            let mut items = Vec::new();
            for entry in table.iter()? {
                let (name, value) = entry?;
                if (excluded.is_none() && name.value().is_empty())
                    || excluded.as_deref() == Some(name.value())
                {
                    continue;
                }
                items.push(parse_node_subnet(name.value(), value.value())?);
            }
            Ok(items)
        })
        .await
    }

    pub async fn get_node_dataplane(
        &self,
        node_name: &str,
    ) -> Result<Option<DataplanePeerMetadata>> {
        let node_name = node_name.to_string();
        self.call("redb-read:get-node-dataplane", move |database| {
            let read = database.begin_read()?;
            let table = match read.open_table(tables::NODE_DATAPLANE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            let Some(value) = table.get(node_name.as_str())? else {
                return Ok(None);
            };
            let body: Value = serde_json::from_slice(value.value())
                .map_err(|error| anyhow!("malformed persisted node dataplane JSON: {error}"))?;
            let mode = required_string(&body, "node dataplane", "mode")?;
            let encryption = required_string(&body, "node dataplane", "encryption")?;
            let public_key = optional_string(&body, "node dataplane", "public_key")?;
            let endpoint = optional_string(&body, "node dataplane", "endpoint")?;
            let port = optional_port(&body, "port")?;
            Ok(Some(DataplanePeerMetadata::try_new(
                node_name,
                DataplaneMode::parse(mode)?,
                DataplaneEncryption::parse(Some(encryption))?,
                public_key,
                endpoint,
                port,
            )?))
        })
        .await
    }
}

enum RedbHistoricalCandidates {
    Items(Vec<ResourceCollectionKey>),
    Expired { oldest_available: i64 },
}

enum RedbHistoricalIdentity {
    Resource(Resource),
    Absent,
    Expired { oldest_available: i64 },
}

enum RedbHistoricalHistory {
    Events {
        current: Option<Resource>,
        events: Vec<MembershipHistoryEvent>,
    },
    Expired {
        oldest_available: i64,
    },
}

fn durable_target_for_scope(
    api_version: &str,
    kind: &str,
    scope: &ResourceCollectionScope,
) -> DurableWatchTarget {
    match scope {
        ResourceCollectionScope::Cluster => DurableWatchTarget::cluster(api_version, kind),
        ResourceCollectionScope::AllNamespaces => DurableWatchTarget::namespaced(api_version, kind),
        ResourceCollectionScope::Namespace(namespace) => {
            DurableWatchTarget::namespaced_in_namespace(api_version, kind, namespace)
        }
    }
}

fn history_prefix(
    api_version: &str,
    kind: &str,
    scope: &ResourceCollectionScope,
) -> (Vec<u8>, bool) {
    match scope {
        ResourceCollectionScope::Cluster => {
            let mut prefix = vec![b'C'];
            push_history_component(&mut prefix, api_version);
            push_history_component(&mut prefix, kind);
            push_history_component(&mut prefix, "");
            (prefix, false)
        }
        ResourceCollectionScope::AllNamespaces => {
            let mut prefix = vec![b'N'];
            push_history_component(&mut prefix, api_version);
            push_history_component(&mut prefix, kind);
            (prefix, true)
        }
        ResourceCollectionScope::Namespace(namespace) => {
            let mut prefix = vec![b'N'];
            push_history_component(&mut prefix, api_version);
            push_history_component(&mut prefix, kind);
            push_history_component(&mut prefix, namespace);
            (prefix, true)
        }
    }
}

fn history_identity_prefix(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    namespaced: bool,
) -> Vec<u8> {
    let mut key = vec![if namespaced { b'N' } else { b'C' }];
    push_history_component(&mut key, api_version);
    push_history_component(&mut key, kind);
    push_history_component(&mut key, namespace.unwrap_or_default());
    push_history_component(&mut key, name);
    key
}

fn list_current_identity_page_in_read(
    read: &redb::ReadTransaction,
    api_version: &str,
    kind: &str,
    scope: &RedbCollectionScope,
    limit: Option<usize>,
    cursor: Option<ResourceCollectionKey>,
    position: WatchReplayPosition,
) -> Result<RedbResourceList> {
    let logical_scope = match scope {
        RedbCollectionScope::Cluster => ResourceCollectionScope::Cluster,
        RedbCollectionScope::AllNamespaces => ResourceCollectionScope::AllNamespaces,
        RedbCollectionScope::Namespace(namespace) => {
            ResourceCollectionScope::Namespace(namespace.clone())
        }
        RedbCollectionScope::LegacyAny => {
            unreachable!("legacy path does not use current identity index")
        }
    };
    let (prefix, namespaced) = history_prefix(api_version, kind, &logical_scope);
    let end = lex_next(&prefix).unwrap_or_else(|| vec![0xff]);
    let start = cursor
        .as_ref()
        .map(|after| {
            history_identity_prefix(
                api_version,
                kind,
                after.namespace(),
                after.name(),
                namespaced,
            )
        })
        .and_then(|key| lex_next(&key))
        .unwrap_or(prefix);
    let probe = limit.map_or(usize::MAX, |value| value.saturating_add(1));
    let index = read.open_table(tables::RESOURCE_CURRENT_BY_IDENTITY)?;
    let mut items = Vec::with_capacity(probe.min(64));
    for entry in index.range(start.as_slice()..end.as_slice())? {
        if items.len() == probe {
            break;
        }
        let (key, _) = entry?;
        let mut synthetic_history_key = key.value().to_vec();
        synthetic_history_key.extend_from_slice(&0_u64.to_be_bytes());
        let Some((namespace, name, _)) =
            decode_history_identity(&synthetic_history_key, namespaced)
        else {
            return Err(anyhow!("malformed current resource identity key"));
        };
        if let Some(resource) =
            read_current_identity(read, api_version, kind, namespace.as_deref(), &name)?
        {
            items.push(resource);
        }
    }
    let has_more = limit.is_some_and(|value| items.len() > value);
    if let Some(limit) = limit {
        items.truncate(limit);
    }
    let continuation = has_more.then(|| {
        let item = items.last().expect("positive page has a final identity");
        ResourceCollectionKey::new(item.namespace.clone(), item.name.clone())
    });
    Ok(RedbResourceList {
        items,
        position,
        continuation,
        remaining_item_count: None,
    })
}

fn push_history_component(key: &mut Vec<u8>, value: &str) {
    key.extend_from_slice(value.as_bytes());
    key.push(0);
}

fn decode_history_identity(
    encoded: &[u8],
    namespaced: bool,
) -> Option<(Option<String>, String, usize)> {
    let identity_end = encoded.len().checked_sub(8)?;
    let identity = encoded.get(..identity_end)?;
    let (&scope, encoded_components) = identity.split_first()?;
    if (scope == b'N') != namespaced {
        return None;
    }
    let mut parts = encoded_components.split(|byte| *byte == 0);
    let api_version = std::str::from_utf8(parts.next()?).ok()?;
    let kind = std::str::from_utf8(parts.next()?).ok()?;
    let namespace = std::str::from_utf8(parts.next()?).ok()?;
    let name = std::str::from_utf8(parts.next()?).ok()?;
    if api_version.is_empty() || kind.is_empty() || name.is_empty() || parts.next().is_none() {
        return None;
    }
    Some((
        namespaced.then(|| namespace.to_string()),
        name.to_string(),
        identity_end,
    ))
}

fn read_current_identity(
    read: &redb::ReadTransaction,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
) -> Result<Option<Resource>> {
    if api_version == "v1" && kind == "Namespace" && namespace.is_none() {
        let table = read.open_table(tables::NAMESPACES)?;
        return Ok(table
            .get(name)?
            .map(|body| namespace_from_body(name, body.value())));
    }
    let table = read.open_table(if namespace.is_some() {
        tables::RES_NS
    } else {
        tables::RES_CLUSTER
    })?;
    let key = resource_key(api_version, kind, namespace, name);
    Ok(table.get(key.as_slice())?.map(|value| {
        let (resource_version, body) = value.value();
        resource_from_body(
            api_version,
            kind,
            namespace.map(str::to_string),
            name,
            resource_version as i64,
            body,
        )
    }))
}

fn historical_position_unavailable(
    position: WatchReplayPosition,
    current: WatchReplayPosition,
) -> bool {
    position.event_id > current.event_id
        || position.resource_version_filter_through_event_id > current.event_id
        || (position.event_id == 0
            && position.resource_version_filter_through_event_id == 0
            && position.resource_version > current.resource_version)
}

fn cursor_covers_current(position: WatchReplayPosition, current: WatchReplayPosition) -> bool {
    position.event_id >= current.event_id
        || (position.resource_version_filter_through_event_id >= current.event_id
            && position.resource_version >= current.resource_version)
        || (position.event_id == 0
            && position.resource_version_filter_through_event_id == 0
            && position.resource_version >= current.resource_version)
}

fn read_current_targets(
    read: &redb::ReadTransaction,
    targets: &[DurableWatchTarget],
) -> Result<Vec<Resource>> {
    let mut resources = Vec::new();
    for target in targets {
        if target.api_version() == "v1"
            && target.kind() == "Namespace"
            && matches!(target.scope(), DurableWatchScope::Cluster)
        {
            let table = read.open_table(tables::NAMESPACES)?;
            for entry in table.iter()? {
                let (name, body) = entry?;
                let data: Value = serde_json::from_slice(body.value())?;
                let resource_version = data
                    .pointer("/metadata/resourceVersion")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse().ok())
                    .unwrap_or_default();
                resources.push(resource_from_data(
                    "v1",
                    "Namespace",
                    resource_version,
                    data,
                ));
                if let Some(last) = resources.last_mut() {
                    last.name = name.value().to_string();
                }
            }
            continue;
        }
        let namespaced = !matches!(target.scope(), DurableWatchScope::Cluster);
        let table = read.open_table(if namespaced {
            tables::RES_NS
        } else {
            tables::RES_CLUSTER
        })?;
        for entry in table.iter()? {
            let (key, value) = entry?;
            let (resource_version, body) = value.value();
            let Some((api_version, kind, namespace, name)) =
                decode_resource_key(key.value(), namespaced)
            else {
                continue;
            };
            if target.api_version() == api_version
                && target.kind() == kind
                && target_matches_namespace(target, namespace)
            {
                resources.push(resource_from_body(
                    api_version,
                    kind,
                    namespace.map(str::to_string),
                    name,
                    resource_version as i64,
                    body,
                ));
            }
        }
    }
    Ok(resources)
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct MembershipKey {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    name: String,
}

impl MembershipKey {
    fn from_resource(resource: &Resource) -> Self {
        Self {
            api_version: resource.api_version.clone(),
            kind: resource.kind.clone(),
            namespace: resource.namespace.clone(),
            name: resource.name.clone(),
        }
    }
}

struct MembershipHistoryEvent {
    event_id: i64,
    event_type: String,
    resource: Resource,
}

enum ReconstructedMembership {
    Expired,
    Items(Vec<Resource>),
}

struct MembershipReconstructor {
    state: BTreeMap<MembershipKey, Resource>,
    needs_predecessor: HashSet<MembershipKey>,
    position: WatchReplayPosition,
    expired: bool,
}

impl MembershipReconstructor {
    fn new(current: Vec<Resource>, position: WatchReplayPosition) -> Self {
        Self {
            state: current
                .into_iter()
                .map(|resource| (MembershipKey::from_resource(&resource), resource))
                .collect(),
            needs_predecessor: HashSet::new(),
            position,
            expired: false,
        }
    }

    fn observe(&mut self, event: &MembershipHistoryEvent) {
        if self.expired {
            return;
        }
        let key = MembershipKey::from_resource(&event.resource);
        if self.needs_predecessor.remove(&key) {
            if event.event_type == "DELETED" {
                self.expired = true;
                return;
            }
            self.state.insert(key.clone(), event.resource.clone());
        }
        if self
            .position
            .represents_event(event.event_id, event.resource.resource_version)
        {
            return;
        }
        match event.event_type.as_str() {
            "ADDED" => {
                self.state.remove(&key);
            }
            "MODIFIED" => {
                self.needs_predecessor.insert(key);
            }
            "DELETED" => {
                self.state.insert(key, event.resource.clone());
            }
            _ => self.expired = true,
        }
    }

    fn can_stop_before(&self, event_id: i64) -> bool {
        self.position.event_id > 0
            && self.position.resource_version_filter_through_event_id == 0
            && event_id <= self.position.event_id
            && self.needs_predecessor.is_empty()
    }

    fn finish(self) -> ReconstructedMembership {
        if self.expired || !self.needs_predecessor.is_empty() {
            ReconstructedMembership::Expired
        } else {
            ReconstructedMembership::Items(self.state.into_values().collect())
        }
    }
}

fn reconstruct_membership_in_read(
    read: &redb::ReadTransaction,
    targets: &[DurableWatchTarget],
    current: Vec<Resource>,
    position: WatchReplayPosition,
) -> Result<ReconstructedMembership> {
    let table = read.open_table(tables::WATCH_EVENTS)?;
    let mut reconstructor = MembershipReconstructor::new(current, position);
    for entry in table.iter()?.rev() {
        let (event_id, encoded) = entry?;
        if reconstructor.can_stop_before(event_id.value() as i64) {
            break;
        }
        let stored: StoredWatchEvent<'_> = serde_json::from_slice(encoded.value())?;
        if !targets.iter().any(|target| stored_matches(target, &stored)) {
            continue;
        }
        let event = durable_event_from_stored(stored)?;
        reconstructor.observe(&MembershipHistoryEvent {
            event_id: event_id.value() as i64,
            event_type: event.event_type().to_string(),
            resource: event.into_resource(),
        });
    }
    Ok(reconstructor.finish())
}

fn apply_selectors(
    items: &mut Vec<Resource>,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
) -> Result<()> {
    let labels = label_selector
        .filter(|selector| !selector.trim().is_empty())
        .map(klights_types::LabelSelector::parse)
        .transpose()?;
    let fields = field_selector
        .filter(|selector| !selector.trim().is_empty())
        .map(klights_types::FieldSelector::parse)
        .transpose()?;
    items.retain(|resource| {
        labels
            .as_ref()
            .is_none_or(|selector| selector.matches_resource(resource.data.as_ref()))
            && fields.as_ref().is_none_or(|selector| {
                selector.matches_resource_with_identity(
                    &resource.api_version,
                    &resource.kind,
                    resource.data.as_ref(),
                )
            })
    });
    Ok(())
}

fn sort_for_targets(items: &mut [Resource], targets: &[DurableWatchTarget]) {
    items.sort_unstable_by(|left, right| {
        let order = |resource: &Resource| {
            targets
                .iter()
                .position(|target| {
                    target.api_version() == resource.api_version
                        && target.kind() == resource.kind
                        && target_matches_namespace(target, resource.namespace.as_deref())
                })
                .unwrap_or(usize::MAX)
        };
        (order(left), &left.namespace, &left.name).cmp(&(
            order(right),
            &right.namespace,
            &right.name,
        ))
    });
}

fn parse_node_subnet(name: &str, body: &[u8]) -> Result<StoredNodeSubnet> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|error| anyhow!("malformed persisted node subnet JSON: {error}"))?;
    let subnet_text = required_string(&value, "node subnet", "subnet")?;
    let subnet = PodSubnet::parse(subnet_text).map_err(|error| anyhow!("bad subnet: {error}"))?;
    if subnet.prefix() != 24 || subnet.to_string() != subnet_text {
        return Err(anyhow!("persisted node subnet CIDR must be canonical /24"));
    }
    let subnet_base_int = required_u32(&value, "node subnet", "subnet_base_int")?;
    if subnet_base_int != subnet.base() {
        return Err(anyhow!(
            "persisted node subnet base integer does not match its CIDR"
        ));
    }
    let gateway_ip = required_string(&value, "node subnet", "vtep_ip")?
        .parse::<Ipv4Addr>()
        .map_err(|error| anyhow!("bad vtep_ip: {error}"))?;
    if gateway_ip != subnet.base_ip() {
        return Err(anyhow!(
            "persisted node subnet gateway compatibility field does not match its CIDR"
        ));
    }
    let node_ip = required_string(&value, "node subnet", "node_ip")?
        .parse()
        .map_err(|error| anyhow!("bad node_ip: {error}"))?;
    let mode = match value.get("mode") {
        None => NodePeerMode::Root,
        Some(Value::String(mode)) if mode == "root" => NodePeerMode::Root,
        Some(Value::String(mode)) if mode == "rootless" => NodePeerMode::Rootless,
        Some(Value::String(mode)) => {
            return Err(anyhow!("unknown persisted node peer mode {mode:?}"));
        }
        Some(_) => return Err(anyhow!("node subnet field mode must be a string")),
    };
    let hostport_range = match value.get("hostport_range") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(
            HostPortRange::parse(value)
                .map_err(|error| anyhow!("invalid persisted hostport_range {value:?}: {error}"))?,
        ),
        Some(_) => {
            return Err(anyhow!(
                "node subnet field hostport_range must be a string or null"
            ));
        }
    };
    match (mode, hostport_range) {
        (NodePeerMode::Root, Some(_)) => {
            return Err(anyhow!(
                "persisted root node subnet must not carry a host-port range"
            ));
        }
        (NodePeerMode::Rootless, None) => {
            return Err(anyhow!(
                "persisted rootless node subnet requires a host-port range"
            ));
        }
        _ => {}
    }
    Ok(StoredNodeSubnet {
        node_name: NodeName::parse(name).map_err(|error| anyhow!("bad node name: {error}"))?,
        subnet,
        subnet_base_int,
        gateway_ip,
        node_ip,
        mode,
        hostport_range,
    })
}

fn required_string<'a>(value: &'a Value, context: &str, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{context} field {field} must be a string"))
}

fn optional_string(value: &Value, context: &str, field: &str) -> Result<Option<String>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(anyhow!("{context} field {field} must be a string or null")),
    }
}

fn required_u32(value: &Value, context: &str, field: &str) -> Result<u32> {
    let value = value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("{context} field {field} must be an unsigned integer"))?;
    u32::try_from(value).map_err(|_| anyhow!("{context} field {field} exceeds u32"))
}

fn optional_port(value: &Value, field: &str) -> Result<Option<u16>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => {
            let port = value
                .as_u64()
                .and_then(|value| u16::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or_else(|| anyhow!("node dataplane field {field} must be a non-zero u16"))?;
            Ok(Some(port))
        }
        Some(_) => Err(anyhow!(
            "node dataplane field {field} must be an integer or null"
        )),
    }
}
