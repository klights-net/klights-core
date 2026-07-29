//! `RedbWatchStore` — watch event history, catch-up, and GC.

use std::{collections::BTreeMap, sync::Arc};

use ::redb::{ReadableDatabase, ReadableTable};
use anyhow::Result;
use serde_json::Value;

use crate::datastore::types::*;
use klights_cluster_core::WatchReplayPosition;
use klights_cluster_datastore::redb::RedbAccessor;
use klights_cluster_datastore::redb::read_core::RedbCheckedWatchRead;
use klights_cluster_datastore::redb::read_core::RedbPositionedWatchRead;
use klights_cluster_datastore::redb::read_core::RedbReadCore;
use klights_cluster_datastore::redb::tables;

const CLUSTER_NAMESPACE_KEY: &str = "#cluster";
const DEFAULT_MIN_WATCH_EVENTS_PER_SCOPE: i64 = 1_024;
const MIN_SCOPE_COUNT_BEFORE_EXPIRING_SCOPES: usize = 16;

fn durable_targets(targets: &[WatchTarget]) -> Vec<klights_cluster_store::DurableWatchTarget> {
    targets
        .iter()
        .map(|target| match &target.scope {
            WatchTargetScope::Cluster => klights_cluster_store::DurableWatchTarget::cluster(
                &target.api_version,
                &target.kind,
            ),
            WatchTargetScope::Namespaced(None) => {
                klights_cluster_store::DurableWatchTarget::namespaced(
                    &target.api_version,
                    &target.kind,
                )
            }
            WatchTargetScope::Namespaced(Some(namespace)) => {
                klights_cluster_store::DurableWatchTarget::namespaced_in_namespace(
                    &target.api_version,
                    &target.kind,
                    namespace,
                )
            }
        })
        .collect()
}

fn durable_to_catchup(event: klights_cluster_store::DurableWatchEvent) -> CatchUpResource {
    let event_type = std::borrow::Cow::Owned(event.event_type().to_string());
    CatchUpResource {
        resource: event.into_resource(),
        event_type,
    }
}

fn durable_floor_to_legacy(floor: klights_cluster_store::DurableReplayFloor) -> WatchReplayFloor {
    let (target, floor_resource_version, floor_event_id, position_is_exact) = floor.into_parts();
    let (api_version, kind, namespace_key) = match target {
        klights_cluster_store::DurableReplayTarget::All => {
            ("*".to_string(), "*".to_string(), "*".to_string())
        }
        klights_cluster_store::DurableReplayTarget::Cluster { api_version, kind } => {
            (api_version, kind, CLUSTER_NAMESPACE_KEY.to_string())
        }
        klights_cluster_store::DurableReplayTarget::Namespaced {
            api_version,
            kind,
            namespace,
        } => (api_version, kind, namespace),
    };
    WatchReplayFloor {
        api_version,
        kind,
        namespace_key,
        floor_resource_version,
        floor_event_id,
        position_is_exact,
    }
}

fn watch_events_min_scope_rows(max_rows: i64, scope_count: usize) -> usize {
    if max_rows <= 0 || scope_count == 0 {
        return 0;
    }
    let fair_share = (max_rows as usize) / scope_count;
    let dynamic_floor = if fair_share == 0 && scope_count <= MIN_SCOPE_COUNT_BEFORE_EXPIRING_SCOPES
    {
        1
    } else {
        fair_share
    };
    (max_rows.clamp(1, DEFAULT_MIN_WATCH_EVENTS_PER_SCOPE) as usize).min(dynamic_floor)
}

fn watch_events_max_rows(max_rows: i64) -> usize {
    if max_rows <= 0 { 0 } else { max_rows as usize }
}

fn watch_events_batch_cap(batch_cap: i64) -> usize {
    if batch_cap < 0 {
        usize::MAX
    } else {
        batch_cap as usize
    }
}

#[derive(Clone)]
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
        let targets = durable_targets(targets);
        Ok(RedbReadCore::new(self.accessor.clone())
            .watch_events_since(&targets, since_rv)
            .await?
            .into_iter()
            .map(durable_to_catchup)
            .collect())
    }

    pub async fn watch_list_checked(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<WatchReplayRead> {
        let targets = durable_targets(targets);
        Ok(
            match RedbReadCore::new(self.accessor.clone())
                .watch_events_since_checked(&targets, since_rv, None)
                .await?
            {
                RedbCheckedWatchRead::Events(events) => {
                    WatchReplayRead::Events(events.into_iter().map(durable_to_catchup).collect())
                }
                RedbCheckedWatchRead::Expired => WatchReplayRead::Expired,
            },
        )
    }

    pub async fn watch_list_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead> {
        let targets = durable_targets(targets);
        Ok(
            match RedbReadCore::new(self.accessor.clone())
                .watch_events_since_checked(&targets, since_rv, Some(limit))
                .await?
            {
                RedbCheckedWatchRead::Events(events) => {
                    WatchReplayRead::Events(events.into_iter().map(durable_to_catchup).collect())
                }
                RedbCheckedWatchRead::Expired => WatchReplayRead::Expired,
            },
        )
    }

    pub async fn watch_list_raw_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead<klights_cluster_store::DurableRawWatchEvent>> {
        let targets = durable_targets(targets);
        Ok(
            match RedbReadCore::new(self.accessor.clone())
                .raw_watch_events_since_checked(&targets, since_rv, limit)
                .await?
            {
                RedbCheckedWatchRead::Events(events) => WatchReplayRead::Events(events),
                RedbCheckedWatchRead::Expired => WatchReplayRead::Expired,
            },
        )
    }

    pub async fn watch_list_positioned_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<CatchUpResource>> {
        let targets = durable_targets(targets);
        Ok(
            match RedbReadCore::new(self.accessor.clone())
                .positioned_watch_events(&targets, position, limit)
                .await?
            {
                RedbPositionedWatchRead::Expired => PositionedWatchReplayRead::Expired,
                RedbPositionedWatchRead::Events(page) => {
                    PositionedWatchReplayRead::Events(PositionedWatchReplay {
                        events: page
                            .events
                            .into_iter()
                            .map(|event| klights_cluster_core::PositionedWatchEvent {
                                position: event.position,
                                event: durable_to_catchup(event.event),
                            })
                            .collect(),
                        next_position: page.next_position,
                    })
                }
            },
        )
    }

    pub async fn current_watch_replay_position(&self) -> Result<WatchReplayPosition> {
        RedbReadCore::new(self.accessor.clone())
            .allocator_position()
            .await
    }

    pub async fn watch_list_raw_positioned_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<klights_cluster_store::DurableRawWatchEvent>> {
        let targets = durable_targets(targets);
        Ok(
            match RedbReadCore::new(self.accessor.clone())
                .positioned_raw_watch_events(&targets, position, limit)
                .await?
            {
                RedbPositionedWatchRead::Expired => PositionedWatchReplayRead::Expired,
                RedbPositionedWatchRead::Events(page) => {
                    PositionedWatchReplayRead::Events(PositionedWatchReplay {
                        events: page.events,
                        next_position: page.next_position,
                    })
                }
            },
        )
    }

    pub async fn watch_list_deleted_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        Ok(RedbReadCore::new(self.accessor.clone())
            .all_watch_events_since(since_rv, true)
            .await?
            .into_iter()
            .map(durable_to_catchup)
            .collect())
    }

    pub async fn watch_list_all_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        Ok(RedbReadCore::new(self.accessor.clone())
            .all_watch_events_since(since_rv, false)
            .await?
            .into_iter()
            .map(durable_to_catchup)
            .collect())
    }

    pub async fn watch_list_all_since_paged(
        &self,
        since_rv: i64,
        _after_resource_version: i64,
        after_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        Ok(RedbReadCore::new(self.accessor.clone())
            .all_watch_events_since_paged(since_rv, after_id, None, limit)
            .await?
            .into_iter()
            .map(|(event_id, event)| (event_id, durable_to_catchup(event)))
            .collect())
    }

    pub async fn watch_list_all_after_id_bounded(
        &self,
        after_id: i64,
        through_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        Ok(RedbReadCore::new(self.accessor.clone())
            .all_watch_events_since_paged(0, after_id, Some(through_id), limit)
            .await?
            .into_iter()
            .map(|(event_id, event)| (event_id, durable_to_catchup(event)))
            .collect())
    }

    pub async fn list_watch_replay_floors(&self) -> Result<Vec<WatchReplayFloor>> {
        Ok(RedbReadCore::new(self.accessor.clone())
            .replay_floors()
            .await?
            .into_iter()
            .map(durable_floor_to_legacy)
            .collect())
    }

    pub async fn list_watch_replay_floors_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotReplayFloorCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<WatchReplayFloor>> {
        Ok(RedbReadCore::new(self.accessor.clone())
            .replay_floors_paged(after, limit)
            .await?
            .into_iter()
            .map(durable_floor_to_legacy)
            .collect())
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
            let entries: Vec<(u64, u64, Option<Vec<u8>>)> = {
                let tbl = w.open_table(tables::WATCH_EVENTS)?;
                let mut entries = Vec::new();
                for entry in tbl.iter()? {
                    let (key, event_ref) = entry?;
                    let event_id = key.value();
                    let body = event_ref.value().to_vec();
                    let event: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    let rv = event
                        .get("resourceVersion")
                        .and_then(Value::as_u64)
                        .unwrap_or_default();
                    entries.push((event_id, rv, floor_key_for_event(&event)));
                }
                entries
            };
            let candidates = watch_gc_candidates(&entries, max_rows, batch_cap);
            if candidates.is_empty() {
                w.commit()?;
                return Ok(0);
            }

            let mut keys_to_remove = Vec::with_capacity(candidates.len());
            let mut floor_updates: BTreeMap<Vec<u8>, (u64, u64)> = BTreeMap::new();
            for (event_id, rv, floor_key) in candidates {
                if let Some(key) = floor_key {
                    floor_updates
                        .entry(key)
                        .and_modify(|floor| {
                            floor.0 = floor.0.max(rv);
                            floor.1 = floor.1.max(event_id);
                        })
                        .or_insert((rv, event_id));
                }
                keys_to_remove.push(event_id);
            }

            {
                let mut rv_floors = w.open_table(tables::WATCH_REPLAY_FLOORS)?;
                let mut position_floors = w.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
                for (key, (floor_rv, floor_event_id)) in floor_updates {
                    let existing = rv_floors.get(key.as_slice())?.map(|guard| guard.value());
                    if existing.is_none_or(|current| floor_rv > current) {
                        rv_floors.insert(key.as_slice(), floor_rv)?;
                    }
                    let (floor_rv, floor_event_id) = position_floors
                        .get(key.as_slice())?
                        .and_then(|value| decode_position_floor(value.value()))
                        .map_or((floor_rv, floor_event_id), |existing| {
                            (existing.0.max(floor_rv), existing.1.max(floor_event_id))
                        });
                    let encoded = encode_position_floor(floor_rv, floor_event_id);
                    position_floors.insert(key.as_slice(), encoded.as_slice())?;
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
            let mut entries = Vec::new();
            for entry in tbl.iter()? {
                let (key, event_ref) = entry?;
                let event_id = key.value();
                let body = event_ref.value().to_vec();
                let event: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let rv = event
                    .get("resourceVersion")
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                entries.push((event_id, rv, floor_key_for_event(&event)));
            }
            Ok(watch_gc_candidates(&entries, max_rows, batch_cap).len())
        })
        .await
    }
}

fn watch_gc_candidates(
    entries: &[(u64, u64, Option<Vec<u8>>)],
    max_rows: i64,
    batch_cap: i64,
) -> Vec<(u64, u64, Option<Vec<u8>>)> {
    let max_rows = watch_events_max_rows(max_rows);
    if entries.len() <= max_rows {
        return Vec::new();
    }

    let global_prunable = entries.len() - max_rows;
    let batch_cap = watch_events_batch_cap(batch_cap);
    let mut scope_totals: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
    for (_, _, floor_key) in entries {
        if let Some(key) = floor_key {
            *scope_totals.entry(key.clone()).or_default() += 1;
        }
    }
    let min_scope_rows = watch_events_min_scope_rows(max_rows as i64, scope_totals.len());

    let mut seen_by_scope: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
    let mut candidates = Vec::new();
    for (idx, (event_id, rv, floor_key)) in entries.iter().enumerate() {
        if idx >= global_prunable || candidates.len() >= batch_cap {
            break;
        }

        if let Some(key) = floor_key {
            let seen = seen_by_scope.entry(key.clone()).or_default();
            let scope_rank = scope_totals.get(key).copied().unwrap_or(0) - *seen;
            *seen += 1;
            if scope_rank <= min_scope_rows {
                continue;
            }
        }

        candidates.push((*event_id, *rv, floor_key.clone()));
    }
    candidates
}

fn encode_position_floor(resource_version: u64, event_id: u64) -> [u8; 16] {
    let mut encoded = [0_u8; 16];
    encoded[..8].copy_from_slice(&resource_version.to_be_bytes());
    encoded[8..].copy_from_slice(&event_id.to_be_bytes());
    encoded
}

fn decode_position_floor(encoded: &[u8]) -> Option<(u64, u64)> {
    if encoded.len() != 16 {
        return None;
    }
    Some((
        u64::from_be_bytes(encoded[..8].try_into().ok()?),
        u64::from_be_bytes(encoded[8..].try_into().ok()?),
    ))
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

    use klights_cluster_datastore::redb as open_boundary;
    use klights_cluster_datastore::redb::RedbAccessor;
    use klights_cluster_datastore::redb::mutation_helpers as helpers;
    use klights_supervisor::TaskSupervisor;

    use super::*;

    async fn store() -> RedbWatchStore {
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let db = open_boundary::open_in_memory(supervisor.as_ref())
            .await
            .unwrap();
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
    async fn replay_floor_keyset_pages_are_bounded_complete_and_exclusive() {
        let s = store().await;
        let db = s.accessor.db().unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut floors = write
                .open_table(tables::WATCH_REPLAY_POSITION_FLOORS)
                .unwrap();
            for index in 0..=klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
                let key = floor_key("v1", "ConfigMap", &format!("ns-{index:04}"));
                floors
                    .insert(
                        key.as_slice(),
                        encode_position_floor(index as u64, index as u64).as_slice(),
                    )
                    .unwrap();
            }
        }
        write.commit().unwrap();

        let page_limit =
            std::num::NonZeroUsize::new(klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE).unwrap();
        let mut after = None;
        let mut delivered = Vec::new();
        let mut page_lengths = Vec::new();
        loop {
            let page = s
                .list_watch_replay_floors_paged(after.as_ref(), page_limit)
                .await
                .unwrap();
            if page.is_empty() {
                break;
            }
            page_lengths.push(page.len());
            delivered.extend(page.iter().map(|floor| floor.namespace_key.clone()));
            let last = page.last().unwrap();
            after = Some(
                klights_cluster_store::SnapshotReplayFloorCursor::try_new(
                    klights_cluster_store::DurableReplayTarget::Namespaced {
                        api_version: last.api_version.clone(),
                        kind: last.kind.clone(),
                        namespace: last.namespace_key.clone(),
                    },
                )
                .unwrap(),
            );
        }
        assert_eq!(
            page_lengths,
            vec![klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE, 1]
        );
        assert_eq!(
            delivered.len(),
            klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE + 1
        );
        assert!(delivered.windows(2).all(|pair| pair[0] < pair[1]));

        let oversized =
            std::num::NonZeroUsize::new(klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE + 1)
                .unwrap();
        assert!(
            s.list_watch_replay_floors_paged(None, oversized)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn watch_list_filters_by_target() {
        let s = store().await;
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
        let s = store().await;
        insert_watch_event(&s, 1, "v1", "Pod", Some("ns"), "p", "ADDED");
        insert_watch_event(&s, 2, "v1", "Pod", Some("ns"), "p", "DELETED");
        insert_watch_event(&s, 3, "v1", "Pod", Some("ns"), "q", "MODIFIED");

        let results = s.watch_list_deleted_since(0).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].resource.name, "p");
    }

    #[tokio::test]
    async fn watch_list_respects_since_rv() {
        let s = store().await;
        insert_watch_event(&s, 1, "v1", "Pod", Some("ns"), "old", "ADDED");
        insert_watch_event(&s, 2, "v1", "Pod", Some("ns"), "new", "ADDED");

        let results = s.watch_list_all_since(1).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].resource.name, "new");
    }

    #[tokio::test]
    async fn watch_list_all_since_paged_keysets_across_resource_versions() {
        let s = store().await;
        for i in 1..=5 {
            insert_watch_event(&s, i, "v1", "Pod", Some("ns"), &format!("p{i}"), "ADDED");
        }
        let limit = std::num::NonZeroUsize::new(2).unwrap();

        let page1 = s.watch_list_all_since_paged(1, 0, 0, limit).await.unwrap();
        assert_eq!(
            page1
                .iter()
                .map(|(_, event)| event.resource.resource_version)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(
            page1.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![2, 3]
        );

        let last = page1.last().unwrap();
        let page2 = s
            .watch_list_all_since_paged(1, last.1.resource.resource_version, last.0, limit)
            .await
            .unwrap();
        assert_eq!(
            page2
                .iter()
                .map(|(_, event)| event.resource.resource_version)
                .collect::<Vec<_>>(),
            vec![4, 5]
        );
        assert_eq!(
            page2.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![4, 5]
        );
    }

    #[tokio::test]
    async fn positioned_replay_preserves_one_hundred_same_revision_siblings() {
        let s = store().await;
        for index in 0..100 {
            insert_watch_event(
                &s,
                7,
                "v1",
                "Pod",
                Some("ns"),
                &format!("same-rv-{index}"),
                "ADDED",
            );
        }
        let targets = [WatchTarget::namespaced_in_namespace("v1", "Pod", "ns")];
        let limit = std::num::NonZeroUsize::new(3).unwrap();
        let mut position = WatchReplayPosition::from_resource_version(6);
        let mut delivered = Vec::new();
        loop {
            let PositionedWatchReplayRead::Events(replay) = s
                .watch_list_positioned_checked_bounded(&targets, position, limit)
                .await
                .unwrap()
            else {
                panic!("fresh redb replay must not expire");
            };
            position = replay.next_position;
            let count = replay.events.len();
            delivered.extend(
                replay
                    .events
                    .into_iter()
                    .map(|event| event.event.resource.name),
            );
            if count < limit.get() {
                break;
            }
        }
        assert_eq!(delivered.len(), 100);
        assert_eq!(
            delivered
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            100
        );
    }

    #[tokio::test]
    async fn positioned_replay_gc_expiry_is_scope_specific() {
        let s = store().await;
        insert_watch_event(&s, 1, "v1", "Secret", Some("seed"), "anchor", "ADDED");
        let target = [WatchTarget::namespaced_in_namespace(
            "v1",
            "ConfigMap",
            "target",
        )];
        let quiet = [WatchTarget::namespaced_in_namespace(
            "v1", "Secret", "quiet",
        )];
        let limit = std::num::NonZeroUsize::new(3).unwrap();
        let PositionedWatchReplayRead::Events(target_start) = s
            .watch_list_positioned_checked_bounded(
                &target,
                WatchReplayPosition::from_resource_version(1),
                limit,
            )
            .await
            .unwrap()
        else {
            panic!("fresh target position must not expire");
        };
        let PositionedWatchReplayRead::Events(quiet_start) = s
            .watch_list_positioned_checked_bounded(
                &quiet,
                WatchReplayPosition::from_resource_version(1),
                limit,
            )
            .await
            .unwrap()
        else {
            panic!("fresh quiet position must not expire");
        };
        for index in 0..5 {
            insert_watch_event(
                &s,
                2 + index,
                "v1",
                "ConfigMap",
                Some("target"),
                &format!("item-{index}"),
                "ADDED",
            );
        }
        s.gc_watch(1, 100).await.unwrap();

        assert!(matches!(
            s.watch_list_positioned_checked_bounded(&target, target_start.next_position, limit,)
                .await
                .unwrap(),
            PositionedWatchReplayRead::Expired
        ));
        assert!(matches!(
            s.watch_list_positioned_checked_bounded(&quiet, quiet_start.next_position, limit,)
                .await
                .unwrap(),
            PositionedWatchReplayRead::Events(_)
        ));
    }

    #[tokio::test]
    async fn legacy_wildcard_floor_expires_unknown_positioned_scope() {
        let s = store().await;
        for rv in 1..=5 {
            insert_watch_event(
                &s,
                rv,
                "v1",
                "Secret",
                Some("seed"),
                &format!("seed-{rv}"),
                "ADDED",
            );
        }
        let db = s.accessor.db().unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut rv_floors = write.open_table(tables::WATCH_REPLAY_FLOORS).unwrap();
            let mut position_floors = write
                .open_table(tables::WATCH_REPLAY_POSITION_FLOORS)
                .unwrap();
            let wildcard = floor_key("*", "*", "*");
            rv_floors.insert(wildcard.as_slice(), 5).unwrap();
            position_floors
                .insert(wildcard.as_slice(), encode_position_floor(5, 5).as_slice())
                .unwrap();
        }
        write.commit().unwrap();

        let target = [WatchTarget::namespaced_in_namespace(
            "example.test/v1",
            "Unknown",
            "default",
        )];
        assert!(matches!(
            s.watch_list_positioned_checked_bounded(
                &target,
                WatchReplayPosition {
                    resource_version: 4,
                    event_id: 4,
                    resource_version_filter_through_event_id: 0,
                },
                std::num::NonZeroUsize::new(8).unwrap(),
            )
            .await
            .unwrap(),
            PositionedWatchReplayRead::Expired
        ));
    }

    #[tokio::test]
    async fn gc_watch_trims_oldest() {
        let s = store().await;
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
