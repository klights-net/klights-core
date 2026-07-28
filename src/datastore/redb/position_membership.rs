use ::redb::{ReadableDatabase, ReadableTable};
use anyhow::Result;
use serde::Deserialize;
use serde_json::{Value, value::RawValue};

use super::{RedbDatastore, helpers};
use crate::datastore::position_membership::{
    MembershipHistoryEvent, MembershipReconstructor, ReconstructedMembership,
    apply_membership_selectors, resource_from_history, sort_for_watch_targets,
};
use crate::datastore::{
    Resource, ResourceList, SnapshotAtRv, WatchReplayPosition, WatchTarget, WatchTargetScope,
};
use klights_cluster_datastore::redb::tables;

const CLUSTER_NAMESPACE_KEY: &str = "#cluster";

#[derive(Deserialize)]
struct StoredEvent<'a> {
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

impl RedbDatastore {
    pub async fn snapshot_resources_at_position(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        position: WatchReplayPosition,
    ) -> Result<SnapshotAtRv> {
        let sort_targets = targets.to_vec();
        let targets = sort_targets.clone();
        let label_selector = label_selector.map(str::to_string);
        let field_selector = field_selector.map(str::to_string);
        let raw = self
            .accessor
            .call("snapshot_resources_at_position", move |database| {
                let read = database.begin_read()?;
                let current_position = helpers::watch_replay_position_in_read(&read)?;
                if position.event_id > current_position.event_id
                    || position.resource_version_filter_through_event_id > current_position.event_id
                    || (position.event_id == 0
                        && position.resource_version_filter_through_event_id == 0
                        && position.resource_version > current_position.resource_version)
                {
                    return Ok(PositionRawSnapshot::Expired);
                }
                if cursor_covers_current(position, current_position) {
                    return read_current_targets(&read, &targets).map(PositionRawSnapshot::Items);
                }
                if position_expired(&read, &targets, position)? {
                    return Ok(PositionRawSnapshot::Expired);
                }
                let current = read_current_targets(&read, &targets)?;
                Ok(
                    match reconstruct_in_read(&read, &targets, current, position)? {
                        ReconstructedMembership::Expired => PositionRawSnapshot::Expired,
                        ReconstructedMembership::Items(items) => PositionRawSnapshot::Items(items),
                    },
                )
            })
            .await?;

        let mut items = match raw {
            PositionRawSnapshot::Expired => return Ok(SnapshotAtRv::Expired),
            PositionRawSnapshot::Items(items) => apply_membership_selectors(
                items,
                label_selector.as_deref(),
                field_selector.as_deref(),
            )?,
        };
        sort_for_watch_targets(&mut items, &sort_targets);
        Ok(SnapshotAtRv::List(ResourceList {
            items,
            resource_version: position.resource_version,
            watch_replay_position: Some(position),
            continue_token: None,
            remaining_item_count: None,
        }))
    }
}

enum PositionRawSnapshot {
    Expired,
    Items(Vec<Resource>),
}

fn cursor_covers_current(position: WatchReplayPosition, current: WatchReplayPosition) -> bool {
    position.event_id >= current.event_id
        || (position.resource_version_filter_through_event_id >= current.event_id
            && position.resource_version >= current.resource_version)
        || (position.event_id == 0
            && position.resource_version_filter_through_event_id == 0
            && position.resource_version >= current.resource_version)
}

fn position_expired(
    read: &::redb::ReadTransaction,
    targets: &[WatchTarget],
    position: WatchReplayPosition,
) -> Result<bool> {
    for target in targets {
        if (position.event_id > 0
            && target_floor(read, target, true)?.is_some_and(|floor| position.event_id < floor))
            || ((position.event_id == 0 || position.resource_version_filter_through_event_id > 0)
                && target_floor(read, target, false)?
                    .is_some_and(|floor| position.resource_version < floor))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn target_floor(
    read: &::redb::ReadTransaction,
    target: &WatchTarget,
    event_id: bool,
) -> Result<Option<i64>> {
    let table = if event_id {
        read.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?
    } else {
        return target_rv_floor(read, target);
    };
    let scoped: Result<Option<i64>> = match &target.scope {
        WatchTargetScope::Cluster => Ok(table
            .get(floor_key(&target.api_version, &target.kind, CLUSTER_NAMESPACE_KEY).as_slice())?
            .and_then(|value| decode_position_floor(value.value()))),
        WatchTargetScope::Namespaced(Some(namespace)) => Ok(table
            .get(floor_key(&target.api_version, &target.kind, namespace).as_slice())?
            .and_then(|value| decode_position_floor(value.value()))),
        WatchTargetScope::Namespaced(None) => {
            let prefix = floor_key_prefix(&target.api_version, &target.kind);
            let mut floor = None;
            for entry in table.iter()? {
                let (key, value) = entry?;
                let Some(namespace) = key.value().strip_prefix(prefix.as_slice()) else {
                    continue;
                };
                if namespace.is_empty() || namespace == CLUSTER_NAMESPACE_KEY.as_bytes() {
                    continue;
                }
                if let Some(value) = decode_position_floor(value.value()) {
                    floor = Some(floor.map_or(value, |current: i64| current.max(value)));
                }
            }
            Ok(floor)
        }
    };
    let scoped = scoped?;
    Ok(super::replay_floor::LegacyReplayFloor::read(read)?
        .and_then(|legacy| legacy.merge_event_id(scoped))
        .or(scoped))
}

fn target_rv_floor(read: &::redb::ReadTransaction, target: &WatchTarget) -> Result<Option<i64>> {
    let table = read.open_table(tables::WATCH_REPLAY_FLOORS)?;
    let scoped: Result<Option<i64>> = match &target.scope {
        WatchTargetScope::Cluster => Ok(table
            .get(floor_key(&target.api_version, &target.kind, CLUSTER_NAMESPACE_KEY).as_slice())?
            .map(|value| value.value() as i64)),
        WatchTargetScope::Namespaced(Some(namespace)) => Ok(table
            .get(floor_key(&target.api_version, &target.kind, namespace).as_slice())?
            .map(|value| value.value() as i64)),
        WatchTargetScope::Namespaced(None) => {
            let prefix = floor_key_prefix(&target.api_version, &target.kind);
            let mut floor = None;
            for entry in table.iter()? {
                let (key, value) = entry?;
                let Some(namespace) = key.value().strip_prefix(prefix.as_slice()) else {
                    continue;
                };
                if namespace.is_empty() || namespace == CLUSTER_NAMESPACE_KEY.as_bytes() {
                    continue;
                }
                let value = value.value() as i64;
                floor = Some(floor.map_or(value, |current: i64| current.max(value)));
            }
            Ok(floor)
        }
    };
    let scoped = scoped?;
    Ok(super::replay_floor::LegacyReplayFloor::read(read)?
        .and_then(|legacy| legacy.merge_resource_version(scoped))
        .or(scoped))
}

fn read_current_targets(
    read: &::redb::ReadTransaction,
    targets: &[WatchTarget],
) -> Result<Vec<Resource>> {
    let mut resources = Vec::new();
    for target in targets {
        if target.api_version == "v1"
            && target.kind == "Namespace"
            && matches!(&target.scope, WatchTargetScope::Cluster)
        {
            let table = read.open_table(tables::NAMESPACES)?;
            for entry in table.iter()? {
                let (name, body) = entry?;
                let data: Value = serde_json::from_slice(body.value())?;
                let rv = data
                    .pointer("/metadata/resourceVersion")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse().ok())
                    .unwrap_or_default();
                resources.push(resource_from_history(
                    "v1".into(),
                    "Namespace".into(),
                    None,
                    name.value().to_string(),
                    rv,
                    data,
                ));
            }
            continue;
        }
        let table = match &target.scope {
            WatchTargetScope::Cluster => read.open_table(tables::RES_CLUSTER)?,
            WatchTargetScope::Namespaced(_) => read.open_table(tables::RES_NS)?,
        };
        for entry in table.iter()? {
            let (_key, value) = entry?;
            let (rv, body) = value.value();
            let Some(resource) = helpers::resource_in_ns(&[], rv, body) else {
                continue;
            };
            if target_matches(
                target,
                &resource.api_version,
                &resource.kind,
                resource.namespace.as_deref(),
            ) {
                resources.push(resource);
            }
        }
    }
    Ok(resources)
}

fn reconstruct_in_read(
    read: &::redb::ReadTransaction,
    targets: &[WatchTarget],
    current: Vec<Resource>,
    position: WatchReplayPosition,
) -> Result<ReconstructedMembership> {
    let table = read.open_table(tables::WATCH_EVENTS)?;
    let mut reconstructor = MembershipReconstructor::new(current, position);
    for entry in table.iter()?.rev() {
        let (event_id, body) = entry?;
        if reconstructor.can_stop_before(event_id.value() as i64) {
            break;
        }
        let event: StoredEvent<'_> = serde_json::from_slice(body.value())?;
        let api_version = event.api_version.unwrap_or("");
        let kind = event.kind.unwrap_or("");
        if !targets
            .iter()
            .any(|target| target_matches(target, api_version, kind, event.namespace))
        {
            continue;
        }
        let data = event
            .data
            .map(|data| serde_json::from_str(data.get()))
            .transpose()?
            .unwrap_or(Value::Null);
        reconstructor.observe(&MembershipHistoryEvent {
            event_id: event_id.value() as i64,
            event_type: event.event_type.unwrap_or("").to_string(),
            resource: resource_from_history(
                api_version.to_string(),
                kind.to_string(),
                event.namespace.map(str::to_string),
                event.name.unwrap_or("").to_string(),
                event.resource_version.unwrap_or_default(),
                data,
            ),
        });
    }
    Ok(reconstructor.finish())
}

fn target_matches(
    target: &WatchTarget,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
) -> bool {
    target.api_version == api_version
        && target.kind == kind
        && match &target.scope {
            WatchTargetScope::Cluster => namespace.is_none(),
            WatchTargetScope::Namespaced(Some(want)) => namespace == Some(want.as_str()),
            WatchTargetScope::Namespaced(None) => namespace.is_some(),
        }
}

fn decode_position_floor(encoded: &[u8]) -> Option<i64> {
    (encoded.len() == 16).then(|| {
        i64::try_from(u64::from_be_bytes(
            encoded[8..].try_into().expect("fixed slice"),
        ))
        .unwrap_or(i64::MAX)
    })
}

fn floor_key(api_version: &str, kind: &str, namespace: &str) -> Vec<u8> {
    let mut key = floor_key_prefix(api_version, kind);
    key.extend_from_slice(namespace.as_bytes());
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
