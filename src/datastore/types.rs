//! Datastore types shared across the trait surface and every backend.
//!
//! Anything used in a `DatastoreBackend` method signature lives here so the
//! trait module stays SQL-free and a future backend implementor can build
//! against `crate::datastore::*` without pulling in SQLite-specific code.

use anyhow::{Result, anyhow};
#[cfg(test)]
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;

use klights_cluster_core::Resource;

#[cfg(test)]
#[derive(Serialize)]
struct TestWatchEnvelope<'a> {
    #[serde(rename = "type")]
    event_type: &'a str,
    object: &'a Value,
}

#[cfg(test)]
pub(crate) fn with_staged_test_resource_event(
    staged: klights_cluster_store::StagedPostCommit,
    event_type: &str,
    name: &str,
    data: std::sync::Arc<Value>,
) -> klights_cluster_store::StagedPostCommit {
    let data = hydrate_staged_test_resource(
        std::sync::Arc::unwrap_or_clone(data),
        staged.api_version(),
        staged.kind(),
        staged.namespace(),
        name,
        staged.resource_version(),
    );
    let is_envelope = data
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|value| matches!(value, "ADDED" | "MODIFIED" | "DELETED" | "ERROR"))
        && data.get("object").is_some();
    let resource_data = if is_envelope {
        data.get("object")
            .expect("checked staged watch envelope object")
            .clone()
    } else {
        data.clone()
    };
    let resource = Resource::try_from_data(std::sync::Arc::new(resource_data))
        .expect("staged test resource has canonical identity");
    let encoded_json = if is_envelope {
        serde_json::to_vec(&data)
    } else {
        serde_json::to_vec(&TestWatchEnvelope {
            event_type,
            object: resource.data.as_ref(),
        })
    }
    .ok()
    .map(Bytes::from);
    staged.with_test_event(event_type, resource, encoded_json)
}

#[cfg(test)]
fn hydrate_staged_test_resource(
    mut data: Value,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    resource_version: i64,
) -> Value {
    if data
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|event_type| matches!(event_type, "ADDED" | "MODIFIED" | "DELETED" | "ERROR"))
        && let Some(object) = data.get_mut("object")
    {
        *object = hydrate_staged_test_resource(
            std::mem::take(object),
            api_version,
            kind,
            namespace,
            name,
            resource_version,
        );
        return data;
    }
    if let Some(object) = data.as_object_mut() {
        object.insert("apiVersion".to_string(), Value::from(api_version));
        object.insert("kind".to_string(), Value::from(kind));
        let metadata = object
            .entry("metadata")
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        if let Some(metadata) = metadata.as_object_mut() {
            metadata.insert("name".to_string(), Value::from(name));
            if let Some(namespace) = namespace {
                metadata.insert("namespace".to_string(), Value::from(namespace));
            }
            metadata.insert(
                "resourceVersion".to_string(),
                Value::from(resource_version.to_string()),
            );
        }
    }
    data
}

pub const POD_CLEANUP_REASON_NODE_LOST: &str = "NodeLost";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PodCleanupIntent {
    pub node_name: String,
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub reason: String,
    pub resource_version: i64,
    pub created_at_ms: i64,
    pub pod_data: Value,
}

impl From<PodCleanupIntent> for klights_cluster_core::LogApplyPodCleanupIntentRow {
    fn from(row: PodCleanupIntent) -> Self {
        Self {
            node_name: row.node_name,
            namespace: row.namespace,
            pod_name: row.pod_name,
            pod_uid: row.pod_uid,
            reason: row.reason,
            resource_version: row.resource_version,
            created_at_ms: row.created_at_ms,
            pod_data: row.pod_data,
        }
    }
}

impl From<klights_cluster_core::LogApplyPodCleanupIntentRow> for PodCleanupIntent {
    fn from(row: klights_cluster_core::LogApplyPodCleanupIntentRow) -> Self {
        Self {
            node_name: row.node_name,
            namespace: row.namespace,
            pod_name: row.pod_name,
            pod_uid: row.pod_uid,
            reason: row.reason,
            resource_version: row.resource_version,
            created_at_ms: row.created_at_ms,
            pod_data: row.pod_data,
        }
    }
}

/// Leader metadata stamped into a replica backup during full snapshot restore.
///
/// A running replica does not read this metadata. It is persisted into the
/// backup `cluster.db` so that a later restart-as-leader sees the same cluster
/// identity instead of generating a new one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatedSnapshotMetadata {
    pub cluster_id: String,
    pub leader_epoch: i64,
    pub membership: ReplicatedMembershipState,
    /// Exact command-codec activation proof captured with the authoritative
    /// snapshot. `None` is fail-closed and removes any destination-local
    /// marker; only `Some(3)` may reopen proposal capability after restore.
    pub command_codec_activation_version: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicatedMembershipState {
    LegacyOmitted,
    AuthoritativeAbsent,
    Present(klights_cluster_core::ClusterMembership),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableAllocatorObservation {
    pub position: klights_cluster_core::WatchReplayPosition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterMetadataObservation {
    pub metadata: klights_cluster_core::ClusterMetadata,
    pub membership: ReplicatedMembershipState,
}

/// Per-resource-scope compaction boundary for durable watch replay.
/// Snapshot restore replaces these rows authoritatively together with watch
/// history so a failover neither skips compacted events nor spuriously expires
/// a cursor that remains valid on the leader.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchReplayFloor {
    pub api_version: String,
    pub kind: String,
    pub namespace_key: String,
    pub floor_resource_version: i64,
    pub floor_event_id: i64,
    /// Whether `floor_event_id` is an exact retained-history cursor. Missing
    /// from older snapshots decodes as `false`; restore normalizes nonzero
    /// event floors from stable snapshots back to exact boundaries.
    #[serde(default)]
    pub position_is_exact: bool,
}

impl WatchReplayFloor {
    pub const fn retention_boundary(&self) -> klights_cluster_store::ReplayRetentionBoundary {
        if self.position_is_exact {
            klights_cluster_store::ReplayRetentionBoundary::Exact(WatchReplayPosition {
                resource_version: self.floor_resource_version,
                event_id: self.floor_event_id,
                resource_version_filter_through_event_id: 0,
            })
        } else {
            klights_cluster_store::ReplayRetentionBoundary::LegacyRvOnly {
                resource_version: self.floor_resource_version,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceListQuery<'a> {
    pub label_selector: Option<&'a str>,
    pub field_selector: Option<&'a str>,
    pub limit: Option<i64>,
    pub continue_token: Option<&'a str>,
}

impl<'a> ResourceListQuery<'a> {
    pub const fn new(
        label_selector: Option<&'a str>,
        field_selector: Option<&'a str>,
        limit: Option<i64>,
        continue_token: Option<&'a str>,
    ) -> Self {
        Self {
            label_selector,
            field_selector,
            limit,
            continue_token,
        }
    }

    pub const fn all() -> Self {
        Self::new(None, None, None, None)
    }

    pub fn page_request(self) -> Result<ListPageRequest> {
        ListPageRequest::try_new(self.limit, self.continue_token.map(str::to_string))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedCreateOptions {
    pub resource_version: i64,
    pub meta_uid: Option<String>,
}

impl ReplicatedCreateOptions {
    pub fn new(resource_version: i64, meta_uid: Option<String>) -> Self {
        Self {
            resource_version,
            meta_uid,
        }
    }
}

/// A resource returned by the watch catch-up path with the exact event type emitted
/// at this resourceVersion.
#[derive(Debug, Clone)]
pub struct CatchUpResource {
    pub resource: Resource,
    /// One of `ADDED`, `MODIFIED`, `DELETED`. Held as `Cow<'static, str>` so
    /// the common case (initial-list ADDED, replay path with the standard
    /// three labels) reuses static literals — avoiding a per-event String
    /// allocation across N watchers × M events/sec.
    pub event_type: std::borrow::Cow<'static, str>,
}

/// Result of a checked durable watch replay read.
///
/// `Expired` means the requested resume RV is outside the retained
/// `watch_events` window and callers must relist instead of advancing from a
/// partial suffix.
#[derive(Debug, Clone)]
pub enum WatchReplayRead<T = CatchUpResource> {
    Events(Vec<T>),
    Expired,
}

use klights_cluster_core::{PositionedWatchEvent, WatchReplayPosition};

#[derive(Clone, Debug)]
pub struct PositionedWatchReplay<T> {
    pub events: Vec<PositionedWatchEvent<T>>,
    /// Position covered by the read snapshot. This advances even for an empty
    /// matching page, anchoring the cursor before a later lower-RV row lands.
    pub next_position: WatchReplayPosition,
}

#[derive(Clone, Debug)]
pub enum PositionedWatchReplayRead<T> {
    Events(PositionedWatchReplay<T>),
    Expired,
}

impl CatchUpResource {
    pub fn added(resource: Resource) -> Self {
        Self {
            resource,
            event_type: std::borrow::Cow::Borrowed("ADDED"),
        }
    }

    pub fn into_parts(self) -> (Resource, Cow<'static, str>) {
        (self.resource, self.event_type)
    }
}

#[cfg(test)]
mod resource_arc_tests {
    use super::*;
    use crate::watch::WatchEvent;
    use serde_json::json;

    #[test]
    fn resource_helpers_convert_watch_event_and_added_catchup() {
        let event = WatchEvent::added(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "frontend",
                "namespace": "default",
                "uid": "uid-frontend",
                "resourceVersion": "42"
            }
        }));

        let resource = Resource::try_from_watch_event(&event).unwrap();
        assert_eq!(resource.api_version, "v1");
        assert_eq!(resource.kind, "Pod");
        assert_eq!(resource.namespace.as_deref(), Some("default"));
        assert_eq!(resource.name, "frontend");
        assert_eq!(resource.uid, "uid-frontend");
        assert_eq!(resource.resource_version, 42);

        let catchup = CatchUpResource::added(resource);
        assert_eq!(catchup.event_type.as_ref(), "ADDED");
        assert!(matches!(catchup.event_type, std::borrow::Cow::Borrowed(_)));
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum WatchTargetScope {
    Cluster,
    Namespaced(Option<String>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WatchTarget {
    pub api_version: String,
    pub kind: String,
    pub scope: WatchTargetScope,
}

impl WatchTarget {
    pub fn cluster(api_version: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            scope: WatchTargetScope::Cluster,
        }
    }

    pub fn namespaced(api_version: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            scope: WatchTargetScope::Namespaced(None),
        }
    }

    pub fn namespaced_in_namespace(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            api_version: api_version.into(),
            kind: kind.into(),
            scope: WatchTargetScope::Namespaced(Some(namespace.into())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResourceList {
    pub items: Vec<Resource>,
    pub resource_version: i64,
    /// Durable watch-log position captured atomically with this LIST snapshot.
    /// Backends that cannot provide an atomic position leave this unset.
    pub watch_replay_position: Option<WatchReplayPosition>,
    pub continue_token: Option<String>,
    /// Number of items remaining after the current page (set when continue_token is Some)
    pub remaining_item_count: Option<i64>,
}

/// Outcome of a historical-snapshot LIST (resourceVersionMatch=Exact or a
/// consistent paginated continuation). See
/// `DatastoreBackend::snapshot_resources_at_rv`.
#[derive(Debug, Clone)]
pub enum SnapshotAtRv {
    /// The requested rv is at or beyond the current state — the caller should
    /// serve the live list instead (the fast path; no reconstruction needed).
    Current,
    /// The requested rv predates the reconstructable history window. The caller
    /// must answer `410 Gone` (reason `Expired`).
    Expired,
    /// The reconstructed page: resources exactly as they existed at the
    /// requested rv, already selector-filtered and paginated.
    List(ResourceList),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListPageRequest {
    limit: Option<i64>,
    continue_token: Option<String>,
}

impl ListPageRequest {
    pub fn try_new(limit: Option<i64>, continue_token: Option<String>) -> Result<Self> {
        let limit = match limit {
            None | Some(0) => None,
            Some(limit) if limit > 0 => Some(limit),
            Some(limit) => {
                return Err(anyhow!(
                    "Invalid list limit {limit}: limit must be greater than or equal to 0"
                ));
            }
        };
        Ok(Self {
            limit,
            continue_token: continue_token.filter(|token| !token.is_empty()),
        })
    }

    pub fn unbounded() -> Self {
        Self {
            limit: None,
            continue_token: None,
        }
    }

    pub fn limit(&self) -> Option<i64> {
        self.limit
    }

    pub fn continue_token(&self) -> Option<&str> {
        self.continue_token.as_deref()
    }

    pub fn apply_to_sorted_resource_list(&self, mut list: ResourceList) -> ResourceList {
        if let Some(token) = self.continue_token() {
            list.items.retain(|item| item.name.as_str() > token);
        }

        list.continue_token = None;
        list.remaining_item_count = None;
        if let Some(limit) = self.limit
            && i64::try_from(list.items.len()).unwrap_or(i64::MAX) > limit
            && let Ok(limit) = usize::try_from(limit)
        {
            list.remaining_item_count =
                Some(i64::try_from(list.items.len() - limit).unwrap_or(i64::MAX));
            list.items.truncate(limit);
            list.continue_token = list.items.last().map(|item| item.name.clone());
        }
        list
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppliedOutboxRecord {
    pub idempotency_key: String,
    pub subject_key: String,
    pub operation: String,
    pub first_seen_ms: i64,
    pub applied_rv: Option<i64>,
    pub result_proto: Vec<u8>,
    #[serde(default)]
    pub status_stamp: Option<i64>,
}

impl From<AppliedOutboxRecord> for klights_cluster_core::LogApplyAppliedOutboxRow {
    fn from(record: AppliedOutboxRecord) -> Self {
        Self {
            idempotency_key: record.idempotency_key,
            subject_key: record.subject_key,
            operation: record.operation,
            first_seen_ms: record.first_seen_ms,
            applied_rv: record.applied_rv,
            result_proto: record.result_proto,
            status_stamp: record.status_stamp,
        }
    }
}

impl From<klights_cluster_core::LogApplyAppliedOutboxRow> for AppliedOutboxRecord {
    fn from(row: klights_cluster_core::LogApplyAppliedOutboxRow) -> Self {
        Self {
            idempotency_key: row.idempotency_key,
            subject_key: row.subject_key,
            operation: row.operation,
            first_seen_ms: row.first_seen_ms,
            applied_rv: row.applied_rv,
            result_proto: row.result_proto,
            status_stamp: row.status_stamp,
        }
    }
}
