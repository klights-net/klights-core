//! Durable values shared by focused cluster-persistence ports.
//!
//! These values contain no SQLite/Redb or application-layer types. Concrete
//! embedded adapters and root composition use the same lower-owned contract.

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

use klights_cluster_core::Resource;

pub const POD_CLEANUP_REASON_NODE_LOST: &str = "NodeLost";

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
    pub const fn retention_boundary(&self) -> crate::ReplayRetentionBoundary {
        if self.position_is_exact {
            crate::ReplayRetentionBoundary::Exact(WatchReplayPosition {
                resource_version: self.floor_resource_version,
                event_id: self.floor_event_id,
                resource_version_filter_through_event_id: 0,
            })
        } else {
            crate::ReplayRetentionBoundary::LegacyRvOnly {
                resource_version: self.floor_resource_version,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceListOptions<'a> {
    pub label_selector: Option<&'a str>,
    pub field_selector: Option<&'a str>,
    pub limit: Option<i64>,
    pub continue_token: Option<&'a str>,
}

impl<'a> ResourceListOptions<'a> {
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
    use serde_json::json;

    #[test]
    fn resource_helpers_convert_watch_event_and_added_catchup() {
        let resource = Resource::try_from_data(std::sync::Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "frontend",
                "namespace": "default",
                "uid": "uid-frontend",
                "resourceVersion": "42"
            }
        })))
        .unwrap();
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
/// consistent paginated continuation). See the focused persistence contract.
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
