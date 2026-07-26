//! Datastore types shared across the trait surface and every backend.
//!
//! Anything used in a `DatastoreBackend` method signature lives here so the
//! trait module stays SQL-free and a future backend implementor can build
//! against `crate::datastore::*` without pulling in SQLite-specific code.

use anyhow::{Result, anyhow};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::borrow::Cow;
use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::watch::WatchEvent;
use klights_types::{NodeName, PodSubnet};

use klights_cluster_core::Resource;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PodSlotAdmissionState {
    Admitted,
    Terminating,
}

impl PodSlotAdmissionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "Admitted",
            Self::Terminating => "Terminating",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "Admitted" => Ok(Self::Admitted),
            "Terminating" => Ok(Self::Terminating),
            other => Err(anyhow!("invalid pod slot admission state {other:?}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodSlotAdmissionResult {
    Admitted {
        resource_version: i64,
    },
    Blocked {
        blocking_uid: String,
        blocking_node: String,
        state: PodSlotAdmissionState,
        resource_version: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodSlotMutationResult {
    Changed { resource_version: i64 },
    Unchanged { resource_version: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodSlotClearResult {
    Cleared {
        resource_version: i64,
    },
    NotFound,
    UidMismatch {
        blocking_uid: String,
        blocking_node: String,
        state: PodSlotAdmissionState,
        resource_version: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodSlotAdmissionEvent {
    Changed {
        namespace: String,
        pod_name: String,
        pod_uid: String,
        state: PodSlotAdmissionState,
        resource_version: i64,
    },
    Cleared {
        namespace: String,
        pod_name: String,
        pod_uid: String,
        resource_version: i64,
    },
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
    pub const fn retention_boundary(
        &self,
    ) -> crate::datastore::replay_retention::ReplayRetentionBoundary {
        if self.position_is_exact {
            crate::datastore::replay_retention::ReplayRetentionBoundary::Exact(
                WatchReplayPosition {
                    resource_version: self.floor_resource_version,
                    event_id: self.floor_event_id,
                    resource_version_filter_through_event_id: 0,
                },
            )
        } else {
            crate::datastore::replay_retention::ReplayRetentionBoundary::LegacyRvOnly {
                resource_version: self.floor_resource_version,
            }
        }
    }
}

impl klights_cluster_core::ResourceEventObject for WatchEvent {
    fn resource_object(&self) -> &Arc<Value> {
        &self.object
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodNetworkAllocationPod<'a> {
    pub namespace: &'a str,
    pub name: &'a str,
    pub uid: &'a str,
}

impl<'a> PodNetworkAllocationPod<'a> {
    pub fn new(namespace: &'a str, name: &'a str, uid: &'a str) -> Self {
        Self {
            namespace,
            name,
            uid,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodNetworkAllocationSubnet {
    pub base_int: u32,
    pub size: u32,
}

impl PodNetworkAllocationSubnet {
    pub fn new(base_int: u32, size: u32) -> Self {
        Self { base_int, size }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodNetworkAllocationLink<'a> {
    pub veth_host: &'a str,
    pub netns_path: &'a str,
}

impl<'a> PodNetworkAllocationLink<'a> {
    pub fn new(veth_host: &'a str, netns_path: &'a str) -> Self {
        Self {
            veth_host,
            netns_path,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PodNetworkAllocationRequest<'a> {
    pub sandbox_id: &'a str,
    pub pod: PodNetworkAllocationPod<'a>,
    pub subnet: PodNetworkAllocationSubnet,
    pub link: PodNetworkAllocationLink<'a>,
}

impl<'a> PodNetworkAllocationRequest<'a> {
    pub fn new(
        sandbox_id: &'a str,
        pod: PodNetworkAllocationPod<'a>,
        subnet: PodNetworkAllocationSubnet,
        link: PodNetworkAllocationLink<'a>,
    ) -> Self {
        Self {
            sandbox_id,
            pod,
            subnet,
            link,
        }
    }

    pub fn into_owned(self) -> OwnedPodNetworkAllocationRequest {
        OwnedPodNetworkAllocationRequest {
            sandbox_id: self.sandbox_id.to_string(),
            namespace: self.pod.namespace.to_string(),
            pod_name: self.pod.name.to_string(),
            pod_uid: self.pod.uid.to_string(),
            subnet_base_int: self.subnet.base_int,
            subnet_size: self.subnet.size,
            veth_host: self.link.veth_host.to_string(),
            netns_path: self.link.netns_path.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedPodNetworkAllocationRequest {
    pub sandbox_id: String,
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub subnet_base_int: u32,
    pub subnet_size: u32,
    pub veth_host: String,
    pub netns_path: String,
}

impl OwnedPodNetworkAllocationRequest {
    pub fn as_borrowed(&self) -> PodNetworkAllocationRequest<'_> {
        PodNetworkAllocationRequest::new(
            &self.sandbox_id,
            PodNetworkAllocationPod::new(&self.namespace, &self.pod_name, &self.pod_uid),
            PodNetworkAllocationSubnet::new(self.subnet_base_int, self.subnet_size),
            PodNetworkAllocationLink::new(&self.veth_host, &self.netns_path),
        )
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

/// Durable watch replay row with routing/cursor fields lifted out of the JSON
/// object payload. Selectorless JSON watch streams can use this shape to avoid
/// parsing `watch_events.data` just to recover metadata already stored in
/// columns.
#[derive(Debug, Clone)]
pub struct RawWatchEvent {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub resource_version: i64,
    pub event_type: Cow<'static, str>,
    pub object_json: Bytes,
}

impl RawWatchEvent {
    pub fn topic(&self) -> klights_watch::WatchTopic {
        klights_watch::WatchTopic::new(&self.api_version, &self.kind)
    }

    pub fn key(&self) -> (Option<String>, String) {
        (self.namespace.clone(), self.name.clone())
    }
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

    pub fn into_watch_event(self) -> WatchEvent {
        let CatchUpResource {
            resource,
            event_type,
        } = self;
        let Resource {
            api_version,
            kind,
            namespace,
            name,
            uid,
            resource_version,
            data,
            ..
        } = resource;

        // Cheap if we hold the only Arc ref (steady-state); copy-on-write otherwise.
        let mut data = Arc::unwrap_or_clone(data);
        if let Some(obj) = data.as_object_mut() {
            obj.insert("apiVersion".to_string(), serde_json::json!(api_version));
            obj.insert("kind".to_string(), serde_json::json!(kind));
            let metadata = obj
                .entry("metadata")
                .or_insert_with(|| serde_json::json!({}));
            if let Some(meta) = metadata.as_object_mut() {
                meta.insert("name".to_string(), serde_json::json!(name));
                meta.insert("uid".to_string(), serde_json::json!(uid));
                if let Some(namespace) = namespace {
                    meta.insert("namespace".to_string(), serde_json::json!(namespace));
                }
                meta.insert(
                    "resourceVersion".to_string(),
                    serde_json::json!(resource_version.to_string()),
                );
            }
        }

        WatchEvent::from_type(event_type.as_ref(), data)
    }
}

#[cfg(test)]
mod resource_arc_tests {
    use super::*;
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

/// One row from the `node_subnets` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSubnet {
    pub node_name: NodeName,
    /// CIDR block for this node's pods (e.g. "10.42.1.0/24").
    pub subnet: PodSubnet,
    /// Base address of `subnet` as a `u32` (host byte order). Stored for DB allocation logic.
    pub subnet_base_int: u32,
    /// First address of the subnet, retained for row-shape compatibility with
    /// older node-subnet allocation code.
    pub gateway_ip: Ipv4Addr,
    /// Host's primary underlay IP used for direct/WireGuard peer routing.
    pub node_ip: Ipv4Addr,
    /// Peer mode projected from the node's `klights.io/mode` annotation
    /// (F2-04). Defaults to `Root` for legacy rows or pre-F2-05 nodes.
    pub mode: klights_types::NodePeerMode,
    /// Rootless host-port graft range projected from `klights.io/hostport-range`.
    /// `None` for root peers; `Some` for rootless peers when the annotation
    /// parses cleanly.
    pub hostport_range: Option<klights_types::HostPortRange>,
}

/// Pod-level network state captured at the CNI boundary. Returned by
/// `DatastoreBackend::get_pod_network` so callers (network teardown,
/// IP release, host-port flushing) can address each piece of state by
/// name instead of by tuple position.
///
/// Future hybrid clusters will likely need a `network_provider_kind` field
/// here so the routing path can pick the right tear-down primitive — adding
/// fields to the struct is a
/// non-breaking change for every call site, which the previous
/// `(String, String, String)` tuple was not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodNetworkEndpoint {
    pub ip_addr: String,
    pub veth_host: String,
    pub netns_path: String,
}

/// Identifier of one CRI sandbox we created for a pod. Returned in
/// bulk by `DatastoreBackend::list_sandboxes` so the GC and shutdown
/// reconcilers can scan the live sandbox set without juggling tuple
/// positions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxRef {
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub sandbox_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PodWorkqueueKind {
    Pod,
    Namespace,
}

impl PodWorkqueueKind {
    pub fn as_str(self) -> &'static str {
        match self {
            PodWorkqueueKind::Pod => "pod",
            PodWorkqueueKind::Namespace => "namespace",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "pod" => Ok(Self::Pod),
            "namespace" => Ok(Self::Namespace),
            other => Err(anyhow!("invalid pod_workqueue kind '{}'", other)),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PodWorkqueueEntry {
    pub id: i64,
    pub kind: PodWorkqueueKind,
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub payload: Value,
    pub attempt_count: i64,
    pub next_attempt_at_ms: i64,
}

/// Reachability mode recorded in the `pod_endpoints` table.
///
/// `EncryptedDirect` — pod is reachable directly at its pod IP through the
/// encrypted pod-CIDR dataplane.
/// `Hostport` — pod is reachable via (host_ip, host_port) on its node, used
/// in rootless / hybrid clusters where direct overlay reach is unavailable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PodEndpointMode {
    EncryptedDirect,
    Hostport,
}

impl PodEndpointMode {
    pub fn as_str(self) -> &'static str {
        match self {
            PodEndpointMode::EncryptedDirect => "encrypted_direct",
            PodEndpointMode::Hostport => "hostport",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "encrypted_direct" => Ok(PodEndpointMode::EncryptedDirect),
            "hostport" => Ok(PodEndpointMode::Hostport),
            other => Err(anyhow!("unknown pod_endpoint mode: {}", other)),
        }
    }
}

/// One row of the `pod_endpoints` table — cross-mode reachability for one pod.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodEndpointRow {
    pub pod_uid: String,
    pub namespace: String,
    pub pod_name: String,
    pub node_name: String,
    pub mode: PodEndpointMode,
    pub pod_ip: Ipv4Addr,
    pub node_ip: Ipv4Addr,
    pub host_port_tcp: Option<u16>,
    pub host_port_udp: Option<u16>,
    pub generation: i64,
    pub updated_at: i64,
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

/// Internal-only event emitted by `pod_endpoints` CRUD calls.
///
/// Distinct from K8s `WatchEvent` because pod_endpoints is not a K8s
/// resource — these events never leave the daemon. Phase 2 reconcilers
/// (rootless DNAT writer, bypass4netns sync) consume this stream via
/// `Datastore::subscribe_pod_endpoints`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PodEndpointEvent {
    Upsert(PodEndpointRow),
    Delete { pod_uid: String, pod_ip: Ipv4Addr },
}

/// Pending watch event staged during a DB write, to be broadcast after commit.
///
/// Returned from create/update/patch/delete operations so callers can broadcast
/// the event outside the transaction boundary. Lives at the trait surface so
/// any backend's mutation methods can stage events the same way.
#[derive(Clone, Debug)]
pub struct PendingWatchEvent {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub resource_version: i64,
    #[cfg(test)]
    pub event: WatchEvent,
}

impl PendingWatchEvent {
    pub fn from_signal_metadata(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<&str>,
        resource_version: i64,
    ) -> Self {
        let api_version = api_version.into();
        let kind = kind.into();
        let namespace = namespace.map(str::to_string);

        Self {
            #[cfg(test)]
            event: WatchEvent::modified(serde_json::json!({
                "apiVersion": api_version.clone(),
                "kind": kind.clone(),
                "metadata": {
                    "namespace": namespace.clone(),
                    "resourceVersion": resource_version.to_string()
                }
            })),
            api_version,
            kind,
            namespace,
            resource_version,
        }
    }

    #[cfg(test)]
    pub fn from_event(event: WatchEvent) -> Self {
        let object = event.object.as_ref();
        let metadata = object.get("metadata").unwrap_or(&Value::Null);
        Self {
            api_version: object
                .get("apiVersion")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            kind: object
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            namespace: metadata
                .get("namespace")
                .and_then(Value::as_str)
                .map(str::to_string),
            resource_version: metadata
                .get("resourceVersion")
                .and_then(Value::as_str)
                .and_then(|rv| rv.parse::<i64>().ok())
                .unwrap_or_default(),
            event,
        }
    }
}

#[cfg(test)]
mod pod_endpoint_mode_tests {
    use super::PodEndpointMode;

    #[test]
    fn encrypted_direct_is_live_pod_endpoint_label() {
        assert_eq!(
            PodEndpointMode::EncryptedDirect.as_str(),
            "encrypted_direct"
        );
        assert_eq!(
            PodEndpointMode::parse("encrypted_direct").unwrap(),
            PodEndpointMode::EncryptedDirect
        );
        assert!(PodEndpointMode::parse("vxlan").is_err());
        assert_eq!(
            PodEndpointMode::parse("hostport").unwrap(),
            PodEndpointMode::Hostport
        );
    }
}

#[cfg(test)]
mod replay_retention_tests {
    use super::WatchReplayPosition;
    use crate::datastore::replay_retention::{ReplayAvailability, ReplayRetentionBoundary};

    #[test]
    fn exact_and_legacy_retention_boundaries_classify_positions() {
        let exact = ReplayRetentionBoundary::Exact(WatchReplayPosition {
            resource_version: 10,
            event_id: 40,
            resource_version_filter_through_event_id: 0,
        });
        let legacy = ReplayRetentionBoundary::LegacyRvOnly {
            resource_version: 10,
        };

        let cases = [
            (
                exact,
                WatchReplayPosition {
                    resource_version: 10,
                    event_id: 39,
                    resource_version_filter_through_event_id: 0,
                },
                ReplayAvailability::Expired,
            ),
            (
                exact,
                WatchReplayPosition {
                    resource_version: 10,
                    event_id: 40,
                    resource_version_filter_through_event_id: 0,
                },
                ReplayAvailability::Available,
            ),
            (
                exact,
                WatchReplayPosition::from_resource_version(10),
                ReplayAvailability::Available,
            ),
            (
                legacy,
                WatchReplayPosition::from_resource_version(9),
                ReplayAvailability::Expired,
            ),
            (
                legacy,
                WatchReplayPosition::from_resource_version(10),
                ReplayAvailability::Available,
            ),
            (
                legacy,
                WatchReplayPosition {
                    resource_version: 10,
                    event_id: 40,
                    resource_version_filter_through_event_id: 0,
                },
                ReplayAvailability::Expired,
            ),
            // A resource-version-filtered-through cursor (resume from RV,
            // filtering rows up to a later event id) expires when the RV
            // anchor precedes the floor even though the filter-through event
            // id is the subscription high-water and never precedes it.
            (
                exact,
                WatchReplayPosition::from_resource_version_through_event_id(9, 100),
                ReplayAvailability::Expired,
            ),
            // ... and when the event window itself precedes the floor.
            (
                exact,
                WatchReplayPosition::from_resource_version_through_event_id(10, 39),
                ReplayAvailability::Expired,
            ),
            // Both anchor and window at or above the floor stay available.
            (
                exact,
                WatchReplayPosition::from_resource_version_through_event_id(10, 40),
                ReplayAvailability::Available,
            ),
        ];

        for (boundary, cursor, expected) in cases {
            assert_eq!(boundary.classify(cursor), expected);
        }

        let newer_rv = ReplayRetentionBoundary::Exact(WatchReplayPosition {
            resource_version: 20,
            event_id: 5,
            resource_version_filter_through_event_id: 0,
        });
        let newer_event = ReplayRetentionBoundary::Exact(WatchReplayPosition {
            resource_version: 10,
            event_id: 40,
            resource_version_filter_through_event_id: 0,
        });
        assert_eq!(
            ReplayRetentionBoundary::classify_all(
                [newer_rv, newer_event],
                WatchReplayPosition {
                    resource_version: 10,
                    event_id: 10,
                    resource_version_filter_through_event_id: 0,
                },
            ),
            ReplayAvailability::Expired,
            "scope composition must keep real boundaries instead of pairing max RV with max event ID"
        );
    }
}
