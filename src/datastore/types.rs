//! Datastore types shared across the trait surface and every backend.
//!
//! Anything used in a `DatastoreBackend` method signature lives here so the
//! trait module stays SQL-free and a future backend implementor can build
//! against `crate::datastore::*` without pulling in SQLite-specific code.

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::networking::{NodeName, PodSubnet, VtepMac};
use crate::watch::WatchEvent;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource {
    pub id: i64,
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    /// Kubernetes object UID, mirrored from `metadata.uid` and the backend UID
    /// column. This is the identity-stable guard for delete/recreate slots.
    pub uid: String,
    pub resource_version: i64,
    /// JSON body. Held as `Arc<Value>` so cloning a `Resource` is O(1) refcount
    /// bump instead of a deep-walk of every Map/String/Vec in the tree. Mutate
    /// via `Arc::make_mut(&mut resource.data)` for copy-on-write.
    pub data: Arc<Value>,
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

/// Leader metadata stamped into a replica backup during full snapshot restore.
///
/// A running replica does not read this metadata. It is persisted into the
/// backup `cluster.db` so that a later restart-as-leader sees the same cluster
/// identity instead of generating a new one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatedSnapshotMetadata {
    pub cluster_id: String,
    pub leader_epoch: i64,
    pub membership: Option<crate::control_plane::client::membership::ClusterMembership>,
}

impl Resource {
    pub fn uid_from_data(data: &Value) -> String {
        data.pointer("/metadata/uid")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string()
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

/// Optimistic write guards for a single Kubernetes object identity.
///
/// `resource_version` protects against stale snapshots of the same object.
/// `uid` protects a name slot across delete/recreate, where the name is reused
/// but the Kubernetes object identity changed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourcePreconditions {
    pub uid: Option<String>,
    pub resource_version: Option<i64>,
}

impl ResourcePreconditions {
    pub fn resource_version(resource_version: i64) -> Self {
        Self {
            uid: None,
            resource_version: Some(resource_version),
        }
    }

    pub fn uid(uid: impl Into<String>) -> Self {
        Self {
            uid: Some(uid.into()),
            resource_version: None,
        }
    }

    pub fn uid_and_resource_version(uid: impl Into<String>, resource_version: i64) -> Self {
        Self {
            uid: Some(uid.into()),
            resource_version: Some(resource_version),
        }
    }

    pub fn from_resource(resource: &Resource) -> Self {
        Self::uid_and_resource_version(resource.uid.clone(), resource.resource_version)
    }

    pub fn from_metadata(metadata: &Value, resource_version: i64) -> Result<Self> {
        let uid = metadata
            .get("uid")
            .and_then(|v| v.as_str())
            .filter(|uid| !uid.trim().is_empty())
            .ok_or_else(|| anyhow!("metadata.uid is required for UID-qualified write"))?;
        Ok(Self::uid_and_resource_version(
            uid.to_string(),
            resource_version,
        ))
    }
}

/// Patch format for resource updates that do not use optimistic concurrency checks.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PatchKind {
    Merge,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourcePatchRequest {
    pub patch_kind: PatchKind,
    pub patch: Value,
    pub preconditions: ResourcePreconditions,
}

impl ResourcePatchRequest {
    pub fn new(patch_kind: PatchKind, patch: Value, preconditions: ResourcePreconditions) -> Self {
        Self {
            patch_kind,
            patch,
            preconditions,
        }
    }

    pub fn without_preconditions(patch_kind: PatchKind, patch: Value) -> Self {
        Self::new(patch_kind, patch, ResourcePreconditions::default())
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

impl CatchUpResource {
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

    fn sample() -> Resource {
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "p".to_string(),
            uid: "uid-p".to_string(),
            resource_version: 1,
            data: Arc::new(json!({"spec": {"x": 1}, "status": {"y": 2}})),
        }
    }

    #[test]
    fn cloning_resource_is_shallow_arc_bump() {
        let r = sample();
        assert_eq!(Arc::strong_count(&r.data), 1);
        let r2 = r.clone();
        assert_eq!(Arc::strong_count(&r.data), 2);
        assert!(Arc::ptr_eq(&r.data, &r2.data));
    }

    #[test]
    fn make_mut_forks_when_shared() {
        let r = sample();
        let mut r2 = r.clone();
        assert_eq!(Arc::strong_count(&r.data), 2);

        // Mutate r2; original r must be untouched.
        Arc::make_mut(&mut r2.data)
            .as_object_mut()
            .unwrap()
            .insert("forked".to_string(), json!(true));

        assert_eq!(Arc::strong_count(&r.data), 1);
        assert_eq!(Arc::strong_count(&r2.data), 1);
        assert!(!Arc::ptr_eq(&r.data, &r2.data));
        assert!(r.data.get("forked").is_none());
        assert_eq!(r2.data.get("forked"), Some(&json!(true)));
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
        if let Some(limit) = self.limit {
            let limit = limit as usize;
            if list.items.len() > limit {
                list.remaining_item_count = Some((list.items.len() - limit) as i64);
                list.items.truncate(limit);
                list.continue_token = list.items.last().map(|item| item.name.clone());
            }
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
    /// Legacy VXLAN VTEP IP (first address of the subnet) when an explicit
    /// VXLAN route mode owns `klights.vxlan`.
    pub vtep_ip: Ipv4Addr,
    /// Kernel-assigned MAC of `klights.vxlan` when an explicit VXLAN route mode
    /// creates it. `None` is expected for default WireGuard, direct-route, and
    /// rootless peers.
    pub vtep_mac: Option<VtepMac>,
    /// Host's primary underlay IP (used as the VXLAN UDP source/destination).
    pub node_ip: Ipv4Addr,
    /// Peer mode projected from the node's `klights.io/mode` annotation
    /// (F2-04). Defaults to `Root` for legacy rows or pre-F2-05 nodes.
    pub mode: crate::controllers::annotations::NodePeerMode,
    /// Rootless host-port graft range projected from `klights.io/hostport-range`.
    /// `None` for root peers; `Some` for rootless peers when the annotation
    /// parses cleanly.
    pub hostport_range: Option<crate::networking::types::HostPortRange>,
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
/// `Vxlan` — pod is reachable directly at its pod IP via the cluster overlay.
/// `Hostport` — pod is reachable via (host_ip, host_port) on its node, used
/// in rootless / hybrid clusters where direct overlay reach is unavailable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PodEndpointMode {
    Vxlan,
    Hostport,
}

impl PodEndpointMode {
    pub fn as_str(self) -> &'static str {
        match self {
            PodEndpointMode::Vxlan => "vxlan",
            PodEndpointMode::Hostport => "hostport",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "vxlan" => Ok(PodEndpointMode::Vxlan),
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
    pub event: WatchEvent,
}
