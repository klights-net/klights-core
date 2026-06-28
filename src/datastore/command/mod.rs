//! Stable `StorageCommand` / `StorageResponse` codec for HA replication.
//!
//! Raft proposals carry logical mutations as `StorageCommand` values — never
//! SQLite WAL frames or backend-specific operations. Production apply converts
//! them into versioned log-apply commits before the raft state machine mutates
//! cluster state through the generic-to-typed helper path.
//!
//! ## Design invariants
//!
//! * **Node-local operations are excluded.**  Only `ClusterReplicated` and
//!   `ConfigReplicated` operations (per the domain map in `domain.rs`) have
//!   command variants.  Node-local ops (`pod_sandboxes`, `pod_networks`,
//!   `pod_workqueue`, `pod_endpoints`) stay as direct local backend calls.
//! * **Deterministic metadata.**  In HA mode the leader fills `CommandMeta`
//!   (RV, UID, timestamp, authoring node) before replication.  SingleNode
//!   mode generates them locally before building the command.
//! * **Dual codec.**  JSON (serde_json) and protobuf (prost) encode/decode
//!   paths must both round-trip cleanly.  A compatibility test decodes at
//!   least one older-version fixture so rolling upgrades have a concrete
//!   contract.
//! * **`#[non_exhaustive]`** on the serde enums so future variants don't
//!   break decode in rolling-upgrade scenarios.

use serde::{Deserialize, Serialize};

use super::types::{
    PatchKind, ResourceBatchOperation, ResourceBatchPutMode, ResourcePreconditions,
};
use prost::Message;

// ---------------------------------------------------------------------------
// Codec version
// ---------------------------------------------------------------------------

/// Codec version embedded in every `CommandMeta`.
///
/// Bumped **only** when a backward-incompatible change is made to the
/// command or response serialization.  Rolling upgrades must handle
/// `COMMAND_CODEC_VERSION - 1` at minimum.
pub const COMMAND_CODEC_VERSION: u32 = 2;

// ---------------------------------------------------------------------------
// CommandId
// ---------------------------------------------------------------------------

/// Unique identifier for a storage command.
///
/// Typically a UUID v4, but the codec does not enforce a format — any
/// unique string works.  Used for idempotency deduplication in the
/// replicated apply layer.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommandId(pub String);

impl CommandId {
    /// Generate a new random command ID (UUID v4).
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

impl Default for CommandId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for CommandId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ---------------------------------------------------------------------------
// CommandMeta
// ---------------------------------------------------------------------------

/// Metadata attached to every replicated command.
///
/// In HA mode the leader fills all fields before proposing the command
/// to the consensus layer.  In SingleNode mode the command builder
/// generates them locally.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandMeta {
    /// Unique command identifier for idempotency dedup.
    pub command_id: CommandId,
    /// Codec version of this command.  Must equal `COMMAND_CODEC_VERSION`
    /// for the running binary; decoders reject mismatches.
    pub codec_version: u32,
    /// Resource version assigned by the leader (or locally in SingleNode).
    /// In Raft mode this becomes the committed log index.
    pub resource_version: i64,
    /// UID for the target resource.  `None` for non-resource commands
    /// (config updates, namespace deletes).
    pub uid: Option<String>,
    /// Epoch-millis timestamp when the command was authored.
    pub timestamp_ms: i64,
    /// Name of the node that authored this command.
    pub authoring_node: String,
}

// ---------------------------------------------------------------------------
// StorageCommand (serde — JSON path)
// ---------------------------------------------------------------------------

/// A versioned storage command representing a logical mutation.
///
/// Only **ClusterReplicated** and **ConfigReplicated** operations have
/// command variants.  Node-local operations (sandbox, pod network,
/// workqueue, endpoints) are excluded — they stay as direct local
/// backend calls per the domain map in `domain.rs`.
///
/// Read operations (get/list/find) are also excluded — they never
/// cross the replication boundary.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StorageCommand {
    // -- Resource CRUD (ClusterReplicated) --
    /// Create a new K8s resource.
    CreateResource {
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        #[serde(with = "serde_bytes_base64")]
        data: serde_json::Value,
    },

    /// Update an existing K8s resource (full replace).
    UpdateResource {
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        #[serde(with = "serde_bytes_base64")]
        data: serde_json::Value,
        expected_rv: i64,
        preconditions: ResourcePreconditions,
    },

    /// Delete a K8s resource by key.
    DeleteResource {
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        preconditions: ResourcePreconditions,
    },

    /// Apply a merge patch to a resource.
    PatchResource {
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        patch_kind: PatchKind,
        #[serde(with = "serde_bytes_base64")]
        patch: serde_json::Value,
        preconditions: ResourcePreconditions,
        #[serde(default)]
        strict_resource_version: bool,
    },

    /// Update only the `.status` subtree of a resource.
    UpdateStatus {
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        #[serde(with = "serde_bytes_base64")]
        status: serde_json::Value,
        expected_rv: Option<i64>,
        preconditions: ResourcePreconditions,
        /// Worker-observed monotonic stamp for the Pod status snapshot this
        /// command carries. Producers (the kubelet status outbox) stamp each
        /// snapshot with a strictly increasing per-worker value so the leader
        /// can drop a stale snapshot that a retry/backoff lets overtake a
        /// newer one (the pipelined "resend arrives after a newer update"
        /// lost-update race). `None` for non-outbox writes (direct API/status
        /// updates, leases, nodes) which keep their existing ordering
        /// semantics. See the leader-side gate in the SQLite backend.
        #[serde(default)]
        observed_status_stamp: Option<i64>,
    },

    /// Apply multiple resource mutations under one resourceVersion.
    ApplyResourceBatch {
        operations: Vec<ResourceBatchOperation>,
    },

    // -- Namespace operations (ClusterReplicated) --
    /// Create a namespace.
    CreateNamespace {
        name: String,
        #[serde(with = "serde_bytes_base64")]
        data: serde_json::Value,
    },

    /// Update a namespace (full replace).
    UpdateNamespace {
        name: String,
        #[serde(with = "serde_bytes_base64")]
        data: serde_json::Value,
        expected_rv: i64,
    },

    /// Delete a namespace.
    DeleteNamespace { name: String },

    /// Delete all resources inside a namespace (cascading delete step).
    DeleteNamespaceContents { name: String },

    // -- Cluster-internal state (ClusterReplicated) --
    /// Allocate or return the existing /24 subnet for a node.
    ///
    /// `subnet` is either the cluster CIDR (for fresh allocations the
    /// allocator derives a /24 from it) or the exact /24 subnet for
    /// snapshot restore replay.
    AllocateNodeSubnet {
        node_name: String,
        subnet: String,
        node_ip: String,
    },

    /// Update the VXLAN VTEP MAC for a node.
    UpdateNodeVtepMac { node_name: String, vtep_mac: String },

    /// Persist peer-mode + hostport-range projected from Node annotations.
    UpdateNodePeerAttributes {
        node_name: String,
        mode: String,
        hostport_range: Option<String>,
    },

    /// Persist cluster-visible dataplane metadata for a node.
    ///
    /// Carries only public/typed metadata. The node-local WireGuard private
    /// key is never part of this replicated command surface.
    UpdateNodeDataplane {
        node_name: String,
        mode: String,
        encryption: String,
        public_key: Option<String>,
        endpoint: String,
        port: Option<u16>,
    },

    /// Delete a node's subnet record.
    DeleteNodeSubnet { node_name: String },

    /// Try to admit a concrete Pod UID into a cluster-visible name slot.
    PodSlotTryAdmit {
        namespace: String,
        pod_name: String,
        pod_uid: String,
        node_name: String,
    },

    /// Mark an admitted Pod UID as terminating in the cluster-visible slot.
    PodSlotMarkTerminating {
        namespace: String,
        pod_name: String,
        pod_uid: String,
        node_name: String,
    },

    /// Clear the cluster-visible slot only if it still belongs to this UID.
    PodSlotClearIfUid {
        namespace: String,
        pod_name: String,
        pod_uid: String,
        node_name: String,
    },

    /// Atomically move an active Pod row into the UID-bound cleanup-intent
    /// table and delete the active Pod API row.
    MovePodToCleanupIntent {
        node_name: String,
        namespace: String,
        pod_name: String,
        pod_uid: String,
        reason: String,
    },

    /// Remove one UID-bound Pod cleanup intent after local cleanup completed.
    DeletePodCleanupIntent {
        node_name: String,
        namespace: String,
        pod_name: String,
        pod_uid: String,
        reason: String,
    },

    /// Remove every Pod cleanup intent for a deleted Node.
    DeletePodCleanupIntentsForNode { node_name: String },

    // -- Watch history (ClusterReplicated) --
    /// Append a watch event row.  Followers use this to reconstruct their
    /// local watch replay buffer during catch-up.  `event_bytes` is the
    /// JSON-serialized `WatchEvent`; recipients decode with
    /// `serde_json::from_slice`.
    WatchEventAppend { event_bytes: Vec<u8>, rv: i64 },

    /// GC oldest watch event rows.  Replicated so every backend's
    /// watch history converges on the same retention window.
    GcWatchEvents { max_rows: i64, batch_cap: i64 },

    // -- Resource version counter (ConfigReplicated) --
    /// Advance the cluster-wide resource version counter.  Maps to
    /// `DatastoreBackend::advance_resource_version_after`.
    AdvanceResourceVersion { min_rv: i64, new_rv: i64 },

    // -- Bootstrap metadata (ClusterReplicated) --
    /// Ensure cluster identity metadata exists. Writes `cluster_id`
    /// and `leader_epoch` only on first apply; subsequent applies
    /// verify the cluster_id matches (idempotent). This command is
    /// the raft-backed replacement for direct `set_klights_meta`
    /// calls during seed bootstrap.
    EnsureClusterMetadata { cluster_id: String },

    /// Generic raft-backed `_klights_meta` key/value write. Routes
    /// through the raft proposer so every member's metadata table
    /// converges. Replaces direct `set_klights_meta` calls that
    /// previously bypassed raft and produced local-only rows.
    SetKlightsMeta { key: String, value: String },
}

impl StorageCommand {
    /// Returns a discriminant name for the command variant (for logging/metrics).
    pub fn variant_name(&self) -> &'static str {
        match self {
            StorageCommand::CreateResource { .. } => "CreateResource",
            StorageCommand::UpdateResource { .. } => "UpdateResource",
            StorageCommand::DeleteResource { .. } => "DeleteResource",
            StorageCommand::PatchResource { .. } => "PatchResource",
            StorageCommand::UpdateStatus { .. } => "UpdateStatus",
            StorageCommand::ApplyResourceBatch { .. } => "ApplyResourceBatch",
            StorageCommand::CreateNamespace { .. } => "CreateNamespace",
            StorageCommand::UpdateNamespace { .. } => "UpdateNamespace",
            StorageCommand::DeleteNamespace { .. } => "DeleteNamespace",
            StorageCommand::DeleteNamespaceContents { .. } => "DeleteNamespaceContents",
            StorageCommand::AllocateNodeSubnet { .. } => "AllocateNodeSubnet",
            StorageCommand::UpdateNodeVtepMac { .. } => "UpdateNodeVtepMac",
            StorageCommand::UpdateNodePeerAttributes { .. } => "UpdateNodePeerAttributes",
            StorageCommand::UpdateNodeDataplane { .. } => "UpdateNodeDataplane",
            StorageCommand::DeleteNodeSubnet { .. } => "DeleteNodeSubnet",
            StorageCommand::PodSlotTryAdmit { .. } => "PodSlotTryAdmit",
            StorageCommand::PodSlotMarkTerminating { .. } => "PodSlotMarkTerminating",
            StorageCommand::PodSlotClearIfUid { .. } => "PodSlotClearIfUid",
            StorageCommand::MovePodToCleanupIntent { .. } => "MovePodToCleanupIntent",
            StorageCommand::DeletePodCleanupIntent { .. } => "DeletePodCleanupIntent",
            StorageCommand::DeletePodCleanupIntentsForNode { .. } => {
                "DeletePodCleanupIntentsForNode"
            }
            StorageCommand::WatchEventAppend { .. } => "WatchEventAppend",
            StorageCommand::GcWatchEvents { .. } => "GcWatchEvents",
            StorageCommand::AdvanceResourceVersion { .. } => "AdvanceResourceVersion",
            StorageCommand::EnsureClusterMetadata { .. } => "EnsureClusterMetadata",
            StorageCommand::SetKlightsMeta { .. } => "SetKlightsMeta",
        }
    }

    /// Build a `CreateResource` command from a JSON `Value`.
    pub fn create_resource(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<&str>,
        name: impl Into<String>,
        data: serde_json::Value,
    ) -> Self {
        Self::CreateResource {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: namespace.map(String::from),
            name: name.into(),
            data,
        }
    }

    /// Build an `UpdateResource` command from a JSON `Value` and an
    /// optimistic-concurrency expected resource version.
    pub fn update_resource(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<&str>,
        name: impl Into<String>,
        data: serde_json::Value,
        expected_rv: i64,
    ) -> Self {
        Self::UpdateResource {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: namespace.map(String::from),
            name: name.into(),
            data,
            expected_rv,
            preconditions: ResourcePreconditions {
                uid: None,
                resource_version: Some(expected_rv),
            },
        }
    }

    /// Build an `UpdateStatus` command from a JSON status subtree.
    pub fn update_status(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<&str>,
        name: impl Into<String>,
        status: serde_json::Value,
        expected_rv: Option<i64>,
    ) -> Self {
        Self::UpdateStatus {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: namespace.map(String::from),
            name: name.into(),
            status,
            expected_rv,
            preconditions: ResourcePreconditions {
                uid: None,
                resource_version: expected_rv,
            },
            observed_status_stamp: None,
        }
    }

    /// Build a `PatchResource` command.
    pub fn patch_resource(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<&str>,
        name: impl Into<String>,
        patch_kind: PatchKind,
        patch: serde_json::Value,
    ) -> Self {
        Self::PatchResource {
            api_version: api_version.into(),
            kind: kind.into(),
            namespace: namespace.map(String::from),
            name: name.into(),
            patch_kind,
            patch,
            preconditions: ResourcePreconditions::default(),
            strict_resource_version: false,
        }
    }

    pub fn apply_resource_batch(operations: Vec<ResourceBatchOperation>) -> Self {
        Self::ApplyResourceBatch { operations }
    }

    /// Build a `CreateNamespace` command from a JSON namespace body.
    pub fn create_namespace(name: impl Into<String>, data: serde_json::Value) -> Self {
        Self::CreateNamespace {
            name: name.into(),
            data,
        }
    }
}

// ---------------------------------------------------------------------------
// StorageResponse (serde — JSON path)
// ---------------------------------------------------------------------------

/// Backend-neutral response from applying a `StorageCommand`.
///
/// Carries enough data for the caller to construct the API response
/// without depending on any storage engine's row types.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StorageResponse {
    /// A resource was created, updated, or patched.
    Resource {
        resource_version: i64,
        #[serde(with = "serde_bytes_base64")]
        data: serde_json::Value,
    },

    /// Command completed with no data payload (deletes, config updates).
    Ack { resource_version: i64 },

    /// Node subnet allocation result.  Carries the F2-04 peer attributes
    /// (`mode`, `hostport_range`) so rootless peers don't lose information
    /// when the response crosses the replication boundary.
    NodeSubnet {
        node_name: String,
        subnet: String,
        subnet_base_int: u32,
        vtep_ip: String,
        node_ip: String,
        mode: String,
        hostport_range: Option<String>,
    },

    /// Command application failed.
    Error { message: String },
}

// ---------------------------------------------------------------------------
// CommandError — typed error for command application
// ---------------------------------------------------------------------------

/// Typed errors that a backend may return when applying a command.
///
/// These map to Kubernetes API error semantics so the replicated layer
/// can translate them without understanding storage-engine internals.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandError {
    /// Optimistic concurrency conflict (maps to 409 Conflict).
    Conflict { message: String },

    /// Target resource or namespace not found (maps to 404 Not Found).
    NotFound { message: String },

    /// Generic internal error (maps to 500 Internal Server Error).
    Internal { message: String },
}

// ---------------------------------------------------------------------------
// Protobuf wire types (prost::Message)
// ---------------------------------------------------------------------------

pub mod codec;
/// Protobuf wire type for `CommandMeta`.
pub mod proto;

pub use codec::*;
pub use proto::*;
