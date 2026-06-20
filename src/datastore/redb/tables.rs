//! Centralized typed-table definitions for the redb backend.
//! Every `TableDefinition` used by `crate::datastore::redb::*` lives here.
//! This is the redb analogue of `sqlite/queries.rs` (per DSB-00b).

use ::redb::TableDefinition;

// Two resource tables — cluster-scoped and namespaced — avoid the
// scope_byte prefix problem and let range scans naturally cover the
// right set of keys.
//
// Key layout (both tables): [len(av)][av][len(kind)][kind][ns_part?][len(name)][name]
//   ns_part (cluster table): omitted
//   ns_part (namespaced table): [len(ns)][ns]
// Value: (resource_version: u64, body: Vec<u8> /* JSON */).
pub(super) const RES_CLUSTER: TableDefinition<&[u8], (u64, &[u8])> =
    TableDefinition::new("res_cluster");
pub(super) const RES_NS: TableDefinition<&[u8], (u64, &[u8])> = TableDefinition::new("res_ns");

pub(super) const NAMESPACES: TableDefinition<&str, &[u8]> = TableDefinition::new("namespaces");

pub(super) const WATCH_EVENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("watch_events");
pub(super) const WATCH_REPLAY_FLOORS: TableDefinition<&[u8], u64> =
    TableDefinition::new("watch_replay_floors");

pub(super) const APPLIED_OUTBOX: TableDefinition<&str, &[u8]> =
    TableDefinition::new("applied_outbox");

// Materialized owner-reference table.  Key: ordered bytes
// (owner_uid + NUL + tag_byte + owned_av + NUL + owned_kind + NUL
//  + ns + NUL + owned_name).
// Value: (resource_version: u64, body: Vec<u8> /* JSON */).
// Range scan by owner_uid prefix returns owned resources directly.
pub(super) const RESOURCES_BY_OWNER: TableDefinition<&[u8], (u64, &[u8])> =
    TableDefinition::new("resources_by_owner");

// Secondary index: resource_version → resource_key for list-by-RV.
pub(super) const RV_TO_KEY: TableDefinition<u64, &[u8]> = TableDefinition::new("rv_to_key");

pub(super) const POD_SANDBOXES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("pod_sandboxes");

pub(super) const POD_NETWORKS: TableDefinition<&str, &[u8]> = TableDefinition::new("pod_networks");

pub(super) const NODE_SUBNETS: TableDefinition<&str, &[u8]> = TableDefinition::new("node_subnets");

pub(super) const POD_SLOT_ADMISSIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("pod_slot_admissions");

pub(super) const NODE_DATAPLANE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("node_dataplane");

pub(super) const POD_ENDPOINTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("pod_endpoints");

pub(super) const POD_WORKQUEUE: TableDefinition<u64, &[u8]> = TableDefinition::new("pod_workqueue");

pub(super) const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

/// klights_meta table — mirrors SQLite's _klights_meta for backend-neutral
/// metadata (cluster_id, join_token, etc.).  Key/value are both UTF-8 strings.
pub(super) const KLIGHTS_META: TableDefinition<&str, &str> = TableDefinition::new("klights_meta");
