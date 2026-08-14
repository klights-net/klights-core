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
pub const RES_CLUSTER: TableDefinition<&[u8], (u64, &[u8])> = TableDefinition::new("res_cluster");
pub const RES_NS: TableDefinition<&[u8], (u64, &[u8])> = TableDefinition::new("res_ns");

pub const NAMESPACES: TableDefinition<&str, &[u8]> = TableDefinition::new("namespaces");

pub const WATCH_EVENTS_LEGACY: TableDefinition<u64, &[u8]> = TableDefinition::new("watch_events");
/// Apply-order keyed durable watch log. The resourceVersion lives in the
/// encoded value, allowing multiple resource identities to share one RV.
pub const WATCH_EVENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("watch_events_v2");
/// Derived ordered membership of durable history. Keys encode one resource
/// identity followed by the big-endian event id; values repeat that id so a
/// positioned page can seek an identity window and dereference only the
/// retained events for those identities. This is derived from `WATCH_EVENTS`
/// and is rebuilt at the open/restore boundary when upgrading older stores.
pub const RESOURCE_HISTORY_BY_IDENTITY: TableDefinition<&[u8], u64> =
    TableDefinition::new("resource_history_by_identity_v1");
/// Derived lexical current-resource identity index.  Unlike the physical
/// resource tables' length-prefixed keys, this is ordered as Kubernetes LIST
/// requires: scope, apiVersion, kind, namespace, name.
pub const RESOURCE_CURRENT_BY_IDENTITY: TableDefinition<&[u8], u8> =
    TableDefinition::new("resource_current_by_identity_v1");
pub const WATCH_REPLAY_FLOORS: TableDefinition<&[u8], u64> =
    TableDefinition::new("watch_replay_floors");
pub const WATCH_REPLAY_POSITION_FLOORS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("watch_replay_position_floors");

pub const APPLIED_OUTBOX: TableDefinition<&str, &[u8]> = TableDefinition::new("applied_outbox");
pub const OUTBOX_STREAM_WATERMARKS: TableDefinition<&[u8], i64> =
    TableDefinition::new("outbox_stream_watermarks");

// Materialized owner-reference table.  Key: ordered bytes
// (owner_uid + NUL + tag_byte + owned_av + NUL + owned_kind + NUL
//  + ns + NUL + owned_name).
// Value: (resource_version: u64, body: Vec<u8> /* JSON */).
// Range scan by owner_uid prefix returns owned resources directly.
pub const RESOURCES_BY_OWNER: TableDefinition<&[u8], (u64, &[u8])> =
    TableDefinition::new("resources_by_owner");

// Secondary index: resource_version → resource_key for list-by-RV.
pub const RV_TO_KEY: TableDefinition<u64, &[u8]> = TableDefinition::new("rv_to_key");

pub const NODE_SUBNETS: TableDefinition<&str, &[u8]> = TableDefinition::new("node_subnets");

pub const NODE_DATAPLANE: TableDefinition<&str, &[u8]> = TableDefinition::new("node_dataplane");

pub const POD_CLEANUP_INTENTS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("pod_cleanup_intents");

pub const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

/// klights_meta table — mirrors SQLite's _klights_meta for backend-neutral
/// metadata (cluster_id, join_token, etc.).  Key/value are both UTF-8 strings.
pub const KLIGHTS_META: TableDefinition<&str, &str> = TableDefinition::new("klights_meta");
