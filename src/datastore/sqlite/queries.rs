//! Centralized SQL strings for the SQLite backend.
//!
//! Every CREATE/SELECT/INSERT/UPDATE/DELETE statement issued by
//! `crate::datastore::sqlite::*` lives here. Schema bootstrap statements
//! (CREATE TABLE / CREATE INDEX) stay in `schema.rs` because they are
//! conceptually "the schema" not "queries against it"; everything else is
//! here.
//!
//! When porting to a second backend (`postgres/queries.rs`,
//! `mysql/queries.rs`), this is the only file that needs translation.

// ---------------------------------------------------------------------------
// PRAGMA / opener
//
// Plaintext profile only today; the SQLCipher (`Encrypted`) profile shares
// these and additionally pins `mmap_size = 0` (encrypted pages cannot be
// memory-mapped raw) plus the `key`/`cipher_compatibility` PRAGMAs that
// DSB-06 will add.
// ---------------------------------------------------------------------------

pub(super) const PRAGMA_JOURNAL_MODE: &str = "journal_mode";
pub(super) const PRAGMA_SYNCHRONOUS: &str = "synchronous";
pub(super) const PRAGMA_AUTO_VACUUM: &str = "auto_vacuum";
pub(super) const PRAGMA_CACHE_SIZE: &str = "cache_size";
pub(super) const PRAGMA_TEMP_STORE: &str = "temp_store";
pub(super) const PRAGMA_MMAP_SIZE: &str = "mmap_size";
pub(super) const PRAGMA_FOREIGN_KEYS: &str = "foreign_keys";
pub(super) const PRAGMA_BUSY_TIMEOUT: &str = "busy_timeout";

pub(super) const PRAGMA_VALUE_JOURNAL_MODE_WAL: &str = "WAL";
pub(super) const PRAGMA_VALUE_SYNCHRONOUS_NORMAL: &str = "NORMAL";
pub(super) const PRAGMA_VALUE_AUTO_VACUUM_INCREMENTAL: &str = "INCREMENTAL";
/// Negative cache size = KiB cap (≈ 40 MB). Stored as a SQL literal so
/// `queries.rs` only exposes `pub(super) const NAME: &str` items per
/// DSB-00b discipline.
pub(super) const PRAGMA_VALUE_CACHE_SIZE: &str = "-40000";
pub(super) const PRAGMA_VALUE_TEMP_STORE_MEMORY: &str = "MEMORY";
/// 128 MiB mmap window. Disabled (0) under SQLCipher per DSB-06.
pub(super) const PRAGMA_VALUE_MMAP_SIZE: &str = "134217728";
pub(super) const PRAGMA_VALUE_FOREIGN_KEYS_ON: &str = "ON";
pub(super) const PRAGMA_VALUE_BUSY_TIMEOUT_MS: &str = "5000";

// ---------------------------------------------------------------------------
// metadata / resource_version
// ---------------------------------------------------------------------------

pub(super) const METADATA_INCREMENT_RV: &str = "UPDATE metadata SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) WHERE key = 'resource_version'";
pub(super) const METADATA_SELECT_RV_INT: &str =
    "SELECT CAST(value AS INTEGER) FROM metadata WHERE key = 'resource_version'";
pub(super) const METADATA_SET_RV: &str =
    "UPDATE metadata SET value = ?1 WHERE key = 'resource_version'";

// ---------------------------------------------------------------------------
// _klights_meta (schema fingerprint, per-binary local state)
// ---------------------------------------------------------------------------

pub(super) const META_SELECT: &str = "SELECT value FROM _klights_meta WHERE key = ?1";
pub(super) const META_INSERT: &str =
    "INSERT OR REPLACE INTO _klights_meta (key, value) VALUES (?1, ?2)";

// ---------------------------------------------------------------------------
// watch_events
// ---------------------------------------------------------------------------

pub(super) const WATCH_EVENTS_INSERT: &str = "INSERT INTO watch_events (api_version, kind, namespace, name, resource_version, event_type, data) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";
// Removed: WATCH_EVENTS_INSERT_COMMAND, NAMESPACE_WATCH_INSERT_*. All
// watch_events insertions now route through
// crud::resources::insert_watch_event_in_conn so at-least-once
// replication replay is idempotent on UNIQUE(resource_version) for
// every code path.
/// Lookup an existing watch_events row by resource_version so the apply path
/// can recognize a benign at-least-once replay (same RV, same content) and
/// distinguish it from real divergence (same RV, different content).
pub(super) const WATCH_EVENTS_SELECT_BY_RV: &str = "SELECT api_version, kind, namespace, name, event_type, data FROM watch_events WHERE resource_version = ?1";

pub(super) const WATCH_EVENTS_LIST_CLUSTER_SINCE: &str = "SELECT api_version, kind, NULL as namespace, name, resource_version, event_type, data \
     FROM watch_events \
     WHERE api_version = ?1 AND kind = ?2 AND namespace IS NULL AND resource_version > ?3 \
     ORDER BY resource_version ASC";

pub(super) const WATCH_EVENTS_LIST_NAMESPACED_SINCE_HEAD: &str = "SELECT api_version, kind, namespace, name, resource_version, event_type, data \
     FROM watch_events \
     WHERE api_version = ?1 AND kind = ?2 AND resource_version > ?3";

pub(super) const WATCH_EVENTS_LIST_TARGETS_HEAD: &str = "SELECT api_version, kind, namespace, name, resource_version, event_type, data \
     FROM watch_events WHERE resource_version > ?1 AND (";

pub(super) const WATCH_EVENTS_LIST_ALL_SINCE: &str = "SELECT api_version, kind, namespace, name, resource_version, event_type, data \
     FROM watch_events \
     WHERE resource_version > ?1 \
     ORDER BY resource_version ASC";

pub(super) const WATCH_EVENTS_LIST_DELETED_SINCE: &str = "SELECT api_version, kind, namespace, name, resource_version, event_type, data \
     FROM watch_events \
     WHERE resource_version > ?1 AND event_type = 'DELETED' \
     ORDER BY resource_version ASC";

#[cfg(test)]
pub(super) const WATCH_EVENTS_COUNT: &str = "SELECT COUNT(*) FROM watch_events";

/// Lowest retained watch-event `resource_version`. The GC trims by id (oldest
/// first) and `resource_version` is monotonic with id, so the row with the
/// smallest id carries the smallest retained RV. Used to detect watches whose
/// resume point predates the window (→ `410 Gone`).
pub(super) const WATCH_EVENTS_MIN_RV: &str =
    "SELECT resource_version FROM watch_events ORDER BY id ASC LIMIT 1";

pub(super) const WATCH_EVENTS_GC: &str = "DELETE FROM watch_events
     WHERE id IN (
         SELECT id FROM watch_events
         WHERE id <= COALESCE((SELECT MAX(id) FROM watch_events), 0) - ?1
         ORDER BY id ASC
         LIMIT ?2
     )";

// ---------------------------------------------------------------------------
// applied_outbox
// ---------------------------------------------------------------------------

pub(super) const APPLIED_OUTBOX_GET: &str = "SELECT idempotency_key, subject_key, operation, \
     first_seen_ms, applied_rv, result_proto FROM applied_outbox WHERE idempotency_key = ?1";

pub(super) const APPLIED_OUTBOX_LIST_ALL: &str = "SELECT idempotency_key, subject_key, operation, \
     first_seen_ms, applied_rv, result_proto FROM applied_outbox ORDER BY idempotency_key";

pub(super) const APPLIED_OUTBOX_INSERT: &str = "INSERT OR IGNORE INTO applied_outbox \
     (idempotency_key, subject_key, operation, first_seen_ms, applied_rv, result_proto) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6)";

pub(super) const APPLIED_OUTBOX_UPSERT_EXACT: &str = "INSERT INTO applied_outbox \
     (idempotency_key, subject_key, operation, first_seen_ms, applied_rv, result_proto, status_stamp) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
     ON CONFLICT(idempotency_key) DO UPDATE SET \
       subject_key = excluded.subject_key, \
       operation = excluded.operation, \
       first_seen_ms = excluded.first_seen_ms, \
       applied_rv = excluded.applied_rv, \
       result_proto = excluded.result_proto, \
       status_stamp = excluded.status_stamp";

/// Highest worker-observed status stamp already recorded for a Pod status
/// subject. The leader compares an incoming status snapshot's stamp against
/// this to drop a stale snapshot that a retry let overtake a newer one.
pub(super) const APPLIED_OUTBOX_MAX_STATUS_STAMP_FOR_SUBJECT: &str =
    "SELECT MAX(status_stamp) FROM applied_outbox WHERE subject_key = ?1";

// ---------------------------------------------------------------------------
// pod_cleanup_intents
// ---------------------------------------------------------------------------

pub(super) const POD_CLEANUP_INTENT_UPSERT: &str = "INSERT INTO pod_cleanup_intents \
     (node_name, namespace, pod_name, pod_uid, reason, resource_version, created_at_ms, pod_data) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
     ON CONFLICT(node_name, namespace, pod_name, pod_uid, reason) DO UPDATE SET \
       resource_version = excluded.resource_version, \
       created_at_ms = excluded.created_at_ms, \
       pod_data = excluded.pod_data";

pub(super) const POD_CLEANUP_INTENT_LIST_BY_NODE: &str = "SELECT node_name, namespace, pod_name, pod_uid, reason, resource_version, created_at_ms, pod_data \
     FROM pod_cleanup_intents WHERE node_name = ?1 ORDER BY namespace, pod_name, pod_uid, reason";

pub(super) const POD_CLEANUP_INTENT_DELETE: &str = "DELETE FROM pod_cleanup_intents \
     WHERE node_name = ?1 AND namespace = ?2 AND pod_name = ?3 AND pod_uid = ?4 AND reason = ?5";

pub(super) const POD_CLEANUP_INTENTS_DELETE_BY_NODE: &str =
    "DELETE FROM pod_cleanup_intents WHERE node_name = ?1";

pub(super) const REPLACE_STATE_DELETE_WATCH_EVENTS: &str = "DELETE FROM watch_events";
pub(super) const REPLACE_STATE_DELETE_APPLIED_OUTBOX: &str = "DELETE FROM applied_outbox";
pub(super) const REPLACE_STATE_DELETE_POD_CLEANUP_INTENTS: &str = "DELETE FROM pod_cleanup_intents";
pub(super) const REPLACE_STATE_DELETE_NAMESPACED_RESOURCES: &str =
    "DELETE FROM namespaced_resources";
pub(super) const REPLACE_STATE_DELETE_CLUSTER_RESOURCES: &str = "DELETE FROM cluster_resources";
pub(super) const REPLACE_STATE_DELETE_NAMESPACES: &str = "DELETE FROM namespaces";
pub(super) const REPLACE_STATE_DELETE_NODE_DATAPLANE: &str = "DELETE FROM node_dataplane";
pub(super) const REPLACE_STATE_DELETE_NODE_SUBNETS: &str = "DELETE FROM node_subnets";

// ---------------------------------------------------------------------------
// namespaces
// ---------------------------------------------------------------------------

pub(super) const NAMESPACES_INSERT: &str =
    "INSERT INTO namespaces (name, uid, resource_version, data) VALUES (?1, ?2, ?3, ?4)";
pub(super) const NAMESPACES_UPSERT_EXACT: &str = "INSERT INTO namespaces \
     (name, uid, resource_version, data) VALUES (?1, ?2, ?3, ?4) \
     ON CONFLICT(name) DO UPDATE SET \
     uid = excluded.uid, resource_version = excluded.resource_version, data = excluded.data";

pub(super) const NAMESPACE_GET: &str =
    "SELECT name, resource_version, uid, data FROM namespaces WHERE name = ?1";

pub(super) const NAMESPACES_LIST_HEAD: &str =
    "SELECT name, resource_version, uid, data FROM namespaces";

pub(super) const NAMESPACE_UPDATE: &str = "UPDATE namespaces SET uid = ?1, resource_version = ?2, data = ?3 WHERE name = ?4 AND resource_version = ?5";

pub(super) const NAMESPACE_GET_DATA: &str = "SELECT data FROM namespaces WHERE name = ?1";

pub(super) const NAMESPACE_RESOURCES_DELETE_NON_PODS: &str = "DELETE FROM namespaced_resources
     WHERE namespace = ?1 AND kind != 'Pod'";

pub(super) const NAMESPACE_DELETE: &str = "DELETE FROM namespaces WHERE name = ?1";

pub(super) const NAMESPACE_EXISTS: &str = "SELECT 1 FROM namespaces WHERE name = ?1";

pub(super) const NAMESPACE_RESOURCES_LIST_ALL: &str =
    "SELECT id, api_version, kind, namespace, name, resource_version, uid, data
     FROM namespaced_resources
     WHERE namespace = ?1
     ORDER BY kind, name";

pub(super) const NAMESPACE_RESOURCES_LIST_OF_KIND: &str =
    "SELECT id, api_version, kind, namespace, name, resource_version, uid, data
     FROM namespaced_resources
     WHERE namespace = ?1 AND kind = ?2
     ORDER BY kind, name";

pub(super) const NAMESPACE_RESOURCES_LIST_EXCLUDING_KIND: &str =
    "SELECT id, api_version, kind, namespace, name, resource_version, uid, data
     FROM namespaced_resources
     WHERE namespace = ?1 AND kind <> ?2
     ORDER BY kind, name";

pub(super) const NAMESPACE_RESOURCES_COUNT: &str =
    "SELECT COUNT(*) FROM namespaced_resources WHERE namespace = ?1";

// ---------------------------------------------------------------------------
// namespaced_resources / cluster_resources core CRUD
// ---------------------------------------------------------------------------

pub(super) const NAMESPACED_INSERT: &str = "INSERT INTO namespaced_resources (api_version, kind, namespace, name, uid, resource_version, created_rv, data) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6, ?7)";
pub(super) const NAMESPACED_UPSERT_EXACT: &str = "INSERT INTO namespaced_resources \
     (api_version, kind, namespace, name, uid, resource_version, created_rv, data) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6, ?7) \
     ON CONFLICT(api_version, kind, namespace, name) DO UPDATE SET \
     uid = excluded.uid, resource_version = excluded.resource_version, data = excluded.data";

pub(super) const CLUSTER_INSERT: &str = "INSERT INTO cluster_resources (api_version, kind, name, uid, resource_version, created_rv, data) VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6)";
pub(super) const CLUSTER_UPSERT_EXACT: &str = "INSERT INTO cluster_resources \
     (api_version, kind, name, uid, resource_version, created_rv, data) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6) \
     ON CONFLICT(api_version, kind, name) DO UPDATE SET \
     uid = excluded.uid, resource_version = excluded.resource_version, data = excluded.data";

pub(super) const NAMESPACED_GET_EVENT_COMPAT: &str = "SELECT id, api_version, kind, namespace, name, resource_version, uid, data FROM namespaced_resources WHERE api_version IN ('v1', 'events.k8s.io/v1') AND kind = ?1 AND namespace = ?2 AND name = ?3 LIMIT 1";

pub(super) const NAMESPACED_GET: &str = "SELECT id, api_version, kind, namespace, name, resource_version, uid, data FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub(super) const CLUSTER_GET: &str = "SELECT id, api_version, kind, name, resource_version, uid, data FROM cluster_resources WHERE api_version = ?1 AND kind = ?2 AND name = ?3";

pub(super) const NAMESPACED_LIST_HEAD: &str = "SELECT id, api_version, kind, namespace, name, resource_version, uid, data FROM namespaced_resources ";

pub(super) const NAMESPACED_COUNT_HEAD: &str = "SELECT COUNT(*) FROM namespaced_resources ";

pub(super) const CLUSTER_LIST_HEAD: &str = "SELECT id, api_version, kind, name, resource_version, uid, data FROM cluster_resources WHERE api_version = ?1 AND kind = ?2";

pub(super) const CLUSTER_LIST_ALL: &str = "SELECT id, api_version, kind, name, resource_version, uid, data FROM cluster_resources ORDER BY api_version, kind, name";

pub(super) const CLUSTER_COUNT_HEAD: &str =
    "SELECT COUNT(*) FROM cluster_resources WHERE api_version = ?1 AND kind = ?2";

pub(super) const NAMESPACED_LIST_BY_AV_KIND_HEAD: &str = "WHERE api_version = ?1 AND kind = ?2";

pub(super) const NAMESPACED_LIST_BY_KIND_EVENT_COMPAT_HEAD: &str =
    "WHERE api_version IN ('v1', 'events.k8s.io/v1') AND kind = ?1";

pub(super) const NAMESPACED_KEYS_FOR_SCOPE: &str = "SELECT namespace, name
         FROM namespaced_resources
         WHERE api_version = ?1 AND kind = ?2";

pub(super) const CLUSTER_KEYS_FOR_SCOPE: &str = "SELECT name
         FROM cluster_resources
         WHERE api_version = ?1 AND kind = ?2";

pub(super) const NAMESPACED_UPDATE_BY_RV: &str = "UPDATE namespaced_resources SET resource_version = ?1, uid = ?2, data = ?3 WHERE api_version = ?4 AND kind = ?5 AND namespace = ?6 AND name = ?7 AND (?8 IS NULL OR resource_version = ?8) AND (?9 IS NULL OR uid = ?9)";

pub(super) const NAMESPACED_SELECT_ID: &str = "SELECT id FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub(super) const NAMESPACED_SELECT_STATUS_ROW: &str = "SELECT id, resource_version, uid, data FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub(super) const NAMESPACED_UPDATE_STATUS_BY_ID: &str = "UPDATE namespaced_resources SET resource_version = ?1, data = ?2 WHERE id = ?3 AND resource_version = ?4 AND uid = ?5";

pub(super) const CLUSTER_UPDATE_BY_RV: &str = "UPDATE cluster_resources SET resource_version = ?1, uid = ?2, data = ?3 WHERE api_version = ?4 AND kind = ?5 AND name = ?6 AND (?7 IS NULL OR resource_version = ?7) AND (?8 IS NULL OR uid = ?8)";

pub(super) const CLUSTER_SELECT_ID: &str =
    "SELECT id FROM cluster_resources WHERE api_version = ?1 AND kind = ?2 AND name = ?3";

pub(super) const CLUSTER_SELECT_STATUS_ROW: &str = "SELECT id, resource_version, uid, data FROM cluster_resources WHERE api_version = ?1 AND kind = ?2 AND name = ?3";

pub(super) const CLUSTER_UPDATE_STATUS_BY_ID: &str = "UPDATE cluster_resources SET resource_version = ?1, data = ?2 WHERE id = ?3 AND resource_version = ?4 AND uid = ?5";

pub(super) const NAMESPACED_GET_DATA_FOR_DELETE: &str =
    "SELECT resource_version, uid, data FROM namespaced_resources
     WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub(super) const NAMESPACED_DELETE: &str = "DELETE FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4 AND uid = ?5";
pub(super) const NAMESPACED_DELETE_BY_KEY: &str = "DELETE FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub(super) const CLUSTER_GET_DATA_FOR_DELETE: &str =
    "SELECT resource_version, uid, data FROM cluster_resources
     WHERE api_version = ?1 AND kind = ?2 AND name = ?3";

pub(super) const CLUSTER_DELETE: &str =
    "DELETE FROM cluster_resources WHERE api_version = ?1 AND kind = ?2 AND name = ?3 AND uid = ?4";
pub(super) const CLUSTER_DELETE_BY_KEY: &str =
    "DELETE FROM cluster_resources WHERE api_version = ?1 AND kind = ?2 AND name = ?3";

// ---------------------------------------------------------------------------
// merge_patch — namespaced + cluster paths
// ---------------------------------------------------------------------------

pub(super) const NAMESPACED_GET_FOR_PATCH: &str = "SELECT id, resource_version, uid, data FROM namespaced_resources                          WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub(super) const CLUSTER_GET_FOR_PATCH: &str = "SELECT id, resource_version, uid, data FROM cluster_resources                          WHERE api_version = ?1 AND kind = ?2 AND name = ?3";

pub(super) const NAMESPACED_UPDATE_PATCH: &str = "UPDATE namespaced_resources
         SET resource_version = ?1, uid = ?2, data = ?3
         WHERE api_version = ?4 AND kind = ?5 AND namespace = ?6 AND name = ?7 AND uid = ?8";

pub(super) const NAMESPACED_PATCH_WATCH_INSERT: &str = "INSERT INTO watch_events
         (api_version, kind, namespace, name, resource_version, event_type, data)
         VALUES (?1, ?2, ?3, ?4, ?5, 'MODIFIED', ?6)";

pub(super) const CLUSTER_UPDATE_PATCH: &str = "UPDATE cluster_resources
     SET resource_version = ?1, uid = ?2, data = ?3
     WHERE api_version = ?4 AND kind = ?5 AND name = ?6 AND uid = ?7";

pub(super) const CLUSTER_PATCH_WATCH_INSERT: &str = "INSERT INTO watch_events
     (api_version, kind, namespace, name, resource_version, event_type, data)
     VALUES (?1, ?2, NULL, ?3, ?4, 'MODIFIED', ?5)";

// ---------------------------------------------------------------------------
// node_subnets
// ---------------------------------------------------------------------------

pub(super) const NODE_SUBNET_SELECT_BY_NAME: &str = "SELECT node_name, subnet, subnet_base_int, vtep_ip, vtep_mac, \
                node_ip, mode, hostport_range \
         FROM node_subnets WHERE node_name = ?1";

pub(super) const NODE_SUBNET_INSERT_OR_IGNORE: &str = "INSERT OR IGNORE INTO node_subnets \
         (node_name, subnet, subnet_base_int, vtep_ip, vtep_mac, \
          node_ip, mode, hostport_range, created_at) \
         VALUES (?1, ?2, ?3, ?4, NULL, ?5, 'root', NULL, ?6)";
pub(super) const NODE_SUBNET_UPSERT_EXACT: &str = "INSERT INTO node_subnets \
         (node_name, subnet, subnet_base_int, vtep_ip, vtep_mac, \
          node_ip, mode, hostport_range, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0) \
         ON CONFLICT(node_name) DO UPDATE SET \
         subnet = excluded.subnet, \
         subnet_base_int = excluded.subnet_base_int, \
         vtep_ip = excluded.vtep_ip, \
         vtep_mac = excluded.vtep_mac, \
         node_ip = excluded.node_ip, \
         mode = excluded.mode, \
         hostport_range = excluded.hostport_range";

pub(super) const NODE_SUBNET_UPDATE_VTEP_MAC: &str =
    "UPDATE node_subnets SET vtep_mac = ?1 WHERE node_name = ?2";

pub(super) const NODE_SUBNET_LIST_PEERS: &str = "SELECT node_name, subnet, subnet_base_int, vtep_ip, vtep_mac, \
                node_ip, mode, hostport_range \
         FROM node_subnets WHERE node_name != ?1";

pub(super) const NODE_SUBNET_UPDATE_PEER_ATTRIBUTES: &str =
    "UPDATE node_subnets SET mode = ?1, hostport_range = ?2 WHERE node_name = ?3";

pub(super) const NODE_SUBNET_DELETE: &str = "DELETE FROM node_subnets WHERE node_name = ?1";

pub(super) const NODE_DATAPLANE_UPSERT: &str = concat!(
    "INSERT INTO node_dataplane ",
    "(node_name, mode, encryption, public_key, endpoint, port, updated_at) ",
    "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ",
    "ON CONFLICT(node_name) DO UPDATE SET ",
    "mode = excluded.mode, ",
    "encryption = excluded.encryption, ",
    "public_key = excluded.public_key, ",
    "endpoint = excluded.endpoint, ",
    "port = excluded.port, ",
    "updated_at = excluded.updated_at"
);

pub(super) const NODE_DATAPLANE_SELECT_BY_NAME: &str = "SELECT node_name, mode, encryption, public_key, endpoint, port \
       FROM node_dataplane WHERE node_name = ?1";

pub(super) const NODE_DATAPLANE_DELETE: &str = "DELETE FROM node_dataplane WHERE node_name = ?1";

// ---------------------------------------------------------------------------
// pod_slot_admissions
// ---------------------------------------------------------------------------

pub(super) const POD_SLOT_ADMISSION_SELECT: &str = "SELECT pod_uid, node_name, state, updated_rv FROM pod_slot_admissions \
     WHERE namespace = ?1 AND pod_name = ?2";

pub(super) const POD_SLOT_ADMISSION_INSERT: &str = "INSERT INTO pod_slot_admissions \
     (namespace, pod_name, pod_uid, node_name, state, updated_rv, updated_at_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

pub(super) const POD_SLOT_ADMISSION_UPDATE: &str = "UPDATE pod_slot_admissions \
     SET pod_uid = ?3, node_name = ?4, state = ?5, updated_rv = ?6, updated_at_ms = ?7 \
     WHERE namespace = ?1 AND pod_name = ?2";

pub(super) const POD_SLOT_ADMISSION_DELETE_IF_UID: &str = "DELETE FROM pod_slot_admissions \
     WHERE namespace = ?1 AND pod_name = ?2 AND pod_uid = ?3";

pub(super) const NODE_META_POD_SLOT_RV_SELECT: &str =
    "SELECT value FROM _node_meta WHERE key = 'pod_slot_resource_version'";

pub(super) const NODE_META_POD_SLOT_RV_UPSERT: &str = "INSERT INTO _node_meta (key, value) VALUES ('pod_slot_resource_version', ?1) \
     ON CONFLICT(key) DO UPDATE SET value = excluded.value";

// ---------------------------------------------------------------------------
// pod_sandboxes
// ---------------------------------------------------------------------------

pub(super) const POD_SANDBOX_INSERT_OR_REPLACE: &str = "INSERT INTO pod_runtime \
     (namespace, pod_name, pod_uid, node_name, sandbox_id, created_ms) \
     VALUES (?1, ?2, ?3, '', ?4, ?5) \
     ON CONFLICT(pod_uid) DO UPDATE SET \
       namespace = excluded.namespace, \
       pod_name = excluded.pod_name, \
       sandbox_id = excluded.sandbox_id";

pub(super) const POD_SANDBOX_GET: &str = "SELECT sandbox_id FROM pod_runtime \
     WHERE namespace = ?1 AND pod_name = ?2 AND sandbox_id IS NOT NULL \
     ORDER BY created_ms DESC LIMIT 1";

pub(super) const POD_SANDBOX_GET_FOR_UID: &str = "SELECT sandbox_id FROM pod_runtime \
     WHERE namespace = ?1 AND pod_name = ?2 AND pod_uid = ?3 AND sandbox_id IS NOT NULL";

pub(super) const POD_SANDBOX_LIST: &str =
    "SELECT namespace, pod_name, pod_uid, sandbox_id FROM pod_runtime WHERE sandbox_id IS NOT NULL";

pub(super) const POD_SANDBOX_DELETE: &str =
    "DELETE FROM pod_runtime WHERE namespace = ?1 AND pod_name = ?2";

pub(super) const POD_SANDBOX_DELETE_FOR_UID: &str = "DELETE FROM pod_runtime \
     WHERE namespace = ?1 AND pod_name = ?2 AND pod_uid = ?3 AND sandbox_id = ?4";

// ---------------------------------------------------------------------------
// pod_networks
// ---------------------------------------------------------------------------

pub(super) const POD_NETWORK_INSERT: &str = "INSERT INTO pod_networks \
     (sandbox_id, namespace, pod_name, pod_uid, ip_addr, ip_int, veth_host, netns_path, created_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)";

pub(super) const POD_NETWORK_INSERT_ON_CONFLICT_NOTHING: &str = "INSERT INTO pod_networks \
     (sandbox_id, namespace, pod_name, pod_uid, ip_addr, ip_int, veth_host, netns_path, created_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
     ON CONFLICT(ip_int) DO NOTHING";

pub(super) const POD_NETWORK_GET_BY_SANDBOX: &str =
    "SELECT ip_addr, ip_int FROM pod_networks WHERE sandbox_id = ?1";

pub(super) const POD_NETWORK_MAX_IP_IN_RANGE: &str = "SELECT MAX(ip_int) FROM pod_networks \
     WHERE ip_int >= ?1 AND ip_int <= ?2";

pub(super) const POD_NETWORK_GET_ENDPOINT: &str =
    "SELECT ip_addr, veth_host, netns_path FROM pod_networks WHERE sandbox_id = ?1";

pub(super) const POD_NETWORK_GET_ENDPOINT_FOR_POD: &str = "SELECT ip_addr, veth_host, netns_path FROM pod_networks \
     WHERE namespace = ?1 AND pod_name = ?2 AND pod_uid = ?3 \
     ORDER BY created_ms DESC LIMIT 1";

pub(super) const POD_NETWORK_DELETE: &str = "DELETE FROM pod_networks WHERE sandbox_id = ?1";

pub(super) const POD_NETWORK_LIST_SANDBOX_IDS: &str = "SELECT sandbox_id FROM pod_networks";

pub(super) const POD_NETWORK_COUNT_BY_IP: &str =
    "SELECT COUNT(*) FROM pod_networks WHERE ip_int = ?1";

// ---------------------------------------------------------------------------
// pod_endpoints
// ---------------------------------------------------------------------------

pub(super) const POD_ENDPOINT_UPSERT: &str = "INSERT OR REPLACE INTO pod_endpoints \
     (pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, host_port_tcp, host_port_udp, generation, updated_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";

pub(super) const POD_ENDPOINT_GET_IP_FOR_DELETE: &str =
    "SELECT pod_ip FROM pod_endpoints WHERE pod_uid = ?1";

pub(super) const POD_ENDPOINT_DELETE: &str = "DELETE FROM pod_endpoints WHERE pod_uid = ?1";

pub(super) const POD_ENDPOINT_LIST_BY_NODE: &str = "SELECT pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
            host_port_tcp, host_port_udp, generation, updated_ms \
     FROM pod_endpoints WHERE node_name = ?1 ORDER BY pod_uid";

pub(super) const POD_ENDPOINT_LIST_ALL: &str = "SELECT pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
            host_port_tcp, host_port_udp, generation, updated_ms \
     FROM pod_endpoints ORDER BY pod_uid";

pub(super) const POD_ENDPOINT_GET_BY_POD_IP: &str = "SELECT pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip, \
            host_port_tcp, host_port_udp, generation, updated_ms \
     FROM pod_endpoints WHERE pod_ip = ?1 LIMIT 1";

// ---------------------------------------------------------------------------
// pod_workqueue
// ---------------------------------------------------------------------------

pub(super) const POD_WORKQUEUE_TAIL_OTHER: &str = "SELECT COALESCE(MAX(next_due_ms), 0) \
     FROM pod_workqueue \
     WHERE NOT (kind = ?1 AND namespace = ?2 AND pod_name = ?3 AND pod_uid = ?4)";

pub(super) const POD_WORKQUEUE_UPSERT: &str = "INSERT INTO pod_workqueue \
     (kind, namespace, pod_name, pod_uid, payload, attempt_count, next_due_ms, last_error, enqueued_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
     ON CONFLICT(kind, namespace, pod_name, pod_uid) DO UPDATE SET \
       payload = excluded.payload, \
       attempt_count = excluded.attempt_count, \
       next_due_ms = excluded.next_due_ms, \
       last_error = excluded.last_error, \
       enqueued_ms = excluded.enqueued_ms";

pub(super) const POD_WORKQUEUE_PEEK_NEXT_DUE: &str = "SELECT MIN(next_due_ms) FROM pod_workqueue";

pub(super) const POD_WORKQUEUE_CLAIM_DUE: &str = "SELECT id, kind, namespace, pod_name, pod_uid, payload, attempt_count, next_due_ms \
     FROM pod_workqueue \
     WHERE next_due_ms <= ?1 \
     ORDER BY next_due_ms ASC, id ASC \
     LIMIT 1";

pub(super) const POD_WORKQUEUE_DELETE_BY_ID: &str = "DELETE FROM pod_workqueue WHERE id = ?1";

// ---------------------------------------------------------------------------
// ownership / owner_uid lookups via resource_owner_refs index
// ---------------------------------------------------------------------------

pub(super) const OWNERSHIP_INDEXED_NAMESPACED_BY_UID: &str = "SELECT r.id, r.api_version, r.kind, r.namespace, r.name, r.resource_version, r.uid, r.data \
     FROM namespaced_resources r \
     INNER JOIN resource_owner_refs o ON o.api_version = r.api_version AND o.kind = r.kind AND o.namespace = r.namespace AND o.name = r.name \
     WHERE o.owner_uid = ?1";

pub(super) const OWNERSHIP_INDEXED_CLUSTER_BY_UID: &str = "SELECT r.id, r.api_version, r.kind, r.name, r.resource_version, r.uid, r.data \
     FROM cluster_resources r \
     INNER JOIN resource_owner_refs o ON o.api_version = r.api_version AND o.kind = r.kind AND o.namespace = '' AND o.name = r.name \
     WHERE o.owner_uid = ?1";

pub(super) const OWNERSHIP_INDEXED_NAMESPACED_BY_KIND_AV_UID: &str = "SELECT r.id, r.api_version, r.kind, r.namespace, r.name, r.resource_version, r.uid, r.data \
     FROM namespaced_resources r \
     INNER JOIN resource_owner_refs o ON o.api_version = r.api_version AND o.kind = r.kind AND o.namespace = r.namespace AND o.name = r.name \
     WHERE r.kind = ?1 AND r.namespace = ?2 AND r.api_version = ?3 AND o.owner_uid = ?4";

pub(super) const OWNERSHIP_INDEXED_CLUSTER_BY_KIND_AV_UID: &str = "SELECT r.id, r.api_version, r.kind, r.name, r.resource_version, r.uid, r.data \
     FROM cluster_resources r \
     INNER JOIN resource_owner_refs o ON o.api_version = r.api_version AND o.kind = r.kind AND o.namespace = '' AND o.name = r.name \
     WHERE r.kind = ?1 AND r.api_version = ?2 AND o.owner_uid = ?3";

pub(super) const OWNERSHIP_INDEXED_NAMESPACED_EMPTY_UID_BY_IDENTITY: &str = "SELECT r.id, r.api_version, r.kind, r.namespace, r.name, r.resource_version, r.uid, r.data \
     FROM resource_owner_refs o \
     INNER JOIN namespaced_resources r ON r.api_version = o.api_version AND r.kind = o.kind AND r.namespace = o.namespace AND r.name = o.name \
     WHERE o.owner_kind = ?1 AND o.owner_name = ?2 AND o.owner_uid = ''";

pub(super) const SELECT_KLIGHTS_META: &str = "SELECT value FROM _klights_meta WHERE key = ?1";

pub(super) const UPSERT_KLIGHTS_META: &str =
    "INSERT OR REPLACE INTO _klights_meta (key, value) VALUES (?1, ?2)";

pub(super) const APPLIED_OUTBOX_UPDATE_RESULT: &str = "UPDATE applied_outbox \
     SET subject_key = ?2, applied_rv = ?3, result_proto = ?4, status_stamp = ?5 \
     WHERE idempotency_key = ?1";
pub(super) const APPLIED_OUTBOX_DELETE_STALE_PLACEHOLDERS: &str = "DELETE FROM applied_outbox \
     WHERE applied_rv IS NULL
     AND subject_key = ''
     AND result_proto = X''
     AND first_seen_ms < ?1";
pub(super) const APPLIED_OUTBOX_DELETE_EXPIRED: &str =
    "DELETE FROM applied_outbox WHERE first_seen_ms < ?1";

pub(super) const APPLIED_OUTBOX_DELETE_BY_KEY: &str =
    "DELETE FROM applied_outbox WHERE idempotency_key = ?1";

pub(super) const APPLIED_OUTBOX_DELETE_UNCOMMITTED_PLACEHOLDER_BY_KEY: &str = "DELETE FROM applied_outbox \
     WHERE idempotency_key = ?1 \
       AND subject_key = '' \
       AND applied_rv IS NULL \
       AND length(result_proto) = 0";

// ---------------------------------------------------------------------------
// Selector index tables (resource_labels, resource_fields)
// ---------------------------------------------------------------------------

pub(super) const LABEL_INDEX_DELETE_FOR_RESOURCE: &str = "DELETE FROM resource_labels WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub(super) const FIELD_INDEX_DELETE_FOR_RESOURCE: &str = "DELETE FROM resource_fields WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub(super) const LABEL_INDEX_INSERT: &str = "INSERT INTO resource_labels (api_version, kind, namespace, name, key, value) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";

pub(super) const FIELD_INDEX_INSERT: &str = "INSERT INTO resource_fields (api_version, kind, namespace, name, field, value) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";

pub(super) const REPLACE_STATE_DELETE_RESOURCE_LABELS: &str = "DELETE FROM resource_labels";
pub(super) const REPLACE_STATE_DELETE_RESOURCE_FIELDS: &str = "DELETE FROM resource_fields";
pub(super) const REPLACE_STATE_DELETE_RESOURCE_OWNER_REFS: &str = "DELETE FROM resource_owner_refs";

// ---------------------------------------------------------------------------
// Owner reference index table (resource_owner_refs)
// ---------------------------------------------------------------------------

// Allow dead code until the index is integrated into GC and ownership lookups
pub(super) const OWNER_REF_INDEX_DELETE: &str = "DELETE FROM resource_owner_refs WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub(super) const OWNER_REF_INDEX_INSERT: &str = "INSERT INTO resource_owner_refs (api_version, kind, namespace, name, owner_uid, owner_api_version, owner_kind, owner_name, controller, block_owner_deletion, ordinal) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_mutation_sql_keeps_uid_predicates() {
        for (name, sql) in [
            ("NAMESPACED_INSERT", NAMESPACED_INSERT),
            ("CLUSTER_INSERT", CLUSTER_INSERT),
        ] {
            let normalized = sql.to_ascii_lowercase();
            assert!(
                normalized.contains("insert") && normalized.contains("uid"),
                "{name} must persist resource uid: {sql}"
            );
        }

        let uid_qualified_updates = [
            ("NAMESPACED_UPDATE_BY_RV", NAMESPACED_UPDATE_BY_RV),
            ("CLUSTER_UPDATE_BY_RV", CLUSTER_UPDATE_BY_RV),
            (
                "NAMESPACED_UPDATE_STATUS_BY_ID",
                NAMESPACED_UPDATE_STATUS_BY_ID,
            ),
            ("CLUSTER_UPDATE_STATUS_BY_ID", CLUSTER_UPDATE_STATUS_BY_ID),
            ("NAMESPACED_UPDATE_PATCH", NAMESPACED_UPDATE_PATCH),
            ("CLUSTER_UPDATE_PATCH", CLUSTER_UPDATE_PATCH),
            ("NAMESPACED_DELETE", NAMESPACED_DELETE),
            ("CLUSTER_DELETE", CLUSTER_DELETE),
        ];

        for (name, sql) in uid_qualified_updates {
            let normalized = sql.to_ascii_lowercase();
            assert!(
                normalized.contains("where") && normalized.contains("uid"),
                "{name} must qualify resource writes by uid: {sql}"
            );
        }
    }

    #[test]
    fn namespace_teardown_sql_never_bulk_deletes_pods() {
        // R4: invariant now enforced by check_supervisor_spawn.sh
    }
}
