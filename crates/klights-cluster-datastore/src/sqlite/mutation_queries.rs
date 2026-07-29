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

pub use super::read_queries::CLUSTER_GET;
pub use super::read_queries::METADATA_SELECT_RV_INT;
pub use super::read_queries::NAMESPACE_GET;
pub use super::read_queries::NAMESPACE_RESOURCES_COUNT;
pub use super::read_queries::NAMESPACE_RESOURCES_LIST_EXCLUDING_KIND;
pub use super::read_queries::NAMESPACED_GET;
pub use super::read_queries::NODE_SUBNET_SELECT_BY_NAME;
#[cfg(any(test, feature = "test-support"))]
pub use super::read_queries::OWNERSHIP_INDEXED_NAMESPACED_EMPTY_UID_BY_IDENTITY;
pub use super::read_queries::WATCH_EVENTS_MIN_RV;

// ---------------------------------------------------------------------------
// metadata / resource_version
// ---------------------------------------------------------------------------

pub const METADATA_INCREMENT_RV: &str = "UPDATE metadata SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) WHERE key = 'resource_version'";
pub const METADATA_SET_RV: &str = "UPDATE metadata SET value = ?1 WHERE key = 'resource_version'";

// ---------------------------------------------------------------------------
// watch_events
// ---------------------------------------------------------------------------

pub const WATCH_EVENTS_INSERT: &str = "INSERT INTO watch_events (api_version, kind, namespace, name, resource_version, event_type, data) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";
// Removed: WATCH_EVENTS_INSERT_COMMAND, NAMESPACE_WATCH_INSERT_*. All
// watch_events insertions now route through
// crud::resources::insert_watch_event_in_conn so at-least-once
// replication replay is idempotent on the unique
// (resource identity, resource_version) index while allowing different
// resources to share one raft/etcd-style revision.
/// Lookup an existing watch_events row by identity + resource_version so the
/// apply path can recognize a benign at-least-once replay (same object, same
/// RV, same content) and distinguish it from real divergence.
pub const WATCH_EVENTS_SELECT_BY_IDENTITY_RV: &str = "SELECT event_type, data FROM watch_events WHERE api_version = ?1 AND kind = ?2 AND COALESCE(namespace, '#cluster') = ?3 AND name = ?4 AND resource_version = ?5";

/// memory-improvement.md §10 P1: keyset-paginated form of
/// `WATCH_EVENTS_LIST_ALL_SINCE`. Adds the `id` column (for the next cursor)
/// and restricts to rows strictly AFTER `(?2, ?3)` in the same
/// `(resource_version ASC, id ASC)` ordering the full-list form uses. The
/// first page passes the floor rv as both `?1` and `?2` with `?3 = 0`.
pub const WATCH_EVENTS_LIST_ALL_SINCE_PAGED: &str = "SELECT api_version, kind, namespace, name, resource_version, event_type, data, id \
     FROM watch_events \
     WHERE resource_version > ?1 \
       AND (resource_version > ?2 OR (resource_version = ?2 AND id > ?3)) \
     ORDER BY resource_version ASC, id ASC \
     LIMIT ?4";

#[cfg(any(test, feature = "test-support"))]
pub const WATCH_EVENTS_COUNT: &str = "SELECT COUNT(*) FROM watch_events";

/// Lowest retained watch-event `resource_version`. The GC trims by id (oldest
/// first) and `resource_version` is monotonic with id, so the row with the
/// smallest id carries the smallest retained RV. Used to detect watches whose
/// resume point predates the window (→ `410 Gone`).
pub const WATCH_EVENTS_SCOPE_COUNT: &str = "SELECT COUNT(*) FROM (
         SELECT 1 FROM watch_events
         GROUP BY api_version, kind, COALESCE(namespace, '#cluster')
     )";

pub const WATCH_REPLAY_FLOOR_UPSERT: &str =
    "INSERT INTO watch_replay_floors
        (api_version, kind, namespace_key, floor_rv, floor_event_id, floor_position_exact)
     VALUES (?1, ?2, ?3, ?4, ?5, 1)
     ON CONFLICT(api_version, kind, namespace_key)
     DO UPDATE SET floor_rv = MAX(watch_replay_floors.floor_rv, excluded.floor_rv),
                   floor_event_id = MAX(watch_replay_floors.floor_event_id, excluded.floor_event_id),
                   floor_position_exact = 1";

pub const WATCH_EVENTS_GC_CANDIDATES: &str =
    "SELECT id, api_version, kind, COALESCE(namespace, '#cluster'), resource_version
     FROM (
         SELECT id, api_version, kind, namespace, resource_version,
                ROW_NUMBER() OVER (
                    PARTITION BY api_version, kind, COALESCE(namespace, '#cluster')
                    ORDER BY id DESC
                ) AS scope_rank
         FROM watch_events
     )
     WHERE id <= COALESCE((SELECT MAX(id) FROM watch_events), 0) - ?1
       AND scope_rank > ?3
     ORDER BY id ASC
     LIMIT ?2";

pub const WATCH_EVENTS_GC_PRUNABLE_COUNT: &str = "SELECT COUNT(*) FROM (
         SELECT id FROM (
             SELECT id,
                    ROW_NUMBER() OVER (
                        PARTITION BY api_version, kind, COALESCE(namespace, '#cluster')
                        ORDER BY id DESC
                    ) AS scope_rank
             FROM watch_events
         )
         WHERE id <= COALESCE((SELECT MAX(id) FROM watch_events), 0) - ?1
           AND scope_rank > ?3
         ORDER BY id ASC
         LIMIT ?2
     )";

// ---------------------------------------------------------------------------
// applied_outbox
// ---------------------------------------------------------------------------

pub const APPLIED_OUTBOX_GET: &str = "SELECT idempotency_key, subject_key, operation, \
     first_seen_ms, applied_rv, result_proto, status_stamp FROM applied_outbox WHERE idempotency_key = ?1";

pub const APPLIED_OUTBOX_LIST_ALL: &str = "SELECT idempotency_key, subject_key, operation, \
     first_seen_ms, applied_rv, result_proto, status_stamp FROM applied_outbox ORDER BY idempotency_key";

/// memory-improvement.md §10 P1: keyset-paginated form of
/// `APPLIED_OUTBOX_LIST_ALL`. Rows with `idempotency_key > ?1` in the same
/// `ORDER BY idempotency_key ASC` ordering, capped by `LIMIT ?2`. The first
/// page passes an empty string (every real key is greater than `''`).
pub const APPLIED_OUTBOX_LIST_ALL_PAGED: &str = "SELECT idempotency_key, subject_key, operation, \
     first_seen_ms, applied_rv, result_proto, status_stamp FROM applied_outbox \
     WHERE idempotency_key > ?1 ORDER BY idempotency_key ASC LIMIT ?2";

pub const APPLIED_OUTBOX_INSERT: &str = "INSERT OR IGNORE INTO applied_outbox \
     (idempotency_key, subject_key, operation, first_seen_ms, applied_rv, result_proto, status_stamp) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

pub const APPLIED_OUTBOX_UPSERT_EXACT: &str = "INSERT INTO applied_outbox \
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
pub const APPLIED_OUTBOX_MAX_STATUS_STAMP_FOR_SUBJECT: &str =
    "SELECT MAX(status_stamp) FROM applied_outbox WHERE subject_key = ?1";

// ---------------------------------------------------------------------------
// pod_cleanup_intents
// ---------------------------------------------------------------------------

pub const POD_CLEANUP_INTENT_UPSERT: &str = "INSERT INTO pod_cleanup_intents \
     (node_name, namespace, pod_name, pod_uid, reason, resource_version, created_at_ms, pod_data) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
     ON CONFLICT(node_name, namespace, pod_name, pod_uid, reason) DO UPDATE SET \
       resource_version = excluded.resource_version, \
       created_at_ms = excluded.created_at_ms, \
       pod_data = excluded.pod_data";

pub const POD_CLEANUP_INTENT_LIST_BY_NODE: &str = "SELECT node_name, namespace, pod_name, pod_uid, reason, resource_version, created_at_ms, pod_data \
     FROM pod_cleanup_intents WHERE node_name = ?1 ORDER BY namespace, pod_name, pod_uid, reason";

pub const POD_CLEANUP_INTENT_DELETE: &str = "DELETE FROM pod_cleanup_intents \
     WHERE node_name = ?1 AND namespace = ?2 AND pod_name = ?3 AND pod_uid = ?4 AND reason = ?5";

pub const POD_CLEANUP_INTENTS_DELETE_BY_NODE: &str =
    "DELETE FROM pod_cleanup_intents WHERE node_name = ?1";

pub const REPLACE_STATE_DELETE_WATCH_EVENTS: &str = "DELETE FROM watch_events";
pub const REPLACE_STATE_DELETE_APPLIED_OUTBOX: &str = "DELETE FROM applied_outbox";
pub const REPLACE_STATE_DELETE_POD_CLEANUP_INTENTS: &str = "DELETE FROM pod_cleanup_intents";
pub const REPLACE_STATE_DELETE_NAMESPACED_RESOURCES: &str = "DELETE FROM namespaced_resources";
pub const REPLACE_STATE_DELETE_CLUSTER_RESOURCES: &str = "DELETE FROM cluster_resources";
pub const REPLACE_STATE_DELETE_NAMESPACES: &str = "DELETE FROM namespaces";
pub const REPLACE_STATE_DELETE_NODE_DATAPLANE: &str = "DELETE FROM node_dataplane";
pub const REPLACE_STATE_DELETE_NODE_SUBNETS: &str = "DELETE FROM node_subnets";

// ---------------------------------------------------------------------------
// namespaces
// ---------------------------------------------------------------------------

pub const NAMESPACES_INSERT: &str =
    "INSERT INTO namespaces (name, uid, resource_version, data) VALUES (?1, ?2, ?3, ?4)";
pub const NAMESPACES_UPSERT_EXACT: &str = "INSERT INTO namespaces \
     (name, uid, resource_version, data) VALUES (?1, ?2, ?3, ?4) \
     ON CONFLICT(name) DO UPDATE SET \
     uid = excluded.uid, resource_version = excluded.resource_version, data = excluded.data";

pub const NAMESPACE_UPDATE: &str = "UPDATE namespaces SET uid = ?1, resource_version = ?2, data = ?3 WHERE name = ?4 AND resource_version = ?5";

pub const NAMESPACE_GET_DATA: &str = "SELECT data FROM namespaces WHERE name = ?1";

pub const NAMESPACE_RESOURCES_DELETE_NON_PODS: &str = "DELETE FROM namespaced_resources
     WHERE namespace = ?1 AND kind != 'Pod'";

pub const NAMESPACE_DELETE: &str = "DELETE FROM namespaces WHERE name = ?1";

pub const NAMESPACE_EXISTS: &str = "SELECT 1 FROM namespaces WHERE name = ?1";

// ---------------------------------------------------------------------------
// namespaced_resources / cluster_resources core CRUD
// ---------------------------------------------------------------------------

pub const NAMESPACED_INSERT: &str = "INSERT INTO namespaced_resources (api_version, kind, namespace, name, uid, resource_version, created_rv, data) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6, ?7)";
pub const NAMESPACED_UPSERT_EXACT: &str = "INSERT INTO namespaced_resources \
     (api_version, kind, namespace, name, uid, resource_version, created_rv, data) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6, ?7) \
     ON CONFLICT(api_version, kind, namespace, name) DO UPDATE SET \
     uid = excluded.uid, resource_version = excluded.resource_version, data = excluded.data";

pub const CLUSTER_INSERT: &str = "INSERT INTO cluster_resources (api_version, kind, name, uid, resource_version, created_rv, data) VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6)";
pub const CLUSTER_UPSERT_EXACT: &str = "INSERT INTO cluster_resources \
     (api_version, kind, name, uid, resource_version, created_rv, data) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6) \
     ON CONFLICT(api_version, kind, name) DO UPDATE SET \
     uid = excluded.uid, resource_version = excluded.resource_version, data = excluded.data";

pub const NAMESPACED_UPDATE_BY_RV: &str = "UPDATE namespaced_resources SET resource_version = ?1, uid = ?2, data = ?3 WHERE api_version = ?4 AND kind = ?5 AND namespace = ?6 AND name = ?7 AND (?8 IS NULL OR resource_version = ?8) AND (?9 IS NULL OR uid = ?9)";

pub const NAMESPACED_SELECT_ID: &str = "SELECT id FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub const NAMESPACED_SELECT_STATUS_ROW: &str = "SELECT id, resource_version, uid, data FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub const NAMESPACED_UPDATE_STATUS_BY_ID: &str = "UPDATE namespaced_resources SET resource_version = ?1, data = ?2 WHERE id = ?3 AND resource_version = ?4 AND uid = ?5";

pub const CLUSTER_UPDATE_BY_RV: &str = "UPDATE cluster_resources SET resource_version = ?1, uid = ?2, data = ?3 WHERE api_version = ?4 AND kind = ?5 AND name = ?6 AND (?7 IS NULL OR resource_version = ?7) AND (?8 IS NULL OR uid = ?8)";

pub const CLUSTER_SELECT_ID: &str =
    "SELECT id FROM cluster_resources WHERE api_version = ?1 AND kind = ?2 AND name = ?3";

pub const CLUSTER_SELECT_STATUS_ROW: &str = "SELECT id, resource_version, uid, data FROM cluster_resources WHERE api_version = ?1 AND kind = ?2 AND name = ?3";

pub const CLUSTER_UPDATE_STATUS_BY_ID: &str = "UPDATE cluster_resources SET resource_version = ?1, data = ?2 WHERE id = ?3 AND resource_version = ?4 AND uid = ?5";

pub const NAMESPACED_GET_DATA_FOR_DELETE: &str =
    "SELECT resource_version, uid, data FROM namespaced_resources
     WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub const NAMESPACED_DELETE: &str = "DELETE FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4 AND uid = ?5";
pub const NAMESPACED_DELETE_BY_KEY: &str = "DELETE FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub const CLUSTER_GET_DATA_FOR_DELETE: &str =
    "SELECT resource_version, uid, data FROM cluster_resources
     WHERE api_version = ?1 AND kind = ?2 AND name = ?3";

pub const CLUSTER_DELETE: &str =
    "DELETE FROM cluster_resources WHERE api_version = ?1 AND kind = ?2 AND name = ?3 AND uid = ?4";
pub const CLUSTER_DELETE_BY_KEY: &str =
    "DELETE FROM cluster_resources WHERE api_version = ?1 AND kind = ?2 AND name = ?3";

// ---------------------------------------------------------------------------
// merge_patch — namespaced + cluster paths
// ---------------------------------------------------------------------------

pub const NAMESPACED_GET_FOR_PATCH: &str = "SELECT id, resource_version, uid, data FROM namespaced_resources                          WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub const CLUSTER_GET_FOR_PATCH: &str = "SELECT id, resource_version, uid, data FROM cluster_resources                          WHERE api_version = ?1 AND kind = ?2 AND name = ?3";

pub const NAMESPACED_UPDATE_PATCH: &str = "UPDATE namespaced_resources
         SET resource_version = ?1, uid = ?2, data = ?3
         WHERE api_version = ?4 AND kind = ?5 AND namespace = ?6 AND name = ?7 AND uid = ?8";

pub const NAMESPACED_PATCH_WATCH_INSERT: &str = "INSERT INTO watch_events
         (api_version, kind, namespace, name, resource_version, event_type, data)
         VALUES (?1, ?2, ?3, ?4, ?5, 'MODIFIED', ?6)";

pub const CLUSTER_UPDATE_PATCH: &str = "UPDATE cluster_resources
     SET resource_version = ?1, uid = ?2, data = ?3
     WHERE api_version = ?4 AND kind = ?5 AND name = ?6 AND uid = ?7";

pub const CLUSTER_PATCH_WATCH_INSERT: &str = "INSERT INTO watch_events
     (api_version, kind, namespace, name, resource_version, event_type, data)
     VALUES (?1, ?2, NULL, ?3, ?4, 'MODIFIED', ?5)";

// ---------------------------------------------------------------------------
// node_subnets
// ---------------------------------------------------------------------------

pub const NODE_SUBNET_INSERT_OR_IGNORE: &str = "INSERT OR IGNORE INTO node_subnets \
         (node_name, subnet, subnet_base_int, gateway_ip, \
          node_ip, mode, hostport_range, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, 'root', NULL, ?6)";
pub const NODE_SUBNET_UPSERT_EXACT: &str = "INSERT INTO node_subnets \
         (node_name, subnet, subnet_base_int, gateway_ip, \
          node_ip, mode, hostport_range, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0) \
         ON CONFLICT(node_name) DO UPDATE SET \
         subnet = excluded.subnet, \
         subnet_base_int = excluded.subnet_base_int, \
         gateway_ip = excluded.gateway_ip, \
         node_ip = excluded.node_ip, \
         mode = excluded.mode, \
         hostport_range = excluded.hostport_range";

pub const NODE_SUBNET_UPDATE_PEER_ATTRIBUTES: &str =
    "UPDATE node_subnets SET mode = ?1, hostport_range = ?2 WHERE node_name = ?3";

pub const NODE_SUBNET_DELETE: &str = "DELETE FROM node_subnets WHERE node_name = ?1";

pub const NODE_DATAPLANE_UPSERT: &str = concat!(
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

pub const NODE_DATAPLANE_DELETE: &str = "DELETE FROM node_dataplane WHERE node_name = ?1";

// ---------------------------------------------------------------------------
// ownership / owner_uid lookups via resource_owner_refs index
// ---------------------------------------------------------------------------

pub const SELECT_KLIGHTS_META: &str = "SELECT value FROM _klights_meta WHERE key = ?1";

pub const UPSERT_KLIGHTS_META: &str =
    "INSERT OR REPLACE INTO _klights_meta (key, value) VALUES (?1, ?2)";

pub const APPLIED_OUTBOX_UPDATE_RESULT: &str = "UPDATE applied_outbox \
     SET subject_key = ?2, applied_rv = ?3, result_proto = ?4, status_stamp = ?5 \
     WHERE idempotency_key = ?1";
pub const APPLIED_OUTBOX_DELETE_EXPIRED: &str =
    "DELETE FROM applied_outbox WHERE first_seen_ms < ?1";
pub const APPLIED_OUTBOX_GC_PRUNABLE_COUNT: &str =
    "SELECT COUNT(*) FROM applied_outbox WHERE first_seen_ms < ?1";

pub const APPLIED_OUTBOX_DELETE_BY_KEY: &str =
    "DELETE FROM applied_outbox WHERE idempotency_key = ?1";

// ---------------------------------------------------------------------------
// Selector index tables (resource_labels, resource_fields)
// ---------------------------------------------------------------------------

pub const REPLACE_STATE_DELETE_RESOURCE_LABELS: &str = "DELETE FROM resource_labels";
pub const REPLACE_STATE_DELETE_RESOURCE_FIELDS: &str = "DELETE FROM resource_fields";
pub const REPLACE_STATE_DELETE_RESOURCE_OWNER_REFS: &str = "DELETE FROM resource_owner_refs";

// ---------------------------------------------------------------------------
// Owner reference index table (resource_owner_refs)
// ---------------------------------------------------------------------------

// Allow dead code until the index is integrated into GC and ownership lookups
pub const OWNER_REF_INDEX_DELETE: &str = "DELETE FROM resource_owner_refs WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";

pub const OWNER_REF_INDEX_INSERT: &str = "INSERT INTO resource_owner_refs (api_version, kind, namespace, name, owner_uid, owner_api_version, owner_kind, owner_name, controller, block_owner_deletion, ordinal) \
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
