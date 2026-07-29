//! SQLite query text owned by passive resource/history reads and indexes.

pub const METADATA_SELECT_RV_INT: &str =
    "SELECT CAST(value AS INTEGER) FROM metadata WHERE key = 'resource_version'";

pub const WATCH_EVENTS_LIST_CLUSTER_SINCE: &str = "SELECT api_version, kind, NULL as namespace, name, resource_version, event_type, data \
     FROM watch_events \
     WHERE api_version = ?1 AND kind = ?2 AND namespace IS NULL AND resource_version > ?3 \
     ORDER BY resource_version ASC, id ASC";
pub const WATCH_EVENTS_LIST_NAMESPACED_SINCE_HEAD: &str = "SELECT api_version, kind, namespace, name, resource_version, event_type, data \
     FROM watch_events \
     WHERE api_version = ?1 AND kind = ?2 AND resource_version > ?3";
pub const WATCH_EVENTS_LIST_TARGETS_HEAD: &str = "SELECT api_version, kind, namespace, name, resource_version, event_type, data \
     FROM watch_events WHERE resource_version > ?1 AND (";
pub const WATCH_EVENTS_LIST_ALL_SINCE: &str = "SELECT api_version, kind, namespace, name, resource_version, event_type, data \
     FROM watch_events \
     WHERE resource_version > ?1 \
     ORDER BY resource_version ASC, id ASC";
pub const WATCH_EVENTS_LIST_DELETED_SINCE: &str = "SELECT api_version, kind, namespace, name, resource_version, event_type, data \
     FROM watch_events \
     WHERE resource_version > ?1 AND event_type = 'DELETED' \
     ORDER BY resource_version ASC, id ASC";
pub const WATCH_EVENTS_MIN_RV: &str =
    "SELECT resource_version FROM watch_events ORDER BY id ASC LIMIT 1";
pub const WATCH_REPLAY_RETENTION_FLOOR_FOR_SCOPE: &str =
    "SELECT floor_rv, floor_event_id, floor_position_exact FROM watch_replay_floors
     WHERE api_version = ?1 AND kind = ?2 AND namespace_key = ?3";
pub const WATCH_REPLAY_RETENTION_FLOOR_FOR_NAMESPACED_ALL: &str =
    "SELECT floor_rv, floor_event_id, floor_position_exact FROM watch_replay_floors
     WHERE api_version = ?1 AND kind = ?2 AND namespace_key <> '#cluster'";

pub const NAMESPACE_GET: &str =
    "SELECT name, resource_version, uid, data FROM namespaces WHERE name = ?1";
pub const NAMESPACES_LIST_HEAD: &str = "SELECT name, resource_version, uid, data FROM namespaces";
pub const NAMESPACE_RESOURCES_LIST_ALL: &str =
    "SELECT id, api_version, kind, namespace, name, resource_version, uid, data
     FROM namespaced_resources
     WHERE namespace = ?1
     ORDER BY kind, name";
pub const NAMESPACE_RESOURCES_LIST_OF_KIND: &str =
    "SELECT id, api_version, kind, namespace, name, resource_version, uid, data
     FROM namespaced_resources
     WHERE namespace = ?1 AND kind = ?2
     ORDER BY kind, name";
pub const NAMESPACE_RESOURCES_LIST_EXCLUDING_KIND: &str =
    "SELECT id, api_version, kind, namespace, name, resource_version, uid, data
     FROM namespaced_resources
     WHERE namespace = ?1 AND kind <> ?2
     ORDER BY kind, name";
pub const NAMESPACE_RESOURCES_COUNT: &str =
    "SELECT COUNT(*) FROM namespaced_resources WHERE namespace = ?1";

pub const NAMESPACED_GET_EVENT_COMPAT: &str = "SELECT id, api_version, kind, namespace, name, resource_version, uid, data FROM namespaced_resources WHERE api_version IN ('v1', 'events.k8s.io/v1') AND kind = ?1 AND namespace = ?2 AND name = ?3 LIMIT 1";
pub const NAMESPACED_GET: &str = "SELECT id, api_version, kind, namespace, name, resource_version, uid, data FROM namespaced_resources WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";
pub const CLUSTER_GET: &str = "SELECT id, api_version, kind, name, resource_version, uid, data FROM cluster_resources WHERE api_version = ?1 AND kind = ?2 AND name = ?3";
pub const NAMESPACED_LIST_HEAD: &str = "SELECT id, api_version, kind, namespace, name, resource_version, uid, data FROM namespaced_resources ";
pub const NAMESPACED_COUNT_HEAD: &str = "SELECT COUNT(*) FROM namespaced_resources ";
pub const CLUSTER_LIST_HEAD: &str = "SELECT id, api_version, kind, name, resource_version, uid, data FROM cluster_resources WHERE api_version = ?1 AND kind = ?2";
pub const CLUSTER_LIST_ALL: &str = "SELECT id, api_version, kind, name, resource_version, uid, data FROM cluster_resources ORDER BY api_version, kind, name";
pub const CLUSTER_COUNT_HEAD: &str =
    "SELECT COUNT(*) FROM cluster_resources WHERE api_version = ?1 AND kind = ?2";
pub const NAMESPACED_LIST_BY_AV_KIND_HEAD: &str = "WHERE api_version = ?1 AND kind = ?2";
pub const NAMESPACED_LIST_BY_KIND_EVENT_COMPAT_HEAD: &str =
    "WHERE api_version IN ('v1', 'events.k8s.io/v1') AND kind = ?1";
pub const NAMESPACED_KEYS_FOR_SCOPE: &str = "SELECT namespace, name
         FROM namespaced_resources
         WHERE api_version = ?1 AND kind = ?2";
pub const CLUSTER_KEYS_FOR_SCOPE: &str = "SELECT name
         FROM cluster_resources
         WHERE api_version = ?1 AND kind = ?2";

pub const NODE_SUBNET_SELECT_BY_NAME: &str = "SELECT node_name, subnet, subnet_base_int, gateway_ip, \
                node_ip, mode, hostport_range \
         FROM node_subnets WHERE node_name = ?1";
pub const NODE_SUBNET_LIST_PEERS: &str = "SELECT node_name, subnet, subnet_base_int, gateway_ip, \
                node_ip, mode, hostport_range \
         FROM node_subnets WHERE node_name != ?1";
pub const NODE_DATAPLANE_SELECT_BY_NAME: &str = "SELECT node_name, mode, encryption, public_key, endpoint, port \
       FROM node_dataplane WHERE node_name = ?1";

pub const OWNERSHIP_INDEXED_NAMESPACED_BY_UID: &str = "SELECT r.id, r.api_version, r.kind, r.namespace, r.name, r.resource_version, r.uid, r.data \
     FROM namespaced_resources r \
     INNER JOIN resource_owner_refs o ON o.api_version = r.api_version AND o.kind = r.kind AND o.namespace = r.namespace AND o.name = r.name \
     WHERE o.owner_uid = ?1";
pub const OWNERSHIP_INDEXED_CLUSTER_BY_UID: &str = "SELECT r.id, r.api_version, r.kind, r.name, r.resource_version, r.uid, r.data \
     FROM cluster_resources r \
     INNER JOIN resource_owner_refs o ON o.api_version = r.api_version AND o.kind = r.kind AND o.namespace = '' AND o.name = r.name \
     WHERE o.owner_uid = ?1";
pub const OWNERSHIP_INDEXED_NAMESPACED_BY_KIND_AV_UID: &str = "SELECT r.id, r.api_version, r.kind, r.namespace, r.name, r.resource_version, r.uid, r.data \
     FROM namespaced_resources r \
     INNER JOIN resource_owner_refs o ON o.api_version = r.api_version AND o.kind = r.kind AND o.namespace = r.namespace AND o.name = r.name \
     WHERE r.kind = ?1 AND r.namespace = ?2 AND r.api_version = ?3 AND o.owner_uid = ?4";
pub const OWNERSHIP_INDEXED_CLUSTER_BY_KIND_AV_UID: &str = "SELECT r.id, r.api_version, r.kind, r.name, r.resource_version, r.uid, r.data \
     FROM cluster_resources r \
     INNER JOIN resource_owner_refs o ON o.api_version = r.api_version AND o.kind = r.kind AND o.namespace = '' AND o.name = r.name \
     WHERE r.kind = ?1 AND r.api_version = ?2 AND o.owner_uid = ?3";
pub const OWNERSHIP_INDEXED_NAMESPACED_EMPTY_UID_BY_IDENTITY: &str = "SELECT r.id, r.api_version, r.kind, r.namespace, r.name, r.resource_version, r.uid, r.data \
     FROM resource_owner_refs o \
     INNER JOIN namespaced_resources r ON r.api_version = o.api_version AND r.kind = o.kind AND r.namespace = o.namespace AND r.name = o.name \
     WHERE o.owner_kind = ?1 AND o.owner_name = ?2 AND o.owner_uid = ''";

pub const LABEL_INDEX_DELETE_FOR_RESOURCE: &str = "DELETE FROM resource_labels WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";
pub const FIELD_INDEX_DELETE_FOR_RESOURCE: &str = "DELETE FROM resource_fields WHERE api_version = ?1 AND kind = ?2 AND namespace = ?3 AND name = ?4";
pub const LABEL_INDEX_INSERT: &str = "INSERT INTO resource_labels (api_version, kind, namespace, name, key, value) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
pub const FIELD_INDEX_INSERT: &str = "INSERT INTO resource_fields (api_version, kind, namespace, name, field, value) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
