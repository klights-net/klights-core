use std::net::Ipv4Addr;

use crate::networking::{NodeName, PodSubnet, VtepMac};

use super::NodeSubnet;

/// Standalone function that initializes the schema on a raw connection.
/// Used by the opener in `executor.rs::open_with_opts`.
pub(super) fn init_schema_in_conn(conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;

    // Namespaced resources: namespace is NOT NULL, UNIQUE(api_version, kind, namespace, name).
    // api_version leads identity, watch, and owner-uid indexes so cross-api-group
    // resources with the same kind/name (e.g. example.alpha/v1 Widget vs
    // example.beta/v1 Widget) do not collide.
    // created_rv tracks the resource_version at INSERT time so watch catch-up
    // can emit ADDED (not MODIFIED) for newly-created resources.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS namespaced_resources (id INTEGER PRIMARY KEY, api_version TEXT NOT NULL, kind TEXT NOT NULL, namespace TEXT NOT NULL, name TEXT NOT NULL, uid TEXT NOT NULL, resource_version INTEGER NOT NULL, created_rv INTEGER NOT NULL DEFAULT 0, data BLOB NOT NULL)",
        [],
    )?;
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_namespaced_unique ON namespaced_resources(api_version, kind, namespace, name)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_namespaced_watch ON namespaced_resources(api_version, kind, namespace, resource_version)",
        [],
    )?;
    // First-ownerRef expression index retained as a fast path for legacy/simple
    // owner queries. Correct GC owner matching is done through the normalized
    // resource_owner_refs table so non-first owners are never missed.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_namespaced_owner_uid \
         ON namespaced_resources(api_version, kind, namespace, json_extract(data, '$.metadata.ownerReferences[0].uid')) \
         WHERE json_extract(data, '$.metadata.ownerReferences[0].uid') IS NOT NULL",
        [],
    )?;
    // First-ownerRef UID index for broad owner walks. GC queries still verify
    // every ownerReferences entry for Kubernetes-compatible multi-owner cases.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_namespaced_owner_uid_any_kind \
         ON namespaced_resources(json_extract(data, '$.metadata.ownerReferences[0].uid')) \
         WHERE json_extract(data, '$.metadata.ownerReferences[0].uid') IS NOT NULL",
        [],
    )?;

    // Cluster-scoped resources: no namespace column, UNIQUE(api_version, kind, name).
    // See comment on namespaced_resources above for the api_version-leading index rationale.
    // created_rv tracks the resource_version at INSERT time so watch catch-up
    // can emit ADDED (not MODIFIED) for newly-created resources.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS cluster_resources (id INTEGER PRIMARY KEY, api_version TEXT NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL, uid TEXT NOT NULL, resource_version INTEGER NOT NULL, created_rv INTEGER NOT NULL DEFAULT 0, data BLOB NOT NULL)",
        [],
    )?;
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_cluster_unique ON cluster_resources(api_version, kind, name)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cluster_watch ON cluster_resources(api_version, kind, resource_version)",
        [],
    )?;
    // Cluster-scoped first-ownerRef index retained for simple owner lookups.
    // Cluster-scoped sibling of idx_namespaced_owner_uid: lets the GC walk
    // and any cluster-scoped owner-walks hit the index instead of scanning
    // the whole table. Recreated with api_version as leading column.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cluster_owner_uid \
         ON cluster_resources(api_version, kind, json_extract(data, '$.metadata.ownerReferences[0].uid')) \
         WHERE json_extract(data, '$.metadata.ownerReferences[0].uid') IS NOT NULL",
        [],
    )?;
    // Cluster-scoped first-ownerRef UID index for owner walks that span Kinds.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cluster_owner_uid_any_kind \
         ON cluster_resources(json_extract(data, '$.metadata.ownerReferences[0].uid')) \
         WHERE json_extract(data, '$.metadata.ownerReferences[0].uid') IS NOT NULL",
        [],
    )?;

    // Selector index tables: pre-extracted label key-value pairs and field
    // selector values, maintained in the same transaction as resource writes.
    // Queries with label/field selectors + LIMIT probe these indexes instead of
    // JSON-decoding every row in the main resource table.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS resource_labels (api_version TEXT NOT NULL, kind TEXT NOT NULL, namespace TEXT NOT NULL, name TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_resource_labels_lookup ON resource_labels(api_version, kind, namespace, key, value, name)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_resource_labels_exists ON resource_labels(api_version, kind, namespace, key, name)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_resource_labels_resource ON resource_labels(api_version, kind, namespace, name)",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS resource_fields (api_version TEXT NOT NULL, kind TEXT NOT NULL, namespace TEXT NOT NULL, name TEXT NOT NULL, field TEXT NOT NULL, value TEXT NOT NULL)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_resource_fields_lookup ON resource_fields(api_version, kind, namespace, field, value, name)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_resource_fields_resource ON resource_fields(api_version, kind, namespace, name)",
        [],
    )?;

    // Owner reference index table: pre-extracted owner references for fast
    // ownership lookups without JSON-decoding every resource blob.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS resource_owner_refs (
            api_version TEXT NOT NULL,
            kind TEXT NOT NULL,
            namespace TEXT NOT NULL,
            name TEXT NOT NULL,
            owner_uid TEXT NOT NULL,
            owner_api_version TEXT,
            owner_kind TEXT,
            owner_name TEXT,
            controller INTEGER NOT NULL DEFAULT 0,
            block_owner_deletion INTEGER NOT NULL DEFAULT 0,
            ordinal INTEGER NOT NULL,
            PRIMARY KEY(api_version, kind, namespace, name, owner_uid, ordinal)
        )",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_resource_owner_refs_uid \
         ON resource_owner_refs(owner_uid, namespace, api_version, kind, name)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_resource_owner_refs_owner_identity \
         ON resource_owner_refs(owner_api_version, owner_kind, owner_name, namespace, owner_uid)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_resource_owner_refs_resource ON resource_owner_refs(api_version, kind, namespace, name)",
        [],
    )?;

    // Namespaces table with name as PRIMARY KEY
    conn.execute(
        "CREATE TABLE IF NOT EXISTS namespaces (name TEXT PRIMARY KEY, uid TEXT NOT NULL, resource_version INTEGER NOT NULL, data BLOB NOT NULL)",
        [],
    )?;

    // Durable watch history for watch catch-up, lagged recovery, and replica
    // promotion. Local watch delivery/cache state is only the in-memory
    // broadcast/subscriber layer and is rebuilt from this table plus current
    // resources after restart.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS watch_events (
            id INTEGER PRIMARY KEY,
            api_version TEXT NOT NULL,
            kind TEXT NOT NULL,
            namespace TEXT,
            name TEXT NOT NULL,
            resource_version INTEGER NOT NULL UNIQUE,
            event_type TEXT NOT NULL,
            data BLOB NOT NULL
        )",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_watch_events_ns
         ON watch_events(api_version, kind, namespace, resource_version)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_watch_events_cluster
         ON watch_events(api_version, kind, resource_version)",
        [],
    )?;

    // Applied outbox idempotency ledger. Leader-side outbox apply stores one
    // row in the same cluster datastore that owns the corresponding mutation,
    // so worker retries can replay a stable result without repeating effects.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS applied_outbox (
            idempotency_key TEXT PRIMARY KEY,
            subject_key     TEXT NOT NULL,
            operation       TEXT NOT NULL,
            first_seen_ms   INTEGER NOT NULL,
            applied_rv      INTEGER,
            result_proto    BLOB NOT NULL,
            status_stamp    INTEGER
        )",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_applied_outbox_subject
         ON applied_outbox(subject_key, first_seen_ms)",
        [],
    )?;

    // UID-bound cleanup intents for Pods whose active API object was removed
    // without kubelet contact, e.g. a Pod left behind on a lost node.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS pod_cleanup_intents (
            node_name        TEXT NOT NULL,
            namespace        TEXT NOT NULL,
            pod_name         TEXT NOT NULL,
            pod_uid          TEXT NOT NULL,
            reason           TEXT NOT NULL,
            resource_version INTEGER NOT NULL,
            created_at_ms    INTEGER NOT NULL,
            pod_data         BLOB NOT NULL,
            PRIMARY KEY(node_name, namespace, pod_name, pod_uid, reason)
        )",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_pod_cleanup_intents_node
         ON pod_cleanup_intents(node_name, namespace, pod_name, pod_uid, reason)",
        [],
    )?;

    // Metadata table for resource_version counter
    conn.execute(
        "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        [],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO metadata (key, value) VALUES ('resource_version', '0')",
        [],
    )?;

    // _klights_meta: schema fingerprint and other per-binary local state.
    // This is separate from `metadata` because metadata is legacy (resource_version)
    // and _klights_meta is the DSB-02+ surface for new metadata items.
    // Both tables persist; metadata stays for compatibility.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _klights_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        [],
    )?;

    // node_subnets: one row per klights node in the cluster.
    // Populated by the node_subnet controller at startup.
    // vtep_ip is the /32 address assigned on klights.vxlan (first addr of subnet).
    // vtep_mac is the kernel-assigned MAC of klights.vxlan (NULL for rootless peers — F2-04).
    // node_ip is the host's primary InternalIP (underlay address for VXLAN UDP).
    // mode is the peer mode projected from klights.io/mode annotation (F2-04).
    // hostport_range is the rootless host-port graft range (NULL for root peers).
    conn.execute(
        "CREATE TABLE IF NOT EXISTS node_subnets (
            node_name       TEXT PRIMARY KEY,
            subnet          TEXT NOT NULL UNIQUE,
            subnet_base_int INTEGER NOT NULL,
            vtep_ip         TEXT NOT NULL,
            vtep_mac        TEXT,
            node_ip         TEXT NOT NULL,
            mode            TEXT NOT NULL DEFAULT 'root',
            hostport_range  TEXT,
            created_at      INTEGER NOT NULL
        )",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_node_subnets_subnet ON node_subnets(subnet)",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS node_dataplane (
            node_name  TEXT PRIMARY KEY,
            mode       TEXT NOT NULL CHECK(mode IN ('root','rootless')),
            encryption TEXT NOT NULL CHECK(encryption IN ('enabled','disabled')),
            public_key TEXT,
            endpoint   TEXT NOT NULL,
            port       INTEGER,
            updated_at INTEGER NOT NULL
        )",
        [],
    )?;
    Ok(())
}

pub(super) fn row_to_node_subnet(row: &rusqlite::Row<'_>) -> rusqlite::Result<NodeSubnet> {
    use crate::controllers::annotations::{NodePeerMode, parse_node_peer_mode};
    use crate::networking::types::HostPortRange;

    let node_name_str: String = row.get(0)?;
    let subnet_str: String = row.get(1)?;
    let vtep_ip_str: String = row.get(3)?;
    // F2-04: vtep_mac is nullable; tolerate an empty string from test fixtures.
    let vtep_mac_opt: Option<String> = row.get(4)?;
    let node_ip_str: String = row.get(5)?;
    let mode_str: String = row.get(6).unwrap_or_else(|_| "root".to_string());
    let hostport_range_opt: Option<String> = row.get(7).unwrap_or(None);

    let node_name = NodeName::parse(&node_name_str).map_err(parse_err(0))?;
    let subnet = PodSubnet::parse(&subnet_str).map_err(parse_err(1))?;
    let vtep_ip: Ipv4Addr = vtep_ip_str.parse().map_err(|e: std::net::AddrParseError| {
        rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let vtep_mac = match vtep_mac_opt.as_deref() {
        None | Some("") => None,
        Some(s) => Some(VtepMac::parse(s).map_err(parse_err(4))?),
    };
    let node_ip: Ipv4Addr = node_ip_str.parse().map_err(|e: std::net::AddrParseError| {
        rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let mode = parse_node_peer_mode(Some(mode_str.as_str())).unwrap_or(NodePeerMode::Root);
    let hostport_range = hostport_range_opt
        .as_deref()
        .filter(|s| !s.is_empty())
        .and_then(|s| HostPortRange::parse(s).ok());

    Ok(NodeSubnet {
        node_name,
        subnet,
        subnet_base_int: row.get::<_, i64>(2)? as u32,
        vtep_ip,
        vtep_mac,
        node_ip,
        mode,
        hostport_range,
    })
}

fn parse_err(idx: usize) -> impl Fn(String) -> rusqlite::Error {
    move |msg| {
        rusqlite::Error::FromSqlConversionFailure(
            idx,
            rusqlite::types::Type::Text,
            Box::new(NodeSubnetParseError(msg)),
        )
    }
}

#[derive(Debug)]
struct NodeSubnetParseError(String);

impl std::fmt::Display for NodeSubnetParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for NodeSubnetParseError {}
