use std::net::Ipv4Addr;

use crate::networking::{NodeName, PodSubnet};
use rusqlite::OptionalExtension;

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
    // Namespace-leading index for "list every kind in one namespace"
    // (`WHERE namespace=? ORDER BY kind, name`), used by namespace-content
    // listing and GC. The identity/watch indexes above lead with api_version,
    // so without this index those reads full-SCAN + temp-B-tree sort the
    // table on the single serialized DB thread (slow as the table grows,
    // and it starves raft DB I/O).
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_namespaced_namespace ON namespaced_resources(namespace, kind, name)",
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
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            api_version TEXT NOT NULL,
            kind TEXT NOT NULL,
            namespace TEXT,
            name TEXT NOT NULL,
            resource_version INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            data BLOB NOT NULL
        )",
        [],
    )?;
    migrate_watch_events_allow_same_rv(conn)?;
    migrate_watch_events_monotonic_id(conn)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_watch_events_ns
         ON watch_events(api_version, kind, namespace, resource_version, id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_watch_events_cluster
         ON watch_events(api_version, kind, resource_version, id)",
        [],
    )?;
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_watch_events_identity_rv
         ON watch_events(api_version, kind, COALESCE(namespace, '#cluster'), name, resource_version)",
        [],
    )?;
    // resource_version-leading index for rv-ordered reads: the raft
    // snapshot streamer (`WATCH_EVENTS_LIST_ALL_SINCE[_PAGED]`), the deleted-
    // event sweep, and watch catch-up all read `ORDER BY resource_version,
    // id`. Every other watch_events index leads with (api_version, kind), so
    // those reads otherwise full-SCAN + temp-B-tree sort the whole table on
    // the single serialized DB thread — slow as watch_events grows through a
    // conformance run, and it blocks raft log/apply I/O long enough to miss
    // heartbeat deadlines.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_watch_events_rv_id
         ON watch_events(resource_version, id)",
        [],
    )?;
    // GC ranks each resource scope by newest `id` and then deletes old rows.
    // Match that window partition/order so the serialized DB thread does not
    // sort the growing watch_events table every GC tick.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_watch_events_scope_id_desc
         ON watch_events(api_version, kind, COALESCE(namespace, '#cluster'), id DESC)",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS watch_replay_floors (
            api_version   TEXT NOT NULL,
            kind          TEXT NOT NULL,
            namespace_key TEXT NOT NULL,
            floor_rv      INTEGER NOT NULL,
            floor_event_id INTEGER NOT NULL DEFAULT 0,
            floor_position_exact INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY(api_version, kind, namespace_key)
        )",
        [],
    )?;
    migrate_watch_replay_floor_event_id(conn)?;
    migrate_watch_replay_floor_position_exact(conn)?;

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
            status_stamp    INTEGER,
            reserved_rv     INTEGER
        )",
        [],
    )?;
    migrate_applied_outbox_reserved_rv(conn)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_applied_outbox_subject
         ON applied_outbox(subject_key, first_seen_ms)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_applied_outbox_pending_reserved
         ON applied_outbox(subject_key, reserved_rv)
         WHERE reserved_rv IS NOT NULL AND applied_rv IS NULL AND length(result_proto) = 0",
        [],
    )?;

    // Raft-replicated worker outbox stream watermarks. This is durable cluster
    // metadata, not resource state: resource/namespace deletes must never remove
    // these rows. The primary key is the lookup/reload index used by leaders.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS outbox_stream_watermarks (
            client_id TEXT NOT NULL,
            stream_id INTEGER NOT NULL,
            last_seq  INTEGER NOT NULL,
            PRIMARY KEY(client_id, stream_id)
        ) WITHOUT ROWID",
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
    // vtep_ip is the first address of the allocated subnet, retained for
    // compatibility with existing row shape.
    // node_ip is the host's primary InternalIP.
    // mode is the peer mode projected from klights.io/mode annotation (F2-04).
    // hostport_range is the rootless host-port graft range (NULL for root peers).
    conn.execute(
        "CREATE TABLE IF NOT EXISTS node_subnets (
            node_name       TEXT PRIMARY KEY,
            subnet          TEXT NOT NULL UNIQUE,
            subnet_base_int INTEGER NOT NULL,
            vtep_ip         TEXT NOT NULL,
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

fn migrate_watch_events_allow_same_rv(conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
    let create_sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'watch_events'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let Some(create_sql) = create_sql else {
        return Ok(());
    };
    if !create_sql.contains("resource_version INTEGER NOT NULL UNIQUE") {
        return Ok(());
    }

    let tx = conn.transaction()?;
    tx.execute("ALTER TABLE watch_events RENAME TO watch_events_old", [])?;
    tx.execute(
        "CREATE TABLE watch_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            api_version TEXT NOT NULL,
            kind TEXT NOT NULL,
            namespace TEXT,
            name TEXT NOT NULL,
            resource_version INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            data BLOB NOT NULL
        )",
        [],
    )?;
    tx.execute(
        "INSERT INTO watch_events
         (id, api_version, kind, namespace, name, resource_version, event_type, data)
         SELECT id, api_version, kind, namespace, name, resource_version, event_type, data
         FROM watch_events_old
         ORDER BY id ASC",
        [],
    )?;
    tx.execute("DROP TABLE watch_events_old", [])?;
    tx.commit()
}

fn migrate_watch_events_monotonic_id(conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
    let create_sql: String = conn.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'watch_events'",
        [],
        |row| row.get(0),
    )?;
    let retained_high_water: i64 =
        conn.query_row("SELECT COALESCE(MAX(id), 0) FROM watch_events", [], |row| {
            row.get(0)
        })?;
    let retained_floor_high_water: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(floor_event_id), 0) FROM watch_replay_floors",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);
    let high_water = retained_high_water.max(retained_floor_high_water);
    if create_sql.to_ascii_uppercase().contains("AUTOINCREMENT") {
        if high_water > retained_high_water {
            super::Datastore::advance_watch_event_allocator_in_conn(conn, high_water)?;
        }
        return Ok(());
    }

    let tx = conn.transaction()?;
    tx.execute("ALTER TABLE watch_events RENAME TO watch_events_old", [])?;
    tx.execute(
        "CREATE TABLE watch_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            api_version TEXT NOT NULL,
            kind TEXT NOT NULL,
            namespace TEXT,
            name TEXT NOT NULL,
            resource_version INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            data BLOB NOT NULL
        )",
        [],
    )?;
    tx.execute(
        "INSERT INTO watch_events
         (id, api_version, kind, namespace, name, resource_version, event_type, data)
         SELECT id, api_version, kind, namespace, name, resource_version, event_type, data
         FROM watch_events_old
         ORDER BY id ASC",
        [],
    )?;
    tx.execute("DROP TABLE watch_events_old", [])?;
    if high_water > retained_high_water {
        super::Datastore::advance_watch_event_allocator_in_conn(&tx, high_water)?;
    }
    tx.commit()
}

fn migrate_applied_outbox_reserved_rv(conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
    let has_reserved_rv = {
        let mut stmt = conn.prepare("PRAGMA table_info(applied_outbox)")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        let mut found = false;
        for row in rows {
            if row? == "reserved_rv" {
                found = true;
                break;
            }
        }
        found
    };
    if !has_reserved_rv {
        conn.execute(
            "ALTER TABLE applied_outbox ADD COLUMN reserved_rv INTEGER",
            [],
        )?;
    }
    Ok(())
}

fn migrate_watch_replay_floor_event_id(conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
    let has_floor_event_id = {
        let mut stmt = conn.prepare("PRAGMA table_info(watch_replay_floors)")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        let mut found = false;
        for row in rows {
            if row? == "floor_event_id" {
                found = true;
                break;
            }
        }
        found
    };
    if !has_floor_event_id {
        conn.execute(
            "ALTER TABLE watch_replay_floors ADD COLUMN floor_event_id INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    Ok(())
}

fn migrate_watch_replay_floor_position_exact(
    conn: &mut rusqlite::Connection,
) -> rusqlite::Result<()> {
    let has_floor_position_exact = {
        let mut stmt = conn.prepare("PRAGMA table_info(watch_replay_floors)")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
            .iter()
            .any(|column| column == "floor_position_exact")
    };
    if !has_floor_position_exact {
        // Existing rows were persisted before the boundary encoded whether an
        // event ID was exact. Preserve that unknownness instead of treating a
        // historical `0` as a valid positioned floor.
        conn.execute(
            "ALTER TABLE watch_replay_floors
             ADD COLUMN floor_position_exact INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    Ok(())
}

pub(super) fn row_to_node_subnet(row: &rusqlite::Row<'_>) -> rusqlite::Result<NodeSubnet> {
    use crate::controllers::annotations::{NodePeerMode, parse_node_peer_mode};
    use crate::networking::types::HostPortRange;

    let node_name_str: String = row.get(0)?;
    let subnet_str: String = row.get(1)?;
    let vtep_ip_str: String = row.get(3)?;
    let node_ip_str: String = row.get(4)?;
    let mode_str: String = row.get(5).unwrap_or_else(|_| "root".to_string());
    let hostport_range_opt: Option<String> = row.get(6).unwrap_or(None);

    let node_name = NodeName::parse(&node_name_str).map_err(parse_err(0))?;
    let subnet = PodSubnet::parse(&subnet_str).map_err(parse_err(1))?;
    let vtep_ip: Ipv4Addr = vtep_ip_str.parse().map_err(|e: std::net::AddrParseError| {
        rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(e))
    })?;
    let node_ip: Ipv4Addr = node_ip_str.parse().map_err(|e: std::net::AddrParseError| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(e))
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

#[cfg(test)]
mod tests {
    use super::init_schema_in_conn;

    fn explain(conn: &rusqlite::Connection, sql: &str) -> String {
        let mut out = String::new();
        let mut stmt = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .expect("prepare explain");
        let mut rows = stmt.query([]).expect("query explain");
        while let Ok(Some(row)) = rows.next() {
            // EXPLAIN QUERY PLAN columns: (id, parent, notused, detail)
            let detail: String = row.get(3).unwrap_or_default();
            out.push_str(&detail);
            out.push('\n');
        }
        out
    }

    fn index_exists(conn: &rusqlite::Connection, name: &str) -> bool {
        conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
            [name],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0)
            == 1
    }

    #[test]
    fn upgrades_legacy_watch_event_allocator_without_reusing_gc_ids() {
        let mut conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
        conn.execute_batch(
            "CREATE TABLE watch_events (
                id INTEGER PRIMARY KEY,
                api_version TEXT NOT NULL,
                kind TEXT NOT NULL,
                namespace TEXT,
                name TEXT NOT NULL,
                resource_version INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                data BLOB NOT NULL
            );
            CREATE TABLE watch_replay_floors (
                api_version TEXT NOT NULL,
                kind TEXT NOT NULL,
                namespace_key TEXT NOT NULL,
                floor_rv INTEGER NOT NULL,
                floor_event_id INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(api_version, kind, namespace_key)
            );
            INSERT INTO watch_replay_floors
                (api_version, kind, namespace_key, floor_rv, floor_event_id)
            VALUES ('v1', 'ConfigMap', 'default', 41, 41);",
        )
        .expect("seed legacy schema after full watch GC");

        init_schema_in_conn(&mut conn).expect("upgrade schema");

        let table_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'watch_events'",
                [],
                |row| row.get(0),
            )
            .expect("watch_events DDL");
        assert!(table_sql.to_ascii_uppercase().contains("AUTOINCREMENT"));
        assert_eq!(
            conn.query_row(
                "SELECT seq FROM sqlite_sequence WHERE name = 'watch_events'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("durable allocator high-water"),
            41
        );

        conn.execute(
            "INSERT INTO watch_events
             (api_version, kind, namespace, name, resource_version, event_type, data)
             VALUES ('v1', 'ConfigMap', 'default', 'after-upgrade', 42, 'ADDED', x'00')",
            [],
        )
        .expect("insert after upgrade");
        assert_eq!(conn.last_insert_rowid(), 42);
    }

    /// Regression: the raft snapshot/GC `watch_events` paged reads order by
    /// `(resource_version, id)`, but every existing watch_events index led
    /// with `(api_version, kind)`, so the planner full-SCANned + temp-B-tree
    /// sorted the whole table on the single serialized DB thread. As
    /// watch_events grows through a conformance run this saturates the DB
    /// thread and starves raft heartbeats. There must be an index leading
    /// with `resource_version` that the rv-ordered reads seek into.
    #[test]
    fn watch_events_rv_ordered_reads_use_index_not_full_scan() {
        let mut conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
        init_schema_in_conn(&mut conn).expect("init schema");
        assert!(
            index_exists(&conn, "idx_watch_events_rv_id"),
            "schema must create idx_watch_events_rv_id for rv-ordered watch_event reads"
        );
        // Seed enough rows that the planner prefers the index over a scan.
        for rv in 1..=2000i64 {
            conn.execute(
                "INSERT INTO watch_events
                 (api_version, kind, namespace, name, resource_version, event_type, data)
                 VALUES ('v1','Pod','ns','n',?1,'MODIFIED',x'00')",
                [rv],
            )
            .unwrap();
        }
        let plan = explain(
            &conn,
            "SELECT id, resource_version FROM watch_events
             WHERE resource_version > 0
               AND (resource_version > 0 OR (resource_version = 0 AND id > 0))
             ORDER BY resource_version ASC, id ASC LIMIT 500",
        );
        assert!(
            !plan.contains("SCAN watch_events"),
            "rv-ordered watch_events read must seek an index, not full-scan. Plan:\n{plan}"
        );
    }

    /// Regression: watch_events GC ranks rows inside each
    /// `(api_version, kind, namespace)` scope by descending `id`. Without a
    /// matching expression index, SQLite scans an unrelated identity index and
    /// builds a temp B-tree for the window order on the serialized DB thread.
    #[test]
    fn watch_events_gc_scope_rank_uses_matching_index() {
        let mut conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
        init_schema_in_conn(&mut conn).expect("init schema");
        assert!(
            index_exists(&conn, "idx_watch_events_scope_id_desc"),
            "schema must create idx_watch_events_scope_id_desc for watch_event GC scope ranking"
        );
        for rv in 1..=2000i64 {
            conn.execute(
                "INSERT INTO watch_events
                 (api_version, kind, namespace, name, resource_version, event_type, data)
                 VALUES ('v1','Pod',?1,?2,?3,'MODIFIED',x'00')",
                rusqlite::params![format!("ns{}", rv % 5), format!("n{rv}"), rv],
            )
            .unwrap();
        }
        let plan = explain(
            &conn,
            "SELECT id, api_version, kind, COALESCE(namespace, '#cluster'), resource_version
             FROM (
                 SELECT id, api_version, kind, namespace, resource_version,
                        ROW_NUMBER() OVER (
                            PARTITION BY api_version, kind, COALESCE(namespace, '#cluster')
                            ORDER BY id DESC
                        ) AS scope_rank
                 FROM watch_events
             )
             WHERE id <= COALESCE((SELECT MAX(id) FROM watch_events), 0) - 1000
               AND scope_rank > 1000
             ORDER BY id ASC
             LIMIT 100",
        );
        assert!(
            plan.contains("idx_watch_events_scope_id_desc"),
            "watch_events GC scope ranking must use its matching index. Plan:\n{plan}"
        );
        assert!(
            !plan.contains("USE TEMP B-TREE FOR RIGHT PART OF ORDER BY"),
            "watch_events GC scope ranking must not sort each partition on the DB thread. Plan:\n{plan}"
        );
    }

    /// Regression: listing every kind in one namespace (`WHERE namespace=?
    /// ORDER BY kind, name`) full-SCANned because every namespaced index led
    /// with `api_version`. There must be a namespace-leading index.
    #[test]
    fn namespaced_all_kinds_in_namespace_uses_index_not_full_scan() {
        let mut conn = rusqlite::Connection::open_in_memory().expect("open in-memory");
        init_schema_in_conn(&mut conn).expect("init schema");
        assert!(
            index_exists(&conn, "idx_namespaced_namespace"),
            "schema must create idx_namespaced_namespace for all-kinds-in-namespace lists"
        );
        for i in 1..=2000i64 {
            conn.execute(
                "INSERT INTO namespaced_resources
                 (api_version, kind, namespace, name, uid, resource_version, created_rv, data)
                 VALUES ('v1',?1,'ns',?2,'u',?3,?3,x'00')",
                rusqlite::params![format!("Kind{}", i % 7), format!("n{i}"), i],
            )
            .unwrap();
        }
        let plan = explain(
            &conn,
            "SELECT data FROM namespaced_resources
             WHERE namespace='ns' ORDER BY kind, name",
        );
        assert!(
            !plan.contains("SCAN namespaced_resources"),
            "all-kinds-in-namespace list must seek an index, not full-scan. Plan:\n{plan}"
        );
    }
}
