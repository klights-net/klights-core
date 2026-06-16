use std::path::Path;

use crate::datastore::errors::OpenError;
use rusqlite::OptionalExtension;
use sha2::{Digest, Sha256};

pub fn init_schema_in_conn(conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS outbox (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            idempotency_key     TEXT NOT NULL UNIQUE,
            enqueued_ms         INTEGER NOT NULL,
            subject_key         TEXT NOT NULL,
            subject_api_version TEXT NOT NULL,
            subject_kind        TEXT NOT NULL,
            subject_namespace   TEXT,
            subject_name        TEXT NOT NULL,
            subject_uid         TEXT,
            pod_uid             TEXT NOT NULL DEFAULT '',
            operation           TEXT NOT NULL CHECK(operation IN (
                'PodStatus',
                'RuntimeReconcile',
                'ProbeReadiness',
                'DeadlineExceeded',
                'ContainerStatusSnapshot',
                'EphemeralContainerStatuses',
                'PodMetadata',
                'NodeRegistration',
                'NodeDataplane',
                'NodeStatus',
                'LeaseRenew',
                'EventCreate'
            )),
            is_terminal_pod_delete INTEGER NOT NULL DEFAULT 0 CHECK(is_terminal_pod_delete IN (0, 1)),
            payload_proto       BLOB NOT NULL,
            attempt             INTEGER NOT NULL DEFAULT 0,
            next_due_ms         INTEGER NOT NULL,
            leased_until_ms     INTEGER NOT NULL DEFAULT 0,
            lease_token         TEXT,
            last_error          TEXT,
            CHECK (subject_kind != 'Pod' OR pod_uid <> '')
        );
        CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox(next_due_ms, id);
        CREATE INDEX IF NOT EXISTS idx_outbox_lease ON outbox(leased_until_ms);
        CREATE INDEX IF NOT EXISTS idx_outbox_subject ON outbox(subject_key, id);
        CREATE INDEX IF NOT EXISTS idx_outbox_pod_uid ON outbox(pod_uid) WHERE pod_uid <> '';

        CREATE TABLE IF NOT EXISTS pod_runtime (
            pod_uid     TEXT NOT NULL PRIMARY KEY,
            namespace   TEXT NOT NULL,
            pod_name    TEXT NOT NULL,
            node_name   TEXT NOT NULL,
            sandbox_id  TEXT,
            cgroup_path TEXT,
            created_ms  INTEGER NOT NULL,
            started_ms  INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_pod_runtime_name ON pod_runtime(namespace, pod_name);

        CREATE TABLE IF NOT EXISTS pod_status_checkpoints (
            pod_uid    TEXT NOT NULL PRIMARY KEY,
            namespace  TEXT NOT NULL,
            pod_name   TEXT NOT NULL,
            base_rv    INTEGER NOT NULL,
            applied_rv INTEGER,
            status_json BLOB NOT NULL,
            updated_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pod_status_checkpoints_name
            ON pod_status_checkpoints(namespace, pod_name);

        CREATE TABLE IF NOT EXISTS pod_runtime_observation_checkpoints (
            pod_uid          TEXT NOT NULL PRIMARY KEY,
            container_ids    TEXT NOT NULL,
            generation       INTEGER NOT NULL,
            updated_ms       INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pod_slot_admissions (
            namespace        TEXT NOT NULL,
            pod_name         TEXT NOT NULL,
            pod_uid          TEXT NOT NULL,
            node_name        TEXT NOT NULL,
            state            TEXT NOT NULL CHECK(state IN ('Admitted','Terminating')),
            updated_rv       INTEGER NOT NULL,
            updated_at_ms    INTEGER NOT NULL,
            PRIMARY KEY (namespace, pod_name)
        );

        CREATE TABLE IF NOT EXISTS pod_networks (
            sandbox_id TEXT PRIMARY KEY,
            namespace  TEXT NOT NULL,
            pod_name   TEXT NOT NULL,
            pod_uid    TEXT NOT NULL,
            ip_addr    TEXT NOT NULL,
            ip_int     INTEGER NOT NULL UNIQUE,
            veth_host  TEXT NOT NULL,
            netns_path TEXT NOT NULL,
            created_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pod_networks_uid ON pod_networks(pod_uid);
        CREATE INDEX IF NOT EXISTS idx_pod_networks_name ON pod_networks(namespace, pod_name);

        CREATE TABLE IF NOT EXISTS pod_endpoints (
            pod_uid       TEXT NOT NULL PRIMARY KEY,
            namespace     TEXT NOT NULL,
            pod_name      TEXT NOT NULL,
            node_name     TEXT NOT NULL,
            mode          TEXT NOT NULL CHECK(mode IN ('encrypted_direct','hostport')),
            pod_ip        TEXT NOT NULL,
            node_ip       TEXT,
            host_port_tcp INTEGER,
            host_port_udp INTEGER,
            generation    INTEGER NOT NULL,
            updated_ms    INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS pod_endpoints_node ON pod_endpoints(node_name);
        CREATE INDEX IF NOT EXISTS pod_endpoints_ns_pod ON pod_endpoints(namespace, pod_name);
        CREATE INDEX IF NOT EXISTS pod_endpoints_pod_ip ON pod_endpoints(pod_ip);

        CREATE TABLE IF NOT EXISTS pod_workqueue (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            pod_uid       TEXT NOT NULL,
            namespace     TEXT NOT NULL,
            pod_name      TEXT NOT NULL,
            kind          TEXT NOT NULL CHECK(kind IN ('pod','namespace')),
            enqueued_ms   INTEGER NOT NULL,
            next_due_ms   INTEGER NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            payload       BLOB NOT NULL,
            last_error    TEXT,
            UNIQUE(kind, namespace, pod_name, pod_uid)
        );
        CREATE INDEX IF NOT EXISTS idx_pod_workqueue_due ON pod_workqueue(next_due_ms);
        CREATE INDEX IF NOT EXISTS idx_pod_workqueue_uid ON pod_workqueue(pod_uid);

        CREATE TABLE IF NOT EXISTS probe_state (
            pod_uid          TEXT NOT NULL,
            container_name   TEXT NOT NULL,
            probe_kind       TEXT NOT NULL,
            last_result_ms   INTEGER,
            last_success     INTEGER,
            consecutive_fail INTEGER NOT NULL DEFAULT 0,
            next_eligible_ms INTEGER NOT NULL,
            PRIMARY KEY (pod_uid, container_name, probe_kind)
        );

        CREATE TABLE IF NOT EXISTS replication_checkpoint (
            singleton_key   INTEGER PRIMARY KEY CHECK (singleton_key = 0),
            last_applied_rv INTEGER NOT NULL,
            leader_epoch    INTEGER NOT NULL,
            cluster_id      TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS outbox_dead_letter (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            original_id         INTEGER NOT NULL,
            idempotency_key     TEXT NOT NULL,
            enqueued_ms         INTEGER NOT NULL,
            subject_key         TEXT NOT NULL,
            subject_api_version TEXT NOT NULL,
            subject_kind        TEXT NOT NULL,
            subject_namespace   TEXT,
            subject_name        TEXT NOT NULL,
            subject_uid         TEXT,
            pod_uid             TEXT NOT NULL DEFAULT '',
            operation           TEXT NOT NULL,
            payload_proto       BLOB NOT NULL,
            attempts            INTEGER NOT NULL,
            last_error          TEXT NOT NULL,
            moved_at_ms         INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS _node_meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        -- T3: the `log_apply_entries` table (Phase 2 LeaderFollower path)
        -- is removed. `raft_log_entries` is the sole durable log, backed
        -- by openraft's RaftLogStorage. Each row holds a serialized
        -- openraft::Entry<TypeConfig> plus its (term, leader_node_id).
        CREATE TABLE IF NOT EXISTS raft_log_entries (
            log_index      INTEGER PRIMARY KEY,
            term           INTEGER NOT NULL,
            leader_node_id INTEGER NOT NULL,
            entry_blob     BLOB    NOT NULL
        );

        -- Singleton metadata for the Raft layer: persisted vote, last
        -- committed log id, last purged log id. One row per key.
        CREATE TABLE IF NOT EXISTS raft_meta (
            key   TEXT PRIMARY KEY,
            value BLOB NOT NULL
        );
        ",
    )?;

    migrate_pod_endpoint_encrypted_direct_mode(conn)
}

fn migrate_pod_endpoint_encrypted_direct_mode(
    conn: &mut rusqlite::Connection,
) -> rusqlite::Result<()> {
    let table_sql: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='pod_endpoints'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let Some(table_sql) = table_sql else {
        return Ok(());
    };
    if !table_sql.contains("'vxlan'") || table_sql.contains("'encrypted_direct'") {
        return Ok(());
    }

    let tx = conn.transaction()?;
    tx.execute_batch(
        "
        ALTER TABLE pod_endpoints RENAME TO pod_endpoints_old;
        DROP INDEX IF EXISTS pod_endpoints_node;
        DROP INDEX IF EXISTS pod_endpoints_ns_pod;
        DROP INDEX IF EXISTS pod_endpoints_pod_ip;
        CREATE TABLE pod_endpoints (
            pod_uid       TEXT NOT NULL PRIMARY KEY,
            namespace     TEXT NOT NULL,
            pod_name      TEXT NOT NULL,
            node_name     TEXT NOT NULL,
            mode          TEXT NOT NULL CHECK(mode IN ('encrypted_direct','hostport')),
            pod_ip        TEXT NOT NULL,
            node_ip       TEXT,
            host_port_tcp INTEGER,
            host_port_udp INTEGER,
            generation    INTEGER NOT NULL,
            updated_ms    INTEGER NOT NULL
        );
        INSERT INTO pod_endpoints (
            pod_uid,
            namespace,
            pod_name,
            node_name,
            mode,
            pod_ip,
            node_ip,
            host_port_tcp,
            host_port_udp,
            generation,
            updated_ms
        )
        SELECT
            pod_uid,
            namespace,
            pod_name,
            node_name,
            CASE mode WHEN 'vxlan' THEN 'encrypted_direct' ELSE mode END,
            pod_ip,
            node_ip,
            host_port_tcp,
            host_port_udp,
            generation,
            updated_ms
        FROM pod_endpoints_old;
        DROP TABLE pod_endpoints_old;
        CREATE INDEX pod_endpoints_node ON pod_endpoints(node_name);
        CREATE INDEX pod_endpoints_ns_pod ON pod_endpoints(namespace, pod_name);
        CREATE INDEX pod_endpoints_pod_ip ON pod_endpoints(pod_ip);
        ",
    )?;

    let fingerprint = compute_fingerprint(&tx)?;
    tx.execute(
        "INSERT OR REPLACE INTO _node_meta (key, value) VALUES ('schema_fingerprint', ?1)",
        [&fingerprint],
    )?;
    tx.commit()
}

pub fn check_or_init_fingerprint(
    conn: &rusqlite::Connection,
    db_path: &Path,
) -> Result<(), OpenError> {
    let current = compute_fingerprint(conn).map_err(|e| OpenError::Corrupt {
        path: db_path.display().to_string(),
        details: format!("failed to compute node schema fingerprint: {e}"),
    })?;
    let stored: Option<String> = conn
        .query_row(
            "SELECT value FROM _node_meta WHERE key = 'schema_fingerprint'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| OpenError::Corrupt {
            path: db_path.display().to_string(),
            details: format!("failed to read node schema_fingerprint: {e}"),
        })?;

    match stored {
        None => {
            conn.execute(
                "INSERT INTO _node_meta (key, value) VALUES ('schema_fingerprint', ?1)",
                [&current],
            )
            .map_err(|e| OpenError::Corrupt {
                path: db_path.display().to_string(),
                details: format!("failed to write node schema_fingerprint: {e}"),
            })?;
            Ok(())
        }
        Some(actual) if actual == current => Ok(()),
        Some(actual) => Err(OpenError::SchemaMismatch {
            path: db_path.display().to_string(),
            expected: current,
            actual,
            hint: "node.db schema changed — restart with --wipe for this pre-prod branch"
                .to_string(),
        }),
    }
}

fn compute_fingerprint(conn: &rusqlite::Connection) -> rusqlite::Result<String> {
    let mut ddl: Vec<String> = conn
        .prepare(
            "SELECT sql FROM sqlite_master \
             WHERE type='table' AND name NOT LIKE 'sqlite_%' \
             ORDER BY name",
        )?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    for sql in &mut ddl {
        *sql = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    }

    let mut hasher = Sha256::new();
    for stmt in &ddl {
        hasher.update(stmt.as_bytes());
    }
    let bytes = hasher.finalize();
    Ok(bytes.iter().map(|b| format!("{:02x}", b)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn init_schema_migrates_old_pod_endpoint_mode_label() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE pod_endpoints (
                pod_uid       TEXT NOT NULL PRIMARY KEY,
                namespace     TEXT NOT NULL,
                pod_name      TEXT NOT NULL,
                node_name     TEXT NOT NULL,
                mode          TEXT NOT NULL CHECK(mode IN ('vxlan', 'hostport')),
                pod_ip        TEXT NOT NULL,
                node_ip       TEXT,
                host_port_tcp INTEGER,
                host_port_udp INTEGER,
                generation    INTEGER NOT NULL,
                updated_ms    INTEGER NOT NULL
            );
            CREATE INDEX pod_endpoints_node ON pod_endpoints(node_name);
            CREATE INDEX pod_endpoints_ns_pod ON pod_endpoints(namespace, pod_name);
            CREATE INDEX pod_endpoints_pod_ip ON pod_endpoints(pod_ip);
            CREATE TABLE _node_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO _node_meta (key, value) VALUES ('schema_fingerprint', 'old');
            INSERT INTO pod_endpoints (
                pod_uid,
                namespace,
                pod_name,
                node_name,
                mode,
                pod_ip,
                node_ip,
                generation,
                updated_ms
            ) VALUES (
                'uid-old',
                'default',
                'pod-old',
                'node-a',
                'vxlan',
                '10.42.0.10',
                '192.0.2.10',
                7,
                1700000000
            );
            ",
        )
        .unwrap();

        init_schema_in_conn(&mut conn).unwrap();
        check_or_init_fingerprint(&conn, Path::new("node.db")).unwrap();

        let mode: String = conn
            .query_row(
                "SELECT mode FROM pod_endpoints WHERE pod_uid = 'uid-old'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(mode, "encrypted_direct");

        let table_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='pod_endpoints'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(table_sql.contains("'encrypted_direct'"));
        assert!(!table_sql.contains("'vxlan', 'hostport'"));

        let index_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type='index' AND tbl_name='pod_endpoints' \
                 AND name IN ('pod_endpoints_node','pod_endpoints_ns_pod','pod_endpoints_pod_ip')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(index_count, 3);
    }
}
