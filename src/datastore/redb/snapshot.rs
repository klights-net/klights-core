//! DatastoreSnapshotter implementation for RedbDatastore (DSB-R-09a).
//!
//! Snapshot captures ClusterReplicated tables from a ReadTransaction.
//! Restore populates a fresh state.redb from an envelope.
//! NodeLocal tables (pod_sandboxes, pod_networks, pod_endpoints,
//! pod_workqueue) are excluded from cluster snapshots.

use ::redb::{ReadableDatabase, ReadableTable};
use anyhow::Result;
use async_trait::async_trait;

use crate::datastore::command::COMMAND_CODEC_VERSION;
use crate::datastore::snapshot::{
    DatastoreSnapshotter, SnapshotEntry, SnapshotEnvelope, SnapshotTable,
    compute_schema_fingerprint,
};

use super::RedbDatastore;
use super::tables;

/// Tables included in cluster snapshots (ClusterReplicated + ConfigReplicated).
/// NodeLocal tables are excluded — they belong to individual nodes.
const SNAPSHOT_TABLES: &[&str] = &[
    "res_cluster",
    "res_ns",
    "namespaces",
    "watch_events",
    "resources_by_owner",
    "rv_to_key",
    "node_subnets",
    "meta",
];

#[async_trait]
impl DatastoreSnapshotter for RedbDatastore {
    fn backend_kind(&self) -> &'static str {
        "redb"
    }

    fn schema_fingerprint(&self) -> String {
        compute_schema_fingerprint(SNAPSHOT_TABLES)
    }

    async fn snapshot(&self) -> Result<SnapshotEnvelope> {
        let r = self.accessor.db().unwrap().begin_read()?;

        let last_applied_rv: i64 = {
            let tbl = r.open_table(tables::META)?;
            tbl.get("rv")?
                .map(|g| {
                    std::str::from_utf8(g.value())
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(0)
                })
                .unwrap_or(0)
        };

        let last_applied_command_id: Option<String> = {
            let tbl = r.open_table(tables::META)?;
            tbl.get("last_applied_command_id")?
                .map(|g| std::str::from_utf8(g.value()).unwrap_or("").to_string())
        };

        let mut tables = Vec::new();

        // --- RES_CLUSTER ---
        {
            let tbl = r.open_table(tables::RES_CLUSTER)?;
            let mut entries = Vec::new();
            for e in tbl.iter()? {
                let (k, v) = e?;
                let key = k.value().to_vec();
                let value = serde_json::to_vec(&(v.value().0, v.value().1.to_vec()))?;
                entries.push(SnapshotEntry { key, value });
            }
            tables.push(SnapshotTable {
                name: "res_cluster".into(),
                entries,
            });
        }

        // --- RES_NS ---
        {
            let tbl = r.open_table(tables::RES_NS)?;
            let mut entries = Vec::new();
            for e in tbl.iter()? {
                let (k, v) = e?;
                let key = k.value().to_vec();
                let value = serde_json::to_vec(&(v.value().0, v.value().1.to_vec()))?;
                entries.push(SnapshotEntry { key, value });
            }
            tables.push(SnapshotTable {
                name: "res_ns".into(),
                entries,
            });
        }

        // --- NAMESPACES ---
        {
            let tbl = r.open_table(tables::NAMESPACES)?;
            let mut entries = Vec::new();
            for e in tbl.iter()? {
                let (k, v) = e?;
                let key = k.value().as_bytes().to_vec();
                let value = v.value().to_vec();
                entries.push(SnapshotEntry { key, value });
            }
            tables.push(SnapshotTable {
                name: "namespaces".into(),
                entries,
            });
        }

        // --- WATCH_EVENTS ---
        {
            let tbl = r.open_table(tables::WATCH_EVENTS)?;
            let mut entries = Vec::new();
            for e in tbl.iter()? {
                let (k, v) = e?;
                let key = k.value().to_le_bytes().to_vec();
                let value = v.value().to_vec();
                entries.push(SnapshotEntry { key, value });
            }
            tables.push(SnapshotTable {
                name: "watch_events".into(),
                entries,
            });
        }

        // --- RESOURCES_BY_OWNER ---
        {
            let tbl = r.open_table(tables::RESOURCES_BY_OWNER)?;
            let mut entries = Vec::new();
            for e in tbl.iter()? {
                let (k, v) = e?;
                let key = k.value().to_vec();
                let value = serde_json::to_vec(&(v.value().0, v.value().1.to_vec()))?;
                entries.push(SnapshotEntry { key, value });
            }
            tables.push(SnapshotTable {
                name: "resources_by_owner".into(),
                entries,
            });
        }

        // --- RV_TO_KEY ---
        {
            let tbl = r.open_table(tables::RV_TO_KEY)?;
            let mut entries = Vec::new();
            for e in tbl.iter()? {
                let (k, v) = e?;
                let key = k.value().to_le_bytes().to_vec();
                let value = v.value().to_vec();
                entries.push(SnapshotEntry { key, value });
            }
            tables.push(SnapshotTable {
                name: "rv_to_key".into(),
                entries,
            });
        }

        // --- NODE_SUBNETS ---
        {
            let tbl = r.open_table(tables::NODE_SUBNETS)?;
            let mut entries = Vec::new();
            for e in tbl.iter()? {
                let (k, v) = e?;
                let key = k.value().as_bytes().to_vec();
                let value = v.value().to_vec();
                entries.push(SnapshotEntry { key, value });
            }
            tables.push(SnapshotTable {
                name: "node_subnets".into(),
                entries,
            });
        }

        // --- META (only RV and command id) ---
        {
            let mut entries = Vec::new();
            {
                let tbl = r.open_table(tables::META)?;
                if let Some(g) = tbl.get("rv")? {
                    entries.push(SnapshotEntry {
                        key: b"rv".to_vec(),
                        value: g.value().to_vec(),
                    });
                }
                if let Some(g) = tbl.get("last_applied_command_id")? {
                    entries.push(SnapshotEntry {
                        key: b"last_applied_command_id".to_vec(),
                        value: g.value().to_vec(),
                    });
                }
            }
            if !entries.is_empty() {
                tables.push(SnapshotTable {
                    name: "meta".into(),
                    entries,
                });
            }
        }

        Ok(SnapshotEnvelope {
            backend_kind: "redb".to_string(),
            schema_fingerprint: self.schema_fingerprint(),
            codec_version: COMMAND_CODEC_VERSION,
            last_applied_rv,
            last_applied_command_id,
            tables,
        })
    }

    async fn restore(&self, envelope: &SnapshotEnvelope) -> Result<()> {
        self.validate_envelope(envelope)
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let w = self.accessor.db().unwrap().begin_write()?;

        for table in &envelope.tables {
            match table.name.as_str() {
                "res_cluster" => {
                    let mut tbl = w.open_table(tables::RES_CLUSTER)?;
                    for entry in &table.entries {
                        let (rv, body): (u64, Vec<u8>) = serde_json::from_slice(&entry.value)?;
                        tbl.insert(entry.key.as_slice(), (rv, body.as_slice()))?;
                    }
                }
                "res_ns" => {
                    let mut tbl = w.open_table(tables::RES_NS)?;
                    for entry in &table.entries {
                        let (rv, body): (u64, Vec<u8>) = serde_json::from_slice(&entry.value)?;
                        tbl.insert(entry.key.as_slice(), (rv, body.as_slice()))?;
                    }
                }
                "namespaces" => {
                    let mut tbl = w.open_table(tables::NAMESPACES)?;
                    for entry in &table.entries {
                        let name = std::str::from_utf8(&entry.key)
                            .map_err(|e| anyhow::anyhow!("bad namespace key: {e}"))?;
                        tbl.insert(name, entry.value.as_slice())?;
                    }
                }
                "watch_events" => {
                    let mut tbl = w.open_table(tables::WATCH_EVENTS)?;
                    for entry in &table.entries {
                        let rv = u64::from_le_bytes(
                            entry.key[..8]
                                .try_into()
                                .map_err(|_| anyhow::anyhow!("bad watch event key len"))?,
                        );
                        tbl.insert(rv, entry.value.as_slice())?;
                    }
                }
                "resources_by_owner" => {
                    let mut tbl = w.open_table(tables::RESOURCES_BY_OWNER)?;
                    for entry in &table.entries {
                        let (rv, body): (u64, Vec<u8>) = serde_json::from_slice(&entry.value)?;
                        tbl.insert(entry.key.as_slice(), (rv, body.as_slice()))?;
                    }
                }
                "rv_to_key" => {
                    let mut tbl = w.open_table(tables::RV_TO_KEY)?;
                    for entry in &table.entries {
                        let rv = u64::from_le_bytes(
                            entry.key[..8]
                                .try_into()
                                .map_err(|_| anyhow::anyhow!("bad rv_to_key key len"))?,
                        );
                        tbl.insert(rv, entry.value.as_slice())?;
                    }
                }
                "node_subnets" => {
                    let mut tbl = w.open_table(tables::NODE_SUBNETS)?;
                    for entry in &table.entries {
                        let name = std::str::from_utf8(&entry.key)
                            .map_err(|e| anyhow::anyhow!("bad node_subnet key: {e}"))?;
                        tbl.insert(name, entry.value.as_slice())?;
                    }
                }
                "meta" => {
                    let mut tbl = w.open_table(tables::META)?;
                    for entry in &table.entries {
                        let key = std::str::from_utf8(&entry.key)
                            .map_err(|e| anyhow::anyhow!("bad meta key: {e}"))?;
                        tbl.insert(key, entry.value.as_slice())?;
                    }
                }
                other => {
                    return Err(anyhow::anyhow!("unknown table in snapshot: {other}"));
                }
            }
        }

        w.commit()
            .map_err(|e| anyhow::anyhow!("restore commit failed: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::backend::DatastoreBackend;
    use crate::datastore::redb::RedbDatastore;
    use crate::pod_identity::PodIdentity;
    use serde_json::json;

    async fn fresh_redb() -> RedbDatastore {
        RedbDatastore::new_in_memory().await.unwrap()
    }

    #[tokio::test]
    async fn snapshot_round_trip_preserves_cluster_state_and_rv() {
        let db = fresh_redb().await;

        // Create some cluster state
        db.create_namespace("ns1", json!({"metadata":{"name":"ns1"}}))
            .await
            .unwrap();
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("ns1"),
            "cm1",
            json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm1","namespace":"ns1"},"data":{"k":"v"}}),
        )
        .await
        .unwrap();
        db.create_resource(
            "v1",
            "Node",
            None,
            "n1",
            json!({"apiVersion":"v1","kind":"Node","metadata":{"name":"n1"}}),
        )
        .await
        .unwrap();
        db.allocate_node_subnet("n1", "10.42.0.0/16", "192.168.1.1")
            .await
            .unwrap();

        let rv_before = db.get_current_resource_version().await.unwrap();

        // Snapshot
        let envelope = db.snapshot().await.unwrap();
        assert_eq!(envelope.backend_kind, "redb");
        assert_eq!(envelope.codec_version, COMMAND_CODEC_VERSION);
        assert!(envelope.last_applied_rv >= rv_before);

        // Restore into a fresh database
        let db2 = fresh_redb().await;
        db2.restore(&envelope).await.unwrap();

        // Verify restored state
        let ns = db2.get_namespace("ns1").await.unwrap();
        assert!(ns.is_some());

        let cm = db2
            .get_resource("v1", "ConfigMap", Some("ns1"), "cm1")
            .await
            .unwrap();
        assert!(cm.is_some());
        assert_eq!(cm.unwrap().data["data"]["k"], "v");

        let node = db2.get_resource("v1", "Node", None, "n1").await.unwrap();
        assert!(node.is_some());

        let rv_after = db2.get_current_resource_version().await.unwrap();
        assert_eq!(rv_after, envelope.last_applied_rv);
    }

    #[tokio::test]
    async fn snapshot_excludes_node_local_tables() {
        let db = fresh_redb().await;

        // Create node-local state
        db.record_sandbox("ns1", "pod1", "uid1", "sid1")
            .await
            .unwrap();
        db.ipam_allocate_and_record_pod_network(
            "sid1",
            &PodIdentity::new("ns1", "pod1", "uid1"),
            0x0A2A0100,
            256,
            "veth0",
            "/ns/sid1",
        )
        .await
        .unwrap();

        // Snapshot
        let envelope = db.snapshot().await.unwrap();

        // Verify node-local tables are NOT in the snapshot
        let table_names: Vec<&str> = envelope.tables.iter().map(|t| t.name.as_str()).collect();
        assert!(!table_names.contains(&"pod_sandboxes"));
        assert!(!table_names.contains(&"pod_networks"));
        assert!(!table_names.contains(&"pod_endpoints"));
        assert!(!table_names.contains(&"pod_workqueue"));
    }

    #[tokio::test]
    async fn restore_rejects_backend_mismatch() {
        let db = fresh_redb().await;
        let db2 = fresh_redb().await;

        let envelope = db.snapshot().await.unwrap();

        // Tamper with backend kind
        let mut bad = envelope.clone();
        bad.backend_kind = "sqlite".to_string();

        let err = db2.restore(&bad).await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("backend"));
    }

    #[tokio::test]
    async fn restore_rejects_codec_mismatch() {
        let db = fresh_redb().await;
        let db2 = fresh_redb().await;

        let envelope = db.snapshot().await.unwrap();

        let mut bad = envelope.clone();
        bad.codec_version = 999;

        let err = db2.restore(&bad).await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("codec"));
    }

    #[tokio::test]
    async fn restore_rejects_schema_mismatch() {
        let db = fresh_redb().await;
        let db2 = fresh_redb().await;

        let envelope = db.snapshot().await.unwrap();

        let mut bad = envelope.clone();
        bad.schema_fingerprint = "bad-fingerprint".to_string();

        let err = db2.restore(&bad).await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("schema"));
    }

    #[tokio::test]
    async fn schema_fingerprint_is_stable() {
        let db = fresh_redb().await;
        let fp1 = db.schema_fingerprint();
        let fp2 = db.schema_fingerprint();
        assert_eq!(fp1, fp2);
    }
}
