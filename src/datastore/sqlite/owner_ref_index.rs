//! Owner reference index maintenance — pre-extracted owner references for fast
//! ownership lookups without JSON-decoding every resource blob.
//!
//! The `resource_owner_refs` table backs this module: one row per owner reference
//! per resource. Write paths call `upsert_owner_refs` / `delete_owner_refs` inside
//! their existing `db_call` closure so the index stays transactionally consistent
//! with the main resource table.

use serde_json::Value;

use super::queries;

/// Owner reference extracted from `metadata.ownerReferences`.
#[derive(Debug, Clone, PartialEq)]
struct OwnerRef {
    uid: String,
    api_version: String,
    kind: String,
    name: String,
    controller: bool,
    block_owner_deletion: bool,
}

/// Extract all owner references from `metadata.ownerReferences`.
fn extract_owner_refs(data: &Value) -> Vec<OwnerRef> {
    let owner_refs = data
        .get("metadata")
        .and_then(|m| m.get("ownerReferences"))
        .and_then(|ors| ors.as_array());

    match owner_refs {
        Some(refs) => refs
            .iter()
            .filter_map(|ref_value| {
                // uid is required by the field shape, but Kubernetes
                // conformance includes circular ownerRef cases with uid=="";
                // keep those rows so empty-UID lookups can stay indexed.
                let uid = ref_value.get("uid")?.as_str()?;

                Some(OwnerRef {
                    uid: uid.to_string(),
                    api_version: ref_value
                        .get("apiVersion")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    kind: ref_value
                        .get("kind")
                        .and_then(|k| k.as_str())
                        .unwrap_or("")
                        .to_string(),
                    name: ref_value
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or("")
                        .to_string(),
                    controller: ref_value
                        .get("controller")
                        .and_then(|c| c.as_bool())
                        .unwrap_or(false),
                    block_owner_deletion: ref_value
                        .get("blockOwnerDeletion")
                        .and_then(|b| b.as_bool())
                        .unwrap_or(false),
                })
            })
            .collect(),
        None => Vec::new(),
    }
}

/// Delete all owner ref rows for one resource.
pub(super) fn delete_owner_refs(
    conn: &rusqlite::Connection,
    api_version: &str,
    kind: &str,
    namespace: &str,
    name: &str,
) -> rusqlite::Result<()> {
    conn.execute(
        queries::OWNER_REF_INDEX_DELETE,
        rusqlite::params![api_version, kind, namespace, name],
    )?;
    Ok(())
}

/// Delete + re-insert all owner ref rows for a resource from its JSON bytes.
///
/// Accepts the serialized `data_bytes` to avoid requiring the caller to
/// deserialize the JSON — the cost of one extra deserialization per write
/// is negligible compared to the write itself.
pub(super) fn upsert_owner_refs(
    conn: &rusqlite::Connection,
    api_version: &str,
    kind: &str,
    namespace: &str,
    name: &str,
    data_bytes: &[u8],
) -> rusqlite::Result<()> {
    let data: Value = serde_json::from_slice(data_bytes)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    delete_owner_refs(conn, api_version, kind, namespace, name)?;
    insert_owner_refs(conn, api_version, kind, namespace, name, &data)
}

/// Insert owner ref rows for one resource from its deserialized JSON body.
fn insert_owner_refs(
    conn: &rusqlite::Connection,
    api_version: &str,
    kind: &str,
    namespace: &str,
    name: &str,
    data: &Value,
) -> rusqlite::Result<()> {
    let owner_refs = extract_owner_refs(data);
    if !owner_refs.is_empty() {
        let mut stmt = conn.prepare(queries::OWNER_REF_INDEX_INSERT)?;
        for (ordinal, owner_ref) in owner_refs.into_iter().enumerate() {
            stmt.execute(rusqlite::params![
                api_version,
                kind,
                namespace,
                name,
                owner_ref.uid,
                owner_ref.api_version,
                owner_ref.kind,
                owner_ref.name,
                if owner_ref.controller { 1 } else { 0 },
                if owner_ref.block_owner_deletion { 1 } else { 0 },
                ordinal as i64,
            ])?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_owner_refs_full() {
        let data = json!({
            "metadata": {
                "ownerReferences": [
                    {
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "nginx-7d6874b5f9",
                        "uid": "abc123",
                        "controller": true,
                        "blockOwnerDeletion": true
                    }
                ]
            }
        });

        let refs = extract_owner_refs(&data);
        assert_eq!(refs.len(), 1);
        assert_eq!(
            refs[0],
            OwnerRef {
                uid: "abc123".to_string(),
                api_version: "apps/v1".to_string(),
                kind: "ReplicaSet".to_string(),
                name: "nginx-7d6874b5f9".to_string(),
                controller: true,
                block_owner_deletion: true,
            }
        );
    }

    #[test]
    fn extract_owner_refs_multiple() {
        let data = json!({
            "metadata": {
                "ownerReferences": [
                    {
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "nginx-7d6874b5f9",
                        "uid": "abc123"
                    },
                    {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": "another-pod",
                        "uid": "def456",
                        "controller": false,
                        "blockOwnerDeletion": false
                    }
                ]
            }
        });

        let refs = extract_owner_refs(&data);
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].uid, "abc123");
        assert_eq!(refs[1].uid, "def456");
    }

    #[test]
    fn extract_owner_refs_missing_optional_fields() {
        let data = json!({
            "metadata": {
                "ownerReferences": [
                    {
                        "uid": "xyz789"
                    }
                ]
            }
        });

        let refs = extract_owner_refs(&data);
        assert_eq!(refs.len(), 1);
        assert_eq!(
            refs[0],
            OwnerRef {
                uid: "xyz789".to_string(),
                api_version: "".to_string(),
                kind: "".to_string(),
                name: "".to_string(),
                controller: false,
                block_owner_deletion: false,
            }
        );
    }

    #[test]
    fn extract_owner_refs_empty_when_no_owner_references() {
        let data = json!({"metadata": {}});
        assert!(extract_owner_refs(&data).is_empty());
    }

    #[test]
    fn extract_owner_refs_empty_when_no_metadata() {
        let data = json!({});
        assert!(extract_owner_refs(&data).is_empty());
    }

    #[test]
    fn extract_owner_refs_skips_missing_uid() {
        let data = json!({
            "metadata": {
                "ownerReferences": [
                    {
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "nginx-7d6874b5f9"
                    }
                ]
            }
        });

        let refs = extract_owner_refs(&data);
        assert!(refs.is_empty());
    }

    #[test]
    fn extract_owner_refs_keeps_empty_uid() {
        let data = json!({
            "metadata": {
                "ownerReferences": [
                    {
                        "uid": ""
                    }
                ]
            }
        });

        let refs = extract_owner_refs(&data);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].uid, "");
    }

    #[test]
    fn extract_owner_refs_preserves_empty_uid_for_identity_lookup() {
        let data = json!({
            "metadata": {
                "ownerReferences": [
                    {
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "cycle",
                        "uid": ""
                    }
                ]
            }
        });

        let refs = extract_owner_refs(&data);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].uid, "");
        assert_eq!(refs[0].api_version, "apps/v1");
        assert_eq!(refs[0].kind, "ReplicaSet");
        assert_eq!(refs[0].name, "cycle");
    }

    #[test]
    fn upsert_and_delete_round_trip() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        super::super::schema::init_schema_in_conn(&mut conn).unwrap();

        let data = json!({
            "metadata": {
                "ownerReferences": [
                    {
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "nginx-7d6874b5f9",
                        "uid": "abc123",
                        "controller": true,
                        "blockOwnerDeletion": true
                    }
                ]
            }
        });
        let data_bytes = serde_json::to_vec(&data).unwrap();

        upsert_owner_refs(&conn, "v1", "Pod", "default", "pod-1", &data_bytes).unwrap();

        // Verify owner ref row
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_owner_refs WHERE api_version='v1' AND kind='Pod' AND namespace='default' AND name='pod-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        // Verify fields
        let (owner_uid, owner_kind, controller, block_owner_deletion): (String, String, i64, i64) = conn
            .query_row(
                "SELECT owner_uid, owner_kind, controller, block_owner_deletion FROM resource_owner_refs WHERE api_version='v1' AND kind='Pod' AND namespace='default' AND name='pod-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(owner_uid, "abc123");
        assert_eq!(owner_kind, "ReplicaSet");
        assert_eq!(controller, 1);
        assert_eq!(block_owner_deletion, 1);

        // Delete
        delete_owner_refs(&conn, "v1", "Pod", "default", "pod-1").unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resource_owner_refs", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn upsert_replaces_existing_owner_refs() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        super::super::schema::init_schema_in_conn(&mut conn).unwrap();

        let data_v1 = json!({
            "metadata": {
                "ownerReferences": [
                    {
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "nginx-1",
                        "uid": "abc123"
                    }
                ]
            }
        });

        let data_v2 = json!({
            "metadata": {
                "ownerReferences": [
                    {
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "nginx-2",
                        "uid": "def456",
                        "controller": true
                    },
                    {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": "another",
                        "uid": "xyz789"
                    }
                ]
            }
        });

        upsert_owner_refs(
            &conn,
            "v1",
            "Pod",
            "default",
            "pod-1",
            &serde_json::to_vec(&data_v1).unwrap(),
        )
        .unwrap();

        upsert_owner_refs(
            &conn,
            "v1",
            "Pod",
            "default",
            "pod-1",
            &serde_json::to_vec(&data_v2).unwrap(),
        )
        .unwrap();

        // Should have exactly 2 owner refs from v2 (abc123 replaced)
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_owner_refs WHERE api_version='v1' AND kind='Pod' AND namespace='default' AND name='pod-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);

        // Verify the new refs are present
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_owner_refs WHERE owner_uid='def456'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        // Old ref should be gone
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_owner_refs WHERE owner_uid='abc123'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn cluster_scoped_uses_empty_namespace() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        super::super::schema::init_schema_in_conn(&mut conn).unwrap();

        let data = json!({
            "metadata": {
                "ownerReferences": [
                    {
                        "uid": "xyz789"
                    }
                ]
            }
        });

        upsert_owner_refs(
            &conn,
            "v1",
            "Node",
            "",
            "node-1",
            &serde_json::to_vec(&data).unwrap(),
        )
        .unwrap();

        let ns: String = conn
            .query_row(
                "SELECT namespace FROM resource_owner_refs WHERE api_version='v1' AND kind='Node' AND name='node-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ns, "");
    }

    #[test]
    fn ordinal_is_array_index() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        super::super::schema::init_schema_in_conn(&mut conn).unwrap();

        let data = json!({
            "metadata": {
                "ownerReferences": [
                    {"uid": "first"},
                    {"uid": "second"},
                    {"uid": "third"}
                ]
            }
        });

        upsert_owner_refs(
            &conn,
            "v1",
            "Pod",
            "default",
            "pod-1",
            &serde_json::to_vec(&data).unwrap(),
        )
        .unwrap();

        let ordinals: Vec<i64> = conn
            .prepare("SELECT ordinal FROM resource_owner_refs ORDER BY ordinal")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(ordinals, vec![0, 1, 2]);
    }
}
