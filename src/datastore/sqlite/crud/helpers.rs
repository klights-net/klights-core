//! Static helpers used by SQLite resource CRUD.

use super::super::queries;
use super::*;

/// Read-side K8s Events compat shim. Identity (insert/update/delete) is
/// strict per (api_version, kind, namespace, name) — the new unique index
/// enforces it. But Events have a long-standing K8s compat where the same
/// resource may be addressed via either `core/v1` or `events.k8s.io/v1`,
/// so a client that POSTed to one group expects to also see the row via
/// the other group's READ path.
///
/// For Event reads, expand the api_version filter to cover both groups.
/// For everything else, return the single api_version verbatim. This is
/// explicit and documented — NOT the ambiguous identity bypass that used
/// to live inline as `event_kind_lookup`.
pub fn event_read_api_versions(api_version: &str, kind: &str) -> Vec<&'static str> {
    if kind == "Event" && (api_version == "v1" || api_version == "events.k8s.io/v1") {
        vec!["v1", "events.k8s.io/v1"]
    } else {
        // Caller already owns the single api_version string; the static lifetime
        // here is unused (callers won't read element 0 in this branch).
        Vec::new()
    }
}

/// Returns true iff this read needs the cross-group expansion above.
pub fn needs_event_v1_compat(api_version: &str, kind: &str) -> bool {
    !event_read_api_versions(api_version, kind).is_empty()
}

/// Insert a row into `watch_events` for a CRUD mutation. Used by
/// create/update/update_status/patch/delete on both the namespaced and
/// cluster tables — bind `namespace = None` for cluster-scoped resources
/// and rusqlite stores NULL.
///
/// Replication is at-least-once: after a follower disconnects and
/// reconnects, the leader's snapshot may include an entry the follower
/// already applied (then later mutated). The follower's apply path may
/// then call this helper for the same resource identity and RV, hitting
/// the unique identity/RV index and rolling back the entire apply
/// transaction. To make replay idempotent, on a UNIQUE failure we look up
/// the existing row at this identity/RV: if it has the same `event_type`
/// and identical `data`, the duplicate is a benign replay and we silently
/// succeed; otherwise the divergence is real and we propagate the
/// original error.
pub struct WatchEventInsert<'a> {
    pub api_version: &'a str,
    pub kind: &'a str,
    pub namespace: Option<&'a str>,
    pub name: &'a str,
    pub resource_version: i64,
    pub event_type: &'a str,
    pub data: &'a [u8],
}

impl<'a> WatchEventInsert<'a> {
    pub fn new(
        api_version: &'a str,
        kind: &'a str,
        namespace: Option<&'a str>,
        name: &'a str,
        resource_version: i64,
        event_type: &'a str,
        data: &'a [u8],
    ) -> Self {
        Self {
            api_version,
            kind,
            namespace,
            name,
            resource_version,
            event_type,
            data,
        }
    }
}

pub fn insert_watch_event_in_conn(
    conn: &rusqlite::Connection,
    event: WatchEventInsert<'_>,
) -> rusqlite::Result<()> {
    let WatchEventInsert {
        api_version,
        kind,
        namespace,
        name,
        resource_version,
        event_type,
        data,
    } = event;
    match conn.execute(
        queries::WATCH_EVENTS_INSERT,
        rusqlite::params![
            api_version,
            kind,
            namespace,
            name,
            resource_version,
            event_type,
            data
        ],
    ) {
        Ok(_) => Ok(()),
        Err(err) => {
            if !is_watch_events_unique_violation(&err) {
                return Err(err);
            }
            // UNIQUE(identity, resource_version) hit. Check the existing row matches.
            let namespace_key = namespace.unwrap_or("#cluster");
            let existing = conn.query_row(
                queries::WATCH_EVENTS_SELECT_BY_IDENTITY_RV,
                rusqlite::params![api_version, kind, namespace_key, name, resource_version],
                |row| {
                    Ok(ExistingWatchEvent {
                        event_type: row.get(0)?,
                        data: row.get(1)?,
                    })
                },
            );
            match existing {
                Ok(ex) if ex.matches(event_type, data) => Ok(()),
                _ => Err(err),
            }
        }
    }
}

pub struct ExistingWatchEvent {
    event_type: String,
    data: Vec<u8>,
}

impl ExistingWatchEvent {
    fn matches(&self, event_type: &str, data: &[u8]) -> bool {
        self.event_type == event_type && self.data == data
    }
}

pub fn is_watch_events_unique_violation(err: &rusqlite::Error) -> bool {
    if let rusqlite::Error::SqliteFailure(sql_err, msg) = err
        && sql_err.code == rusqlite::ErrorCode::ConstraintViolation
        && let Some(m) = msg.as_deref()
    {
        return m.contains("idx_watch_events_identity_rv")
            || (m.contains("watch_events.api_version")
                && m.contains("watch_events.kind")
                && m.contains("watch_events.name")
                && m.contains("watch_events.resource_version"));
    }
    false
}

pub fn serde_to_sqlite_error(error: serde_json::Error) -> tokio_rusqlite::Error {
    tokio_rusqlite::Error::Rusqlite(rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
}

pub fn advance_metadata_rv_to_at_least(
    conn: &rusqlite::Connection,
    resource_version: i64,
) -> rusqlite::Result<()> {
    let current_rv: i64 = conn.query_row(queries::METADATA_SELECT_RV_INT, [], |row| row.get(0))?;
    if current_rv < resource_version {
        conn.execute(
            queries::METADATA_SET_RV,
            rusqlite::params![resource_version.to_string()],
        )?;
    }
    Ok(())
}

/// Structural equality on two resource bodies that ignores
/// `metadata.resourceVersion`. Used as the dedupe gate in update_resource;
/// avoids cloning multi-KB Pod/CRD payloads just to strip a single field
/// before comparison.
pub fn resource_data_equal_ignoring_rv(a: &Value, b: &Value) -> bool {
    let (Some(am), Some(bm)) = (a.as_object(), b.as_object()) else {
        return a == b;
    };
    if am.len() != bm.len() {
        return false;
    }
    for (key, av) in am {
        let Some(bv) = bm.get(key) else {
            return false;
        };
        if key == "metadata" {
            if !metadata_equal_ignoring_rv(av, bv) {
                return false;
            }
        } else if av != bv {
            return false;
        }
    }
    true
}

pub fn metadata_equal_ignoring_rv(a: &Value, b: &Value) -> bool {
    let (Some(am), Some(bm)) = (a.as_object(), b.as_object()) else {
        return a == b;
    };
    let a_count = am
        .iter()
        .filter(|(k, _)| k.as_str() != "resourceVersion")
        .count();
    let b_count = bm
        .iter()
        .filter(|(k, _)| k.as_str() != "resourceVersion")
        .count();
    if a_count != b_count {
        return false;
    }
    for (k, av) in am {
        if k == "resourceVersion" {
            continue;
        }
        match bm.get(k) {
            Some(bv) if av == bv => continue,
            _ => return false,
        }
    }
    true
}

pub fn row_to_namespaced_resource(row: &rusqlite::Row<'_>) -> rusqlite::Result<Resource> {
    let data_bytes: Vec<u8> = row.get(7)?;
    let data: Value = serde_json::from_slice(&data_bytes)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    Ok(Resource {
        id: row.get(0)?,
        api_version: row.get(1)?,
        kind: row.get(2)?,
        namespace: Some(row.get(3)?),
        name: row.get(4)?,
        resource_version: row.get(5)?,
        uid: row.get(6)?,
        data: std::sync::Arc::new(data),
    })
}

pub fn row_to_cluster_resource(row: &rusqlite::Row<'_>) -> rusqlite::Result<Resource> {
    let data_bytes: Vec<u8> = row.get(6)?;
    let data: Value = serde_json::from_slice(&data_bytes)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    Ok(Resource {
        id: row.get(0)?,
        api_version: row.get(1)?,
        kind: row.get(2)?,
        namespace: None,
        name: row.get(3)?,
        resource_version: row.get(4)?,
        uid: row.get(5)?,
        data: std::sync::Arc::new(data),
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn event_read_api_versions_expands_for_v1_event() {
        let v = event_read_api_versions("v1", "Event");
        assert_eq!(v, vec!["v1", "events.k8s.io/v1"]);
    }

    #[test]
    fn event_api_versions_empty_for_pod() {
        assert!(event_read_api_versions("v1", "Pod").is_empty());
    }

    #[test]
    fn needs_event_v1_compat_true_for_event() {
        assert!(needs_event_v1_compat("v1", "Event"));
    }

    #[test]
    fn needs_event_v1_compat_false_for_pod() {
        assert!(!needs_event_v1_compat("v1", "Pod"));
    }

    #[test]
    fn resource_data_equal_ignoring_rv_ignores_rv_field() {
        let a =
            json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"x","resourceVersion":"1"}});
        let b =
            json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"x","resourceVersion":"2"}});
        assert!(resource_data_equal_ignoring_rv(&a, &b));
    }

    #[test]
    fn resource_data_equal_detects_name_diff() {
        let a = json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"x"}});
        let b = json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"y"}});
        assert!(!resource_data_equal_ignoring_rv(&a, &b));
    }

    #[test]
    fn metadata_equal_ignoring_rv_ignores_only_rv() {
        let a = json!({"name":"x","resourceVersion":"1","uid":"u"});
        let b = json!({"name":"x","resourceVersion":"2","uid":"u"});
        assert!(metadata_equal_ignoring_rv(&a, &b));
    }

    #[test]
    fn metadata_equal_detects_uid_diff() {
        let a = json!({"name":"x","resourceVersion":"1","uid":"u1"});
        let b = json!({"name":"x","resourceVersion":"2","uid":"u2"});
        assert!(!metadata_equal_ignoring_rv(&a, &b));
    }

    #[test]
    fn existing_watch_event_matches() {
        let ev = ExistingWatchEvent {
            event_type: "ADDED".into(),
            data: b"x".to_vec(),
        };
        assert!(ev.matches("ADDED", b"x"));
    }

    #[test]
    fn existing_watch_event_rejects_type_mismatch() {
        let ev = ExistingWatchEvent {
            event_type: "ADDED".into(),
            data: b"x".to_vec(),
        };
        assert!(!ev.matches("MODIFIED", b"x"));
    }
}
