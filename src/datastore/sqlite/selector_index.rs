//! Selector index maintenance — pre-extracted label/field rows for fast
//! selector+limit queries without JSON-decoding every resource blob.
//!
//! Two index tables back this module:
//! - `resource_labels(api_version, kind, namespace, name, key, value)` — one
//!   row per label key-value pair per resource.
//! - `resource_fields(api_version, kind, namespace, name, field, value)` —
//!   one row per indexed field selector value per resource.
//!
//! Write paths call `upsert_index_entries` / `delete_index_entries` inside
//! their existing `db_call` closure so the index stays transactionally
//! consistent with the main resource table.

use serde_json::Value;

use super::filters::resolve_field_path;
use super::queries;

/// When residual selectors (inequality, notin, unindexed fields) are present,
/// fetch `min(limit * FACTOR, MAX)` candidate rows from SQL, then apply
/// residual filters in Rust. This bounds the candidate scan to prevent
/// monopolizing the single SQLite worker.
pub const SELECTOR_RESIDUAL_SCAN_FACTOR: usize = 64;
pub const SELECTOR_RESIDUAL_MAX_CANDIDATES: usize = 4096;

/// Indexed field paths per (api_version, kind).
///
/// Only paths that appear in Kubernetes field selectors are indexed.
/// `metadata.name` and `metadata.namespace` are already pushed to SQL via
/// `split_sql_pushdown_conditions` and do not need index table entries.
pub(super) fn indexed_field_paths(api_version: &str, kind: &str) -> &'static [&'static str] {
    match (api_version, kind) {
        ("v1", "Pod") => &[
            "spec.nodeName",
            "status.phase",
            "spec.restartPolicy",
            "spec.schedulerName",
            "spec.serviceAccountName",
            "status.podIP",
        ],
        ("v1", "Node") => &["spec.unschedulable"],
        ("v1", "PersistentVolume") => &["status.phase"],
        ("v1", "PersistentVolumeClaim") => &["status.phase"],
        ("v1", "Secret") => &["type"],
        ("v1", "Event") | ("events.k8s.io/v1", "Event") => &[
            "reason",
            "type",
            "source",
            "involvedObject.kind",
            "involvedObject.uid",
            "involvedObject.name",
            "involvedObject.namespace",
        ],
        _ => &[],
    }
}

/// Default value for an indexed field path when the resource JSON omits it.
///
/// Some Kubernetes fields are `omitempty` booleans whose schema default is
/// `false` — e.g. `Node.spec.unschedulable`. The kubelet may write the
/// field explicitly on registration, but a subsequent merge-patch round-
/// trip (such as a label patch) can re-serialize the resource and drop the
/// default. Without indexing the default, the pushdown EXISTS clause for
/// `?fieldSelector=spec.unschedulable=false` would skip nodes that
/// genuinely satisfy the predicate, breaking the upstream conformance
/// helper `GetReadySchedulableNodes` (it lists with that exact selector)
/// and cascading every multi-node scheduling test to fail with "no ready,
/// schedulable nodes in the cluster".
///
/// Only fields whose absence is semantically equivalent to a single,
/// well-defined Kubernetes API default belong here. Do NOT add fields
/// like `spec.nodeName` (absent = unscheduled, no schema default):
/// defaulting them would make `spec.nodeName=foo` match every unscheduled
/// pod.
pub(super) fn indexed_field_default(
    api_version: &str,
    kind: &str,
    path: &str,
) -> Option<&'static str> {
    match (api_version, kind, path) {
        ("v1", "Node", "spec.unschedulable") => Some("false"),
        _ => None,
    }
}

/// Extract all label key-value pairs from `metadata.labels`.
fn extract_labels(data: &Value) -> Vec<(String, String)> {
    let labels = data
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .and_then(|l| l.as_object());
    match labels {
        Some(map) => map
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect(),
        None => Vec::new(),
    }
}

/// Delete all index rows (labels + fields) for one resource.
pub(super) fn delete_index_entries(
    conn: &rusqlite::Connection,
    api_version: &str,
    kind: &str,
    namespace: &str,
    name: &str,
) -> rusqlite::Result<()> {
    conn.execute(
        queries::LABEL_INDEX_DELETE_FOR_RESOURCE,
        rusqlite::params![api_version, kind, namespace, name],
    )?;
    conn.execute(
        queries::FIELD_INDEX_DELETE_FOR_RESOURCE,
        rusqlite::params![api_version, kind, namespace, name],
    )?;
    Ok(())
}

/// Delete + re-insert all index rows for a resource from its JSON bytes.
///
/// Accepts the serialized `data_bytes` to avoid requiring the caller to
/// deserialize the JSON — the cost of one extra deserialization per write
/// is negligible compared to the write itself.
pub(super) fn upsert_index_entries(
    conn: &rusqlite::Connection,
    api_version: &str,
    kind: &str,
    namespace: &str,
    name: &str,
    data_bytes: &[u8],
) -> rusqlite::Result<()> {
    let data: Value = serde_json::from_slice(data_bytes)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    delete_index_entries(conn, api_version, kind, namespace, name)?;
    insert_index_entries(conn, api_version, kind, namespace, name, &data)
}

/// Insert index rows for one resource from its deserialized JSON body.
fn insert_index_entries(
    conn: &rusqlite::Connection,
    api_version: &str,
    kind: &str,
    namespace: &str,
    name: &str,
    data: &Value,
) -> rusqlite::Result<()> {
    // Labels
    let labels = extract_labels(data);
    if !labels.is_empty() {
        let mut stmt = conn.prepare(queries::LABEL_INDEX_INSERT)?;
        for (key, value) in &labels {
            stmt.execute(rusqlite::params![
                api_version,
                kind,
                namespace,
                name,
                key,
                value
            ])?;
        }
    }

    // Fields
    let fields = indexed_field_paths(api_version, kind);
    if !fields.is_empty() {
        let mut stmt = conn.prepare(queries::FIELD_INDEX_INSERT)?;
        for field_path in fields {
            let resolved = resolve_field_path(data, field_path);
            let value: Option<std::borrow::Cow<'_, str>> = match resolved {
                Some(v) => Some(v),
                None => indexed_field_default(api_version, kind, field_path)
                    .map(std::borrow::Cow::Borrowed),
            };
            if let Some(value) = value {
                stmt.execute(rusqlite::params![
                    api_version,
                    kind,
                    namespace,
                    name,
                    field_path,
                    value.as_ref()
                ])?;
            }
        }
    }
    Ok(())
}

/// Resolved pushdown plan for selector queries: SQL EXISTS/NOT EXISTS fragments
/// from the index tables, plus residual requirements that must be evaluated in Rust.
pub(super) struct SelectorPushdown {
    /// SQL WHERE clause fragments (each is a complete AND-able condition).
    pub sql_clauses: Vec<String>,
    /// SQL parameter values, one Vec per clause (flattened into a single params vec at bind time).
    pub sql_params: Vec<String>,
    /// Label requirements that cannot be pushed to SQL (Inequality, NotIn multi-value).
    pub residual_labels: Vec<crate::label_selector::LabelRequirement>,
    /// Field conditions that cannot be pushed to SQL (not in indexed_field_paths).
    pub residual_fields: Vec<super::filters::FieldSelectorCondition>,
}

/// Build a pushdown plan from parsed label requirements and field conditions.
///
/// `param_offset` is the number of SQL parameters already in the base query,
/// so the generated `?N` placeholders start at `param_offset + 1`.
///
/// `cluster_scoped` controls the namespace join: namespaced queries use
/// `rl.namespace = r.namespace`, cluster-scoped use `rl.namespace = ''`.
///
/// Pushable label variants: Equality, Inequality, Exists, NotExists, In,
/// NotIn (all fully pushed via EXISTS/NOT EXISTS).
/// Field conditions are pushed if their path is in `indexed_field_paths`.
pub(super) fn build_selector_pushdown(
    label_requirements: &[crate::label_selector::LabelRequirement],
    field_conditions: &[super::filters::FieldSelectorCondition],
    api_version: &str,
    kind: &str,
    param_offset: usize,
    cluster_scoped: bool,
) -> SelectorPushdown {
    use crate::label_selector::LabelRequirement;

    let ns_join = if cluster_scoped {
        "rl.namespace = ''"
    } else {
        "rl.namespace = r.namespace"
    };
    let ns_join_rf = if cluster_scoped {
        "rf.namespace = ''"
    } else {
        "rf.namespace = r.namespace"
    };

    let mut sql_clauses = Vec::new();
    let mut sql_params = Vec::new();
    let residual_labels = Vec::new();
    let mut residual_fields = Vec::new();

    let indexed_fields: Vec<&str> = indexed_field_paths(api_version, kind).to_vec();

    for req in label_requirements {
        match req {
            LabelRequirement::Equality { key, value } => {
                let p1 = param_offset + sql_params.len() + 1;
                let p2 = p1 + 1;
                sql_clauses.push(format!(
                    "EXISTS (SELECT 1 FROM resource_labels rl WHERE rl.api_version = r.api_version AND rl.kind = r.kind AND {ns_join} AND rl.name = r.name AND rl.key = ?{p1} AND rl.value = ?{p2})"
                ));
                sql_params.push(key.clone());
                sql_params.push(value.clone());
            }
            LabelRequirement::Exists { key } => {
                let p = param_offset + sql_params.len() + 1;
                sql_clauses.push(format!(
                    "EXISTS (SELECT 1 FROM resource_labels rl WHERE rl.api_version = r.api_version AND rl.kind = r.kind AND {ns_join} AND rl.name = r.name AND rl.key = ?{p})"
                ));
                sql_params.push(key.clone());
            }
            LabelRequirement::NotExists { key } => {
                let p = param_offset + sql_params.len() + 1;
                sql_clauses.push(format!(
                    "NOT EXISTS (SELECT 1 FROM resource_labels rl WHERE rl.api_version = r.api_version AND rl.kind = r.kind AND {ns_join} AND rl.name = r.name AND rl.key = ?{p})"
                ));
                sql_params.push(key.clone());
            }
            LabelRequirement::In { key, values } => {
                let kp = param_offset + sql_params.len() + 1;
                let placeholders: Vec<String> = (0..values.len())
                    .map(|i| format!("?{}", kp + 1 + i))
                    .collect();
                sql_clauses.push(format!(
                    "EXISTS (SELECT 1 FROM resource_labels rl WHERE rl.api_version = r.api_version AND rl.kind = r.kind AND {ns_join} AND rl.name = r.name AND rl.key = ?{kp} AND rl.value IN ({vp}))",
                    vp = placeholders.join(", "),
                ));
                sql_params.push(key.clone());
                for v in values {
                    sql_params.push(v.clone());
                }
            }
            // Inequality: key != value. K8s semantics: matches when label is
            // absent OR label value != specified. NOT EXISTS correctly handles
            // the "label absent" case — it returns true when no row matches the
            // key+value pair.
            LabelRequirement::Inequality { key, value } => {
                let p1 = param_offset + sql_params.len() + 1;
                let p2 = p1 + 1;
                sql_clauses.push(format!(
                    "NOT EXISTS (SELECT 1 FROM resource_labels rl WHERE rl.api_version = r.api_version AND rl.kind = r.kind AND {ns_join} AND rl.name = r.name AND rl.key = ?{p1} AND rl.value = ?{p2})"
                ));
                sql_params.push(key.clone());
                sql_params.push(value.clone());
            }
            // NotIn: key notin (values...). Same as Inequality but with a value
            // set. NOT EXISTS handles the "label absent" case.
            LabelRequirement::NotIn { key, values } => {
                let kp = param_offset + sql_params.len() + 1;
                let placeholders: Vec<String> = (0..values.len())
                    .map(|i| format!("?{}", kp + 1 + i))
                    .collect();
                sql_clauses.push(format!(
                    "NOT EXISTS (SELECT 1 FROM resource_labels rl WHERE rl.api_version = r.api_version AND rl.kind = r.kind AND {ns_join} AND rl.name = r.name AND rl.key = ?{kp} AND rl.value IN ({vp}))",
                    vp = placeholders.join(", "),
                ));
                sql_params.push(key.clone());
                for v in values {
                    sql_params.push(v.clone());
                }
            }
        }
    }

    for cond in field_conditions {
        let (path, expected, is_eq) = cond;
        if indexed_fields.contains(&path.as_str()) {
            let p1 = param_offset + sql_params.len() + 1;
            let p2 = p1 + 1;
            if *is_eq {
                sql_clauses.push(format!(
                    "EXISTS (SELECT 1 FROM resource_fields rf WHERE rf.api_version = r.api_version AND rf.kind = r.kind AND {ns_join_rf} AND rf.name = r.name AND rf.field = ?{p1} AND rf.value = ?{p2})"
                ));
            } else {
                sql_clauses.push(format!(
                    "NOT EXISTS (SELECT 1 FROM resource_fields rf WHERE rf.api_version = r.api_version AND rf.kind = r.kind AND {ns_join_rf} AND rf.name = r.name AND rf.field = ?{p1} AND rf.value = ?{p2})"
                ));
            }
            sql_params.push(path.clone());
            sql_params.push(expected.clone());
        } else {
            residual_fields.push(cond.clone());
        }
    }

    SelectorPushdown {
        sql_clauses,
        sql_params,
        residual_labels,
        residual_fields,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_labels_returns_pairs() {
        let data = json!({"metadata": {"labels": {"app": "nginx", "tier": "fe"}}});
        let mut labels = extract_labels(&data);
        labels.sort();
        assert_eq!(
            labels,
            vec![
                ("app".to_string(), "nginx".to_string()),
                ("tier".to_string(), "fe".to_string())
            ]
        );
    }

    #[test]
    fn extract_labels_empty_when_no_labels() {
        let data = json!({"metadata": {}});
        assert!(extract_labels(&data).is_empty());
    }

    #[test]
    fn extract_labels_empty_when_no_metadata() {
        let data = json!({});
        assert!(extract_labels(&data).is_empty());
    }

    #[test]
    fn indexed_field_paths_pod() {
        let paths = indexed_field_paths("v1", "Pod");
        assert!(paths.contains(&"spec.nodeName"));
        assert!(paths.contains(&"status.phase"));
    }

    #[test]
    fn indexed_field_paths_node() {
        let paths = indexed_field_paths("v1", "Node");
        assert!(paths.contains(&"spec.unschedulable"));
    }

    #[test]
    fn indexed_field_paths_unknown_kind_empty() {
        assert!(indexed_field_paths("v1", "ConfigMap").is_empty());
    }

    #[test]
    fn indexed_field_paths_event_cross_group() {
        assert!(!indexed_field_paths("events.k8s.io/v1", "Event").is_empty());
    }

    #[test]
    fn indexed_field_default_node_unschedulable_is_false() {
        // Schema default for Node.spec.unschedulable is false. The pushdown
        // index must materialize that default so `?fieldSelector=...=false`
        // matches Nodes whose stored JSON omits the field.
        assert_eq!(
            indexed_field_default("v1", "Node", "spec.unschedulable"),
            Some("false")
        );
    }

    #[test]
    fn indexed_field_default_returns_none_for_fields_without_safe_default() {
        // Defaulting these would silently broaden matches against the
        // wrong rows (e.g. every unscheduled pod matching `spec.nodeName=x`).
        assert_eq!(indexed_field_default("v1", "Pod", "spec.nodeName"), None);
        assert_eq!(indexed_field_default("v1", "Pod", "status.phase"), None);
        assert_eq!(
            indexed_field_default("v1", "PersistentVolume", "status.phase"),
            None
        );
        assert_eq!(indexed_field_default("v1", "Node", "metadata.name"), None);
    }

    #[test]
    fn insert_index_entries_materializes_default_for_absent_unschedulable() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        super::super::schema::init_schema_in_conn(&mut conn).unwrap();
        let data = json!({"metadata": {"name": "n"}, "spec": {}});
        let bytes = serde_json::to_vec(&data).unwrap();
        upsert_index_entries(&conn, "v1", "Node", "", "n", &bytes).unwrap();
        let value: String = conn
            .query_row(
                "SELECT value FROM resource_fields WHERE api_version='v1' AND kind='Node' AND namespace='' AND name='n' AND field='spec.unschedulable'",
                [],
                |row| row.get(0),
            )
            .expect("default row must be inserted when JSON omits spec.unschedulable");
        assert_eq!(value, "false");
    }

    #[test]
    fn upsert_and_delete_round_trip() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        super::super::schema::init_schema_in_conn(&mut conn).unwrap();

        let data = json!({
            "metadata": {"labels": {"app": "test"}},
            "spec": {"nodeName": "worker-1"},
            "status": {"phase": "Running"}
        });
        let data_bytes = serde_json::to_vec(&data).unwrap();

        upsert_index_entries(&conn, "v1", "Pod", "default", "pod-1", &data_bytes).unwrap();

        // Verify label row
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_labels WHERE api_version='v1' AND kind='Pod' AND namespace='default' AND name='pod-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        // Verify field row for spec.nodeName
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_fields WHERE api_version='v1' AND kind='Pod' AND namespace='default' AND name='pod-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2); // spec.nodeName, status.phase (restartPolicy/schedulerName absent from data)

        // Delete
        delete_index_entries(&conn, "v1", "Pod", "default", "pod-1").unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resource_labels", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resource_fields", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn upsert_replaces_existing_entries() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        super::super::schema::init_schema_in_conn(&mut conn).unwrap();

        let data_v1 = json!({"metadata": {"labels": {"app": "v1"}}});
        let data_v2 = json!({"metadata": {"labels": {"app": "v2", "env": "prod"}}});

        upsert_index_entries(
            &conn,
            "v1",
            "Pod",
            "default",
            "pod-1",
            &serde_json::to_vec(&data_v1).unwrap(),
        )
        .unwrap();
        upsert_index_entries(
            &conn,
            "v1",
            "Pod",
            "default",
            "pod-1",
            &serde_json::to_vec(&data_v2).unwrap(),
        )
        .unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM resource_labels WHERE api_version='v1' AND kind='Pod' AND namespace='default' AND name='pod-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2); // app=v2, env=prod — no stale v1 entry
    }

    #[test]
    fn cluster_scoped_uses_empty_namespace() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        super::super::schema::init_schema_in_conn(&mut conn).unwrap();

        let data = json!({"metadata": {"labels": {"role": "control-plane"}}, "spec": {"unschedulable": true}});
        upsert_index_entries(
            &conn,
            "v1",
            "Node",
            "",
            "node-1",
            &serde_json::to_vec(&data).unwrap(),
        )
        .unwrap();

        let val: String = conn
            .query_row(
                "SELECT value FROM resource_fields WHERE api_version='v1' AND kind='Node' AND namespace='' AND name='node-1' AND field='spec.unschedulable'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(val, "true");
    }

    // --- build_selector_pushdown unit tests ---

    use crate::label_selector::LabelRequirement;

    #[test]
    fn pushdown_equality_generates_exists_with_two_params() {
        let pd = build_selector_pushdown(
            &[LabelRequirement::Equality {
                key: "app".into(),
                value: "nginx".into(),
            }],
            &[],
            "v1",
            "Pod",
            3,
            false,
        );
        assert_eq!(pd.sql_clauses.len(), 1);
        assert!(pd.sql_clauses[0].contains("EXISTS"));
        assert!(pd.sql_clauses[0].contains("resource_labels"));
        assert_eq!(pd.sql_params.len(), 2);
        assert_eq!(pd.sql_params[0], "app");
        assert_eq!(pd.sql_params[1], "nginx");
        assert!(pd.residual_labels.is_empty());
        // Params should start at offset+1 = 4
        assert!(pd.sql_clauses[0].contains("?4"));
        assert!(pd.sql_clauses[0].contains("?5"));
    }

    #[test]
    fn pushdown_exists_generates_key_only_exists() {
        let pd = build_selector_pushdown(
            &[LabelRequirement::Exists { key: "app".into() }],
            &[],
            "v1",
            "Pod",
            0,
            false,
        );
        assert_eq!(pd.sql_clauses.len(), 1);
        assert!(pd.sql_clauses[0].contains("EXISTS"));
        assert!(pd.sql_clauses[0].contains("rl.key = ?1"));
        assert_eq!(pd.sql_params, vec!["app"]);
    }

    #[test]
    fn pushdown_not_exists_generates_not_exists() {
        let pd = build_selector_pushdown(
            &[LabelRequirement::NotExists { key: "app".into() }],
            &[],
            "v1",
            "Pod",
            0,
            false,
        );
        assert_eq!(pd.sql_clauses.len(), 1);
        assert!(pd.sql_clauses[0].contains("NOT EXISTS"));
        assert_eq!(pd.sql_params, vec!["app"]);
    }

    #[test]
    fn pushdown_in_multi_generates_in_clause() {
        let pd = build_selector_pushdown(
            &[LabelRequirement::In {
                key: "tier".into(),
                values: vec!["fe".into(), "be".into()],
            }],
            &[],
            "v1",
            "Pod",
            0,
            false,
        );
        assert_eq!(pd.sql_clauses.len(), 1);
        assert!(pd.sql_clauses[0].contains("IN"));
        assert!(pd.sql_clauses[0].contains("?1"));
        assert!(pd.sql_clauses[0].contains("?2"));
        assert!(pd.sql_clauses[0].contains("?3"));
        assert_eq!(pd.sql_params, vec!["tier", "fe", "be"]);
    }

    #[test]
    fn pushdown_inequality_generates_not_exists() {
        let pd = build_selector_pushdown(
            &[LabelRequirement::Inequality {
                key: "app".into(),
                value: "nginx".into(),
            }],
            &[],
            "v1",
            "Pod",
            0,
            false,
        );
        assert_eq!(pd.sql_clauses.len(), 1);
        assert!(pd.sql_clauses[0].contains("NOT EXISTS"));
        assert!(pd.sql_clauses[0].contains("rl.key = ?1"));
        assert!(pd.sql_clauses[0].contains("rl.value = ?2"));
        assert_eq!(pd.sql_params, vec!["app", "nginx"]);
        assert!(pd.residual_labels.is_empty());
    }

    #[test]
    fn pushdown_notin_generates_not_exists_with_in_clause() {
        let pd = build_selector_pushdown(
            &[LabelRequirement::NotIn {
                key: "app".into(),
                values: vec!["a".into(), "b".into()],
            }],
            &[],
            "v1",
            "Pod",
            0,
            false,
        );
        assert_eq!(pd.sql_clauses.len(), 1);
        assert!(pd.sql_clauses[0].contains("NOT EXISTS"));
        assert!(pd.sql_clauses[0].contains("IN"));
        assert_eq!(pd.sql_params, vec!["app", "a", "b"]);
        assert!(pd.residual_labels.is_empty());
    }

    #[test]
    fn pushdown_field_equality_for_indexed_path() {
        let pd = build_selector_pushdown(
            &[],
            &[("spec.nodeName".into(), "node-1".into(), true)],
            "v1",
            "Pod",
            0,
            false,
        );
        assert_eq!(pd.sql_clauses.len(), 1);
        assert!(pd.sql_clauses[0].contains("resource_fields"));
        assert!(pd.sql_clauses[0].contains("EXISTS"));
        assert_eq!(pd.sql_params, vec!["spec.nodeName", "node-1"]);
        assert!(pd.residual_fields.is_empty());
    }

    #[test]
    fn pushdown_field_inequality_for_indexed_path() {
        let pd = build_selector_pushdown(
            &[],
            &[("spec.nodeName".into(), "node-1".into(), false)],
            "v1",
            "Pod",
            0,
            false,
        );
        assert_eq!(pd.sql_clauses.len(), 1);
        assert!(pd.sql_clauses[0].contains("NOT EXISTS"));
        assert_eq!(pd.sql_params, vec!["spec.nodeName", "node-1"]);
    }

    #[test]
    fn pushdown_field_unindexed_is_residual() {
        let pd = build_selector_pushdown(
            &[],
            &[("spec.someOtherField".into(), "x".into(), true)],
            "v1",
            "Pod",
            0,
            false,
        );
        assert!(pd.sql_clauses.is_empty());
        assert_eq!(pd.residual_fields.len(), 1);
    }

    #[test]
    fn pushdown_combined_label_and_field() {
        let pd = build_selector_pushdown(
            &[LabelRequirement::Equality {
                key: "app".into(),
                value: "nginx".into(),
            }],
            &[("spec.nodeName".into(), "node-1".into(), true)],
            "v1",
            "Pod",
            5,
            false,
        );
        assert_eq!(pd.sql_clauses.len(), 2);
        assert_eq!(pd.sql_params.len(), 4);
        // Label params at ?6, ?7; field params at ?8, ?9
        assert!(pd.sql_clauses[0].contains("?6"));
        assert!(pd.sql_clauses[1].contains("?8"));
    }

    #[test]
    fn pushdown_no_requirements_returns_empty() {
        let pd = build_selector_pushdown(&[], &[], "v1", "Pod", 0, false);
        assert!(pd.sql_clauses.is_empty());
        assert!(pd.sql_params.is_empty());
        assert!(pd.residual_labels.is_empty());
        assert!(pd.residual_fields.is_empty());
    }

    #[test]
    fn pushdown_cluster_scoped_uses_empty_namespace() {
        let pd = build_selector_pushdown(
            &[LabelRequirement::Equality {
                key: "role".into(),
                value: "control-plane".into(),
            }],
            &[("spec.unschedulable".into(), "true".into(), true)],
            "v1",
            "Node",
            0,
            true,
        );
        assert_eq!(pd.sql_clauses.len(), 2);
        // Both clauses should use literal '' for namespace, not r.namespace
        assert!(pd.sql_clauses[0].contains("rl.namespace = ''"));
        assert!(pd.sql_clauses[1].contains("rf.namespace = ''"));
        assert!(!pd.sql_clauses[0].contains("r.namespace"));
        assert!(!pd.sql_clauses[1].contains("r.namespace"));
    }

    #[test]
    fn pushdown_namespaced_uses_r_namespace() {
        let pd = build_selector_pushdown(
            &[LabelRequirement::Equality {
                key: "app".into(),
                value: "nginx".into(),
            }],
            &[],
            "v1",
            "Pod",
            0,
            false,
        );
        assert!(pd.sql_clauses[0].contains("rl.namespace = r.namespace"));
    }
}
