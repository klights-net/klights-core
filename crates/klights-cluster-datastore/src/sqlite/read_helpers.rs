//! SQLite row decoding and Kubernetes read-compatibility helpers.

use klights_cluster_core::Resource;
use serde_json::Value;

pub fn event_read_api_versions(api_version: &str, kind: &str) -> Vec<&'static str> {
    if kind == "Event" && (api_version == "v1" || api_version == "events.k8s.io/v1") {
        vec!["v1", "events.k8s.io/v1"]
    } else {
        Vec::new()
    }
}

pub fn needs_event_v1_compat(api_version: &str, kind: &str) -> bool {
    !event_read_api_versions(api_version, kind).is_empty()
}

pub fn row_to_namespaced_resource(row: &rusqlite::Row<'_>) -> rusqlite::Result<Resource> {
    let data_bytes: Vec<u8> = row.get(7)?;
    let data: Value = serde_json::from_slice(&data_bytes)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
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
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
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
    use super::{event_read_api_versions, needs_event_v1_compat};

    #[test]
    fn event_read_api_versions_expands_for_core_v1_event() {
        let v = event_read_api_versions("v1", "Event");
        assert_eq!(v, vec!["v1", "events.k8s.io/v1"]);
        assert!(needs_event_v1_compat("v1", "Event"));
    }

    #[test]
    fn event_read_api_versions_expands_for_events_k8s_io_v1_event() {
        let v = event_read_api_versions("events.k8s.io/v1", "Event");
        assert_eq!(v, vec!["v1", "events.k8s.io/v1"]);
        assert!(needs_event_v1_compat("events.k8s.io/v1", "Event"));
    }

    #[test]
    fn event_read_api_versions_does_not_expand_for_non_event_resource() {
        assert!(event_read_api_versions("v1", "Pod").is_empty());
        assert!(event_read_api_versions("v1", "ConfigMap").is_empty());
        assert!(event_read_api_versions("apps/v1", "Deployment").is_empty());
        assert!(!needs_event_v1_compat("v1", "Pod"));
    }

    #[test]
    fn event_read_api_versions_does_not_expand_for_event_in_unrelated_group() {
        // A custom kind that happens to be named "Event" but lives outside the
        // K8s Events compat envelope (e.g., a CRD called Event in some.group/v1)
        // must not pick up the cross-version compat behavior — that would
        // randomly merge unrelated rows.
        assert!(event_read_api_versions("some.group/v1", "Event").is_empty());
        assert!(!needs_event_v1_compat("some.group/v1", "Event"));
    }
}
