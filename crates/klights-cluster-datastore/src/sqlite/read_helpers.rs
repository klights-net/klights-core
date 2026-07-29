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
