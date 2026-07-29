//! Status-only SQLite mutation primitive owned by the 10C.2 live packet.

use super::{mutation_helpers, owner_ref_index, queries, transaction_primitives};
use klights_cluster_core::ResourcePreconditions;
use klights_cluster_datastore::sqlite::selector_index;
use serde_json::Value;

pub(crate) struct StatusUpdate {
    pub id: i64,
    pub resource_version: i64,
    pub data: Vec<u8>,
    pub changed: bool,
}

pub(crate) fn update_status_in_conn(
    conn: &rusqlite::Connection,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    status: Value,
    preconditions: ResourcePreconditions,
) -> tokio_rusqlite::Result<StatusUpdate> {
    let expected_rv = preconditions.resource_version;
    let expected_uid = preconditions.uid;
    let (id, current_rv, live_uid, current_bytes): (i64, i64, String, Vec<u8>) =
        if let Some(namespace) = namespace {
            conn.query_row(
                queries::NAMESPACED_SELECT_STATUS_ROW,
                rusqlite::params![api_version, kind, namespace, name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?
        } else {
            conn.query_row(
                queries::CLUSTER_SELECT_STATUS_ROW,
                rusqlite::params![api_version, kind, name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?
        };
    if expected_rv.is_some_and(|expected| expected != current_rv)
        || expected_uid
            .as_deref()
            .is_some_and(|expected| expected != live_uid)
    {
        return Err(tokio_rusqlite::Error::Rusqlite(
            rusqlite::Error::QueryReturnedNoRows,
        ));
    }

    let mut current: Value =
        serde_json::from_slice(&current_bytes).map_err(mutation_helpers::serde_to_sqlite_error)?;
    if current.get("status") == Some(&status) {
        tracing::info!(
            target: "klights::datastore::noop_update",
            operation = "update_status_only",
            api_version,
            kind,
            namespace = namespace.unwrap_or(""),
            name,
            uid = %live_uid,
            resource_version = current_rv,
            reason = "status unchanged",
            "skipped no-op datastore write"
        );
        return Ok(StatusUpdate {
            id,
            resource_version: current_rv,
            data: current_bytes,
            changed: false,
        });
    }
    if let Some(object) = current.as_object_mut() {
        object.insert("status".to_string(), status);
    } else {
        current = serde_json::json!({ "status": status });
    }
    let merged = serde_json::to_vec(&current).map_err(mutation_helpers::serde_to_sqlite_error)?;
    let new_rv = transaction_primitives::next_resource_version_in_tx(conn)?;
    let rows = if let Some(_namespace) = namespace {
        conn.execute(
            queries::NAMESPACED_UPDATE_STATUS_BY_ID,
            rusqlite::params![new_rv, &merged, id, current_rv, &live_uid],
        )?
    } else {
        conn.execute(
            queries::CLUSTER_UPDATE_STATUS_BY_ID,
            rusqlite::params![new_rv, &merged, id, current_rv, &live_uid],
        )?
    };
    if rows == 0 {
        return Err(tokio_rusqlite::Error::Rusqlite(
            rusqlite::Error::QueryReturnedNoRows,
        ));
    }
    let namespace_key = namespace.unwrap_or("");
    selector_index::upsert_index_entries(conn, api_version, kind, namespace_key, name, &merged)?;
    owner_ref_index::upsert_owner_refs(conn, api_version, kind, namespace_key, name, &merged)?;
    mutation_helpers::insert_watch_event_in_conn(
        conn,
        mutation_helpers::WatchEventInsert::new(
            api_version,
            kind,
            namespace,
            name,
            new_rv,
            "MODIFIED",
            &merged,
        ),
    )?;
    Ok(StatusUpdate {
        id,
        resource_version: new_rv,
        data: merged,
        changed: true,
    })
}
