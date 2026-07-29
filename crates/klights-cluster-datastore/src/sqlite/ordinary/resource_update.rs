use super::{
    mutation_helpers, owner_ref_index, queries, resource_shape, selector_index,
    transaction_primitives, use_namespaced_table,
};
use rusqlite::TransactionBehavior;
use serde_json::Value;

pub struct MarkResourceForDeletionInput {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub expected_resource_version: Option<i64>,
    pub expected_uid: Option<String>,
    pub grace_seconds: i64,
    pub deletion_timestamp: String,
}

pub fn mark_resource_for_deletion_in_conn(
    conn: &mut rusqlite::Connection,
    input: MarkResourceForDeletionInput,
) -> tokio_rusqlite::Result<Option<(i64, Vec<u8>)>> {
    let MarkResourceForDeletionInput {
        api_version,
        kind,
        namespace,
        name,
        expected_resource_version,
        expected_uid,
        grace_seconds,
        deletion_timestamp,
    } = input;
    let namespaced = use_namespaced_table(&api_version, &kind, &namespace.as_deref());
    let namespace = namespaced.then(|| namespace.unwrap_or_else(|| "default".to_string()));
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (_id, current_rv, current_uid, current_bytes): (i64, i64, String, Vec<u8>) =
        if let Some(namespace) = namespace.as_deref() {
            tx.query_row(
                queries::NAMESPACED_SELECT_STATUS_ROW,
                rusqlite::params![&api_version, &kind, namespace, &name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?
        } else {
            tx.query_row(
                queries::CLUSTER_SELECT_STATUS_ROW,
                rusqlite::params![&api_version, &kind, &name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?
        };
    if expected_uid
        .as_deref()
        .is_some_and(|uid| uid != current_uid)
    {
        return Err(tokio_rusqlite::Error::Rusqlite(
            rusqlite::Error::QueryReturnedNoRows,
        ));
    }
    if expected_resource_version.is_some_and(|rv| rv != current_rv) {
        return Err(tokio_rusqlite::Error::Rusqlite(
            rusqlite::Error::QueryReturnedNoRows,
        ));
    }

    let mut current: Value =
        serde_json::from_slice(&current_bytes).map_err(mutation_helpers::serde_to_sqlite_error)?;
    if has_deletion_timestamp(&current) {
        tx.commit()?;
        return Ok(Some((current_rv, current_bytes)));
    }
    ensure_deletion_timestamp(&mut current, grace_seconds, &deletion_timestamp);
    let merged = serde_json::to_vec(&current).map_err(mutation_helpers::serde_to_sqlite_error)?;
    let new_rv = transaction_primitives::next_resource_version_in_tx(&tx)?;
    let rows = if let Some(namespace) = namespace.as_deref() {
        tx.execute(
            queries::NAMESPACED_UPDATE_BY_RV,
            rusqlite::params![
                new_rv,
                &current_uid,
                &merged,
                &api_version,
                &kind,
                namespace,
                &name,
                expected_resource_version,
                expected_uid.as_deref(),
            ],
        )?
    } else {
        tx.execute(
            queries::CLUSTER_UPDATE_BY_RV,
            rusqlite::params![
                new_rv,
                &current_uid,
                &merged,
                &api_version,
                &kind,
                &name,
                expected_resource_version,
                expected_uid.as_deref(),
            ],
        )?
    };
    if rows == 0 {
        return Err(tokio_rusqlite::Error::Rusqlite(
            rusqlite::Error::QueryReturnedNoRows,
        ));
    }
    let namespace_key = namespace.as_deref().unwrap_or("");
    selector_index::upsert_index_entries(&tx, &api_version, &kind, namespace_key, &name, &merged)?;
    owner_ref_index::upsert_owner_refs(&tx, &api_version, &kind, namespace_key, &name, &merged)?;
    tx.commit()?;
    Ok(Some((new_rv, merged)))
}

fn ensure_deletion_timestamp(data: &mut Value, grace_seconds: i64, deletion_timestamp: &str) {
    let Some(meta) = data
        .get_mut("metadata")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    if meta
        .get("deletionTimestamp")
        .and_then(serde_json::Value::as_str)
        .is_none_or(str::is_empty)
    {
        meta.insert(
            "deletionTimestamp".to_string(),
            Value::String(deletion_timestamp.to_string()),
        );
    }
    meta.entry("deletionGracePeriodSeconds".to_string())
        .or_insert_with(|| Value::from(grace_seconds));
}

fn has_deletion_timestamp(data: &Value) -> bool {
    data.pointer("/metadata/deletionTimestamp")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| !value.is_empty())
}

pub struct UpdateResourceInput {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub uid: String,
    pub data: Value,
    pub expected_resource_version: Option<i64>,
    pub expected_uid: Option<String>,
    pub preserve_latest_status: bool,
}

pub fn update_resource_in_conn(
    conn: &mut rusqlite::Connection,
    input: UpdateResourceInput,
) -> tokio_rusqlite::Result<(i64, i64, Value)> {
    let UpdateResourceInput {
        api_version,
        kind,
        namespace,
        name,
        uid,
        mut data,
        expected_resource_version,
        expected_uid,
        preserve_latest_status,
    } = input;
    let namespaced = use_namespaced_table(&api_version, &kind, &namespace.as_deref());
    let namespace = namespaced.then(|| namespace.unwrap_or_else(|| "default".to_string()));
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if preserve_latest_status {
        preserve_latest_status_subresource(
            &tx,
            &api_version,
            &kind,
            namespace.as_deref(),
            &name,
            &mut data,
        )?;
    }
    let data_bytes = serde_json::to_vec(&data).map_err(mutation_helpers::serde_to_sqlite_error)?;
    let new_rv = transaction_primitives::next_resource_version_in_tx(&tx)?;
    let rows = if let Some(namespace) = namespace.as_deref() {
        tx.execute(
            queries::NAMESPACED_UPDATE_BY_RV,
            rusqlite::params![
                new_rv,
                &uid,
                &data_bytes,
                &api_version,
                &kind,
                namespace,
                &name,
                expected_resource_version,
                expected_uid.as_deref(),
            ],
        )?
    } else {
        tx.execute(
            queries::CLUSTER_UPDATE_BY_RV,
            rusqlite::params![
                new_rv,
                &uid,
                &data_bytes,
                &api_version,
                &kind,
                &name,
                expected_resource_version,
                expected_uid.as_deref(),
            ],
        )?
    };
    if rows == 0 {
        return Err(tokio_rusqlite::Error::Rusqlite(
            rusqlite::Error::QueryReturnedNoRows,
        ));
    }
    let id = if let Some(namespace) = namespace.as_deref() {
        tx.query_row(
            queries::NAMESPACED_SELECT_ID,
            rusqlite::params![&api_version, &kind, namespace, &name],
            |row| row.get(0),
        )?
    } else {
        tx.query_row(
            queries::CLUSTER_SELECT_ID,
            rusqlite::params![&api_version, &kind, &name],
            |row| row.get(0),
        )?
    };
    let namespace_key = namespace.as_deref().unwrap_or("");
    selector_index::upsert_index_entries(
        &tx,
        &api_version,
        &kind,
        namespace_key,
        &name,
        &data_bytes,
    )?;
    owner_ref_index::upsert_owner_refs(
        &tx,
        &api_version,
        &kind,
        namespace_key,
        &name,
        &data_bytes,
    )?;
    mutation_helpers::insert_watch_event_in_conn(
        &tx,
        mutation_helpers::WatchEventInsert::new(
            &api_version,
            &kind,
            namespace.as_deref(),
            &name,
            new_rv,
            "MODIFIED",
            &data_bytes,
        ),
    )?;
    tx.commit()?;
    Ok((id, new_rv, data))
}

fn preserve_latest_status_subresource(
    tx: &rusqlite::Transaction<'_>,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    proposed: &mut Value,
) -> tokio_rusqlite::Result<()> {
    if !klights_types::has_builtin_status_subresource(api_version, kind) {
        return Ok(());
    }
    let current_bytes: Vec<u8> = if let Some(namespace) = namespace {
        tx.query_row(
            queries::NAMESPACED_SELECT_STATUS_ROW,
            rusqlite::params![api_version, kind, namespace, name],
            |row| row.get(3),
        )?
    } else {
        tx.query_row(
            queries::CLUSTER_SELECT_STATUS_ROW,
            rusqlite::params![api_version, kind, name],
            |row| row.get(3),
        )?
    };
    let current: Value =
        serde_json::from_slice(&current_bytes).map_err(mutation_helpers::serde_to_sqlite_error)?;
    klights_types::preserve_status_subresource_on_main_update(
        api_version,
        kind,
        &current,
        proposed,
    );
    resource_shape::preserve_server_metadata_fields_from_existing(proposed, &current);
    Ok(())
}
