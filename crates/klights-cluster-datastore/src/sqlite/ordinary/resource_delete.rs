use super::{
    mutation_helpers, owner_ref_index, queries, resource_shape, selector_index,
    transaction_primitives, use_namespaced_table,
};
use klights_cluster_core::ResourcePreconditions;
use rusqlite::TransactionBehavior;

pub struct DeleteResourceInput {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub preconditions: ResourcePreconditions,
}

pub enum DeleteResourceAttempt {
    Deleted(i64, Vec<u8>),
    NotFound,
    PreconditionFailed {
        message: String,
        live_uid: Option<String>,
    },
}

pub fn delete_resource_in_conn(
    conn: &mut rusqlite::Connection,
    input: DeleteResourceInput,
) -> tokio_rusqlite::Result<DeleteResourceAttempt> {
    let DeleteResourceInput {
        api_version,
        kind,
        namespace,
        name,
        preconditions,
    } = input;
    let namespaced = use_namespaced_table(&api_version, &kind, &namespace.as_deref());
    let namespace = namespaced.then(|| namespace.unwrap_or_else(|| "default".to_string()));
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current = if let Some(namespace) = namespace.as_deref() {
        tx.query_row(
            queries::NAMESPACED_GET_DATA_FOR_DELETE,
            rusqlite::params![&api_version, &kind, namespace, &name],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
    } else {
        tx.query_row(
            queries::CLUSTER_GET_DATA_FOR_DELETE,
            rusqlite::params![&api_version, &kind, &name],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
    };
    let (current_rv, current_uid, data) = match current {
        Ok(current) => current,
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            return Ok(DeleteResourceAttempt::NotFound);
        }
        Err(error) => return Err(tokio_rusqlite::Error::Rusqlite(error)),
    };
    if let Err(error) = resource_shape::validate_resource_preconditions(
        &preconditions,
        Some(&current_uid),
        current_rv,
    ) {
        return Ok(DeleteResourceAttempt::PreconditionFailed {
            message: error.to_string(),
            live_uid: Some(current_uid),
        });
    }
    let rv = transaction_primitives::next_resource_version_in_tx(&tx)?;
    let rows = if let Some(namespace) = namespace.as_deref() {
        tx.execute(
            queries::NAMESPACED_DELETE,
            rusqlite::params![&api_version, &kind, namespace, &name, &current_uid],
        )?
    } else {
        tx.execute(
            queries::CLUSTER_DELETE,
            rusqlite::params![&api_version, &kind, &name, &current_uid],
        )?
    };
    if rows == 0 {
        return Ok(DeleteResourceAttempt::NotFound);
    }
    let namespace_key = namespace.as_deref().unwrap_or("");
    selector_index::delete_index_entries(&tx, &api_version, &kind, namespace_key, &name)?;
    owner_ref_index::delete_owner_refs(&tx, &api_version, &kind, namespace_key, &name)?;
    mutation_helpers::insert_watch_event_in_conn(
        &tx,
        mutation_helpers::WatchEventInsert::new(
            &api_version,
            &kind,
            namespace.as_deref(),
            &name,
            rv,
            "DELETED",
            &data,
        ),
    )?;
    tx.commit()?;
    Ok(DeleteResourceAttempt::Deleted(rv, data))
}
