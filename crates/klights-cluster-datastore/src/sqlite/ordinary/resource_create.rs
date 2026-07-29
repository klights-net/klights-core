use super::{
    mutation_helpers, owner_ref_index, queries, selector_index, transaction_primitives,
    use_namespaced_table,
};
use rusqlite::TransactionBehavior;

pub struct CreateResourceInput {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub uid: String,
    pub data: Vec<u8>,
}

pub fn create_resource_in_conn(
    conn: &mut rusqlite::Connection,
    input: CreateResourceInput,
) -> tokio_rusqlite::Result<(i64, i64)> {
    let CreateResourceInput {
        api_version,
        kind,
        namespace,
        name,
        uid,
        data,
    } = input;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let rv = transaction_primitives::next_resource_version_in_tx(&tx)?;
    if use_namespaced_table(&api_version, &kind, &namespace.as_deref()) {
        let namespace = namespace.unwrap_or_else(|| "default".to_string());
        tx.execute(
            queries::NAMESPACED_INSERT,
            rusqlite::params![&api_version, &kind, &namespace, &name, &uid, rv, &data],
        )?;
        selector_index::upsert_index_entries(&tx, &api_version, &kind, &namespace, &name, &data)?;
        owner_ref_index::upsert_owner_refs(&tx, &api_version, &kind, &namespace, &name, &data)?;
        mutation_helpers::insert_watch_event_in_conn(
            &tx,
            mutation_helpers::WatchEventInsert::new(
                &api_version,
                &kind,
                Some(&namespace),
                &name,
                rv,
                "ADDED",
                &data,
            ),
        )?;
    } else {
        tx.execute(
            queries::CLUSTER_INSERT,
            rusqlite::params![&api_version, &kind, &name, &uid, rv, &data],
        )?;
        selector_index::upsert_index_entries(&tx, &api_version, &kind, "", &name, &data)?;
        owner_ref_index::upsert_owner_refs(&tx, &api_version, &kind, "", &name, &data)?;
        mutation_helpers::insert_watch_event_in_conn(
            &tx,
            mutation_helpers::WatchEventInsert::new(
                &api_version,
                &kind,
                None,
                &name,
                rv,
                "ADDED",
                &data,
            ),
        )?;
    }
    let rowid = tx.last_insert_rowid();
    tx.commit()?;
    Ok((rowid, rv))
}
