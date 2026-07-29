use super::{crud, queries, transaction_primitives};

pub(crate) enum NamespaceDeleteResult {
    Deleted { rv: i64, data: Vec<u8> },
    HasRemainingContent,
}

pub(crate) fn create_namespace_in_conn(
    conn: &rusqlite::Connection,
    name: String,
    uid: String,
    data: Vec<u8>,
) -> tokio_rusqlite::Result<i64> {
    let rv = transaction_primitives::next_resource_version_in_tx(conn)?;
    conn.execute(
        queries::NAMESPACES_INSERT,
        rusqlite::params![&name, &uid, rv, &data],
    )?;
    crud::helpers::insert_watch_event_in_conn(
        conn,
        crud::helpers::WatchEventInsert::new("v1", "Namespace", None, &name, rv, "ADDED", &data),
    )?;
    Ok(rv)
}

pub(crate) fn update_namespace_in_conn(
    conn: &rusqlite::Connection,
    name: String,
    uid: String,
    data: Vec<u8>,
    expected_rv: i64,
) -> tokio_rusqlite::Result<i64> {
    let rv = transaction_primitives::next_resource_version_in_tx(conn)?;
    let rows = conn.execute(
        queries::NAMESPACE_UPDATE,
        rusqlite::params![&uid, rv, &data, &name, expected_rv],
    )?;
    if rows == 0 {
        return Err(tokio_rusqlite::Error::Rusqlite(
            rusqlite::Error::QueryReturnedNoRows,
        ));
    }
    crud::helpers::insert_watch_event_in_conn(
        conn,
        crud::helpers::WatchEventInsert::new("v1", "Namespace", None, &name, rv, "MODIFIED", &data),
    )?;
    Ok(rv)
}

pub(crate) fn delete_namespace_in_conn(
    conn: &mut rusqlite::Connection,
    name: String,
) -> tokio_rusqlite::Result<NamespaceDeleteResult> {
    let tx = conn.transaction()?;
    let remaining: i64 = tx.query_row(
        queries::NAMESPACE_RESOURCES_COUNT,
        rusqlite::params![&name],
        |row| row.get(0),
    )?;
    if remaining > 0 {
        return Ok(NamespaceDeleteResult::HasRemainingContent);
    }
    let namespace_rv = transaction_primitives::next_resource_version_in_tx(&tx)?;
    let namespace_data: Vec<u8> = tx.query_row(
        queries::NAMESPACE_GET_DATA,
        rusqlite::params![&name],
        |row| row.get(0),
    )?;
    let ns_rows = tx.execute(queries::NAMESPACE_DELETE, rusqlite::params![&name])?;
    if ns_rows == 0 {
        return Err(tokio_rusqlite::Error::Rusqlite(
            rusqlite::Error::QueryReturnedNoRows,
        ));
    }
    crud::helpers::insert_watch_event_in_conn(
        &tx,
        crud::helpers::WatchEventInsert::new(
            "v1",
            "Namespace",
            None,
            &name,
            namespace_rv,
            "DELETED",
            &namespace_data,
        ),
    )?;
    tx.commit()?;
    Ok(NamespaceDeleteResult::Deleted {
        rv: namespace_rv,
        data: namespace_data,
    })
}

pub(crate) fn delete_namespace_contents_in_conn(
    conn: &mut rusqlite::Connection,
    name: String,
) -> tokio_rusqlite::Result<()> {
    let tx = conn.transaction()?;
    tx.query_row(
        queries::NAMESPACE_EXISTS,
        rusqlite::params![&name],
        |_row| Ok(()),
    )?;
    tx.execute(
        queries::NAMESPACE_RESOURCES_DELETE_NON_PODS,
        rusqlite::params![&name],
    )?;
    tx.commit()?;
    Ok(())
}
