use super::super::crud::helpers::{
    WatchEventInsert, insert_watch_event_in_conn, serde_to_sqlite_error,
};
use super::super::{create_pending_watch_event, queries};
use crate::datastore::types::PendingWatchEvent;
use crate::log_apply::LogApplyNamespaceRow;
use rusqlite::OptionalExtension;

pub(in crate::datastore::sqlite) struct NamespaceStateApplier<'tx, 'conn> {
    tx: &'tx rusqlite::Transaction<'conn>,
}

impl<'tx, 'conn> NamespaceStateApplier<'tx, 'conn> {
    pub(in crate::datastore::sqlite) fn new(tx: &'tx rusqlite::Transaction<'conn>) -> Self {
        Self { tx }
    }

    pub(in crate::datastore::sqlite) fn put_namespace(
        &self,
        row: LogApplyNamespaceRow,
        emit_watch_events: bool,
    ) -> tokio_rusqlite::Result<Option<PendingWatchEvent>> {
        let data_bytes = serde_json::to_vec(&row.data)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        let existing = self
            .tx
            .query_row(
                queries::NAMESPACE_GET,
                rusqlite::params![&row.name],
                |db_row| Ok((db_row.get::<_, i64>(1)?, db_row.get::<_, Vec<u8>>(3)?)),
            )
            .optional()?;
        if existing.as_ref().is_some_and(|(rv, existing_bytes)| {
            *rv == row.resource_version && *existing_bytes == data_bytes
        }) {
            return Ok(None);
        }
        self.tx.execute(
            queries::NAMESPACES_UPSERT_EXACT,
            rusqlite::params![&row.name, &row.uid, row.resource_version, &data_bytes],
        )?;
        let event_type = if existing.is_some() {
            "MODIFIED"
        } else {
            "ADDED"
        };
        if !emit_watch_events {
            return Ok(None);
        }
        insert_watch_event_in_conn(
            self.tx,
            WatchEventInsert::new(
                "v1",
                "Namespace",
                None,
                &row.name,
                row.resource_version,
                event_type,
                &data_bytes,
            ),
        )?;
        Ok(Some(create_pending_watch_event(
            "v1",
            "Namespace",
            None,
            &row.name,
            row.resource_version,
            event_type,
            row.data,
        )))
    }

    pub(in crate::datastore::sqlite) fn delete_namespace(
        &self,
        resource_version: i64,
        name: &str,
        emit_watch_events: bool,
    ) -> tokio_rusqlite::Result<Option<PendingWatchEvent>> {
        let existing = self
            .tx
            .query_row(
                queries::NAMESPACE_GET_DATA,
                rusqlite::params![name],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        let Some(data_bytes) = existing else {
            return Ok(None);
        };
        self.tx
            .execute(queries::NAMESPACE_DELETE, rusqlite::params![name])?;
        if !emit_watch_events {
            return Ok(None);
        }
        insert_watch_event_in_conn(
            self.tx,
            WatchEventInsert::new(
                "v1",
                "Namespace",
                None,
                name,
                resource_version,
                "DELETED",
                &data_bytes,
            ),
        )?;
        let data: serde_json::Value =
            serde_json::from_slice(&data_bytes).map_err(serde_to_sqlite_error)?;
        Ok(Some(create_pending_watch_event(
            "v1",
            "Namespace",
            None,
            name,
            resource_version,
            "DELETED",
            data,
        )))
    }

    pub(in crate::datastore::sqlite) fn delete_namespace_contents(
        &self,
        name: &str,
    ) -> tokio_rusqlite::Result<()> {
        let mut stmt = self
            .tx
            .prepare(queries::NAMESPACE_RESOURCES_LIST_EXCLUDING_KIND)?;
        let rows = stmt
            .query_map(rusqlite::params![name, "Pod"], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        self.tx.execute(
            queries::NAMESPACE_RESOURCES_DELETE_NON_PODS,
            rusqlite::params![name],
        )?;
        for (api_version, kind, namespace, resource_name) in rows {
            super::super::selector_index::delete_index_entries(
                self.tx,
                &api_version,
                &kind,
                &namespace,
                &resource_name,
            )?;
            super::super::owner_ref_index::delete_owner_refs(
                self.tx,
                &api_version,
                &kind,
                &namespace,
                &resource_name,
            )?;
        }
        Ok(())
    }
}
