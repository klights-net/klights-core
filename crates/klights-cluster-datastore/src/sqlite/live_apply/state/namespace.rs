use super::super::mutation_helpers::{
    WatchEventInsert, insert_watch_event_in_conn, serde_to_sqlite_error,
};
use super::super::{
    ApplyConflictCode, apply_conflict_error, create_staged_post_commit, mutation_queries,
};
use klights_cluster_core::LogApplyNamespaceRow;
use klights_cluster_store::StagedPostCommit;
use rusqlite::OptionalExtension;

pub(super) struct NamespaceStateApplier<'tx, 'conn> {
    tx: &'tx rusqlite::Transaction<'conn>,
}

impl<'tx, 'conn> NamespaceStateApplier<'tx, 'conn> {
    pub(super) fn new(tx: &'tx rusqlite::Transaction<'conn>) -> Self {
        Self { tx }
    }

    pub(super) fn put_namespace(
        &self,
        row: LogApplyNamespaceRow,
        emit_watch_events: bool,
    ) -> klights_supervisor::DbClosureResult<Option<StagedPostCommit>> {
        let data_bytes = serde_json::to_vec(&row.data)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        let existing = self
            .tx
            .query_row(
                mutation_queries::NAMESPACE_GET,
                rusqlite::params![&row.name],
                |db_row| {
                    Ok((
                        db_row.get::<_, i64>(1)?,
                        db_row.get::<_, String>(2)?,
                        db_row.get::<_, Vec<u8>>(3)?,
                    ))
                },
            )
            .optional()?;
        if existing.as_ref().is_some_and(|(rv, _uid, existing_bytes)| {
            *rv == row.resource_version && *existing_bytes == data_bytes
        }) {
            return Ok(None);
        }
        let had_existing = existing.is_some();
        if existing.is_none() {
            // Create-only path: the namespace does not exist yet. Use a plain
            // INSERT so that a concurrent explicit-name create that races past
            // the same check hits the PRIMARY KEY constraint and is rejected as
            // AlreadyExists instead of silently overwriting via UPSERT.
            self.tx
                .execute(
                    mutation_queries::NAMESPACES_INSERT,
                    rusqlite::params![&row.name, &row.uid, row.resource_version, &data_bytes],
                )
                .map_err(|err| match err {
                    rusqlite::Error::SqliteFailure(e, _)
                        if e.code == rusqlite::ErrorCode::ConstraintViolation =>
                    {
                        apply_conflict_error(
                            ApplyConflictCode::AlreadyExists,
                            format!("Namespace \"{}\" already exists", row.name),
                        )
                    }
                    other => klights_supervisor::DbError::Sqlite(other),
                })?;
        } else {
            let (_existing_rv, existing_uid, _existing_bytes) = existing.unwrap();
            if existing_uid != row.uid {
                // Concurrent create race: the namespace already exists under a
                // different UID, meaning another explicit-name create won the
                // PRIMARY KEY insert. Reject this as AlreadyExists instead of
                // silently overwriting via UPSERT.
                self.tx
                    .execute(
                        mutation_queries::NAMESPACES_INSERT,
                        rusqlite::params![&row.name, &row.uid, row.resource_version, &data_bytes],
                    )
                    .map_err(|err| match err {
                        rusqlite::Error::SqliteFailure(e, _)
                            if e.code == rusqlite::ErrorCode::ConstraintViolation =>
                        {
                            apply_conflict_error(
                                ApplyConflictCode::AlreadyExists,
                                format!("Namespace \"{}\" already exists", row.name),
                            )
                        }
                        other => klights_supervisor::DbError::Sqlite(other),
                    })?;
            } else {
                // Same UID: idempotent replay or legitimate update that Raft has
                // serialized after the create. UPSERT is safe here.
                self.tx.execute(
                    mutation_queries::NAMESPACES_UPSERT_EXACT,
                    rusqlite::params![&row.name, &row.uid, row.resource_version, &data_bytes],
                )?;
            }
        }
        let event_type = if had_existing { "MODIFIED" } else { "ADDED" };
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
        Ok(Some(create_staged_post_commit(
            "v1",
            "Namespace",
            None,
            &row.name,
            row.resource_version,
            event_type,
            row.data,
        )))
    }

    pub(super) fn delete_namespace(
        &self,
        resource_version: i64,
        name: &str,
        emit_watch_events: bool,
    ) -> klights_supervisor::DbClosureResult<Option<StagedPostCommit>> {
        let existing = self
            .tx
            .query_row(
                mutation_queries::NAMESPACE_GET_DATA,
                rusqlite::params![name],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        let Some(data_bytes) = existing else {
            return Ok(None);
        };
        self.tx
            .execute(mutation_queries::NAMESPACE_DELETE, rusqlite::params![name])?;
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
        Ok(Some(create_staged_post_commit(
            "v1",
            "Namespace",
            None,
            name,
            resource_version,
            "DELETED",
            data,
        )))
    }

    pub(super) fn delete_namespace_contents(
        &self,
        name: &str,
    ) -> klights_supervisor::DbClosureResult<()> {
        let mut stmt = self
            .tx
            .prepare(mutation_queries::NAMESPACE_RESOURCES_LIST_EXCLUDING_KIND)?;
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
            mutation_queries::NAMESPACE_RESOURCES_DELETE_NON_PODS,
            rusqlite::params![name],
        )?;
        for (api_version, kind, namespace, resource_name) in rows {
            crate::sqlite::selector_index::delete_index_entries(
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
