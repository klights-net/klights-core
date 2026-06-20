//! Resource delete — hard-delete with precondition validation and watch
//! event emission carrying the deleted object body.

use super::super::owner_ref_index;
use super::super::queries;
use super::super::selector_index;
use super::helpers::*;
use super::*;
use rusqlite::TransactionBehavior;

use crate::datastore::sqlite::create_pending_watch_event;

impl Datastore {
    /// Apply a patch against the current state of a resource without a
    /// compare-and-swap resourceVersion check.
    ///
    /// Returns `Ok(None)` when the row is missing.
    pub async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()> {
        self.delete_resource_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            ResourcePreconditions::default(),
        )
        .await
    }

    pub async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<()> {
        self.delete_resource_with_preconditions_observed_rv(
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
        .await
        .map(|_| ())
    }

    pub async fn delete_resource_with_preconditions_observed_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<i64> {
        enum DeleteAttempt {
            Deleted(i64, Vec<u8>),
            NotFound,
            PreconditionFailed {
                message: String,
                live_uid: Option<String>,
            },
        }

        // tokio-rusqlite::call closures must be `'static`.
        let av = api_version.to_string();
        let k = kind.to_string();
        let n = name.to_string();

        // Route to correct table based on resource scope.
        // Hard-delete: read the row's data first (so the watch_events DELETED event
        // carries the object body), DELETE the row, then INSERT the watch_event.
        let result = if use_namespaced_table(api_version, kind, &namespace) {
            let ns = namespace.unwrap_or("default").to_string();
            let preconditions = preconditions.clone();
            self.db_call("db_query", move |conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current = match tx.query_row(
                    queries::NAMESPACED_GET_DATA_FOR_DELETE,
                    rusqlite::params![&av, &k, &ns, &n],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                        ))
                    },
                ) {
                    Ok(current) => current,
                    Err(rusqlite::Error::QueryReturnedNoRows) => {
                        return Ok(DeleteAttempt::NotFound);
                    }
                    Err(e) => return Err(tokio_rusqlite::Error::Rusqlite(e)),
                };
                let (current_rv, current_uid, data_bytes) = current;
                if let Err(err) =
                    validate_resource_preconditions(&preconditions, Some(&current_uid), current_rv)
                {
                    return Ok(DeleteAttempt::PreconditionFailed {
                        message: err.to_string(),
                        live_uid: Some(current_uid),
                    });
                }
                let rv = Self::next_resource_version_in_tx(&tx)?;
                let rows = tx.execute(
                    queries::NAMESPACED_DELETE,
                    rusqlite::params![&av, &k, &ns, &n, &current_uid],
                )?;
                if rows == 0 {
                    return Ok(DeleteAttempt::NotFound);
                }
                selector_index::delete_index_entries(&tx, &av, &k, &ns, &n)?;
                owner_ref_index::delete_owner_refs(&tx, &av, &k, &ns, &n)?;
                insert_watch_event_in_conn(
                    &tx,
                    WatchEventInsert::new(&av, &k, Some(&ns), &n, rv, "DELETED", &data_bytes),
                )?;
                tx.commit()?;
                Ok(DeleteAttempt::Deleted(rv, data_bytes))
            })
            .await
        } else {
            let preconditions = preconditions.clone();
            self.db_call("db_query", move |conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current = match tx.query_row(
                    queries::CLUSTER_GET_DATA_FOR_DELETE,
                    rusqlite::params![&av, &k, &n],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                        ))
                    },
                ) {
                    Ok(current) => current,
                    Err(rusqlite::Error::QueryReturnedNoRows) => {
                        return Ok(DeleteAttempt::NotFound);
                    }
                    Err(e) => return Err(tokio_rusqlite::Error::Rusqlite(e)),
                };
                let (current_rv, current_uid, data_bytes) = current;
                if let Err(err) =
                    validate_resource_preconditions(&preconditions, Some(&current_uid), current_rv)
                {
                    return Ok(DeleteAttempt::PreconditionFailed {
                        message: err.to_string(),
                        live_uid: Some(current_uid),
                    });
                }
                let rv = Self::next_resource_version_in_tx(&tx)?;
                let rows = tx.execute(
                    queries::CLUSTER_DELETE,
                    rusqlite::params![&av, &k, &n, &current_uid],
                )?;
                if rows == 0 {
                    return Ok(DeleteAttempt::NotFound);
                }
                selector_index::delete_index_entries(&tx, &av, &k, "", &n)?;
                owner_ref_index::delete_owner_refs(&tx, &av, &k, "", &n)?;
                insert_watch_event_in_conn(
                    &tx,
                    WatchEventInsert::new(&av, &k, None, &n, rv, "DELETED", &data_bytes),
                )?;
                tx.commit()?;
                Ok(DeleteAttempt::Deleted(rv, data_bytes))
            })
            .await
        };

        match result {
            Ok(DeleteAttempt::Deleted(rv, data_bytes)) => {
                if let Ok(data) = serde_json::from_slice::<Value>(&data_bytes) {
                    let pending = create_pending_watch_event(
                        api_version,
                        kind,
                        namespace,
                        name,
                        rv,
                        "DELETED",
                        data,
                    );
                    self.publish_watch_event(pending);
                }
                Ok(rv)
            }
            Ok(DeleteAttempt::NotFound) => Err(anyhow!("Resource not found")),
            Ok(DeleteAttempt::PreconditionFailed { message, live_uid }) => {
                if let Some(expected_uid) = preconditions.uid.as_deref()
                    && live_uid.as_deref() != Some(expected_uid)
                {
                    warn_uid_precondition_mismatch(
                        "delete_resource",
                        api_version,
                        kind,
                        namespace,
                        name,
                        expected_uid,
                        live_uid.as_deref(),
                    );
                }
                Err(crate::datastore::errors::DatastoreError::conflict(message).into())
            }
            Err(e) => Err(anyhow!("Failed to delete resource: {}", e)),
        }
    }
}
