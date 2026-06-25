//! Resource update and status-subresource writes — public update path with
//! precondition validation, deduplication, and status-only atomic updates.

use anyhow::Context;
use rusqlite::TransactionBehavior;

use super::super::owner_ref_index;
use super::super::queries;
use super::super::selector_index;
use super::helpers::*;
use super::*;

use crate::datastore::sqlite::create_pending_watch_event;

struct ResourceUpdateWithPreconditions<'a> {
    api_version: &'a str,
    kind: &'a str,
    namespace: Option<&'a str>,
    name: &'a str,
    data: Value,
    preconditions: ResourcePreconditions,
    preserve_latest_status: bool,
}

struct MainUpdatePreconditionCheck<'a> {
    api_version: &'a str,
    kind: &'a str,
    namespace: Option<&'a str>,
    name: &'a str,
    preconditions: &'a ResourcePreconditions,
    current: &'a Resource,
    preserve_latest_status: bool,
}

impl Datastore {
    fn preserve_latest_status_subresource_in_tx(
        tx: &rusqlite::Transaction<'_>,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        proposed: &mut Value,
    ) -> tokio_rusqlite::Result<()> {
        if !crate::resource_semantics::has_builtin_status_subresource(api_version, kind) {
            return Ok(());
        }

        let current_bytes: Vec<u8> = if use_namespaced_table(api_version, kind, &namespace) {
            tx.query_row(
                queries::NAMESPACED_SELECT_STATUS_ROW,
                rusqlite::params![api_version, kind, namespace.unwrap_or("default"), name],
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
            serde_json::from_slice(&current_bytes).map_err(serde_to_sqlite_error)?;
        crate::resource_semantics::preserve_status_subresource_on_main_update(
            api_version,
            kind,
            &current,
            proposed,
        );
        preserve_server_metadata_fields_from_existing(proposed, &current);
        Ok(())
    }

    pub async fn update_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        self.update_resource_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            data,
            ResourcePreconditions::resource_version(expected_rv),
        )
        .await
    }

    pub async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        self.update_resource_with_preconditions_impl(ResourceUpdateWithPreconditions {
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
            preserve_latest_status: false,
        })
        .await
    }

    pub async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        self.update_resource_with_preconditions_impl(ResourceUpdateWithPreconditions {
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
            preserve_latest_status: true,
        })
        .await
    }

    async fn preconditions_for_main_update_against_current(
        &self,
        check: MainUpdatePreconditionCheck<'_>,
    ) -> Result<ResourcePreconditions> {
        let MainUpdatePreconditionCheck {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            current,
            preserve_latest_status,
        } = check;
        let mut effective = preconditions.clone();
        let Some(expected_rv) = preconditions.resource_version else {
            return Ok(effective);
        };
        if !preserve_latest_status
            || !crate::resource_semantics::has_builtin_status_subresource(api_version, kind)
            || current.resource_version == expected_rv
            || current.resource_version < expected_rv
        {
            return Ok(effective);
        }

        let field_selector = format!("metadata.name={name}");
        let snapshot = self
            .snapshot_resources_at_rv(
                api_version,
                kind,
                namespace,
                ResourceListQuery::new(None, Some(&field_selector), Some(1), None),
                expected_rv,
            )
            .await?;
        let SnapshotAtRv::List(snapshot) = snapshot else {
            return Ok(effective);
        };
        let Some(base) = snapshot.items.into_iter().find(|resource| {
            resource.name == current.name && resource.namespace == current.namespace
        }) else {
            return Ok(effective);
        };
        if base.uid == current.uid
            && resource_client_owned_state_equal(base.data.as_ref(), current.data.as_ref())
        {
            effective.resource_version = Some(current.resource_version);
        }
        Ok(effective)
    }

    async fn update_resource_with_preconditions_impl(
        &self,
        request: ResourceUpdateWithPreconditions<'_>,
    ) -> Result<Resource> {
        let ResourceUpdateWithPreconditions {
            api_version,
            kind,
            namespace,
            name,
            mut data,
            preconditions,
            preserve_latest_status,
        } = request;
        let mut effective_preconditions = preconditions.clone();
        ensure_resource_type_meta(&mut data, api_version, kind);
        ensure_metadata_identity(&mut data, namespace, name);
        ensure_pod_status_ip_arrays(&mut data, api_version, kind);

        // Deduplication: check if data actually changed
        let existing = self
            .get_resource(api_version, kind, namespace, name)
            .await?;

        if let Some(ref existing_resource) = existing {
            validate_metadata_uid_immutable(&data, &existing_resource.data)?;
            if let Some(expected_uid) = preconditions.uid.as_deref() {
                let live_uid = metadata_uid(&existing_resource.data);
                if live_uid != Some(expected_uid) {
                    warn_uid_precondition_mismatch(
                        "update_resource",
                        api_version,
                        kind,
                        namespace,
                        name,
                        expected_uid,
                        live_uid,
                    );
                }
            }
            effective_preconditions = self
                .preconditions_for_main_update_against_current(MainUpdatePreconditionCheck {
                    api_version,
                    kind,
                    namespace,
                    name,
                    preconditions: &preconditions,
                    current: existing_resource,
                    preserve_latest_status,
                })
                .await?;
            validate_resource_preconditions(
                &effective_preconditions,
                metadata_uid(&existing_resource.data),
                existing_resource.resource_version,
            )?;
            preserve_server_metadata_fields_from_existing(&mut data, &existing_resource.data);

            // Dedupe: skip the write if the only change vs. the persisted copy
            // is metadata.resourceVersion. Compare structurally without
            // cloning either side.
            if resource_data_equal_ignoring_rv(&existing_resource.data, &data) {
                crate::datastore::diagnostics::log_noop_resource_write(
                    crate::datastore::diagnostics::NoopResourceWrite {
                        operation: "update_resource",
                        api_version,
                        kind,
                        namespace,
                        name,
                        uid: &existing_resource.uid,
                        resource_version: existing_resource.resource_version,
                        reason: "object unchanged",
                    },
                );
                return Ok(existing_resource.clone());
            }
        }
        let uid = ensure_metadata_uid(&mut data);

        // tokio-rusqlite::call closures must be `'static`.
        let av = api_version.to_string();
        let k = kind.to_string();
        let n = name.to_string();
        let expected_rv = effective_preconditions.resource_version;
        let expected_uid_for_log = effective_preconditions.uid.clone();
        let expected_uid = effective_preconditions.uid;

        let result = if use_namespaced_table(api_version, kind, &namespace) {
            let ns = namespace.unwrap_or("default").to_string();
            let expected_uid = expected_uid.clone();
            let uid = uid.clone();
            let mut data = data.clone();
            self.db_call("db_query", move |conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                if preserve_latest_status {
                    Self::preserve_latest_status_subresource_in_tx(
                        &tx,
                        &av,
                        &k,
                        Some(&ns),
                        &n,
                        &mut data,
                    )?;
                }
                let data_bytes = serde_json::to_vec(&data).map_err(serde_to_sqlite_error)?;
                let new_rv = Self::next_resource_version_in_tx(&tx)?;
                let rows = tx.execute(
                    queries::NAMESPACED_UPDATE_BY_RV,
                    rusqlite::params![
                        new_rv,
                        &uid,
                        &data_bytes,
                        &av,
                        &k,
                        &ns,
                        &n,
                        expected_rv,
                        expected_uid.as_deref()
                    ],
                )?;
                if rows == 0 {
                    return Err(tokio_rusqlite::Error::Rusqlite(
                        rusqlite::Error::QueryReturnedNoRows,
                    ));
                }
                let id: i64 = tx.query_row(
                    queries::NAMESPACED_SELECT_ID,
                    rusqlite::params![&av, &k, &ns, &n],
                    |row| row.get(0),
                )?;
                selector_index::upsert_index_entries(&tx, &av, &k, &ns, &n, &data_bytes)?;
                owner_ref_index::upsert_owner_refs(&tx, &av, &k, &ns, &n, &data_bytes)?;
                insert_watch_event_in_conn(
                    &tx,
                    WatchEventInsert::new(&av, &k, Some(&ns), &n, new_rv, "MODIFIED", &data_bytes),
                )?;
                tx.commit()?;
                Ok((id, new_rv, data))
            })
            .await
        } else {
            let expected_uid = expected_uid.clone();
            let uid = uid.clone();
            let mut data = data.clone();
            self.db_call("db_query", move |conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                if preserve_latest_status {
                    Self::preserve_latest_status_subresource_in_tx(
                        &tx, &av, &k, None, &n, &mut data,
                    )?;
                }
                let data_bytes = serde_json::to_vec(&data).map_err(serde_to_sqlite_error)?;
                let new_rv = Self::next_resource_version_in_tx(&tx)?;
                let rows = tx.execute(
                    queries::CLUSTER_UPDATE_BY_RV,
                    rusqlite::params![
                        new_rv,
                        &uid,
                        &data_bytes,
                        &av,
                        &k,
                        &n,
                        expected_rv,
                        expected_uid.as_deref()
                    ],
                )?;
                if rows == 0 {
                    return Err(tokio_rusqlite::Error::Rusqlite(
                        rusqlite::Error::QueryReturnedNoRows,
                    ));
                }
                let id: i64 = tx.query_row(
                    queries::CLUSTER_SELECT_ID,
                    rusqlite::params![&av, &k, &n],
                    |row| row.get(0),
                )?;
                selector_index::upsert_index_entries(&tx, &av, &k, "", &n, &data_bytes)?;
                owner_ref_index::upsert_owner_refs(&tx, &av, &k, "", &n, &data_bytes)?;
                insert_watch_event_in_conn(
                    &tx,
                    WatchEventInsert::new(&av, &k, None, &n, new_rv, "MODIFIED", &data_bytes),
                )?;
                tx.commit()?;
                Ok((id, new_rv, data))
            })
            .await
        };

        match result {
            Ok((id, new_rv, data)) => {
                let pending = create_pending_watch_event(
                    api_version,
                    kind,
                    namespace,
                    name,
                    new_rv,
                    "MODIFIED",
                    data.clone(),
                );
                self.publish_watch_event(pending);

                Ok(Resource {
                    id,
                    api_version: api_version.to_string(),
                    kind: kind.to_string(),
                    namespace: namespace.map(str::to_string),
                    name: name.to_string(),
                    uid,
                    resource_version: new_rv,
                    data: std::sync::Arc::new(data),
                })
            }
            Err(tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows)) => {
                if let Some(expected_uid) = expected_uid_for_log.as_deref() {
                    self.warn_uid_precondition_mismatch_if_live(
                        "update_resource",
                        api_version,
                        kind,
                        namespace,
                        name,
                        expected_uid,
                    )
                    .await;
                }
                Err(crate::datastore::errors::DatastoreError::conflict(
                    "Resource not found or version conflict",
                )
                .into())
            }
            Err(e) => Err(anyhow!("Failed to update resource: {}", e)),
        }
    }

    /// Update only the `.status` subtree of a resource atomically inside SQLite.
    ///
    /// Uses `json_set(data, '$.status', json(?))` so `.spec`, `.metadata`, and any
    /// other top-level fields are preserved verbatim — there is no read-modify-write
    /// race window where a concurrent `.spec` edit could be lost.
    ///
    /// `expected_rv = Some(rv)` enables compare-and-swap (returns 409 Conflict on
    /// mismatch). `expected_rv = None` skips the check and unconditionally writes.
    pub async fn update_status_only(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        self.update_status_only_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            status,
            ResourcePreconditions {
                uid: None,
                resource_version: expected_rv,
            },
        )
        .await
    }

    pub async fn update_status_only_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        let av = api_version.to_string();
        let k = kind.to_string();
        let n = name.to_string();
        let expected_rv = preconditions.resource_version;
        let expected_uid_for_log = preconditions.uid.clone();
        let expected_uid = preconditions.uid;

        struct StatusUpdateOutcome {
            id: i64,
            resource_version: i64,
            data: Vec<u8>,
            changed: bool,
        }

        let result = if use_namespaced_table(api_version, kind, &namespace) {
            let ns = namespace.unwrap_or("default").to_string();
            let expected_uid = expected_uid.clone();
            let status = status.clone();
            self.db_call("db_query", move |conn| {
                let (id, current_rv, live_uid, current_bytes): (i64, i64, String, Vec<u8>) = conn
                    .query_row(
                    queries::NAMESPACED_SELECT_STATUS_ROW,
                    rusqlite::params![&av, &k, &ns, &n],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )?;
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
                    serde_json::from_slice(&current_bytes).map_err(serde_to_sqlite_error)?;
                if current.get("status") == Some(&status) {
                    crate::datastore::diagnostics::log_noop_resource_write(
                        crate::datastore::diagnostics::NoopResourceWrite {
                            operation: "update_status_only",
                            api_version: &av,
                            kind: &k,
                            namespace: Some(&ns),
                            name: &n,
                            uid: &live_uid,
                            resource_version: current_rv,
                            reason: "status unchanged",
                        },
                    );
                    return Ok(StatusUpdateOutcome {
                        id,
                        resource_version: current_rv,
                        data: current_bytes,
                        changed: false,
                    });
                }
                if let Some(obj) = current.as_object_mut() {
                    obj.insert("status".to_string(), status);
                } else {
                    current = serde_json::json!({ "status": status });
                }
                let merged = serde_json::to_vec(&current).map_err(serde_to_sqlite_error)?;
                let new_rv = Self::next_resource_version_in_conn(conn)?;
                let rows = conn.execute(
                    queries::NAMESPACED_UPDATE_STATUS_BY_ID,
                    rusqlite::params![new_rv, &merged, id, current_rv, &live_uid],
                )?;
                if rows == 0 {
                    return Err(tokio_rusqlite::Error::Rusqlite(
                        rusqlite::Error::QueryReturnedNoRows,
                    ));
                }
                selector_index::upsert_index_entries(conn, &av, &k, &ns, &n, &merged)?;
                owner_ref_index::upsert_owner_refs(conn, &av, &k, &ns, &n, &merged)?;
                insert_watch_event_in_conn(
                    conn,
                    WatchEventInsert::new(&av, &k, Some(&ns), &n, new_rv, "MODIFIED", &merged),
                )?;
                Ok(StatusUpdateOutcome {
                    id,
                    resource_version: new_rv,
                    data: merged,
                    changed: true,
                })
            })
            .await
        } else {
            let expected_uid = expected_uid.clone();
            self.db_call("db_query", move |conn| {
                let (id, current_rv, live_uid, current_bytes): (i64, i64, String, Vec<u8>) = conn
                    .query_row(
                    queries::CLUSTER_SELECT_STATUS_ROW,
                    rusqlite::params![&av, &k, &n],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )?;
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
                    serde_json::from_slice(&current_bytes).map_err(serde_to_sqlite_error)?;
                if current.get("status") == Some(&status) {
                    crate::datastore::diagnostics::log_noop_resource_write(
                        crate::datastore::diagnostics::NoopResourceWrite {
                            operation: "update_status_only",
                            api_version: &av,
                            kind: &k,
                            namespace: None,
                            name: &n,
                            uid: &live_uid,
                            resource_version: current_rv,
                            reason: "status unchanged",
                        },
                    );
                    return Ok(StatusUpdateOutcome {
                        id,
                        resource_version: current_rv,
                        data: current_bytes,
                        changed: false,
                    });
                }
                if let Some(obj) = current.as_object_mut() {
                    obj.insert("status".to_string(), status);
                } else {
                    current = serde_json::json!({ "status": status });
                }
                let merged = serde_json::to_vec(&current).map_err(serde_to_sqlite_error)?;
                let new_rv = Self::next_resource_version_in_conn(conn)?;
                let rows = conn.execute(
                    queries::CLUSTER_UPDATE_STATUS_BY_ID,
                    rusqlite::params![new_rv, &merged, id, current_rv, &live_uid],
                )?;
                if rows == 0 {
                    return Err(tokio_rusqlite::Error::Rusqlite(
                        rusqlite::Error::QueryReturnedNoRows,
                    ));
                }
                selector_index::upsert_index_entries(conn, &av, &k, "", &n, &merged)?;
                owner_ref_index::upsert_owner_refs(conn, &av, &k, "", &n, &merged)?;
                insert_watch_event_in_conn(
                    conn,
                    WatchEventInsert::new(&av, &k, None, &n, new_rv, "MODIFIED", &merged),
                )?;
                Ok(StatusUpdateOutcome {
                    id,
                    resource_version: new_rv,
                    data: merged,
                    changed: true,
                })
            })
            .await
        };

        match result {
            Ok(outcome) => {
                let data: Value = serde_json::from_slice(&outcome.data)
                    .context("deserialize merged status payload")?;

                if outcome.changed {
                    let pending = create_pending_watch_event(
                        api_version,
                        kind,
                        namespace,
                        name,
                        outcome.resource_version,
                        "MODIFIED",
                        data.clone(),
                    );
                    self.publish_watch_event(pending);
                };

                Ok(Resource {
                    id: outcome.id,
                    api_version: api_version.to_string(),
                    kind: kind.to_string(),
                    namespace: namespace.map(str::to_string),
                    name: name.to_string(),
                    uid: Resource::uid_from_data(&data),
                    resource_version: outcome.resource_version,
                    data: std::sync::Arc::new(data),
                })
            }
            Err(tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows)) => {
                if let Some(expected_uid) = expected_uid_for_log.as_deref() {
                    self.warn_uid_precondition_mismatch_if_live(
                        "update_status_only",
                        api_version,
                        kind,
                        namespace,
                        name,
                        expected_uid,
                    )
                    .await;
                }
                Err(crate::datastore::errors::DatastoreError::conflict(
                    "Resource not found or version conflict",
                )
                .into())
            }
            Err(e) => Err(anyhow!("Failed to update status: {}", e)),
        }
    }
}
