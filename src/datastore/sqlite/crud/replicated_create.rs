#![cfg(test)]
//! TO-BE-CLEANUP: legacy replicated StorageCommand test support only.
//!
//! Replicated create resource — converges a follower cache to the
//! leader's object identity, including delete/recreate slots where the
//! same name now has a different UID.

use anyhow::Context;

use super::super::owner_ref_index;
use super::super::queries;
use super::super::selector_index;
use super::helpers::*;
use super::*;

use crate::datastore::sqlite::create_pending_watch_event;

impl Datastore {
    pub async fn apply_replicated_create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        mut data: Value,
        options: ReplicatedCreateOptions,
    ) -> Result<Resource> {
        let ReplicatedCreateOptions {
            resource_version,
            meta_uid,
        } = options;
        ensure_resource_type_meta(&mut data, api_version, kind);
        ensure_metadata_identity(&mut data, namespace, name);
        ensure_pod_status_ip_arrays(&mut data, api_version, kind);
        let incoming_uid = ensure_metadata_uid(&mut data);
        if let Some(expected_uid) = meta_uid.as_deref()
            && expected_uid != incoming_uid
        {
            return Err(crate::datastore::errors::DatastoreError::conflict(format!(
                    "replicated create UID precondition failed: expected {expected_uid} got {incoming_uid}"
                ))
                .into());
        }
        if resource_version <= 0 {
            return Err(anyhow!(
                "replicated create resourceVersion must be positive"
            ));
        }

        let av = api_version.to_string();
        let k = kind.to_string();
        let ns = namespace.map(str::to_string);
        let n = name.to_string();
        let data_bytes = serde_json::to_vec(&data)?;
        let incoming_uid_for_db = incoming_uid.clone();

        enum ApplyCreateResult {
            AlreadyReflected {
                id: i64,
                current_rv: i64,
                current_data: Vec<u8>,
            },
            Inserted(i64),
            UpdatedSameUid(i64),
            ReplacedDifferentUid {
                old_uid: String,
                deleted_rv: i64,
                old_data: Vec<u8>,
                id: i64,
            },
        }

        let result = if use_namespaced_table(api_version, kind, &namespace) {
            let ns = namespace.unwrap_or("default").to_string();
            let incoming_uid_for_db = incoming_uid_for_db.clone();
            self.db_call("replicated_create_resource", move |conn| {
                let current = match conn.query_row(
                    queries::NAMESPACED_GET,
                    rusqlite::params![&av, &k, &ns, &n],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, Vec<u8>>(7)?,
                        ))
                    },
                ) {
                    Ok(current) => Some(current),
                    Err(rusqlite::Error::QueryReturnedNoRows) => None,
                    Err(e) => return Err(tokio_rusqlite::Error::Rusqlite(e)),
                };

                match current {
                    None => {
                        conn.execute(
                            queries::NAMESPACED_INSERT,
                            rusqlite::params![
                                &av,
                                &k,
                                &ns,
                                &n,
                                &incoming_uid_for_db,
                                resource_version,
                                &data_bytes
                            ],
                        )?;
                        advance_metadata_rv_to_at_least(conn, resource_version)?;
                        selector_index::upsert_index_entries(conn, &av, &k, &ns, &n, &data_bytes)?;
                        owner_ref_index::upsert_owner_refs(conn, &av, &k, &ns, &n, &data_bytes)?;
                        insert_watch_event_in_conn(
                            conn,
                            WatchEventInsert::new(
                                &av,
                                &k,
                                Some(&ns),
                                &n,
                                resource_version,
                                "ADDED",
                                &data_bytes,
                            ),
                        )?;
                        Ok(ApplyCreateResult::Inserted(conn.last_insert_rowid()))
                    }
                    Some((id, current_rv, current_uid, current_data))
                        if current_uid == incoming_uid_for_db =>
                    {
                        if current_rv >= resource_version {
                            Ok(ApplyCreateResult::AlreadyReflected {
                                id,
                                current_rv,
                                current_data,
                            })
                        } else {
                            conn.execute(
                                queries::NAMESPACED_UPDATE_BY_RV,
                                rusqlite::params![
                                    resource_version,
                                    &incoming_uid_for_db,
                                    &data_bytes,
                                    &av,
                                    &k,
                                    &ns,
                                    &n,
                                    Option::<i64>::None,
                                    Option::<&str>::None
                                ],
                            )?;
                            advance_metadata_rv_to_at_least(conn, resource_version)?;
                            selector_index::upsert_index_entries(
                                conn,
                                &av,
                                &k,
                                &ns,
                                &n,
                                &data_bytes,
                            )?;
                            owner_ref_index::upsert_owner_refs(
                                conn,
                                &av,
                                &k,
                                &ns,
                                &n,
                                &data_bytes,
                            )?;
                            insert_watch_event_in_conn(
                                conn,
                                WatchEventInsert::new(
                                    &av,
                                    &k,
                                    Some(&ns),
                                    &n,
                                    resource_version,
                                    "MODIFIED",
                                    &data_bytes,
                                ),
                            )?;
                            Ok(ApplyCreateResult::UpdatedSameUid(id))
                        }
                    }
                    Some((_id, _current_rv, current_uid, current_data)) => {
                        let deleted_rv = resource_version.saturating_sub(1);
                        conn.execute(
                            queries::NAMESPACED_DELETE,
                            rusqlite::params![&av, &k, &ns, &n, &current_uid],
                        )?;
                        selector_index::delete_index_entries(conn, &av, &k, &ns, &n)?;
                        owner_ref_index::delete_owner_refs(conn, &av, &k, &ns, &n)?;
                        conn.execute(
                            queries::NAMESPACED_INSERT,
                            rusqlite::params![
                                &av,
                                &k,
                                &ns,
                                &n,
                                &incoming_uid_for_db,
                                resource_version,
                                &data_bytes
                            ],
                        )?;
                        advance_metadata_rv_to_at_least(conn, resource_version)?;
                        selector_index::upsert_index_entries(conn, &av, &k, &ns, &n, &data_bytes)?;
                        owner_ref_index::upsert_owner_refs(conn, &av, &k, &ns, &n, &data_bytes)?;
                        insert_watch_event_in_conn(
                            conn,
                            WatchEventInsert::new(
                                &av,
                                &k,
                                Some(&ns),
                                &n,
                                resource_version,
                                "ADDED",
                                &data_bytes,
                            ),
                        )?;
                        Ok(ApplyCreateResult::ReplacedDifferentUid {
                            old_uid: current_uid,
                            deleted_rv,
                            old_data: current_data,
                            id: conn.last_insert_rowid(),
                        })
                    }
                }
            })
            .await
        } else {
            self.db_call("replicated_create_resource", move |conn| {
                let current = match conn.query_row(
                    queries::CLUSTER_GET,
                    rusqlite::params![&av, &k, &n],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, Vec<u8>>(6)?,
                        ))
                    },
                ) {
                    Ok(current) => Some(current),
                    Err(rusqlite::Error::QueryReturnedNoRows) => None,
                    Err(e) => return Err(tokio_rusqlite::Error::Rusqlite(e)),
                };

                match current {
                    None => {
                        conn.execute(
                            queries::CLUSTER_INSERT,
                            rusqlite::params![
                                &av,
                                &k,
                                &n,
                                &incoming_uid_for_db,
                                resource_version,
                                &data_bytes
                            ],
                        )?;
                        advance_metadata_rv_to_at_least(conn, resource_version)?;
                        selector_index::upsert_index_entries(conn, &av, &k, "", &n, &data_bytes)?;
                        owner_ref_index::upsert_owner_refs(conn, &av, &k, "", &n, &data_bytes)?;
                        insert_watch_event_in_conn(
                            conn,
                            WatchEventInsert::new(
                                &av,
                                &k,
                                None,
                                &n,
                                resource_version,
                                "ADDED",
                                &data_bytes,
                            ),
                        )?;
                        Ok(ApplyCreateResult::Inserted(conn.last_insert_rowid()))
                    }
                    Some((id, current_rv, current_uid, current_data))
                        if current_uid == incoming_uid_for_db =>
                    {
                        if current_rv >= resource_version {
                            Ok(ApplyCreateResult::AlreadyReflected {
                                id,
                                current_rv,
                                current_data,
                            })
                        } else {
                            conn.execute(
                                queries::CLUSTER_UPDATE_BY_RV,
                                rusqlite::params![
                                    resource_version,
                                    &incoming_uid_for_db,
                                    &data_bytes,
                                    &av,
                                    &k,
                                    &n,
                                    Option::<i64>::None,
                                    Option::<&str>::None
                                ],
                            )?;
                            advance_metadata_rv_to_at_least(conn, resource_version)?;
                            selector_index::upsert_index_entries(
                                conn,
                                &av,
                                &k,
                                "",
                                &n,
                                &data_bytes,
                            )?;
                            owner_ref_index::upsert_owner_refs(conn, &av, &k, "", &n, &data_bytes)?;
                            insert_watch_event_in_conn(
                                conn,
                                WatchEventInsert::new(
                                    &av,
                                    &k,
                                    None,
                                    &n,
                                    resource_version,
                                    "MODIFIED",
                                    &data_bytes,
                                ),
                            )?;
                            Ok(ApplyCreateResult::UpdatedSameUid(id))
                        }
                    }
                    Some((_id, _current_rv, current_uid, current_data)) => {
                        let deleted_rv = resource_version.saturating_sub(1);
                        conn.execute(
                            queries::CLUSTER_DELETE,
                            rusqlite::params![&av, &k, &n, &current_uid],
                        )?;
                        selector_index::delete_index_entries(conn, &av, &k, "", &n)?;
                        owner_ref_index::delete_owner_refs(conn, &av, &k, "", &n)?;
                        conn.execute(
                            queries::CLUSTER_INSERT,
                            rusqlite::params![
                                &av,
                                &k,
                                &n,
                                &incoming_uid_for_db,
                                resource_version,
                                &data_bytes
                            ],
                        )?;
                        advance_metadata_rv_to_at_least(conn, resource_version)?;
                        selector_index::upsert_index_entries(conn, &av, &k, "", &n, &data_bytes)?;
                        owner_ref_index::upsert_owner_refs(conn, &av, &k, "", &n, &data_bytes)?;
                        insert_watch_event_in_conn(
                            conn,
                            WatchEventInsert::new(
                                &av,
                                &k,
                                None,
                                &n,
                                resource_version,
                                "ADDED",
                                &data_bytes,
                            ),
                        )?;
                        Ok(ApplyCreateResult::ReplacedDifferentUid {
                            old_uid: current_uid,
                            deleted_rv,
                            old_data: current_data,
                            id: conn.last_insert_rowid(),
                        })
                    }
                }
            })
            .await
        };

        let id = match result {
            Ok(ApplyCreateResult::AlreadyReflected {
                id,
                current_rv,
                current_data,
            }) => {
                let current_data: Value = serde_json::from_slice(&current_data)
                    .context("deserialize already-reflected replicated create payload")?;
                let uid = Resource::uid_from_data(&current_data);
                return Ok(Resource {
                    id,
                    api_version: api_version.to_string(),
                    kind: kind.to_string(),
                    namespace: ns,
                    name: name.to_string(),
                    uid,
                    resource_version: current_rv,
                    data: std::sync::Arc::new(current_data),
                });
            }
            Ok(ApplyCreateResult::Inserted(id)) => {
                let pending = create_pending_watch_event(
                    api_version,
                    kind,
                    namespace,
                    name,
                    resource_version,
                    "ADDED",
                    data.clone(),
                );
                self.publish_watch_event(pending);
                id
            }
            Ok(ApplyCreateResult::UpdatedSameUid(id)) => {
                let pending = create_pending_watch_event(
                    api_version,
                    kind,
                    namespace,
                    name,
                    resource_version,
                    "MODIFIED",
                    data.clone(),
                );
                self.publish_watch_event(pending);
                id
            }
            Ok(ApplyCreateResult::ReplacedDifferentUid {
                old_uid,
                deleted_rv,
                old_data,
                id,
            }) => {
                tracing::warn!(
                    api_version = %api_version,
                    kind = %kind,
                    namespace = namespace.unwrap_or(""),
                    name = %name,
                    old_uid = %old_uid,
                    new_uid = %incoming_uid,
                    deleted_rv,
                    resource_version,
                    "replicated create replaced stale same-name resource with different UID"
                );
                if let Ok(old_data) = serde_json::from_slice::<Value>(&old_data) {
                    let pending_delete = create_pending_watch_event(
                        api_version,
                        kind,
                        namespace,
                        name,
                        deleted_rv,
                        "DELETED",
                        old_data,
                    );
                    self.publish_watch_event(pending_delete);
                }
                let pending_add = create_pending_watch_event(
                    api_version,
                    kind,
                    namespace,
                    name,
                    resource_version,
                    "ADDED",
                    data.clone(),
                );
                self.publish_watch_event(pending_add);
                id
            }
            Err(e) => return Err(anyhow!("Failed to apply replicated create resource: {}", e)),
        };

        Ok(Resource {
            id,
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: ns,
            name: name.to_string(),
            uid: incoming_uid,
            resource_version,
            data: std::sync::Arc::new(data),
        })
    }
}
