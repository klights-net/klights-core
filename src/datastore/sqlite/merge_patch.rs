use super::owner_ref_index;
use super::queries;
use super::selector_index;
use super::*;
use anyhow::{Result, anyhow};
use rusqlite::{OptionalExtension, TransactionBehavior};
use serde_json::Value;

/// Owned identity tuple for a patch operation. The `db_call` closure
/// runs on a worker thread and must be `Send + 'static`, which forces
/// the captured strings to be owned. Bundling them in one struct keeps
/// the lifetime contract explicit and lets each SQL statement and the
/// returned `Resource` borrow from the same backing storage instead of
/// each rusqlite::params! call cloning a separate `&String`.
struct PatchKey {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    name: String,
}

impl PatchKey {
    fn from_borrowed(api_version: &str, kind: &str, namespace: Option<&str>, name: &str) -> Self {
        Self {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_owned),
            name: name.to_string(),
        }
    }
}

impl Datastore {
    pub async fn patch_resource_latest(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        patch_kind: PatchKind,
        patch: Value,
    ) -> Result<Option<Resource>> {
        self.patch_resource_latest_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            ResourcePatchRequest::without_preconditions(patch_kind, patch),
        )
        .await
    }

    pub async fn patch_resource_latest_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: ResourcePatchRequest,
    ) -> Result<Option<Resource>> {
        let ResourcePatchRequest {
            patch_kind,
            patch,
            preconditions,
            strict_resource_version,
        } = request;
        // All four owned strings are captured by the move closure below
        // and consumed by both the SQL parameters and the constructed
        // return value. `'static` for the closure (db_call boundary)
        // requires ownership; bundling in PatchKey scopes the
        // allocation to the SQL path it serves.
        let key = PatchKey::from_borrowed(api_version, kind, namespace, name);
        let uses_namespaced =
            use_namespaced_table(&key.api_version, &key.kind, &key.namespace.as_deref());

        let result = self
            .db_call("db_query", move |conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

                struct ExistingResource {
                    id: i64,
                    rv: i64,
                    uid: String,
                    namespace: Option<String>,
                    data: Value,
                }

                let maybe_current: Option<ExistingResource> = if uses_namespaced {
                    let ns = key
                        .namespace
                        .clone()
                        .unwrap_or_else(|| "default".to_string());
                    let ns_for_result = ns.clone();
                    tx.query_row(
                        queries::NAMESPACED_GET_FOR_PATCH,
                        rusqlite::params![&key.api_version, &key.kind, &ns, &key.name],
                        move |row| {
                            let id: i64 = row.get(0)?;
                            let rv: i64 = row.get(1)?;
                            let uid: String = row.get(2)?;
                            let bytes: Vec<u8> = row.get(3)?;
                            let data = serde_json::from_slice::<Value>(&bytes).map_err(|e| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    3,
                                    rusqlite::types::Type::Blob,
                                    Box::new(e),
                                )
                            })?;
                            Ok(ExistingResource {
                                id,
                                rv,
                                uid,
                                namespace: Some(ns_for_result.clone()),
                                data,
                            })
                        },
                    )
                    .optional()?
                } else {
                    tx.query_row(
                        queries::CLUSTER_GET_FOR_PATCH,
                        rusqlite::params![&key.api_version, &key.kind, &key.name],
                        |row| {
                            let id: i64 = row.get(0)?;
                            let rv: i64 = row.get(1)?;
                            let uid: String = row.get(2)?;
                            let bytes: Vec<u8> = row.get(3)?;
                            let data = serde_json::from_slice::<Value>(&bytes).map_err(|e| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    3,
                                    rusqlite::types::Type::Blob,
                                    Box::new(e),
                                )
                            })?;
                            Ok(ExistingResource {
                                id,
                                rv,
                                uid,
                                namespace: None,
                                data,
                            })
                        },
                    )
                    .optional()?
                };

                let Some(current) = maybe_current else {
                    tx.commit()?;
                    return Ok(None);
                };

                if let Some(expected_uid) = preconditions.uid.as_deref()
                    && current.uid != expected_uid
                {
                    warn_uid_precondition_mismatch(
                        "patch_resource_latest",
                        &key.api_version,
                        &key.kind,
                        key.namespace.as_deref(),
                        &key.name,
                        expected_uid,
                        Some(&current.uid),
                    );
                }
                let mut effective_preconditions = preconditions.clone();
                if !strict_resource_version
                    && let Some(expected) = effective_preconditions.resource_version
                    && expected != current.rv
                    && crate::resource_semantics::has_builtin_status_subresource(
                        &key.api_version,
                        &key.kind,
                    )
                    && let Some(base) = Self::resource_snapshot_for_key_at_rv_in_tx(
                        &tx,
                        &key.api_version,
                        &key.kind,
                        key.namespace.as_deref(),
                        &key.name,
                        expected,
                    )?
                    && metadata_uid(&base) == Some(current.uid.as_str())
                    && resource_client_owned_state_equal(&base, &current.data)
                {
                    effective_preconditions.resource_version = Some(current.rv);
                }
                validate_resource_preconditions(
                    &effective_preconditions,
                    Some(&current.uid),
                    current.rv,
                )
                .map_err(|e| {
                    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e.to_string(),
                    )))
                })?;

                let mut patched: Value = current.data.clone();
                let zero_grace_pod_delete =
                    crate::resource_semantics::is_zero_grace_pod_delete_mark_patch(
                        &key.api_version,
                        &key.kind,
                        &patch,
                    );
                let effective_patch = if zero_grace_pod_delete {
                    crate::resource_semantics::pod_delete_mark_patch_without_status(&patch)
                } else {
                    patch
                };
                match patch_kind {
                    PatchKind::Merge => {
                        crate::json_patch::apply_merge_patch(&mut patched, &effective_patch)
                            .map_err(|e| {
                                rusqlite::Error::ToSqlConversionFailure(Box::new(
                                    std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        e.to_string(),
                                    ),
                                ))
                            })?;
                    }
                }
                validate_metadata_uid_immutable(&patched, &current.data).map_err(|e| {
                    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e.to_string(),
                    )))
                })?;
                crate::resource_semantics::preserve_status_subresource_on_main_update(
                    &key.api_version,
                    &key.kind,
                    &current.data,
                    &mut patched,
                );
                if zero_grace_pod_delete {
                    crate::resource_semantics::mark_terminating_pod_unready(&mut patched);
                }
                preserve_server_metadata_fields_from_existing(&mut patched, &current.data);
                ensure_metadata_identity(&mut patched, key.namespace.as_deref(), &key.name);
                ensure_resource_type_meta(&mut patched, &key.api_version, &key.kind);
                let uid = metadata_uid(&patched)
                    .ok_or_else(|| {
                        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "resource metadata.uid is missing",
                        )))
                    })?
                    .to_string();

                if crate::utils::resource_bodies_equal_ignoring_metadata_field(
                    &current.data,
                    &patched,
                    "resourceVersion",
                ) {
                    crate::datastore::diagnostics::log_noop_resource_write(
                        crate::datastore::diagnostics::NoopResourceWrite {
                            operation: "patch_resource_latest",
                            api_version: &key.api_version,
                            kind: &key.kind,
                            namespace: key.namespace.as_deref(),
                            name: &key.name,
                            uid: &current.uid,
                            resource_version: current.rv,
                            reason: "patch result unchanged",
                        },
                    );
                    tx.commit()?;
                    let PatchKey {
                        api_version,
                        kind,
                        name,
                        ..
                    } = key;
                    return Ok(Some(Resource {
                        id: current.id,
                        api_version,
                        kind,
                        namespace: current.namespace,
                        name,
                        uid: current.uid,
                        resource_version: current.rv,
                        data: std::sync::Arc::new(current.data),
                    }));
                }

                let new_rv = Self::next_resource_version_in_tx(&tx)?;
                let data_bytes = serde_json::to_vec(&patched).map_err(|e| {
                    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e.to_string(),
                    )))
                })?;

                if let Some(current_namespace) = current.namespace.clone() {
                    let updated_rows = tx.execute(
                        queries::NAMESPACED_UPDATE_PATCH,
                        rusqlite::params![
                            new_rv,
                            &uid,
                            &data_bytes,
                            &key.api_version,
                            &key.kind,
                            &current_namespace,
                            &key.name,
                            &current.uid,
                        ],
                    )?;
                    if updated_rows == 0 {
                        tx.commit()?;
                        return Ok(None);
                    }
                    selector_index::upsert_index_entries(
                        &tx,
                        &key.api_version,
                        &key.kind,
                        &current_namespace,
                        &key.name,
                        &data_bytes,
                    )?;
                    owner_ref_index::upsert_owner_refs(
                        &tx,
                        &key.api_version,
                        &key.kind,
                        &current_namespace,
                        &key.name,
                        &data_bytes,
                    )?;
                    tx.execute(
                        queries::NAMESPACED_PATCH_WATCH_INSERT,
                        rusqlite::params![
                            &key.api_version,
                            &key.kind,
                            &current_namespace,
                            &key.name,
                            new_rv,
                            &data_bytes,
                        ],
                    )?;
                    tx.commit()?;
                    let PatchKey {
                        api_version,
                        kind,
                        name,
                        ..
                    } = key;
                    return Ok(Some(Resource {
                        id: current.id,
                        api_version,
                        kind,
                        namespace: Some(current_namespace),
                        name,
                        uid,
                        resource_version: new_rv,
                        data: std::sync::Arc::new(patched),
                    }));
                }

                let updated_rows = tx.execute(
                    queries::CLUSTER_UPDATE_PATCH,
                    rusqlite::params![
                        new_rv,
                        &uid,
                        &data_bytes,
                        &key.api_version,
                        &key.kind,
                        &key.name,
                        &current.uid,
                    ],
                )?;
                if updated_rows == 0 {
                    tx.commit()?;
                    return Ok(None);
                }
                selector_index::upsert_index_entries(
                    &tx,
                    &key.api_version,
                    &key.kind,
                    "",
                    &key.name,
                    &data_bytes,
                )?;
                owner_ref_index::upsert_owner_refs(
                    &tx,
                    &key.api_version,
                    &key.kind,
                    "",
                    &key.name,
                    &data_bytes,
                )?;
                tx.execute(
                    queries::CLUSTER_PATCH_WATCH_INSERT,
                    rusqlite::params![&key.api_version, &key.kind, &key.name, new_rv, &data_bytes,],
                )?;
                tx.commit()?;
                let PatchKey {
                    api_version,
                    kind,
                    name,
                    ..
                } = key;
                Ok(Some(Resource {
                    id: current.id,
                    api_version,
                    kind,
                    namespace: None,
                    name,
                    uid,
                    resource_version: new_rv,
                    data: std::sync::Arc::new(patched),
                }))
            })
            .await
            .map_err(|e| anyhow!("Failed to patch resource: {}", e))?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn patch_no_change_branch_preserves_resource_version_and_data() {
        // Pin the no-op detection contract: when the merge patch produces
        // a body equal to the current data, the resourceVersion is
        // preserved (no new RV minted) and the returned data matches the
        // existing row exactly. PatchKey ownership refactor must not
        // alter this behaviour.
        let db = Datastore::new_in_memory().await.unwrap();
        let initial = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "noop-cm", "namespace": "default"},
            "data": {"k": "v"}
        });
        let created = db
            .create_resource("v1", "ConfigMap", Some("default"), "noop-cm", initial)
            .await
            .unwrap();
        let initial_rv = created.resource_version;

        // Apply a merge patch that sets the same value — no-op path.
        let result = db
            .patch_resource_latest(
                "v1",
                "ConfigMap",
                Some("default"),
                "noop-cm",
                PatchKind::Merge,
                serde_json::json!({"data": {"k": "v"}}),
            )
            .await
            .unwrap()
            .expect("resource must exist");

        assert_eq!(
            result.resource_version, initial_rv,
            "no-op patch must not mint a new resourceVersion"
        );
        assert_eq!(result.data["data"]["k"], "v");
        assert_eq!(result.namespace.as_deref(), Some("default"));
        assert_eq!(result.name, "noop-cm");
        assert_eq!(result.api_version, "v1");
        assert_eq!(result.kind, "ConfigMap");
    }

    #[tokio::test]
    async fn patch_missing_resource_returns_none() {
        let db = Datastore::new_in_memory().await.unwrap();
        let result = db
            .patch_resource_latest(
                "v1",
                "ConfigMap",
                Some("default"),
                "missing-cm",
                PatchKind::Merge,
                serde_json::json!({"data": {"k": "v"}}),
            )
            .await
            .unwrap();
        assert!(result.is_none());
    }
}
