//! Resource update and status-subresource writes — public update path with
//! precondition validation, deduplication, and status-only atomic updates.

use anyhow::Context;

use super::super::ordinary;
use super::mutation_helpers::*;
use super::*;

use super::super::{create_staged_post_commit, mutation_diagnostics};

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
    pub async fn mark_resource_for_deletion_without_watch(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<Option<Resource>> {
        let av = api_version.to_string();
        let k = kind.to_string();
        let n = name.to_string();
        let expected_rv = preconditions.resource_version;
        let expected_uid = preconditions.uid;
        let deletion_timestamp =
            klights_cluster_core::k8s_time::format_legacy_timestamp(self.wall_clock.now_utc());

        let namespace_owned = namespace.map(str::to_string);
        let mark_outcome = self
            .db_call("db_query", move |conn| {
                ordinary::mark_resource_for_deletion_in_conn(
                    conn,
                    ordinary::MarkResourceForDeletionInput {
                        api_version: av,
                        kind: k,
                        namespace: namespace_owned,
                        name: n,
                        expected_resource_version: expected_rv,
                        expected_uid,
                        grace_seconds,
                        deletion_timestamp,
                    },
                )
            })
            .await;

        let (resource_version, data) = match mark_outcome {
            Ok(Some(resource_data)) => resource_data,
            Ok(None) => return Ok(None),
            Err(tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows)) => {
                return Err(crate::errors::DatastoreError::conflict(
                    "Resource not found or version conflict",
                )
                .into());
            }
            Err(err) => return Err(anyhow!("Failed to mark resource for delete: {err}")),
        };

        Ok(Some(Resource {
            id: 0,
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            namespace: namespace.map(str::to_string),
            name: name.to_string(),
            uid: Resource::uid_from_data(&serde_json::from_slice::<Value>(&data)?),
            resource_version,
            data: std::sync::Arc::new(
                serde_json::from_slice(&data).context("deserialize marked delete payload")?,
            ),
        }))
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
            || !klights_types::has_builtin_status_subresource(api_version, kind)
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
                ResourceListOptions::new(None, Some(&field_selector), Some(1), None),
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
        if api_version == "v1" && kind == "Namespace" {
            if namespace.is_some() {
                return Err(anyhow!("Namespace is cluster-scoped"));
            }
            let Some(existing) = self.get_namespace(name).await? else {
                return Err(crate::errors::DatastoreError::not_found(format!(
                    "Namespace {name} not found"
                ))
                .into());
            };
            validate_metadata_uid_immutable(&data, existing.data.as_ref())?;
            validate_resource_preconditions(
                &preconditions,
                Some(existing.uid.as_str()),
                existing.resource_version,
            )?;
            preserve_server_metadata_fields_from_existing(&mut data, existing.data.as_ref());
            return self
                .update_namespace(name, data, existing.resource_version)
                .await;
        }
        #[cfg(any(test, feature = "test-support"))]
        self.pause_resource_mutation_if_requested(
            ResourceMutationPauseOperation::MainUpdate,
            api_version,
            kind,
            namespace,
            name,
        )
        .await;
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
                mutation_diagnostics::log_noop_resource_write(
                    mutation_diagnostics::NoopResourceWrite {
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

        let namespace_owned = namespace.map(str::to_string);
        let uid_for_update = uid.clone();
        let result = self
            .db_call("db_query", move |conn| {
                ordinary::update_resource_in_conn(
                    conn,
                    ordinary::UpdateResourceInput {
                        api_version: av,
                        kind: k,
                        namespace: namespace_owned,
                        name: n,
                        uid: uid_for_update,
                        data,
                        expected_resource_version: expected_rv,
                        expected_uid,
                        preserve_latest_status,
                    },
                )
            })
            .await;

        match result {
            Ok((id, new_rv, data)) => {
                let _pending = create_staged_post_commit(
                    api_version,
                    kind,
                    namespace,
                    name,
                    new_rv,
                    "MODIFIED",
                    data.clone(),
                );
                #[cfg(any(test, feature = "test-support"))]
                self.publish_watch_event(_pending);

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
                Err(crate::errors::DatastoreError::conflict(
                    "Resource not found or version conflict",
                )
                .into())
            }
            Err(e) => Err(anyhow!("Failed to update resource: {}", e)),
        }
    }
}
