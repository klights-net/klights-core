//! Deterministic cluster resource application for log/raft commits.
//!
//! This module owns the resource-row mutation boundary for committed cluster
//! state. Resource table bytes and watch event bytes are derived from the same
//! normalized payload so raft members cannot diverge by recomputing local state
//! while applying the same committed entry.

use super::super::cluster_replace::{ApplyConflictCode, apply_conflict_error, other_error};
use super::super::crud::helpers::{
    WatchEventInsert, insert_watch_event_in_conn, serde_to_sqlite_error,
};
use super::super::{create_pending_watch_event, owner_ref_index, queries, selector_index};
use crate::datastore::types::{PatchKind, PendingWatchEvent};
use crate::log_apply::{LogApplyResourceKey, LogApplyResourcePatch, LogApplyResourceRow};
use rusqlite::OptionalExtension;

pub(in crate::datastore::sqlite) struct ClusterStateApplier<'tx, 'conn> {
    tx: &'tx rusqlite::Transaction<'conn>,
}

struct ResourceWriteSink<'tx, 'conn> {
    tx: &'tx rusqlite::Transaction<'conn>,
}

impl<'tx, 'conn> ResourceWriteSink<'tx, 'conn> {
    fn new(tx: &'tx rusqlite::Transaction<'conn>) -> Self {
        Self { tx }
    }

    fn upsert_resource_from_bytes(
        &self,
        identity: ResourceIdentity<'_>,
        uid: &str,
        resource_version: i64,
        data_bytes: &[u8],
    ) -> tokio_rusqlite::Result<()> {
        match identity.scope {
            ResourceScope::Namespaced(namespace) => {
                self.tx.execute(
                    queries::NAMESPACED_UPSERT_EXACT,
                    rusqlite::params![
                        identity.api_version,
                        identity.kind,
                        namespace,
                        identity.name,
                        uid,
                        resource_version,
                        data_bytes
                    ],
                )?;
            }
            ResourceScope::Cluster => {
                self.tx.execute(
                    queries::CLUSTER_UPSERT_EXACT,
                    rusqlite::params![
                        identity.api_version,
                        identity.kind,
                        identity.name,
                        uid,
                        resource_version,
                        data_bytes
                    ],
                )?;
            }
        }
        Ok(())
    }

    fn upsert_indexes_from_bytes(
        &self,
        identity: ResourceIdentity<'_>,
        data_bytes: &[u8],
    ) -> tokio_rusqlite::Result<()> {
        selector_index::upsert_index_entries(
            self.tx,
            identity.api_version,
            identity.kind,
            identity.index_namespace(),
            identity.name,
            data_bytes,
        )?;
        owner_ref_index::upsert_owner_refs(
            self.tx,
            identity.api_version,
            identity.kind,
            identity.index_namespace(),
            identity.name,
            data_bytes,
        )?;
        Ok(())
    }

    fn delete_resource_from_identity(
        &self,
        identity: ResourceIdentity<'_>,
    ) -> tokio_rusqlite::Result<()> {
        match identity.scope {
            ResourceScope::Namespaced(namespace) => {
                self.tx.execute(
                    queries::NAMESPACED_DELETE_BY_KEY,
                    rusqlite::params![
                        identity.api_version,
                        identity.kind,
                        namespace,
                        identity.name
                    ],
                )?;
            }
            ResourceScope::Cluster => {
                self.tx.execute(
                    queries::CLUSTER_DELETE_BY_KEY,
                    rusqlite::params![identity.api_version, identity.kind, identity.name],
                )?;
            }
        }
        Ok(())
    }

    fn delete_indexes_from_identity(
        &self,
        identity: ResourceIdentity<'_>,
    ) -> tokio_rusqlite::Result<()> {
        selector_index::delete_index_entries(
            self.tx,
            identity.api_version,
            identity.kind,
            identity.index_namespace(),
            identity.name,
        )?;
        owner_ref_index::delete_owner_refs(
            self.tx,
            identity.api_version,
            identity.kind,
            identity.index_namespace(),
            identity.name,
        )?;
        Ok(())
    }

    fn emit_watch_from_bytes(
        &self,
        emit_watch_events: bool,
        identity: ResourceIdentity<'_>,
        resource_version: i64,
        event_type: &str,
        data_bytes: &[u8],
        data: serde_json::Value,
    ) -> tokio_rusqlite::Result<Option<PendingWatchEvent>> {
        if !emit_watch_events {
            return Ok(None);
        }
        insert_watch_event_in_conn(
            self.tx,
            WatchEventInsert::new(
                identity.api_version,
                identity.kind,
                identity.namespace(),
                identity.name,
                resource_version,
                event_type,
                data_bytes,
            ),
        )?;
        Ok(Some(create_pending_watch_event(
            identity.api_version,
            identity.kind,
            identity.namespace(),
            identity.name,
            resource_version,
            event_type,
            data,
        )))
    }
}

impl<'tx, 'conn> ClusterStateApplier<'tx, 'conn> {
    pub(in crate::datastore::sqlite) fn new(tx: &'tx rusqlite::Transaction<'conn>) -> Self {
        Self { tx }
    }

    pub(in crate::datastore::sqlite) fn apply_put_resource(
        &self,
        mut row: LogApplyResourceRow,
        emit_watch_events: bool,
    ) -> tokio_rusqlite::Result<Option<PendingWatchEvent>> {
        let mut namespace_owned = String::new();
        let sink = ResourceWriteSink::new(self.tx);
        let existing = {
            let identity = resource_identity(
                &row.api_version,
                &row.kind,
                row.namespace.as_deref(),
                &row.name,
                &mut namespace_owned,
            );
            self.get_existing_resource(identity)?
        };
        validate_put_resource_apply_preconditions(&row, existing.as_ref())?;
        normalize_committed_resource_for_apply(&mut row, existing.as_ref())?;
        let identity = resource_identity(
            &row.api_version,
            &row.kind,
            row.namespace.as_deref(),
            &row.name,
            &mut namespace_owned,
        );
        let data_bytes = serde_json::to_vec(&row.data)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        let same_resource_version_and_body =
            existing.as_ref().is_some_and(|(rv, _uid, existing_bytes)| {
                *rv == row.resource_version && *existing_bytes == data_bytes
            });
        let event_type = match klights_cluster_core::decide_resource_put(
            existing.is_some(),
            same_resource_version_and_body,
        ) {
            klights_cluster_core::ResourceWriteDecision::NoOp => {
                sink.upsert_indexes_from_bytes(identity, &data_bytes)?;
                return Ok(None);
            }
            klights_cluster_core::ResourceWriteDecision::Write(event_type) => event_type,
        };
        sink.upsert_resource_from_bytes(identity, &row.uid, row.resource_version, &data_bytes)?;
        sink.upsert_indexes_from_bytes(identity, &data_bytes)?;
        sink.emit_watch_from_bytes(
            emit_watch_events,
            identity,
            row.resource_version,
            event_type.as_str(),
            &data_bytes,
            row.data,
        )
    }

    pub(in crate::datastore::sqlite) fn apply_patch_resource_latest(
        &self,
        patch: LogApplyResourcePatch,
        emit_watch_events: bool,
    ) -> tokio_rusqlite::Result<Option<PendingWatchEvent>> {
        let mut namespace_owned = String::new();
        let sink = ResourceWriteSink::new(self.tx);
        let identity = resource_identity(
            &patch.api_version,
            &patch.kind,
            patch.namespace.as_deref(),
            &patch.name,
            &mut namespace_owned,
        );
        let existing = self.get_existing_resource(identity)?;
        let Some((current_rv, current_uid, current_bytes)) = existing else {
            if patch.require_existing {
                return Err(apply_conflict_error(
                    ApplyConflictCode::NotFound,
                    "Resource not found (404 Not Found)",
                ));
            }
            return Ok(None);
        };
        let patched_data = apply_latest_patch_to_current_resource(
            &patch,
            current_rv,
            &current_uid,
            &current_bytes,
            identity.namespace(),
        )?;
        let data_bytes = serde_json::to_vec(&patched_data)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        sink.upsert_resource_from_bytes(
            identity,
            &current_uid,
            patch.resource_version,
            &data_bytes,
        )?;
        sink.upsert_indexes_from_bytes(identity, &data_bytes)?;
        sink.emit_watch_from_bytes(
            emit_watch_events,
            identity,
            patch.resource_version,
            "MODIFIED",
            &data_bytes,
            patched_data,
        )
    }

    pub(in crate::datastore::sqlite) fn apply_delete_resource(
        &self,
        resource_version: i64,
        key: LogApplyResourceKey,
        emit_watch_events: bool,
    ) -> tokio_rusqlite::Result<Option<PendingWatchEvent>> {
        let mut namespace_owned = String::new();
        let sink = ResourceWriteSink::new(self.tx);
        let identity = resource_identity(
            &key.api_version,
            &key.kind,
            key.namespace.as_deref(),
            &key.name,
            &mut namespace_owned,
        );
        let existing = self.get_existing_resource(identity)?;
        let Some((current_rv, current_uid, data_bytes)) = existing else {
            return Ok(None);
        };
        let requested_uid = (!key.uid.is_empty()).then_some(key.uid.as_str());
        let event_type = klights_cluster_core::decide_resource_delete(
            requested_uid,
            key.precondition_resource_version,
            Some(klights_cluster_core::CurrentResourceState {
                uid: Some(current_uid.as_str()),
                resource_version: current_rv,
            }),
        )
        .map_err(apply_precondition_error)?;
        let klights_cluster_core::ResourceDeleteDecision::Delete(event_type) = event_type else {
            return Ok(None);
        };
        sink.delete_resource_from_identity(identity)?;
        sink.delete_indexes_from_identity(identity)?;
        let data: serde_json::Value =
            serde_json::from_slice(&data_bytes).map_err(serde_to_sqlite_error)?;
        sink.emit_watch_from_bytes(
            emit_watch_events,
            identity,
            resource_version,
            event_type.as_str(),
            &data_bytes,
            data,
        )
    }

    fn get_existing_resource(
        &self,
        identity: ResourceIdentity<'_>,
    ) -> tokio_rusqlite::Result<Option<ExistingResourceRow>> {
        match identity.scope {
            ResourceScope::Namespaced(namespace) => self
                .tx
                .query_row(
                    queries::NAMESPACED_GET,
                    rusqlite::params![
                        identity.api_version,
                        identity.kind,
                        namespace,
                        identity.name
                    ],
                    |db_row| {
                        Ok((
                            db_row.get::<_, i64>(5)?,
                            db_row.get::<_, String>(6)?,
                            db_row.get::<_, Vec<u8>>(7)?,
                        ))
                    },
                )
                .optional()
                .map_err(tokio_rusqlite::Error::from),
            ResourceScope::Cluster => self
                .tx
                .query_row(
                    queries::CLUSTER_GET,
                    rusqlite::params![identity.api_version, identity.kind, identity.name],
                    |db_row| {
                        Ok((
                            db_row.get::<_, i64>(4)?,
                            db_row.get::<_, String>(5)?,
                            db_row.get::<_, Vec<u8>>(6)?,
                        ))
                    },
                )
                .optional()
                .map_err(tokio_rusqlite::Error::from),
        }
    }
}

type ExistingResourceRow = (i64, String, Vec<u8>);

#[derive(Clone, Copy)]
struct ResourceIdentity<'a> {
    api_version: &'a str,
    kind: &'a str,
    scope: ResourceScope<'a>,
    name: &'a str,
}

impl<'a> ResourceIdentity<'a> {
    fn namespace(self) -> Option<&'a str> {
        self.scope.namespace()
    }

    fn index_namespace(self) -> &'a str {
        self.scope.index_namespace()
    }
}

#[derive(Clone, Copy)]
enum ResourceScope<'a> {
    Namespaced(&'a str),
    Cluster,
}

impl<'a> ResourceScope<'a> {
    fn namespace(self) -> Option<&'a str> {
        match self {
            Self::Namespaced(namespace) => Some(namespace),
            Self::Cluster => None,
        }
    }

    fn index_namespace(self) -> &'a str {
        match self {
            Self::Namespaced(namespace) => namespace,
            Self::Cluster => "",
        }
    }
}

fn resource_identity<'a>(
    api_version: &'a str,
    kind: &'a str,
    namespace: Option<&str>,
    name: &'a str,
    namespace_owned: &'a mut String,
) -> ResourceIdentity<'a> {
    if super::super::use_namespaced_table(api_version, kind, &namespace) {
        *namespace_owned = namespace.unwrap_or("default").to_string();
        ResourceIdentity {
            api_version,
            kind,
            scope: ResourceScope::Namespaced(namespace_owned.as_str()),
            name,
        }
    } else {
        ResourceIdentity {
            api_version,
            kind,
            scope: ResourceScope::Cluster,
            name,
        }
    }
}

fn normalize_committed_resource_for_apply(
    row: &mut LogApplyResourceRow,
    existing: Option<&ExistingResourceRow>,
) -> tokio_rusqlite::Result<()> {
    merge_status_only_row_with_existing(row, existing)?;
    preserve_newer_same_uid_row_on_stale_committed_put(row, existing)?;
    preserve_same_uid_server_metadata_from_existing(row, existing)?;
    Ok(())
}

fn apply_latest_patch_to_current_resource(
    patch: &LogApplyResourcePatch,
    current_rv: i64,
    current_uid: &str,
    current_bytes: &[u8],
    namespace: Option<&str>,
) -> tokio_rusqlite::Result<serde_json::Value> {
    klights_cluster_core::validate_apply_preconditions(
        klights_cluster_core::ApplyPreconditions {
            uid: patch.precondition_uid.as_deref(),
            resource_version: patch.precondition_resource_version,
            ..klights_cluster_core::ApplyPreconditions::default()
        },
        Some(klights_cluster_core::CurrentResourceState {
            uid: Some(current_uid),
            resource_version: current_rv,
        }),
    )
    .map_err(apply_precondition_error)?;
    let current: serde_json::Value =
        serde_json::from_slice(current_bytes).map_err(serde_to_sqlite_error)?;
    let mut patched = current.clone();
    let zero_grace_pod_delete = klights_types::is_zero_grace_pod_delete_mark_patch(
        &patch.api_version,
        &patch.kind,
        &patch.patch,
    );
    let effective_patch = if zero_grace_pod_delete {
        klights_types::pod_delete_mark_patch_without_status(&patch.patch)
    } else {
        patch.patch.clone()
    };
    match patch.patch_kind {
        PatchKind::Merge => {
            klights_types::apply_merge_patch(&mut patched, &effective_patch);
        }
    }
    if zero_grace_pod_delete {
        let transition_time = patch
            .terminating_pod_unready_timestamp
            .as_deref()
            .or_else(|| deterministic_terminating_unready_timestamp(&patched, Some(&current)))
            .unwrap_or("1970-01-01T00:00:00Z")
            .to_string();
        klights_types::mark_terminating_pod_unready_at(&mut patched, &transition_time);
    }
    crate::datastore::sqlite::resource_shape::validate_metadata_uid_immutable(&patched, &current)
        .map_err(|err| {
        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            err.to_string(),
        )))
    })?;
    crate::datastore::sqlite::resource_shape::ensure_metadata_identity(
        &mut patched,
        namespace,
        &patch.name,
    );
    crate::datastore::sqlite::resource_shape::preserve_server_metadata_fields_from_existing(
        &mut patched,
        &current,
    );
    crate::datastore::sqlite::resource_shape::ensure_resource_type_meta(
        &mut patched,
        &patch.api_version,
        &patch.kind,
    );
    if crate::datastore::sqlite::resource_shape::metadata_uid(&patched).is_none()
        && let Some(metadata) = patched
            .get_mut("metadata")
            .and_then(serde_json::Value::as_object_mut)
    {
        metadata.insert(
            "uid".to_string(),
            serde_json::Value::String(current_uid.to_string()),
        );
    }
    patched = crate::datastore::sqlite::resource_shape::hydrate_watch_event_data(
        patched,
        &patch.api_version,
        &patch.kind,
        namespace,
        &patch.name,
        patch.resource_version,
    );
    crate::datastore::sqlite::resource_shape::ensure_pod_status_ip_arrays(
        &mut patched,
        &patch.api_version,
        &patch.kind,
    );
    Ok(patched)
}

fn merge_status_only_row_with_existing(
    row: &mut LogApplyResourceRow,
    existing: Option<&ExistingResourceRow>,
) -> tokio_rusqlite::Result<()> {
    if !row.status_only {
        return Ok(());
    }
    let Some((current_rv, current_uid, existing_bytes)) = existing else {
        return Ok(());
    };
    let status = row
        .data
        .get("status")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
    let mut status = status;
    let mut live: serde_json::Value =
        serde_json::from_slice(existing_bytes).map_err(serde_to_sqlite_error)?;
    let freshness = if row
        .precondition_resource_version
        .is_some_and(|expected_rv| expected_rv < *current_rv)
    {
        crate::datastore::status_merge_policy::StatusApplyFreshness::Stale
    } else {
        crate::datastore::status_merge_policy::StatusApplyFreshness::Fresh
    };
    crate::datastore::status_merge_policy::merge_status_for_apply(
        &row.api_version,
        &row.kind,
        &live,
        &mut status,
        freshness,
        crate::datastore::status_merge_policy::StatusApplyOrigin::ReplicatedApply,
    );
    let Some(live_obj) = live.as_object_mut() else {
        return Err(other_error(
            "status-only log_apply target is not a JSON object",
        ));
    };
    live_obj.insert("status".to_string(), status);
    live = crate::datastore::sqlite::resource_shape::hydrate_watch_event_data(
        live,
        &row.api_version,
        &row.kind,
        row.namespace.as_deref(),
        &row.name,
        row.resource_version,
    );
    crate::datastore::sqlite::resource_shape::ensure_pod_status_ip_arrays(
        &mut live,
        &row.api_version,
        &row.kind,
    );
    if freshness == crate::datastore::status_merge_policy::StatusApplyFreshness::Stale
        && row.precondition_uid.as_deref() == Some(current_uid.as_str())
    {
        row.precondition_resource_version = None;
    }
    row.uid = current_uid.clone();
    row.data = live;
    Ok(())
}

fn preserve_newer_same_uid_row_on_stale_committed_put(
    row: &mut LogApplyResourceRow,
    existing: Option<&ExistingResourceRow>,
) -> tokio_rusqlite::Result<()> {
    if row.status_only {
        return Ok(());
    }
    let Some(expected_rv) = row.precondition_resource_version else {
        return Ok(());
    };
    let Some((current_rv, current_uid, existing_bytes)) = existing else {
        return Ok(());
    };
    if expected_rv >= *current_rv {
        return Ok(());
    }

    let fallback_uid = if row.uid.is_empty() {
        None
    } else {
        Some(row.uid.as_str())
    };
    let incoming_uid =
        crate::datastore::sqlite::resource_shape::metadata_uid(&row.data).or(fallback_uid);
    if incoming_uid != Some(current_uid.as_str()) {
        return Ok(());
    }

    let mut existing_data: serde_json::Value =
        serde_json::from_slice(existing_bytes).map_err(serde_to_sqlite_error)?;
    if existing_data
        .pointer("/metadata/deletionTimestamp")
        .filter(|value| !value.is_null())
        .filter(|value| {
            value
                .as_str()
                .is_none_or(|timestamp| !timestamp.trim().is_empty())
        })
        .is_some()
    {
        return Ok(());
    }

    if let (Some(existing_generation), Some(incoming_generation)) = (
        metadata_generation(&existing_data),
        metadata_generation(&row.data),
    ) && incoming_generation >= existing_generation
    {
        return Ok(());
    }

    existing_data = crate::datastore::sqlite::resource_shape::hydrate_watch_event_data(
        existing_data,
        &row.api_version,
        &row.kind,
        row.namespace.as_deref(),
        &row.name,
        row.resource_version,
    );
    crate::datastore::sqlite::resource_shape::ensure_pod_status_ip_arrays(
        &mut existing_data,
        &row.api_version,
        &row.kind,
    );
    row.uid = current_uid.clone();
    row.data = existing_data;
    Ok(())
}

fn metadata_generation(data: &serde_json::Value) -> Option<i64> {
    data.pointer("/metadata/generation")
        .and_then(|value| value.as_i64())
}

fn preserve_same_uid_server_metadata_from_existing(
    row: &mut LogApplyResourceRow,
    existing: Option<&ExistingResourceRow>,
) -> tokio_rusqlite::Result<()> {
    let Some((current_rv, current_uid, existing_bytes)) = existing else {
        return Ok(());
    };
    let fallback_uid = if row.uid.is_empty() {
        None
    } else {
        Some(row.uid.as_str())
    };
    let incoming_uid =
        crate::datastore::sqlite::resource_shape::metadata_uid(&row.data).or(fallback_uid);
    if incoming_uid != Some(current_uid.as_str()) {
        return Ok(());
    }

    let existing_data: serde_json::Value =
        serde_json::from_slice(existing_bytes).map_err(serde_to_sqlite_error)?;
    crate::datastore::sqlite::resource_shape::preserve_server_metadata_fields_from_existing(
        &mut row.data,
        &existing_data,
    );
    if row
        .precondition_resource_version
        .is_some_and(|expected| expected != *current_rv)
    {
        crate::datastore::stale_apply_policy::apply_same_uid_stale_full_resource_policy(
            &row.api_version,
            &row.kind,
            &mut row.data,
            &existing_data,
        );
    }
    if row.api_version == "v1"
        && row.kind == "Pod"
        && existing_data
            .pointer("/metadata/deletionTimestamp")
            .filter(|value| !value.is_null())
            .filter(|value| {
                value
                    .as_str()
                    .is_none_or(|timestamp| !timestamp.trim().is_empty())
            })
            .is_some()
    {
        let transition_time =
            deterministic_terminating_unready_timestamp(&row.data, Some(&existing_data))
                .unwrap_or("1970-01-01T00:00:00Z")
                .to_string();
        klights_types::mark_terminating_pod_unready_at(&mut row.data, &transition_time);
    }
    Ok(())
}

fn deterministic_terminating_unready_timestamp<'a>(
    data: &'a serde_json::Value,
    existing: Option<&'a serde_json::Value>,
) -> Option<&'a str> {
    data.pointer("/metadata/deletionTimestamp")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            existing
                .and_then(|value| value.pointer("/metadata/deletionTimestamp"))
                .and_then(|value| value.as_str())
                .filter(|value| !value.trim().is_empty())
        })
        .or_else(|| pod_terminating_condition_time(data))
        .or_else(|| existing.and_then(pod_terminating_condition_time))
}

fn pod_terminating_condition_time(data: &serde_json::Value) -> Option<&str> {
    data.pointer("/status/conditions")
        .and_then(|value| value.as_array())
        .and_then(|conditions| {
            conditions.iter().find_map(|condition| {
                let condition_type = condition.get("type").and_then(|value| value.as_str());
                let is_readiness =
                    matches!(condition_type, Some("Ready") | Some("ContainersReady"));
                let is_false =
                    condition.get("status").and_then(|value| value.as_str()) == Some("False");
                let is_terminating = condition.get("reason").and_then(|value| value.as_str())
                    == Some("PodTerminating");
                (is_readiness && is_false && is_terminating)
                    .then(|| {
                        condition
                            .get("lastTransitionTime")
                            .and_then(|value| value.as_str())
                    })
                    .flatten()
            })
        })
}

fn validate_put_resource_apply_preconditions(
    row: &LogApplyResourceRow,
    existing: Option<&ExistingResourceRow>,
) -> tokio_rusqlite::Result<()> {
    klights_cluster_core::validate_apply_preconditions(
        klights_cluster_core::ApplyPreconditions {
            require_absent: row.require_absent,
            require_existing: row.require_existing,
            uid: row.precondition_uid.as_deref(),
            resource_version: row.precondition_resource_version,
        },
        existing.map(
            |(resource_version, uid, _)| klights_cluster_core::CurrentResourceState {
                uid: Some(uid.as_str()),
                resource_version: *resource_version,
            },
        ),
    )
    .map_err(apply_precondition_error)
}

fn apply_precondition_error(
    violation: klights_cluster_core::ApplyPreconditionViolation,
) -> tokio_rusqlite::Error {
    match violation {
        klights_cluster_core::ApplyPreconditionViolation::AlreadyExists => apply_conflict_error(
            ApplyConflictCode::AlreadyExists,
            "Resource already exists (409 Conflict)",
        ),
        klights_cluster_core::ApplyPreconditionViolation::NotFound => apply_conflict_error(
            ApplyConflictCode::NotFound,
            "Resource not found (404 Not Found)",
        ),
        klights_cluster_core::ApplyPreconditionViolation::Uid { .. } => apply_conflict_error(
            ApplyConflictCode::UidPrecondition,
            "UID precondition failed (409 Conflict)",
        ),
        klights_cluster_core::ApplyPreconditionViolation::ResourceVersion { expected, actual } => {
            apply_conflict_error(
                ApplyConflictCode::ResourceVersionPrecondition,
                format!(
                    "resourceVersion precondition failed: expected {expected} got {actual} (409 Conflict)"
                ),
            )
        }
    }
}
