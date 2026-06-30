//! Deterministic cluster resource application for log/raft commits.
//!
//! This module owns the resource-row mutation boundary for committed cluster
//! state. Resource table bytes and watch event bytes are derived from the same
//! normalized payload so raft members cannot diverge by recomputing local state
//! while applying the same committed entry.

use super::super::cluster_replace::{
    ApplyConflictCode, apply_conflict_error, other_error, record_raft_authoritative_apply_conflict,
};
use super::super::crud::helpers::{
    WatchEventInsert, insert_watch_event_in_conn, serde_to_sqlite_error,
};
use super::super::{create_pending_watch_event, owner_ref_index, queries, selector_index};
use crate::datastore::types::{PatchKind, PendingWatchEvent};
use crate::log_apply::{LogApplyResourceKey, LogApplyResourcePatch, LogApplyResourceRow};
use rusqlite::OptionalExtension;
use std::collections::HashSet;

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
        raft_authoritative: bool,
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
        let normalized_before_validation = row.status_only;
        if normalized_before_validation {
            normalize_committed_resource_for_apply(&mut row, existing.as_ref())?;
        }
        if raft_authoritative {
            validate_put_resource_presence_preconditions(&row, existing.as_ref())?;
            log_raft_put_conflict_if_any(&row, existing.as_ref());
        } else {
            validate_put_resource_apply_preconditions(&row, existing.as_ref())?;
        }
        if !normalized_before_validation {
            normalize_committed_resource_for_apply(&mut row, existing.as_ref())?;
        }
        let identity = resource_identity(
            &row.api_version,
            &row.kind,
            row.namespace.as_deref(),
            &row.name,
            &mut namespace_owned,
        );
        let data_bytes = serde_json::to_vec(&row.data)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        if existing.as_ref().is_some_and(|(rv, _uid, existing_bytes)| {
            *rv == row.resource_version && *existing_bytes == data_bytes
        }) {
            sink.upsert_indexes_from_bytes(identity, &data_bytes)?;
            return Ok(None);
        }
        sink.upsert_resource_from_bytes(identity, &row.uid, row.resource_version, &data_bytes)?;
        sink.upsert_indexes_from_bytes(identity, &data_bytes)?;
        let event_type = if existing.is_some() {
            "MODIFIED"
        } else {
            "ADDED"
        };
        sink.emit_watch_from_bytes(
            emit_watch_events,
            identity,
            row.resource_version,
            event_type,
            &data_bytes,
            row.data,
        )
    }

    pub(in crate::datastore::sqlite) fn apply_patch_resource_latest(
        &self,
        patch: LogApplyResourcePatch,
        emit_watch_events: bool,
        raft_authoritative: bool,
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
            raft_authoritative,
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
        raft_authoritative: bool,
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
        if !key.uid.is_empty() && key.uid.as_str() != current_uid.as_str() {
            return Ok(None);
        }
        if let Some(expected_rv) = key.precondition_resource_version
            && expected_rv != current_rv
        {
            if raft_authoritative {
                record_raft_authoritative_apply_conflict();
                tracing::warn!(
                    api_version = %key.api_version, kind = %key.kind,
                    namespace = ?key.namespace, name = %key.name,
                    local_rv = %current_rv, committed_rv = %expected_rv,
                    "raft authoritative DELETE: rv precondition bypassed, removing stale row"
                );
            } else {
                return Err(apply_conflict_error(
                    ApplyConflictCode::ResourceVersionPrecondition,
                    format!(
                        "resourceVersion precondition failed: expected {expected_rv} got {current_rv} (409 Conflict)"
                    ),
                ));
            }
        }
        sink.delete_resource_from_identity(identity)?;
        sink.delete_indexes_from_identity(identity)?;
        let data: serde_json::Value =
            serde_json::from_slice(&data_bytes).map_err(serde_to_sqlite_error)?;
        sink.emit_watch_from_bytes(
            emit_watch_events,
            identity,
            resource_version,
            "DELETED",
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
    raft_authoritative: bool,
) -> tokio_rusqlite::Result<serde_json::Value> {
    if let Some(expected_uid) = patch.precondition_uid.as_deref()
        && expected_uid != current_uid
    {
        if raft_authoritative {
            record_raft_authoritative_apply_conflict();
            tracing::warn!(
                api_version = %patch.api_version, kind = %patch.kind,
                namespace = ?patch.namespace, name = %patch.name,
                local_uid = %current_uid, committed_uid = %expected_uid,
                "raft authoritative PATCH: uid precondition bypassed, applying patch to current row"
            );
        } else {
            return Err(apply_conflict_error(
                ApplyConflictCode::UidPrecondition,
                "UID precondition failed (409 Conflict)",
            ));
        }
    }
    if let Some(expected_rv) = patch.precondition_resource_version
        && expected_rv != current_rv
    {
        if raft_authoritative {
            record_raft_authoritative_apply_conflict();
            tracing::warn!(
                api_version = %patch.api_version, kind = %patch.kind,
                namespace = ?patch.namespace, name = %patch.name,
                local_rv = %current_rv, committed_rv = %expected_rv,
                "raft authoritative PATCH: rv precondition bypassed, applying patch to current row"
            );
        } else {
            return Err(apply_conflict_error(
                ApplyConflictCode::ResourceVersionPrecondition,
                format!(
                    "resourceVersion precondition failed: expected {expected_rv} got {current_rv} (409 Conflict)"
                ),
            ));
        }
    }
    let current: serde_json::Value =
        serde_json::from_slice(current_bytes).map_err(serde_to_sqlite_error)?;
    let mut patched = current.clone();
    let zero_grace_pod_delete = crate::resource_semantics::is_zero_grace_pod_delete_mark_patch(
        &patch.api_version,
        &patch.kind,
        &patch.patch,
    );
    let effective_patch = if zero_grace_pod_delete {
        crate::resource_semantics::pod_delete_mark_patch_without_status(&patch.patch)
    } else {
        patch.patch.clone()
    };
    match patch.patch_kind {
        PatchKind::Merge => {
            crate::json_patch::apply_merge_patch(&mut patched, &effective_patch).map_err(
                |err| {
                    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        err.to_string(),
                    )))
                },
            )?;
        }
    }
    if zero_grace_pod_delete {
        let transition_time = patch
            .terminating_pod_unready_timestamp
            .as_deref()
            .or_else(|| deterministic_terminating_unready_timestamp(&patched, Some(&current)))
            .unwrap_or("1970-01-01T00:00:00Z")
            .to_string();
        crate::resource_semantics::mark_terminating_pod_unready_at(&mut patched, &transition_time);
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
        preserve_live_pod_node_for_stale_put(row, &existing_data);
        preserve_live_owner_refs_for_stale_pod_put(row, &existing_data);
        preserve_finalizers_from_existing(&mut row.data, &existing_data);
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
        crate::resource_semantics::mark_terminating_pod_unready_at(&mut row.data, &transition_time);
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

fn preserve_live_pod_node_for_stale_put(
    row: &mut LogApplyResourceRow,
    existing: &serde_json::Value,
) {
    if row.api_version != "v1" || row.kind != "Pod" {
        return;
    }
    let Some(existing_node) = existing
        .pointer("/spec/nodeName")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
    else {
        return;
    };
    let incoming_node = row
        .data
        .pointer("/spec/nodeName")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty());
    if incoming_node == Some(existing_node.as_str()) {
        return;
    }
    let Some(object) = row.data.as_object_mut() else {
        return;
    };
    let spec = object
        .entry("spec".to_string())
        .or_insert_with(|| serde_json::Value::Object(Default::default()));
    let Some(spec) = spec.as_object_mut() else {
        return;
    };
    spec.insert(
        "nodeName".to_string(),
        serde_json::Value::String(existing_node),
    );
}

fn preserve_live_owner_refs_for_stale_pod_put(
    row: &mut LogApplyResourceRow,
    existing: &serde_json::Value,
) {
    if row.api_version != "v1" || row.kind != "Pod" {
        return;
    }
    let Some(existing_owner_refs) = existing
        .pointer("/metadata/ownerReferences")
        .and_then(|value| value.as_array())
        .filter(|refs| !refs.is_empty())
    else {
        return;
    };
    let incoming_owner_refs = row
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|value| value.as_array())
        .map(|refs| refs.to_vec());
    let Some(metadata) = row
        .data
        .get_mut("metadata")
        .and_then(|value| value.as_object_mut())
    else {
        tracing::trace!(
            api_version = %row.api_version,
            kind = %row.kind,
            namespace = ?row.namespace,
            name = %row.name,
            existing_count = existing_owner_refs.len(),
            "pod stale PUT owner reference preservation skipped: metadata block missing"
        );
        return;
    };

    let incoming_owner_refs = match incoming_owner_refs {
        None => {
            tracing::debug!(
                api_version = %row.api_version,
                kind = %row.kind,
                namespace = ?row.namespace,
                name = %row.name,
                incoming_count = 0,
                existing_count = existing_owner_refs.len(),
                merged_count = existing_owner_refs.len(),
                incoming_uids = "missing",
                existing_uids = ?format_uids(existing_owner_refs),
                merged_uids = ?format_uids(existing_owner_refs),
                "stale Pod PUT retains missing ownerReferences from live row"
            );
            metadata.insert(
                "ownerReferences".to_string(),
                serde_json::Value::Array(existing_owner_refs.to_vec()),
            );
            return;
        }
        Some(incoming_owner_refs) if incoming_owner_refs.is_empty() => {
            tracing::debug!(
                api_version = %row.api_version,
                kind = %row.kind,
                namespace = ?row.namespace,
                name = %row.name,
                incoming_count = 0,
                existing_count = existing_owner_refs.len(),
                merged_count = 0,
                incoming_uids = "explicit-empty",
                existing_uids = ?format_uids(existing_owner_refs),
                merged_uids = "cleared",
                "stale Pod PUT explicit empty ownerReferences clears live owner references"
            );
            return;
        }
        Some(incoming_owner_refs) => incoming_owner_refs,
    };

    let incoming_count = incoming_owner_refs.len();
    let incoming_uids = format_uids(&incoming_owner_refs);
    let mut incoming_identities = HashSet::with_capacity(incoming_owner_refs.len());
    for owner_ref in incoming_owner_refs.iter() {
        if let Some(identity) = owner_reference_identity(owner_ref) {
            incoming_identities.insert(identity);
        }
    }

    let mut merged_owner_refs = incoming_owner_refs;
    for owner_ref in existing_owner_refs.iter() {
        if let Some(identity) = owner_reference_identity(owner_ref)
            && !incoming_identities.contains(&identity)
        {
            incoming_identities.insert(identity);
            merged_owner_refs.push(owner_ref.clone());
        }
    }

    tracing::debug!(
        api_version = %row.api_version,
        kind = %row.kind,
        namespace = ?row.namespace,
        name = %row.name,
        incoming_count = incoming_count,
        existing_count = existing_owner_refs.len(),
        merged_count = merged_owner_refs.len(),
        incoming_uids = incoming_uids.as_str(),
        existing_uids = ?format_uids(existing_owner_refs),
        merged_uids = ?format_uids(&merged_owner_refs),
        "stale Pod PUT merges missing live ownerReferences into stale incoming ownerReferences"
    );
    metadata.insert(
        "ownerReferences".to_string(),
        serde_json::Value::Array(merged_owner_refs),
    );
}

#[derive(Hash, Eq, PartialEq)]
enum OwnerReferenceIdentity {
    Uid(String),
    ApiKindName(String, String, String),
}

fn owner_reference_identity(owner_ref: &serde_json::Value) -> Option<OwnerReferenceIdentity> {
    if let Some(uid) = owner_ref
        .get("uid")
        .and_then(|value| value.as_str())
        .filter(|uid| !uid.trim().is_empty())
    {
        return Some(OwnerReferenceIdentity::Uid(uid.to_string()));
    }

    let api_version = owner_ref
        .get("apiVersion")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())?;
    let kind = owner_ref
        .get("kind")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())?;
    let name = owner_ref
        .get("name")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())?;
    Some(OwnerReferenceIdentity::ApiKindName(
        api_version.to_string(),
        kind.to_string(),
        name.to_string(),
    ))
}

fn format_uids(owner_references: &[serde_json::Value]) -> String {
    let mut uids = Vec::with_capacity(owner_references.len());
    for owner_ref in owner_references {
        if let Some(uid) = owner_ref.get("uid").and_then(|value| value.as_str()) {
            uids.push(uid);
        }
    }
    if uids.is_empty() {
        "none".to_string()
    } else {
        uids.join(",")
    }
}

fn preserve_finalizers_from_existing(data: &mut serde_json::Value, existing: &serde_json::Value) {
    let Some(existing_finalizers) = existing
        .pointer("/metadata/finalizers")
        .and_then(|value| value.as_array())
        .filter(|finalizers| !finalizers.is_empty())
    else {
        return;
    };
    let Some(metadata) = data
        .get_mut("metadata")
        .and_then(|value| value.as_object_mut())
    else {
        return;
    };
    let mut merged = metadata
        .get("finalizers")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    for finalizer in existing_finalizers {
        if !merged.iter().any(|value| value == finalizer) {
            merged.push(finalizer.clone());
        }
    }
    metadata.insert("finalizers".to_string(), serde_json::Value::Array(merged));
}

fn validate_put_resource_apply_preconditions(
    row: &LogApplyResourceRow,
    existing: Option<&ExistingResourceRow>,
) -> tokio_rusqlite::Result<()> {
    if row.require_absent && existing.is_some() {
        return Err(apply_conflict_error(
            ApplyConflictCode::AlreadyExists,
            "Resource already exists (409 Conflict)",
        ));
    }
    if row.require_existing && existing.is_none() {
        return Err(apply_conflict_error(
            ApplyConflictCode::NotFound,
            "Resource not found (404 Not Found)",
        ));
    }
    let Some((current_rv, current_uid, _)) = existing else {
        return Ok(());
    };
    if let Some(expected_uid) = row.precondition_uid.as_deref()
        && expected_uid != current_uid
    {
        return Err(apply_conflict_error(
            ApplyConflictCode::UidPrecondition,
            "UID precondition failed (409 Conflict)",
        ));
    }
    if let Some(expected_rv) = row.precondition_resource_version
        && expected_rv != *current_rv
    {
        return Err(apply_conflict_error(
            ApplyConflictCode::ResourceVersionPrecondition,
            format!(
                "resourceVersion precondition failed: expected {expected_rv} got {current_rv} (409 Conflict)"
            ),
        ));
    }
    Ok(())
}

/// Raft-authoritative variant: enforces only structural conditions
/// (require_absent / require_existing) and skips staleness-related
/// conditions. The committed raft entry wins over stale local rv/uid.
fn validate_put_resource_presence_preconditions(
    row: &LogApplyResourceRow,
    existing: Option<&ExistingResourceRow>,
) -> tokio_rusqlite::Result<()> {
    if row.require_absent && existing.is_some() {
        return Err(apply_conflict_error(
            ApplyConflictCode::AlreadyExists,
            "Resource already exists (409 Conflict)",
        ));
    }
    if row.require_existing && existing.is_none() {
        return Err(apply_conflict_error(
            ApplyConflictCode::NotFound,
            "Resource not found (404 Not Found)",
        ));
    }
    Ok(())
}

fn log_raft_put_conflict_if_any(row: &LogApplyResourceRow, existing: Option<&ExistingResourceRow>) {
    let Some((current_rv, current_uid, _)) = existing else {
        return;
    };
    if let Some(expected_uid) = row.precondition_uid.as_deref()
        && expected_uid != current_uid.as_str()
    {
        record_raft_authoritative_apply_conflict();
        tracing::warn!(
            api_version = %row.api_version, kind = %row.kind,
            namespace = ?row.namespace, name = %row.name,
            local_uid = %current_uid, committed_uid = %expected_uid,
            "raft authoritative PUT: uid precondition bypassed, reconciling to committed value"
        );
    }
    if let Some(expected_rv) = row.precondition_resource_version
        && expected_rv != *current_rv
    {
        record_raft_authoritative_apply_conflict();
        tracing::warn!(
            api_version = %row.api_version, kind = %row.kind,
            namespace = ?row.namespace, name = %row.name,
            local_rv = %current_rv, committed_rv = %expected_rv,
            "raft authoritative PUT: rv precondition bypassed, reconciling to committed value"
        );
    }
}
