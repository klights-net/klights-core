use super::crud::helpers::{WatchEventInsert, insert_watch_event_in_conn, serde_to_sqlite_error};
use super::owner_ref_index;
use super::selector_index;
use super::{Datastore, create_pending_watch_event, queries, use_namespaced_table};
use crate::bootstrap::cluster_meta::{
    KEY_CLUSTER_ID, KEY_LEADER_EPOCH, KEY_RAFT_LEADER_HINT, KEY_RAFT_TERM, KEY_RAFT_VOTERS,
};
use crate::datastore::types::{
    AppliedOutboxRecord, PatchKind, PendingWatchEvent, ReplicatedSnapshotMetadata,
};
use crate::log_apply::{
    LogApplyAppliedOutboxRow, LogApplyCommit, LogApplyMutation, LogApplyNamespaceRow,
    LogApplyNodeDataplaneRow, LogApplyNodeSubnetAllocation, LogApplyNodeSubnetRow,
    LogApplyPodCleanupIntentKey, LogApplyPodCleanupIntentRow, LogApplyResourceKey,
    LogApplyResourcePatch, LogApplyResourceRow, LogApplyWatchEventRow,
};
use anyhow::{Result, anyhow};
use rusqlite::OptionalExtension;

impl Datastore {
    /// Replace cluster-replicated Kubernetes resources from a full leader snapshot.
    ///
    /// This deliberately bypasses normal CRUD helpers because bootstrap restore
    /// must preserve the leader's resourceVersion and must not manufacture local
    /// delete RVs for stale rows that are absent from the leader snapshot.
    pub async fn replace_replicated_resource_state(
        &self,
        entries: Vec<LogApplyCommit>,
        current_rv: i64,
        metadata: Option<ReplicatedSnapshotMetadata>,
    ) -> Result<()> {
        let pending = self
            .db_call("replace_replicated_resource_state", move |conn| {
                replace_resource_state_in_conn(conn, entries, current_rv, metadata)
            })
            .await
            .map_err(|err| anyhow!("failed to replace replicated resource state: {err}"))?;

        for event in pending {
            self.publish_watch_event(event);
        }
        Ok(())
    }
}

fn replace_resource_state_in_conn(
    conn: &mut rusqlite::Connection,
    entries: Vec<LogApplyCommit>,
    current_rv: i64,
    metadata: Option<ReplicatedSnapshotMetadata>,
) -> tokio_rusqlite::Result<Vec<PendingWatchEvent>> {
    if current_rv < 0 {
        return Err(other_error("snapshot current_rv must be non-negative"));
    }

    let tx = conn.transaction()?;
    tx.execute(queries::REPLACE_STATE_DELETE_WATCH_EVENTS, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_APPLIED_OUTBOX, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_POD_CLEANUP_INTENTS, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_NODE_DATAPLANE, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_NODE_SUBNETS, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_NAMESPACED_RESOURCES, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_CLUSTER_RESOURCES, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_RESOURCE_LABELS, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_RESOURCE_FIELDS, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_RESOURCE_OWNER_REFS, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_NAMESPACES, [])?;

    let has_explicit_watch_history = entries.iter().any(|commit| {
        commit
            .mutations
            .iter()
            .any(|mutation| matches!(mutation, LogApplyMutation::PutWatchEvent(_)))
    });
    let mut pending = Vec::with_capacity(entries.len());
    for commit in entries {
        if commit.resource_version <= 0 {
            return Err(other_error(format!(
                "snapshot entry has non-positive resourceVersion {}",
                commit.resource_version
            )));
        }
        if commit.resource_version > current_rv {
            return Err(other_error(format!(
                "snapshot entry resourceVersion {} is ahead of leader current_rv {}",
                commit.resource_version, current_rv
            )));
        }
        let (_applied_rv, commit_pending) =
            apply_commit_in_tx_with_watch_events(&tx, commit, !has_explicit_watch_history)?;
        pending.extend(commit_pending);
    }
    if has_explicit_watch_history {
        restore_created_rv_from_watch_history(&tx)?;
    }

    tx.execute(
        queries::METADATA_SET_RV,
        rusqlite::params![current_rv.to_string()],
    )?;
    if let Some(metadata) = metadata {
        tx.execute(
            queries::UPSERT_KLIGHTS_META,
            rusqlite::params![KEY_CLUSTER_ID, metadata.cluster_id],
        )?;
        tx.execute(
            queries::UPSERT_KLIGHTS_META,
            rusqlite::params![KEY_LEADER_EPOCH, metadata.leader_epoch.to_string()],
        )?;
        if let Some(membership) = metadata.membership {
            let voters = serde_json::to_string(&membership.voters)
                .map_err(|err| other_error(format!("failed to serialize voters: {err}")))?;
            tx.execute(
                queries::UPSERT_KLIGHTS_META,
                rusqlite::params![KEY_RAFT_VOTERS, voters],
            )?;
            tx.execute(
                queries::UPSERT_KLIGHTS_META,
                rusqlite::params![KEY_RAFT_TERM, membership.term.to_string()],
            )?;
            tx.execute(
                queries::UPSERT_KLIGHTS_META,
                rusqlite::params![
                    KEY_RAFT_LEADER_HINT,
                    membership.leader_hint.unwrap_or_default()
                ],
            )?;
        }
    }
    tx.commit()?;

    Ok(pending)
}

fn restore_created_rv_from_watch_history(
    tx: &rusqlite::Transaction<'_>,
) -> tokio_rusqlite::Result<()> {
    tx.execute(
        "UPDATE namespaced_resources AS r
         SET created_rv = (
             SELECT MIN(w.resource_version)
             FROM watch_events w
             WHERE w.event_type = 'ADDED'
               AND w.api_version = r.api_version
               AND w.kind = r.kind
               AND w.namespace = r.namespace
               AND w.name = r.name
               AND json_extract(w.data, '$.metadata.uid') = r.uid
         )
         WHERE EXISTS (
             SELECT 1
             FROM watch_events w
             WHERE w.event_type = 'ADDED'
               AND w.api_version = r.api_version
               AND w.kind = r.kind
               AND w.namespace = r.namespace
               AND w.name = r.name
               AND json_extract(w.data, '$.metadata.uid') = r.uid
         )",
        [],
    )?;
    tx.execute(
        "UPDATE cluster_resources AS r
         SET created_rv = (
             SELECT MIN(w.resource_version)
             FROM watch_events w
             WHERE w.event_type = 'ADDED'
               AND w.api_version = r.api_version
               AND w.kind = r.kind
               AND w.namespace IS NULL
               AND w.name = r.name
               AND json_extract(w.data, '$.metadata.uid') = r.uid
         )
         WHERE EXISTS (
             SELECT 1
             FROM watch_events w
             WHERE w.event_type = 'ADDED'
               AND w.api_version = r.api_version
               AND w.kind = r.kind
               AND w.namespace IS NULL
               AND w.name = r.name
               AND json_extract(w.data, '$.metadata.uid') = r.uid
         )",
        [],
    )?;
    Ok(())
}

impl Datastore {
    pub async fn apply_log_apply_commit(&self, commit: LogApplyCommit) -> Result<()> {
        let pending = self
            .db_call("apply_log_apply_commit", move |conn| {
                let tx = conn.transaction()?;
                let pending = apply_commit_in_tx(&tx, commit)?;
                tx.commit()?;
                Ok(pending)
            })
            .await
            .map_err(|err| anyhow!("failed to apply log_apply commit: {err}"))?;

        for event in pending {
            self.publish_watch_event(event);
        }
        Ok(())
    }

    pub async fn apply_raft_log_apply_commit(
        &self,
        commit: LogApplyCommit,
    ) -> Result<crate::datastore::raft::types::StorageCommandResult> {
        let outcome = self
            .db_call("apply_raft_log_apply_commit", move |conn| {
                let tx = conn.transaction()?;
                let outcome = apply_commit_in_tx_for_raft(&tx, commit)?;
                tx.commit()?;
                Ok(outcome)
            })
            .await
            .map_err(|err| anyhow!("failed to apply raft log_apply commit: {err}"))?;

        for event in outcome.pending {
            self.publish_watch_event(event);
        }
        Ok(outcome.result)
    }
}

pub(crate) struct RaftLogApplyOutcome {
    pub result: crate::datastore::raft::types::StorageCommandResult,
    pub pending: Vec<PendingWatchEvent>,
}

pub(crate) fn apply_commit_in_tx_for_raft(
    tx: &rusqlite::Transaction<'_>,
    commit: LogApplyCommit,
) -> tokio_rusqlite::Result<RaftLogApplyOutcome> {
    let outbox_template = commit.mutations.iter().find_map(|mutation| match mutation {
        LogApplyMutation::PutAppliedOutbox(row) => Some(row.clone()),
        _ => None,
    });
    if let Some(template) = outbox_template.as_ref()
        && let Some(existing) = applied_outbox_record_in_tx(tx, &template.idempotency_key)?
        && !is_uncommitted_outbox_placeholder(&existing)
    {
        return Ok(RaftLogApplyOutcome {
            result: storage_result_from_applied_outbox(&existing)?,
            pending: Vec::new(),
        });
    }

    tx.execute("SAVEPOINT raft_apply_attempt", [])?;
    match apply_commit_in_tx_returning_rv(tx, commit) {
        Ok((rv, pending)) => {
            tx.execute("RELEASE raft_apply_attempt", [])?;
            Ok(RaftLogApplyOutcome {
                result: crate::datastore::raft::types::StorageCommandResult {
                    applied_rv: Some(rv),
                    error_message: None,
                },
                pending,
            })
        }
        Err(err) if is_terminal_apply_conflict(&err) => {
            tx.execute("ROLLBACK TO raft_apply_attempt", [])?;
            tx.execute("RELEASE raft_apply_attempt", [])?;
            let message = err.to_string();
            if let Some(mut row) = outbox_template {
                row.applied_rv = None;
                row.result_proto = crate::datastore::command::encode_response_protobuf(
                    &crate::datastore::command::StorageResponse::Error {
                        message: message.clone(),
                    },
                )
                .unwrap_or_default();
                put_applied_outbox_row(tx, row)?;
            }
            Ok(RaftLogApplyOutcome {
                result: crate::datastore::raft::types::StorageCommandResult {
                    applied_rv: None,
                    error_message: Some(message),
                },
                pending: Vec::new(),
            })
        }
        Err(err) => {
            tx.execute("ROLLBACK TO raft_apply_attempt", [])?;
            tx.execute("RELEASE raft_apply_attempt", [])?;
            Err(err)
        }
    }
}

pub(crate) fn apply_commit_in_tx(
    tx: &rusqlite::Transaction<'_>,
    commit: LogApplyCommit,
) -> tokio_rusqlite::Result<Vec<PendingWatchEvent>> {
    let (_applied_rv, pending) = apply_commit_in_tx_returning_rv(tx, commit)?;
    Ok(pending)
}

pub(crate) fn apply_commit_in_tx_returning_rv(
    tx: &rusqlite::Transaction<'_>,
    commit: LogApplyCommit,
) -> tokio_rusqlite::Result<(i64, Vec<PendingWatchEvent>)> {
    let has_explicit_watch_history = commit
        .mutations
        .iter()
        .any(|mutation| matches!(mutation, LogApplyMutation::PutWatchEvent(_)));
    apply_commit_in_tx_with_watch_events(tx, commit, !has_explicit_watch_history)
}

fn apply_commit_in_tx_with_watch_events(
    tx: &rusqlite::Transaction<'_>,
    commit: LogApplyCommit,
    emit_watch_events: bool,
) -> tokio_rusqlite::Result<(i64, Vec<PendingWatchEvent>)> {
    if commit.resource_version < 0 {
        return Err(other_error(
            "log_apply commit resourceVersion must be non-negative",
        ));
    }
    let commit = stamp_provisional_resource_version_in_tx(tx, commit)?;
    let applied_rv = commit.resource_version;
    let mut pending = Vec::with_capacity(commit.mutations.len());
    for mutation in commit.mutations {
        match mutation {
            LogApplyMutation::PutResource(row) => {
                if row.resource_version != commit.resource_version {
                    return Err(other_error("resource row RV does not match commit RV"));
                }
                if let Some(event) = put_resource_row(tx, row, emit_watch_events)? {
                    pending.push(event);
                }
            }
            LogApplyMutation::PatchResourceLatest(patch) => {
                if patch.resource_version != commit.resource_version {
                    return Err(other_error("resource patch RV does not match commit RV"));
                }
                if let Some(event) = patch_resource_latest_row(tx, patch, emit_watch_events)? {
                    pending.push(event);
                }
            }
            LogApplyMutation::DeleteResource(key) => {
                if let Some(event) =
                    delete_resource_row(tx, commit.resource_version, key, emit_watch_events)?
                {
                    pending.push(event);
                }
            }
            LogApplyMutation::PutNamespace(row) => {
                if row.resource_version != commit.resource_version {
                    return Err(other_error("namespace row RV does not match commit RV"));
                }
                if let Some(event) = put_namespace_row(tx, row, emit_watch_events)? {
                    pending.push(event);
                }
            }
            LogApplyMutation::DeleteNamespace { name } => {
                if let Some(event) =
                    delete_namespace_row(tx, commit.resource_version, &name, emit_watch_events)?
                {
                    pending.push(event);
                }
            }
            LogApplyMutation::DeleteNamespaceContents { name } => {
                delete_namespace_contents_rows(tx, &name)?;
            }
            LogApplyMutation::PutNodeSubnet(row) => put_node_subnet_row(tx, row)?,
            LogApplyMutation::AllocateNodeSubnet(allocation) => {
                let row = allocate_node_subnet_row(tx, allocation)?;
                put_node_subnet_row(tx, row)?;
            }
            LogApplyMutation::DeleteNodeSubnet { node_name } => {
                tx.execute(queries::NODE_SUBNET_DELETE, rusqlite::params![node_name])?;
            }
            LogApplyMutation::PutNodeDataplane(row) => put_node_dataplane_row(tx, row)?,
            LogApplyMutation::DeleteNodeDataplane { node_name } => {
                tx.execute(queries::NODE_DATAPLANE_DELETE, rusqlite::params![node_name])?;
            }
            LogApplyMutation::PutAppliedOutbox(row) => put_applied_outbox_row(tx, row)?,
            LogApplyMutation::DeleteAppliedOutbox { idempotency_key } => {
                tx.execute(
                    queries::APPLIED_OUTBOX_DELETE_BY_KEY,
                    rusqlite::params![idempotency_key],
                )?;
            }
            LogApplyMutation::GcAppliedOutbox {
                cutoff_ms,
                operations: _,
            } => {
                tx.execute(
                    queries::APPLIED_OUTBOX_DELETE_EXPIRED,
                    rusqlite::params![cutoff_ms],
                )?;
            }
            LogApplyMutation::GcWatchEvents {
                max_rows,
                batch_cap,
            } => {
                let removed = super::gc::gc_watch_events_in_tx(tx, max_rows, batch_cap)?;
                if removed > 0 {
                    let _ = tx.execute("PRAGMA incremental_vacuum(1000)", []);
                }
            }
            LogApplyMutation::PutWatchEvent(row) => pending.push(put_watch_event_row(tx, row)?),
            LogApplyMutation::AdvanceResourceVersion { .. } => {}
            LogApplyMutation::PutKlightsMeta { key, value } => {
                tx.execute(
                    crate::datastore::sqlite::queries::UPSERT_KLIGHTS_META,
                    rusqlite::params![&key, &value],
                )?;
            }
            LogApplyMutation::PutPodCleanupIntent(row) => {
                if row.resource_version != commit.resource_version {
                    return Err(other_error(
                        "pod cleanup intent RV does not match commit RV",
                    ));
                }
                put_pod_cleanup_intent_row(tx, row)?;
            }
            LogApplyMutation::DeletePodCleanupIntent(key) => {
                delete_pod_cleanup_intent_row(tx, key)?;
            }
            LogApplyMutation::DeletePodCleanupIntentsForNode { node_name } => {
                tx.execute(
                    queries::POD_CLEANUP_INTENTS_DELETE_BY_NODE,
                    rusqlite::params![node_name],
                )?;
            }
        }
    }
    advance_metadata_rv_to_at_least_tx(tx, commit.resource_version)?;
    Ok((applied_rv, pending))
}

fn stamp_provisional_resource_version_in_tx(
    tx: &rusqlite::Transaction<'_>,
    mut commit: LogApplyCommit,
) -> tokio_rusqlite::Result<LogApplyCommit> {
    let rv = if commit.resource_version == 0 {
        Datastore::next_resource_version_in_tx(tx)?
    } else {
        commit.resource_version
    };
    commit.resource_version = rv;
    for mutation in &mut commit.mutations {
        match mutation {
            LogApplyMutation::PutResource(row) => {
                if row.resource_version == 0 {
                    row.resource_version = rv;
                }
                if row.resource_version == rv {
                    row.data = crate::datastore::sqlite::resource_shape::hydrate_watch_event_data(
                        std::mem::take(&mut row.data),
                        &row.api_version,
                        &row.kind,
                        row.namespace.as_deref(),
                        &row.name,
                        rv,
                    );
                }
            }
            LogApplyMutation::PatchResourceLatest(patch) if patch.resource_version == 0 => {
                patch.resource_version = rv;
            }
            LogApplyMutation::PatchResourceLatest(_) => {}
            LogApplyMutation::PutNamespace(row) => {
                if row.resource_version == 0 {
                    row.resource_version = rv;
                }
                if row.resource_version == rv {
                    row.data = crate::datastore::sqlite::resource_shape::hydrate_watch_event_data(
                        std::mem::take(&mut row.data),
                        "v1",
                        "Namespace",
                        None,
                        &row.name,
                        rv,
                    );
                }
            }
            LogApplyMutation::PutPodCleanupIntent(row) if row.resource_version == 0 => {
                row.resource_version = rv;
            }
            LogApplyMutation::PutAppliedOutbox(row) => {
                if row.applied_rv.is_none() {
                    row.applied_rv = Some(rv);
                }
                if row.result_proto.is_empty()
                    || crate::datastore::command::decode_response_protobuf(&row.result_proto)
                        .is_ok_and(|response| {
                            matches!(
                                response,
                                crate::datastore::command::StorageResponse::Ack {
                                    resource_version: 0
                                }
                            )
                        })
                {
                    row.result_proto = crate::datastore::command::encode_response_protobuf(
                        &crate::datastore::command::StorageResponse::Ack {
                            resource_version: rv,
                        },
                    )
                    .unwrap_or_default();
                }
            }
            LogApplyMutation::AdvanceResourceVersion { resource_version } => {
                *resource_version = (*resource_version).max(rv);
            }
            _ => {}
        }
    }
    Ok(commit)
}

fn put_resource_row(
    tx: &rusqlite::Transaction<'_>,
    mut row: LogApplyResourceRow,
    emit_watch_events: bool,
) -> tokio_rusqlite::Result<Option<PendingWatchEvent>> {
    let namespaced = use_namespaced_table(&row.api_version, &row.kind, &row.namespace.as_deref());
    if namespaced {
        let namespace_owned = row
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let namespace = namespace_owned.as_str();
        let existing = tx
            .query_row(
                queries::NAMESPACED_GET,
                rusqlite::params![&row.api_version, &row.kind, namespace, &row.name],
                |db_row| {
                    Ok((
                        db_row.get::<_, i64>(5)?,
                        db_row.get::<_, String>(6)?,
                        db_row.get::<_, Vec<u8>>(7)?,
                    ))
                },
            )
            .optional()?;
        validate_put_resource_apply_preconditions(&row, existing.as_ref())?;
        merge_status_only_row_with_existing(&mut row, existing.as_ref())?;
        let data_bytes = serde_json::to_vec(&row.data)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        if existing.as_ref().is_some_and(|(rv, _uid, existing_bytes)| {
            *rv == row.resource_version && *existing_bytes == data_bytes
        }) {
            selector_index::upsert_index_entries(
                tx,
                &row.api_version,
                &row.kind,
                namespace,
                &row.name,
                &data_bytes,
            )?;
            owner_ref_index::upsert_owner_refs(
                tx,
                &row.api_version,
                &row.kind,
                namespace,
                &row.name,
                &data_bytes,
            )?;
            return Ok(None);
        }
        tx.execute(
            queries::NAMESPACED_UPSERT_EXACT,
            rusqlite::params![
                &row.api_version,
                &row.kind,
                namespace,
                &row.name,
                &row.uid,
                row.resource_version,
                &data_bytes
            ],
        )?;
        selector_index::upsert_index_entries(
            tx,
            &row.api_version,
            &row.kind,
            namespace,
            &row.name,
            &data_bytes,
        )?;
        owner_ref_index::upsert_owner_refs(
            tx,
            &row.api_version,
            &row.kind,
            namespace,
            &row.name,
            &data_bytes,
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
            tx,
            WatchEventInsert::new(
                &row.api_version,
                &row.kind,
                Some(namespace),
                &row.name,
                row.resource_version,
                event_type,
                &data_bytes,
            ),
        )?;
        Ok(Some(create_pending_watch_event(
            &row.api_version,
            &row.kind,
            Some(namespace),
            &row.name,
            row.resource_version,
            event_type,
            row.data,
        )))
    } else {
        let existing = tx
            .query_row(
                queries::CLUSTER_GET,
                rusqlite::params![&row.api_version, &row.kind, &row.name],
                |db_row| {
                    Ok((
                        db_row.get::<_, i64>(4)?,
                        db_row.get::<_, String>(5)?,
                        db_row.get::<_, Vec<u8>>(6)?,
                    ))
                },
            )
            .optional()?;
        validate_put_resource_apply_preconditions(&row, existing.as_ref())?;
        merge_status_only_row_with_existing(&mut row, existing.as_ref())?;
        let data_bytes = serde_json::to_vec(&row.data)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        if existing.as_ref().is_some_and(|(rv, _uid, existing_bytes)| {
            *rv == row.resource_version && *existing_bytes == data_bytes
        }) {
            selector_index::upsert_index_entries(
                tx,
                &row.api_version,
                &row.kind,
                "",
                &row.name,
                &data_bytes,
            )?;
            owner_ref_index::upsert_owner_refs(
                tx,
                &row.api_version,
                &row.kind,
                "",
                &row.name,
                &data_bytes,
            )?;
            return Ok(None);
        }
        tx.execute(
            queries::CLUSTER_UPSERT_EXACT,
            rusqlite::params![
                &row.api_version,
                &row.kind,
                &row.name,
                &row.uid,
                row.resource_version,
                &data_bytes
            ],
        )?;
        selector_index::upsert_index_entries(
            tx,
            &row.api_version,
            &row.kind,
            "",
            &row.name,
            &data_bytes,
        )?;
        owner_ref_index::upsert_owner_refs(
            tx,
            &row.api_version,
            &row.kind,
            "",
            &row.name,
            &data_bytes,
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
            tx,
            WatchEventInsert::new(
                &row.api_version,
                &row.kind,
                None,
                &row.name,
                row.resource_version,
                event_type,
                &data_bytes,
            ),
        )?;
        Ok(Some(create_pending_watch_event(
            &row.api_version,
            &row.kind,
            None,
            &row.name,
            row.resource_version,
            event_type,
            row.data,
        )))
    }
}

fn patch_resource_latest_row(
    tx: &rusqlite::Transaction<'_>,
    patch: LogApplyResourcePatch,
    emit_watch_events: bool,
) -> tokio_rusqlite::Result<Option<PendingWatchEvent>> {
    let namespaced =
        use_namespaced_table(&patch.api_version, &patch.kind, &patch.namespace.as_deref());
    if namespaced {
        let namespace_owned = patch
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let namespace = namespace_owned.as_str();
        let existing = tx
            .query_row(
                queries::NAMESPACED_GET,
                rusqlite::params![&patch.api_version, &patch.kind, namespace, &patch.name],
                |db_row| {
                    Ok((
                        db_row.get::<_, i64>(5)?,
                        db_row.get::<_, String>(6)?,
                        db_row.get::<_, Vec<u8>>(7)?,
                    ))
                },
            )
            .optional()?;
        let Some((current_rv, current_uid, current_bytes)) = existing else {
            if patch.require_existing {
                return Err(other_error("Resource not found (404 Not Found)"));
            }
            return Ok(None);
        };
        let patched_data = apply_latest_patch_to_current_resource(
            &patch,
            current_rv,
            &current_uid,
            &current_bytes,
            Some(namespace),
        )?;
        let data_bytes = serde_json::to_vec(&patched_data)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        tx.execute(
            queries::NAMESPACED_UPSERT_EXACT,
            rusqlite::params![
                &patch.api_version,
                &patch.kind,
                namespace,
                &patch.name,
                &current_uid,
                patch.resource_version,
                &data_bytes
            ],
        )?;
        selector_index::upsert_index_entries(
            tx,
            &patch.api_version,
            &patch.kind,
            namespace,
            &patch.name,
            &data_bytes,
        )?;
        owner_ref_index::upsert_owner_refs(
            tx,
            &patch.api_version,
            &patch.kind,
            namespace,
            &patch.name,
            &data_bytes,
        )?;
        if !emit_watch_events {
            return Ok(None);
        }
        insert_watch_event_in_conn(
            tx,
            WatchEventInsert::new(
                &patch.api_version,
                &patch.kind,
                Some(namespace),
                &patch.name,
                patch.resource_version,
                "MODIFIED",
                &data_bytes,
            ),
        )?;
        Ok(Some(create_pending_watch_event(
            &patch.api_version,
            &patch.kind,
            Some(namespace),
            &patch.name,
            patch.resource_version,
            "MODIFIED",
            patched_data,
        )))
    } else {
        let existing = tx
            .query_row(
                queries::CLUSTER_GET,
                rusqlite::params![&patch.api_version, &patch.kind, &patch.name],
                |db_row| {
                    Ok((
                        db_row.get::<_, i64>(4)?,
                        db_row.get::<_, String>(5)?,
                        db_row.get::<_, Vec<u8>>(6)?,
                    ))
                },
            )
            .optional()?;
        let Some((current_rv, current_uid, current_bytes)) = existing else {
            if patch.require_existing {
                return Err(other_error("Resource not found (404 Not Found)"));
            }
            return Ok(None);
        };
        let patched_data = apply_latest_patch_to_current_resource(
            &patch,
            current_rv,
            &current_uid,
            &current_bytes,
            None,
        )?;
        let data_bytes = serde_json::to_vec(&patched_data)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        tx.execute(
            queries::CLUSTER_UPSERT_EXACT,
            rusqlite::params![
                &patch.api_version,
                &patch.kind,
                &patch.name,
                &current_uid,
                patch.resource_version,
                &data_bytes
            ],
        )?;
        selector_index::upsert_index_entries(
            tx,
            &patch.api_version,
            &patch.kind,
            "",
            &patch.name,
            &data_bytes,
        )?;
        owner_ref_index::upsert_owner_refs(
            tx,
            &patch.api_version,
            &patch.kind,
            "",
            &patch.name,
            &data_bytes,
        )?;
        if !emit_watch_events {
            return Ok(None);
        }
        insert_watch_event_in_conn(
            tx,
            WatchEventInsert::new(
                &patch.api_version,
                &patch.kind,
                None,
                &patch.name,
                patch.resource_version,
                "MODIFIED",
                &data_bytes,
            ),
        )?;
        Ok(Some(create_pending_watch_event(
            &patch.api_version,
            &patch.kind,
            None,
            &patch.name,
            patch.resource_version,
            "MODIFIED",
            patched_data,
        )))
    }
}

fn apply_latest_patch_to_current_resource(
    patch: &LogApplyResourcePatch,
    current_rv: i64,
    current_uid: &str,
    current_bytes: &[u8],
    namespace: Option<&str>,
) -> tokio_rusqlite::Result<serde_json::Value> {
    if let Some(expected_uid) = patch.precondition_uid.as_deref()
        && expected_uid != current_uid
    {
        return Err(other_error("UID precondition failed (409 Conflict)"));
    }
    if let Some(expected_rv) = patch.precondition_resource_version
        && expected_rv != current_rv
    {
        return Err(other_error(format!(
            "resourceVersion precondition failed: expected {expected_rv} got {current_rv} (409 Conflict)"
        )));
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
            .map(std::borrow::Cow::Borrowed)
            .unwrap_or_else(|| std::borrow::Cow::Owned(crate::utils::k8s_timestamp()));
        crate::resource_semantics::mark_terminating_pod_unready_at(
            &mut patched,
            transition_time.as_ref(),
        );
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
    existing: Option<&(i64, String, Vec<u8>)>,
) -> tokio_rusqlite::Result<()> {
    if !row.status_only {
        return Ok(());
    }
    let Some((_current_rv, current_uid, existing_bytes)) = existing else {
        return Ok(());
    };
    let status = row
        .data
        .get("status")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
    let mut live: serde_json::Value =
        serde_json::from_slice(existing_bytes).map_err(serde_to_sqlite_error)?;
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
    row.uid = current_uid.clone();
    row.data = live;
    Ok(())
}

fn validate_put_resource_apply_preconditions(
    row: &LogApplyResourceRow,
    existing: Option<&(i64, String, Vec<u8>)>,
) -> tokio_rusqlite::Result<()> {
    if row.require_absent && existing.is_some() {
        return Err(other_error("Resource already exists (409 Conflict)"));
    }
    if row.require_existing && existing.is_none() {
        return Err(other_error("Resource not found (404 Not Found)"));
    }
    let Some((current_rv, current_uid, _)) = existing else {
        return Ok(());
    };
    if let Some(expected_uid) = row.precondition_uid.as_deref()
        && expected_uid != current_uid
    {
        return Err(other_error("UID precondition failed (409 Conflict)"));
    }
    if let Some(expected_rv) = row.precondition_resource_version
        && expected_rv != *current_rv
    {
        return Err(other_error(format!(
            "resourceVersion precondition failed: expected {expected_rv} got {current_rv} (409 Conflict)"
        )));
    }
    Ok(())
}

fn delete_resource_row(
    tx: &rusqlite::Transaction<'_>,
    resource_version: i64,
    key: LogApplyResourceKey,
    emit_watch_events: bool,
) -> tokio_rusqlite::Result<Option<PendingWatchEvent>> {
    let namespace_ref = key.namespace.as_deref();
    if use_namespaced_table(&key.api_version, &key.kind, &namespace_ref) {
        let namespace = namespace_ref.unwrap_or("default");
        let existing = tx
            .query_row(
                queries::NAMESPACED_GET_DATA_FOR_DELETE,
                rusqlite::params![&key.api_version, &key.kind, namespace, &key.name],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .optional()?;
        let Some((current_rv, current_uid, data_bytes)) = existing else {
            return Ok(None);
        };
        // UID guard: a non-empty key.uid means the leader (or a previous
        // apply that captured the original UID) qualified the delete to a
        // specific resource identity. If the current row has a different
        // UID, this delete is stale (a same-name replacement landed) and
        // must be a no-op. Empty key.uid is permitted only for snapshot /
        // backfill paths that reconstruct deletes without UID context.
        if !key.uid.is_empty() && key.uid != current_uid {
            return Ok(None);
        }
        if let Some(expected_rv) = key.precondition_resource_version
            && expected_rv != current_rv
        {
            return Err(other_error(format!(
                "resourceVersion precondition failed: expected {expected_rv} got {current_rv} (409 Conflict)"
            )));
        }
        tx.execute(
            queries::NAMESPACED_DELETE_BY_KEY,
            rusqlite::params![&key.api_version, &key.kind, namespace, &key.name],
        )?;
        selector_index::delete_index_entries(
            tx,
            &key.api_version,
            &key.kind,
            namespace,
            &key.name,
        )?;
        owner_ref_index::delete_owner_refs(tx, &key.api_version, &key.kind, namespace, &key.name)?;
        if !emit_watch_events {
            return Ok(None);
        }
        insert_watch_event_in_conn(
            tx,
            WatchEventInsert::new(
                &key.api_version,
                &key.kind,
                Some(namespace),
                &key.name,
                resource_version,
                "DELETED",
                &data_bytes,
            ),
        )?;
        let data = serde_json::from_slice(&data_bytes).unwrap_or(serde_json::Value::Null);
        Ok(Some(create_pending_watch_event(
            &key.api_version,
            &key.kind,
            Some(namespace),
            &key.name,
            resource_version,
            "DELETED",
            data,
        )))
    } else {
        let existing = tx
            .query_row(
                queries::CLUSTER_GET_DATA_FOR_DELETE,
                rusqlite::params![&key.api_version, &key.kind, &key.name],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                },
            )
            .optional()?;
        let Some((current_rv, current_uid, data_bytes)) = existing else {
            return Ok(None);
        };
        if !key.uid.is_empty() && key.uid != current_uid {
            return Ok(None);
        }
        if let Some(expected_rv) = key.precondition_resource_version
            && expected_rv != current_rv
        {
            return Err(other_error(format!(
                "resourceVersion precondition failed: expected {expected_rv} got {current_rv} (409 Conflict)"
            )));
        }
        tx.execute(
            queries::CLUSTER_DELETE_BY_KEY,
            rusqlite::params![&key.api_version, &key.kind, &key.name],
        )?;
        selector_index::delete_index_entries(tx, &key.api_version, &key.kind, "", &key.name)?;
        owner_ref_index::delete_owner_refs(tx, &key.api_version, &key.kind, "", &key.name)?;
        if !emit_watch_events {
            return Ok(None);
        }
        insert_watch_event_in_conn(
            tx,
            WatchEventInsert::new(
                &key.api_version,
                &key.kind,
                None,
                &key.name,
                resource_version,
                "DELETED",
                &data_bytes,
            ),
        )?;
        let data = serde_json::from_slice(&data_bytes).unwrap_or(serde_json::Value::Null);
        Ok(Some(create_pending_watch_event(
            &key.api_version,
            &key.kind,
            None,
            &key.name,
            resource_version,
            "DELETED",
            data,
        )))
    }
}

fn put_namespace_row(
    tx: &rusqlite::Transaction<'_>,
    row: LogApplyNamespaceRow,
    emit_watch_events: bool,
) -> tokio_rusqlite::Result<Option<PendingWatchEvent>> {
    let data_bytes = serde_json::to_vec(&row.data)
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
    let existing = tx
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
    tx.execute(
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
        tx,
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

fn delete_namespace_row(
    tx: &rusqlite::Transaction<'_>,
    resource_version: i64,
    name: &str,
    emit_watch_events: bool,
) -> tokio_rusqlite::Result<Option<PendingWatchEvent>> {
    let existing = tx
        .query_row(
            queries::NAMESPACE_GET_DATA,
            rusqlite::params![name],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    let Some(data_bytes) = existing else {
        return Ok(None);
    };
    tx.execute(queries::NAMESPACE_DELETE, rusqlite::params![name])?;
    if !emit_watch_events {
        return Ok(None);
    }
    insert_watch_event_in_conn(
        tx,
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
    let data = serde_json::from_slice(&data_bytes).unwrap_or(serde_json::Value::Null);
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

fn delete_namespace_contents_rows(
    tx: &rusqlite::Transaction<'_>,
    name: &str,
) -> tokio_rusqlite::Result<()> {
    let mut stmt = tx.prepare(queries::NAMESPACE_RESOURCES_LIST_EXCLUDING_KIND)?;
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
    tx.execute(
        queries::NAMESPACE_RESOURCES_DELETE_NON_PODS,
        rusqlite::params![name],
    )?;
    for (api_version, kind, namespace, resource_name) in rows {
        selector_index::delete_index_entries(tx, &api_version, &kind, &namespace, &resource_name)?;
        owner_ref_index::delete_owner_refs(tx, &api_version, &kind, &namespace, &resource_name)?;
    }
    Ok(())
}

fn put_applied_outbox_row(
    tx: &rusqlite::Transaction<'_>,
    row: LogApplyAppliedOutboxRow,
) -> tokio_rusqlite::Result<()> {
    tx.execute(
        queries::APPLIED_OUTBOX_UPSERT_EXACT,
        rusqlite::params![
            row.idempotency_key,
            row.subject_key,
            row.operation,
            row.first_seen_ms,
            row.applied_rv,
            row.result_proto,
            row.status_stamp
        ],
    )?;
    Ok(())
}

fn allocate_node_subnet_row(
    tx: &rusqlite::Transaction<'_>,
    allocation: LogApplyNodeSubnetAllocation,
) -> tokio_rusqlite::Result<LogApplyNodeSubnetRow> {
    let node_name_typed = crate::networking::NodeName::parse(&allocation.node_name)
        .map_err(|err| other_error(format!("Invalid node name {}: {err}", allocation.node_name)))?;
    let node_ip_typed: std::net::Ipv4Addr = allocation
        .node_ip
        .parse()
        .map_err(|err| other_error(format!("Invalid node IP {}: {err}", allocation.node_ip)))?;
    let cluster =
        crate::networking::ClusterCidr::parse(&allocation.cluster_cidr).map_err(|err| {
            other_error(format!(
                "Invalid cluster CIDR {}: {err}",
                allocation.cluster_cidr
            ))
        })?;
    if cluster.prefix() > 24 {
        return Err(other_error(format!(
            "cluster CIDR prefix must be <= /24 (got /{})",
            cluster.prefix()
        )));
    }

    let existing = tx
        .query_row(
            queries::NODE_SUBNET_SELECT_BY_NAME,
            rusqlite::params![node_name_typed.as_str()],
            |row| {
                Ok(LogApplyNodeSubnetRow {
                    node_name: row.get(0)?,
                    subnet: row.get(1)?,
                    subnet_base_int: row.get::<_, i64>(2)? as u32,
                    vtep_ip: row.get(3)?,
                    vtep_mac: row.get(4)?,
                    node_ip: row.get(5)?,
                    mode: row.get(6)?,
                    hostport_range: row.get(7)?,
                })
            },
        )
        .optional()?;
    if let Some(existing) = existing {
        return Ok(existing);
    }

    let mut allocated = std::collections::BTreeSet::new();
    {
        let mut stmt = tx.prepare("SELECT subnet_base_int FROM node_subnets")?;
        let rows = stmt.query_map([], |row| row.get::<_, i64>(0))?;
        for row in rows {
            allocated.insert(row? as u32);
        }
    }

    let cluster_base = cluster.network();
    let host_bits = 32u32.saturating_sub(cluster.prefix() as u32);
    let subnet_count = 1u32.checked_shl(host_bits - 8).unwrap_or(1).max(1);
    for i in 0..subnet_count {
        let base = cluster_base + (i << 8);
        if allocated.contains(&base) {
            continue;
        }
        let subnet_typed =
            crate::networking::PodSubnet::parse(&format!("{}/24", std::net::Ipv4Addr::from(base)))
                .expect("constructed /24 must parse");
        let vtep_ip = std::net::Ipv4Addr::from(base);
        return Ok(LogApplyNodeSubnetRow {
            node_name: node_name_typed.as_str().to_string(),
            subnet: subnet_typed.to_string(),
            subnet_base_int: base,
            vtep_ip: vtep_ip.to_string(),
            vtep_mac: None,
            node_ip: node_ip_typed.to_string(),
            mode: "root".to_string(),
            hostport_range: None,
        });
    }

    Err(tokio_rusqlite::Error::Rusqlite(
        rusqlite::Error::QueryReturnedNoRows,
    ))
}

fn applied_outbox_record_in_tx(
    tx: &rusqlite::Transaction<'_>,
    idempotency_key: &str,
) -> tokio_rusqlite::Result<Option<AppliedOutboxRecord>> {
    tx.query_row(queries::APPLIED_OUTBOX_GET, [idempotency_key], |row| {
        Ok(AppliedOutboxRecord {
            idempotency_key: row.get(0)?,
            subject_key: row.get(1)?,
            operation: row.get(2)?,
            first_seen_ms: row.get(3)?,
            applied_rv: row.get(4)?,
            result_proto: row.get(5)?,
            status_stamp: row.get(6)?,
        })
    })
    .optional()
    .map_err(tokio_rusqlite::Error::from)
}

fn is_uncommitted_outbox_placeholder(row: &AppliedOutboxRecord) -> bool {
    row.applied_rv.is_none() && row.subject_key.is_empty() && row.result_proto.is_empty()
}

fn storage_result_from_applied_outbox(
    row: &AppliedOutboxRecord,
) -> tokio_rusqlite::Result<crate::datastore::raft::types::StorageCommandResult> {
    match crate::datastore::command::decode_response_protobuf(&row.result_proto) {
        Ok(crate::datastore::command::StorageResponse::Error { message }) => {
            Ok(crate::datastore::raft::types::StorageCommandResult {
                applied_rv: row.applied_rv,
                error_message: Some(message),
            })
        }
        Ok(_) => Ok(crate::datastore::raft::types::StorageCommandResult {
            applied_rv: row.applied_rv,
            error_message: None,
        }),
        Err(err) => Err(other_error(format!(
            "failed to decode applied_outbox result: {err}"
        ))),
    }
}

fn is_terminal_apply_conflict(err: &tokio_rusqlite::Error) -> bool {
    let msg = err.to_string();
    msg.contains("409 Conflict") || msg.contains("404 Not Found")
}

fn put_pod_cleanup_intent_row(
    tx: &rusqlite::Transaction<'_>,
    row: LogApplyPodCleanupIntentRow,
) -> tokio_rusqlite::Result<()> {
    let pod_data = serde_json::to_vec(&row.pod_data)
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
    tx.execute(
        queries::POD_CLEANUP_INTENT_UPSERT,
        rusqlite::params![
            row.node_name,
            row.namespace,
            row.pod_name,
            row.pod_uid,
            row.reason,
            row.resource_version,
            row.created_at_ms,
            pod_data
        ],
    )?;
    Ok(())
}

fn delete_pod_cleanup_intent_row(
    tx: &rusqlite::Transaction<'_>,
    key: LogApplyPodCleanupIntentKey,
) -> tokio_rusqlite::Result<()> {
    tx.execute(
        queries::POD_CLEANUP_INTENT_DELETE,
        rusqlite::params![
            key.node_name,
            key.namespace,
            key.pod_name,
            key.pod_uid,
            key.reason
        ],
    )?;
    Ok(())
}

fn put_watch_event_row(
    tx: &rusqlite::Transaction<'_>,
    row: LogApplyWatchEventRow,
) -> tokio_rusqlite::Result<PendingWatchEvent> {
    let data_bytes = serde_json::to_vec(&row.data)
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
    insert_watch_event_in_conn(
        tx,
        WatchEventInsert::new(
            &row.api_version,
            &row.kind,
            row.namespace.as_deref(),
            &row.name,
            row.resource_version,
            &row.event_type,
            &data_bytes,
        ),
    )?;
    Ok(create_pending_watch_event(
        &row.api_version,
        &row.kind,
        row.namespace.as_deref(),
        &row.name,
        row.resource_version,
        &row.event_type,
        row.data,
    ))
}

fn put_node_subnet_row(
    tx: &rusqlite::Transaction<'_>,
    row: LogApplyNodeSubnetRow,
) -> tokio_rusqlite::Result<()> {
    tx.execute(
        queries::NODE_SUBNET_UPSERT_EXACT,
        rusqlite::params![
            row.node_name,
            row.subnet,
            i64::from(row.subnet_base_int),
            row.vtep_ip,
            row.vtep_mac,
            row.node_ip,
            row.mode,
            row.hostport_range
        ],
    )?;
    Ok(())
}

fn put_node_dataplane_row(
    tx: &rusqlite::Transaction<'_>,
    row: LogApplyNodeDataplaneRow,
) -> tokio_rusqlite::Result<()> {
    tx.execute(
        queries::NODE_DATAPLANE_UPSERT,
        rusqlite::params![
            row.node_name,
            row.mode,
            row.encryption,
            row.public_key,
            row.endpoint,
            row.port.map(i64::from),
            0i64
        ],
    )?;
    Ok(())
}

fn advance_metadata_rv_to_at_least_tx(
    tx: &rusqlite::Transaction<'_>,
    resource_version: i64,
) -> tokio_rusqlite::Result<()> {
    let current_rv: i64 = tx.query_row(queries::METADATA_SELECT_RV_INT, [], |row| row.get(0))?;
    if current_rv < resource_version {
        tx.execute(
            queries::METADATA_SET_RV,
            rusqlite::params![resource_version.to_string()],
        )?;
    }
    Ok(())
}

fn other_error(message: impl Into<String>) -> tokio_rusqlite::Error {
    tokio_rusqlite::Error::Other(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn subnet_commit(resource_version: i64, node_name: &str, subnet: Ipv4Addr) -> LogApplyCommit {
        LogApplyCommit::new(
            resource_version,
            vec![LogApplyMutation::PutNodeSubnet(LogApplyNodeSubnetRow {
                node_name: node_name.to_string(),
                subnet: format!("{subnet}/24"),
                subnet_base_int: u32::from(subnet),
                vtep_ip: subnet.to_string(),
                vtep_mac: None,
                node_ip: "192.0.2.1".to_string(),
                mode: "root".to_string(),
                hostport_range: None,
            })],
        )
    }

    fn dataplane_commit(
        resource_version: i64,
        node_name: &str,
        endpoint: &str,
        port: u16,
    ) -> LogApplyCommit {
        LogApplyCommit::new(
            resource_version,
            vec![LogApplyMutation::PutNodeDataplane(
                LogApplyNodeDataplaneRow {
                    node_name: node_name.to_string(),
                    mode: "root".to_string(),
                    encryption: "enabled".to_string(),
                    public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
                    endpoint: endpoint.to_string(),
                    port: Some(port),
                },
            )],
        )
    }

    fn node_commit(resource_version: i64, name: &str, uid: &str) -> LogApplyCommit {
        LogApplyCommit::new(
            resource_version,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "Node".to_string(),
                namespace: None,
                name: name.to_string(),
                uid: uid.to_string(),
                resource_version,
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {
                        "name": name,
                        "uid": uid,
                        "resourceVersion": resource_version.to_string()
                    }
                }),
                require_absent: false,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            })],
        )
    }

    #[tokio::test]
    async fn replace_replicated_resource_state_applies_and_prunes_peer_state() {
        let db = crate::datastore::test_support::in_memory().await;
        db.allocate_node_subnet("stale", "10.43.0.0/16", "192.0.2.200")
            .await
            .unwrap();
        db.update_node_dataplane(
            crate::networking::wireguard::DataplanePeerMetadata::try_new(
                "stale".to_string(),
                crate::networking::wireguard::DataplaneMode::Root,
                crate::networking::wireguard::DataplaneEncryption::Enabled,
                Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
                Some("192.0.2.200".to_string()),
                Some(51_820),
            )
            .unwrap(),
        )
        .await
        .unwrap();

        db.replace_replicated_resource_state(
            vec![
                subnet_commit(1, "leader", Ipv4Addr::new(10, 42, 5, 0)),
                dataplane_commit(2, "leader", "192.0.2.1", 51_820),
            ],
            2,
            None,
        )
        .await
        .unwrap();

        assert!(
            db.get_node_subnet("leader").await.unwrap().is_some(),
            "snapshot restore must apply replicated node_subnets rows"
        );
        assert!(
            db.get_node_dataplane("leader").await.unwrap().is_some(),
            "snapshot restore must apply replicated node_dataplane rows"
        );
        assert!(
            db.get_node_subnet("stale").await.unwrap().is_none(),
            "snapshot restore must remove local peer rows absent from the leader snapshot"
        );
        assert!(
            db.get_node_dataplane("stale").await.unwrap().is_none(),
            "snapshot restore must remove stale dataplane metadata absent from the leader snapshot"
        );
    }

    #[tokio::test]
    async fn replace_replicated_resource_state_applies_peer_rows_at_snapshot_rv() {
        let db = crate::datastore::test_support::in_memory().await;

        db.replace_replicated_resource_state(
            vec![
                node_commit(10, "worker", "node-uid"),
                subnet_commit(10, "worker", Ipv4Addr::new(10, 43, 0, 0)),
                dataplane_commit(10, "worker", "192.0.2.10", 7679),
            ],
            10,
            None,
        )
        .await
        .unwrap();

        assert_eq!(db.get_current_resource_version().await.unwrap(), 10);
        assert!(db.get_node_subnet("worker").await.unwrap().is_some());
        assert!(db.get_node_dataplane("worker").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn replace_replicated_resource_state_clears_stale_owner_ref_index_rows() {
        let db = crate::datastore::test_support::in_memory().await;
        let pod = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "stale",
                "namespace": "default",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "name": "stale-rs",
                    "uid": "stale-owner"
                }]
            },
            "spec": { "containers": [{ "name": "c", "image": "nginx" }] }
        });

        db.create_resource("v1", "Pod", Some("default"), "stale", pod)
            .await
            .unwrap();
        let before: i64 = db
            .db_call("test_count_owner_refs", |conn| {
                conn.query_row("SELECT COUNT(*) FROM resource_owner_refs", [], |row| {
                    row.get(0)
                })
                .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .unwrap();
        assert_eq!(before, 1);

        db.replace_replicated_resource_state(Vec::new(), 0, None)
            .await
            .unwrap();

        let after: i64 = db
            .db_call("test_count_owner_refs", |conn| {
                conn.query_row("SELECT COUNT(*) FROM resource_owner_refs", [], |row| {
                    row.get(0)
                })
                .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .unwrap();
        assert_eq!(
            after, 0,
            "snapshot replacement must clear owner-ref index rows for resources absent from the leader snapshot"
        );
    }

    #[tokio::test]
    async fn replace_replicated_resource_state_restores_created_rv_from_watch_history() {
        let db = crate::datastore::test_support::in_memory().await;

        db.replace_replicated_resource_state(
            vec![
                LogApplyCommit::new(
                    5,
                    vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                        api_version: "v1".to_string(),
                        kind: "ConfigMap".to_string(),
                        namespace: Some("default".to_string()),
                        name: "from-snapshot".to_string(),
                        uid: "cm-uid".to_string(),
                        resource_version: 5,
                        data: serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "ConfigMap",
                            "metadata": {
                                "name": "from-snapshot",
                                "namespace": "default",
                                "uid": "cm-uid",
                                "resourceVersion": "5"
                            },
                            "data": {"state": "current"}
                        }),
                        require_absent: false,
                        require_existing: false,
                        precondition_uid: None,
                        precondition_resource_version: None,
                        status_only: false,
                    })],
                ),
                LogApplyCommit::put_watch_event(LogApplyWatchEventRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "from-snapshot".to_string(),
                    resource_version: 2,
                    event_type: "ADDED".to_string(),
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "from-snapshot",
                            "namespace": "default",
                            "uid": "cm-uid",
                            "resourceVersion": "2"
                        },
                        "data": {"state": "created"}
                    }),
                }),
                LogApplyCommit::put_watch_event(LogApplyWatchEventRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "from-snapshot".to_string(),
                    resource_version: 5,
                    event_type: "MODIFIED".to_string(),
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "from-snapshot",
                            "namespace": "default",
                            "uid": "cm-uid",
                            "resourceVersion": "5"
                        },
                        "data": {"state": "current"}
                    }),
                }),
            ],
            5,
            None,
        )
        .await
        .unwrap();

        let created_rv: i64 = db
            .db_call("test_created_rv_after_snapshot", |conn| {
                conn.query_row(
                    "SELECT created_rv FROM namespaced_resources \
                     WHERE api_version = 'v1' AND kind = 'ConfigMap' \
                       AND namespace = 'default' AND name = 'from-snapshot'",
                    [],
                    |row| row.get(0),
                )
                .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .unwrap();
        assert_eq!(
            created_rv, 2,
            "snapshot restore must preserve the leader's resource creation RV"
        );
    }

    #[tokio::test]
    async fn stale_uid_delete_does_not_remove_same_name_replacement() {
        // T1.1: follower applies a stale DeleteResource commit whose
        // LogApplyResourceKey.uid points at an older (already-replaced)
        // Pod identity. The same-name replacement Pod with a different
        // UID must remain in cluster.db; the stale delete is a no-op.
        let db = crate::datastore::test_support::in_memory().await;

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "p1",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "p1",
                    "namespace": "default",
                    "uid": "pod-uid-A"
                }
            }),
        )
        .await
        .unwrap();

        db.apply_log_apply_commit(LogApplyCommit::new(
            5,
            vec![LogApplyMutation::DeleteResource(LogApplyResourceKey {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "p1".to_string(),
                uid: "pod-uid-A".to_string(),
                precondition_resource_version: None,
            })],
        ))
        .await
        .unwrap();

        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "p1",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "p1",
                    "namespace": "default",
                    "uid": "pod-uid-B"
                }
            }),
        )
        .await
        .unwrap();

        // Replay the stale UID-A delete commit. With UID-qualified
        // deletes this must be a no-op; without the guard it would hit
        // by (api_version, kind, namespace, name) and remove UID-B's row.
        db.apply_log_apply_commit(LogApplyCommit::new(
            7,
            vec![LogApplyMutation::DeleteResource(LogApplyResourceKey {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "p1".to_string(),
                uid: "pod-uid-A".to_string(),
                precondition_resource_version: None,
            })],
        ))
        .await
        .unwrap();

        let surviving = db
            .get_resource("v1", "Pod", Some("default"), "p1")
            .await
            .unwrap()
            .expect("replacement Pod with UID-B must survive stale UID-A delete");
        assert_eq!(
            surviving.uid, "pod-uid-B",
            "stale UID-A delete must not free the same-name slot for UID-B"
        );
    }

    #[tokio::test]
    async fn raft_apply_missing_required_resource_returns_terminal_result() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "stale-status",
            serde_json::json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "stale-status",
                    "namespace": "default",
                    "uid": "deploy-uid"
                },
                "spec": {"selector": {"matchLabels": {"app": "stale-status"}}},
                "status": {"replicas": 1}
            }),
        )
        .await
        .unwrap();
        db.delete_resource_with_preconditions(
            "apps/v1",
            "Deployment",
            Some("default"),
            "stale-status",
            crate::datastore::ResourcePreconditions::uid("deploy-uid"),
        )
        .await
        .unwrap();

        let result = db
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                10,
                vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "apps/v1".to_string(),
                    kind: "Deployment".to_string(),
                    namespace: Some("default".to_string()),
                    name: "stale-status".to_string(),
                    uid: "deploy-uid".to_string(),
                    resource_version: 10,
                    data: serde_json::json!({
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "metadata": {
                            "name": "stale-status",
                            "namespace": "default",
                            "uid": "deploy-uid",
                            "resourceVersion": "10"
                        },
                        "spec": {"selector": {"matchLabels": {"app": "stale-status"}}},
                        "status": {"replicas": 2}
                    }),
                    require_absent: false,
                    require_existing: true,
                    precondition_uid: Some("deploy-uid".to_string()),
                    precondition_resource_version: None,
                    status_only: false,
                })],
            ))
            .await
            .expect("raft apply must not fail committed log entry on stale missing resource");

        assert_eq!(result.applied_rv, None);
        assert!(
            result
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains("404 Not Found")),
            "missing resource must be returned as a terminal command result, got {result:?}"
        );
        assert!(
            db.get_resource("apps/v1", "Deployment", Some("default"), "stale-status")
                .await
                .unwrap()
                .is_none(),
            "stale apply must not recreate the deleted resource"
        );
    }

    #[tokio::test]
    async fn apply_log_apply_commit_broadcasts_explicit_watch_event() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "bound-pod",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "bound-pod",
                    "namespace": "default",
                    "uid": "pod-uid"
                },
                "spec": {"containers": [{"name": "c", "image": "pause"}]}
            }),
        )
        .await
        .unwrap();
        let mut watch_rx = db.subscribe_watch(crate::watch::WatchTopic::new("v1", "Pod"));

        let leader_watch_row = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "bound-pod",
                "namespace": "default",
                "uid": "pod-uid",
                "resourceVersion": "7"
            },
            "spec": {
                "nodeName": "mn-controlplane3",
                "containers": [{"name": "c", "image": "pause"}]
            }
        });

        db.apply_log_apply_commit(LogApplyCommit::new(
            7,
            vec![LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "bound-pod".to_string(),
                resource_version: 7,
                event_type: "MODIFIED".to_string(),
                data: leader_watch_row,
            })],
        ))
        .await
        .unwrap();

        let event = watch_rx
            .try_recv()
            .expect("explicit watch-history apply must wake local watchers");
        assert_eq!(event.event_type, crate::watch::EventType::Modified);
        assert_eq!(event.resource_version(), Some(7));
        assert_eq!(
            event
                .object
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str()),
            Some("mn-controlplane3")
        );
    }

    #[tokio::test]
    async fn apply_log_apply_commit_replays_explicit_watch_event_without_synthesizing() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "exact-watch",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "exact-watch",
                    "namespace": "default",
                    "uid": "cm-uid"
                },
                "data": {"state": "initial"}
            }),
        )
        .await
        .unwrap();

        let resource_row = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "exact-watch",
                "namespace": "default",
                "uid": "cm-uid",
                "resourceVersion": "7"
            },
            "data": {"state": "current-row"}
        });
        let leader_watch_row = serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "exact-watch",
                "namespace": "default",
                "uid": "cm-uid",
                "resourceVersion": "7"
            },
            "data": {"state": "leader-watch-history"}
        });

        db.apply_log_apply_commit(LogApplyCommit::new(
            7,
            vec![
                LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "exact-watch".to_string(),
                    uid: "cm-uid".to_string(),
                    resource_version: 7,
                    data: resource_row,
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                }),
                LogApplyMutation::PutWatchEvent(LogApplyWatchEventRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "exact-watch".to_string(),
                    resource_version: 7,
                    event_type: "MODIFIED".to_string(),
                    data: leader_watch_row.clone(),
                }),
            ],
        ))
        .await
        .unwrap();

        let watch_data: String = db
            .db_call("test_exact_watch_history_after_log_apply", |conn| {
                conn.query_row(
                    "SELECT CAST(data AS TEXT) FROM watch_events WHERE resource_version = 7",
                    [],
                    |row| row.get(0),
                )
                .map_err(tokio_rusqlite::Error::from)
            })
            .await
            .unwrap();
        assert_eq!(watch_data, leader_watch_row.to_string());
    }
}
