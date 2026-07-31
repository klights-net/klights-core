use super::state::{ApplyEffects, RaftClusterStateApplier, resolve_noop_put_resource_in_tx};
use super::{queries, transaction_primitives};
use klights_cluster_core::{
    ClusterMutation, LogApplyCommit, LogApplyMutation, OutboxStreamWatermark, Resource,
    SnapshotRestoreOperation, WatchReplayPosition,
};
use klights_cluster_store::StagedPostCommit;
use rusqlite::OptionalExtension;

pub struct RaftLogApplyOutcome {
    pub committed_outcome: klights_cluster_core::CommittedApplyOutcome,
    pub returned_resource: Option<Resource>,
    pub pod_endpoint_effect: klights_cluster_core::PodEndpointEffect,
    pub pending: Vec<StagedPostCommit>,
}

impl RaftLogApplyOutcome {
    fn try_new(
        committed_outcome: klights_cluster_core::CommittedApplyOutcome,
        pending: Vec<StagedPostCommit>,
        pod_endpoint_effect: klights_cluster_core::PodEndpointEffect,
    ) -> tokio_rusqlite::Result<Self> {
        let returned_resource = match &committed_outcome {
            klights_cluster_core::CommittedApplyOutcome::Visible { resource, .. } => {
                resource.clone()
            }
            _ => None,
        };
        Ok(Self {
            committed_outcome,
            returned_resource,
            pod_endpoint_effect,
            pending,
        })
    }

    fn with_returned_resource(mut self, resource: Option<Resource>) -> Self {
        self.returned_resource = resource;
        self
    }
}

fn pod_status_target(commit: &LogApplyCommit) -> Option<(Option<String>, String)> {
    commit
        .mutations()
        .iter()
        .find_map(|mutation| match mutation {
            LogApplyMutation::PutResource(row)
                if row.api_version == "v1" && row.kind == "Pod" && row.status_only =>
            {
                Some((row.namespace.clone(), row.name.clone()))
            }
            _ => None,
        })
}

struct ApplyCommit {
    resource_version: i64,
    outbox_watermark: Option<OutboxStreamWatermark>,
    mutations: Vec<LogApplyMutation>,
    preserve_historical_bytes: bool,
}

impl ApplyCommit {
    fn from_live(commit: LogApplyCommit) -> tokio_rusqlite::Result<Self> {
        commit
            .validate_live_template()
            .map_err(|error| other_error(error.to_string()))?;
        let (resource_version, outbox_watermark, mutations) = commit.into_parts();
        Ok(Self {
            resource_version,
            outbox_watermark,
            mutations,
            preserve_historical_bytes: false,
        })
    }
}

impl From<SnapshotRestoreOperation> for ApplyCommit {
    fn from(operation: SnapshotRestoreOperation) -> Self {
        let (resource_version, outbox_watermark, mutations) = operation.into_parts();
        Self {
            resource_version,
            outbox_watermark,
            mutations,
            preserve_historical_bytes: true,
        }
    }
}

fn pod_state_in_tx(
    tx: &rusqlite::Transaction<'_>,
    target: Option<&(Option<String>, String)>,
) -> tokio_rusqlite::Result<Option<serde_json::Value>> {
    let Some((namespace, name)) = target else {
        return Ok(None);
    };
    let bytes = match namespace.as_deref() {
        Some(namespace) => tx
            .query_row(
                queries::NAMESPACED_GET_DATA_FOR_DELETE,
                rusqlite::params!["v1", "Pod", namespace, name],
                |row| row.get::<_, Vec<u8>>(2),
            )
            .optional()?,
        None => tx
            .query_row(
                queries::CLUSTER_GET_DATA_FOR_DELETE,
                rusqlite::params!["v1", "Pod", name],
                |row| row.get::<_, Vec<u8>>(2),
            )
            .optional()?,
    };
    bytes
        .map(|bytes| {
            serde_json::from_slice(&bytes).map_err(super::mutation_helpers::serde_to_sqlite_error)
        })
        .transpose()
}

fn pod_endpoint_effect(
    target: Option<&(Option<String>, String)>,
    before: Option<&serde_json::Value>,
    after: Option<&serde_json::Value>,
) -> klights_cluster_core::PodEndpointEffect {
    if target.is_none() {
        return klights_cluster_core::PodEndpointEffect::NotApplicable;
    }
    if before.zip(after).is_some_and(|(before, after)| {
        klights_cluster_core::pod_endpoint_state(before)
            .differs_from(&klights_cluster_core::pod_endpoint_state(after))
    }) {
        klights_cluster_core::PodEndpointEffect::Changed
    } else {
        klights_cluster_core::PodEndpointEffect::Unchanged
    }
}

pub fn apply_commit_in_tx_for_raft_with_context(
    tx: &rusqlite::Transaction<'_>,
    commit: LogApplyCommit,
    context: &super::TransactionContext<'_>,
) -> tokio_rusqlite::Result<RaftLogApplyOutcome> {
    let pod_target = pod_status_target(&commit);
    let pod_before = pod_state_in_tx(tx, pod_target.as_ref())?;
    commit
        .validate_live_template()
        .map_err(|error| other_error(error.to_string()))?;
    if commit.mutations().is_empty() && commit.outbox_watermark().is_none() {
        return RaftLogApplyOutcome::try_new(
            klights_cluster_core::CommittedApplyOutcome::NoPublicChange {
                resource_version: transaction_primitives::current_resource_version(tx)?,
                reason: klights_cluster_core::NoPublicChangeReason::LedgerOnly,
            },
            Vec::new(),
            klights_cluster_core::PodEndpointEffect::NotApplicable,
        );
    }
    let before_position = WatchReplayPosition {
        resource_version: transaction_primitives::current_resource_version(tx)?,
        event_id: transaction_primitives::watch_event_allocator_high_water(tx)?,
        resource_version_filter_through_event_id: 0,
    };
    let outbox_template = commit
        .mutations()
        .iter()
        .find_map(|mutation| match mutation {
            LogApplyMutation::PutAppliedOutbox(row) => Some(row.clone()),
            _ => None,
        });
    let terminal_watermark = commit.outbox_watermark().cloned();
    if let Some(template) = outbox_template.as_ref()
        && let Some(existing) = applied_outbox_record_in_tx(tx, &template.idempotency_key)?
    {
        let (resource_version, error_message, returned_resource) =
            receipt_from_applied_outbox(&existing, context)?;
        if let Some(message) = error_message {
            let outcome = klights_cluster_core::CommittedApplyOutcome::Rejected(
                committed_rejection_from_message(message),
            );
            return RaftLogApplyOutcome::try_new(
                outcome,
                Vec::new(),
                pod_endpoint_effect(
                    pod_target.as_ref(),
                    pod_before.as_ref(),
                    pod_before.as_ref(),
                ),
            );
        }
        let resource_version = resource_version.ok_or_else(|| {
            other_error("duplicate applied_outbox row has no applied resourceVersion")
        })?;
        return Ok(RaftLogApplyOutcome::try_new(
            klights_cluster_core::CommittedApplyOutcome::NoPublicChange {
                resource_version,
                reason: klights_cluster_core::NoPublicChangeReason::DuplicateIdempotencyKey,
            },
            Vec::new(),
            pod_endpoint_effect(
                pod_target.as_ref(),
                pod_before.as_ref(),
                pod_before.as_ref(),
            ),
        )?
        .with_returned_resource(returned_resource));
    }

    // A duplicate watermark has already been applied. Do not allocate a V1
    // public RV for an entry that will have no visible effect.
    if matches!(
        outbox_watermark_decision_in_tx(tx, commit.outbox_watermark())?,
        klights_cluster_core::OutboxWatermarkDecision::Duplicate
    ) {
        return RaftLogApplyOutcome::try_new(
            klights_cluster_core::CommittedApplyOutcome::NoPublicChange {
                resource_version: transaction_primitives::current_resource_version(tx)?,
                reason: klights_cluster_core::NoPublicChangeReason::DuplicateWatermark,
            },
            Vec::new(),
            pod_endpoint_effect(
                pod_target.as_ref(),
                pod_before.as_ref(),
                pod_before.as_ref(),
            ),
        );
    }

    if let Some((subject_key, incoming_stamp)) =
        klights_cluster_core::stamped_pod_status_subject_and_stamp(&commit)
    {
        let last_applied_stamp: Option<i64> = tx.query_row(
            queries::APPLIED_OUTBOX_MAX_STATUS_STAMP_FOR_SUBJECT,
            rusqlite::params![subject_key],
            |row| row.get::<_, Option<i64>>(0),
        )?;

        if klights_cluster_core::decide_status_stamp(last_applied_stamp, Some(incoming_stamp))
            == klights_cluster_core::StatusStampDecision::RecordLedgerOnly
        {
            let (applied_rv, pending, _applied_mutation) = apply_commit_in_tx_with_watch_events(
                tx,
                {
                    let mut outbox_commit = ApplyCommit::from_live(
                        klights_cluster_core::commit_with_outbox_rows_only(commit),
                    )?;
                    outbox_commit.resource_version =
                        transaction_primitives::current_resource_version(tx)?;
                    outbox_commit
                },
                true,
                context,
            )?;
            let reason = match last_applied_stamp.cmp(&Some(incoming_stamp)) {
                std::cmp::Ordering::Greater => {
                    klights_cluster_core::NoPublicChangeReason::StaleStatusStamp
                }
                std::cmp::Ordering::Equal => {
                    klights_cluster_core::NoPublicChangeReason::EqualStatusStamp
                }
                std::cmp::Ordering::Less => {
                    return Err(other_error(
                        "status-stamp decision recorded ledger-only for a newer stamp",
                    ));
                }
            };
            return RaftLogApplyOutcome::try_new(
                klights_cluster_core::CommittedApplyOutcome::NoPublicChange {
                    resource_version: applied_rv,
                    reason,
                },
                pending,
                pod_endpoint_effect(
                    pod_target.as_ref(),
                    pod_before.as_ref(),
                    pod_before.as_ref(),
                ),
            );
        }
    }

    tx.execute("SAVEPOINT raft_apply_attempt", [])?;
    match apply_commit_in_tx_returning_rv_and_mutation_with_context(tx, commit, context) {
        Ok((rv, pending, applied_mutation)) => {
            if let (Some(template), Some(resource)) =
                (outbox_template.as_ref(), applied_mutation.as_ref())
                && template.operation == klights_cluster_core::log_apply::POD_METADATA_OPERATION
                && resource.api_version == "v1"
                && resource.kind == "Pod"
            {
                let result_proto = context
                    .encode(&klights_cluster_core::command::StorageResponse::Resource {
                        resource_version: resource.resource_version,
                        data: (*resource.data).clone(),
                    })
                    .map_err(|error| {
                        other_error(format!(
                            "failed to encode durable actor-finalization receipt: {error}"
                        ))
                    })?;
                tx.execute(
                    queries::APPLIED_OUTBOX_UPDATE_RESULT,
                    rusqlite::params![
                        &template.idempotency_key,
                        &template.subject_key,
                        rv,
                        result_proto,
                        template.status_stamp
                    ],
                )?;
                if tx.changes() != 1 {
                    return Err(other_error(
                        "committed actor-finalization receipt had no applied_outbox ledger row",
                    ));
                }
            }
            tx.execute("RELEASE raft_apply_attempt", [])?;
            let after_position = WatchReplayPosition {
                resource_version: transaction_primitives::current_resource_version(tx)?,
                event_id: transaction_primitives::watch_event_allocator_high_water(tx)?,
                resource_version_filter_through_event_id: 0,
            };
            let visible_change = after_position.resource_version > before_position.resource_version
                || after_position.event_id > before_position.event_id;
            let resource = applied_mutation;
            let outcome = if visible_change || resource.is_some() {
                klights_cluster_core::CommittedApplyOutcome::Visible {
                    resource_version: rv,
                    resource,
                }
            } else {
                klights_cluster_core::CommittedApplyOutcome::NoPublicChange {
                    resource_version: rv,
                    reason: klights_cluster_core::NoPublicChangeReason::LedgerOnly,
                }
            };
            let pod_after = pod_state_in_tx(tx, pod_target.as_ref())?;
            RaftLogApplyOutcome::try_new(
                outcome,
                pending,
                pod_endpoint_effect(pod_target.as_ref(), pod_before.as_ref(), pod_after.as_ref()),
            )
        }
        Err(err) if is_terminal_apply_conflict(&err) => {
            tx.execute("ROLLBACK TO raft_apply_attempt", [])?;
            tx.execute("RELEASE raft_apply_attempt", [])?;
            let message = err.to_string();
            let rejection = committed_rejection_from_conflict(&err, message.clone())?;
            if let Some(watermark) = terminal_watermark.as_ref() {
                upsert_outbox_watermark_in_tx(tx, watermark)?;
            }
            if let Some(mut row) = outbox_template {
                row.applied_rv = None;
                row.result_proto = context
                    .encode(&klights_cluster_core::command::StorageResponse::Error {
                        message: message.clone(),
                    })
                    .unwrap_or_default();
                RaftClusterStateApplier::new(tx).put_applied_outbox(row)?;
            }
            RaftLogApplyOutcome::try_new(
                klights_cluster_core::CommittedApplyOutcome::Rejected(rejection),
                Vec::new(),
                pod_endpoint_effect(
                    pod_target.as_ref(),
                    pod_before.as_ref(),
                    pod_before.as_ref(),
                ),
            )
        }
        Err(err) => {
            tx.execute("ROLLBACK TO raft_apply_attempt", [])?;
            tx.execute("RELEASE raft_apply_attempt", [])?;
            Err(err)
        }
    }
}

fn committed_rejection_from_conflict(
    error: &tokio_rusqlite::Error,
    message: String,
) -> tokio_rusqlite::Result<klights_cluster_core::CommittedApplyRejection> {
    let tokio_rusqlite::Error::Other(inner) = error else {
        return Err(other_error(
            "terminal committed-apply rejection had no typed conflict",
        ));
    };
    let conflict = inner
        .downcast_ref::<ApplyConflictError>()
        .ok_or_else(|| other_error("terminal committed-apply rejection had no typed conflict"))?;
    Ok(match conflict.code {
        ApplyConflictCode::NotFound => {
            klights_cluster_core::CommittedApplyRejection::NotFound { message }
        }
        ApplyConflictCode::AlreadyExists => {
            klights_cluster_core::CommittedApplyRejection::AlreadyExists { message }
        }
        ApplyConflictCode::UidPrecondition => {
            klights_cluster_core::CommittedApplyRejection::UidConflict { message }
        }
        ApplyConflictCode::ResourceVersionPrecondition => {
            klights_cluster_core::CommittedApplyRejection::ResourceVersionConflict { message }
        }
    })
}

fn committed_rejection_from_message(
    message: String,
) -> klights_cluster_core::CommittedApplyRejection {
    if message.contains("resourceVersion") {
        klights_cluster_core::CommittedApplyRejection::ResourceVersionConflict { message }
    } else if message.contains("UID") || message.contains("uid") {
        klights_cluster_core::CommittedApplyRejection::UidConflict { message }
    } else if message.contains("already exists") {
        klights_cluster_core::CommittedApplyRejection::AlreadyExists { message }
    } else if message.contains("not found") {
        klights_cluster_core::CommittedApplyRejection::NotFound { message }
    } else {
        klights_cluster_core::CommittedApplyRejection::InvalidCommit { message }
    }
}

pub fn apply_commit_in_tx_with_context(
    tx: &rusqlite::Transaction<'_>,
    commit: LogApplyCommit,
    context: &super::TransactionContext<'_>,
) -> tokio_rusqlite::Result<Vec<StagedPostCommit>> {
    let (_applied_rv, pending) = apply_commit_in_tx_returning_rv_with_context(tx, commit, context)?;
    Ok(pending)
}

pub(crate) fn apply_commit_in_tx_returning_rv_with_context(
    tx: &rusqlite::Transaction<'_>,
    commit: LogApplyCommit,
    context: &super::TransactionContext<'_>,
) -> tokio_rusqlite::Result<(i64, Vec<StagedPostCommit>)> {
    let (applied_rv, pending, _applied_mutation) =
        apply_commit_in_tx_returning_rv_and_mutation_with_context(tx, commit, context)?;
    Ok((applied_rv, pending))
}

pub fn apply_commit_in_tx_returning_rv_and_mutation_with_context(
    tx: &rusqlite::Transaction<'_>,
    commit: LogApplyCommit,
    context: &super::TransactionContext<'_>,
) -> tokio_rusqlite::Result<(i64, Vec<StagedPostCommit>, Option<Resource>)> {
    let has_explicit_watch_history = commit
        .mutations()
        .iter()
        .any(|mutation| matches!(mutation, LogApplyMutation::PutWatchEvent(_)));
    apply_commit_in_tx_with_watch_events(
        tx,
        ApplyCommit::from_live(commit)?,
        !has_explicit_watch_history,
        context,
    )
}

pub fn apply_snapshot_restore_operation_in_tx(
    tx: &rusqlite::Transaction<'_>,
    operation: SnapshotRestoreOperation,
    emit_watch_events: bool,
    context: &super::TransactionContext<'_>,
) -> tokio_rusqlite::Result<(i64, Vec<StagedPostCommit>, Option<Resource>)> {
    apply_commit_in_tx_with_watch_events(
        tx,
        ApplyCommit::from(operation),
        emit_watch_events,
        context,
    )
}

fn apply_commit_in_tx_with_watch_events(
    tx: &rusqlite::Transaction<'_>,
    commit: ApplyCommit,
    emit_watch_events: bool,
    context: &super::TransactionContext<'_>,
) -> tokio_rusqlite::Result<(i64, Vec<StagedPostCommit>, Option<Resource>)> {
    if commit.resource_version < 0 {
        return Err(other_error(
            "log_apply commit resourceVersion must be non-negative",
        ));
    }
    let commit = resolve_bound_pod_finalizations_in_tx(tx, commit)?;
    let emit_watch_events = emit_watch_events
        && !commit
            .mutations
            .iter()
            .any(|mutation| matches!(mutation, LogApplyMutation::PutWatchEvent(_)));
    // Watermark gaps and duplicates retain precedence over resource CAS. Only
    // an applicable stream position may inspect and collapse a resource put.
    let watermark_decision = outbox_watermark_decision_in_tx(tx, commit.outbox_watermark.as_ref())?;
    let commit = if matches!(
        watermark_decision,
        klights_cluster_core::OutboxWatermarkDecision::Apply
    ) {
        resolve_noop_put_resources_in_tx(tx, commit)?
    } else {
        commit
    };
    let commit = stamp_provisional_resource_version_in_tx(tx, commit, context)?;
    let applied_rv = commit.resource_version;
    let watermark = commit.outbox_watermark.clone();
    let watermark_only_snapshot_restore = watermark.is_some() && commit.mutations.is_empty();
    if watermark_only_snapshot_restore {
        if let Some(watermark) = watermark.as_ref() {
            upsert_outbox_watermark_in_tx(tx, watermark)?;
        }
        advance_metadata_rv_to_at_least_tx(tx, commit.resource_version)?;
        return Ok((applied_rv, Vec::new(), None));
    }
    match watermark_decision {
        klights_cluster_core::OutboxWatermarkDecision::Duplicate => {
            advance_metadata_rv_to_at_least_tx(tx, commit.resource_version)?;
            return Ok((applied_rv, Vec::new(), None));
        }
        klights_cluster_core::OutboxWatermarkDecision::Gap { last_seq, next_seq } => {
            return Err(other_error(format!(
                "outbox stream gap for seq {next_seq}: last committed seq is {last_seq}"
            )));
        }
        klights_cluster_core::OutboxWatermarkDecision::Apply => {}
    }
    let mutation_count = commit.mutations.len();
    let deleted_resource = deleted_resource_from_stamped_commit(&commit)?;
    let returned_resource_target = returned_resource_target(&commit);
    let apply_start = std::time::Instant::now();
    let mut effects = ApplyEffects::new();
    let mut applier = RaftClusterStateApplier::new(tx);
    for mutation in commit.mutations {
        applier.apply_cluster_mutation(
            commit.resource_version,
            ClusterMutation::from(mutation),
            emit_watch_events,
            &mut effects,
        )?;
    }
    let applied_mutation = match deleted_resource {
        Some(resource) => Some(resource),
        None => returned_resource_target
            .as_ref()
            .map(|target| read_returned_resource_in_tx(tx, target))
            .transpose()?
            .flatten(),
    };
    if let Some(watermark) = watermark.as_ref() {
        upsert_outbox_watermark_in_tx(tx, watermark)?;
    }
    advance_metadata_rv_to_at_least_tx(tx, commit.resource_version)?;
    let pending = effects.into_pending_watch_events();
    log_slow_log_apply_commit(
        apply_start.elapsed(),
        commit.resource_version,
        mutation_count,
        pending.len(),
        emit_watch_events,
    );
    Ok((applied_rv, pending, applied_mutation))
}

fn resolve_noop_put_resources_in_tx(
    tx: &rusqlite::Transaction<'_>,
    mut commit: ApplyCommit,
) -> tokio_rusqlite::Result<ApplyCommit> {
    if commit.preserve_historical_bytes {
        return Ok(commit);
    }
    let mutations = std::mem::take(&mut commit.mutations);
    let mut resolved = Vec::with_capacity(mutations.len());
    let mut removed_noop = false;
    for mutation in mutations {
        match mutation {
            LogApplyMutation::PutResource(row) => {
                if let Some(row) = resolve_noop_put_resource_in_tx(tx, row)? {
                    resolved.push(LogApplyMutation::PutResource(row));
                } else {
                    removed_noop = true;
                }
            }
            mutation => resolved.push(mutation),
        }
    }
    if removed_noop
        && resolved
            .iter()
            .all(|mutation| matches!(mutation, LogApplyMutation::PutAppliedOutbox(_)))
    {
        // Ledger/watermark durability records the already-visible RV without
        // consuming another public RV for the removed resource no-op.
        commit.resource_version = transaction_primitives::current_resource_version(tx)?;
    }
    commit.mutations = resolved;
    Ok(commit)
}

fn resolve_bound_pod_finalizations_in_tx(
    tx: &rusqlite::Transaction<'_>,
    mut commit: ApplyCommit,
) -> tokio_rusqlite::Result<ApplyCommit> {
    let mutations = std::mem::take(&mut commit.mutations);
    let mut resolved = Vec::with_capacity(mutations.len().saturating_add(1));
    for mutation in mutations {
        let LogApplyMutation::FinalizeBoundPod(finalization) = mutation else {
            resolved.push(mutation);
            continue;
        };
        let current = tx
            .query_row(
                queries::NAMESPACED_GET,
                rusqlite::params!["v1", "Pod", &finalization.namespace, &finalization.name],
                |row| {
                    Ok((
                        row.get::<_, i64>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, Vec<u8>>(7)?,
                    ))
                },
            )
            .optional()?;
        let Some((current_rv, current_uid, data_bytes)) = current else {
            continue;
        };
        if current_uid != finalization.pod_uid {
            continue;
        }
        let data: serde_json::Value = serde_json::from_slice(&data_bytes)
            .map_err(super::mutation_helpers::serde_to_sqlite_error)?;
        let assigned_node = data
            .pointer("/spec/nodeName")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty());
        let has_finalizers = data
            .pointer("/metadata/finalizers")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|finalizers| !finalizers.is_empty());
        let terminating = data
            .pointer("/metadata/deletionTimestamp")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.is_empty())
            || (data
                .pointer("/status/phase")
                .and_then(serde_json::Value::as_str)
                == Some("Failed")
                && data
                    .pointer("/status/reason")
                    .and_then(serde_json::Value::as_str)
                    == Some("NodeLost"));
        if assigned_node != Some(finalization.node_name.as_str()) || has_finalizers || !terminating
        {
            continue;
        }
        resolved.push(LogApplyMutation::PutWatchEvent(
            klights_cluster_core::LogApplyWatchEventRow {
                event_id: None,
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some(finalization.namespace.clone()),
                name: finalization.name.clone(),
                resource_version: commit.resource_version,
                event_type: "DELETED".to_string(),
                data: super::resource_shape::hydrate_watch_event_data(
                    data,
                    "v1",
                    "Pod",
                    Some(finalization.namespace.as_str()),
                    &finalization.name,
                    commit.resource_version,
                ),
            },
        ));
        resolved.push(LogApplyMutation::DeleteResource(
            klights_cluster_core::LogApplyResourceKey {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some(finalization.namespace),
                name: finalization.name,
                uid: current_uid,
                precondition_resource_version: Some(current_rv),
            },
        ));
    }
    commit.mutations = resolved;
    Ok(commit)
}

#[derive(Clone, Debug)]
enum ReturnedResourceTarget {
    Resource {
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
    },
    Namespace {
        name: String,
    },
}

fn returned_resource_target(commit: &ApplyCommit) -> Option<ReturnedResourceTarget> {
    commit.mutations.iter().find_map(|mutation| match mutation {
        LogApplyMutation::PutResource(row) => Some(ReturnedResourceTarget::Resource {
            api_version: row.api_version.clone(),
            kind: row.kind.clone(),
            namespace: row.namespace.clone(),
            name: row.name.clone(),
        }),
        LogApplyMutation::PatchResourceLatest(patch) => Some(ReturnedResourceTarget::Resource {
            api_version: patch.api_version.clone(),
            kind: patch.kind.clone(),
            namespace: patch.namespace.clone(),
            name: patch.name.clone(),
        }),
        LogApplyMutation::PutNamespace(row) => Some(ReturnedResourceTarget::Namespace {
            name: row.name.clone(),
        }),
        _ => None,
    })
}

fn read_returned_resource_in_tx(
    tx: &rusqlite::Transaction<'_>,
    target: &ReturnedResourceTarget,
) -> tokio_rusqlite::Result<Option<Resource>> {
    match target {
        ReturnedResourceTarget::Resource {
            api_version,
            kind,
            namespace: Some(namespace),
            name,
        } => tx
            .query_row(
                queries::NAMESPACED_GET,
                rusqlite::params![api_version, kind, namespace, name],
                |row| {
                    let data: Vec<u8> = row.get(7)?;
                    Ok(Resource {
                        id: row.get(0)?,
                        api_version: row.get(1)?,
                        kind: row.get(2)?,
                        namespace: row.get(3)?,
                        name: row.get(4)?,
                        resource_version: row.get(5)?,
                        uid: row.get(6)?,
                        data: std::sync::Arc::new(serde_json::from_slice(&data).map_err(
                            |error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    data.len(),
                                    rusqlite::types::Type::Blob,
                                    Box::new(error),
                                )
                            },
                        )?),
                    })
                },
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from),
        ReturnedResourceTarget::Resource {
            api_version,
            kind,
            namespace: None,
            name,
        } => tx
            .query_row(
                queries::CLUSTER_GET,
                rusqlite::params![api_version, kind, name],
                |row| {
                    let data: Vec<u8> = row.get(6)?;
                    Ok(Resource {
                        id: row.get(0)?,
                        api_version: row.get(1)?,
                        kind: row.get(2)?,
                        namespace: None,
                        name: row.get(3)?,
                        resource_version: row.get(4)?,
                        uid: row.get(5)?,
                        data: std::sync::Arc::new(serde_json::from_slice(&data).map_err(
                            |error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    data.len(),
                                    rusqlite::types::Type::Blob,
                                    Box::new(error),
                                )
                            },
                        )?),
                    })
                },
            )
            .optional()
            .map_err(tokio_rusqlite::Error::from),
        ReturnedResourceTarget::Namespace { name } => tx
            .query_row(queries::NAMESPACE_GET, [name], |row| {
                let data: Vec<u8> = row.get(3)?;
                Ok(Resource {
                    id: 0,
                    api_version: "v1".to_string(),
                    kind: "Namespace".to_string(),
                    namespace: None,
                    name: row.get(0)?,
                    resource_version: row.get(1)?,
                    uid: row.get(2)?,
                    data: std::sync::Arc::new(serde_json::from_slice(&data).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            data.len(),
                            rusqlite::types::Type::Blob,
                            Box::new(error),
                        )
                    })?),
                })
            })
            .optional()
            .map_err(tokio_rusqlite::Error::from),
    }
}

fn deleted_resource_from_stamped_commit(
    commit: &ApplyCommit,
) -> tokio_rusqlite::Result<Option<Resource>> {
    let Some(deleted_key) = commit.mutations.iter().find_map(|mutation| match mutation {
        LogApplyMutation::DeleteResource(key) => Some(key),
        _ => None,
    }) else {
        return Ok(None);
    };
    let Some(watch_row) = commit.mutations.iter().find_map(|mutation| match mutation {
        LogApplyMutation::PutWatchEvent(row)
            if row.event_type == "DELETED"
                && row.api_version == deleted_key.api_version
                && row.kind == deleted_key.kind
                && row.namespace == deleted_key.namespace
                && row.name == deleted_key.name
                && row.resource_version == commit.resource_version =>
        {
            Some(row)
        }
        _ => None,
    }) else {
        return Ok(None);
    };
    let source_resource_version = deleted_key
        .precondition_resource_version
        .unwrap_or(watch_row.resource_version);
    let mut data = watch_row.data.clone();
    if let Some(metadata) = data
        .pointer_mut("/metadata")
        .and_then(serde_json::Value::as_object_mut)
    {
        metadata.remove("resourceVersion");
    }
    Ok(Some(Resource {
        id: 0,
        api_version: watch_row.api_version.clone(),
        kind: watch_row.kind.clone(),
        namespace: watch_row.namespace.clone(),
        name: watch_row.name.clone(),
        uid: Resource::uid_from_data(&watch_row.data),
        resource_version: source_resource_version,
        data: std::sync::Arc::new(data),
    }))
}

fn outbox_watermark_decision_in_tx(
    tx: &rusqlite::Transaction<'_>,
    watermark: Option<&OutboxStreamWatermark>,
) -> tokio_rusqlite::Result<klights_cluster_core::OutboxWatermarkDecision> {
    let Some(watermark) = watermark else {
        return klights_cluster_core::decide_outbox_watermark(None, None)
            .map_err(|err| other_error(err.to_string()));
    };
    klights_cluster_core::decide_outbox_watermark(None, Some(watermark))
        .map_err(|err| other_error(err.to_string()))?;
    let last_seq: Option<i64> = tx
        .query_row(
            "SELECT last_seq FROM outbox_stream_watermarks WHERE client_id = ?1 AND stream_id = ?2",
            rusqlite::params![&watermark.client_id, watermark.stream_id],
            |row| row.get(0),
        )
        .optional()?;
    klights_cluster_core::decide_outbox_watermark(last_seq, Some(watermark))
        .map_err(|err| other_error(err.to_string()))
}

fn upsert_outbox_watermark_in_tx(
    tx: &rusqlite::Transaction<'_>,
    watermark: &OutboxStreamWatermark,
) -> tokio_rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO outbox_stream_watermarks (client_id, stream_id, last_seq) VALUES (?1, ?2, ?3) \
         ON CONFLICT(client_id, stream_id) DO UPDATE SET last_seq = excluded.last_seq",
        rusqlite::params![
            &watermark.client_id,
            watermark.stream_id,
            watermark.stream_seq
        ],
    )?;
    Ok(())
}

fn stamp_provisional_resource_version_in_tx(
    tx: &rusqlite::Transaction<'_>,
    mut commit: ApplyCommit,
    context: &super::TransactionContext<'_>,
) -> tokio_rusqlite::Result<ApplyCommit> {
    let is_outbox_ledger_only = !commit.mutations.is_empty()
        && commit
            .mutations
            .iter()
            .all(|mutation| matches!(mutation, LogApplyMutation::PutAppliedOutbox(_)));
    let rv = if commit.resource_version == 0 && !is_outbox_ledger_only {
        transaction_primitives::next_resource_version_in_tx(tx)?
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
                if row.resource_version == rv && !commit.preserve_historical_bytes {
                    row.data = super::resource_shape::hydrate_watch_event_data(
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
                if row.resource_version == rv && !commit.preserve_historical_bytes {
                    row.data = super::resource_shape::hydrate_watch_event_data(
                        std::mem::take(&mut row.data),
                        "v1",
                        "Namespace",
                        None,
                        &row.name,
                        rv,
                    );
                }
            }
            LogApplyMutation::PutWatchEvent(row) => {
                if row.resource_version == 0 {
                    row.resource_version = rv;
                }
                if row.resource_version == rv && !commit.preserve_historical_bytes {
                    row.data = super::resource_shape::hydrate_watch_event_data(
                        std::mem::take(&mut row.data),
                        &row.api_version,
                        &row.kind,
                        row.namespace.as_deref(),
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
                    || context.decode(&row.result_proto).is_ok_and(|response| {
                        matches!(
                            response,
                            klights_cluster_core::command::StorageResponse::Ack { .. }
                        )
                    })
                    || context.decode(&row.result_proto).is_ok_and(|response| {
                        matches!(
                            response,
                            klights_cluster_core::command::StorageResponse::Ack {
                                resource_version: 0
                            }
                        )
                    })
                {
                    row.result_proto = context
                        .encode(&klights_cluster_core::command::StorageResponse::Ack {
                            resource_version: rv,
                        })
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

fn applied_outbox_record_in_tx(
    tx: &rusqlite::Transaction<'_>,
    idempotency_key: &str,
) -> tokio_rusqlite::Result<Option<klights_cluster_core::LogApplyAppliedOutboxRow>> {
    tx.query_row(queries::APPLIED_OUTBOX_GET, [idempotency_key], |row| {
        Ok(klights_cluster_core::LogApplyAppliedOutboxRow {
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

fn receipt_from_applied_outbox(
    row: &klights_cluster_core::LogApplyAppliedOutboxRow,
    context: &super::TransactionContext<'_>,
) -> tokio_rusqlite::Result<(Option<i64>, Option<String>, Option<Resource>)> {
    match context.decode(&row.result_proto) {
        Ok(klights_cluster_core::command::StorageResponse::Error { message }) => {
            Ok((row.applied_rv, Some(message), None))
        }
        Ok(klights_cluster_core::command::StorageResponse::Resource {
            resource_version,
            data,
        }) => {
            let mut resource = Resource::try_from_data(std::sync::Arc::new(data))
                .map_err(|error| other_error(error.to_string()))?;
            resource.resource_version = resource_version;
            Ok((row.applied_rv, None, Some(resource)))
        }
        Ok(_) => Ok((row.applied_rv, None, None)),
        Err(err) => Err(other_error(format!(
            "failed to decode applied_outbox result: {err}"
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyConflictCode {
    NotFound,
    AlreadyExists,
    UidPrecondition,
    ResourceVersionPrecondition,
}

#[derive(Debug)]
struct ApplyConflictError {
    code: ApplyConflictCode,
    message: String,
}

impl std::fmt::Display for ApplyConflictError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for ApplyConflictError {}

pub fn apply_conflict_error(
    code: ApplyConflictCode,
    message: impl Into<String>,
) -> tokio_rusqlite::Error {
    tokio_rusqlite::Error::Other(Box::new(ApplyConflictError {
        code,
        message: message.into(),
    }))
}

#[doc(hidden)]
pub fn is_terminal_apply_conflict(err: &tokio_rusqlite::Error) -> bool {
    match err {
        tokio_rusqlite::Error::Other(inner) => inner.downcast_ref::<ApplyConflictError>().is_some(),
        _ => false,
    }
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

fn log_slow_log_apply_commit(
    elapsed: std::time::Duration,
    resource_version: i64,
    mutation_count: usize,
    pending_watch_events: usize,
    emit_watch_events: bool,
) {
    if elapsed.as_millis() < 50 {
        return;
    }
    tracing::warn!(
        target: "klights::datastore::slowdown",
        operation = "log_apply_commit",
        elapsed_ms = elapsed.as_millis(),
        resource_version,
        mutation_count,
        pending_watch_events,
        emit_watch_events,
        "slow log_apply commit"
    );
}

pub fn other_error(message: impl Into<String>) -> tokio_rusqlite::Error {
    tokio_rusqlite::Error::Other(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    )))
}
