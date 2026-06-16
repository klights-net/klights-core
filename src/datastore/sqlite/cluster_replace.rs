use super::cluster_state_apply::{ApplyEffects, RaftClusterStateApplier};
use super::{Datastore, queries};
use crate::bootstrap::cluster_meta::{
    KEY_CLUSTER_ID, KEY_LEADER_EPOCH, KEY_RAFT_LEADER_HINT, KEY_RAFT_TERM, KEY_RAFT_VOTERS,
};
use crate::datastore::types::{AppliedOutboxRecord, PendingWatchEvent, ReplicatedSnapshotMetadata};
use crate::log_apply::{ClusterMutation, LogApplyCommit, LogApplyMutation};
#[cfg(test)]
use crate::log_apply::{LogApplyNodeDataplaneRow, LogApplyNodeSubnetRow, LogApplyWatchEventRow};
#[cfg(test)]
use crate::log_apply::{LogApplyResourceKey, LogApplyResourcePatch, LogApplyResourceRow};
use anyhow::{Result, anyhow};
use rusqlite::OptionalExtension;
use std::sync::atomic::{AtomicU64, Ordering};

static RAFT_AUTHORITATIVE_APPLY_CONFLICTS_TOTAL: AtomicU64 = AtomicU64::new(0);

pub(super) fn record_raft_authoritative_apply_conflict() {
    RAFT_AUTHORITATIVE_APPLY_CONFLICTS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

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

        self.publish_watch_events(pending);
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
            apply_commit_in_tx_with_watch_events(&tx, commit, !has_explicit_watch_history, false)?;
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

        self.publish_watch_events(pending);
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

        self.publish_watch_events(outcome.pending);
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
    let reserved_rv = commit.resource_version;
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

    // Authoritative apply: committed raft entries are ground truth.
    // Staleness-related preconditions (precondition_resource_version / precondition_uid) are
    // bypassed so a stale follower converges from the log without relying solely on snapshot
    // install (finding.md H2). Structural conditions (require_absent / require_existing) are
    // still enforced — they represent true API-level invariants (e.g. duplicate-create detection
    // on the leader) and must be propagated back to the proposer as terminal results.
    tx.execute("SAVEPOINT raft_apply_attempt", [])?;
    match apply_commit_in_tx_returning_rv(tx, commit, true) {
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
            rollback_uncommitted_metadata_rv_if_current_tx(tx, reserved_rv)?;
            if let Some(mut row) = outbox_template {
                row.applied_rv = None;
                row.result_proto = crate::datastore::command::encode_response_protobuf(
                    &crate::datastore::command::StorageResponse::Error {
                        message: message.clone(),
                    },
                )
                .unwrap_or_default();
                RaftClusterStateApplier::new(tx)
                    .outbox_mut()
                    .put_applied_outbox(row)?;
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
    let (_applied_rv, pending) = apply_commit_in_tx_returning_rv(tx, commit, false)?;
    Ok(pending)
}

pub(crate) fn apply_commit_in_tx_returning_rv(
    tx: &rusqlite::Transaction<'_>,
    commit: LogApplyCommit,
    raft_authoritative: bool,
) -> tokio_rusqlite::Result<(i64, Vec<PendingWatchEvent>)> {
    let has_explicit_watch_history = commit
        .mutations
        .iter()
        .any(|mutation| matches!(mutation, LogApplyMutation::PutWatchEvent(_)));
    apply_commit_in_tx_with_watch_events(
        tx,
        commit,
        !has_explicit_watch_history,
        raft_authoritative,
    )
}

fn apply_commit_in_tx_with_watch_events(
    tx: &rusqlite::Transaction<'_>,
    commit: LogApplyCommit,
    emit_watch_events: bool,
    raft_authoritative: bool,
) -> tokio_rusqlite::Result<(i64, Vec<PendingWatchEvent>)> {
    if commit.resource_version < 0 {
        return Err(other_error(
            "log_apply commit resourceVersion must be non-negative",
        ));
    }
    let commit = stamp_provisional_resource_version_in_tx(tx, commit)?;
    let applied_rv = commit.resource_version;
    let mut effects = ApplyEffects::new();
    let mut applier = RaftClusterStateApplier::new(tx);
    for mutation in commit.mutations {
        applier.apply_cluster_mutation(
            commit.resource_version,
            ClusterMutation::from(mutation),
            emit_watch_events,
            raft_authoritative,
            &mut effects,
        )?;
    }
    advance_metadata_rv_to_at_least_tx(tx, commit.resource_version)?;
    Ok((applied_rv, effects.into_pending_watch_events()))
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
    row.applied_rv.is_none() && row.result_proto.is_empty()
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ApplyConflictCode {
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

pub(super) fn apply_conflict_error(
    code: ApplyConflictCode,
    message: impl Into<String>,
) -> tokio_rusqlite::Error {
    tokio_rusqlite::Error::Other(Box::new(ApplyConflictError {
        code,
        message: message.into(),
    }))
}

fn is_terminal_apply_conflict(err: &tokio_rusqlite::Error) -> bool {
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

pub(crate) fn rollback_uncommitted_metadata_rv_if_current_tx(
    tx: &rusqlite::Transaction<'_>,
    reserved_rv: i64,
) -> tokio_rusqlite::Result<()> {
    if reserved_rv <= 0 {
        return Ok(());
    }
    let current_rv: i64 = tx.query_row(queries::METADATA_SELECT_RV_INT, [], |row| row.get(0))?;
    if current_rv != reserved_rv {
        return Ok(());
    }
    let committed_rv: i64 = tx.query_row(
        "SELECT COALESCE(MAX(rv), 0) FROM (
            SELECT CAST(resource_version AS INTEGER) AS rv FROM cluster_resources
            UNION ALL SELECT CAST(resource_version AS INTEGER) FROM namespaced_resources
            UNION ALL SELECT CAST(resource_version AS INTEGER) FROM namespaces
            UNION ALL SELECT CAST(resource_version AS INTEGER) FROM watch_events
            UNION ALL SELECT CAST(resource_version AS INTEGER) FROM pod_cleanup_intents
            UNION ALL SELECT CAST(applied_rv AS INTEGER) FROM applied_outbox WHERE applied_rv IS NOT NULL
        )",
        [],
        |row| row.get(0),
    )?;
    if committed_rv < reserved_rv {
        tx.execute(
            queries::METADATA_SET_RV,
            rusqlite::params![committed_rv.to_string()],
        )?;
    }
    Ok(())
}

pub(super) fn other_error(message: impl Into<String>) -> tokio_rusqlite::Error {
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
                node_ip: "192.0.2.1".to_string(),
                mode: "root".to_string(),
                hostport_range: None,
            })],
        )
    }

    #[test]
    fn terminal_apply_conflict_classification_uses_typed_codes_not_message_text() {
        for code in [
            ApplyConflictCode::NotFound,
            ApplyConflictCode::AlreadyExists,
            ApplyConflictCode::UidPrecondition,
            ApplyConflictCode::ResourceVersionPrecondition,
        ] {
            let err = apply_conflict_error(code, "typed conflict without status text");
            assert!(
                is_terminal_apply_conflict(&err),
                "typed conflict {code:?} must classify as terminal"
            );
        }

        let transient = other_error("transient text mentioning 409 Conflict and 404 Not Found");
        assert!(
            !is_terminal_apply_conflict(&transient),
            "untyped internal errors must not classify as terminal by message text"
        );
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

    /// A committed PUT with require_existing=true on a row that is absent on the follower
    /// returns a terminal "404 Not Found" result because require_existing is a structural
    /// API invariant (not a staleness-related precondition) and is preserved in the raft
    /// authoritative apply path. Staleness-related preconditions (precondition_rv /
    /// precondition_uid) are bypassed by the raft path.
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

    // ── Task 1: Committed Raft Apply Authoritative Over Stale Follower State ─────────────────

    /// Committed delete must remove a row even when the follower's local rv differs from the
    /// precondition the leader encoded (the follower missed an intermediate update).
    #[tokio::test]
    async fn committed_delete_applies_even_when_local_row_is_stale() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "stale-cm",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "stale-cm", "namespace": "default", "uid": "cm-stale-del"}
            }),
        )
        .await
        .unwrap();
        // Committed delete carries precondition_rv=999 (the leader's rv at delete time). The
        // follower has the row at rv=1 (it missed an intermediate update). The delete must
        // converge the follower: the row is removed despite the rv mismatch.
        let result = db
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                50,
                vec![LogApplyMutation::DeleteResource(LogApplyResourceKey {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "stale-cm".to_string(),
                    uid: "cm-stale-del".to_string(),
                    precondition_resource_version: Some(999),
                })],
            ))
            .await
            .expect("raft delete must not error on stale rv");
        assert!(
            result.error_message.is_none(),
            "committed delete must succeed without error_message: {result:?}"
        );
        let row = db
            .get_resource("v1", "ConfigMap", Some("default"), "stale-cm")
            .await
            .unwrap();
        assert!(
            row.is_none(),
            "committed delete must remove stale row; row still present: {row:?}"
        );
    }

    #[tokio::test]
    async fn committed_namespace_delete_errors_on_corrupt_stored_json_instead_of_emitting_null_event()
     {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_namespace(
            "corrupt-delete-ns",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": "corrupt-delete-ns",
                    "uid": "corrupt-delete-ns-uid"
                }
            }),
        )
        .await
        .expect("seed Namespace");

        db.db_call("test_corrupt_delete_namespace_data", |conn| {
            conn.execute(
                "UPDATE namespaces SET data = ?1 WHERE name = 'corrupt-delete-ns'",
                [b"{not-json".as_slice()],
            )?;
            Ok(())
        })
        .await
        .expect("corrupt stored JSON");

        let err = db
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                60,
                vec![LogApplyMutation::DeleteNamespace {
                    name: "corrupt-delete-ns".to_string(),
                }],
            ))
            .await
            .expect_err("corrupt stored JSON must fail committed namespace delete apply");
        assert!(
            err.to_string().contains("expected ident")
                || err.to_string().contains("key must be a string"),
            "error must come from JSON decoding, got: {err}"
        );
    }

    /// Committed put must overwrite a stale local row regardless of precondition_resource_version
    /// mismatch (the follower has an older rv than the precondition the leader captured).
    #[tokio::test]
    async fn committed_put_overwrites_stale_local_row() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "put-target",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "put-target", "namespace": "default", "uid": "cm-put-uid"},
                "data": {"k": "old"}
            }),
        )
        .await
        .unwrap();
        // Committed PUT carries precondition_rv=500 (the leader's rv). Follower has rv=1.
        // The PUT must overwrite the local row with the committed value.
        let result = db
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                60,
                vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "put-target".to_string(),
                    uid: "cm-put-uid".to_string(),
                    resource_version: 60,
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "put-target",
                            "namespace": "default",
                            "uid": "cm-put-uid",
                            "resourceVersion": "60"
                        },
                        "data": {"k": "committed"}
                    }),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: Some("cm-put-uid".to_string()),
                    precondition_resource_version: Some(500), // mismatch: follower has rv≠500
                    status_only: false,
                })],
            ))
            .await
            .expect("raft put must not error on stale rv");
        assert!(
            result.error_message.is_none(),
            "committed PUT must succeed: {result:?}"
        );
        let row = db
            .get_resource("v1", "ConfigMap", Some("default"), "put-target")
            .await
            .unwrap()
            .expect("committed PUT must materialise the row");
        assert_eq!(
            row.data.pointer("/data/k").and_then(|v| v.as_str()),
            Some("committed"),
            "row data must reflect the committed value"
        );
        assert_eq!(row.resource_version, 60, "row must carry the committed rv");
    }

    #[tokio::test]
    async fn stale_same_uid_committed_put_does_not_revert_newer_client_owned_state() {
        let db = crate::datastore::test_support::in_memory().await;
        let created = db
            .create_resource(
                "apps/v1",
                "Deployment",
                Some("default"),
                "web",
                serde_json::json!({
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {
                        "name": "web",
                        "namespace": "default",
                        "uid": "deploy-stale-put-uid",
                        "generation": 2
                    },
                    "spec": {"replicas": 10},
                    "status": {"replicas": 13, "availableReplicas": 8}
                }),
            )
            .await
            .unwrap();

        let mut scaled = (*created.data).clone();
        scaled["metadata"]["generation"] = serde_json::json!(3);
        scaled["spec"]["replicas"] = serde_json::json!(30);
        db.update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web",
            scaled,
            created.resource_version,
        )
        .await
        .unwrap();

        let result = db
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                60,
                vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "apps/v1".to_string(),
                    kind: "Deployment".to_string(),
                    namespace: Some("default".to_string()),
                    name: "web".to_string(),
                    uid: "deploy-stale-put-uid".to_string(),
                    resource_version: 60,
                    data: serde_json::json!({
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "metadata": {
                            "name": "web",
                            "namespace": "default",
                            "uid": "deploy-stale-put-uid",
                            "resourceVersion": "60",
                            "generation": 2
                        },
                        "spec": {"replicas": 10},
                        "status": {"replicas": 13, "availableReplicas": 8}
                    }),
                    require_absent: false,
                    require_existing: true,
                    precondition_uid: Some("deploy-stale-put-uid".to_string()),
                    precondition_resource_version: Some(created.resource_version),
                    status_only: false,
                })],
            ))
            .await
            .expect("stale committed PUT should apply without surfacing a state-machine error");
        assert!(
            result.error_message.is_none(),
            "stale committed PUT must not fail the raft apply: {result:?}"
        );

        let row = db
            .get_resource("apps/v1", "Deployment", Some("default"), "web")
            .await
            .unwrap()
            .expect("deployment remains after stale committed put");
        assert_eq!(
            row.data.pointer("/spec/replicas"),
            Some(&serde_json::json!(30)),
            "same-UID stale committed PUT must not roll back a newer Deployment scale update"
        );
        assert_eq!(
            row.data.pointer("/metadata/generation"),
            Some(&serde_json::json!(3)),
            "same-UID stale committed PUT must preserve the newer client-owned generation"
        );
        assert_eq!(
            row.resource_version, 60,
            "stale committed PUT still advances materialized raft resourceVersion"
        );
    }

    #[tokio::test]
    async fn stale_same_uid_generationless_committed_put_preserves_newer_configmap_state() {
        let db = crate::datastore::test_support::in_memory().await;
        db.apply_raft_log_apply_commit(LogApplyCommit::new(
            10,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "generationless-cm".to_string(),
                uid: "generationless-cm-uid".to_string(),
                resource_version: 10,
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "generationless-cm",
                        "namespace": "default",
                        "uid": "generationless-cm-uid",
                        "resourceVersion": "10"
                    },
                    "data": {"winner": "initial"}
                }),
                require_absent: false,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            })],
        ))
        .await
        .expect("seed generation-less ConfigMap from raft");

        db.apply_raft_log_apply_commit(LogApplyCommit::new(
            20,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "generationless-cm".to_string(),
                uid: "generationless-cm-uid".to_string(),
                resource_version: 20,
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "generationless-cm",
                        "namespace": "default",
                        "uid": "generationless-cm-uid",
                        "resourceVersion": "20"
                    },
                    "data": {"winner": "newer"}
                }),
                require_absent: false,
                require_existing: true,
                precondition_uid: Some("generationless-cm-uid".to_string()),
                precondition_resource_version: Some(10),
                status_only: false,
            })],
        ))
        .await
        .expect("newer generation-less ConfigMap update applies");

        let result = db
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                30,
                vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "generationless-cm".to_string(),
                    uid: "generationless-cm-uid".to_string(),
                    resource_version: 30,
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "generationless-cm",
                            "namespace": "default",
                            "uid": "generationless-cm-uid",
                            "resourceVersion": "30"
                        },
                        "data": {"winner": "stale"}
                    }),
                    require_absent: false,
                    require_existing: true,
                    precondition_uid: Some("generationless-cm-uid".to_string()),
                    precondition_resource_version: Some(10),
                    status_only: false,
                })],
            ))
            .await
            .expect("stale generation-less committed PUT should not fail raft apply");
        assert!(
            result.error_message.is_none(),
            "stale generation-less committed PUT must not fail raft apply: {result:?}"
        );

        let row = db
            .get_resource("v1", "ConfigMap", Some("default"), "generationless-cm")
            .await
            .unwrap()
            .expect("ConfigMap remains after stale committed put");
        assert_eq!(
            row.data
                .pointer("/data/winner")
                .and_then(|value| value.as_str()),
            Some("newer"),
            "generation-less stale committed PUT must preserve newer same-UID state"
        );
        assert!(
            row.data.pointer("/metadata/generation").is_none(),
            "test fixture must remain generation-less"
        );
        assert_eq!(
            row.resource_version, 30,
            "stale generation-less committed PUT still advances materialized raft resourceVersion"
        );
    }

    #[tokio::test]
    async fn newer_generation_committed_put_applies_after_status_only_rv_advance() {
        let db = crate::datastore::test_support::in_memory().await;
        let created = db
            .create_resource(
                "apps/v1",
                "Deployment",
                Some("default"),
                "web",
                serde_json::json!({
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {
                        "name": "web",
                        "namespace": "default",
                        "uid": "deploy-newer-generation-put-uid",
                        "generation": 2
                    },
                    "spec": {"replicas": 10},
                    "status": {"replicas": 13, "availableReplicas": 8}
                }),
            )
            .await
            .unwrap();

        db.update_status_only_with_preconditions(
            "apps/v1",
            "Deployment",
            Some("default"),
            "web",
            serde_json::json!({
                "replicas": 13,
                "availableReplicas": 8,
                "observedGeneration": 2
            }),
            crate::datastore::ResourcePreconditions::uid(created.uid.clone()),
        )
        .await
        .expect("status update advances RV before stale-precondition scale PUT apply");

        let result = db
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                60,
                vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "apps/v1".to_string(),
                    kind: "Deployment".to_string(),
                    namespace: Some("default".to_string()),
                    name: "web".to_string(),
                    uid: "deploy-newer-generation-put-uid".to_string(),
                    resource_version: 60,
                    data: serde_json::json!({
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "metadata": {
                            "name": "web",
                            "namespace": "default",
                            "uid": "deploy-newer-generation-put-uid",
                            "resourceVersion": "60",
                            "generation": 3
                        },
                        "spec": {"replicas": 30},
                        "status": {
                            "replicas": 13,
                            "availableReplicas": 8,
                            "observedGeneration": 2
                        }
                    }),
                    require_absent: false,
                    require_existing: true,
                    precondition_uid: Some("deploy-newer-generation-put-uid".to_string()),
                    precondition_resource_version: Some(created.resource_version),
                    status_only: false,
                })],
            ))
            .await
            .expect("newer-generation committed PUT should apply after status RV advance");
        assert!(
            result.error_message.is_none(),
            "newer-generation committed PUT must not fail raft apply: {result:?}"
        );

        let row = db
            .get_resource("apps/v1", "Deployment", Some("default"), "web")
            .await
            .unwrap()
            .expect("deployment remains after newer-generation committed put");
        assert_eq!(
            row.data.pointer("/spec/replicas"),
            Some(&serde_json::json!(30)),
            "newer-generation stale-precondition committed PUT must apply the scale update"
        );
        assert_eq!(
            row.data.pointer("/metadata/generation"),
            Some(&serde_json::json!(3)),
            "newer-generation stale-precondition committed PUT must publish the new generation"
        );
    }

    #[tokio::test]
    async fn committed_pod_put_preserves_existing_deletion_metadata_for_same_uid() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("gc-2688"),
            "simpletest-01798",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "simpletest-01798",
                    "namespace": "gc-2688",
                    "uid": "pod-uid"
                },
                "spec": {
                    "containers": [{"name": "pause", "image": "registry.k8s.io/pause:3.10"}]
                },
                "status": {
                    "phase": "Pending",
                    "conditions": [
                        {"type": "Ready", "status": "False"},
                        {"type": "ContainersReady", "status": "False"}
                    ]
                }
            }),
        )
        .await
        .unwrap();
        let existing = db
            .get_resource("v1", "Pod", Some("gc-2688"), "simpletest-01798")
            .await
            .unwrap()
            .expect("pod exists before delete mark");
        let mut deleting = (*existing.data).clone();
        deleting["metadata"]["deletionTimestamp"] = serde_json::json!("2026-06-25T02:25:51Z");
        deleting["metadata"]["deletionGracePeriodSeconds"] = serde_json::json!(30);
        db.update_resource(
            "v1",
            "Pod",
            Some("gc-2688"),
            "simpletest-01798",
            deleting,
            existing.resource_version,
        )
        .await
        .unwrap();

        let result = db
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                60,
                vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("gc-2688".to_string()),
                    name: "simpletest-01798".to_string(),
                    uid: "pod-uid".to_string(),
                    resource_version: 60,
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": "simpletest-01798",
                            "namespace": "gc-2688",
                            "uid": "pod-uid",
                            "resourceVersion": "60"
                        },
                        "spec": {
                            "nodeName": "mn-worker",
                            "containers": [{"name": "pause", "image": "registry.k8s.io/pause:3.10"}]
                        },
                        "status": {
                            "phase": "Running",
                            "conditions": [
                                {"type": "PodScheduled", "status": "True"},
                                {"type": "Ready", "status": "True"},
                                {"type": "ContainersReady", "status": "True"}
                            ],
                            "containerStatuses": [{"name": "pause", "ready": true}]
                        }
                    }),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: Some("pod-uid".to_string()),
                    precondition_resource_version: Some(existing.resource_version),
                    status_only: false,
                })],
            ))
            .await
            .expect("raft put applies");
        assert!(
            result.error_message.is_none(),
            "committed Pod PUT must succeed: {result:?}"
        );

        let row = db
            .get_resource("v1", "Pod", Some("gc-2688"), "simpletest-01798")
            .await
            .unwrap()
            .expect("pod remains after committed put");
        assert_eq!(
            row.data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str()),
            Some("2026-06-25T02:25:51Z"),
            "same-UID Pod PUT must not erase a live deletionTimestamp"
        );
        assert_eq!(
            row.data
                .pointer("/metadata/deletionGracePeriodSeconds")
                .and_then(|v| v.as_i64()),
            Some(30),
            "same-UID Pod PUT must not erase deletion grace"
        );
        let ready = row
            .data
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conditions| {
                conditions.iter().find(|condition| {
                    condition.pointer("/type").and_then(|v| v.as_str()) == Some("Ready")
                })
            })
            .expect("Ready condition present");
        assert_eq!(
            ready.pointer("/status").and_then(|v| v.as_str()),
            Some("False"),
            "terminating Pod must stay unready after stale committed PUT"
        );
        assert_eq!(
            row.data
                .pointer("/status/containerStatuses/0/ready")
                .and_then(|v| v.as_bool()),
            Some(false),
            "terminating Pod container readiness must stay false"
        );
    }

    #[tokio::test]
    async fn committed_put_preserves_existing_deletion_metadata_for_non_pod_same_uid() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "terminating-deploy",
            serde_json::json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": "terminating-deploy",
                    "namespace": "default",
                    "uid": "deploy-uid",
                    "generation": 1
                },
                "spec": {"replicas": 1},
                "status": {"availableReplicas": 1}
            }),
        )
        .await
        .unwrap();
        let existing = db
            .get_resource(
                "apps/v1",
                "Deployment",
                Some("default"),
                "terminating-deploy",
            )
            .await
            .unwrap()
            .expect("deployment exists before delete mark");
        let mut deleting = (*existing.data).clone();
        deleting["metadata"]["deletionTimestamp"] = serde_json::json!("2026-06-25T02:35:00Z");
        deleting["metadata"]["deletionGracePeriodSeconds"] = serde_json::json!(30);
        deleting["metadata"]["finalizers"] = serde_json::json!(["example.com/protect"]);
        db.update_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "terminating-deploy",
            deleting,
            existing.resource_version,
        )
        .await
        .unwrap();

        let result = db
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                61,
                vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "apps/v1".to_string(),
                    kind: "Deployment".to_string(),
                    namespace: Some("default".to_string()),
                    name: "terminating-deploy".to_string(),
                    uid: "deploy-uid".to_string(),
                    resource_version: 61,
                    data: serde_json::json!({
                        "apiVersion": "apps/v1",
                        "kind": "Deployment",
                        "metadata": {
                            "name": "terminating-deploy",
                            "namespace": "default",
                            "uid": "deploy-uid",
                            "resourceVersion": "61",
                            "generation": 2
                        },
                        "spec": {"replicas": 2},
                        "status": {"availableReplicas": 1}
                    }),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: Some("deploy-uid".to_string()),
                    precondition_resource_version: Some(existing.resource_version),
                    status_only: false,
                })],
            ))
            .await
            .expect("raft put applies");
        assert!(
            result.error_message.is_none(),
            "committed Deployment PUT must succeed: {result:?}"
        );

        let row = db
            .get_resource(
                "apps/v1",
                "Deployment",
                Some("default"),
                "terminating-deploy",
            )
            .await
            .unwrap()
            .expect("deployment remains after committed put");
        assert_eq!(
            row.data
                .pointer("/metadata/deletionTimestamp")
                .and_then(|v| v.as_str()),
            Some("2026-06-25T02:35:00Z"),
            "same-UID non-Pod PUT must not erase a live deletionTimestamp"
        );
        assert_eq!(
            row.data
                .pointer("/metadata/deletionGracePeriodSeconds")
                .and_then(|v| v.as_i64()),
            Some(30),
            "same-UID non-Pod PUT must not erase deletion grace"
        );
        assert_eq!(
            row.data
                .pointer("/metadata/finalizers/0")
                .and_then(|v| v.as_str()),
            Some("example.com/protect"),
            "stale same-UID non-Pod PUT must not erase live finalizers"
        );
        assert_eq!(
            row.data.pointer("/spec/replicas"),
            Some(&serde_json::json!(2))
        );
    }

    /// Committed patch must apply to the current local state regardless of precondition mismatch,
    /// reconciling the follower toward the committed result before last_applied advances.
    #[tokio::test]
    async fn committed_patch_conflict_reconciles_to_committed_value_before_advancing() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "patch-target",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "patch-target", "namespace": "default", "uid": "cm-patch"},
                "data": {"existing": "yes"}
            }),
        )
        .await
        .unwrap();
        // Committed patch has precondition_rv=888 (leader rv). Follower has rv=1. The patch
        // must still be applied to the current local state.
        let result = db
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                70,
                vec![LogApplyMutation::PatchResourceLatest(
                    LogApplyResourcePatch {
                        api_version: "v1".to_string(),
                        kind: "ConfigMap".to_string(),
                        namespace: Some("default".to_string()),
                        name: "patch-target".to_string(),
                        resource_version: 70,
                        patch_kind: crate::datastore::types::PatchKind::Merge,
                        patch: serde_json::json!({"data": {"added": "by-patch"}}),
                        precondition_uid: Some("cm-patch".to_string()),
                        precondition_resource_version: Some(888), // mismatch
                        require_existing: true,
                        terminating_pod_unready_timestamp: None,
                    },
                )],
            ))
            .await
            .expect("raft patch must not error on stale rv");
        assert!(
            result.error_message.is_none(),
            "committed PATCH must succeed: {result:?}"
        );
        let row = db
            .get_resource("v1", "ConfigMap", Some("default"), "patch-target")
            .await
            .unwrap()
            .expect("committed PATCH must preserve the row");
        assert_eq!(
            row.data.pointer("/data/added").and_then(|v| v.as_str()),
            Some("by-patch"),
            "patch field must be present after committed apply"
        );
    }

    /// A committed patch conflict must NOT advance last_applied while leaving the local state
    /// divergent — the follower must reconcile before the index is recorded as applied.
    ///
    /// This test exercises the `apply_commit_in_tx_for_raft` path and verifies that the returned
    /// result does not carry `error_message` (the old "conflict swallowed, last_applied advanced"
    /// signal), which is the prior buggy behavior.
    #[tokio::test]
    async fn committed_patch_conflict_does_not_advance_applied_index_with_divergence() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "diverge-cm",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "diverge-cm", "namespace": "default", "uid": "cm-div"},
                "data": {"state": "stale"}
            }),
        )
        .await
        .unwrap();
        // Committed patch with precondition_rv mismatch. With the fix the apply succeeds
        // (no error_message). Without the fix the conflict is swallowed with error_message set,
        // which is the "advanced index with divergence" bug.
        let result = db
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                75,
                vec![LogApplyMutation::PatchResourceLatest(
                    LogApplyResourcePatch {
                        api_version: "v1".to_string(),
                        kind: "ConfigMap".to_string(),
                        namespace: Some("default".to_string()),
                        name: "diverge-cm".to_string(),
                        resource_version: 75,
                        patch_kind: crate::datastore::types::PatchKind::Merge,
                        patch: serde_json::json!({"data": {"state": "reconciled"}}),
                        precondition_uid: Some("cm-div".to_string()),
                        precondition_resource_version: Some(777),
                        require_existing: true,
                        terminating_pod_unready_timestamp: None,
                    },
                )],
            ))
            .await
            .expect("raft patch must succeed even with stale precondition");
        assert!(
            result.error_message.is_none(),
            "committed patch must reconcile state rather than swallow conflict: got error_message={:?}",
            result.error_message
        );
        assert!(
            result.applied_rv.is_some(),
            "applied_rv must be set after successful reconcile"
        );
    }

    /// Re-applying an already-committed entry (local state already equals the committed state)
    /// must advance last_applied silently without emitting a conflict.
    #[tokio::test]
    async fn idempotent_reapply_of_already_committed_state_advances_silently() {
        let db = crate::datastore::test_support::in_memory().await;
        // Apply a committed PUT once to establish local state.
        let first = db
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                80,
                vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "idempotent-cm".to_string(),
                    uid: "cm-idem".to_string(),
                    resource_version: 80,
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "idempotent-cm",
                            "namespace": "default",
                            "uid": "cm-idem",
                            "resourceVersion": "80"
                        },
                        "data": {"v": "1"}
                    }),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                })],
            ))
            .await
            .unwrap();
        assert!(
            first.error_message.is_none(),
            "first apply must succeed: {first:?}"
        );

        // Re-apply the identical commit (simulating restart or redundant delivery).
        let second = db
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                80,
                vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "idempotent-cm".to_string(),
                    uid: "cm-idem".to_string(),
                    resource_version: 80,
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "idempotent-cm",
                            "namespace": "default",
                            "uid": "cm-idem",
                            "resourceVersion": "80"
                        },
                        "data": {"v": "1"}
                    }),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: None,
                    precondition_resource_version: None,
                    status_only: false,
                })],
            ))
            .await
            .unwrap();
        assert!(
            second.error_message.is_none(),
            "idempotent re-apply must not set error_message: {second:?}"
        );
    }

    /// An apply conflict (local diverges from committed) must be observable: the result must NOT
    /// carry error_message (swallowing the divergence), and the row must be reconciled.
    #[tokio::test]
    async fn apply_conflict_is_observable_not_silently_successful() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            "observable-cm",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "observable-cm",
                    "namespace": "default",
                    "uid": "cm-obs"
                },
                "data": {"state": "stale"}
            }),
        )
        .await
        .unwrap();
        // Committed PUT with precondition_rv=999 (mismatch with local rv=1).
        // Old behavior: swallowed as error_message, stale row unchanged.
        // New behavior: row reconciled to committed value, no error_message.
        let conflicts_before = RAFT_AUTHORITATIVE_APPLY_CONFLICTS_TOTAL.load(Ordering::Relaxed);
        let result = db
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                90,
                vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "observable-cm".to_string(),
                    uid: "cm-obs".to_string(),
                    resource_version: 90,
                    data: serde_json::json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "observable-cm",
                            "namespace": "default",
                            "uid": "cm-obs",
                            "resourceVersion": "90"
                        },
                        "data": {"state": "authoritative"}
                    }),
                    require_absent: false,
                    require_existing: false,
                    precondition_uid: Some("cm-obs".to_string()),
                    precondition_resource_version: Some(999),
                    status_only: false,
                })],
            ))
            .await
            .unwrap();
        // The conflict must NOT be silently swallowed as error_message — the committed value wins.
        assert!(
            result.error_message.is_none(),
            "conflict must be reconciled (not swallowed as error_message): {result:?}"
        );
        assert!(
            RAFT_AUTHORITATIVE_APPLY_CONFLICTS_TOTAL.load(Ordering::Relaxed) > conflicts_before,
            "authoritative apply conflict must increment the observability counter"
        );
        let row = db
            .get_resource("v1", "ConfigMap", Some("default"), "observable-cm")
            .await
            .unwrap()
            .expect("row must exist after authoritative PUT");
        assert_eq!(
            row.data.pointer("/data/state").and_then(|v| v.as_str()),
            Some("authoritative"),
            "row must reflect the committed value after conflict reconciliation"
        );
    }

    /// A follower that holds a stale row (the leader already deleted it via a committed log entry)
    /// must converge to the leader fingerprint (row absent) without requiring a snapshot install.
    #[tokio::test]
    async fn follower_converges_to_leader_fingerprint_without_snapshot_after_stale_delete() {
        // Simulate a follower that holds a row the leader committed as deleted.
        let follower = crate::datastore::test_support::in_memory().await;
        follower
            .create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                "conv-cm",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "conv-cm",
                        "namespace": "default",
                        "uid": "cm-conv"
                    }
                }),
            )
            .await
            .unwrap();

        // Leader backend has the resource deleted (empty). Committed delete arrives via log.
        let result = follower
            .apply_raft_log_apply_commit(LogApplyCommit::new(
                100,
                vec![LogApplyMutation::DeleteResource(LogApplyResourceKey {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    namespace: Some("default".to_string()),
                    name: "conv-cm".to_string(),
                    uid: "cm-conv".to_string(),
                    precondition_resource_version: None, // no precondition — pure committed delete
                })],
            ))
            .await
            .expect("committed log delete must not fail");
        assert!(
            result.error_message.is_none(),
            "no error_message after authoritative delete"
        );

        // Follower now matches leader fingerprint: row absent.
        let row = follower
            .get_resource("v1", "ConfigMap", Some("default"), "conv-cm")
            .await
            .unwrap();
        assert!(
            row.is_none(),
            "follower must converge to leader fingerprint without snapshot; row still present"
        );
    }

    /// Applying the same committed entry encoded as JSON and as protobuf must produce identical
    /// cluster.db rows (api_version, kind, namespace, name, uid, rv, data).
    #[tokio::test]
    async fn committed_apply_json_and_protobuf_paths_produce_identical_rows() {
        let commit = LogApplyCommit::new(
            110,
            vec![LogApplyMutation::PutResource(LogApplyResourceRow {
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "encoding-cm".to_string(),
                uid: "cm-enc".to_string(),
                resource_version: 110,
                data: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": "encoding-cm",
                        "namespace": "default",
                        "uid": "cm-enc",
                        "resourceVersion": "110"
                    },
                    "data": {"encoded": "true"}
                }),
                require_absent: false,
                require_existing: false,
                precondition_uid: None,
                precondition_resource_version: None,
                status_only: false,
            })],
        );

        // JSON path
        let json_bytes = crate::log_apply::encode_commit_json(&commit).unwrap();
        let commit_from_json = crate::log_apply::decode_commit_json(&json_bytes).unwrap();
        let db_json = crate::datastore::test_support::in_memory().await;
        db_json
            .apply_raft_log_apply_commit(commit_from_json)
            .await
            .unwrap();
        let row_json = db_json
            .get_resource("v1", "ConfigMap", Some("default"), "encoding-cm")
            .await
            .unwrap()
            .expect("json path must materialise row");

        // Protobuf path
        let proto_bytes = crate::log_apply::encode_commit_protobuf(&commit).unwrap();
        let commit_from_proto = crate::log_apply::decode_commit_protobuf(&proto_bytes).unwrap();
        let db_proto = crate::datastore::test_support::in_memory().await;
        db_proto
            .apply_raft_log_apply_commit(commit_from_proto)
            .await
            .unwrap();
        let row_proto = db_proto
            .get_resource("v1", "ConfigMap", Some("default"), "encoding-cm")
            .await
            .unwrap()
            .expect("proto path must materialise row");

        assert_eq!(
            row_json.uid, row_proto.uid,
            "JSON and protobuf paths must produce identical uid"
        );
        assert_eq!(
            row_json.resource_version, row_proto.resource_version,
            "JSON and protobuf paths must produce identical rv"
        );
        assert_eq!(
            row_json.data, row_proto.data,
            "JSON and protobuf paths must produce identical data"
        );
    }
}
