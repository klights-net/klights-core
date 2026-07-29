//! Authoritative SQLite restore transaction.

use super::super::{live_apply, mutation_queries as queries, transaction_primitives};
use klights_cluster_core::{ClusterMembership, LogApplyMutation, SnapshotRestoreOperation};
use klights_cluster_store::{
    CLUSTER_ID_META_KEY as KEY_CLUSTER_ID, LEADER_EPOCH_META_KEY as KEY_LEADER_EPOCH,
    RAFT_LEADER_HINT_META_KEY as KEY_RAFT_LEADER_HINT, RAFT_TERM_META_KEY as KEY_RAFT_TERM,
    RAFT_VOTERS_META_KEY as KEY_RAFT_VOTERS, StagedPostCommit,
};

pub struct SnapshotReplayFloor {
    pub api_version: String,
    pub kind: String,
    pub namespace_key: String,
    pub floor_resource_version: i64,
    pub floor_event_id: i64,
    pub position_is_exact: bool,
}

pub enum SnapshotMembership {
    LegacyOmitted,
    AuthoritativeAbsent,
    Present(ClusterMembership),
}

pub struct SnapshotMetadata {
    pub cluster_id: String,
    pub leader_epoch: i64,
    pub membership: SnapshotMembership,
    pub command_codec_activation_version: Option<u32>,
}

pub fn replace_resource_state_in_conn(
    conn: &mut rusqlite::Connection,
    entries: Vec<SnapshotRestoreOperation>,
    current_rv: i64,
    watch_event_high_water: Option<i64>,
    watch_replay_floors: Option<Vec<SnapshotReplayFloor>>,
    metadata: Option<SnapshotMetadata>,
    context: &live_apply::TransactionContext<'_>,
) -> tokio_rusqlite::Result<Vec<StagedPostCommit>> {
    if current_rv < 0 {
        return Err(live_apply::other_error(
            "snapshot current_rv must be non-negative",
        ));
    }

    let tx = conn.transaction()?;
    if let Some(metadata) = metadata.as_ref() {
        match metadata.command_codec_activation_version {
            Some(3) => {
                tx.execute(
                    queries::UPSERT_KLIGHTS_META,
                    rusqlite::params![
                        klights_cluster_store::COMMAND_CODEC_ACTIVATION_VERSION_META_KEY,
                        klights_cluster_store::COMMAND_CODEC_V3_ACTIVATION_VALUE
                    ],
                )?;
            }
            None => {
                tx.execute(
                    "DELETE FROM _klights_meta WHERE key = ?1",
                    [klights_cluster_store::COMMAND_CODEC_ACTIVATION_VERSION_META_KEY],
                )?;
            }
            Some(other) => {
                return Err(live_apply::other_error(format!(
                    "snapshot command codec activation version must be exact v3, got {other}"
                )));
            }
        }
    }
    tx.execute(queries::REPLACE_STATE_DELETE_WATCH_EVENTS, [])?;
    tx.execute("DELETE FROM watch_replay_floors", [])?;
    // Snapshot replacement is authoritative. Reset the local allocator before
    // replay so legacy rows without explicit IDs do not inherit a divergent
    // follower sequence; new snapshots set the exact leader boundary below.
    transaction_primitives::set_watch_event_allocator(&tx, 0)?;
    tx.execute(queries::REPLACE_STATE_DELETE_APPLIED_OUTBOX, [])?;
    tx.execute("DELETE FROM outbox_stream_watermarks", [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_POD_CLEANUP_INTENTS, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_NODE_DATAPLANE, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_NODE_SUBNETS, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_NAMESPACED_RESOURCES, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_CLUSTER_RESOURCES, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_RESOURCE_LABELS, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_RESOURCE_FIELDS, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_RESOURCE_OWNER_REFS, [])?;
    tx.execute(queries::REPLACE_STATE_DELETE_NAMESPACES, [])?;

    let has_explicit_watch_history = entries.iter().any(|operation| {
        operation
            .mutations()
            .iter()
            .any(|mutation| matches!(mutation, LogApplyMutation::PutWatchEvent(_)))
    });
    let emit_synthetic_watch_events =
        watch_event_high_water.is_none() && !has_explicit_watch_history;
    let mut pending = Vec::with_capacity(entries.len());
    for operation in entries {
        if operation.resource_version() <= 0 {
            return Err(live_apply::other_error(format!(
                "snapshot entry has non-positive resourceVersion {}",
                operation.resource_version()
            )));
        }
        if operation.resource_version() > current_rv {
            return Err(live_apply::other_error(format!(
                "snapshot entry resourceVersion {} is ahead of leader current_rv {}",
                operation.resource_version(),
                current_rv
            )));
        }
        let (_applied_rv, commit_pending, _applied_mutation) =
            live_apply::apply_snapshot_restore_operation_in_tx(
                &tx,
                operation,
                emit_synthetic_watch_events,
                context,
            )?;
        pending.extend(commit_pending);
    }
    if has_explicit_watch_history {
        restore_created_rv_from_watch_history(&tx)?;
    }
    if let Some(high_water) = watch_event_high_water {
        if high_water < 0 {
            return Err(live_apply::other_error(
                "snapshot watch_event_high_water must be non-negative",
            ));
        }
        let restored_max =
            tx.query_row("SELECT COALESCE(MAX(id), 0) FROM watch_events", [], |row| {
                row.get::<_, i64>(0)
            })?;
        if high_water < restored_max {
            return Err(live_apply::other_error(format!(
                "snapshot watch_event_high_water {high_water} is below restored event ID {restored_max}"
            )));
        }
        transaction_primitives::set_watch_event_allocator(&tx, high_water)?;
    }
    if let Some(floors) = watch_replay_floors {
        for floor in floors {
            let position_is_exact = floor.position_is_exact;
            if floor.floor_resource_version < 0 || (position_is_exact && floor.floor_event_id < 0) {
                return Err(live_apply::other_error(
                    "snapshot watch replay floor must be non-negative",
                ));
            }
            if position_is_exact
                && watch_event_high_water
                    .is_some_and(|high_water| floor.floor_event_id > high_water)
            {
                return Err(live_apply::other_error(format!(
                    "snapshot replay floor event ID {} exceeds allocator high-water {}",
                    floor.floor_event_id,
                    watch_event_high_water.unwrap_or_default()
                )));
            }
            tx.execute(
                "INSERT INTO watch_replay_floors
                 (api_version, kind, namespace_key, floor_rv, floor_event_id, floor_position_exact)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    floor.api_version,
                    floor.kind,
                    floor.namespace_key,
                    floor.floor_resource_version,
                    floor.floor_event_id,
                    position_is_exact,
                ],
            )?;
        }
    } else {
        let legacy_event_floor = match watch_event_high_water {
            Some(high_water) => high_water,
            None => transaction_primitives::watch_event_allocator_high_water(&tx)?,
        };
        let legacy_floor_is_exact = 1;
        tx.execute(
            "INSERT INTO watch_replay_floors
             (api_version, kind, namespace_key, floor_rv, floor_event_id, floor_position_exact)
             VALUES ('*', '*', '*', ?1, ?2, ?3)",
            rusqlite::params![current_rv, legacy_event_floor, legacy_floor_is_exact],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO watch_replay_floors
             (api_version, kind, namespace_key, floor_rv, floor_event_id, floor_position_exact)
             SELECT api_version, kind, namespace_key, ?1, ?2, ?3 FROM (
                 SELECT api_version, kind, COALESCE(namespace, '#cluster') AS namespace_key
                   FROM watch_events
                 UNION
                 SELECT api_version, kind, namespace FROM namespaced_resources
                 UNION
                 SELECT api_version, kind, '#cluster' FROM cluster_resources
                 UNION
                 SELECT 'v1', 'Namespace', '#cluster' FROM namespaces
             )",
            rusqlite::params![current_rv, legacy_event_floor, legacy_floor_is_exact],
        )?;
    }

    tx.execute(
        queries::METADATA_SET_RV,
        rusqlite::params![current_rv.to_string()],
    )?;
    if let Some(metadata) = metadata {
        if !metadata.cluster_id.is_empty() {
            tx.execute(
                queries::UPSERT_KLIGHTS_META,
                rusqlite::params![KEY_CLUSTER_ID, metadata.cluster_id],
            )?;
            tx.execute(
                queries::UPSERT_KLIGHTS_META,
                rusqlite::params![KEY_LEADER_EPOCH, metadata.leader_epoch.to_string()],
            )?;
        }
        match metadata.membership {
            SnapshotMembership::LegacyOmitted => {}
            SnapshotMembership::AuthoritativeAbsent => {
                tx.execute(
                    "DELETE FROM _klights_meta WHERE key IN (?1, ?2, ?3)",
                    rusqlite::params![KEY_RAFT_VOTERS, KEY_RAFT_TERM, KEY_RAFT_LEADER_HINT],
                )?;
            }
            SnapshotMembership::Present(membership) => {
                let voters = serde_json::to_string(&membership.voters).map_err(|error| {
                    live_apply::other_error(format!("failed to serialize voters: {error}"))
                })?;
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
