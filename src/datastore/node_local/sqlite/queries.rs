pub(super) const META_GET: &str = "SELECT value FROM _node_meta WHERE key = ?1";
pub(super) const META_SET: &str = "INSERT INTO _node_meta (key, value) VALUES (?1, ?2) \
     ON CONFLICT(key) DO UPDATE SET value = excluded.value";

pub(super) const POD_RUNTIME_ADMIT: &str = "INSERT INTO pod_runtime \
     (pod_uid, namespace, pod_name, node_name, created_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5) \
     ON CONFLICT(pod_uid) DO UPDATE SET \
       namespace = excluded.namespace, \
       pod_name = excluded.pod_name, \
       node_name = excluded.node_name";

pub(super) const POD_RUNTIME_RECORD_SANDBOX: &str = "UPDATE pod_runtime \
     SET sandbox_id = ?2 WHERE pod_uid = ?1";
pub(super) const POD_RUNTIME_RECORD_CGROUP: &str = "UPDATE pod_runtime \
     SET cgroup_path = ?2 WHERE pod_uid = ?1";
pub(super) const POD_RUNTIME_DELETE_UID: &str = "DELETE FROM pod_runtime WHERE pod_uid = ?1";
pub(super) const POD_RUNTIME_GET_UID: &str = "SELECT pod_uid, namespace, pod_name, node_name, \
     sandbox_id, cgroup_path, created_ms, started_ms FROM pod_runtime WHERE pod_uid = ?1";
pub(super) const POD_RUNTIME_LIST: &str = "SELECT pod_uid, namespace, pod_name, node_name, \
     sandbox_id, cgroup_path, created_ms, started_ms FROM pod_runtime ORDER BY pod_uid";
pub(super) const POD_RUNTIME_LIST_NS: &str = "SELECT pod_uid, namespace, pod_name, node_name, \
     sandbox_id, cgroup_path, created_ms, started_ms FROM pod_runtime WHERE namespace = ?1 ORDER BY pod_uid";

pub(super) const POD_STATUS_CHECKPOINT_UPSERT: &str = "INSERT INTO pod_status_checkpoints \
     (pod_uid, namespace, pod_name, base_rv, applied_rv, status_json, updated_ms) \
     VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6) \
     ON CONFLICT(pod_uid) DO UPDATE SET \
       namespace = excluded.namespace, \
       pod_name = excluded.pod_name, \
       base_rv = excluded.base_rv, \
       applied_rv = NULL, \
       status_json = excluded.status_json, \
       updated_ms = excluded.updated_ms";
pub(super) const POD_STATUS_CHECKPOINT_GET_UID: &str = "SELECT pod_uid, namespace, pod_name, \
     base_rv, applied_rv, status_json, updated_ms FROM pod_status_checkpoints WHERE pod_uid = ?1";
pub(super) const POD_STATUS_CHECKPOINT_MARK_APPLIED: &str = "UPDATE pod_status_checkpoints \
     SET applied_rv = ?2, updated_ms = ?3 WHERE pod_uid = ?1";
pub(super) const POD_STATUS_CHECKPOINT_DELETE_UID: &str =
    "DELETE FROM pod_status_checkpoints WHERE pod_uid = ?1";

pub(super) const RUNTIME_OBSERVATION_CHECKPOINT_UPSERT: &str = "INSERT INTO pod_runtime_observation_checkpoints \
     (pod_uid, container_ids, generation, updated_ms) \
     VALUES (?1, ?2, ?3, ?4) \
     ON CONFLICT(pod_uid) DO UPDATE SET \
       container_ids = excluded.container_ids, \
       generation = excluded.generation, \
       updated_ms = excluded.updated_ms";
pub(super) const RUNTIME_OBSERVATION_CHECKPOINT_GET_UID: &str = "SELECT pod_uid, container_ids, generation, updated_ms \
     FROM pod_runtime_observation_checkpoints WHERE pod_uid = ?1";
pub(super) const RUNTIME_OBSERVATION_CHECKPOINT_DELETE_UID: &str =
    "DELETE FROM pod_runtime_observation_checkpoints WHERE pod_uid = ?1";

pub(super) const OUTBOX_INSERT: &str = "INSERT INTO outbox \
     (idempotency_key, enqueued_ms, subject_key, subject_api_version, subject_kind, \
      subject_namespace, subject_name, subject_uid, pod_uid, operation, payload_proto, next_due_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)";
pub(super) const OUTBOX_ROW_SELECT: &str = "SELECT id, idempotency_key, enqueued_ms, \
     subject_key, subject_api_version, subject_kind, subject_namespace, subject_name, \
     subject_uid, pod_uid, operation, payload_proto, attempt, next_due_ms, \
     leased_until_ms, lease_token, last_error FROM outbox WHERE id = ?1";
pub(super) const OUTBOX_CLAIM_NEXT_DUE: &str = "SELECT id FROM outbox candidate \
     WHERE candidate.next_due_ms <= ?1 \
       AND (candidate.leased_until_ms = 0 OR candidate.leased_until_ms <= ?1) \
       AND NOT EXISTS ( \
           SELECT 1 FROM outbox older \
           WHERE older.subject_key = candidate.subject_key \
             AND older.id < candidate.id \
       ) \
     ORDER BY CASE candidate.operation \
           WHEN 'LeaseRenew' THEN 0 \
           WHEN 'NodeStatus' THEN 1 \
           ELSE 2 \
       END, candidate.next_due_ms ASC, candidate.id ASC LIMIT 1";
pub(super) const OUTBOX_SET_LEASE: &str =
    "UPDATE outbox SET leased_until_ms = ?2, lease_token = ?3 WHERE id = ?1";
pub(super) const OUTBOX_RENEW_LEASE: &str = "UPDATE outbox \
     SET leased_until_ms = ?3 WHERE id = ?1 AND lease_token = ?2";
pub(super) const OUTBOX_MARK_FAILED: &str = "UPDATE outbox \
     SET attempt = attempt + 1, next_due_ms = ?3, leased_until_ms = 0, lease_token = NULL, last_error = ?4 \
     WHERE id = ?1 AND lease_token = ?2";
pub(super) const OUTBOX_COMPLETE: &str = "DELETE FROM outbox WHERE id = ?1 AND lease_token = ?2";
pub(super) const OUTBOX_COMPLETE_BY_ID: &str = "DELETE FROM outbox WHERE id = ?1";
// Strict per-subject single-in-flight: a candidate is excluded if ANY older
// same-subject row exists, regardless of whether that older row is currently
// due or leased. This matches OUTBOX_CLAIM_NEXT_DUE exactly so the batch and
// single-row claim share one per-subject FIFO invariant. Without it, a younger
// same-subject row would be claimable while an older retry is leased/in-flight,
// putting two snapshots of one Pod in flight concurrently — they can then apply
// in raft order != stamp order and clobber the newer status (lost update under
// WAN latency / packet loss). Cross-subject pipelining is preserved because the
// exclusion only ever fires for the SAME subject_key.
pub(super) const OUTBOX_CLAIM_DUE_BATCH: &str = "SELECT id FROM outbox candidate \
     WHERE candidate.next_due_ms <= ?1 \
       AND (candidate.leased_until_ms = 0 OR candidate.leased_until_ms <= ?1) \
       AND NOT EXISTS ( \
           SELECT 1 FROM outbox older \
           WHERE older.subject_key = candidate.subject_key \
             AND older.id < candidate.id \
       ) \
     ORDER BY CASE candidate.operation \
           WHEN 'LeaseRenew' THEN 0 \
           WHEN 'NodeStatus' THEN 1 \
           ELSE 2 \
       END, candidate.next_due_ms ASC, candidate.id ASC LIMIT ?2";
pub(super) const OUTBOX_REQUEUE_EXPIRED: &str = "UPDATE outbox SET leased_until_ms = 0, lease_token = NULL WHERE leased_until_ms > 0 AND leased_until_ms <= ?1";
pub(super) const OUTBOX_NEXT_WAKE: &str = "SELECT MIN(CASE WHEN leased_until_ms > ?1 THEN leased_until_ms ELSE next_due_ms END) FROM outbox";

pub(super) const PROBE_STATE_UPSERT: &str = "INSERT INTO probe_state \
     (pod_uid, container_name, probe_kind, last_result_ms, last_success, consecutive_fail, next_eligible_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, CASE WHEN ?5 = 1 THEN 0 ELSE 1 END, ?4) \
     ON CONFLICT(pod_uid, container_name, probe_kind) DO UPDATE SET \
       last_result_ms = excluded.last_result_ms, \
       last_success = excluded.last_success, \
       consecutive_fail = CASE WHEN excluded.last_success = 1 THEN 0 ELSE probe_state.consecutive_fail + 1 END, \
       next_eligible_ms = excluded.next_eligible_ms";
pub(super) const PROBE_STATE_GET: &str = "SELECT pod_uid, container_name, probe_kind, \
     last_result_ms, last_success, consecutive_fail, next_eligible_ms \
     FROM probe_state WHERE pod_uid = ?1 AND container_name = ?2 AND probe_kind = ?3";

pub(super) const REPLICATION_CHECKPOINT_GET: &str = "SELECT last_applied_rv, leader_epoch, cluster_id \
     FROM replication_checkpoint WHERE singleton_key = 0";
pub(super) const REPLICATION_CHECKPOINT_SET: &str = "INSERT INTO replication_checkpoint \
     (singleton_key, last_applied_rv, leader_epoch, cluster_id) VALUES (0, ?1, ?2, ?3) \
     ON CONFLICT(singleton_key) DO UPDATE SET \
       last_applied_rv = excluded.last_applied_rv, \
       leader_epoch = excluded.leader_epoch, \
       cluster_id = excluded.cluster_id";

// T3: LOG_APPLY_* query constants removed — the `log_apply_entries`
// table is gone. Raft `raft_log_entries` is the sole durable log.

// --- Phase 3 Raft (openraft storage-v2) ------------------------------------
// raft_log_entries: serialized openraft::Entry<TypeConfig> blob, keyed by
// log_index, with term and leader_node_id duplicated for index/log-state
// queries without deserializing every blob.
pub(super) const RAFT_LOG_INSERT: &str = "INSERT INTO raft_log_entries \
     (log_index, term, leader_node_id, entry_blob) VALUES (?1, ?2, ?3, ?4) \
     ON CONFLICT(log_index) DO UPDATE SET \
       term = excluded.term, \
       leader_node_id = excluded.leader_node_id, \
       entry_blob = excluded.entry_blob";
pub(super) const RAFT_LOG_GET_RANGE: &str = "SELECT entry_blob FROM raft_log_entries \
     WHERE log_index >= ?1 AND log_index < ?2 ORDER BY log_index ASC";
pub(super) const RAFT_LOG_LAST: &str = "SELECT log_index, term, leader_node_id \
     FROM raft_log_entries ORDER BY log_index DESC LIMIT 1";
// Truncate logs from log_index (inclusive) onward — used when a follower
// receives an AppendEntries that conflicts with its local tail. This is
// the divergence-recovery primitive missing from the Phase 2 path.
pub(super) const RAFT_LOG_TRUNCATE_FROM: &str =
    "DELETE FROM raft_log_entries WHERE log_index >= ?1";
// Purge logs up to log_index inclusive — used after snapshot install.
pub(super) const RAFT_LOG_PURGE_UPTO: &str = "DELETE FROM raft_log_entries WHERE log_index <= ?1";

// raft_meta: singleton key/value rows for vote, last-committed, last-purged.
pub(super) const RAFT_META_GET: &str = "SELECT value FROM raft_meta WHERE key = ?1";
pub(super) const RAFT_META_SET: &str = "INSERT INTO raft_meta (key, value) VALUES (?1, ?2) \
     ON CONFLICT(key) DO UPDATE SET value = excluded.value";

pub(super) const DEAD_LETTER_INSERT: &str = "INSERT INTO outbox_dead_letter \
     (original_id, idempotency_key, enqueued_ms, subject_key, subject_api_version, \
      subject_kind, subject_namespace, subject_name, subject_uid, pod_uid, \
      operation, payload_proto, attempts, last_error, moved_at_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)";
pub(super) const DEAD_LETTER_LIST: &str = "SELECT id, original_id, idempotency_key, enqueued_ms, \
     subject_key, subject_api_version, subject_kind, subject_namespace, subject_name, \
     subject_uid, pod_uid, operation, payload_proto, attempts, last_error, moved_at_ms \
     FROM outbox_dead_letter ORDER BY id";
pub(super) const DEAD_LETTER_GET: &str = "SELECT id, original_id, idempotency_key, enqueued_ms, \
     subject_key, subject_api_version, subject_kind, subject_namespace, subject_name, \
     subject_uid, pod_uid, operation, payload_proto, attempts, last_error, moved_at_ms \
     FROM outbox_dead_letter WHERE id = ?1";
pub(super) const DEAD_LETTER_DELETE: &str = "DELETE FROM outbox_dead_letter WHERE id = ?1";
pub(super) const DEAD_LETTER_COUNT: &str = "SELECT COUNT(*) FROM outbox_dead_letter";
pub(super) const OUTBOX_COUNT: &str = "SELECT COUNT(*) FROM outbox";
pub(super) const OUTBOX_OLDEST_ENQUEUED: &str = "SELECT MIN(enqueued_ms) FROM outbox";
