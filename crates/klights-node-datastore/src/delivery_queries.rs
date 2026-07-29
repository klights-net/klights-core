use std::sync::LazyLock;

pub(super) const META_GET: &str = "SELECT value FROM _node_meta WHERE key = ?1";
pub(super) const META_SET: &str = "INSERT INTO _node_meta (key, value) VALUES (?1, ?2) \
     ON CONFLICT(key) DO UPDATE SET value = excluded.value";

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
     (client_id, idempotency_key, enqueued_ms, subject_key, subject_api_version, subject_kind, \
      subject_namespace, subject_name, subject_uid, pod_uid, operation, \
      priority_class, supersedable_pod_status, is_terminal_pod_delete, \
      stream_id, stream_seq, payload_proto, next_due_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)";
pub(super) const OUTBOX_ROW_SELECT: &str = "SELECT id, client_id, idempotency_key, enqueued_ms, \
     subject_key, subject_api_version, subject_kind, subject_namespace, subject_name, \
     subject_uid, pod_uid, operation, priority_class, supersedable_pod_status, \
     is_terminal_pod_delete, stream_id, stream_seq, \
     payload_proto, attempt, next_due_ms, leased_until_ms, lease_token, last_error \
     FROM outbox WHERE id = ?1";

const OUTBOX_CLAIM_DUE_SELECT_AND_WHERE: &str = "WITH eligible AS ( \
     SELECT candidate.id, candidate.priority_class, candidate.enqueued_ms, \
            candidate.is_terminal_pod_delete, candidate.supersedable_pod_status \
     FROM outbox candidate \
     WHERE candidate.next_due_ms <= ?1 \
       AND (candidate.leased_until_ms = 0 OR candidate.leased_until_ms <= ?1) \
       AND ( \
           (candidate.stream_id > 0 AND candidate.stream_seq > 0 \
            AND NOT EXISTS ( \
                SELECT 1 FROM outbox older_stream \
                WHERE older_stream.stream_id = candidate.stream_id \
                  AND ( \
                      (older_stream.stream_seq > 0 \
                       AND older_stream.stream_seq < candidate.stream_seq) \
                      OR (older_stream.stream_seq = 0 \
                          AND older_stream.id < candidate.id) \
                  ) \
            ) \
            AND NOT EXISTS ( \
                SELECT 1 FROM outbox_dead_letter dead_head \
                WHERE dead_head.stream_id = candidate.stream_id \
                  AND dead_head.stream_seq > 0 \
                  AND dead_head.stream_seq < candidate.stream_seq \
            )) \
           OR ((candidate.stream_id <= 0 OR candidate.stream_seq <= 0) \
               AND NOT EXISTS ( \
                   SELECT 1 FROM outbox older \
                   WHERE older.subject_key = candidate.subject_key \
                     AND older.id < candidate.id \
               )) \
       ) \
     ) SELECT id FROM eligible candidate ";

static OUTBOX_CLAIM_NEXT_DUE_SQL: LazyLock<String> = LazyLock::new(|| outbox_claim_due_sql("1"));
static OUTBOX_CLAIM_DUE_BATCH_SQL: LazyLock<String> = LazyLock::new(|| outbox_claim_due_sql("?2"));

fn outbox_claim_due_sql(limit: &str) -> String {
    use klights_node_store::{OUTBOX_DIAGNOSTIC_AGING_MS, OutboxPriority};

    format!(
        "{OUTBOX_CLAIM_DUE_SELECT_AND_WHERE}ORDER BY CASE \
         WHEN candidate.priority_class = {diagnostic} THEN CASE \
           WHEN candidate.enqueued_ms <= (?1 - {aging_ms}) THEN {workload} \
           WHEN EXISTS (SELECT 1 FROM eligible status_candidate \
                        WHERE status_candidate.is_terminal_pod_delete = 0 \
                          AND status_candidate.supersedable_pod_status = 1) \
           THEN {diagnostic} ELSE {workload} END \
         ELSE candidate.priority_class END, \
         candidate.enqueued_ms ASC, candidate.id ASC LIMIT {limit}",
        diagnostic = OutboxPriority::Diagnostic.persisted_value(),
        workload = OutboxPriority::Workload.persisted_value(),
        aging_ms = OUTBOX_DIAGNOSTIC_AGING_MS,
    )
}

pub(super) fn outbox_claim_next_due() -> &'static str {
    OUTBOX_CLAIM_NEXT_DUE_SQL.as_str()
}

pub(super) fn outbox_claim_due_batch() -> &'static str {
    OUTBOX_CLAIM_DUE_BATCH_SQL.as_str()
}

pub(super) const OUTBOX_SET_LEASE: &str = "UPDATE outbox SET leased_until_ms = ?2, lease_token = ?3 \
     WHERE id = ?1 AND next_due_ms <= ?4 AND leased_until_ms <= ?4";
pub(super) const OUTBOX_RENEW_LEASE: &str = "UPDATE outbox \
     SET leased_until_ms = ?3 WHERE id = ?1 AND lease_token = ?2";
pub(super) const OUTBOX_MARK_FAILED: &str = "UPDATE outbox \
     SET attempt = attempt + 1, next_due_ms = ?3, leased_until_ms = 0, lease_token = NULL, last_error = ?4 \
     WHERE id = ?1 AND lease_token = ?2";
pub(super) const OUTBOX_COMPLETE: &str = "DELETE FROM outbox WHERE id = ?1 AND lease_token = ?2";
pub(super) const OUTBOX_COMPLETE_SUPERSEDED_TERMINAL_POD_DELETE_STATUS: &str = "DELETE FROM outbox WHERE subject_key = ?1 AND id < ?2 \
     AND is_terminal_pod_delete = 0 \
     AND supersedable_pod_status = 1";
pub(super) const OUTBOX_REQUEUE_EXPIRED: &str = "UPDATE outbox SET leased_until_ms = 0, lease_token = NULL WHERE leased_until_ms > 0 AND leased_until_ms <= ?1";
pub(super) const OUTBOX_NEXT_WAKE: &str = "WITH wake_candidates AS ( \
     SELECT candidate.*, \
            MAX(candidate.next_due_ms, CASE WHEN candidate.leased_until_ms > ?1 \
                THEN candidate.leased_until_ms ELSE candidate.next_due_ms END) AS wake_ms \
     FROM outbox candidate \
     ) \
     SELECT MIN(candidate.wake_ms) FROM wake_candidates candidate \
     WHERE ( \
         (candidate.stream_id > 0 AND candidate.stream_seq > 0 \
          AND NOT EXISTS ( \
              SELECT 1 FROM outbox older_stream \
              WHERE older_stream.stream_id = candidate.stream_id \
                AND ( \
                    (older_stream.stream_seq > 0 \
                     AND older_stream.stream_seq < candidate.stream_seq) \
                    OR (older_stream.stream_seq = 0 \
                        AND older_stream.id < candidate.id) \
                ) \
          ) \
          AND NOT EXISTS ( \
              SELECT 1 FROM outbox_dead_letter dead_head \
              WHERE dead_head.stream_id = candidate.stream_id \
                AND dead_head.stream_seq > 0 \
                AND dead_head.stream_seq < candidate.stream_seq \
          )) \
         OR ((candidate.stream_id <= 0 OR candidate.stream_seq <= 0) \
             AND NOT EXISTS ( \
                 SELECT 1 FROM outbox older \
                 WHERE older.subject_key = candidate.subject_key \
                   AND older.id < candidate.id \
             )) \
     )";

pub(super) const REPLICATION_CHECKPOINT_GET: &str = "SELECT last_applied_rv, leader_epoch, cluster_id \
     FROM replication_checkpoint WHERE singleton_key = 0";
pub(super) const REPLICATION_CHECKPOINT_SET: &str = "INSERT INTO replication_checkpoint \
     (singleton_key, last_applied_rv, leader_epoch, cluster_id) VALUES (0, ?1, ?2, ?3) \
     ON CONFLICT(singleton_key) DO UPDATE SET \
       last_applied_rv = excluded.last_applied_rv, \
       leader_epoch = excluded.leader_epoch, \
       cluster_id = excluded.cluster_id";

pub(super) const DEAD_LETTER_INSERT: &str = "INSERT INTO outbox_dead_letter \
     (original_id, client_id, idempotency_key, enqueued_ms, subject_key, subject_api_version, \
      subject_kind, subject_namespace, subject_name, subject_uid, pod_uid, \
      operation, stream_id, stream_seq, payload_proto, attempts, last_error, moved_at_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)";
pub(super) const DEAD_LETTER_LIST: &str = "SELECT id, original_id, client_id, idempotency_key, enqueued_ms, \
     subject_key, subject_api_version, subject_kind, subject_namespace, subject_name, \
     subject_uid, pod_uid, operation, stream_id, stream_seq, payload_proto, attempts, last_error, moved_at_ms \
     FROM outbox_dead_letter ORDER BY id";
pub(super) const DEAD_LETTER_GET: &str = "SELECT id, original_id, client_id, idempotency_key, enqueued_ms, \
     subject_key, subject_api_version, subject_kind, subject_namespace, subject_name, \
     subject_uid, pod_uid, operation, stream_id, stream_seq, payload_proto, attempts, last_error, moved_at_ms \
     FROM outbox_dead_letter WHERE id = ?1";
pub(super) const DEAD_LETTER_DELETE: &str = "DELETE FROM outbox_dead_letter \
     WHERE id = ?1 AND (stream_id = 0 OR stream_seq = 0)";
pub(super) const DEAD_LETTER_DELETE_AFTER_REPLAY: &str =
    "DELETE FROM outbox_dead_letter WHERE id = ?1";
pub(super) const DEAD_LETTER_COUNT: &str = "SELECT COUNT(*) FROM outbox_dead_letter";
pub(super) const OUTBOX_COUNT: &str = "SELECT COUNT(*) FROM outbox";
pub(super) const OUTBOX_OLDEST_ENQUEUED: &str = "SELECT MIN(enqueued_ms) FROM outbox";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbox_single_and_batch_claim_queries_share_one_policy() {
        let single = outbox_claim_next_due();
        let batch = outbox_claim_due_batch();
        assert_eq!(single.strip_suffix("1"), batch.strip_suffix("?2"));
        for operation in [
            "PodStatus",
            "RuntimeReconcile",
            "ProbeReadiness",
            "DeadlineExceeded",
            "ContainerStatusSnapshot",
            "EphemeralContainerStatuses",
            "LeaseRenew",
            "NodeStatus",
            "EventCreate",
        ] {
            assert!(!single.contains(operation));
        }
    }
}
