pub(super) const POD_RUNTIME_ADMIT: &str = "INSERT INTO pod_runtime \
     (pod_uid, namespace, pod_name, node_name, created_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5) \
     ON CONFLICT(pod_uid) DO UPDATE SET \
       namespace = excluded.namespace, \
       pod_name = excluded.pod_name, \
       node_name = excluded.node_name \
     WHERE pod_runtime.namespace = excluded.namespace \
       AND pod_runtime.pod_name = excluded.pod_name \
       AND pod_runtime.node_name = excluded.node_name";
pub(super) const POD_RUNTIME_OWNERSHIP_GET_UID: &str = "SELECT namespace, pod_name, node_name, \
     sandbox_id FROM pod_runtime WHERE pod_uid = ?1";

pub(super) const POD_RUNTIME_RECORD_OWNED_SANDBOX: &str = "INSERT INTO pod_runtime \
     (pod_uid, namespace, pod_name, node_name, sandbox_id, created_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
     ON CONFLICT(pod_uid) DO UPDATE SET sandbox_id = excluded.sandbox_id \
     WHERE pod_runtime.namespace = excluded.namespace \
       AND pod_runtime.pod_name = excluded.pod_name \
       AND pod_runtime.node_name = excluded.node_name \
       AND (pod_runtime.sandbox_id IS NULL \
            OR pod_runtime.sandbox_id = excluded.sandbox_id)";
pub(super) const POD_RUNTIME_RECORD_CGROUP: &str = "UPDATE pod_runtime \
     SET cgroup_path = ?2 WHERE pod_uid = ?1";
pub(super) const POD_RUNTIME_DELETE_UID: &str = "DELETE FROM pod_runtime WHERE pod_uid = ?1";
pub(super) const POD_RUNTIME_GET_UID: &str = "SELECT pod_uid, namespace, pod_name, node_name, \
     sandbox_id, cgroup_path, created_ms, started_ms FROM pod_runtime WHERE pod_uid = ?1";
pub(super) const POD_RUNTIME_LIST: &str = "SELECT pod_uid, namespace, pod_name, node_name, \
     sandbox_id, cgroup_path, created_ms, started_ms FROM pod_runtime ORDER BY pod_uid";
pub(super) const POD_RUNTIME_LIST_NS: &str = "SELECT pod_uid, namespace, pod_name, node_name, \
     sandbox_id, cgroup_path, created_ms, started_ms FROM pod_runtime WHERE namespace = ?1 ORDER BY pod_uid";

pub(super) const POD_SLOT_ADMISSION_SELECT: &str = "SELECT pod_uid, node_name, state, updated_rv \
     FROM pod_slot_admissions WHERE namespace = ?1 AND pod_name = ?2";
pub(super) const POD_SLOT_ADMISSION_INSERT: &str = "INSERT INTO pod_slot_admissions \
     (namespace, pod_name, pod_uid, node_name, state, updated_rv, updated_at_ms) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";
pub(super) const POD_SLOT_ADMISSION_UPDATE: &str = "UPDATE pod_slot_admissions \
     SET pod_uid = ?3, node_name = ?4, state = ?5, updated_rv = ?6, updated_at_ms = ?7 \
     WHERE namespace = ?1 AND pod_name = ?2";
pub(super) const POD_SLOT_ADMISSION_DELETE_IF_UID: &str = "DELETE FROM pod_slot_admissions \
     WHERE namespace = ?1 AND pod_name = ?2 AND pod_uid = ?3";
pub(super) const POD_SLOT_RV_SELECT: &str =
    "SELECT value FROM _node_meta WHERE key = 'pod_slot_resource_version'";
pub(super) const POD_SLOT_RV_UPSERT: &str = "INSERT INTO _node_meta (key, value) \
     VALUES ('pod_slot_resource_version', ?1) \
     ON CONFLICT(key) DO UPDATE SET value = excluded.value";

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
