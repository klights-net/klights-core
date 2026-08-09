use std::sync::Arc;

use klights_node_datastore::{
    SqliteNodeIdentity, SqliteNodeNetworkStateStore, SqliteRuntimeWorkStore,
    delivery::SqliteDeliveryStore, open,
};
use klights_node_store::{
    NodeIdentity, PodCheckpointKey, PodEndpointStore, PodRuntimeAdmission, PodRuntimeStore,
    PodSlotAdmissionEvent, PodSlotAdmissionEventSource, PodSlotAdmissionRequest,
    PodSlotAdmissionResult, PodSlotAdmissionState, PodSlotAdmissionStore, PodSlotClearResult,
    PodSlotMutationResult, PodStatusCheckpointApplied, PodStatusCheckpointStore,
    PodStatusCheckpointUpsert, RuntimeObservationCheckpoint, RuntimeObservationCheckpointStore,
    RuntimeObservationGeneration,
};
use klights_supervisor::{DbExecutor, SystemWallClock, TaskCategoryConfig, TaskSupervisor};
use klights_types::PodIdentity;
use sha2::Digest;

struct NodePersistence {
    executor: DbExecutor,
    identity: SqliteNodeIdentity,
    delivery: SqliteDeliveryStore,
    network: SqliteNodeNetworkStateStore,
    runtime: SqliteRuntimeWorkStore,
}

async fn fresh() -> NodePersistence {
    let executor = open::open_with_opts(
        open::in_memory_opts(),
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
        "sqlite:phase11-relocated-persistence-test",
    )
    .await
    .unwrap();
    let clock = Arc::new(SystemWallClock);
    NodePersistence {
        identity: SqliteNodeIdentity::new(executor.clone()),
        delivery: SqliteDeliveryStore::new(executor.clone(), clock.clone()),
        network: SqliteNodeNetworkStateStore::new(executor.clone(), clock.clone()),
        runtime: SqliteRuntimeWorkStore::new(executor.clone(), clock),
        executor,
    }
}

fn checkpoint(
    pod_uid: &str,
    container_ids: &[&str],
    generation: u32,
    updated_ms: i64,
) -> RuntimeObservationCheckpoint {
    RuntimeObservationCheckpoint::try_new(
        pod_uid,
        container_ids
            .iter()
            .map(|container_id| (*container_id).to_string())
            .collect(),
        RuntimeObservationGeneration::try_from(u64::from(generation)).unwrap(),
        updated_ms,
    )
    .unwrap()
}

#[tokio::test]
async fn node_local_schema_has_only_slim_uid_bound_tables() {
    let db = fresh().await;
    let tables = db
        .executor
        .call_raw("test:phase11_table_inventory", |conn| {
            conn.prepare(
                "SELECT name FROM sqlite_master \
                 WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
            )?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .unwrap();

    for required in [
        "outbox",
        "outbox_dead_letter",
        "pod_runtime",
        "pod_status_checkpoints",
        "pod_networks",
        "pod_endpoints",
        "pod_workqueue",
        "probe_state",
        "replication_checkpoint",
        "_node_meta",
    ] {
        assert!(
            tables.iter().any(|table| table == required),
            "missing {required}"
        );
    }
    for forbidden in [
        "namespaced_resources",
        "cluster_resources",
        "namespaces",
        "watch_events",
        "pod_sandboxes",
    ] {
        assert!(!tables.iter().any(|table| table == forbidden));
    }

    db.executor
        .call_raw("test:phase11_uid_bound_columns", |conn| {
            for table in [
                "outbox",
                "pod_runtime",
                "pod_status_checkpoints",
                "pod_networks",
                "pod_endpoints",
                "pod_workqueue",
                "probe_state",
            ] {
                let mut statement = conn.prepare(&format!("PRAGMA table_info(\"{table}\")"))?;
                let columns = statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                assert!(columns.iter().any(|(name, ty, not_null)| {
                    name == "pod_uid" && ty.eq_ignore_ascii_case("TEXT") && *not_null == 1
                }));
            }
            let table_names = conn
                .prepare(
                    "SELECT name FROM sqlite_master \
                     WHERE type='table' AND name NOT LIKE 'sqlite_%'",
                )?
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for table in table_names {
                let mut statement = conn.prepare(&format!("PRAGMA table_info(\"{table}\")"))?;
                let columns = statement
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                assert!(
                    !columns
                        .iter()
                        .any(|(name, ty)| { name == "data" && ty.eq_ignore_ascii_case("BLOB") })
                );
            }
            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn node_local_schema_and_index_digest_is_stable() {
    let db = fresh().await;
    let digest: String = db
        .executor
        .call_raw("test:node_schema_index_digest", |conn| {
            let mut statement = conn.prepare(
                "SELECT type, name, tbl_name, COALESCE(sql, '') \
                 FROM sqlite_master \
                 WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%' \
                 ORDER BY type, name",
            )?;
            let mut rows = statement.query([])?;
            let mut hasher = sha2::Sha256::new();
            while let Some(row) = rows.next()? {
                for value in [
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ] {
                    hasher.update(value.as_bytes());
                    hasher.update([0]);
                }
                hasher.update([b'\n']);
            }
            let digest = hasher.finalize();
            Ok::<_, tokio_rusqlite::Error>(
                digest
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>(),
            )
        })
        .await
        .unwrap();
    assert_eq!(
        digest,
        "52f00f8f32c9d27367b329af8269638d2d2a01043c79747d1fb93bd097e3f087"
    );
}

#[tokio::test]
async fn pod_status_checkpoint_is_uid_bound_and_status_only() {
    let db = fresh().await;
    let status = serde_json::json!({"phase": "Running", "podIP": "10.42.0.9"});
    db.delivery
        .upsert_pod_status_checkpoint(
            PodStatusCheckpointUpsert::try_new(
                PodIdentity::new("default", "web", "uid-1"),
                7,
                serde_json::to_vec(&status).unwrap(),
                100,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let key = PodCheckpointKey::try_new("uid-1").unwrap();
    let stored = db
        .delivery
        .get_pod_status_checkpoint(key.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.pod(), &PodIdentity::new("default", "web", "uid-1"));
    assert_eq!(stored.base_position(), 7);
    assert_eq!(stored.applied_position(), None);
    let stored_status: serde_json::Value = serde_json::from_slice(stored.status_payload()).unwrap();
    assert_eq!(stored_status["podIP"], "10.42.0.9");
    assert!(stored_status.get("metadata").is_none());

    db.delivery
        .mark_pod_status_checkpoint_applied(
            PodStatusCheckpointApplied::try_new("uid-1", 12, 200).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        db.delivery
            .get_pod_status_checkpoint(key.clone())
            .await
            .unwrap()
            .unwrap()
            .applied_position(),
        Some(12)
    );
    db.delivery
        .delete_pod_status_checkpoint(key.clone())
        .await
        .unwrap();
    assert!(
        db.delivery
            .get_pod_status_checkpoint(key)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn node_meta_mismatch_refuses_boot() {
    let db = fresh().await;
    db.identity
        .ensure_node_identity("cluster-a", "node-a")
        .await
        .unwrap();
    let error = db
        .identity
        .ensure_node_identity("cluster-b", "node-a")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("node.db identity mismatch"));
}

#[tokio::test]
async fn pod_runtime_is_uid_keyed_and_same_name_replacements_are_distinct() {
    let db = fresh().await;
    for uid in ["uid-old", "uid-new"] {
        db.runtime
            .admit_pod_runtime(
                PodRuntimeAdmission::try_new(PodIdentity::new("default", "web", uid), "worker-a")
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        db.runtime
            .list_pod_runtime()
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.pod().uid.clone())
            .collect::<Vec<_>>(),
        ["uid-new", "uid-old"]
    );
}

#[tokio::test]
async fn pod_slot_persistence_preserves_uid_cas_outcomes_and_monotonic_versions() {
    let db = fresh().await;
    let mut events = db.runtime.subscribe();
    let request = |uid: &str| {
        PodSlotAdmissionRequest::try_new(PodIdentity::new("default", "web", uid), "worker-a")
            .unwrap()
    };
    assert!(matches!(
        db.runtime.try_admit(request("uid-old")).await.unwrap(),
        PodSlotAdmissionResult::Admitted { observed_pod_version }
            if observed_pod_version.get() == 1
    ));
    assert!(matches!(
        events.next_event().await.unwrap(),
        Some(PodSlotAdmissionEvent::Changed {
            pod,
            state: PodSlotAdmissionState::Admitted,
            observed_pod_version,
            ..
        }) if pod.uid == "uid-old" && observed_pod_version.get() == 1
    ));
    assert!(matches!(
        db.runtime.try_admit(request("uid-new")).await.unwrap(),
        PodSlotAdmissionResult::Blocked { blocking_uid, observed_pod_version, .. }
            if blocking_uid == "uid-old" && observed_pod_version.get() == 1
    ));
    assert!(matches!(
        db.runtime.mark_terminating(request("uid-old")).await.unwrap(),
        PodSlotMutationResult::Changed { observed_pod_version }
            if observed_pod_version.get() == 2
    ));
    assert!(matches!(
        db.runtime.clear_if_uid(request("uid-new")).await.unwrap(),
        PodSlotClearResult::UidMismatch { blocking_uid, observed_pod_version, .. }
            if blocking_uid == "uid-old" && observed_pod_version.get() == 2
    ));
    assert!(matches!(
        db.runtime.clear_if_uid(request("uid-old")).await.unwrap(),
        PodSlotClearResult::Cleared { observed_pod_version }
            if observed_pod_version.get() == 3
    ));
    assert!(matches!(
        db.runtime.try_admit(request("uid-new")).await.unwrap(),
        PodSlotAdmissionResult::Admitted { observed_pod_version }
            if observed_pod_version.get() == 4
    ));
}

#[tokio::test]
async fn malformed_endpoint_ports_fail_instead_of_wrapping_to_u16() {
    let db = fresh().await;
    for (tcp, udp) in [
        (Some(65_536_i64), None),
        (None, Some(-1_i64)),
        (Some(0_i64), None),
    ] {
        db.executor
            .call_raw("test:malformed_endpoint_port", move |conn| {
                conn.execute("DELETE FROM pod_endpoints", [])?;
                conn.execute(
                    "INSERT INTO pod_endpoints
                     (pod_uid, namespace, pod_name, node_name, mode, pod_ip, node_ip,
                      host_port_tcp, host_port_udp, generation, updated_ms)
                     VALUES ('bad-port', 'default', 'bad-port', 'node-a', 'hostport',
                             '10.42.0.10', '192.0.2.10', ?1, ?2, 1, 1)",
                    rusqlite::params![tcp, udp],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let error = db
            .network
            .get_endpoint_by_pod_ip("10.42.0.10".parse().unwrap())
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("pod endpoint port outside 1..=65535"),
            "unexpected decode error: {error:#}"
        );
    }
}

#[tokio::test]
async fn runtime_observation_checkpoint_survives_actor_restart() {
    let db = fresh().await;
    db.delivery
        .upsert_runtime_observation_checkpoint(checkpoint(
            "uid-restart",
            &["containerd://ctr-abc", "containerd://ctr-def"],
            2,
            1_000,
        ))
        .await
        .unwrap();
    let loaded = db
        .delivery
        .get_runtime_observation_checkpoint(PodCheckpointKey::try_new("uid-restart").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.pod_uid(), "uid-restart");
    assert_eq!(loaded.generation().get(), 2);
    assert_eq!(
        loaded.container_ids(),
        ["containerd://ctr-abc", "containerd://ctr-def"]
    );
    assert_eq!(loaded.updated_ms(), 1_000);
}

#[tokio::test]
async fn runtime_observation_checkpoint_survives_worker_restart() {
    let db = fresh().await;
    for (uid, container, generation, updated) in [
        ("uid-pod-a", "containerd://ctr-a1", 1, 500),
        ("uid-pod-b", "containerd://ctr-b1", 3, 750),
    ] {
        db.delivery
            .upsert_runtime_observation_checkpoint(checkpoint(
                uid,
                &[container],
                generation,
                updated,
            ))
            .await
            .unwrap();
    }
    let a = PodCheckpointKey::try_new("uid-pod-a").unwrap();
    let b = PodCheckpointKey::try_new("uid-pod-b").unwrap();
    assert_eq!(
        db.delivery
            .get_runtime_observation_checkpoint(a.clone())
            .await
            .unwrap()
            .unwrap()
            .generation()
            .get(),
        1
    );
    assert_eq!(
        db.delivery
            .get_runtime_observation_checkpoint(b.clone())
            .await
            .unwrap()
            .unwrap()
            .generation()
            .get(),
        3
    );
    db.delivery
        .delete_runtime_observation_checkpoint(a.clone())
        .await
        .unwrap();
    assert!(
        db.delivery
            .get_runtime_observation_checkpoint(a)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.delivery
            .get_runtime_observation_checkpoint(b)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn runtime_observation_checkpoint_is_uid_bound() {
    let db = fresh().await;
    db.delivery
        .upsert_runtime_observation_checkpoint(checkpoint(
            "uid-alpha",
            &["containerd://alpha-1"],
            5,
            100,
        ))
        .await
        .unwrap();
    db.delivery
        .upsert_runtime_observation_checkpoint(checkpoint(
            "uid-beta",
            &["containerd://beta-1", "containerd://beta-2"],
            7,
            200,
        ))
        .await
        .unwrap();
    let alpha = PodCheckpointKey::try_new("uid-alpha").unwrap();
    let beta = PodCheckpointKey::try_new("uid-beta").unwrap();
    assert_eq!(
        db.delivery
            .get_runtime_observation_checkpoint(alpha.clone())
            .await
            .unwrap()
            .unwrap()
            .container_ids(),
        ["containerd://alpha-1"]
    );
    assert_eq!(
        db.delivery
            .get_runtime_observation_checkpoint(beta.clone())
            .await
            .unwrap()
            .unwrap()
            .container_ids()
            .len(),
        2
    );
    db.delivery
        .delete_runtime_observation_checkpoint(alpha.clone())
        .await
        .unwrap();
    assert!(
        db.delivery
            .get_runtime_observation_checkpoint(alpha)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.delivery
            .get_runtime_observation_checkpoint(beta)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn runtime_observation_checkpoint_is_removed_after_successful_reconcile() {
    let db = fresh().await;
    let key = PodCheckpointKey::try_new("uid-reconcile").unwrap();
    db.delivery
        .upsert_runtime_observation_checkpoint(checkpoint(
            "uid-reconcile",
            &["containerd://ctr-99"],
            1,
            300,
        ))
        .await
        .unwrap();
    assert!(
        db.delivery
            .get_runtime_observation_checkpoint(key.clone())
            .await
            .unwrap()
            .is_some()
    );
    db.delivery
        .delete_runtime_observation_checkpoint(key.clone())
        .await
        .unwrap();
    assert!(
        db.delivery
            .get_runtime_observation_checkpoint(key)
            .await
            .unwrap()
            .is_none()
    );
}
