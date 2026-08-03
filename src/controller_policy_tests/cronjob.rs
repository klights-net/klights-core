use klights_controllers::cronjob::*;

use crate::datastore::DatastoreBackend;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;

async fn reconcile_cronjob_inner(
    store: &dyn crate::datastore::DatastoreBackend,
    dispatcher: Option<&dyn klights_reconcile_api::ControllerDispatcherPort>,
    cronjob: &serde_json::Value,
    resource_version: i64,
) -> anyhow::Result<()> {
    reconcile_cronjob_one_at(
        store,
        dispatcher,
        cronjob,
        resource_version,
        chrono::Utc::now(),
    )
    .await
}

async fn make_raft_cronjob_datastore() -> crate::bootstrap::sequenced_datastore::SequencedDatastore
{
    use crate::datastore::backend::DatastoreHandle;
    use klights_cluster_core::StorageCommand;
    use klights_kubelet::node_outbox::payload::OutboxOperation;

    struct InlineProposer {
        inner: DatastoreHandle,
    }

    #[async_trait]
    impl klights_replication::proposal::RaftProposal for InlineProposer {
        async fn propose_command(
            &self,
            command: StorageCommand,
        ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
            crate::bootstrap::outbox_apply_adapter::propose_command_on_backend(
                self.inner.as_ref(),
                command,
            )
            .await
            .map_err(|err| anyhow::anyhow!("inline cronjob propose: {err}"))
        }

        async fn propose_outbox_command(
            &self,
            idempotency_key: &str,
            operation: &str,
            command: StorageCommand,
            authoring_node: &str,
            _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
        ) -> std::result::Result<
            klights_cluster_core::OutboxApplyOutcome,
            klights_cluster_core::OutboxApplyError,
        > {
            let outcome =
                crate::bootstrap::outbox_apply_adapter::propose_outbox_command_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    OutboxOperation::try_from(operation).map_err(|err| {
                        klights_cluster_core::OutboxApplyError::Retryable(err.to_string())
                    })?,
                    command,
                    authoring_node,
                    None,
                )
                .await?;
            Ok(outcome.into_parts().0)
        }
    }

    let inner = crate::datastore::test_support::in_memory().await;
    let handle: DatastoreHandle = Arc::new(inner);
    crate::bootstrap::sequenced_datastore::SequencedDatastore::new(
        handle.clone(),
        Arc::new(InlineProposer { inner: handle }),
    )
}

#[tokio::test]
async fn test_cronjob_creates_job_when_due() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();
    let old_creation = klights_cluster_core::k8s_time::format_time(
        chrono::Utc::now() - chrono::Duration::minutes(2),
    );

    // CronJob with every-minute schedule and no lastScheduleTime
    // (so it's immediately due)
    let cj = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": "test-cj",
            "namespace": "default",
            "uid": "cj-uid-1",
            "creationTimestamp": old_creation
        },
        "spec": {
            "schedule": "* * * * *",
            "concurrencyPolicy": "Allow",
            "jobTemplate": {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{"name": "c", "image": "nginx"}],
                            "restartPolicy": "Never"
                        }
                    }
                }
            }
        }
    });

    let created = db
        .create_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "test-cj",
            cj.clone(),
        )
        .await
        .unwrap();

    // Reconcile — should create a Job
    reconcile_cronjob_inner(&db, None, &cj, created.resource_version)
        .await
        .unwrap();

    let jobs = db
        .list_resources(
            "batch/v1",
            "Job",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert!(
        !jobs.items.is_empty(),
        "CronJob reconcile should create at least one Job"
    );
}

#[tokio::test]
async fn test_cronjob_reconcile_persists_last_schedule_time_status() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();
    let old_creation = klights_cluster_core::k8s_time::format_time(
        chrono::Utc::now() - chrono::Duration::minutes(2),
    );

    let cj = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": "test-cj-status",
            "namespace": "default",
            "uid": "cj-uid-status",
            "creationTimestamp": old_creation
        },
        "spec": {
            "schedule": "* * * * *",
            "concurrencyPolicy": "Allow",
            "jobTemplate": {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{"name": "c", "image": "nginx"}],
                            "restartPolicy": "Never"
                        }
                    }
                }
            }
        },
        "status": {}
    });

    let created = db
        .create_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "test-cj-status",
            cj.clone(),
        )
        .await
        .unwrap();

    reconcile_cronjob_inner(&db, None, &cj, created.resource_version)
        .await
        .unwrap();

    let updated = db
        .get_resource("batch/v1", "CronJob", Some("default"), "test-cj-status")
        .await
        .unwrap()
        .unwrap();
    let last_schedule_time = updated
        .data
        .pointer("/status/lastScheduleTime")
        .and_then(|v| v.as_str());
    assert!(
        last_schedule_time.is_some_and(|value| !value.is_empty()),
        "CronJob reconcile must persist status.lastScheduleTime so the event-driven scheduler does not re-fire the same schedule: {:?}",
        updated.data
    );
    assert_eq!(
        updated
            .data
            .pointer("/spec/schedule")
            .and_then(|v| v.as_str()),
        Some("* * * * *"),
        "status write must preserve CronJob spec"
    );
}

#[tokio::test]
async fn test_cronjob_reconcile_persists_last_schedule_time_through_raft_status_path() {
    let db = make_raft_cronjob_datastore().await;
    let old_creation = klights_cluster_core::k8s_time::format_time(
        chrono::Utc::now() - chrono::Duration::minutes(2),
    );

    let cj = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": "test-cj-raft-status",
            "namespace": "default",
            "uid": "cj-uid-raft-status",
            "creationTimestamp": old_creation
        },
        "spec": {
            "schedule": "* * * * *",
            "concurrencyPolicy": "Allow",
            "jobTemplate": {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{"name": "c", "image": "nginx"}],
                            "restartPolicy": "Never"
                        }
                    }
                }
            }
        },
        "status": {}
    });

    let created = db
        .create_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "test-cj-raft-status",
            cj.clone(),
        )
        .await
        .unwrap();

    reconcile_cronjob_inner(&db, None, &cj, created.resource_version)
        .await
        .unwrap();

    let updated = db
        .get_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "test-cj-raft-status",
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        updated
            .data
            .pointer("/status/lastScheduleTime")
            .and_then(|v| v.as_str())
            .is_some_and(|value| !value.is_empty()),
        "raft-routed CronJob reconcile must persist status.lastScheduleTime: {:?}",
        updated.data
    );
}

#[tokio::test]
async fn test_cronjob_stale_snapshot_after_delete_does_not_create_job() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let cj = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": "stale-cj",
            "namespace": "default",
            "uid": "stale-cj-uid"
        },
        "spec": {
            "schedule": "* * * * *",
            "concurrencyPolicy": "Allow",
            "jobTemplate": {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{"name": "c", "image": "nginx"}],
                            "restartPolicy": "Never"
                        }
                    }
                }
            }
        }
    });

    let created = db
        .create_resource("batch/v1", "CronJob", Some("default"), "stale-cj", cj)
        .await
        .unwrap();
    let stale_snapshot = created.data.clone();

    db.delete_resource("batch/v1", "CronJob", Some("default"), "stale-cj")
        .await
        .unwrap();

    reconcile_cronjob_inner(&db, None, &stale_snapshot, created.resource_version)
        .await
        .unwrap();

    let jobs = db
        .list_resources(
            "batch/v1",
            "Job",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert!(
        jobs.items.is_empty(),
        "stale CronJob reconcile after delete must not create Jobs"
    );
}

#[tokio::test]
async fn test_cronjob_reconcile_uses_live_suspend_state() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let cj = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": "suspend-cj",
            "namespace": "default",
            "uid": "suspend-cj-uid"
        },
        "spec": {
            "schedule": "* * * * *",
            "concurrencyPolicy": "Allow",
            "jobTemplate": {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{"name": "c", "image": "nginx"}],
                            "restartPolicy": "Never"
                        }
                    }
                }
            }
        }
    });

    let created = db
        .create_resource("batch/v1", "CronJob", Some("default"), "suspend-cj", cj)
        .await
        .unwrap();
    let stale_snapshot = created.data.clone();
    let mut suspended: serde_json::Value = (*created.data).clone();
    suspended["spec"]["suspend"] = json!(true);
    db.update_resource(
        "batch/v1",
        "CronJob",
        Some("default"),
        "suspend-cj",
        suspended,
        created.resource_version,
    )
    .await
    .unwrap();

    reconcile_cronjob_inner(&db, None, &stale_snapshot, created.resource_version)
        .await
        .unwrap();

    let jobs = db
        .list_resources(
            "batch/v1",
            "Job",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert!(
        jobs.items.is_empty(),
        "CronJob reconcile must observe live spec.suspend before creating Jobs"
    );
}

#[tokio::test]
async fn test_cronjob_created_job_is_reconciled_into_pod() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let dispatcher = crate::controller_test_support::dispatcher_for_test(
        &db,
        std::sync::Arc::new(klights_controllers::service::ServiceIpam::new(
            "10.43.128.0/17",
        )),
    );
    let old_creation = klights_cluster_core::k8s_time::format_time(
        chrono::Utc::now() - chrono::Duration::minutes(2),
    );

    let cj = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": "test-cj-reconcile",
            "namespace": "default",
            "uid": "cj-uid-reconcile",
            "creationTimestamp": old_creation
        },
        "spec": {
            "schedule": "* * * * *",
            "concurrencyPolicy": "Allow",
            "jobTemplate": {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{"name": "c", "image": "nginx"}],
                            "restartPolicy": "Never"
                        }
                    }
                }
            }
        }
    });

    let created = db
        .create_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "test-cj-reconcile",
            cj.clone(),
        )
        .await
        .unwrap();

    reconcile_cronjob_inner(
        &db,
        Some(dispatcher.as_ref()),
        &cj,
        created.resource_version,
    )
    .await
    .unwrap();
    dispatcher.dispatch_next_key_for_test().await;

    let pods = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(
        pods.items.len(),
        1,
        "CronJob-created Job must be enqueued so the Job controller creates its Pod"
    );
}

#[tokio::test]
async fn test_cronjob_reconcile_uses_live_resource_for_status_after_stale_input_rv() {
    // The reconciler re-reads the live CronJob before acting. Status writes
    // must therefore use that live row's resourceVersion, not the older RV
    // carried by the event/timer that triggered this reconcile.
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let cj = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {"name": "test-cj-prop", "namespace": "default", "uid": "cj-uid-prop"},
        "spec": {
            "schedule": "* * * * *",
            "concurrencyPolicy": "Allow",
            "jobTemplate": {"spec": {"template": {"spec": {
                "containers": [{"name": "c", "image": "nginx"}],
                "restartPolicy": "Never"
            }}}}
        }
    });
    db.create_resource(
        "batch/v1",
        "CronJob",
        Some("default"),
        "test-cj-prop",
        cj.clone(),
    )
    .await
    .unwrap();

    // Pass a stale resource_version that cannot match the row's current RV.
    // The live CronJob row read inside reconcile still has the valid guard.
    let stale_rv: i64 = 999_999;
    let result = reconcile_cronjob_inner(&db, None, &cj, stale_rv).await;
    assert!(
        result.is_ok(),
        "reconcile_cronjob must write status from the live row despite stale input RV, got {result:?}"
    );

    let live = db
        .get_resource("batch/v1", "CronJob", Some("default"), "test-cj-prop")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        live.data.pointer("/status/observedGeneration"),
        Some(&json!(1)),
        "successful reconcile should persist status through the live-row status writer"
    );
}

#[tokio::test]
async fn test_cronjob_forbid_concurrent_skips_when_active_job() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    // Create an existing active Job owned by the CronJob
    let existing_job = json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": "test-cj-existing",
            "namespace": "default",
            "uid": "job-uid-1",
            "ownerReferences": [{"apiVersion": "batch/v1", "kind": "CronJob", "name": "test-cj2", "uid": "cj-uid-2", "controller": true}]
        },
        "spec": {"template": {"spec": {"containers": [{"name": "c", "image": "nginx"}], "restartPolicy": "Never"}}}
    });
    db.create_resource(
        "batch/v1",
        "Job",
        Some("default"),
        "test-cj-existing",
        existing_job,
    )
    .await
    .unwrap();

    let cj = json!({
        "apiVersion": "batch/v1", "kind": "CronJob",
        "metadata": {"name": "test-cj2", "namespace": "default", "uid": "cj-uid-2"},
        "spec": {
            "schedule": "* * * * *",
            "concurrencyPolicy": "ForbidConcurrent",
            "jobTemplate": {"spec": {"template": {"spec": {"containers": [{"name": "c", "image": "nginx"}], "restartPolicy": "Never"}}}}
        }
    });
    let created = db
        .create_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "test-cj2",
            cj.clone(),
        )
        .await
        .unwrap();

    reconcile_cronjob_inner(&db, None, &cj, created.resource_version)
        .await
        .unwrap();

    let jobs = db
        .list_resources(
            "batch/v1",
            "Job",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    // Only the pre-existing job; ForbidConcurrent should not have created a new one
    assert_eq!(
        jobs.items.len(),
        1,
        "ForbidConcurrent should not create additional Jobs when one is active"
    );
}

#[tokio::test]
async fn test_cronjob_does_not_schedule_before_creation_timestamp() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let creation_timestamp = klights_cluster_core::k8s_time::format_time(chrono::Utc::now());
    let cj = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": "test-cj-new",
            "namespace": "default",
            "uid": "cj-uid-new",
            "creationTimestamp": creation_timestamp
        },
        "spec": {
            "schedule": "* * * * *",
            "concurrencyPolicy": "ForbidConcurrent",
            "jobTemplate": {"spec": {"template": {"spec": {
                "containers": [{"name": "c", "image": "nginx"}],
                "restartPolicy": "Never"
            }}}}
        }
    });
    let created = db
        .create_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "test-cj-new",
            cj.clone(),
        )
        .await
        .unwrap();

    reconcile_cronjob_inner(&db, None, &cj, created.resource_version)
        .await
        .unwrap();

    let jobs = db
        .list_resources(
            "batch/v1",
            "Job",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert!(
        jobs.items.is_empty(),
        "CronJob reconcile must not create a Job for a schedule before creationTimestamp"
    );
}

#[tokio::test]
async fn test_cronjob_history_limits_cleanup_old_completed_jobs() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let cj_uid = "cj-uid-history";
    let cj = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {"name": "test-cj-history", "namespace": "default", "uid": cj_uid},
        "spec": {
            "schedule": "0 0 1 1 *",
            "concurrencyPolicy": "Allow",
            "successfulJobsHistoryLimit": 1,
            "failedJobsHistoryLimit": 1,
            "jobTemplate": {"spec": {"template": {"spec": {
                "containers": [{"name": "c", "image": "nginx"}],
                "restartPolicy": "Never"
            }}}}
        }
    });

    let created = db
        .create_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "test-cj-history",
            cj.clone(),
        )
        .await
        .unwrap();

    // Create 3 old completed successful jobs
    for i in 0..3 {
        let job = json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": format!("test-cj-history-{}", 1000 + i),
                "namespace": "default",
                "uid": format!("job-success-{}", i),
                "creationTimestamp": format!("2025-01-0{}T00:00:00Z", i + 1),
                "ownerReferences": [{
                    "apiVersion": "batch/v1", "kind": "CronJob",
                    "name": "test-cj-history", "uid": cj_uid, "controller": true
                }]
            },
            "spec": {"template": {"spec": {"containers": [{"name": "c", "image": "nginx"}], "restartPolicy": "Never"}}},
            "status": {"conditions": [{"type": "Complete", "status": "True"}]}
        });
        db.create_resource(
            "batch/v1",
            "Job",
            Some("default"),
            &format!("test-cj-history-{}", 1000 + i),
            job,
        )
        .await
        .unwrap();
    }

    // Create 2 old completed failed jobs
    for i in 0..2 {
        let job = json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": format!("test-cj-history-{}", 2000 + i),
                "namespace": "default",
                "uid": format!("job-failed-{}", i),
                "creationTimestamp": format!("2025-01-0{}T00:00:00Z", i + 1),
                "ownerReferences": [{
                    "apiVersion": "batch/v1", "kind": "CronJob",
                    "name": "test-cj-history", "uid": cj_uid, "controller": true
                }]
            },
            "spec": {"template": {"spec": {"containers": [{"name": "c", "image": "nginx"}], "restartPolicy": "Never"}}},
            "status": {"conditions": [{"type": "Failed", "status": "True"}]}
        });
        db.create_resource(
            "batch/v1",
            "Job",
            Some("default"),
            &format!("test-cj-history-{}", 2000 + i),
            job,
        )
        .await
        .unwrap();
    }

    // Reconcile — should clean up old jobs exceeding limits
    reconcile_cronjob_inner(&db, None, &cj, created.resource_version)
        .await
        .unwrap();

    let jobs = db
        .list_resources(
            "batch/v1",
            "Job",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    // Should have 1 successful + 1 failed = 2 jobs remaining
    // (oldest ones deleted: success-0, success-1, failed-0 deleted)
    assert_eq!(
        jobs.items.len(),
        2,
        "Should keep only 1 successful + 1 failed job"
    );

    // Verify the remaining jobs are the newest ones
    let job_names: Vec<String> = jobs
        .items
        .iter()
        .map(|j| j.data["metadata"]["name"].as_str().unwrap().to_string())
        .collect();
    assert!(
        job_names.contains(&"test-cj-history-1002".to_string()),
        "Should keep newest successful job"
    );
    assert!(
        job_names.contains(&"test-cj-history-2001".to_string()),
        "Should keep newest failed job"
    );
    assert!(
        !job_names.contains(&"test-cj-history-1000".to_string()),
        "Should delete oldest successful job"
    );
    assert!(
        !job_names.contains(&"test-cj-history-1001".to_string()),
        "Should delete oldest successful job"
    );
    assert!(
        !job_names.contains(&"test-cj-history-2000".to_string()),
        "Should delete oldest failed job"
    );
}

#[tokio::test]
async fn test_cronjob_history_limits_keep_five_successful_and_two_failed_jobs() {
    let db = crate::datastore::test_support::in_memory().await;
    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();

    let cj_uid = "cj-uid-history-5-2";
    let cj = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {"name": "test-cj-history-5-2", "namespace": "default", "uid": cj_uid},
        "spec": {
            "schedule": "0 0 1 1 *",
            "suspend": true,
            "successfulJobsHistoryLimit": 5,
            "failedJobsHistoryLimit": 2,
            "jobTemplate": {"spec": {"template": {"spec": {
                "containers": [{"name": "c", "image": "nginx"}],
                "restartPolicy": "Never"
            }}}}
        }
    });

    let created = db
        .create_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "test-cj-history-5-2",
            cj.clone(),
        )
        .await
        .unwrap();

    for i in 0..7 {
        let name = format!("test-cj-history-5-2-success-{i}");
        db.create_resource(
            "batch/v1",
            "Job",
            Some("default"),
            &name,
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": name,
                    "namespace": "default",
                    "uid": format!("job-success-5-2-{i}"),
                    "creationTimestamp": format!("2025-01-{:02}T00:00:00Z", i + 1),
                    "ownerReferences": [{
                        "apiVersion": "batch/v1",
                        "kind": "CronJob",
                        "name": "test-cj-history-5-2",
                        "uid": cj_uid,
                        "controller": true
                    }]
                },
                "spec": {"template": {"spec": {
                    "containers": [{"name": "c", "image": "nginx"}],
                    "restartPolicy": "Never"
                }}},
                "status": {"conditions": [{"type": "Complete", "status": "True"}]}
            }),
        )
        .await
        .unwrap();
    }

    for i in 0..4 {
        let name = format!("test-cj-history-5-2-failed-{i}");
        db.create_resource(
            "batch/v1",
            "Job",
            Some("default"),
            &name,
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": name,
                    "namespace": "default",
                    "uid": format!("job-failed-5-2-{i}"),
                    "creationTimestamp": format!("2025-02-{:02}T00:00:00Z", i + 1),
                    "ownerReferences": [{
                        "apiVersion": "batch/v1",
                        "kind": "CronJob",
                        "name": "test-cj-history-5-2",
                        "uid": cj_uid,
                        "controller": true
                    }]
                },
                "spec": {"template": {"spec": {
                    "containers": [{"name": "c", "image": "nginx"}],
                    "restartPolicy": "Never"
                }}},
                "status": {"conditions": [{"type": "Failed", "status": "True"}]}
            }),
        )
        .await
        .unwrap();
    }

    reconcile_cronjob_inner(&db, None, &cj, created.resource_version)
        .await
        .unwrap();

    let jobs = db
        .list_resources(
            "batch/v1",
            "Job",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let job_names: std::collections::HashSet<String> =
        jobs.items.iter().map(|job| job.name.clone()).collect();

    assert_eq!(
        job_names.len(),
        7,
        "CronJob should retain 5 successful and 2 failed Jobs"
    );
    for i in 0..2 {
        assert!(
            !job_names.contains(&format!("test-cj-history-5-2-success-{i}")),
            "oldest successful Jobs above successfulJobsHistoryLimit must be deleted"
        );
    }
    for i in 2..7 {
        assert!(
            job_names.contains(&format!("test-cj-history-5-2-success-{i}")),
            "newest successful Jobs within successfulJobsHistoryLimit must be retained"
        );
    }
    for i in 0..2 {
        assert!(
            !job_names.contains(&format!("test-cj-history-5-2-failed-{i}")),
            "oldest failed Jobs above failedJobsHistoryLimit must be deleted"
        );
    }
    for i in 2..4 {
        assert!(
            job_names.contains(&format!("test-cj-history-5-2-failed-{i}")),
            "newest failed Jobs within failedJobsHistoryLimit must be retained"
        );
    }
}
