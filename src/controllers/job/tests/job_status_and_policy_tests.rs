use super::*;

fn indexed_job_pod_resource(
    id: i64,
    job_name: &str,
    job_uid: &str,
    pod_name: &str,
    index: i64,
    phase: &str,
    deletion_timestamp: Option<&str>,
) -> crate::datastore::Resource {
    let mut metadata = json!({
        "name": pod_name,
        "namespace": "default",
        "annotations": {
            "batch.kubernetes.io/job-completion-index": index.to_string()
        },
        "ownerReferences": [{
            "apiVersion": "batch/v1",
            "kind": "Job",
            "name": job_name,
            "uid": job_uid
        }]
    });
    if let Some(ts) = deletion_timestamp {
        metadata["deletionTimestamp"] = json!(ts);
    }

    crate::datastore::Resource {
        id,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: pod_name.to_string(),
        uid: format!("uid-{pod_name}"),
        resource_version: 1,
        data: std::sync::Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": metadata,
            "status": {"phase": phase}
        })),
    }
}

#[test]
fn test_job_onfailure_restart_counts_exceeding_backoff_marks_failed() {
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "restart-backoff-job", "namespace": "default", "uid": "job-restart-backoff"},
        "spec": {
            "backoffLimit": 1,
            "template": {"spec": {
                "containers": [{"name": "c", "image": "busybox"}],
                "restartPolicy": "OnFailure"
            }}
        }
    });
    let pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "restart-backoff-job-pod".to_string(),
        uid: "uid-restart-backoff-job-pod".to_string(),
        resource_version: 1,
        data: std::sync::Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "restart-backoff-job-pod",
                "namespace": "default",
                "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job", "name": "restart-backoff-job", "uid": "job-restart-backoff"}]
            },
            "spec": {"restartPolicy": "OnFailure"},
            "status": {
                "phase": "Running",
                "containerStatuses": [{
                    "name": "c",
                    "ready": false,
                    "restartCount": 2,
                    "state": {"waiting": {"reason": "CrashLoopBackOff"}}
                }]
            }
        })),
    };

    let status = derive_job_status_from_owned_pods(&job, &[pod]);
    let conditions = status["conditions"].as_array().unwrap();
    let failed = conditions
        .iter()
        .find(|c| c["type"] == "Failed" && c["status"] == "True")
        .expect("restartCount above backoffLimit must mark Job Failed");
    assert_eq!(failed["reason"], "BackoffLimitExceeded");
}

#[test]
fn test_finished_job_status_preserves_terminal_transition_time() {
    let finished_at = "2026-05-21T00:00:00Z";
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "stable-finish-time",
            "namespace": "default",
            "uid": "uid-stable-finish-time"
        },
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        },
        "status": {
            "conditions": [{
                "type": "Complete",
                "status": "True",
                "reason": "CompletionsReached",
                "lastTransitionTime": finished_at
            }],
            "completionTime": finished_at,
            "succeeded": 1
        }
    });
    let pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "stable-finish-time-pod".to_string(),
        uid: "uid-stable-finish-time-pod".to_string(),
        resource_version: 1,
        data: std::sync::Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "stable-finish-time-pod",
                "namespace": "default",
                "ownerReferences": [{
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "name": "stable-finish-time",
                    "uid": "uid-stable-finish-time",
                    "controller": true
                }]
            },
            "spec": {"restartPolicy": "Never"},
            "status": {"phase": "Succeeded"}
        })),
    };

    let status = derive_job_status_from_owned_pods(&job, &[pod]);
    let complete = status["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == "Complete")
        .expect("completed Job should retain Complete condition");
    assert_eq!(
        complete["lastTransitionTime"], finished_at,
        "terminal condition transition time must not be refreshed by later reconciles"
    );
    assert_eq!(
        status["completionTime"], finished_at,
        "Job completionTime must not be refreshed by later reconciles"
    );
}

#[test]
fn test_finished_job_status_preserves_terminal_update_time_when_state_unchanged() {
    let finished_at = "2026-05-21T00:00:00Z";
    let updated_at = "2026-05-21T00:00:01Z";
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "stable-update-time",
            "namespace": "default",
            "uid": "uid-stable-update-time"
        },
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        },
        "status": {
            "conditions": [{
                "type": "Complete",
                "status": "True",
                "reason": "CompletionsReached",
                "message": "Reached expected number of succeeded pods",
                "lastProbeTime": null,
                "lastTransitionTime": finished_at,
                "lastUpdateTime": updated_at
            }],
            "completionTime": finished_at,
            "succeeded": 1
        }
    });
    let pod = crate::datastore::Resource {
        id: 1,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "stable-update-time-pod".to_string(),
        uid: "uid-stable-update-time-pod".to_string(),
        resource_version: 1,
        data: std::sync::Arc::new(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "stable-update-time-pod",
                "namespace": "default",
                "ownerReferences": [{
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "name": "stable-update-time",
                    "uid": "uid-stable-update-time",
                    "controller": true
                }]
            },
            "spec": {"restartPolicy": "Never"},
            "status": {"phase": "Succeeded"}
        })),
    };

    let status = derive_job_status_from_owned_pods(&job, &[pod]);
    let complete = status["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == "Complete")
        .expect("completed Job should retain Complete condition");

    assert_eq!(
        complete["lastUpdateTime"], updated_at,
        "unchanged Job terminal condition state must preserve lastUpdateTime"
    );
}

#[tokio::test]
async fn test_job_success_policy_not_met_no_condition() {
    // Job with successPolicy but threshold not yet reached
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "sp-unmet-job",
            "namespace": "default",
            "uid": "job-sp-3"
        },
        "spec": {
            "completions": 5,
            "parallelism": 5,
            "successPolicy": {
                "rules": [
                    {"succeededCount": 3}
                ]
            },
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "sp-unmet-job", job)
        .await
        .unwrap();

    // Reconcile 1 - create pods
    let job = get_job(&db, "default", "sp-unmet-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "job-sp-3")
        .await
        .unwrap();

    // Mark only 2 pods as Succeeded (rule requires 3)
    for pod in pods.iter().take(2) {
        let mut pod_data: serde_json::Value = (*pod.data).clone();
        pod_data["status"] = json!({"phase": "Succeeded"});
        let pod_data_name = pod_data["metadata"]["name"].as_str().unwrap().to_string();
        db.update_resource(
            "v1",
            "Pod",
            Some("default"),
            &pod_data_name,
            pod_data,
            pod.resource_version,
        )
        .await
        .unwrap();
    }

    // Reconcile 2 - SuccessCriteriaMet should NOT be set (only 2 succeeded, need 3)
    let job = get_job(&db, "default", "sp-unmet-job").await;
    let job = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let conditions = job["status"]["conditions"].as_array().unwrap();
    let scm = conditions
        .iter()
        .find(|c| c["type"] == "SuccessCriteriaMet");
    assert!(
        scm.is_none(),
        "Should NOT have SuccessCriteriaMet when threshold not reached (2 < 3)"
    );
}

#[tokio::test]
async fn test_nonindexed_job_deletes_excess_active_pods_above_parallelism() {
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "cap-job", "namespace": "default", "uid": "job-cap"},
        "spec": {
            "completions": 4,
            "parallelism": 2,
            "template": {
                "metadata": {"labels": {"job": "cap-job"}},
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "cap-job", job)
        .await
        .unwrap();

    for suffix in ["a", "b", "c"] {
        let pod_name = format!("cap-job-{suffix}");
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "default",
                "labels": {"job": "cap-job"},
                "ownerReferences": [{
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "name": "cap-job",
                    "uid": "job-cap",
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {
                "containers": [{"name": "worker", "image": "busybox"}],
                "restartPolicy": "Never"
            },
            "status": {"phase": "Running"}
        });
        db.create_resource("v1", "Pod", Some("default"), &pod_name, pod)
            .await
            .unwrap();
    }

    let job = get_job(&db, "default", "cap-job").await;
    let result = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "job-cap")
        .await
        .unwrap();
    let active_pods = pods
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_none())
        .count();
    assert_eq!(
        active_pods, 2,
        "job controller must cap active pods at parallelism"
    );
    assert_eq!(result["status"]["active"].as_i64(), Some(2));
}

#[tokio::test]
async fn test_job_missing_metadata_returns_error() {
    let db = crate::datastore::test_support::in_memory().await;

    let bad_job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "spec": {
            "template": {
                "spec": {"containers": [{"name": "c", "image": "img"}]}
            }
        }
    });

    let result = reconcile_job_test(&db, &bad_job, "test-node").await;
    assert!(result.is_err(), "Missing metadata should return error");
    assert!(
        result.unwrap_err().to_string().contains("Missing metadata"),
        "Error should mention missing metadata"
    );
}

#[tokio::test]
async fn test_job_missing_spec_returns_error() {
    let db = crate::datastore::test_support::in_memory().await;

    let bad_job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "bad", "namespace": "default", "uid": "uid"}
    });
    let bad_job = crate::controllers::test_utils::store_and_prepare(
        &db,
        "batch/v1",
        "Job",
        Some("default"),
        "bad",
        bad_job,
    )
    .await;

    let result = reconcile_job_test(&db, &bad_job, "test-node").await;
    assert!(result.is_err(), "Missing spec should return error");
    assert!(
        result.unwrap_err().to_string().contains("Missing spec"),
        "Error should mention missing spec"
    );
}

#[tokio::test]
async fn test_job_default_values() {
    // When completions, parallelism, backoffLimit are not specified,
    // defaults should be: completions=1, parallelism=1, backoffLimit=6
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "defaults-job", "namespace": "default", "uid": "job-defaults"},
        "spec": {
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "defaults-job", job)
        .await
        .unwrap();

    let job = get_job(&db, "default", "defaults-job").await;
    let result = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    // Default completions=1, parallelism=1 → creates exactly 1 pod
    let pods = crate::controllers::find_owned_pods(&db, "default", "job-defaults")
        .await
        .unwrap();
    assert_eq!(pods.len(), 1, "Default completions=1 should create 1 pod");

    // Active should be 1
    assert_eq!(result["status"]["active"].as_i64().unwrap(), 1);
}

#[tokio::test]
async fn test_job_status_counts_ready_active_pods() {
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "ready-job", "namespace": "default", "uid": "job-ready"},
        "spec": {
            "completions": 3,
            "parallelism": 3,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "ready-job", job)
        .await
        .unwrap();

    let job = get_job(&db, "default", "ready-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "job-ready")
        .await
        .unwrap();
    assert_eq!(pods.len(), 3);

    for pod in pods.iter().take(2) {
        let mut pod_data: serde_json::Value = (*pod.data).clone();
        let pod_name = pod_data["metadata"]["name"].as_str().unwrap().to_string();
        pod_data["status"] = json!({
            "phase": "Running",
            "conditions": [{
                "type": "Ready",
                "status": "True"
            }]
        });
        db.update_resource(
            "v1",
            "Pod",
            Some("default"),
            &pod_name,
            pod_data,
            pod.resource_version,
        )
        .await
        .unwrap();
    }

    let job = get_job(&db, "default", "ready-job").await;
    let result = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    assert_eq!(result["status"]["active"].as_i64().unwrap(), 3);
    assert_eq!(result["status"]["ready"].as_i64(), Some(2));
}

#[tokio::test]
async fn test_job_skips_reconcile_when_deletion_timestamp_set() {
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "deleting-job",
            "namespace": "default",
            "uid": "job-del",
            "deletionTimestamp": "2026-04-12T00:00:00Z"
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    let result = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    // Should return the job unchanged, no pods created
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
        0,
        "No pods should be created for a Job being deleted"
    );
    assert_eq!(result["metadata"]["name"].as_str(), Some("deleting-job"));
}

#[tokio::test]
async fn test_job_max_failed_indexes_triggers_failure() {
    // Job with maxFailedIndexes=1: when failed_count > 1, Job should be Failed
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "indexed-job", "namespace": "default", "uid": "job-mfi"},
        "spec": {
            "completionMode": "Indexed",
            "completions": 5,
            "parallelism": 5,
            "maxFailedIndexes": 1,
            "backoffLimit": 100,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "indexed-job", job)
        .await
        .unwrap();

    // Create 2 failed pods (exceeds maxFailedIndexes=1)
    for i in 0..2 {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": format!("indexed-job-pod-{}", i),
                "namespace": "default",
                "uid": format!("pod-mfi-{}", i),
                "ownerReferences": [{"uid": "job-mfi", "kind": "Job"}]
            },
            "spec": {"containers": [{"name": "worker"}]},
            "status": {"phase": "Failed"}
        });
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &format!("indexed-job-pod-{}", i),
            pod,
        )
        .await
        .unwrap();
    }

    let job = get_job(&db, "default", "indexed-job").await;
    let result = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let conditions = result["status"]["conditions"].as_array().unwrap();
    let failed_cond = conditions.iter().find(|c| c["type"] == "Failed");
    assert!(
        failed_cond.is_some(),
        "Job must have Failed condition when maxFailedIndexes exceeded"
    );
    assert_eq!(failed_cond.unwrap()["status"], "True");
    assert_eq!(
        failed_cond.unwrap()["reason"].as_str().unwrap(),
        "MaxFailedIndexesExceeded"
    );
}

#[tokio::test]
async fn test_job_max_failed_indexes_not_exceeded_continues() {
    // Job with maxFailedIndexes=2: when failed_count <= 2, job should NOT be Failed
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "indexed-job2", "namespace": "default", "uid": "job-mfi2"},
        "spec": {
            "completionMode": "Indexed",
            "completions": 5,
            "parallelism": 5,
            "maxFailedIndexes": 2,
            "backoffLimit": 100,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "indexed-job2", job)
        .await
        .unwrap();

    // Create exactly 2 failed pods (== maxFailedIndexes, NOT exceeding)
    for i in 0..2 {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": format!("indexed-job2-pod-{}", i),
                "namespace": "default",
                "uid": format!("pod-mfi2-{}", i),
                "ownerReferences": [{"uid": "job-mfi2", "kind": "Job"}]
            },
            "spec": {"containers": [{"name": "worker"}]},
            "status": {"phase": "Failed"}
        });
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &format!("indexed-job2-pod-{}", i),
            pod,
        )
        .await
        .unwrap();
    }

    let job = get_job(&db, "default", "indexed-job2").await;
    let result = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let conditions = result["status"]["conditions"].as_array().unwrap();
    let failed_cond = conditions.iter().find(|c| c["type"] == "Failed");
    assert!(
        failed_cond.is_none(),
        "Job must NOT be Failed when failed_count == maxFailedIndexes (need strictly greater)"
    );
}

#[tokio::test]
async fn test_job_pod_failure_policy_fail_on_exit_code() {
    // Sonobuoy test: podFailurePolicy with onExitCodes operator=In values=[42]
    // When a pod fails with exit code 42, Job should be marked Failed immediately.
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "pfp-job", "namespace": "default", "uid": "job-pfp"},
        "spec": {
            "completions": 3,
            "parallelism": 3,
            "backoffLimit": 100,
            "podFailurePolicy": {
                "rules": [
                    {
                        "action": "FailJob",
                        "onExitCodes": {
                            "operator": "In",
                            "values": [42]
                        }
                    }
                ]
            },
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "pfp-job", job)
        .await
        .unwrap();

    // Create a failed pod with exit code 42
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pfp-pod-0",
            "namespace": "default",
            "uid": "pod-pfp-0",
            "ownerReferences": [{"uid": "job-pfp", "kind": "Job"}]
        },
        "spec": {"containers": [{"name": "worker", "image": "busybox"}]},
        "status": {
            "phase": "Failed",
            "containerStatuses": [{
                "name": "worker",
                "state": {
                    "terminated": {
                        "exitCode": 42,
                        "reason": "Error"
                    }
                }
            }]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "pfp-pod-0", pod)
        .await
        .unwrap();

    let job = get_job(&db, "default", "pfp-job").await;
    let result = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let conditions = result["status"]["conditions"].as_array().unwrap();
    let failed_cond = conditions.iter().find(|c| c["type"] == "Failed");
    assert!(
        failed_cond.is_some(),
        "Job must have Failed condition when pod fails with matched exit code"
    );
    assert_eq!(failed_cond.unwrap()["status"], "True");
    assert_eq!(
        failed_cond.unwrap()["reason"].as_str().unwrap(),
        "PodFailurePolicy"
    );
}

#[tokio::test]
async fn test_job_pod_failure_policy_non_matching_exit_code_continues() {
    // podFailurePolicy with exit code 42 — pod exits with code 1 → no early failure
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "pfp-job2", "namespace": "default", "uid": "job-pfp2"},
        "spec": {
            "completions": 3,
            "parallelism": 3,
            "backoffLimit": 100,
            "podFailurePolicy": {
                "rules": [
                    {
                        "action": "FailJob",
                        "onExitCodes": {
                            "operator": "In",
                            "values": [42]
                        }
                    }
                ]
            },
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "pfp-job2", job)
        .await
        .unwrap();

    // Create a failed pod with exit code 1 (does NOT match policy exit code 42)
    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "pfp2-pod-0",
            "namespace": "default",
            "uid": "pod-pfp2-0",
            "ownerReferences": [{"uid": "job-pfp2", "kind": "Job"}]
        },
        "spec": {"containers": [{"name": "worker"}]},
        "status": {
            "phase": "Failed",
            "containerStatuses": [{
                "name": "worker",
                "state": {
                    "terminated": {"exitCode": 1, "reason": "Error"}
                }
            }]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "pfp2-pod-0", pod)
        .await
        .unwrap();

    let job = get_job(&db, "default", "pfp-job2").await;
    let result = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let conditions = result["status"]["conditions"].as_array().unwrap();
    let failed_cond = conditions.iter().find(|c| c["type"] == "Failed");
    assert!(
        failed_cond.is_none(),
        "Job must NOT be Failed when pod exit code does not match policy"
    );
}

#[tokio::test]
async fn test_job_pod_failure_policy_ignore_exit_code_excludes_failed_count() {
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "pfp-ignore-exit", "namespace": "default", "uid": "job-ignore-exit"},
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "backoffLimit": 0,
            "podFailurePolicy": {
                "rules": [{
                    "action": "Ignore",
                    "onExitCodes": {"operator": "In", "values": [42]}
                }]
            },
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });
    db.create_resource("batch/v1", "Job", Some("default"), "pfp-ignore-exit", job)
        .await
        .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "ignore-exit-pod",
            "namespace": "default",
            "uid": "pod-ignore-exit",
            "ownerReferences": [{"uid": "job-ignore-exit", "kind": "Job"}]
        },
        "spec": {"containers": [{"name": "worker"}]},
        "status": {
            "phase": "Failed",
            "containerStatuses": [{
                "name": "worker",
                "state": {"terminated": {"exitCode": 42, "reason": "Error"}}
            }]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "ignore-exit-pod", pod)
        .await
        .unwrap();

    let job = get_job(&db, "default", "pfp-ignore-exit").await;
    let result = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    assert_eq!(result["status"]["failed"], json!(0));
    let failed_cond = result["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"] == "Failed");
    assert!(
        failed_cond.is_none(),
        "Ignore matching onExitCodes must not trip backoffLimit"
    );
}

#[tokio::test]
async fn test_job_pod_failure_policy_ignore_disruption_target_excludes_failed_count() {
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "pfp-ignore-disruption", "namespace": "default", "uid": "job-ignore-disruption"},
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "backoffLimit": 0,
            "podFailurePolicy": {
                "rules": [{
                    "action": "Ignore",
                    "onPodConditions": [{"type": "DisruptionTarget", "status": "True"}]
                }]
            },
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });
    db.create_resource(
        "batch/v1",
        "Job",
        Some("default"),
        "pfp-ignore-disruption",
        job,
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "ignore-disruption-pod",
            "namespace": "default",
            "uid": "pod-ignore-disruption",
            "ownerReferences": [{"uid": "job-ignore-disruption", "kind": "Job"}]
        },
        "spec": {"containers": [{"name": "worker"}]},
        "status": {
            "phase": "Failed",
            "conditions": [{"type": "DisruptionTarget", "status": "True"}],
            "containerStatuses": [{
                "name": "worker",
                "state": {"terminated": {"exitCode": 137, "reason": "Error"}}
            }]
        }
    });
    db.create_resource("v1", "Pod", Some("default"), "ignore-disruption-pod", pod)
        .await
        .unwrap();

    let job = get_job(&db, "default", "pfp-ignore-disruption").await;
    let result = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    assert_eq!(result["status"]["failed"], json!(0));
    let failed_cond = result["status"]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["type"] == "Failed");
    assert!(
        failed_cond.is_none(),
        "Ignore matching onPodConditions must not trip backoffLimit"
    );
}

#[tokio::test]
async fn test_indexed_job_failindex_marks_failed_indexes_and_fails_when_all_failed() {
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "fi-job", "namespace": "default", "uid": "job-fi"},
        "spec": {
            "completionMode": "Indexed",
            "completions": 3,
            "parallelism": 3,
            "backoffLimit": 100,
            "podFailurePolicy": {
                "rules": [{
                    "action": "FailIndex",
                    "onExitCodes": {"operator": "In", "values": [42]}
                }]
            },
            "template": {
                "spec": {
                    "containers": [{"name": "c", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });
    db.create_resource("batch/v1", "Job", Some("default"), "fi-job", job)
        .await
        .unwrap();

    for i in 0..3 {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": format!("fi-pod-{i}"),
                "namespace": "default",
                "uid": format!("fi-pod-uid-{i}"),
                "annotations": {"batch.kubernetes.io/job-completion-index": i.to_string()},
                "ownerReferences": [{"uid": "job-fi", "kind": "Job"}]
            },
            "spec": {"containers": [{"name": "c"}]},
            "status": {
                "phase": "Failed",
                "containerStatuses": [{
                    "name": "c",
                    "state": {"terminated": {"exitCode": 42}}
                }]
            }
        });
        db.create_resource("v1", "Pod", Some("default"), &format!("fi-pod-{i}"), pod)
            .await
            .unwrap();
    }

    let job = get_job(&db, "default", "fi-job").await;
    let result = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    assert_eq!(result["status"]["failedIndexes"], "0-2");
    let conditions = result["status"]["conditions"].as_array().unwrap();
    let failed_cond = conditions.iter().find(|c| c["type"] == "Failed").unwrap();
    assert_eq!(failed_cond["status"], "True");
    assert_eq!(failed_cond["reason"], "FailedIndexes");
}

#[tokio::test]
async fn test_indexed_job_max_failed_indexes_uses_status_failed_indexes() {
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "mfi-job", "namespace": "default", "uid": "job-mfi-new"},
        "spec": {
            "completionMode": "Indexed",
            "completions": 5,
            "parallelism": 5,
            "backoffLimitPerIndex": 0,
            "maxFailedIndexes": 1,
            "template": {
                "spec": {
                    "containers": [{"name": "c", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });
    db.create_resource("batch/v1", "Job", Some("default"), "mfi-job", job)
        .await
        .unwrap();

    for i in 0..2 {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": format!("mfi-pod-{i}"),
                "namespace": "default",
                "uid": format!("mfi-pod-uid-{i}"),
                "annotations": {"batch.kubernetes.io/job-completion-index": i.to_string()},
                "ownerReferences": [{"uid": "job-mfi-new", "kind": "Job"}]
            },
            "spec": {"containers": [{"name": "c"}]},
            "status": {"phase": "Failed"}
        });
        db.create_resource("v1", "Pod", Some("default"), &format!("mfi-pod-{i}"), pod)
            .await
            .unwrap();
    }

    let job = get_job(&db, "default", "mfi-job").await;
    let result = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    assert_eq!(result["status"]["failedIndexes"], "0-1");
    let conditions = result["status"]["conditions"].as_array().unwrap();
    let failed_cond = conditions.iter().find(|c| c["type"] == "Failed").unwrap();
    assert_eq!(failed_cond["reason"], "MaxFailedIndexesExceeded");
}

#[tokio::test]
async fn test_job_suspend_prevents_pod_creation() {
    let db = crate::datastore::test_support::in_memory().await;
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "susp-job", "namespace": "default", "uid": "uid-susp"},
        "spec": {
            "suspend": true,
            "completions": 3,
            "parallelism": 3,
            "template": {"spec": {"containers": [{"name": "worker", "image": "busybox"}]}}
        },
        "status": {}
    });
    db.create_resource("batch/v1", "Job", Some("default"), "susp-job", job)
        .await
        .unwrap();

    let job = get_job(&db, "default", "susp-job").await;
    let result = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    // No pods should be created when suspended
    let owned = crate::controllers::find_owned_pods(&db, "default", "uid-susp")
        .await
        .unwrap();
    assert!(
        owned.is_empty(),
        "Suspended job must not create pods, got {:?}",
        owned.len()
    );

    // Status must have Suspended condition
    let conditions = result["status"]["conditions"].as_array().unwrap();
    let suspended_cond = conditions.iter().find(|c| c["type"] == "Suspended");
    assert!(
        suspended_cond.is_some(),
        "Suspended job must have Suspended condition"
    );
    assert_eq!(suspended_cond.unwrap()["status"], "True");
}

#[tokio::test]
async fn test_indexed_job_assigns_completion_index_to_pods() {
    let db = crate::datastore::test_support::in_memory().await;
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "idx-job", "namespace": "default", "uid": "uid-idx"},
        "spec": {
            "completionMode": "Indexed",
            "completions": 3,
            "parallelism": 3,
            "template": {"spec": {"containers": [{"name": "worker", "image": "busybox"}]}}
        },
        "status": {}
    });
    db.create_resource("batch/v1", "Job", Some("default"), "idx-job", job)
        .await
        .unwrap();

    let job = get_job(&db, "default", "idx-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    // 3 pods should be created, one per index
    let owned = crate::controllers::find_owned_pods(&db, "default", "uid-idx")
        .await
        .unwrap();
    assert_eq!(owned.len(), 3, "Indexed job must create one pod per index");

    // Each pod must have the completion index annotation and indexed Job hostname.
    let mut indexes: Vec<i64> = owned
        .iter()
        .filter_map(|pod| {
            let index = pod
                .data
                .pointer("/metadata/annotations/batch.kubernetes.io~1job-completion-index")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<i64>().ok())?;
            let hostname = pod.data.pointer("/spec/hostname").and_then(|v| v.as_str());
            assert_eq!(
                hostname,
                Some(format!("idx-job-{index}").as_str()),
                "indexed Job pod must use <job-name>-<index> as hostname"
            );
            Some(index)
        })
        .collect();
    indexes.sort();
    assert_eq!(
        indexes,
        vec![0, 1, 2],
        "Indexes 0,1,2 must be assigned to pods"
    );
}

#[tokio::test]
async fn test_job_concurrent_reconciles_do_not_exceed_parallelism() {
    let db = crate::datastore::test_support::in_memory().await;
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "race-job", "namespace": "default", "uid": "uid-race"},
        "spec": {
            "completions": 4,
            "parallelism": 2,
            "template": {"spec": {"containers": [{"name": "worker", "image": "busybox"}]}}
        },
        "status": {}
    });
    db.create_resource("batch/v1", "Job", Some("default"), "race-job", job)
        .await
        .unwrap();

    let job = get_job(&db, "default", "race-job").await;
    let (first, second) = tokio::join!(
        reconcile_job_test(&db, &job, "test-node"),
        reconcile_job_test(&db, &job, "test-node")
    );
    first.unwrap();
    second.unwrap();

    let owned = crate::controllers::find_owned_pods(&db, "default", "uid-race")
        .await
        .unwrap();
    assert_eq!(
        owned.len(),
        2,
        "concurrent reconciles must not create more pods than parallelism allows"
    );
}

#[tokio::test]
async fn test_nonindexed_job_does_not_create_beyond_remaining_completions() {
    let db = crate::datastore::test_support::in_memory().await;
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "remaining-job", "namespace": "default", "uid": "uid-remaining"},
        "spec": {
            "completions": 4,
            "parallelism": 2,
            "template": {"spec": {"containers": [{"name": "worker", "image": "busybox"}]}}
        },
        "status": {}
    });
    db.create_resource("batch/v1", "Job", Some("default"), "remaining-job", job)
        .await
        .unwrap();

    for i in 0..4 {
        let phase = if i < 3 { "Succeeded" } else { "Running" };
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &format!("remaining-pod-{i}"),
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": format!("remaining-pod-{i}"),
                    "namespace": "default",
                    "ownerReferences": [{
                        "apiVersion": "batch/v1",
                        "kind": "Job",
                        "name": "remaining-job",
                        "uid": "uid-remaining"
                    }]
                },
                "spec": {"nodeName": "test-node"},
                "status": {"phase": phase}
            }),
        )
        .await
        .unwrap();
    }

    let job = get_job(&db, "default", "remaining-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let owned = crate::controllers::find_owned_pods(&db, "default", "uid-remaining")
        .await
        .unwrap();
    assert_eq!(
        owned.len(),
        4,
        "active pods already fill remaining completions and must not trigger an extra pod"
    );
}

#[tokio::test]
async fn test_unmanaged_job_without_ttl_survives_immediate_success_cycle() {
    use crate::kubelet::pod_repository::PodSubresourceWriter;

    let state = crate::api::test_support::build_test_app_state().await;
    state
        .db
        .create_resource(
            "batch/v1",
            "Job",
            Some("default"),
            "ttl-success-job",
            json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": "ttl-success-job",
                    "namespace": "default",
                    "uid": "uid-ttl-success-job"
                },
                "spec": {
                    "completions": 1,
                    "parallelism": 1,
                    "template": {
                        "spec": {
                            "containers": [{"name": "worker", "image": "busybox"}],
                            "restartPolicy": "Never"
                        }
                    }
                },
                "status": {}
            }),
        )
        .await
        .unwrap();

    let job = state
        .db
        .get_resource("batch/v1", "Job", Some("default"), "ttl-success-job")
        .await
        .unwrap()
        .unwrap();
    state
        .controller_dispatcher
        .reconcile(
            &crate::api::inject_resource_version(job.data, job.resource_version),
            &state.db,
            &state.config.node_name,
        )
        .await
        .unwrap();

    let pods = state
        .db
        .list_resources_by_owner_uid("v1", "Pod", Some("default"), "uid-ttl-success-job")
        .await
        .unwrap();
    assert_eq!(pods.len(), 1, "Job reconcile should create one Pod");
    let pod = &pods[0];
    assert_eq!(
        pod.data
            .pointer("/metadata/ownerReferences/0/kind")
            .and_then(|value| value.as_str()),
        Some("Job"),
        "test precondition: created Pod must be owned by the Job"
    );

    state
        .pod_repository
        .replace_status_from_api(
            "default",
            &pod.name,
            json!({
                "phase": "Succeeded",
                "conditions": [
                    {
                        "type": "PodScheduled",
                        "status": "True",
                        "lastTransitionTime": "2026-05-21T00:00:00Z"
                    },
                    {
                        "type": "Initialized",
                        "status": "True",
                        "lastTransitionTime": "2026-05-21T00:00:00Z"
                    },
                    {
                        "type": "Ready",
                        "status": "False",
                        "reason": "PodCompleted",
                        "lastTransitionTime": "2026-05-21T00:00:01Z"
                    },
                    {
                        "type": "ContainersReady",
                        "status": "False",
                        "reason": "PodCompleted",
                        "lastTransitionTime": "2026-05-21T00:00:01Z"
                    }
                ],
                "containerStatuses": [{
                    "name": "worker",
                    "ready": false,
                    "restartCount": 0,
                    "state": {
                        "terminated": {
                            "exitCode": 0,
                            "reason": "Completed",
                            "startedAt": "2026-05-21T00:00:00Z",
                            "finishedAt": "2026-05-21T00:00:01Z"
                        }
                    }
                }],
            }),
            pod.resource_version,
        )
        .await
        .unwrap();

    let updated_pod = state
        .db
        .get_resource("v1", "Pod", Some("default"), &pod.name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        updated_pod
            .data
            .pointer("/metadata/ownerReferences/0/kind")
            .and_then(|value| value.as_str()),
        Some("Job"),
        "test precondition: status update must preserve Pod ownerReferences"
    );
    let queued = state
        .controller_dispatcher
        .queued_reconcile_keys_for_test()
        .await;
    assert!(
        queued.iter().any(|key| {
            key.api_version == "batch/v1"
                && key.kind == "Job"
                && key.namespace.as_deref() == Some("default")
                && key.name == "ttl-success-job"
        }),
        "Pod status side effects should enqueue the owning Job after terminal status"
    );

    let job = state
        .db
        .get_resource("batch/v1", "Job", Some("default"), "ttl-success-job")
        .await
        .unwrap()
        .expect("Job must still exist after the Pod reaches Succeeded");
    state
        .controller_dispatcher
        .reconcile(
            &crate::api::inject_resource_version(job.data, job.resource_version),
            &state.db,
            &state.config.node_name,
        )
        .await
        .unwrap();

    let completed = state
        .db
        .get_resource("batch/v1", "Job", Some("default"), "ttl-success-job")
        .await
        .unwrap()
        .expect("unmanaged completed Job without ttlSecondsAfterFinished must not be deleted");
    assert_eq!(
        completed.data.pointer("/spec/ttlSecondsAfterFinished"),
        None,
        "test precondition: Job should not set ttlSecondsAfterFinished"
    );
    let has_complete_condition = completed
        .data
        .pointer("/status/conditions")
        .and_then(|conditions| conditions.as_array())
        .is_some_and(|conditions| {
            conditions.iter().any(|condition| {
                condition.get("type").and_then(|value| value.as_str()) == Some("Complete")
                    && condition.get("status").and_then(|value| value.as_str()) == Some("True")
            })
        });
    assert!(
        has_complete_condition,
        "successful Pod completion should mark the Job Complete"
    );
}

#[tokio::test]
async fn test_finished_job_with_expired_ttl_starts_foreground_delete_and_marks_owned_pod() {
    let db = crate::datastore::test_support::in_memory().await;
    let finish_time = (chrono::Utc::now() - chrono::Duration::seconds(10))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "ttl-expired-job",
            "namespace": "default",
            "uid": "uid-ttl-expired-job"
        },
        "spec": {
            "ttlSecondsAfterFinished": 0,
            "completions": 1,
            "parallelism": 1,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        },
        "status": {
            "conditions": [{
                "type": "Complete",
                "status": "True",
                "lastTransitionTime": finish_time
            }],
            "succeeded": 1,
            "completionTime": finish_time
        }
    });
    db.create_resource("batch/v1", "Job", Some("default"), "ttl-expired-job", job)
        .await
        .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "ttl-expired-job-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "ttl-expired-job-pod",
                "namespace": "default",
                "uid": "uid-ttl-expired-job-pod",
                "ownerReferences": [{
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "name": "ttl-expired-job",
                    "uid": "uid-ttl-expired-job",
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {
                "containers": [{"name": "worker", "image": "busybox"}],
                "restartPolicy": "Never"
            },
            "status": {"phase": "Succeeded"}
        }),
    )
    .await
    .unwrap();

    let job = get_job(&db, "default", "ttl-expired-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let deleting_job = db
        .get_resource("batch/v1", "Job", Some("default"), "ttl-expired-job")
        .await
        .unwrap()
        .expect("foreground TTL deletion keeps the Job until owned Pods are gone");
    assert!(
        deleting_job
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some(),
        "expired ttlSecondsAfterFinished must mark the finished Job for deletion"
    );
    assert!(
        deleting_job
            .data
            .pointer("/metadata/finalizers")
            .and_then(|value| value.as_array())
            .is_some_and(|finalizers| finalizers
                .iter()
                .any(|value| value.as_str() == Some("foregroundDeletion"))),
        "TTL cleanup must use foreground cascading deletion"
    );

    let pod = db
        .get_resource("v1", "Pod", Some("default"), "ttl-expired-job-pod")
        .await
        .unwrap()
        .expect("owned Pod should enter the actor-owned deletion path");
    assert!(
        pod.data.pointer("/metadata/deletionTimestamp").is_some(),
        "TTL cleanup must cascade to owned Pods through graceful Pod deletion"
    );
}

#[tokio::test]
async fn test_finished_job_with_expired_ttl_is_removed_when_no_dependents_remain() {
    let db = crate::datastore::test_support::in_memory().await;
    let finish_time = (chrono::Utc::now() - chrono::Duration::seconds(10))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    db.create_resource(
        "batch/v1",
        "Job",
        Some("default"),
        "ttl-expired-empty-job",
        json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": "ttl-expired-empty-job",
                "namespace": "default",
                "uid": "uid-ttl-expired-empty-job"
            },
            "spec": {
                "ttlSecondsAfterFinished": 0,
                "completions": 1,
                "parallelism": 1,
                "template": {
                    "spec": {
                        "containers": [{"name": "worker", "image": "busybox"}],
                        "restartPolicy": "Never"
                    }
                }
            },
            "status": {
                "conditions": [{
                    "type": "Complete",
                    "status": "True",
                    "lastTransitionTime": finish_time
                }],
                "succeeded": 1,
                "completionTime": finish_time
            }
        }),
    )
    .await
    .unwrap();

    let job = get_job(&db, "default", "ttl-expired-empty-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let job_after_ttl = db
        .get_resource("batch/v1", "Job", Some("default"), "ttl-expired-empty-job")
        .await
        .unwrap();
    assert!(
        job_after_ttl.is_none(),
        "expired ttlSecondsAfterFinished must remove a finished Job once no dependents remain"
    );
}

#[tokio::test]
async fn test_indexed_success_policy_completion_uses_success_policy_reason_and_zero_active_ready() {
    let db = crate::datastore::test_support::in_memory().await;
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "success-policy-complete", "namespace": "default", "uid": "uid-sp-complete"},
        "spec": {
            "completionMode": "Indexed",
            "completions": 2,
            "parallelism": 2,
            "successPolicy": {
                "rules": [{"succeededCount": 2}]
            },
            "template": {"spec": {"containers": [{"name": "worker", "image": "busybox"}]}}
        },
        "status": {}
    });
    db.create_resource(
        "batch/v1",
        "Job",
        Some("default"),
        "success-policy-complete",
        job,
    )
    .await
    .unwrap();

    for index in 0..2 {
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &format!("sp-complete-pod-{index}"),
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": format!("sp-complete-pod-{index}"),
                    "namespace": "default",
                    "annotations": {
                        "batch.kubernetes.io/job-completion-index": index.to_string()
                    },
                    "ownerReferences": [{
                        "apiVersion": "batch/v1",
                        "kind": "Job",
                        "name": "success-policy-complete",
                        "uid": "uid-sp-complete"
                    }]
                },
                "spec": {"nodeName": "test-node"},
                "status": {
                    "phase": "Succeeded",
                    "conditions": [{"type": "Ready", "status": "True"}]
                }
            }),
        )
        .await
        .unwrap();
    }

    let job = get_job(&db, "default", "success-policy-complete").await;
    let reconciled = reconcile_job_test(&db, &job, "test-node").await.unwrap();
    let conditions = reconciled["status"]["conditions"].as_array().unwrap();
    let complete = conditions
        .iter()
        .find(|condition| condition["type"] == "Complete")
        .expect("successPolicy-completed job must have Complete condition");
    let success = conditions
        .iter()
        .find(|condition| condition["type"] == "SuccessCriteriaMet")
        .expect("successPolicy-completed job must have SuccessCriteriaMet condition");

    assert_eq!(complete["reason"], "SuccessPolicy");
    assert_eq!(success["reason"], "SuccessPolicy");
    assert_eq!(reconciled["status"]["active"], 0);
    assert_eq!(reconciled["status"]["ready"], 0);
    assert_eq!(reconciled["status"]["terminating"], 0);
}

#[tokio::test]
async fn test_indexed_success_policy_terminates_active_pods_before_complete() {
    let db = crate::datastore::test_support::in_memory().await;
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "success-policy-early", "namespace": "default", "uid": "uid-sp-early"},
        "spec": {
            "completionMode": "Indexed",
            "completions": 5,
            "parallelism": 2,
            "successPolicy": {
                "rules": [{"succeededCount": 1}]
            },
            "template": {"spec": {"containers": [{"name": "worker", "image": "busybox"}]}}
        },
        "status": {}
    });
    db.create_resource(
        "batch/v1",
        "Job",
        Some("default"),
        "success-policy-early",
        job,
    )
    .await
    .unwrap();

    for (index, phase) in [(0, "Succeeded"), (1, "Running"), (2, "Running")] {
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &format!("sp-early-pod-{index}"),
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": format!("sp-early-pod-{index}"),
                    "namespace": "default",
                    "annotations": {
                        "batch.kubernetes.io/job-completion-index": index.to_string()
                    },
                    "ownerReferences": [{
                        "apiVersion": "batch/v1",
                        "kind": "Job",
                        "name": "success-policy-early",
                        "uid": "uid-sp-early"
                    }]
                },
                "spec": {"nodeName": "test-node"},
                "status": {"phase": phase}
            }),
        )
        .await
        .unwrap();
    }

    let job = get_job(&db, "default", "success-policy-early").await;
    let reconciled = reconcile_job_test(&db, &job, "test-node").await.unwrap();
    let conditions = reconciled["status"]["conditions"].as_array().unwrap();
    let success = conditions
        .iter()
        .find(|condition| condition["type"] == "SuccessCriteriaMet")
        .expect("successPolicy-met job must publish interim SuccessCriteriaMet");
    assert!(
        !conditions
            .iter()
            .any(|condition| condition["type"] == "Complete"),
        "Complete must wait until active pods are fully removed"
    );

    assert_eq!(success["reason"], "SuccessPolicy");
    assert_eq!(reconciled["status"]["succeeded"], 1);
    assert_eq!(reconciled["status"]["completedIndexes"], "0");
    assert_eq!(reconciled["status"]["active"], 0);
    assert_eq!(reconciled["status"]["ready"], 0);
    assert_eq!(reconciled["status"]["terminating"], 2);
    assert!(reconciled["status"].get("completionTime").is_none());

    let owned = crate::controllers::find_owned_pods(&db, "default", "uid-sp-early")
        .await
        .unwrap();
    let active_owned = owned
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_none())
        .count();
    assert_eq!(
        active_owned, 1,
        "active lingering pods should be marked terminating after successPolicy is met"
    );

    for pod in owned
        .iter()
        .filter(|pod| pod.data.pointer("/metadata/deletionTimestamp").is_some())
    {
        db.delete_resource("v1", "Pod", Some("default"), &pod.name)
            .await
            .unwrap();
    }

    let job = get_job(&db, "default", "success-policy-early").await;
    let reconciled = reconcile_job_test(&db, &job, "test-node").await.unwrap();
    let conditions = reconciled["status"]["conditions"].as_array().unwrap();
    let complete = conditions
        .iter()
        .find(|condition| condition["type"] == "Complete")
        .expect("successPolicy-met job must complete once terminating pods are gone");
    assert_eq!(complete["reason"], "SuccessPolicy");
    assert_eq!(reconciled["status"]["active"], 0);
    assert_eq!(reconciled["status"]["ready"], 0);
    assert_eq!(reconciled["status"]["terminating"], 0);
    assert_eq!(reconciled["status"]["completedIndexes"], "0");
    assert!(reconciled["status"].get("completionTime").is_some());
}

#[test]
fn test_success_policy_status_waits_for_active_pods_before_complete() {
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "success-policy-active",
            "namespace": "default",
            "uid": "uid-sp-active"
        },
        "spec": {
            "completionMode": "Indexed",
            "completions": 5,
            "parallelism": 2,
            "successPolicy": {
                "rules": [{"succeededCount": 1}]
            },
            "template": {"spec": {"containers": [{"name": "worker", "image": "busybox"}]}}
        },
        "status": {"completedIndexes": "0"}
    });
    let succeeded = indexed_job_pod_resource(
        1,
        "success-policy-active",
        "uid-sp-active",
        "success-policy-active-0",
        0,
        "Succeeded",
        None,
    );
    let pending = indexed_job_pod_resource(
        2,
        "success-policy-active",
        "uid-sp-active",
        "success-policy-active-1",
        1,
        "Pending",
        None,
    );

    let status = derive_job_status_from_owned_pods(&job, &[succeeded, pending]);
    let conditions = status["conditions"].as_array().unwrap();
    assert!(
        conditions
            .iter()
            .any(|condition| condition["type"] == "SuccessCriteriaMet"),
        "success policy should publish the interim SuccessCriteriaMet condition"
    );
    assert!(
        !conditions
            .iter()
            .any(|condition| condition["type"] == "Complete"),
        "Complete must wait until active pods are deleted"
    );
    assert_eq!(status["active"], 1);
    assert_eq!(status["ready"], 0);
    assert_eq!(status["terminating"], 0);
    assert_eq!(status["completedIndexes"], "0");
    assert!(status.get("completionTime").is_none());
}

#[test]
fn test_success_policy_status_waits_for_terminating_pods_before_complete() {
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "success-policy-terminating",
            "namespace": "default",
            "uid": "uid-sp-terminating"
        },
        "spec": {
            "completionMode": "Indexed",
            "completions": 5,
            "parallelism": 2,
            "successPolicy": {
                "rules": [{"succeededCount": 1}]
            },
            "template": {"spec": {"containers": [{"name": "worker", "image": "busybox"}]}}
        },
        "status": {"completedIndexes": "0"}
    });
    let succeeded = indexed_job_pod_resource(
        1,
        "success-policy-terminating",
        "uid-sp-terminating",
        "success-policy-terminating-0",
        0,
        "Succeeded",
        None,
    );
    let terminating = indexed_job_pod_resource(
        2,
        "success-policy-terminating",
        "uid-sp-terminating",
        "success-policy-terminating-1",
        1,
        "Pending",
        Some("2026-06-01T00:00:00Z"),
    );

    let status = derive_job_status_from_owned_pods(&job, &[succeeded, terminating]);
    let conditions = status["conditions"].as_array().unwrap();
    assert!(
        conditions
            .iter()
            .any(|condition| condition["type"] == "SuccessCriteriaMet"),
        "success policy should stay visible while pods terminate"
    );
    assert!(
        !conditions
            .iter()
            .any(|condition| condition["type"] == "Complete"),
        "Complete must wait until terminating pods are gone"
    );
    assert_eq!(status["active"], 0);
    assert_eq!(status["ready"], 0);
    assert_eq!(status["terminating"], 1);
    assert_eq!(status["completedIndexes"], "0");
    assert!(status.get("completionTime").is_none());
}

#[tokio::test]
async fn test_job_stale_controller_status_write_preserves_patched_condition() {
    let db = crate::datastore::test_support::in_memory().await;
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "status-condition-job",
            "namespace": "default",
            "uid": "uid-status-condition-job",
            "creationTimestamp": "2026-06-25T00:00:00Z"
        },
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "template": {"spec": {"containers": [{"name": "worker", "image": "busybox"}]}}
        },
        "status": {
            "active": 2,
            "ready": 0,
            "succeeded": 0,
            "failed": 0,
            "conditions": [],
            "startTime": "2026-06-25T00:00:00Z",
            "terminating": 0
        }
    });
    db.create_resource(
        "batch/v1",
        "Job",
        Some("default"),
        "status-condition-job",
        job,
    )
    .await
    .unwrap();

    let stale_job_resource = db
        .get_resource("batch/v1", "Job", Some("default"), "status-condition-job")
        .await
        .unwrap()
        .unwrap();
    let patched_status = json!({
        "active": 2,
        "ready": 0,
        "succeeded": 0,
        "failed": 0,
        "conditions": [{
            "type": "CustomConditionType",
            "status": "True",
            "lastProbeTime": null,
            "lastTransitionTime": "2026-06-25T05:50:49Z"
        }],
        "startTime": "2026-06-25T00:00:00Z",
        "terminating": 0
    });
    db.update_status_only_with_preconditions(
        "batch/v1",
        "Job",
        Some("default"),
        "status-condition-job",
        patched_status,
        crate::datastore::ResourcePreconditions::from_resource(&stale_job_resource),
    )
    .await
    .unwrap();

    let stale_controller_status = json!({
        "active": 2,
        "ready": 2,
        "succeeded": 0,
        "failed": 0,
        "conditions": [],
        "startTime": "2026-06-25T00:00:00Z",
        "terminating": 0
    });
    let stale_write = crate::controllers::common::write_status_for_resource(
        &db,
        &stale_job_resource,
        &stale_controller_status,
    )
    .await;
    assert!(
        stale_write.is_err(),
        "stale controller status write must not retry with a payload that can drop patched status conditions"
    );

    let live = db
        .get_resource("batch/v1", "Job", Some("default"), "status-condition-job")
        .await
        .unwrap()
        .unwrap();
    let conditions = live
        .data
        .pointer("/status/conditions")
        .and_then(|value| value.as_array())
        .expect("live Job status must have conditions");
    assert!(
        conditions
            .iter()
            .any(|condition| condition["type"] == "CustomConditionType"),
        "concurrent /status condition must survive stale controller status retry"
    );
}

#[tokio::test]
async fn test_job_reconcile_after_status_update_uses_current_row_resource_version() {
    let db = crate::datastore::test_support::in_memory().await;
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "status-rv-job",
            "namespace": "default",
            "uid": "uid-status-rv",
            "resourceVersion": "1"
        },
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "template": {"spec": {"containers": [{"name": "worker", "image": "busybox"}]}}
        },
        "status": {}
    });
    db.create_resource("batch/v1", "Job", Some("default"), "status-rv-job", job)
        .await
        .unwrap();

    let job = get_job(&db, "default", "status-rv-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "uid-status-rv")
        .await
        .unwrap();
    let mut pod: serde_json::Value = (*pods[0].data).clone();
    let pod_name = pod["metadata"]["name"].as_str().unwrap().to_string();
    pod["status"] = json!({"phase": "Succeeded"});
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        &pod_name,
        pod,
        pods[0].resource_version,
    )
    .await
    .unwrap();

    let job = get_job(&db, "default", "status-rv-job").await;
    let reconciled = reconcile_job_test(&db, &job, "test-node").await.unwrap();
    assert_eq!(reconciled["status"]["succeeded"], 1);
}

#[tokio::test]
async fn test_indexed_job_skips_succeeded_indexes() {
    let db = crate::datastore::test_support::in_memory().await;
    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "idx2-job", "namespace": "default", "uid": "uid-idx2"},
        "spec": {
            "completionMode": "Indexed",
            "completions": 3,
            "parallelism": 3,
            "template": {"spec": {"containers": [{"name": "worker", "image": "busybox"}]}}
        },
        "status": {}
    });
    db.create_resource("batch/v1", "Job", Some("default"), "idx2-job", job)
        .await
        .unwrap();

    // Pre-create a succeeded pod for index 1
    db.create_resource("v1", "Pod",
            Some("default"), "idx2-pod-1", json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": {
                    "name": "idx2-pod-1", "namespace": "default",
                    "annotations": {"batch.kubernetes.io/job-completion-index": "1"},
                    "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job", "name": "idx2-job", "uid": "uid-idx2"}]
                },
                "spec": {"nodeName": "test-node"},
                "status": {"phase": "Succeeded"}
            })).await.unwrap();

    let job = get_job(&db, "default", "idx2-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let owned = crate::controllers::find_owned_pods(&db, "default", "uid-idx2")
        .await
        .unwrap();
    // Should have 3 pods total: 1 pre-existing (idx 1 succeeded) + 2 new (idx 0 and 2)
    assert_eq!(
        owned.len(),
        3,
        "Must have 3 pods: 1 succeeded + 2 new, got {}",
        owned.len()
    );

    let new_indexes: Vec<i64> = owned
        .iter()
        .filter_map(|pod| {
            if pod.data.pointer("/status/phase").and_then(|p| p.as_str()) != Some("Succeeded") {
                pod.data
                    .pointer("/metadata/annotations/batch.kubernetes.io~1job-completion-index")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
            } else {
                None
            }
        })
        .collect();
    let mut new_indexes_sorted = new_indexes.clone();
    new_indexes_sorted.sort();
    assert_eq!(
        new_indexes_sorted,
        vec![0, 2],
        "New pods must be for indexes 0 and 2 only"
    );
}

#[tokio::test]
async fn test_job_respects_pod_resourcequota() {
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
    db.create_resource(
        "v1",
        "ResourceQuota",
        Some("default"),
        "pods-zero",
        json!({
            "apiVersion": "v1",
            "kind": "ResourceQuota",
            "metadata": {"name": "pods-zero", "namespace": "default"},
            "spec": {"hard": {"pods": "0"}},
            "status": {"hard": {"pods": "0"}, "used": {"pods": "0"}}
        }),
    )
    .await
    .unwrap();

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "quota-job", "namespace": "default", "uid": "uid-quota-job"},
        "spec": {
            "completionMode": "NonIndexed",
            "completions": 1,
            "parallelism": 1,
            "template": {
                "spec": {"containers": [{"name": "worker", "image": "busybox"}]}
            }
        }
    });
    db.create_resource("batch/v1", "Job", Some("default"), "quota-job", job)
        .await
        .unwrap();

    let job = get_job(&db, "default", "quota-job").await;
    let result = reconcile_job_test(&db, &job, "test-node").await;
    assert!(result.is_err(), "Job reconcile should fail on quota deny");

    let pods = crate::controllers::find_owned_pods(&db, "default", "uid-quota-job")
        .await
        .unwrap();
    assert_eq!(pods.len(), 0, "quota deny must prevent pod creation");
}

#[tokio::test]
async fn test_job_applies_limitrange_default_request_cpu_to_pod() {
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
    db.create_resource(
        "v1",
        "LimitRange",
        Some("default"),
        "cpu-defaults",
        json!({
            "apiVersion": "v1",
            "kind": "LimitRange",
            "metadata": {"name": "cpu-defaults", "namespace": "default"},
            "spec": {
                "limits": [{
                    "type": "Container",
                    "defaultRequest": {"cpu": "100m"}
                }]
            }
        }),
    )
    .await
    .unwrap();

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "limitrange-job", "namespace": "default", "uid": "uid-lr-job"},
        "spec": {
            "completionMode": "NonIndexed",
            "completions": 1,
            "parallelism": 1,
            "template": {
                "spec": {"containers": [{"name": "worker", "image": "busybox"}]}
            }
        }
    });
    db.create_resource("batch/v1", "Job", Some("default"), "limitrange-job", job)
        .await
        .unwrap();

    let job = get_job(&db, "default", "limitrange-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "uid-lr-job")
        .await
        .unwrap();
    assert_eq!(pods.len(), 1);
    assert_eq!(
        pods[0]
            .data
            .pointer("/spec/containers/0/resources/requests/cpu")
            .and_then(|v| v.as_str()),
        Some("100m")
    );
}
