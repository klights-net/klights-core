use super::*;

#[tokio::test]
async fn test_job_single_completion() {
    // Job with completions=1 (default) should create 1 pod
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "test-job",
            "namespace": "default",
            "uid": "job-1"
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

    db.create_resource("batch/v1", "Job", Some("default"), "test-job", job)
        .await
        .unwrap();

    // First reconcile - create pod
    let job = get_job(&db, "default", "test-job").await;
    let _job = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "job-1")
        .await
        .unwrap();
    assert_eq!(pods.len(), 1, "Should create 1 pod for completions=1");

    // Mark pod as succeeded
    let mut pod_data: serde_json::Value = (*pods[0].data).clone();
    pod_data["status"] = json!({"phase": "Succeeded"});
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        pods[0].data["metadata"]["name"].as_str().unwrap(),
        pod_data,
        pods[0].resource_version,
    )
    .await
    .unwrap();

    // Second reconcile - set Complete condition
    let job = get_job(&db, "default", "test-job").await;
    let job = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    // Verify Job Complete condition
    let conditions = job["status"]["conditions"].as_array().unwrap();
    let complete = conditions.iter().find(|c| c["type"] == "Complete");
    assert!(complete.is_some(), "Should have Complete condition");
    assert_eq!(complete.unwrap()["status"], "True");
}

#[tokio::test]
async fn test_job_adopts_matching_orphan_pod() {
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "adopt-release",
            "namespace": "default",
            "uid": "job-adopt-uid"
        },
        "spec": {
            "parallelism": 1,
            "completions": 4,
            "template": {
                "metadata": {
                    "labels": {"job": "adopt-release"}
                },
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });
    db.create_resource("batch/v1", "Job", Some("default"), "adopt-release", job)
        .await
        .unwrap();

    let orphan = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "adopt-release-orphan",
            "namespace": "default",
            "uid": "orphan-pod-uid",
            "labels": {"job": "adopt-release"}
        },
        "spec": {
            "containers": [{"name": "worker", "image": "busybox"}],
            "restartPolicy": "Never"
        },
        "status": {"phase": "Running"}
    });
    db.create_resource("v1", "Pod", Some("default"), "adopt-release-orphan", orphan)
        .await
        .unwrap();

    let job = get_job(&db, "default", "adopt-release").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let adopted = db
        .get_resource("v1", "Pod", Some("default"), "adopt-release-orphan")
        .await
        .unwrap()
        .unwrap()
        .data;
    let owner = adopted["metadata"]["ownerReferences"][0].clone();
    assert_eq!(owner["apiVersion"], "batch/v1");
    assert_eq!(owner["kind"], "Job");
    assert_eq!(owner["name"], "adopt-release");
    assert_eq!(owner["uid"], "job-adopt-uid");
    assert_eq!(owner["controller"], true);
}

#[tokio::test]
async fn test_job_releases_owned_pod_that_no_longer_matches_selector() {
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "adopt-release",
            "namespace": "default",
            "uid": "job-release-uid"
        },
        "spec": {
            "parallelism": 0,
            "completions": 4,
            "template": {
                "metadata": {
                    "labels": {"job": "adopt-release"}
                },
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });
    db.create_resource("batch/v1", "Job", Some("default"), "adopt-release", job)
        .await
        .unwrap();

    let owned_non_matching = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "adopt-release-owned",
            "namespace": "default",
            "uid": "owned-pod-uid",
            "labels": {},
            "ownerReferences": [{
                "apiVersion": "batch/v1",
                "kind": "Job",
                "name": "adopt-release",
                "uid": "job-release-uid",
                "controller": true
            }]
        },
        "spec": {
            "containers": [{"name": "worker", "image": "busybox"}],
            "restartPolicy": "Never"
        },
        "status": {"phase": "Running"}
    });
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "adopt-release-owned",
        owned_non_matching,
    )
    .await
    .unwrap();

    let job = get_job(&db, "default", "adopt-release").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let released = db
        .get_resource("v1", "Pod", Some("default"), "adopt-release-owned")
        .await
        .unwrap()
        .unwrap()
        .data;
    assert!(
        released
            .pointer("/metadata/ownerReferences")
            .and_then(|v| v.as_array())
            .is_none_or(|refs| refs.is_empty()),
        "Job must release pods that no longer match its selector"
    );
}

#[tokio::test]
async fn test_job_multiple_completions() {
    // Job with completions=3 should create 3 pods sequentially (parallelism=1)
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "multi-job",
            "namespace": "default",
            "uid": "job-2"
        },
        "spec": {
            "completions": 3,
            "parallelism": 1,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "multi-job", job)
        .await
        .unwrap();

    // Reconcile 1 - create first pod
    let job = get_job(&db, "default", "multi-job").await;
    let _job = reconcile_job_test(&db, &job, "test-node").await.unwrap();
    let mut pods = crate::controllers::find_owned_pods(&db, "default", "job-2")
        .await
        .unwrap();
    assert_eq!(pods.len(), 1, "Should create 1 pod (parallelism=1)");

    // Complete first pod, reconcile - create second pod
    let mut pod1: serde_json::Value = (*pods[0].data).clone();
    pod1["status"] = json!({"phase": "Succeeded"});
    let pod1_name = pod1["metadata"]["name"].as_str().unwrap().to_string();
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        &pod1_name,
        pod1,
        pods[0].resource_version,
    )
    .await
    .unwrap();

    let job = get_job(&db, "default", "multi-job").await;
    let _job = reconcile_job_test(&db, &job, "test-node").await.unwrap();
    pods = crate::controllers::find_owned_pods(&db, "default", "job-2")
        .await
        .unwrap();
    assert_eq!(pods.len(), 2, "Should create second pod");

    // Complete second pod, reconcile - create third pod
    let pod2 = pods
        .iter()
        .find(|p| p.data["status"]["phase"] != "Succeeded")
        .unwrap();
    let mut pod2_data: serde_json::Value = (*pod2.data).clone();
    pod2_data["status"] = json!({"phase": "Succeeded"});
    let pod2_data_name = pod2_data["metadata"]["name"].as_str().unwrap().to_string();
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        &pod2_data_name,
        pod2_data,
        pod2.resource_version,
    )
    .await
    .unwrap();

    let job = get_job(&db, "default", "multi-job").await;
    let _job = reconcile_job_test(&db, &job, "test-node").await.unwrap();
    pods = crate::controllers::find_owned_pods(&db, "default", "job-2")
        .await
        .unwrap();
    assert_eq!(pods.len(), 3, "Should create third pod");

    // Complete third pod, verify Job Complete
    let pod3 = pods
        .iter()
        .find(|p| p.data["status"]["phase"] != "Succeeded")
        .unwrap();
    let mut pod3_data: serde_json::Value = (*pod3.data).clone();
    pod3_data["status"] = json!({"phase": "Succeeded"});
    let pod3_data_name = pod3_data["metadata"]["name"].as_str().unwrap().to_string();
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        &pod3_data_name,
        pod3_data,
        pod3.resource_version,
    )
    .await
    .unwrap();

    let job = get_job(&db, "default", "multi-job").await;
    let job = reconcile_job_test(&db, &job, "test-node").await.unwrap();
    let succeeded = job["status"]["succeeded"].as_i64().unwrap();
    assert_eq!(succeeded, 3, "Should have 3 succeeded pods");
}

#[tokio::test]
async fn test_job_parallelism() {
    // Job with parallelism=2 should create 2 pods concurrently
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "parallel-job",
            "namespace": "default",
            "uid": "job-3"
        },
        "spec": {
            "completions": 5,
            "parallelism": 2,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "parallel-job", job)
        .await
        .unwrap();

    // First reconcile - should create 2 pods (parallelism=2)
    let job = get_job(&db, "default", "parallel-job").await;
    let job = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "job-3")
        .await
        .unwrap();
    assert_eq!(pods.len(), 2, "Should create 2 pods for parallelism=2");

    assert_eq!(job["status"]["active"].as_i64().unwrap(), 2);
}

#[tokio::test]
async fn test_job_backoff_limit() {
    // Job with backoffLimit=1 should fail after 2 failures
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "failing-job",
            "namespace": "default",
            "uid": "job-4"
        },
        "spec": {
            "backoffLimit": 1,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "failing-job", job)
        .await
        .unwrap();

    // Reconcile - create first pod
    let job = get_job(&db, "default", "failing-job").await;
    let _job = reconcile_job_test(&db, &job, "test-node").await.unwrap();
    let mut pods = crate::controllers::find_owned_pods(&db, "default", "job-4")
        .await
        .unwrap();
    assert_eq!(pods.len(), 1);

    // Fail first pod
    let mut pod1: serde_json::Value = (*pods[0].data).clone();
    pod1["status"] = json!({"phase": "Failed"});
    let pod1_name = pod1["metadata"]["name"].as_str().unwrap().to_string();
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        &pod1_name,
        pod1,
        pods[0].resource_version,
    )
    .await
    .unwrap();

    // Reconcile - should create second pod (failed=1, limit=1, so can retry once)
    let job = get_job(&db, "default", "failing-job").await;
    let _job = reconcile_job_test(&db, &job, "test-node").await.unwrap();
    pods = crate::controllers::find_owned_pods(&db, "default", "job-4")
        .await
        .unwrap();
    assert_eq!(pods.len(), 2, "Should create second pod");

    // Fail second pod
    let pod2 = pods
        .iter()
        .find(|p| p.data["status"]["phase"] != "Failed")
        .unwrap();
    let mut pod2_data: serde_json::Value = (*pod2.data).clone();
    pod2_data["status"] = json!({"phase": "Failed"});
    let pod2_data_name = pod2_data["metadata"]["name"].as_str().unwrap().to_string();
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        &pod2_data_name,
        pod2_data,
        pod2.resource_version,
    )
    .await
    .unwrap();

    // Reconcile - should NOT create third pod (failed=2 > limit=1)
    let job = get_job(&db, "default", "failing-job").await;
    let job = reconcile_job_test(&db, &job, "test-node").await.unwrap();
    pods = crate::controllers::find_owned_pods(&db, "default", "job-4")
        .await
        .unwrap();
    assert_eq!(
        pods.len(),
        2,
        "Should NOT create more pods after exceeding backoffLimit"
    );

    // Verify Failed condition
    let conditions = job["status"]["conditions"].as_array().unwrap();
    let failed = conditions.iter().find(|c| c["type"] == "Failed");
    assert!(failed.is_some(), "Should have Failed condition");
    assert_eq!(failed.unwrap()["status"], "True");
}

#[tokio::test]
async fn test_job_status_complete() {
    // Verify Complete condition has correct fields
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "complete-job",
            "namespace": "default",
            "uid": "job-5"
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

    db.create_resource("batch/v1", "Job", Some("default"), "complete-job", job)
        .await
        .unwrap();

    // Create and complete pod
    let job = get_job(&db, "default", "complete-job").await;
    let _job = reconcile_job_test(&db, &job, "test-node").await.unwrap();
    let pods = crate::controllers::find_owned_pods(&db, "default", "job-5")
        .await
        .unwrap();

    let mut pod_data: serde_json::Value = (*pods[0].data).clone();
    pod_data["status"] = json!({"phase": "Succeeded"});
    let pod_data_name = pod_data["metadata"]["name"].as_str().unwrap().to_string();
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        &pod_data_name,
        pod_data,
        pods[0].resource_version,
    )
    .await
    .unwrap();

    // Reconcile - set Complete condition
    let job = get_job(&db, "default", "complete-job").await;
    let job = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    // Verify Complete condition fields
    let conditions = job["status"]["conditions"].as_array().unwrap();
    let complete = conditions.iter().find(|c| c["type"] == "Complete").unwrap();

    assert_eq!(complete["type"], "Complete");
    assert_eq!(complete["status"], "True");
    assert_eq!(complete["reason"], "CompletionsReached");
    assert_eq!(
        complete["message"],
        "Reached expected number of succeeded pods"
    );
    assert!(
        complete.get("lastTransitionTime").is_some(),
        "Should have lastTransitionTime"
    );
}

#[tokio::test]
async fn test_job_status_failed() {
    // Verify Failed condition has correct fields
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "failed-job",
            "namespace": "default",
            "uid": "job-6"
        },
        "spec": {
            "backoffLimit": 0,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "failed-job", job)
        .await
        .unwrap();

    // Create and fail pod
    let job = get_job(&db, "default", "failed-job").await;
    let _job = reconcile_job_test(&db, &job, "test-node").await.unwrap();
    let pods = crate::controllers::find_owned_pods(&db, "default", "job-6")
        .await
        .unwrap();

    let mut pod_data: serde_json::Value = (*pods[0].data).clone();
    pod_data["status"] = json!({"phase": "Failed"});
    let pod_data_name = pod_data["metadata"]["name"].as_str().unwrap().to_string();
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        &pod_data_name,
        pod_data,
        pods[0].resource_version,
    )
    .await
    .unwrap();

    // Reconcile - set Failed condition (backoffLimit=0, so 1 failure exceeds it)
    let job = get_job(&db, "default", "failed-job").await;
    let job = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    // Verify Failed condition fields
    let conditions = job["status"]["conditions"].as_array().unwrap();
    let failed = conditions.iter().find(|c| c["type"] == "Failed").unwrap();

    assert_eq!(failed["type"], "Failed");
    assert_eq!(failed["status"], "True");
    assert!(
        failed.get("lastTransitionTime").is_some(),
        "Should have lastTransitionTime"
    );
    assert_eq!(failed["reason"], "BackoffLimitExceeded");
    assert!(
        failed["message"]
            .as_str()
            .unwrap()
            .contains("backoff limit")
    );
}

#[tokio::test]
async fn test_job_pod_without_phase_counted_as_active() {
    // Pods with no status.phase should be counted as active (Pending)
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "nophase-job", "namespace": "default", "uid": "job-nophase"},
        "spec": {
            "completions": 2,
            "parallelism": 2,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "nophase-job", job)
        .await
        .unwrap();

    // First reconcile - creates 2 pods
    let job = get_job(&db, "default", "nophase-job").await;
    let _job = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    // Pods are created with phase "Pending" — remove phase from one to test the no-phase path
    let pods = crate::controllers::find_owned_pods(&db, "default", "job-nophase")
        .await
        .unwrap();
    assert_eq!(pods.len(), 2);

    // Both pods active (phase=Pending), so active_count == parallelism
    // Second reconcile should NOT create more pods
    let job = get_job(&db, "default", "nophase-job").await;
    let job = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "job-nophase")
        .await
        .unwrap();
    assert_eq!(
        pods.len(),
        2,
        "Should not create more pods when active count matches parallelism"
    );
    assert_eq!(
        job["status"]["active"].as_i64().unwrap(),
        2,
        "Active count should be 2"
    );
}

#[tokio::test]
async fn test_reconcile_job_preserves_custom_status_conditions() {
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "custom-cond-job",
            "namespace": "default",
            "uid": "job-custom-cond"
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        },
        "status": {
            "conditions": [{
                "type": "CustomConditionType",
                "status": "True",
                "reason": "E2EPatched",
                "message": "patched via status subresource"
            }]
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "custom-cond-job", job)
        .await
        .unwrap();

    let job = get_job(&db, "default", "custom-cond-job").await;
    let reconciled = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let conditions = reconciled["status"]["conditions"].as_array().unwrap();
    let custom = conditions
        .iter()
        .find(|c| c["type"] == "CustomConditionType")
        .expect("custom status condition must survive reconcile");
    assert_eq!(custom["status"], "True");
    assert_eq!(custom["reason"], "E2EPatched");
}

#[tokio::test]
async fn test_job_does_not_exceed_parallelism() {
    // When active pods == parallelism, no new pods should be created
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "limit-job", "namespace": "default", "uid": "job-limit"},
        "spec": {
            "completions": 5,
            "parallelism": 2,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "limit-job", job)
        .await
        .unwrap();

    // First reconcile - creates 2 pods (parallelism=2)
    let job = get_job(&db, "default", "limit-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "job-limit")
        .await
        .unwrap();
    assert_eq!(pods.len(), 2, "Should create 2 pods for parallelism=2");

    // Second reconcile without completing any pods - should not create more
    let job = get_job(&db, "default", "limit-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "job-limit")
        .await
        .unwrap();
    assert_eq!(
        pods.len(),
        2,
        "Should NOT create more pods when active==parallelism"
    );
}

#[tokio::test]
async fn test_job_pod_has_owner_references() {
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "owner-job", "namespace": "default", "uid": "job-owner-uid"},
        "spec": {
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "owner-job", job)
        .await
        .unwrap();

    let job = get_job(&db, "default", "owner-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "job-owner-uid")
        .await
        .unwrap();
    assert_eq!(pods.len(), 1);

    let owner_refs = pods[0]
        .data
        .pointer("/metadata/ownerReferences")
        .and_then(|o| o.as_array())
        .unwrap();
    assert_eq!(owner_refs.len(), 1);
    assert_eq!(owner_refs[0]["uid"].as_str(), Some("job-owner-uid"));
    assert_eq!(owner_refs[0]["kind"].as_str(), Some("Job"));
    assert_eq!(owner_refs[0]["name"].as_str(), Some("owner-job"));
    assert_eq!(owner_refs[0]["apiVersion"].as_str(), Some("batch/v1"));
    assert_eq!(owner_refs[0]["controller"].as_bool(), Some(true));
    assert_eq!(owner_refs[0]["blockOwnerDeletion"].as_bool(), Some(true));
}

#[tokio::test]
async fn test_job_complete_does_not_create_more_pods() {
    // Once a Job is complete, no more pods should be created
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "done-job", "namespace": "default", "uid": "job-done"},
        "spec": {
            "completions": 1,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });

    db.create_resource("batch/v1", "Job", Some("default"), "done-job", job)
        .await
        .unwrap();

    // Create and complete the pod
    let job = get_job(&db, "default", "done-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "job-done")
        .await
        .unwrap();
    let mut pod_data: serde_json::Value = (*pods[0].data).clone();
    pod_data["status"] = json!({"phase": "Succeeded"});
    let pod_data_name = pod_data["metadata"]["name"].as_str().unwrap().to_string();
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        &pod_data_name,
        pod_data,
        pods[0].resource_version,
    )
    .await
    .unwrap();

    // Reconcile to mark Complete
    let job = get_job(&db, "default", "done-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    // Reconcile again — should not create more pods
    let job = get_job(&db, "default", "done-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "job-done")
        .await
        .unwrap();
    assert_eq!(pods.len(), 1, "Complete Job must not create more pods");
}

#[tokio::test]
async fn test_job_success_policy_succeeded_count_sets_success_criteria_met() {
    // Job with successPolicy.rules[{succeededCount: 1}] and completions=3
    // When 1 pod succeeds, SuccessCriteriaMet should be set even though completions not reached
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "success-policy-job",
            "namespace": "default",
            "uid": "job-sp-1"
        },
        "spec": {
            "completions": 3,
            "parallelism": 3,
            "successPolicy": {
                "rules": [
                    {"succeededCount": 1}
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

    db.create_resource(
        "batch/v1",
        "Job",
        Some("default"),
        "success-policy-job",
        job,
    )
    .await
    .unwrap();

    // Reconcile 1 - create 3 pods
    let job = get_job(&db, "default", "success-policy-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "job-sp-1")
        .await
        .unwrap();
    assert_eq!(pods.len(), 3, "Should create 3 pods");

    // Mark 1 pod as Succeeded (still 2 pending)
    let mut pod_data: serde_json::Value = (*pods[0].data).clone();
    pod_data["status"] = json!({"phase": "Succeeded"});
    let pod_data_name = pod_data["metadata"]["name"].as_str().unwrap().to_string();
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        &pod_data_name,
        pod_data,
        pods[0].resource_version,
    )
    .await
    .unwrap();

    // Reconcile 2 - should set SuccessCriteriaMet (succeededCount=1 >= rule.succeededCount=1)
    let job = get_job(&db, "default", "success-policy-job").await;
    let job = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let conditions = job["status"]["conditions"].as_array().unwrap();
    let scm = conditions
        .iter()
        .find(|c| c["type"] == "SuccessCriteriaMet");
    assert!(
        scm.is_some(),
        "Should have SuccessCriteriaMet condition when succeededCount rule is satisfied"
    );
    assert_eq!(scm.unwrap()["status"], "True");
}

#[tokio::test]
async fn test_job_success_policy_succeeded_indexes_sets_success_criteria_met() {
    // Job with successPolicy.rules[{succeededIndexes: "0"}] — indexed job
    // When pod index 0 succeeds, SuccessCriteriaMet should be set
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "indexed-sp-job",
            "namespace": "default",
            "uid": "job-sp-2"
        },
        "spec": {
            "completions": 3,
            "parallelism": 3,
            "completionMode": "Indexed",
            "successPolicy": {
                "rules": [
                    {"succeededIndexes": "0"}
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

    db.create_resource("batch/v1", "Job", Some("default"), "indexed-sp-job", job)
        .await
        .unwrap();

    // Reconcile 1 - create pods
    let job = get_job(&db, "default", "indexed-sp-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "job-sp-2")
        .await
        .unwrap();

    // Mark the pod with index 0 as Succeeded (annotated with batch.kubernetes.io/job-completion-index=0)
    // For indexed jobs, pods have annotation batch.kubernetes.io/job-completion-index
    // Find or manually create a pod with index 0
    // Since our job controller assigns indexes for Indexed mode, let's just set the annotation
    let mut pod_data: serde_json::Value = (*pods[0].data).clone();
    pod_data["status"] = json!({"phase": "Succeeded"});
    // Inject the completion-index annotation so the controller can detect which index succeeded
    pod_data["metadata"]["annotations"] = json!({
        "batch.kubernetes.io/job-completion-index": "0"
    });
    let pod_data_name = pod_data["metadata"]["name"].as_str().unwrap().to_string();
    db.update_resource(
        "v1",
        "Pod",
        Some("default"),
        &pod_data_name,
        pod_data,
        pods[0].resource_version,
    )
    .await
    .unwrap();

    // Reconcile 2 - should set SuccessCriteriaMet (index 0 succeeded, rule requires index 0)
    let job = get_job(&db, "default", "indexed-sp-job").await;
    let job = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let conditions = job["status"]["conditions"].as_array().unwrap();
    let scm = conditions
        .iter()
        .find(|c| c["type"] == "SuccessCriteriaMet");
    assert!(
        scm.is_some(),
        "Should have SuccessCriteriaMet condition when succeededIndexes rule is satisfied"
    );
    assert_eq!(scm.unwrap()["status"], "True");
}

#[tokio::test]
async fn test_indexed_job_preserves_completed_indexes_after_terminal_pods_are_deleted() {
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "indexed-history",
            "namespace": "default",
            "uid": "job-indexed-history"
        },
        "spec": {
            "completionMode": "Indexed",
            "completions": 4,
            "parallelism": 4,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        },
        "status": {"completedIndexes": "0-1", "succeeded": 2}
    });
    db.create_resource("batch/v1", "Job", Some("default"), "indexed-history", job)
        .await
        .unwrap();

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "indexed-history-3-live",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "indexed-history-3-live",
                "namespace": "default",
                "uid": "indexed-history-3-live-uid",
                "annotations": {"batch.kubernetes.io/job-completion-index": "3"},
                "ownerReferences": [{
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "name": "indexed-history",
                    "uid": "job-indexed-history",
                    "controller": true
                }]
            },
            "spec": {"containers": [{"name": "worker", "image": "busybox"}], "restartPolicy": "Never"},
            "status": {"phase": "Succeeded"}
        }),
    )
    .await
    .unwrap();

    let job = get_job(&db, "default", "indexed-history").await;
    let reconciled = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    assert_eq!(reconciled["status"]["completedIndexes"], "0-1,3");
    assert_eq!(reconciled["status"]["succeeded"], 3);

    let owned = crate::controllers::find_owned_pods(&db, "default", "job-indexed-history")
        .await
        .unwrap();
    let new_indexes: Vec<_> = owned
        .iter()
        .filter_map(|pod| {
            pod.data
                .pointer("/metadata/annotations/batch.kubernetes.io~1job-completion-index")
                .and_then(|v| v.as_str())
        })
        .collect();
    assert!(
        !new_indexes.contains(&"0") && !new_indexes.contains(&"1"),
        "controller must not recreate indexes already recorded in status.completedIndexes"
    );
    assert!(
        new_indexes.contains(&"2"),
        "controller should only create the missing index 2"
    );
}

#[tokio::test]
async fn test_indexed_job_does_not_complete_with_duplicate_succeeded_index() {
    // Kubernetes completes Indexed Jobs by unique completed indexes, not by the
    // raw number of succeeded Pods. A duplicate succeeded index must not let the
    // Job finish while another index is still missing.
    let db = crate::datastore::test_support::in_memory().await;

    let job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "indexed-dup",
            "namespace": "default",
            "uid": "job-indexed-dup"
        },
        "spec": {
            "completionMode": "Indexed",
            "completions": 4,
            "parallelism": 2,
            "template": {
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });
    db.create_resource("batch/v1", "Job", Some("default"), "indexed-dup", job)
        .await
        .unwrap();

    for (pod_name, index) in [
        ("indexed-dup-0-a", "0"),
        ("indexed-dup-1-a", "1"),
        ("indexed-dup-3-a", "3"),
        ("indexed-dup-3-b", "3"),
    ] {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "default",
                "uid": format!("{pod_name}-uid"),
                "annotations": {"batch.kubernetes.io/job-completion-index": index},
                "ownerReferences": [{
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "name": "indexed-dup",
                    "uid": "job-indexed-dup",
                    "controller": true
                }]
            },
            "spec": {"containers": [{"name": "worker", "image": "busybox"}], "restartPolicy": "Never"},
            "status": {"phase": "Succeeded"}
        });
        db.create_resource("v1", "Pod", Some("default"), pod_name, pod)
            .await
            .unwrap();
    }

    let job = get_job(&db, "default", "indexed-dup").await;
    let reconciled = reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let conditions = reconciled["status"]["conditions"].as_array().unwrap();
    assert!(
        conditions.iter().all(|c| c["type"] != "Complete"),
        "Indexed Job must not complete until every required index has succeeded"
    );
    assert_eq!(reconciled["status"]["completedIndexes"], "0-1,3");

    let pods = crate::controllers::find_owned_pods(&db, "default", "job-indexed-dup")
        .await
        .unwrap();
    assert!(
        pods.iter().any(|pod| {
            pod.data
                .pointer("/metadata/annotations/batch.kubernetes.io~1job-completion-index")
                .and_then(|v| v.as_str())
                == Some("2")
                && pod.data.pointer("/status/phase").and_then(|v| v.as_str()) == Some("Pending")
        }),
        "controller must create a replacement Pod for the missing index 2"
    );
}
