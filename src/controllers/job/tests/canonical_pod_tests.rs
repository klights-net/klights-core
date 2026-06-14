use super::*;

/// Table-driven assertion that the produced Pod has the canonical shape
/// `build_child_pod` enforces (apiVersion, kind, status.phase=Pending,
/// owner reference with controller=true, blockOwnerDeletion=true) plus
/// the Job-specific completion-index annotation/label for indexed Jobs
/// and template label inheritance for both modes.
#[tokio::test]
async fn test_job_create_pod_uses_canonical_template() {
    let db = crate::datastore::test_support::in_memory().await;

    // ---- Non-indexed path ----
    let std_job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "std-job", "namespace": "default", "uid": "std-uid"},
        "spec": {
            "template": {
                "metadata": {"labels": {"app": "std", "team": "core"}},
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });
    db.create_resource("batch/v1", "Job", Some("default"), "std-job", std_job)
        .await
        .unwrap();
    let job = get_job(&db, "default", "std-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "std-uid")
        .await
        .unwrap();
    assert_eq!(pods.len(), 1, "non-indexed Job should produce 1 pod");
    let pod = &pods[0].data;
    assert_eq!(pod["apiVersion"], "v1");
    assert_eq!(pod["kind"], "Pod");
    assert_eq!(pod["status"]["phase"], "Pending");
    assert_eq!(pod["metadata"]["namespace"], "default");
    assert_eq!(pod["metadata"]["labels"]["app"], "std");
    assert_eq!(pod["metadata"]["labels"]["team"], "core");
    let owner = &pod["metadata"]["ownerReferences"][0];
    assert_eq!(owner["uid"], "std-uid");
    assert_eq!(owner["kind"], "Job");
    assert_eq!(owner["apiVersion"], "batch/v1");
    assert_eq!(owner["controller"], true);
    assert_eq!(owner["blockOwnerDeletion"], true);
    assert!(
        pod["metadata"]
            .get("annotations")
            .and_then(|a| a.get("batch.kubernetes.io/job-completion-index"))
            .is_none(),
        "non-indexed Job pod must not carry completion-index annotation"
    );
    assert!(
        pod["metadata"]["labels"]
            .get("batch.kubernetes.io/job-completion-index")
            .is_none(),
        "non-indexed Job pod must not carry completion-index label"
    );

    // ---- Indexed path ----
    let idx_job = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "idx-job", "namespace": "default", "uid": "idx-uid"},
        "spec": {
            "completions": 2,
            "parallelism": 2,
            "completionMode": "Indexed",
            "template": {
                "metadata": {"labels": {"app": "idx"}},
                "spec": {
                    "containers": [{"name": "worker", "image": "busybox"}],
                    "restartPolicy": "Never"
                }
            }
        }
    });
    db.create_resource("batch/v1", "Job", Some("default"), "idx-job", idx_job)
        .await
        .unwrap();
    let job = get_job(&db, "default", "idx-job").await;
    reconcile_job_test(&db, &job, "test-node").await.unwrap();

    let pods = crate::controllers::find_owned_pods(&db, "default", "idx-uid")
        .await
        .unwrap();
    assert_eq!(pods.len(), 2, "indexed Job should produce 2 pods");
    let mut seen_indexes = std::collections::HashSet::new();
    for pod in &pods {
        let pod = &pod.data;
        assert_eq!(pod["apiVersion"], "v1");
        assert_eq!(pod["kind"], "Pod");
        assert_eq!(pod["status"]["phase"], "Pending");
        assert_eq!(pod["metadata"]["labels"]["app"], "idx");

        let owner = &pod["metadata"]["ownerReferences"][0];
        assert_eq!(owner["uid"], "idx-uid");
        assert_eq!(owner["kind"], "Job");
        assert_eq!(owner["controller"], true);

        let idx_anno = pod["metadata"]["annotations"]["batch.kubernetes.io/job-completion-index"]
            .as_str()
            .unwrap()
            .to_string();
        let idx_label = pod["metadata"]["labels"]["batch.kubernetes.io/job-completion-index"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            idx_anno, idx_label,
            "completion-index label and annotation must match"
        );

        // JOB_COMPLETION_INDEX env var must still be injected post-build.
        let env = pod["spec"]["containers"][0]["env"]
            .as_array()
            .expect("worker container env array");
        let env_value = env
            .iter()
            .find(|e| e["name"] == "JOB_COMPLETION_INDEX")
            .expect("JOB_COMPLETION_INDEX env var")["value"]
            .as_str()
            .unwrap();
        assert_eq!(env_value, idx_anno);
        seen_indexes.insert(idx_anno);
    }
    assert_eq!(seen_indexes.len(), 2, "indexes must be unique");
}
