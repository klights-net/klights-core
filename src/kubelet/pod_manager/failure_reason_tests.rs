use crate::kubelet::pod_status_logic::classify_failure_reason;

#[test]
fn pull_image_anyhow_chain_is_err_image_pull() {
    let s = "Failed to pull image docker.io/library/redis:7.4-alpine: CRI pull_image failed for ...: 429 Too Many Requests";
    assert_eq!(classify_failure_reason(s), "ErrImagePull");
}

#[test]
fn cri_pull_image_error_is_err_image_pull() {
    let s = "CRI pull_image failed for x: status: Unknown";
    assert_eq!(classify_failure_reason(s), "ErrImagePull");
}

#[test]
fn failed_to_pull_phrasing_is_err_image_pull() {
    let s = "Some other context: failed to pull and unpack image: 403";
    assert_eq!(classify_failure_reason(s), "ErrImagePull");
}

#[test]
fn generic_runtime_error_is_create_container_error() {
    let s = "failed to start container: exec: sh: not found";
    assert_eq!(classify_failure_reason(s), "CreateContainerError");
}

#[test]
fn case_insensitive() {
    let s = "FAILED TO PULL image";
    assert_eq!(classify_failure_reason(s), "ErrImagePull");
}

#[test]
fn init_container_failure_is_pod_initializing() {
    let s = "Init container init2 failed with exit code 1";
    assert_eq!(classify_failure_reason(s), "PodInitializing");
}

#[test]
fn init_container_failure_large_exit_code() {
    let s = "Init container setup failed with exit code 137";
    assert_eq!(classify_failure_reason(s), "PodInitializing");
}

#[test]
fn build_creation_error_statuses_pod_initializing_has_no_message() {
    use serde_json::json;
    let pod = json!({
        "spec": {
            "containers": [{"name": "run1", "image": "busybox"}]
        }
    });
    let error = "Init container init2 failed with exit code 1";
    let statuses = super::build_creation_error_statuses(&pod, error);
    assert_eq!(statuses.len(), 1);
    let state = &statuses[0]["state"]["waiting"];
    assert_eq!(state["reason"], "PodInitializing");
    assert!(
        state.get("message").is_none() || state["message"].is_null(),
        "PodInitializing must not include a message field"
    );
}

#[test]
fn build_failed_init_container_statuses_shows_terminated_error() {
    use serde_json::json;
    let pod = json!({
        "spec": {
            "initContainers": [
                {"name": "init1", "image": "busybox"},
                {"name": "init2", "image": "busybox"}
            ]
        }
    });
    let statuses = super::build_failed_init_container_statuses(&pod, "init2", 1);
    assert_eq!(statuses.len(), 2);
    // init1 completed successfully
    assert_eq!(statuses[0]["name"], "init1");
    assert_eq!(statuses[0]["state"]["terminated"]["reason"], "Completed");
    // init2 failed
    assert_eq!(statuses[1]["name"], "init2");
    assert_eq!(statuses[1]["state"]["terminated"]["reason"], "Error");
    assert_eq!(statuses[1]["state"]["terminated"]["exitCode"], 1);
    assert_eq!(statuses[1]["ready"], false);
}
