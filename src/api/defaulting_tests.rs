//! Unit tests for the pure helpers in `defaulting.rs`. All sync, no DB,
//! no `AppState`, no admission webhooks — each helper is exercised in
//! isolation against a hand-crafted `serde_json::Value`.

use crate::api::defaulting::*;
use serde_json::{Value, json};

// ============================================================================
// inject_create_metadata
// ============================================================================

#[test]
fn inject_create_metadata_namespaced_stamps_namespace_name_and_uid() {
    let mut body = json!({"metadata": {}});
    inject_create_metadata(Some("default"), &mut body, "my-pod");
    assert_eq!(body["metadata"]["namespace"], "default");
    assert_eq!(body["metadata"]["name"], "my-pod");
    assert!(body["metadata"]["uid"].as_str().unwrap().len() >= 32);
    assert!(body["metadata"]["creationTimestamp"].is_string());
    assert_eq!(body["metadata"]["generation"], 1);
}

#[test]
fn inject_create_metadata_cluster_omits_namespace() {
    let mut body = json!({"metadata": {}});
    inject_create_metadata(None, &mut body, "node-1");
    assert!(
        body["metadata"].get("namespace").is_none(),
        "cluster-scoped resource must not have namespace stamped"
    );
    assert_eq!(body["metadata"]["name"], "node-1");
    assert_eq!(body["metadata"]["generation"], 1);
}

#[test]
fn inject_create_metadata_no_metadata_object_is_noop() {
    let mut body = json!({"spec": {}});
    inject_create_metadata(Some("default"), &mut body, "x");
    assert!(
        body.get("metadata").is_none(),
        "must not synthesize metadata"
    );
}

#[test]
fn inject_create_metadata_existing_uid_preserved() {
    let preset = "11111111-1111-1111-1111-111111111111";
    let mut body = json!({"metadata": {"uid": preset, "generation": 5}});
    inject_create_metadata(Some("default"), &mut body, "x");
    assert_eq!(body["metadata"]["uid"], preset);
    assert_eq!(body["metadata"]["generation"], 5, "non-zero gen preserved");
}

#[test]
fn inject_create_metadata_whitespace_uid_replaced() {
    let mut body = json!({"metadata": {"uid": "   "}});
    inject_create_metadata(None, &mut body, "x");
    let new_uid = body["metadata"]["uid"].as_str().unwrap();
    assert!(!new_uid.trim().is_empty());
    assert_ne!(new_uid, "   ");
}

// ============================================================================
// apply_pod_create_defaults
// ============================================================================

#[test]
fn apply_pod_create_defaults_sets_termination_grace_and_status() {
    let mut pod = json!({
        "spec": {
            "containers": [{"name": "c", "image": "nginx"}]
        }
    });
    apply_pod_create_defaults(&mut pod);
    assert_eq!(pod["spec"]["terminationGracePeriodSeconds"], 30);
    assert_eq!(pod["status"]["phase"], "Pending");
    let conds = pod["status"]["conditions"].as_array().unwrap();
    assert_eq!(conds.len(), 4);
    assert_eq!(conds[0]["type"], "Initialized");
    assert_eq!(conds[1]["type"], "Ready");
    assert!(pod["status"]["qosClass"].is_string());
}

#[test]
fn apply_pod_create_defaults_preserves_explicit_termination_grace() {
    let mut pod = json!({
        "spec": {
            "terminationGracePeriodSeconds": 5,
            "containers": [{"name": "c", "image": "nginx"}]
        }
    });
    apply_pod_create_defaults(&mut pod);
    assert_eq!(pod["spec"]["terminationGracePeriodSeconds"], 5);
}

#[test]
fn apply_pod_create_defaults_qos_class_besteffort() {
    let mut pod = json!({
        "spec": {"containers": [{"name": "c", "image": "nginx"}]}
    });
    apply_pod_create_defaults(&mut pod);
    assert_eq!(pod["status"]["qosClass"], "BestEffort");
}

#[test]
fn apply_pod_create_defaults_qos_class_guaranteed() {
    let mut pod = json!({
        "spec": {
            "containers": [{
                "name": "c",
                "image": "nginx",
                "resources": {
                    "requests": {"cpu": "100m", "memory": "128Mi"},
                    "limits": {"cpu": "100m", "memory": "128Mi"}
                }
            }]
        }
    });
    apply_pod_create_defaults(&mut pod);
    assert_eq!(pod["status"]["qosClass"], "Guaranteed");
}

#[test]
fn apply_pod_create_defaults_sets_serviceaccountname_to_default_when_missing() {
    let mut pod = json!({
        "spec": {
            "containers": [{"name": "c", "image": "nginx"}]
        }
    });
    apply_pod_create_defaults(&mut pod);
    assert_eq!(pod["spec"]["serviceAccountName"], "default");
}

#[test]
fn apply_pod_create_defaults_preserves_explicit_serviceaccountname() {
    let mut pod = json!({
        "spec": {
            "serviceAccountName": "my-sa",
            "containers": [{"name": "c", "image": "nginx"}]
        }
    });
    apply_pod_create_defaults(&mut pod);
    assert_eq!(pod["spec"]["serviceAccountName"], "my-sa");
}

#[test]
fn apply_pod_create_defaults_sets_empty_serviceaccountname_to_default() {
    let mut pod = json!({
        "spec": {
            "serviceAccountName": "",
            "containers": [{"name": "c", "image": "nginx"}]
        }
    });
    apply_pod_create_defaults(&mut pod);
    assert_eq!(pod["spec"]["serviceAccountName"], "default");
}

// ============================================================================
// apply_pvc_create_defaults
// ============================================================================

#[test]
fn apply_pvc_create_defaults_sets_pending_when_status_missing() {
    let mut pvc = json!({"spec": {}});
    apply_pvc_create_defaults(&mut pvc);
    assert_eq!(pvc["status"]["phase"], "Pending");
}

#[test]
fn apply_pvc_create_defaults_sets_pending_when_phase_empty() {
    let mut pvc = json!({"spec": {}, "status": {"phase": ""}});
    apply_pvc_create_defaults(&mut pvc);
    assert_eq!(pvc["status"]["phase"], "Pending");
}

#[test]
fn apply_pvc_create_defaults_preserves_explicit_phase() {
    let mut pvc = json!({"spec": {}, "status": {"phase": "Bound"}});
    apply_pvc_create_defaults(&mut pvc);
    assert_eq!(pvc["status"]["phase"], "Bound");
}

// ============================================================================
// apply_pv_create_defaults
// ============================================================================

#[test]
fn apply_pv_create_defaults_bound_when_claimref_set() {
    let mut pv = json!({
        "spec": {"claimRef": {"name": "my-pvc", "namespace": "default"}}
    });
    apply_pv_create_defaults(&mut pv);
    assert_eq!(pv["status"]["phase"], "Bound");
}

#[test]
fn apply_pv_create_defaults_available_when_no_claimref() {
    let mut pv = json!({"spec": {"capacity": {"storage": "1Gi"}}});
    apply_pv_create_defaults(&mut pv);
    assert_eq!(pv["status"]["phase"], "Available");
}

#[test]
fn apply_pv_create_defaults_preserves_explicit_phase() {
    let mut pv = json!({"spec": {}, "status": {"phase": "Released"}});
    apply_pv_create_defaults(&mut pv);
    assert_eq!(pv["status"]["phase"], "Released");
}

#[test]
fn apply_pv_create_defaults_null_claimref_treated_as_unset() {
    let mut pv = json!({"spec": {"claimRef": null}});
    apply_pv_create_defaults(&mut pv);
    assert_eq!(pv["status"]["phase"], "Available");
}

// ============================================================================
// apply_workload_replicas_default
// ============================================================================

#[test]
fn apply_workload_replicas_default_sets_one_for_each_kind() {
    for kind in [
        "Deployment",
        "StatefulSet",
        "ReplicaSet",
        "ReplicationController",
    ] {
        let mut body = json!({"spec": {}});
        apply_workload_replicas_default(kind, &mut body);
        assert_eq!(body["spec"]["replicas"], 1, "kind={}", kind);
    }
}

#[test]
fn apply_workload_replicas_default_preserves_explicit_zero() {
    let mut body = json!({"spec": {"replicas": 0}});
    apply_workload_replicas_default("Deployment", &mut body);
    assert_eq!(
        body["spec"]["replicas"], 0,
        "explicit replicas:0 must be preserved"
    );
}

#[test]
fn apply_workload_replicas_default_noop_for_other_kinds() {
    let mut body = json!({"spec": {}});
    apply_workload_replicas_default("ConfigMap", &mut body);
    assert!(
        body["spec"].get("replicas").is_none(),
        "non-workload kinds must not get replicas default"
    );
}

#[test]
fn apply_workload_replicas_default_preserves_explicit_value() {
    let mut body = json!({"spec": {"replicas": 5}});
    apply_workload_replicas_default("StatefulSet", &mut body);
    assert_eq!(body["spec"]["replicas"], 5);
}

// ============================================================================
// apply_replicationcontroller_selector_default
// ============================================================================

#[test]
fn apply_replicationcontroller_selector_default_copies_from_template_labels() {
    let mut rc = json!({
        "spec": {
            "template": {
                "metadata": {"labels": {"app": "web", "tier": "frontend"}}
            }
        }
    });
    apply_replicationcontroller_selector_default(&mut rc);
    assert_eq!(rc["spec"]["selector"]["app"], "web");
    assert_eq!(rc["spec"]["selector"]["tier"], "frontend");
}

#[test]
fn apply_replicationcontroller_selector_default_preserves_explicit_selector() {
    let mut rc = json!({
        "spec": {
            "selector": {"app": "explicit"},
            "template": {"metadata": {"labels": {"app": "web"}}}
        }
    });
    apply_replicationcontroller_selector_default(&mut rc);
    assert_eq!(rc["spec"]["selector"]["app"], "explicit");
}

#[test]
fn apply_replicationcontroller_selector_default_empty_selector_replaced() {
    let mut rc = json!({
        "spec": {
            "selector": {},
            "template": {"metadata": {"labels": {"app": "web"}}}
        }
    });
    apply_replicationcontroller_selector_default(&mut rc);
    assert_eq!(rc["spec"]["selector"]["app"], "web");
}

#[test]
fn apply_replicationcontroller_selector_default_noop_when_no_template_labels() {
    let mut rc = json!({"spec": {}});
    apply_replicationcontroller_selector_default(&mut rc);
    assert!(
        rc["spec"].get("selector").is_none(),
        "no template labels → no selector materialized"
    );
}

// ============================================================================
// apply_resourcequota_create_status
// ============================================================================

#[test]
fn apply_resourcequota_create_status_mirrors_spec_hard_and_zeros_used() {
    let mut rq = json!({
        "spec": {"hard": {"cpu": "10", "memory": "20Gi"}}
    });
    apply_resourcequota_create_status(&mut rq);
    assert_eq!(rq["status"]["hard"]["cpu"], "10");
    assert_eq!(rq["status"]["hard"]["memory"], "20Gi");
    assert_eq!(rq["status"]["used"]["cpu"], "0");
    assert_eq!(rq["status"]["used"]["memory"], "0");
}

#[test]
fn apply_resourcequota_create_status_noop_when_spec_hard_missing() {
    let mut rq = json!({"spec": {}});
    apply_resourcequota_create_status(&mut rq);
    assert!(
        rq.get("status").is_none(),
        "no spec.hard → no status.hard/used materialized"
    );
}

// ============================================================================
// increment_generation_if_spec_changed
// ============================================================================

#[test]
fn increment_generation_if_spec_changed_bumps_when_spec_differs() {
    let current = json!({"spec": {"replicas": 1}, "metadata": {"generation": 3}});
    let mut body = json!({"spec": {"replicas": 2}, "metadata": {"generation": 3}});
    increment_generation_if_spec_changed("Deployment", &current, &mut body);
    assert_eq!(body["metadata"]["generation"], 4);
}

#[test]
fn increment_generation_if_spec_changed_noop_when_spec_unchanged() {
    let current = json!({"spec": {"replicas": 1}, "metadata": {"generation": 3}});
    let mut body = json!({"spec": {"replicas": 1}, "metadata": {"generation": 3}});
    increment_generation_if_spec_changed("Deployment", &current, &mut body);
    assert_eq!(body["metadata"]["generation"], 3);
}

#[test]
fn increment_generation_if_spec_changed_noop_for_non_spec_bearing_kind() {
    let current = json!({"spec": {"data": "old"}, "metadata": {"generation": 3}});
    let mut body = json!({"spec": {"data": "new"}, "metadata": {"generation": 3}});
    increment_generation_if_spec_changed("ConfigMap", &current, &mut body);
    assert_eq!(body["metadata"]["generation"], 3);
}

#[test]
fn increment_generation_if_spec_changed_defaults_to_one_when_current_gen_missing() {
    let current = json!({"spec": {"replicas": 1}, "metadata": {}});
    let mut body = json!({"spec": {"replicas": 2}, "metadata": {}});
    increment_generation_if_spec_changed("StatefulSet", &current, &mut body);
    assert_eq!(body["metadata"]["generation"], 2);
}

#[test]
fn increment_generation_if_spec_changed_each_workload_kind_recognised() {
    for kind in [
        "DaemonSet",
        "Deployment",
        "ReplicaSet",
        "StatefulSet",
        "CronJob",
        "Job",
        "ReplicationController",
    ] {
        let current = json!({"spec": {"a": 1}, "metadata": {"generation": 1}});
        let mut body = json!({"spec": {"a": 2}, "metadata": {"generation": 1}});
        increment_generation_if_spec_changed(kind, &current, &mut body);
        assert_eq!(body["metadata"]["generation"], 2, "kind={}", kind);
    }
}

#[test]
fn increment_generation_if_spec_changed_bumps_for_non_workload_spec_kinds() {
    for kind in [
        "Service",
        "HorizontalPodAutoscaler",
        "PodDisruptionBudget",
        "NetworkPolicy",
        "Ingress",
    ] {
        let current = json!({"spec": {"a": 1}, "metadata": {"generation": 1}});
        let mut body = json!({"spec": {"a": 2}, "metadata": {"generation": 1}});
        increment_generation_if_spec_changed(kind, &current, &mut body);
        assert_eq!(body["metadata"]["generation"], 2, "kind={}", kind);
    }
}

// ============================================================================
// set_deletion_timestamp
// ============================================================================

#[test]
fn set_deletion_timestamp_stamps_now_and_grace_zero() {
    let mut body = json!({"metadata": {"name": "x"}});
    set_deletion_timestamp(&mut body);
    assert!(
        body["metadata"]["deletionTimestamp"]
            .as_str()
            .is_some_and(|s| !s.is_empty())
    );
    assert_eq!(body["metadata"]["deletionGracePeriodSeconds"], 0);
}

#[test]
fn set_deletion_timestamp_overwrites_existing() {
    let mut body = json!({
        "metadata": {
            "deletionTimestamp": "old-ts",
            "deletionGracePeriodSeconds": 30
        }
    });
    set_deletion_timestamp(&mut body);
    assert_ne!(body["metadata"]["deletionTimestamp"], "old-ts");
    assert_eq!(body["metadata"]["deletionGracePeriodSeconds"], 0);
}

#[test]
fn set_deletion_timestamp_noop_without_metadata_object() {
    let mut body: Value = json!("not-an-object");
    set_deletion_timestamp(&mut body);
    assert_eq!(body, json!("not-an-object"));
}

// ============================================================================
// extract_owner_uid
// ============================================================================

#[test]
fn extract_owner_uid_returns_uid_when_present() {
    let body = json!({"metadata": {"uid": "abc-123"}});
    assert_eq!(extract_owner_uid(&body), "abc-123");
}

#[test]
fn extract_owner_uid_returns_empty_when_metadata_missing() {
    let body = json!({"spec": {}});
    assert_eq!(extract_owner_uid(&body), "");
}

#[test]
fn extract_owner_uid_returns_empty_when_uid_missing() {
    let body = json!({"metadata": {"name": "x"}});
    assert_eq!(extract_owner_uid(&body), "");
}

#[test]
fn extract_owner_uid_returns_empty_when_uid_not_string() {
    let body = json!({"metadata": {"uid": 42}});
    assert_eq!(extract_owner_uid(&body), "");
}
