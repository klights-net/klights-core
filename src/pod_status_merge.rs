//! Transitional compatibility path and root wire-parity coverage for Pod status policy.

#[deprecated(note = "use klights_types Pod status policy directly; removed in Phase 3.4")]
pub use klights_types::pod_status_merge::{
    PodStatusOwner, PodStatusPatch, merge_owned_and_preserved_conditions,
    merge_pod_status_for_update,
};

#[cfg(test)]
mod tests {
    use super::{PodStatusOwner, merge_pod_status_for_update};
    use serde_json::{Value, json};

    fn status_through_protobuf(status: &Value) -> Value {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "p", "namespace": "default"},
            "spec": {"containers": [{"name": "app", "image": "busybox"}]},
            "status": status,
        });
        let bytes = crate::protobuf::encode_protobuf(&pod).expect("encode pod to protobuf");
        let decoded = crate::protobuf::decode_protobuf(&bytes).expect("decode pod from protobuf");
        decoded.get("status").cloned().unwrap_or_else(|| json!({}))
    }

    #[test]
    fn pod_status_merge_json_and_protobuf_paths_match() {
        let current = json!({
            "apiVersion": "v1", "kind": "Pod",
            "status": {
                "phase": "Succeeded",
                "conditions": [
                    {"type": "Ready", "status": "True", "lastTransitionTime": "2026-06-25T00:00:00Z"},
                    {"type": "DisruptionTarget", "status": "True", "reason": "PreemptionByScheduler", "lastTransitionTime": "2026-06-25T00:00:00Z"}
                ],
                "containerStatuses": [{
                    "name": "app", "image": "busybox", "containerID": "containerd://ctr-1",
                    "restartCount": 0, "ready": false, "started": false,
                    "state": {"terminated": {"exitCode": 0, "reason": "Completed"}}
                }]
            }
        });
        let incoming_status = json!({
            "phase": "Pending",
            "conditions": [
                {"type": "Ready", "status": "False", "lastTransitionTime": "2026-06-25T00:00:01Z"}
            ],
            "containerStatuses": [{
                "name": "app", "image": "busybox", "containerID": "containerd://ctr-1",
                "restartCount": 0, "ready": false, "started": false,
                "state": {"waiting": {"reason": "ContainerCreating"}}
            }]
        });

        let mut incoming_json = incoming_status.clone();
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming_json,
            PodStatusOwner::KubeletRuntime,
        );

        let mut incoming_proto = status_through_protobuf(&incoming_status);
        assert!(
            incoming_proto
                .pointer("/conditions")
                .and_then(Value::as_array)
                .is_some_and(|conditions| conditions.iter().any(|condition| {
                    condition.get("type").and_then(Value::as_str) == Some("Ready")
                        && condition.get("status").and_then(Value::as_str) == Some("False")
                })),
            "protobuf round-trip must preserve incoming Ready=False condition: {incoming_proto:?}"
        );
        merge_pod_status_for_update(
            "v1",
            "Pod",
            &current,
            &mut incoming_proto,
            PodStatusOwner::KubeletRuntime,
        );

        assert_eq!(
            incoming_proto.pointer("/containerStatuses/0/containerID"),
            Some(&json!("containerd://ctr-1")),
            "protobuf round-trip must preserve containerID: {incoming_proto:?}"
        );
        assert_eq!(
            incoming_json, incoming_proto,
            "JSON and protobuf apply paths must produce identical merge results"
        );
        assert!(
            incoming_json
                .pointer("/conditions")
                .and_then(Value::as_array)
                .expect("conditions")
                .iter()
                .any(|condition| condition.get("type").and_then(Value::as_str)
                    == Some("DisruptionTarget")),
            "DisruptionTarget preserved on both paths: {incoming_json:?}"
        );
        assert!(
            incoming_json
                .pointer("/containerStatuses/0/state/terminated")
                .is_some(),
            "terminal state preserved on both paths: {incoming_json:?}"
        );
    }
}
