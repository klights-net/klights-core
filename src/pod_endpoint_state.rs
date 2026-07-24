//! Root JSON adapter for the neutral endpoint-relevant Pod state contract.

use klights_reconcile_api::PodEndpointState;
use serde_json::Value;

pub(crate) fn pod_endpoint_state(pod: &Value) -> PodEndpointState<'_, Value> {
    let status = pod.get("status");
    let metadata = pod.get("metadata");
    let ready = status
        .and_then(|status| status.get("conditions"))
        .and_then(Value::as_array)
        .and_then(|conditions| {
            conditions
                .iter()
                .find(|condition| condition.get("type").and_then(Value::as_str) == Some("Ready"))
        })
        .and_then(|condition| condition.get("status"))
        .and_then(Value::as_str)
        == Some("True");
    let terminal = matches!(
        status
            .and_then(|status| status.get("phase"))
            .and_then(Value::as_str),
        Some("Failed" | "Succeeded")
    );

    PodEndpointState::new(
        ready,
        terminal,
        metadata.and_then(|metadata| metadata.get("labels")),
        status.and_then(|status| status.get("podIP")),
        status.and_then(|status| status.get("podIPs")),
        metadata.and_then(|metadata| metadata.get("deletionTimestamp")),
    )
}

pub(crate) fn pod_endpoint_state_changed(previous: &Value, updated: &Value) -> bool {
    pod_endpoint_state(previous).differs_from(&pod_endpoint_state(updated))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::pod_endpoint_state;

    #[test]
    fn json_adapter_preserves_exact_ready_terminal_and_comparison_semantics() {
        let base = json!({
            "metadata": {"labels": {"app": "web"}},
            "status": {
                "phase": "Running",
                "podIP": "10.42.0.2",
                "podIPs": [{"ip": "10.42.0.2"}],
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        });
        let ready = pod_endpoint_state(&base);
        assert!(ready.is_ready());
        assert!(!ready.is_terminal());
        assert!(!ready.differs_from(&pod_endpoint_state(&base)));

        for changed in [
            json!({"metadata": {"labels": {"app": "api"}}, "status": base["status"]}),
            json!({"metadata": base["metadata"], "status": {"podIP": "10.42.0.3"}}),
            json!({"metadata": {"labels": {"app": "web"}, "deletionTimestamp": "now"}, "status": base["status"]}),
            json!({"metadata": base["metadata"], "status": {"phase": "Succeeded"}}),
        ] {
            assert!(ready.differs_from(&pod_endpoint_state(&changed)));
        }

        let failed = json!({"status": {"phase": "Failed"}});
        let succeeded = json!({"status": {"phase": "Succeeded"}});
        assert!(pod_endpoint_state(&failed).is_terminal());
        assert!(!pod_endpoint_state(&failed).differs_from(&pod_endpoint_state(&succeeded)));
    }
}
