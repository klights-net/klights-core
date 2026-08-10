use crate::pod_creation_state::PodStartSource;
use crate::pod_runtime_state::{PodRuntimeState, StartupDecision, decide_startup_action};

#[test]
fn test_recovery_starts_pending_pod_without_runtime_state() {
    let pod = serde_json::json!({
        "metadata": {
            "creationTimestamp": (chrono::Utc::now() - chrono::Duration::seconds(30)).to_rfc3339(),
        },
        "spec": {"nodeName": "node"},
        "status": {
            "phase": "Pending",
        },
    });

    assert!(matches!(
        decide_startup_action(
            &pod,
            &PodRuntimeState::NotStarted,
            PodStartSource::Recovery,
            "node",
        ),
        StartupDecision::StartFresh
    ));
}

#[test]
fn test_recovery_skips_pod_already_realized_by_runtime() {
    let pod = serde_json::json!({
        "spec": {"nodeName": "node"},
        "status": {
            "phase": "Pending",
        },
    });

    assert!(matches!(
        decide_startup_action(
            &pod,
            &PodRuntimeState::Running,
            PodStartSource::Recovery,
            "node",
        ),
        StartupDecision::Skip
    ));
}

#[test]
fn test_watch_startup_reconciliation_skips_realized_pod_with_pod_ip() {
    let pod = serde_json::json!({
        "spec": {"nodeName": "node"},
        "status": {
            "phase": "Pending",
            "podIP": "10.43.0.5",
        },
    });
    let runtime_state = PodRuntimeState::StartingWithContainers {
        has_running_or_created: false,
    };

    assert!(matches!(
        decide_startup_action(&pod, &runtime_state, PodStartSource::WatchAdded, "node"),
        StartupDecision::Skip
    ));
    assert!(matches!(
        decide_startup_action(&pod, &runtime_state, PodStartSource::Recovery, "node"),
        StartupDecision::RollbackThenStart
    ));
}
