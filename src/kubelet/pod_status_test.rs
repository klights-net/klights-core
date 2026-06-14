//! Tests for pod status update behavior
//!
//! These tests verify pod condition management and status computation.
use crate::kubelet::pod_status_logic::ContainerInfo;
use crate::kubelet::pod_status_logic::compute_pod_phase;
use crate::kubelet::pod_status_logic::get_condition_last_transition_time;
use serde_json::json;

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that empty container states result in Pending phase
    #[test]
    fn test_compute_pod_phase_empty_containers_returns_pending() {
        let containers: Vec<(String, ContainerInfo)> = vec![];

        // All restart policies should return Pending when no container state observed
        assert_eq!(
            compute_pod_phase(&containers, "Always"),
            "Pending",
            "Empty + Always → Pending (no state yet)"
        );
        assert_eq!(
            compute_pod_phase(&containers, "OnFailure"),
            "Pending",
            "Empty + OnFailure → Pending (no state yet)"
        );
        assert_eq!(
            compute_pod_phase(&containers, "Never"),
            "Pending",
            "Empty + Never → Pending (no state yet)"
        );
    }

    /// Test that pod phase transitions from Pending to Running when container starts
    #[test]
    fn test_compute_pod_phase_transitions_to_running_on_container_start() {
        let containers = vec![(
            "container-1".to_string(),
            ContainerInfo {
                state: 1, // Running
                exit_code: 0,
                ..Default::default()
            },
        )];

        assert_eq!(
            compute_pod_phase(&containers, "Always"),
            "Running",
            "One running container → Running"
        );
    }

    /// Test that pod phase is Succeeded when all containers exit with zero
    /// Only applies to restartPolicy=Never (or OnFailure without nonzero exits)
    #[test]
    fn test_compute_pod_phase_succeeded_when_all_exited_zero_never_policy() {
        let containers = vec![
            (
                "container-1".to_string(),
                ContainerInfo {
                    state: 2, // Exited
                    exit_code: 0,
                    ..Default::default()
                },
            ),
            (
                "container-2".to_string(),
                ContainerInfo {
                    state: 2, // Exited
                    exit_code: 0,
                    ..Default::default()
                },
            ),
        ];

        assert_eq!(
            compute_pod_phase(&containers, "Never"),
            "Succeeded",
            "All containers exited with 0 + Never → Succeeded"
        );
    }

    /// Test that with restartPolicy=Always, exited containers still report Running
    #[test]
    fn test_compute_pod_phase_all_exited_zero_always_policy_returns_running() {
        let containers = vec![
            (
                "container-1".to_string(),
                ContainerInfo {
                    state: 2, // Exited
                    exit_code: 0,
                    ..Default::default()
                },
            ),
            (
                "container-2".to_string(),
                ContainerInfo {
                    state: 2, // Exited
                    exit_code: 0,
                    ..Default::default()
                },
            ),
        ];

        assert_eq!(
            compute_pod_phase(&containers, "Always"),
            "Running",
            "All containers exited with 0 + Always → Running (will restart)"
        );
    }

    /// Test that pod phase is Failed when any container exits non-zero
    /// Only applies to restartPolicy=Never
    #[test]
    fn test_compute_pod_phase_failed_when_any_exited_nonzero_never_policy() {
        let containers = vec![
            (
                "container-1".to_string(),
                ContainerInfo {
                    state: 2, // Exited
                    exit_code: 0,
                    ..Default::default()
                },
            ),
            (
                "container-2".to_string(),
                ContainerInfo {
                    state: 2, // Exited
                    exit_code: 1,
                    ..Default::default()
                },
            ),
        ];

        assert_eq!(
            compute_pod_phase(&containers, "Never"),
            "Failed",
            "One container exited with 1 + Never → Failed"
        );
    }

    /// Test pod phase with restartPolicy=OnFailure and non-zero exit
    /// OnFailure with non-zero exit stays Running (will restart)
    #[test]
    fn test_compute_pod_phase_onfailure_with_nonzero_returns_running() {
        let containers = vec![(
            "container-1".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 1,
                ..Default::default()
            },
        )];

        assert_eq!(
            compute_pod_phase(&containers, "OnFailure"),
            "Running",
            "OnFailure + non-zero exit → Running (will restart)"
        );
    }

    /// Test pod phase with restartPolicy=OnFailure and zero exit
    /// OnFailure with zero exit is Succeeded
    #[test]
    fn test_compute_pod_phase_onfailure_with_zero_exit_returns_succeeded() {
        let containers = vec![(
            "container-1".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 0,
                ..Default::default()
            },
        )];

        assert_eq!(
            compute_pod_phase(&containers, "OnFailure"),
            "Succeeded",
            "OnFailure + zero exit → Succeeded"
        );
    }

    /// Test pod phase with restartPolicy=Never and non-zero exit
    #[test]
    fn test_compute_pod_phase_never_with_nonzero_exit_returns_failed() {
        let containers = vec![(
            "container-1".to_string(),
            ContainerInfo {
                state: 2, // Exited
                exit_code: 1,
                ..Default::default()
            },
        )];

        assert_eq!(
            compute_pod_phase(&containers, "Never"),
            "Failed",
            "Never + non-zero exit → Failed"
        );
    }

    /// Test pod phase with mixed running and exited containers
    #[test]
    fn test_compute_pod_phase_mixed_running_and_exited_returns_running() {
        let containers = vec![
            (
                "container-1".to_string(),
                ContainerInfo {
                    state: 1, // Running
                    exit_code: 0,
                    ..Default::default()
                },
            ),
            (
                "container-2".to_string(),
                ContainerInfo {
                    state: 2, // Exited
                    exit_code: 0,
                    ..Default::default()
                },
            ),
        ];

        assert_eq!(
            compute_pod_phase(&containers, "Always"),
            "Running",
            "One running + one exited → Running"
        );
    }

    /// Test that lastTransitionTime is preserved when condition status unchanged
    #[test]
    fn test_get_condition_last_transition_time_preserves_when_status_unchanged() {
        let existing_conditions = vec![
            json!({
                "type": "Ready",
                "status": "True",
                "lastTransitionTime": "2026-04-09T03:20:00Z",
            }),
            json!({
                "type": "ContainersReady",
                "status": "True",
                "lastTransitionTime": "2026-04-09T03:20:00Z",
            }),
        ];

        // Same status as existing - should preserve timestamp
        let result = get_condition_last_transition_time(
            &existing_conditions,
            "Ready",
            "True",
            "2026-04-09T03:25:00Z",
        );
        assert_eq!(result, "2026-04-09T03:20:00Z");
    }

    /// Test that lastTransitionTime updates when condition status changes
    #[test]
    fn test_get_condition_last_transition_time_updates_when_status_changes() {
        let existing_conditions = vec![
            json!({
                "type": "Ready",
                "status": "False",
                "lastTransitionTime": "2026-04-09T03:20:00Z",
            }),
            json!({
                "type": "ContainersReady",
                "status": "False",
                "lastTransitionTime": "2026-04-09T03:20:00Z",
            }),
        ];

        // Different status than existing - should use new timestamp
        let result = get_condition_last_transition_time(
            &existing_conditions,
            "Ready",
            "True",
            "2026-04-09T03:25:00Z",
        );
        assert_eq!(result, "2026-04-09T03:25:00Z");
    }

    /// Test that lastTransitionTime uses new timestamp when condition not found
    #[test]
    fn test_get_condition_last_transition_time_uses_new_when_condition_missing() {
        let existing_conditions = vec![json!({
            "type": "Ready",
            "status": "True",
            "lastTransitionTime": "2026-04-09T03:20:00Z",
        })];

        // Condition type doesn't exist - should use new timestamp
        let result = get_condition_last_transition_time(
            &existing_conditions,
            "Initialized",
            "True",
            "2026-04-09T03:25:00Z",
        );
        assert_eq!(result, "2026-04-09T03:25:00Z");
    }

    /// Test that lastTransitionTime handles empty conditions array
    #[test]
    fn test_get_condition_last_transition_time_handles_empty_conditions() {
        let existing_conditions: Vec<serde_json::Value> = vec![];

        let result = get_condition_last_transition_time(
            &existing_conditions,
            "Ready",
            "True",
            "2026-04-09T03:25:00Z",
        );
        assert_eq!(result, "2026-04-09T03:25:00Z");
    }

    /// Test that condition matching is case-sensitive for type and status
    #[test]
    fn test_get_condition_last_transition_time_case_sensitive_matching() {
        let existing_conditions = vec![json!({
            "type": "Ready",
            "status": "True",
            "lastTransitionTime": "2026-04-09T03:20:00Z",
        })];

        // Different case - should not match
        let result_lower = get_condition_last_transition_time(
            &existing_conditions,
            "ready",
            "True",
            "2026-04-09T03:25:00Z",
        );
        assert_eq!(result_lower, "2026-04-09T03:25:00Z");

        let result_status_lower = get_condition_last_transition_time(
            &existing_conditions,
            "Ready",
            "true",
            "2026-04-09T03:25:00Z",
        );
        assert_eq!(result_status_lower, "2026-04-09T03:25:00Z");

        // Exact match - should preserve
        let result_exact = get_condition_last_transition_time(
            &existing_conditions,
            "Ready",
            "True",
            "2026-04-09T03:25:00Z",
        );
        assert_eq!(result_exact, "2026-04-09T03:20:00Z");
    }
}
