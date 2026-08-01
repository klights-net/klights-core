use anyhow::{Context as _, Result};
use klights_cluster_core::{ResourcePreconditions, StorageCommand};
use klights_kubelet::node::*;
use klights_kubelet::node_registration::{NodeRegistrationAddresses, NodeRegistrationSnapshot};
use klights_kubelet::outbox::{
    Outbox, OutboxCommand, OutboxOperation, OutboxSendPlanner, OutboxSubject,
};

fn build_lease(node_name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {
            "name": node_name,
            "namespace": "kube-node-lease"
        },
        "spec": {
            "holderIdentity": node_name,
            "leaseDurationSeconds": klights_cluster_core::DEFAULT_NODE_LEASE_DURATION_SECONDS,
            "renewTime": klights_cluster_core::k8s_time::format_microtime(
                klights_supervisor::SystemWallClock::now_utc()
            )
        }
    })
}

pub(crate) async fn refresh_node_network_conditions(
    db: &dyn crate::datastore::DatastoreBackend,
    outbox: Option<&Outbox>,
    node_name: &str,
    dataplane_health: &klights_networking::dataplane_health::DataplaneHealth,
) -> Result<NodeNetworkRefreshResult> {
    let Some(existing) = db.get_resource("v1", "Node", None, node_name).await? else {
        return Ok(NodeNetworkRefreshResult::Missing);
    };
    let mut node = existing.data.as_ref().clone();
    let commit_changed = stamp_git_commit_annotation_for_integration_test(&mut node, "test-commit");
    let status_changed = project_network_conditions_for_integration_test(
        &mut node,
        &dataplane_health.snapshot(),
        chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .expect("fixed node condition test timestamp"),
    );

    if let Some(outbox) = outbox {
        if !status_changed {
            return Ok(NodeNetworkRefreshResult::Unchanged);
        }
        let status = node
            .get("status")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        OutboxSendPlanner::new(Some(outbox))
            .route(OutboxCommand {
                idempotency_key: format!("NodeStatus:{node_name}:integration-test"),
                operation: OutboxOperation::NodeStatus,
                subject: OutboxSubject {
                    key: format!("v1/Node/_/{node_name}"),
                    namespace: None,
                    name: node_name.to_string(),
                    uid: Some(existing.uid.clone()),
                },
                pod_uid: String::new(),
                command: klights_cluster_core::StorageCommand::UpdateStatus {
                    api_version: "v1".to_string(),
                    kind: "Node".to_string(),
                    namespace: None,
                    name: node_name.to_string(),
                    status,
                    expected_rv: None,
                    preconditions: klights_cluster_core::ResourcePreconditions::uid(
                        existing.uid.clone(),
                    ),
                    observed_status_stamp: None,
                },
                now_ms: 1_700_000_000_000,
            })
            .await
            .context("Failed to enqueue Node network condition refresh")?;
    } else {
        if !(commit_changed || status_changed) {
            return Ok(NodeNetworkRefreshResult::Unchanged);
        }
        db.update_resource_with_preconditions(
            "v1",
            "Node",
            None,
            node_name,
            node,
            klights_cluster_core::ResourcePreconditions::from_resource(&existing),
        )
        .await
        .context("Failed to update Node network conditions")?;
    }
    Ok(NodeNetworkRefreshResult::Updated)
}

pub(crate) async fn register_node_at_addresses(
    file_process: &klights_supervisor::FileProcessExecutor,
    db: &dyn crate::datastore::DatastoreBackend,
    node_name: &str,
    profile: &klights_kubelet::node_config::NodeRegistrationProfile,
    dataplane_health: Option<&klights_network_api::DataplaneHealthSnapshot>,
    addresses: &NodeRegistrationAddresses,
) -> Result<()> {
    let snapshot = NodeRegistrationSnapshot::capture_local(
        file_process,
        node_name,
        profile,
        addresses.clone(),
        None,
        None,
    )
    .await;
    crate::bootstrap::node_registration_adapter::register_node_snapshot(
        db,
        None,
        dataplane_health,
        &snapshot,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn register_node_with_outbox(
    file_process: &klights_supervisor::FileProcessExecutor,
    db: &dyn crate::datastore::DatastoreBackend,
    outbox: &klights_kubelet::node_outbox::Outbox,
    node_name: &str,
    profile: &klights_kubelet::node_config::NodeRegistrationProfile,
    dataplane_health: Option<&klights_network_api::DataplaneHealthSnapshot>,
    dataplane_external_ip: Option<&str>,
) -> Result<()> {
    let node_ip = klights_kubelet::node_ip::resolve_node_ip(node_name).await;
    let snapshot = NodeRegistrationSnapshot::capture_local(
        file_process,
        node_name,
        profile,
        NodeRegistrationAddresses::new(node_ip, dataplane_external_ip.map(str::to_string)),
        None,
        None,
    )
    .await;
    crate::bootstrap::node_registration_adapter::register_node_snapshot(
        db,
        Some(outbox),
        dataplane_health,
        &snapshot,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::DatastoreBackend;
    use crate::datastore::node_local::LegacyDeliveryTestStore as _;
    use crate::outbox_test_support::OutboxPayload;
    use klights_kubelet::node_heartbeat::{
        NODE_HEARTBEAT_INTERVAL,
        run_heartbeat_with_interval_for_integration_test as run_heartbeat_with_interval,
    };
    use klights_networking::dataplane_health::DataplaneHealth;
    use std::sync::{Arc as StdArc, Mutex};
    use std::time::Duration;

    fn k8s_time_now() -> String {
        klights_cluster_core::k8s_time::format_time(klights_supervisor::SystemWallClock::now_utc())
    }

    fn registration_profile(
        node_mode: &crate::bootstrap::NodeMode,
        node_role: &crate::bootstrap::NodeRole,
    ) -> klights_kubelet::node_config::NodeRegistrationProfile {
        let peer_mode = match node_mode {
            crate::bootstrap::NodeMode::Root => klights_network_api::NodePeerMode::Root,
            crate::bootstrap::NodeMode::Rootless { .. } => {
                klights_network_api::NodePeerMode::Rootless
            }
        };
        let role = match node_role {
            crate::bootstrap::NodeRole::Leader { .. } => {
                klights_kubelet::node_config::KubeletNodeRole::Leader
            }
            crate::bootstrap::NodeRole::Controlplane { as_learner, .. } => {
                klights_kubelet::node_config::KubeletNodeRole::Controlplane {
                    as_learner: *as_learner,
                }
            }
            crate::bootstrap::NodeRole::Worker { .. } => {
                klights_kubelet::node_config::KubeletNodeRole::Worker
            }
        };
        let publish_external_ip = match node_role {
            crate::bootstrap::NodeRole::Leader {
                bootstrap:
                    crate::bootstrap::node_role::LeaderBootstrap::Seed
                    | crate::bootstrap::node_role::LeaderBootstrap::Bootstrap { .. },
            } => false,
            crate::bootstrap::NodeRole::Controlplane {
                leader_endpoints, ..
            } if leader_endpoints.is_empty() => false,
            crate::bootstrap::NodeRole::Worker { .. }
            | crate::bootstrap::NodeRole::Controlplane { .. }
            | crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Join { .. },
            } => true,
        };
        klights_kubelet::node_config::NodeRegistrationProfile::new(
            peer_mode,
            role,
            publish_external_ip,
            klights_types::BuildIdentity::new("v1.34.6+klights-test", "test-commit"),
        )
    }

    async fn register_node(
        db: &dyn DatastoreBackend,
        node_name: &str,
        node_mode: &crate::bootstrap::NodeMode,
        node_role: &crate::bootstrap::NodeRole,
        dataplane_health: Option<&DataplaneHealth>,
        dataplane_external_ip: Option<&str>,
    ) -> Result<()> {
        let file_process = crate::kubelet::file_blocking::test_file_process_executor();
        let profile = registration_profile(node_mode, node_role);
        let dataplane_health = dataplane_health.map(DataplaneHealth::snapshot);
        let node_ip = klights_kubelet::node_ip::resolve_node_ip(node_name).await;
        let snapshot = NodeRegistrationSnapshot::capture_local(
            &file_process,
            node_name,
            &profile,
            NodeRegistrationAddresses::new(node_ip, dataplane_external_ip.map(str::to_string)),
            None,
            None,
        )
        .await;
        crate::bootstrap::node_registration_adapter::register_node_snapshot(
            db,
            None,
            dataplane_health.as_ref(),
            &snapshot,
        )
        .await
    }

    async fn register_node_at_addresses(
        db: &dyn DatastoreBackend,
        node_name: &str,
        node_mode: &crate::bootstrap::NodeMode,
        node_role: &crate::bootstrap::NodeRole,
        dataplane_health: Option<&DataplaneHealth>,
        addresses: &NodeRegistrationAddresses,
    ) -> Result<()> {
        let file_process = crate::kubelet::file_blocking::test_file_process_executor();
        let profile = registration_profile(node_mode, node_role);
        let dataplane_health = dataplane_health.map(DataplaneHealth::snapshot);
        let snapshot = NodeRegistrationSnapshot::capture_local(
            &file_process,
            node_name,
            &profile,
            addresses.clone(),
            None,
            None,
        )
        .await;
        crate::bootstrap::node_registration_adapter::register_node_snapshot(
            db,
            None,
            dataplane_health.as_ref(),
            &snapshot,
        )
        .await
    }

    fn node_condition_status<'a>(node: &'a serde_json::Value, cond_type: &str) -> Option<&'a str> {
        node.pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .and_then(|conds| {
                conds.iter().find_map(|c| {
                    if c.get("type").and_then(|t| t.as_str()) == Some(cond_type) {
                        c.get("status").and_then(|s| s.as_str())
                    } else {
                        None
                    }
                })
            })
    }

    fn node_with_ready_condition(
        status: &str,
        reason: &str,
        last_transition: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a", "resourceVersion": "10"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": status, "reason": reason, "message": "m", "lastTransitionTime": last_transition}
                ]
            }
        })
    }

    // Issue #3: a forwarded worker Node update (apply_against_latest, no RV
    // precondition) must not let a stale worker status.conditions revert the
    // leader's fresher authoritative condition.
    #[test]
    fn merge_node_fields_keeps_leader_unknown_against_stale_worker_ready() {
        // Leader marked Ready=Unknown at 11:00 (lease expiry). The worker's
        // queued snapshot still carries Ready=True from 10:00 (before the blip;
        // the status never transitioned so lastTransitionTime is stale).
        let mut desired = node_with_ready_condition("True", "KubeletReady", "2026-06-18T10:00:00Z");
        let existing =
            node_with_ready_condition("Unknown", "NodeStatusUnknown", "2026-06-18T11:00:00Z");
        merge_existing_node_mutable_fields(&mut desired, &existing);
        assert_eq!(
            node_condition_status(&desired, "Ready"),
            Some("Unknown"),
            "a stale worker Ready=True must not revert the leader's fresher Ready=Unknown"
        );
    }

    #[test]
    fn merge_node_fields_lets_worker_recovery_transition_win() {
        // Worker genuinely recovered: Ready transitioned Unknown->True at 12:00,
        // stamping a lastTransitionTime newer than the leader's 11:00 Unknown.
        let mut desired = node_with_ready_condition("True", "KubeletReady", "2026-06-18T12:00:00Z");
        let existing =
            node_with_ready_condition("Unknown", "NodeStatusUnknown", "2026-06-18T11:00:00Z");
        merge_existing_node_mutable_fields(&mut desired, &existing);
        assert_eq!(
            node_condition_status(&desired, "Ready"),
            Some("True"),
            "a genuine recovery transition (newer lastTransitionTime) must win"
        );
    }

    #[test]
    fn merge_node_fields_accepts_coherent_network_recovery_when_ready_timestamp_ties() {
        let mut desired = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a", "resourceVersion": "10"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "klights is ready", "lastTransitionTime": "2026-06-19T07:44:56Z"},
                    {"type": "NetworkUnavailable", "status": "False", "reason": "RouteCreated", "message": "RouteController created a route", "lastTransitionTime": "2026-06-19T07:44:57Z"}
                ]
            }
        });
        let existing = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a", "resourceVersion": "11"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "False", "reason": "NetworkUnavailable", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T07:44:56Z"},
                    {"type": "NetworkUnavailable", "status": "True", "reason": "DataplaneNotReady", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T07:44:56Z"}
                ]
            }
        });

        merge_existing_node_mutable_fields(&mut desired, &existing);

        assert_eq!(
            node_condition_status(&desired, "Ready"),
            Some("True"),
            "a coherent network recovery must not leave Ready=False when NetworkUnavailable=False is newer"
        );
        assert_eq!(
            node_condition_status(&desired, "NetworkUnavailable"),
            Some("False")
        );
    }

    #[test]
    fn merge_node_fields_accepts_network_recovery_when_pair_timestamps_tie() {
        let mut desired = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a", "resourceVersion": "10"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "klights is ready", "lastTransitionTime": "2026-06-19T08:16:36Z"},
                    {"type": "NetworkUnavailable", "status": "False", "reason": "RouteCreated", "message": "RouteController created a route", "lastTransitionTime": "2026-06-19T08:16:36Z"}
                ]
            }
        });
        let existing = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a", "resourceVersion": "11"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "False", "reason": "NetworkUnavailable", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T08:16:36Z"},
                    {"type": "NetworkUnavailable", "status": "True", "reason": "DataplaneNotReady", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T08:16:36Z"}
                ]
            }
        });

        merge_existing_node_mutable_fields(&mut desired, &existing);

        assert_eq!(
            node_condition_status(&desired, "Ready"),
            Some("True"),
            "a same-second network recovery pair must not be discarded as a stale tie"
        );
        assert_eq!(
            node_condition_status(&desired, "NetworkUnavailable"),
            Some("False")
        );
    }

    #[test]
    fn merge_node_fields_rejects_stale_unavailable_when_ready_is_not_newer() {
        let mut desired = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a", "resourceVersion": "10"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "False", "reason": "NetworkUnavailable", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T10:03:12Z"},
                    {"type": "NetworkUnavailable", "status": "True", "reason": "DataplaneNotReady", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T10:03:12Z"}
                ]
            }
        });
        let existing = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a", "resourceVersion": "11"},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "klights is ready", "lastTransitionTime": "2026-06-19T10:03:13Z"},
                    {"type": "NetworkUnavailable", "status": "False", "reason": "RouteCreated", "message": "RouteController created a route", "lastTransitionTime": "2026-06-19T08:44:08Z"}
                ]
            }
        });

        merge_existing_node_mutable_fields(&mut desired, &existing);

        assert_eq!(
            node_condition_status(&desired, "Ready"),
            Some("True"),
            "a stale pending-connectivity snapshot must not override a fresher Ready=True condition"
        );
        assert_eq!(
            node_condition_status(&desired, "NetworkUnavailable"),
            Some("False")
        );
    }

    #[test]
    fn merge_node_fields_preserves_leader_condition_absent_from_worker() {
        // Worker snapshot lacks a condition the leader authored; the merge must
        // preserve it rather than let the forwarded update drop it.
        let mut desired = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {"conditions": [
                {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "m", "lastTransitionTime": "2026-06-18T10:00:00Z"}
            ]}
        });
        let existing = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": "worker-a"},
            "status": {"conditions": [
                {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "m", "lastTransitionTime": "2026-06-18T10:00:00Z"},
                {"type": "MemoryPressure", "status": "False", "reason": "KubeletHasSufficientMemory", "message": "m", "lastTransitionTime": "2026-06-18T09:00:00Z"}
            ]}
        });
        merge_existing_node_mutable_fields(&mut desired, &existing);
        assert_eq!(
            node_condition_status(&desired, "MemoryPressure"),
            Some("False"),
            "a leader-owned condition absent from the worker snapshot must be preserved"
        );
    }

    async fn create_ready_node(db: &dyn DatastoreBackend, name: &str) {
        db.create_resource(
            "v1",
            "Node",
            None,
            name,
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": name,
                    "annotations": {
                        klights_controllers::annotations::GIT_COMMIT_ANNOTATION: "test-commit"
                    }
                },
                "status": {
                    "conditions": [
                        {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "klights is ready", "lastTransitionTime": k8s_time_now()},
                        {"type": "NetworkUnavailable", "status": "False", "reason": "RouteCreated", "message": "RouteController created a route", "lastTransitionTime": k8s_time_now()}
                    ]
                }
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn refresh_marks_node_not_ready_when_peers_disconnected() {
        let db = crate::datastore::test_support::in_memory().await;
        create_ready_node(&db, "node-a").await;

        let health = DataplaneHealth::new_healthy();
        health.set_peers_disconnected("1 of 1 ready peer unreachable".to_string());

        let wrote = refresh_node_network_conditions(&db, None, "node-a", &health)
            .await
            .expect("refresh must succeed");
        assert_eq!(
            wrote,
            NodeNetworkRefreshResult::Updated,
            "a Ready->NotReady transition must be written"
        );

        let node = db
            .get_resource("v1", "Node", None, "node-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node_condition_status(&node.data, "Ready"), Some("False"));
        assert_eq!(
            node_condition_status(&node.data, "NetworkUnavailable"),
            Some("True")
        );
    }

    #[tokio::test]
    async fn refresh_recovers_node_ready_when_peers_reconnect() {
        let db = crate::datastore::test_support::in_memory().await;
        create_ready_node(&db, "node-a").await;

        let health = DataplaneHealth::new_healthy();
        health.set_peers_disconnected("unreachable".to_string());
        refresh_node_network_conditions(&db, None, "node-a", &health)
            .await
            .unwrap();

        // Peer becomes reachable again.
        health.set_peers_connected();
        let wrote = refresh_node_network_conditions(&db, None, "node-a", &health)
            .await
            .unwrap();
        assert_eq!(
            wrote,
            NodeNetworkRefreshResult::Updated,
            "a NotReady->Ready recovery must be written"
        );

        let node = db
            .get_resource("v1", "Node", None, "node-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node_condition_status(&node.data, "Ready"), Some("True"));
        assert_eq!(
            node_condition_status(&node.data, "NetworkUnavailable"),
            Some("False")
        );
    }

    #[tokio::test]
    async fn refresh_network_conditions_stamps_current_git_commit() {
        use klights_controllers::annotations::GIT_COMMIT_ANNOTATION;

        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "node-a",
                    "annotations": {
                        GIT_COMMIT_ANNOTATION: "oldcommit"
                    }
                },
                "status": {
                    "conditions": [
                        {"type": "Ready", "status": "False", "reason": "NetworkUnavailable", "message": "waiting", "lastTransitionTime": "2026-06-19T07:44:56Z"},
                        {"type": "NetworkUnavailable", "status": "True", "reason": "DataplaneNotReady", "message": "waiting", "lastTransitionTime": "2026-06-19T07:44:56Z"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        let health = DataplaneHealth::new_healthy();
        health.set_peers_connected();
        refresh_node_network_conditions(&db, None, "node-a", &health)
            .await
            .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "node-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            node.data
                .pointer("/metadata/annotations/klights.io~1git-commit")
                .and_then(|value| value.as_str()),
            Some("test-commit"),
            "network status refresh must not forward a stale build commit from the local Node cache"
        );
    }

    #[tokio::test]
    async fn node_effect_git_commit_refresh_uses_uid_rv_resource_command() {
        use klights_controllers::annotations::GIT_COMMIT_ANNOTATION;

        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "node-a",
                    "annotations": {
                        GIT_COMMIT_ANNOTATION: "oldcommit"
                    }
                },
                "status": {
                    "conditions": []
                }
            }),
        )
        .await
        .unwrap();
        let client = crate::control_plane::client::local::LocalApiClient::new(
            std::sync::Arc::new(db.clone()),
            "node-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        );

        refresh_current_git_commit_annotation_via_leader(&client, &client, "node-a", "abcdef12")
            .await
            .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "node-a")
            .await
            .unwrap()
            .expect("Node must still exist");
        assert_eq!(
            node.data
                .pointer("/metadata/annotations/klights.io~1git-commit")
                .and_then(|value| value.as_str()),
            Some("abcdef12"),
            "leader-applied self patch must publish the caller's current build commit"
        );
    }

    #[tokio::test]
    async fn register_node_without_dataplane_health_preserves_network_unavailable() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-a"},
                "status": {
                    "conditions": [
                        {"type": "Ready", "status": "False", "reason": "NetworkUnavailable", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T08:16:36Z"},
                        {"type": "NetworkUnavailable", "status": "True", "reason": "DataplaneNotReady", "message": "Waiting for peer dataplane connectivity", "lastTransitionTime": "2026-06-19T08:16:36Z"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        register_node(
            &db,
            "node-a",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "node-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            node_condition_status(&node.data, "Ready"),
            Some("False"),
            "a registration refresh without dataplane health must not declare peer connectivity ready"
        );
        assert_eq!(
            node_condition_status(&node.data, "NetworkUnavailable"),
            Some("True")
        );
    }

    #[tokio::test]
    async fn register_node_refresh_preserves_existing_network_conditions() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-a"},
                "status": {
                    "conditions": [
                        {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "klights is ready", "lastTransitionTime": "2026-06-19T10:03:13Z"},
                        {"type": "NetworkUnavailable", "status": "False", "reason": "RouteCreated", "message": "RouteController created a route", "lastTransitionTime": "2026-06-19T08:44:08Z"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        let health = DataplaneHealth::new_healthy();
        health.set_peers_pending();
        register_node(
            &db,
            "node-a",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            Some(&health),
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "node-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            node_condition_status(&node.data, "Ready"),
            Some("True"),
            "existing Node registration refresh must not own dataplane readiness"
        );
        assert_eq!(
            node_condition_status(&node.data, "NetworkUnavailable"),
            Some("False")
        );
    }

    #[tokio::test]
    async fn refresh_is_noop_when_conditions_unchanged() {
        let db = crate::datastore::test_support::in_memory().await;
        create_ready_node(&db, "node-a").await;

        // Health already Healthy => same conditions already present => no write.
        let health = DataplaneHealth::new_healthy();
        let wrote = refresh_node_network_conditions(&db, None, "node-a", &health)
            .await
            .expect("refresh must succeed");
        assert!(
            matches!(wrote, NodeNetworkRefreshResult::Unchanged),
            "unchanged conditions must not write (keep the node idle-silent)"
        );
    }

    async fn wait_for_lease_resource_version(
        db: &dyn DatastoreBackend,
        node_name: &str,
        min_rv: i64,
        timeout: Duration,
    ) -> Option<i64> {
        let start = std::time::Instant::now();
        loop {
            let lease = db
                .get_resource(
                    "coordination.k8s.io/v1",
                    "Lease",
                    Some("kube-node-lease"),
                    node_name,
                )
                .await
                .ok()
                .flatten();

            if let Some(resource) = lease
                && resource.resource_version >= min_rv
            {
                return Some(resource.resource_version);
            }

            if start.elapsed() >= timeout {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn test_build_lease_renew_time_uses_canonical_microtime_format() {
        // Lease renewTime is metav1.MicroTime — must serialize as
        // YYYY-MM-DDTHH:MM:SS.ffffffZ (exactly 6 microsecond digits + Z).
        // P0-E2E-20260423-12 regression: prior bug emitted second-precision Z
        // or (via the protobuf round-trip) `+00:00` offsets.
        let lease = build_lease("dp");
        let renew = lease
            .pointer("/spec/renewTime")
            .and_then(|v| v.as_str())
            .expect("renewTime present");
        assert!(
            renew.ends_with("Z"),
            "renewTime must end with Z, got: {renew}"
        );
        assert!(
            !renew.contains("+"),
            "renewTime must not contain offset, got: {renew}"
        );
        assert_eq!(
            renew.len(),
            27,
            "MicroTime is exactly 27 chars, got: {renew}"
        );
        // ".ffffffZ" — period at -8 from the end means there are 6 fractional digits.
        assert_eq!(&renew[19..20], ".", "period at index 19, got: {renew}");
    }

    #[test]
    fn build_lease_uses_canonical_lease_duration() {
        let lease = build_lease("dp");
        assert_eq!(
            lease
                .pointer("/spec/leaseDurationSeconds")
                .and_then(|value| value.as_i64()),
            Some(klights_cluster_core::DEFAULT_NODE_LEASE_DURATION_SECONDS),
            "advertised leaseDurationSeconds must derive from the canonical node-lease constant"
        );
    }

    #[test]
    fn heartbeat_default_interval_derives_from_canonical_cadence() {
        // No literal pin: the renewal timer must equal the single canonical
        // node-lease cadence so the timer and the staleness grace cannot
        // drift apart. (Value itself is owned by node_lease_tracker.)
        assert_eq!(
            NODE_HEARTBEAT_INTERVAL,
            Duration::from_secs(
                klights_controllers::node_lease::DEFAULT_NODE_HEARTBEAT_INTERVAL_SECONDS as u64
            ),
        );
    }

    #[tokio::test]
    async fn test_register_node_creates_node_resource() {
        let db = crate::datastore::test_support::in_memory().await;

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap();
        assert!(
            node.is_some(),
            "Node resource should exist after register_node"
        );

        let data = node.unwrap().data;
        assert_eq!(data["apiVersion"], "v1");
        assert_eq!(data["kind"], "Node");
        assert_eq!(data["metadata"]["name"], "test-node");
        assert_eq!(data["metadata"]["labels"]["kubernetes.io/os"], "linux");
        assert_eq!(
            data["metadata"]["labels"]["kubernetes.io/hostname"],
            "test-node"
        );
        let version = data["status"]["nodeInfo"]["kubeletVersion"]
            .as_str()
            .unwrap();
        assert_eq!(
            version, "v1.34.6+klights-test",
            "root-mode kubeletVersion should use the injected registration profile"
        );
        assert_eq!(data["status"]["nodeInfo"]["operatingSystem"], "linux");
        assert!(
            data["status"]["nodeInfo"]["osImage"]
                .as_str()
                .is_some_and(|value| !value.trim().is_empty()),
            "Node status.nodeInfo.osImage must be populated for kubectl wide output"
        );
        assert!(
            data["status"]["nodeInfo"]["kernelVersion"]
                .as_str()
                .is_some_and(|value| !value.trim().is_empty()),
            "Node status.nodeInfo.kernelVersion must be populated for kubectl wide output"
        );
        assert!(
            data["status"]["nodeInfo"]["containerRuntimeVersion"]
                .as_str()
                .is_some_and(|value| !value.trim().is_empty()),
            "Node status.nodeInfo.containerRuntimeVersion must be populated for kubectl wide output"
        );
    }

    #[tokio::test]
    async fn typed_registration_snapshot_preserves_remote_host_facts_exactly() {
        let db = crate::datastore::test_support::in_memory().await;
        let snapshot = NodeRegistrationSnapshot {
            node_name: "remote-cp".to_string(),
            node_mode: klights_controllers::annotations::NodePeerMode::Root,
            node_role: klights_kubelet::node_config::KubeletNodeRole::Controlplane {
                as_learner: false,
            },
            publish_external_ip: true,
            addresses: NodeRegistrationAddresses::new(
                "172.31.50.2".to_string(),
                Some("192.0.2.50".to_string()),
            ),
            role_projection: Some(klights_leader_api::NodeRoleProjection::ControlPlaneFollower),
            grpc_port: Some(7679),
            host: NodeRegistrationHostFacts {
                cpu_count: 37,
                memory_ki: 98_765_432,
                architecture: "remote-arch".to_string(),
                operating_system: "linux".to_string(),
                os_image: "Remote Linux 42".to_string(),
                kernel_version: "9.8.7-remote".to_string(),
                container_runtime_version: "containerd://9.9.9".to_string(),
                kubelet_version: "v1.34.0-klights.remote".to_string(),
                git_commit: "joinerabc".to_string(),
            },
        };

        crate::bootstrap::node_registration_adapter::register_node_snapshot(
            &db, None, None, &snapshot,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "remote-cp")
            .await
            .unwrap()
            .expect("remote Node must be registered");
        assert_eq!(node.data["status"]["capacity"]["cpu"], "37");
        assert_eq!(node.data["status"]["capacity"]["memory"], "98765432Ki");
        assert_eq!(node.data["status"]["allocatable"]["cpu"], "37");
        assert_eq!(node.data["status"]["allocatable"]["memory"], "98765432Ki");
        assert_eq!(node.data["metadata"]["labels"]["kubernetes.io/os"], "linux");
        assert_eq!(
            node.data["metadata"]["labels"]["kubernetes.io/arch"],
            "remote-arch"
        );
        assert_eq!(
            node.data["status"]["nodeInfo"]["architecture"],
            "remote-arch"
        );
        assert_eq!(node.data["status"]["nodeInfo"]["operatingSystem"], "linux");
        assert_eq!(
            node.data["status"]["nodeInfo"]["osImage"],
            "Remote Linux 42"
        );
        assert_eq!(
            node.data["status"]["nodeInfo"]["kernelVersion"],
            "9.8.7-remote"
        );
        assert_eq!(
            node.data["status"]["nodeInfo"]["containerRuntimeVersion"],
            "containerd://9.9.9"
        );
        assert_eq!(
            node.data["status"]["nodeInfo"]["kubeletVersion"],
            "v1.34.0-klights.remote"
        );
        assert_eq!(
            node.data["metadata"]["annotations"]["klights.io/git-commit"],
            "joinerabc"
        );
    }

    #[tokio::test]
    async fn test_seed_leader_register_node_omits_self_dataplane_endpoint_external_ip() {
        let db = crate::datastore::test_support::in_memory().await;

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            Some("203.0.113.10"),
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .expect("Node resource should exist after register_node");
        let addresses = node.data["status"]["addresses"].as_array().unwrap();
        assert!(
            !addresses.iter().any(|address| {
                address["type"] == "ExternalIP" && address["address"] == "203.0.113.10"
            }),
            "seed leader must not publish a self-authored dataplane endpoint as ExternalIP: {addresses:?}"
        );
    }

    #[tokio::test]
    async fn register_node_at_addresses_separates_internal_and_external_ip_for_worker() {
        let db = crate::datastore::test_support::in_memory().await;
        let addresses = NodeRegistrationAddresses::new(
            "172.31.11.2".to_string(),
            Some("10.99.0.11".to_string()),
        );

        register_node_at_addresses(
            &db,
            "mn-worker",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Worker {
                leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
                token: Some("token".to_string()),
                skip_ca: false,
            },
            None,
            &addresses,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "mn-worker")
            .await
            .unwrap()
            .expect("Node resource should exist after register_node");
        let node_addresses = node.data["status"]["addresses"].as_array().unwrap();
        assert!(node_addresses.iter().any(|address| {
            address["type"] == "InternalIP" && address["address"] == "172.31.11.2"
        }));
        assert!(node_addresses.iter().any(|address| {
            address["type"] == "ExternalIP" && address["address"] == "10.99.0.11"
        }));
        assert!(
            !node_addresses.iter().any(|address| {
                address["type"] == "InternalIP" && address["address"] == "10.99.0.11"
            }),
            "external endpoint must not overwrite Kubernetes InternalIP: {node_addresses:?}"
        );
    }

    #[tokio::test]
    async fn register_node_at_addresses_preserves_existing_external_ip_when_refresh_has_none() {
        let db = crate::datastore::test_support::in_memory().await;
        let role = crate::bootstrap::NodeRole::Worker {
            leader_endpoints: vec!["https://10.99.0.10:7679".to_string()],
            token: Some("token".to_string()),
            skip_ca: false,
        };
        let observed_addresses = NodeRegistrationAddresses::new(
            "172.31.11.2".to_string(),
            Some("10.99.0.11".to_string()),
        );
        register_node_at_addresses(
            &db,
            "mn-worker",
            &crate::bootstrap::NodeMode::Root,
            &role,
            None,
            &observed_addresses,
        )
        .await
        .unwrap();

        let refresh_addresses = NodeRegistrationAddresses::new("172.31.11.2".to_string(), None);
        register_node_at_addresses(
            &db,
            "mn-worker",
            &crate::bootstrap::NodeMode::Root,
            &role,
            None,
            &refresh_addresses,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "mn-worker")
            .await
            .unwrap()
            .expect("Node resource should exist after refresh");
        let node_addresses = node.data["status"]["addresses"].as_array().unwrap();
        assert!(
            node_addresses.iter().any(|address| {
                address["type"] == "ExternalIP" && address["address"] == "10.99.0.11"
            }),
            "registration refresh must preserve peer-observed ExternalIP: {node_addresses:?}"
        );
    }

    #[tokio::test]
    async fn test_register_node_publishes_allocated_pod_cidr() {
        let db = crate::datastore::test_support::in_memory().await;
        db.allocate_node_subnet("test-node", "10.50.0.0/16", "192.0.2.10")
            .await
            .unwrap();

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            Some("203.0.113.10"),
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .expect("Node resource should exist after register_node");
        assert_eq!(
            node.data.pointer("/spec/podCIDR").and_then(|v| v.as_str()),
            Some("10.50.0.0/24")
        );
        assert_eq!(
            node.data
                .pointer("/spec/podCIDRs/0")
                .and_then(|v| v.as_str()),
            Some("10.50.0.0/24")
        );
    }

    #[tokio::test]
    async fn test_register_node_refreshes_existing_node_internal_ip() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "test-node",
                    "labels": {"example.com/preserve": "true"},
                    "annotations": {"example.com/preserve": "true"}
                },
                "spec": {"unschedulable": true},
                "status": {
                    "addresses": [
                        {"type": "Hostname", "address": "test-node"},
                        {"type": "InternalIP", "address": "127.0.0.1"}
                    ]
                }
            }),
        )
        .await
        .unwrap();

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();
        let internal_ip = node.data["status"]["addresses"]
            .as_array()
            .unwrap()
            .iter()
            .find(|addr| addr["type"] == "InternalIP")
            .and_then(|addr| addr["address"].as_str())
            .unwrap();

        assert_ne!(
            internal_ip, "127.0.0.1",
            "restart registration must refresh stale loopback InternalIP"
        );
        assert_eq!(
            node.data["metadata"]["labels"]["example.com/preserve"], "true",
            "register_node must not erase user-managed labels on restart"
        );
        assert_eq!(
            node.data["metadata"]["annotations"]["example.com/preserve"], "true",
            "register_node must not erase user-managed annotations on restart"
        );
        assert_eq!(
            node.data["spec"]["unschedulable"], true,
            "register_node must not uncordon an existing node"
        );
    }

    #[tokio::test]
    async fn test_register_node_refreshes_creation_timestamp_on_restart() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "test-node",
                    "creationTimestamp": "2026-01-01T00:00:00Z"
                },
                "spec": {},
                "status": {}
            }),
        )
        .await
        .unwrap();

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();
        let creation_timestamp = node
            .data
            .pointer("/metadata/creationTimestamp")
            .and_then(|v| v.as_str())
            .unwrap();
        assert_ne!(
            creation_timestamp, "2026-01-01T00:00:00Z",
            "register_node must refresh Node creationTimestamp so kubectl AGE reflects this process start"
        );
    }

    #[tokio::test]
    async fn test_register_node_sets_capacity_and_allocatable() {
        let db = crate::datastore::test_support::in_memory().await;

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();
        let data = node.data;

        // capacity and allocatable must both exist with cpu, memory, pods
        for field in ["capacity", "allocatable"] {
            let section = &data["status"][field];
            assert!(
                section["cpu"].as_str().is_some(),
                "{}.cpu should be a string",
                field
            );
            let cpu: usize = section["cpu"].as_str().unwrap().parse().unwrap();
            assert!(cpu >= 1, "{}.cpu should be >= 1", field);

            assert!(
                section["memory"].as_str().unwrap().ends_with("Ki"),
                "{}.memory should end with Ki",
                field
            );
            assert_eq!(section["pods"], "110");
        }

        // capacity and allocatable should match (klights doesn't reserve resources)
        assert_eq!(
            data["status"]["capacity"]["cpu"],
            data["status"]["allocatable"]["cpu"]
        );
        assert_eq!(
            data["status"]["capacity"]["memory"],
            data["status"]["allocatable"]["memory"]
        );
    }

    #[tokio::test]
    async fn test_register_node_sets_ready_condition() {
        let db = crate::datastore::test_support::in_memory().await;

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();
        let conditions = node.data["status"]["conditions"].as_array().unwrap();

        // Should have 5 conditions: Ready, MemoryPressure, DiskPressure, PIDPressure, NetworkUnavailable
        assert_eq!(conditions.len(), 5);

        let ready = conditions.iter().find(|c| c["type"] == "Ready").unwrap();
        assert_eq!(ready["status"], "True");
        assert_eq!(ready["reason"], "KubeletReady");
        assert!(
            ready.get("lastHeartbeatTime").is_none(),
            "registered Node status must not persist the churny lastHeartbeatTime field"
        );
        assert!(ready.get("lastTransitionTime").is_some());

        // Negative conditions should all be False
        for cond_type in [
            "MemoryPressure",
            "DiskPressure",
            "PIDPressure",
            "NetworkUnavailable",
        ] {
            let cond = conditions.iter().find(|c| c["type"] == cond_type).unwrap();
            assert_eq!(cond["status"], "False", "{} should be False", cond_type);
        }

        // NetworkUnavailable should have specific reason
        let network_cond = conditions
            .iter()
            .find(|c| c["type"] == "NetworkUnavailable")
            .unwrap();
        assert_eq!(network_cond["reason"], "RouteCreated");
    }

    #[tokio::test]
    async fn test_register_node_has_leader_role_label() {
        let db = crate::datastore::test_support::in_memory().await;

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();
        let labels = node.data["metadata"]["labels"].as_object().unwrap();

        // kubectl derives ROLES column from node-role.kubernetes.io/* labels
        assert!(
            labels.contains_key("node-role.kubernetes.io/leader"),
            "leader node must have leader role label for kubectl ROLES column"
        );
        assert!(!labels.contains_key("node-role.kubernetes.io/master"));
        assert!(!labels.contains_key("node-role.kubernetes.io/controlplane"));
        assert!(!labels.contains_key("node-role.kubernetes.io/control-plane"));
    }

    #[tokio::test]
    async fn test_register_node_has_worker_role_label() {
        let db = crate::datastore::test_support::in_memory().await;

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Worker {
                leader_endpoints: vec!["https://leader:7979".to_string()],
                token: Some("token".to_string()),
                skip_ca: false,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();
        let labels = node.data["metadata"]["labels"].as_object().unwrap();

        assert!(labels.contains_key("node-role.kubernetes.io/worker"));
        assert!(!labels.contains_key("node-role.kubernetes.io/leader"));
        assert!(!labels.contains_key("node-role.kubernetes.io/master"));
        assert!(!labels.contains_key("node-role.kubernetes.io/controlplane"));
        assert!(!labels.contains_key("node-role.kubernetes.io/control-plane"));
    }

    #[tokio::test]
    async fn test_register_node_prunes_stale_klights_role_labels_on_refresh() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Node",
            None,
            "test-node",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "test-node",
                    "labels": {
                        "node-role.kubernetes.io/master": "",
                        "node-role.kubernetes.io/controlplane": "",
                        "example.com/preserve": "true"
                    }
                },
                "spec": {},
                "status": {}
            }),
        )
        .await
        .unwrap();

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Worker {
                leader_endpoints: vec!["https://leader:7979".to_string()],
                token: Some("token".to_string()),
                skip_ca: false,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();
        let labels = node.data["metadata"]["labels"].as_object().unwrap();

        assert_eq!(
            labels.get("example.com/preserve").and_then(|v| v.as_str()),
            Some("true")
        );
        assert!(labels.contains_key("node-role.kubernetes.io/worker"));
        assert!(!labels.contains_key("node-role.kubernetes.io/master"));
        assert!(!labels.contains_key("node-role.kubernetes.io/control-plane"));
    }

    #[tokio::test]
    async fn test_register_node_version_format() {
        let db = crate::datastore::test_support::in_memory().await;

        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();

        let version = node.data["status"]["nodeInfo"]["kubeletVersion"]
            .as_str()
            .unwrap();
        // kubectl displays this in the VERSION column
        assert!(
            version.starts_with("v"),
            "kubeletVersion must start with 'v' for kubectl VERSION column, got: {}",
            version
        );
    }

    #[tokio::test]
    async fn test_register_node_rootless_uses_injected_kubelet_version_profile() {
        let db = crate::datastore::test_support::in_memory().await;
        let mode = crate::bootstrap::NodeMode::Rootless {
            rootlesskit_pid: 0,
            user_netns: std::path::PathBuf::from("/proc/self/ns/net"),
        };

        register_node(
            &db,
            "rootless-node",
            &mode,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "rootless-node")
            .await
            .unwrap()
            .unwrap();

        let version = node.data["status"]["nodeInfo"]["kubeletVersion"]
            .as_str()
            .unwrap();
        assert_eq!(version, "v1.34.6+klights-test");
    }

    #[tokio::test]
    async fn test_register_node_sets_daemon_endpoints_kubelet_port() {
        let db = crate::datastore::test_support::in_memory().await;
        register_node(
            &db,
            "test-node",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "test-node")
            .await
            .unwrap()
            .unwrap();

        let port = node
            .data
            .pointer("/status/daemonEndpoints/kubeletEndpoint/Port")
            .and_then(|v| v.as_i64());
        assert_eq!(
            port,
            Some(10250),
            "Node must have status.daemonEndpoints.kubeletEndpoint.Port = 10250"
        );
        assert!(
            node.data
                .pointer("/status/daemonEndpoints/kubeletEndpoint/port")
                .is_none(),
            "Node must not use non-Kubernetes lowercase daemon endpoint port"
        );
    }

    struct RecordingLeaseRenewClient {
        calls: StdArc<Mutex<Vec<String>>>,
    }

    impl RecordingLeaseRenewClient {
        fn new() -> Self {
            Self {
                calls: StdArc::new(Mutex::new(Vec::new())),
            }
        }

        fn call_count(&self) -> usize {
            self.calls
                .lock()
                .expect("recording mutex must remain lockable")
                .len()
        }
    }

    impl klights_leader_api::LeaderNodeLeaseRenewal for RecordingLeaseRenewClient {
        fn renew_node_lease(
            &self,
            request: klights_leader_api::NodeLeaseRenewalRequest,
        ) -> klights_leader_api::NodeLeaseRenewalFuture<
            '_,
            klights_leader_api::NodeLeaseRenewalResult,
        > {
            let node_name = request.node_name().to_string();
            self.calls
                .lock()
                .expect("recording mutex must remain lockable")
                .push(node_name);
            Box::pin(async { Ok(klights_leader_api::NodeLeaseRenewalResult::Renewed) })
        }
    }

    struct FixedHeartbeatClock;

    impl klights_kubelet::node_heartbeat::NodeHeartbeatClock for FixedHeartbeatClock {
        fn now_microtime(&self) -> String {
            "2026-07-27T00:00:00.000000Z".to_string()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_heartbeat_with_interval_never_writes_lease_to_db() {
        // T6: the production heartbeat is memory-only. It renews via the lease
        // client (worker -> leader RPC / leader-local tracker) and must never
        // write a Lease row to cluster.db.
        let db = crate::datastore::test_support::in_memory().await;
        let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(&db);
        let db_handle: crate::datastore::DatastoreHandle = std::sync::Arc::new(db.clone());
        let client = std::sync::Arc::new(RecordingLeaseRenewClient::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let watch_source = std::sync::Arc::new(
            crate::bootstrap::kubelet_ports::DatastorePodWatchSource::new(std::sync::Arc::new(
                crate::positioned_watch_adapter::for_test(&passive_reads, db_handle),
            )),
        );
        let handle = tokio::spawn(run_heartbeat_with_interval(
            watch_source,
            client.clone(),
            std::sync::Arc::new(FixedHeartbeatClock),
            "test-node".to_string(),
            cancel.clone(),
            std::sync::Arc::new(klights_supervisor::TaskSupervisor::new(
                klights_supervisor::TaskCategoryConfig::default(),
            )),
            Duration::from_millis(25),
        ));

        // Over several heartbeat intervals, no Lease row may appear in cluster.db.
        let lease_rv =
            wait_for_lease_resource_version(&db, "test-node", 1, Duration::from_millis(300)).await;
        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;

        assert!(
            lease_rv.is_none(),
            "memory-only heartbeat must not write a Lease to cluster.db"
        );
        assert!(
            client.call_count() > 0,
            "heartbeat must renew via the lease client"
        );
    }

    #[tokio::test]
    async fn node_effect_self_status_uses_fresh_identity_and_only_durable_enqueue() {
        use crate::datastore::backend_kind::BackendKind;
        use crate::datastore::node_local::selector;
        use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

        let cluster: crate::datastore::DatastoreHandle =
            std::sync::Arc::new(crate::datastore::test_support::in_memory().await);
        let created = cluster
            .create_resource(
                "v1",
                "Node",
                None,
                "worker-a",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": "worker-a", "uid": "node-uid-a"},
                    "status": {"conditions": []}
                }),
            )
            .await
            .expect("create Node");
        let leader_query: std::sync::Arc<dyn klights_leader_api::LeaderResourceQuery> =
            std::sync::Arc::new(crate::control_plane::client::local::LocalApiClient::new(
                cluster.clone(),
                "cp-1".to_string(),
                crate::control_plane::client::local::always_leader_watch(),
            ));
        let supervisor = std::sync::Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        let node_local = selector::open_node_local(
            BackendKind::Sqlite,
            None,
            supervisor,
            None,
            "sqlite:node-self-status-contract",
        )
        .await
        .expect("open node-local db");
        let outbox = std::sync::Arc::new(crate::outbox_test_support::outbox_from_node_db(
            node_local.clone(),
        ));
        let publisher = OutboxNodeSelfStatusPublisher::new(
            "worker-a",
            leader_query,
            outbox,
            std::sync::Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: "worker-a".to_string(),
            status: serde_json::json!({
                "conditions": [{"type": "Ready", "status": "True"}]
            }),
            expected_rv: None,
            preconditions: ResourcePreconditions::uid("node-uid-a"),
            observed_status_stamp: None,
        };
        let request = klights_leader_api::NodeSelfStatusRequest::try_new(command.clone())
            .expect("valid self status");

        let result =
            klights_leader_api::LeaderNodeSelfStatus::submit_node_self_status(&publisher, request)
                .await
                .expect("enqueue status");
        assert_eq!(result, klights_leader_api::NodeSelfStatusResult::Enqueued);

        let unchanged = cluster
            .get_resource("v1", "Node", None, "worker-a")
            .await
            .expect("read Node")
            .expect("Node exists");
        assert_eq!(
            unchanged.resource_version, created.resource_version,
            "self-status publisher must not bypass the durable queue with direct leader apply"
        );
        let row = node_local
            .legacy_claim_next_due_outbox(i64::MAX / 2, 1_000, "inspect")
            .await
            .expect("claim queued status")
            .expect("one durable status row");
        assert_eq!(row.operation, OutboxOperation::NodeStatus.as_str());
        assert_eq!(row.subject_key, "v1/Node/worker-a/node-uid-a");
        let decoded = OutboxPayload::decode_protobuf(&row.payload_proto)
            .expect("decode durable status payload");
        assert_eq!(decoded.command, command);
    }

    #[tokio::test]
    async fn node_effect_self_status_rejects_cross_node_before_enqueue() {
        use crate::datastore::backend_kind::BackendKind;
        use crate::datastore::node_local::selector;
        use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

        let cluster: crate::datastore::DatastoreHandle =
            std::sync::Arc::new(crate::datastore::test_support::in_memory().await);
        let leader_query: std::sync::Arc<dyn klights_leader_api::LeaderResourceQuery> =
            std::sync::Arc::new(crate::control_plane::client::local::LocalApiClient::new(
                cluster,
                "cp-1".to_string(),
                crate::control_plane::client::local::always_leader_watch(),
            ));
        let node_local = selector::open_node_local(
            BackendKind::Sqlite,
            None,
            std::sync::Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
            None,
            "sqlite:node-self-status-cross-node",
        )
        .await
        .expect("open node-local db");
        let publisher = OutboxNodeSelfStatusPublisher::new(
            "worker-a",
            leader_query,
            std::sync::Arc::new(crate::outbox_test_support::outbox_from_node_db(
                node_local.clone(),
            )),
            std::sync::Arc::new(klights_kubelet::runtime_clock::SystemRuntimeClock),
        );
        let request =
            klights_leader_api::NodeSelfStatusRequest::try_new(StorageCommand::UpdateStatus {
                api_version: "v1".to_string(),
                kind: "Node".to_string(),
                namespace: None,
                name: "worker-b".to_string(),
                status: serde_json::json!({}),
                expected_rv: None,
                preconditions: ResourcePreconditions::uid("node-uid-b"),
                observed_status_stamp: None,
            })
            .expect("valid shape");
        assert!(matches!(
            klights_leader_api::LeaderNodeSelfStatus::submit_node_self_status(&publisher, request)
                .await,
            Err(klights_leader_api::NodeSelfStatusError::Unauthorized { .. })
        ));
        assert!(
            node_local
                .legacy_claim_next_due_outbox(i64::MAX / 2, 1_000, "inspect")
                .await
                .expect("inspect queue")
                .is_none()
        );
    }

    /// F2-05 closing gate: rootless boot publishes both mode and hostport-range
    /// annotations so peers can project this node as `NodePeerMode::Rootless`.
    #[tokio::test]
    async fn node_status_publishes_mode_annotation() {
        use klights_controllers::annotations::{
            DEFAULT_HOSTPORT_RANGE, HOSTPORT_RANGE_ANNOTATION, NODE_MODE_ANNOTATION,
        };
        let db = crate::datastore::test_support::in_memory().await;
        let mode = crate::bootstrap::NodeMode::Rootless {
            rootlesskit_pid: 0,
            user_netns: std::path::PathBuf::from("/proc/self/ns/net"),
        };
        register_node(
            &db,
            "rootless-node-x",
            &mode,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "rootless-node-x")
            .await
            .unwrap()
            .expect("Node must exist after register_node");
        let annotations = node
            .data
            .pointer("/metadata/annotations")
            .expect("Node must carry annotations");
        assert_eq!(
            annotations
                .get(NODE_MODE_ANNOTATION)
                .and_then(|v| v.as_str()),
            Some("rootless"),
        );
        assert_eq!(
            annotations
                .get(HOSTPORT_RANGE_ANNOTATION)
                .and_then(|v| v.as_str()),
            Some(DEFAULT_HOSTPORT_RANGE),
        );
    }

    /// F2-05 closing gate: root-mode publishes mode=root with an empty
    /// hostport-range so mixed clusters see a uniform annotation shape
    /// without implying root mode has a rootless host-port graft range.
    #[tokio::test]
    async fn node_status_root_publishes_empty_hostport_annotation() {
        use klights_controllers::annotations::{HOSTPORT_RANGE_ANNOTATION, NODE_MODE_ANNOTATION};
        let db = crate::datastore::test_support::in_memory().await;
        register_node(
            &db,
            "root-node-x",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "root-node-x")
            .await
            .unwrap()
            .expect("Node must exist after register_node");
        let annotations = node
            .data
            .pointer("/metadata/annotations")
            .expect("Node must carry annotations");
        assert_eq!(
            annotations
                .get(NODE_MODE_ANNOTATION)
                .and_then(|v| v.as_str()),
            Some("root"),
        );
        assert_eq!(
            annotations
                .get(HOSTPORT_RANGE_ANNOTATION)
                .and_then(|v| v.as_str()),
            Some(""),
            "root mode must publish an empty hostport-range for shape consistency"
        );
    }

    /// F2-05 DRY gate: the Node publisher and the projector consume shared
    /// annotation constants. If a future refactor introduces a duplicate
    /// string, this test fails the symbol equality.
    #[test]
    fn annotation_key_constants_are_shared() {
        use klights_controllers::annotations::{HOSTPORT_RANGE_ANNOTATION, NODE_MODE_ANNOTATION};
        assert_eq!(NODE_MODE_ANNOTATION, "klights.io/mode");
        assert_eq!(HOSTPORT_RANGE_ANNOTATION, "klights.io/hostport-range");
    }

    /// `register_node` publishes the short git commit hash so the wide-only
    /// `COMMIT` column in `kubectl get nodes -o wide` can surface version skew
    /// across nodes in a multinode cluster.
    #[tokio::test]
    async fn node_status_publishes_git_commit_annotation() {
        use klights_controllers::annotations::GIT_COMMIT_ANNOTATION;
        let db = crate::datastore::test_support::in_memory().await;
        register_node(
            &db,
            "commit-node-x",
            &crate::bootstrap::NodeMode::Root,
            &crate::bootstrap::NodeRole::Leader {
                bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Seed,
            },
            None,
            None,
        )
        .await
        .unwrap();

        let node = db
            .get_resource("v1", "Node", None, "commit-node-x")
            .await
            .unwrap()
            .expect("Node must exist after register_node");
        let annotations = node
            .data
            .pointer("/metadata/annotations")
            .expect("Node must carry annotations");
        let commit = annotations
            .get(GIT_COMMIT_ANNOTATION)
            .and_then(|v| v.as_str())
            .expect("Node must publish klights.io/git-commit annotation");
        assert_eq!(
            commit, "test-commit",
            "published git-commit annotation must match the build-time short hash"
        );
        assert!(
            !commit.is_empty(),
            "git-commit annotation must not be empty"
        );
    }
}
