use std::collections::HashMap;

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use klights_cluster_core::StorageCommand;
use serde_json::Value;
use tokio::sync::{Notify, RwLock};

use klights_cluster_core::k8s_time::format_time as k8s_time_format;

pub use klights_cluster_core::DEFAULT_NODE_HEARTBEAT_INTERVAL_SECONDS;
use klights_cluster_core::DEFAULT_NODE_LEASE_DURATION_SECONDS;
pub const DEFAULT_NODE_LEASE_MISSED_HEARTBEATS: i64 = 3;
pub const DEFAULT_NODE_LEASE_GRACE_SECONDS: i64 =
    DEFAULT_NODE_HEARTBEAT_INTERVAL_SECONDS * DEFAULT_NODE_LEASE_MISSED_HEARTBEATS;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeLeaseObservation {
    pub node_name: String,
    pub renew_time: DateTime<Utc>,
    pub lease_duration_seconds: i64,
}

impl NodeLeaseObservation {
    pub fn deadline(&self) -> DateTime<Utc> {
        self.stale_deadline()
    }

    pub fn stale_deadline(&self) -> DateTime<Utc> {
        self.renew_time + chrono::Duration::seconds(self.stale_timeout_seconds())
    }

    pub fn stale_timeout_seconds(&self) -> i64 {
        self.lease_duration_seconds
            .clamp(1, DEFAULT_NODE_LEASE_GRACE_SECONDS)
    }

    pub fn renew_time_string(&self) -> String {
        k8s_time_format(self.renew_time)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeLeaseDeadline {
    pub observed: Option<NodeLeaseObservation>,
    pub deadline: DateTime<Utc>,
}

pub struct NodeLeaseTracker {
    // Interior-mutable so a freshly-promoted leader can reset the grace
    // window (see `reset_grace_window`): with in-memory liveness, a new
    // leader starts blind and must give every not-yet-observed node a fresh
    // startup grace, or it would compute stale deadlines and mass-evict.
    startup_time: RwLock<DateTime<Utc>>,
    startup_grace_seconds: i64,
    leases: RwLock<HashMap<String, NodeLeaseObservation>>,
    changed: Notify,
}

impl NodeLeaseTracker {
    pub fn new_at(startup_time: DateTime<Utc>) -> Self {
        Self {
            startup_time: RwLock::new(startup_time),
            startup_grace_seconds: DEFAULT_NODE_LEASE_DURATION_SECONDS,
            leases: RwLock::new(HashMap::new()),
            changed: Notify::new(),
        }
    }

    #[cfg(test)]
    fn new_for_test(startup_time: DateTime<Utc>) -> Self {
        Self::new_at(startup_time)
    }

    /// Reset the startup grace window to begin at `now`.
    ///
    /// Called when this node (re)acquires raft leadership. A new leader's
    /// in-memory liveness is empty, so never-yet-observed nodes must fall back
    /// to `now + startup_grace` (not an ancient process-start time), giving
    /// them a fresh window to renew before they can be declared stale. Observed
    /// leases are untouched — they keep their real renew-based deadlines.
    pub async fn reset_grace_window(&self, now: DateTime<Utc>) {
        *self.startup_time.write().await = now;
    }

    pub async fn wait_changed(&self) {
        self.changed.notified().await;
    }

    pub async fn observed(&self, node_name: &str) -> Option<NodeLeaseObservation> {
        self.leases.read().await.get(node_name).cloned()
    }

    pub async fn observed_renew_time(&self, node_name: &str) -> Option<String> {
        self.observed(node_name)
            .await
            .map(|observation| observation.renew_time_string())
    }

    pub async fn deadline_for_node(&self, node_name: &str) -> NodeLeaseDeadline {
        if let Some(observed) = self.observed(node_name).await {
            return NodeLeaseDeadline {
                deadline: observed.stale_deadline(),
                observed: Some(observed),
            };
        }
        NodeLeaseDeadline {
            observed: None,
            deadline: *self.startup_time.read().await
                + chrono::Duration::seconds(self.startup_grace_seconds),
        }
    }

    pub async fn record_from_command(
        &self,
        command: &StorageCommand,
        authoring_node: &str,
    ) -> Result<NodeLeaseObservation> {
        klights_cluster_core::validate_lease_renew_command(command, authoring_node)?;
        let (name, data) = match command {
            StorageCommand::CreateResource { name, data, .. }
            | StorageCommand::UpdateResource { name, data, .. } => (name, data),
            _ => {
                return Err(anyhow!(
                    "LeaseRenew must carry a Lease create/update command"
                ));
            }
        };
        self.record_from_lease_object(name, data).await
    }

    pub async fn record_from_lease_object(
        &self,
        fallback_node_name: &str,
        lease: &Value,
    ) -> Result<NodeLeaseObservation> {
        let node_name = lease
            .pointer("/metadata/name")
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .unwrap_or(fallback_node_name)
            .to_string();
        let renew_time = lease
            .pointer("/spec/renewTime")
            .and_then(|value| value.as_str())
            .ok_or_else(|| anyhow!("LeaseRenew missing spec.renewTime"))
            .and_then(parse_renew_time)?;
        let lease_duration_seconds = lease
            .pointer("/spec/leaseDurationSeconds")
            .and_then(|value| value.as_i64())
            .filter(|seconds| *seconds > 0)
            .unwrap_or(DEFAULT_NODE_LEASE_DURATION_SECONDS);
        let observation = NodeLeaseObservation {
            node_name: node_name.clone(),
            renew_time,
            lease_duration_seconds,
        };

        let mut leases = self.leases.write().await;
        let should_update = leases
            .get(&node_name)
            .map(|current| current.renew_time < observation.renew_time)
            .unwrap_or(true);
        if should_update {
            leases.insert(node_name, observation.clone());
            drop(leases);
            self.changed.notify_waiters();
        }
        Ok(observation)
    }
}

fn parse_renew_time(raw: &str) -> Result<DateTime<Utc>> {
    Ok(chrono::DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    #[tokio::test]
    async fn leadership_reset_extends_only_unobserved_node_deadline() {
        let start = Utc.with_ymd_and_hms(2026, 5, 13, 6, 0, 0).unwrap();
        let tracker = NodeLeaseTracker::new_for_test(start);
        tracker
            .record_from_lease_object(
                "seen",
                &json!({
                    "metadata": {"name": "seen"},
                    "spec": {
                        "renewTime": "2026-05-13T06:00:05Z",
                        "leaseDurationSeconds": 10
                    }
                }),
            )
            .await
            .unwrap();
        let observed_before = tracker.deadline_for_node("seen").await;

        let promoted_at = Utc.with_ymd_and_hms(2026, 5, 13, 7, 0, 0).unwrap();
        tracker.reset_grace_window(promoted_at).await;

        assert_eq!(
            tracker.deadline_for_node("never-seen").await.deadline,
            promoted_at + chrono::Duration::seconds(DEFAULT_NODE_LEASE_DURATION_SECONDS)
        );
        assert_eq!(
            tracker.deadline_for_node("seen").await.deadline,
            observed_before.deadline
        );
    }

    #[tokio::test]
    async fn stale_lease_event_cannot_move_observation_backwards() {
        let tracker = NodeLeaseTracker::new_for_test(Utc::now());
        for renew_time in ["2026-05-13T06:00:10Z", "2026-05-13T06:00:05Z"] {
            tracker
                .record_from_lease_object(
                    "worker-a",
                    &json!({"spec": {"renewTime": renew_time, "leaseDurationSeconds": 10}}),
                )
                .await
                .unwrap();
        }
        assert_eq!(
            tracker.observed_renew_time("worker-a").await.as_deref(),
            Some("2026-05-13T06:00:10Z")
        );
    }
}
