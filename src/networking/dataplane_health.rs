//! Dataplane health tracking for multinode readiness.
//!
//! Root and rootless modes both wire into this: when the required local
//! dataplane cannot be established (WireGuard interface missing, pasta
//! UDP port not exposed, WireGuard key unreadable, etc.), the node must
//! become `NotReady` with `NetworkUnavailable=True` instead of silently
//! accepting plaintext or crashing the whole process.
//!
//! Health has two independent dimensions, combined into one status:
//!
//! 1. **Local** dataplane — whether the node's own dataplane device could be
//!    established at boot. A hard local failure (`set_unavailable`) is
//!    monotonic so the first, most specific boot reason is preserved; it can be
//!    cleared again with `set_healthy` once the local dataplane recovers.
//! 2. **Peer connectivity** — whether every *Ready* peer has a WireGuard route
//!    installed. A node is only `Ready` when all Ready peers are reachable; a
//!    peer that is itself NotReady is excluded so a genuinely-down node does not
//!    wedge the rest of the cluster NotReady forever.
//!
//! # Zero-cost at idle
//! Health is two small fields behind a single `Mutex`. Readers take the lock,
//! clone a tiny enum, and release; there is no background task and no polling.

use std::sync::{Arc, Mutex};

/// Immutable snapshot of the current dataplane health.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataplaneHealthStatus {
    /// Dataplane is ready and healthy.
    Healthy,
    /// Dataplane is unavailable. Carries a k8s-compatible reason string.
    Unavailable { reason: String },
}

impl DataplaneHealthStatus {
    pub fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy)
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Healthy => None,
            Self::Unavailable { reason } => Some(reason),
        }
    }
}

/// Peer-connectivity dimension of dataplane health.
#[derive(Clone, Debug, PartialEq, Eq)]
enum PeerConnectivity {
    /// No peers are required for this node to be Ready (single-node clusters).
    NotRequired,
    /// Multinode node that has not yet established connectivity to its peers.
    Pending,
    /// Every Ready peer has a dataplane route installed.
    Connected,
    /// At least one Ready peer cannot be reached over the dataplane.
    Disconnected { reason: String },
}

/// Default message surfaced while a multinode node is waiting for peer
/// connectivity (before its first successful peer-route sync).
const WAITING_FOR_PEERS: &str = "Waiting for peer dataplane connectivity";

/// Mutable health tracker owned by the network plane.
#[derive(Clone)]
pub struct DataplaneHealth {
    inner: Arc<DataplaneHealthInner>,
}

struct DataplaneHealthInner {
    state: Mutex<HealthState>,
}

struct HealthState {
    local: DataplaneHealthStatus,
    peers: PeerConnectivity,
}

impl HealthState {
    fn status(&self) -> DataplaneHealthStatus {
        // A hard local failure dominates: there is no point reporting peer
        // connectivity when the local dataplane device is gone.
        if let DataplaneHealthStatus::Unavailable { reason } = &self.local {
            return DataplaneHealthStatus::Unavailable {
                reason: reason.clone(),
            };
        }
        match &self.peers {
            PeerConnectivity::NotRequired | PeerConnectivity::Connected => {
                DataplaneHealthStatus::Healthy
            }
            PeerConnectivity::Pending => DataplaneHealthStatus::Unavailable {
                reason: WAITING_FOR_PEERS.to_string(),
            },
            PeerConnectivity::Disconnected { reason } => DataplaneHealthStatus::Unavailable {
                reason: reason.clone(),
            },
        }
    }
}

impl DataplaneHealth {
    /// Create a new health tracker starting in the Healthy state with no peer
    /// requirement (single-node default). Callers that cannot establish the
    /// local dataplane must call `set_unavailable` before the node registers as
    /// Ready; multinode nodes should call `set_peers_pending` so they register
    /// `NetworkUnavailable=True` until the first successful peer-route sync.
    pub fn new_healthy() -> Self {
        Self::new(
            DataplaneHealthStatus::Healthy,
            PeerConnectivity::NotRequired,
        )
    }

    fn new(local: DataplaneHealthStatus, peers: PeerConnectivity) -> Self {
        Self {
            inner: Arc::new(DataplaneHealthInner {
                state: Mutex::new(HealthState { local, peers }),
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HealthState> {
        self.inner
            .state
            .lock()
            .expect("dataplane health mutex poisoned")
    }

    /// Mark the local dataplane as unavailable with a k8s-compatible reason.
    /// Monotonic: once the local dataplane is unhealthy, subsequent calls keep
    /// the first (most specific) boot reason until `set_healthy` clears it.
    pub fn set_unavailable(&self, reason: String) {
        let mut state = self.lock();
        if state.local.is_healthy() {
            state.local = DataplaneHealthStatus::Unavailable { reason };
        }
    }

    /// Recover the local dataplane to Healthy. Peer connectivity is unaffected.
    pub fn set_healthy(&self) {
        let mut state = self.lock();
        state.local = DataplaneHealthStatus::Healthy;
    }

    /// Declare that this node requires peer connectivity and has not yet
    /// established it. Registers as `NetworkUnavailable=True`.
    pub fn set_peers_pending(&self) {
        let mut state = self.lock();
        state.peers = PeerConnectivity::Pending;
    }

    /// All Ready peers have a dataplane route installed.
    pub fn set_peers_connected(&self) {
        let mut state = self.lock();
        state.peers = PeerConnectivity::Connected;
    }

    /// At least one Ready peer cannot be reached over the dataplane.
    pub fn set_peers_disconnected(&self, reason: String) {
        let mut state = self.lock();
        state.peers = PeerConnectivity::Disconnected { reason };
    }

    /// Snapshot the current combined health status.
    pub fn status(&self) -> DataplaneHealthStatus {
        self.lock().status()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataplane_health_starts_healthy() {
        let health = DataplaneHealth::new_healthy();
        assert!(health.status().is_healthy());
        assert_eq!(health.status().reason(), None);
    }

    #[test]
    fn dataplane_health_set_unavailable_is_monotonic() {
        let health = DataplaneHealth::new_healthy();
        health.set_unavailable("WireGuard key not found".to_string());
        let status = health.status();
        assert!(!status.is_healthy());
        assert_eq!(
            status,
            DataplaneHealthStatus::Unavailable {
                reason: "WireGuard key not found".to_string(),
            }
        );

        // Second call must not overwrite the first reason.
        health.set_unavailable("something else".to_string());
        assert_eq!(
            health.status(),
            DataplaneHealthStatus::Unavailable {
                reason: "WireGuard key not found".to_string(),
            }
        );
    }

    #[test]
    fn dataplane_health_clone_shares_state() {
        let health = DataplaneHealth::new_healthy();
        let clone = health.clone();
        health.set_unavailable("pasta UDP port not exposed".to_string());
        assert!(!clone.status().is_healthy());
    }

    #[test]
    fn set_healthy_recovers_from_local_unavailable() {
        let health = DataplaneHealth::new_healthy();
        health.set_unavailable("WireGuard dataplane: boom".to_string());
        assert!(!health.status().is_healthy());
        health.set_healthy();
        assert!(
            health.status().is_healthy(),
            "set_healthy must recover the local dataplane (bidirectional)"
        );
    }

    #[test]
    fn pending_peers_report_network_unavailable() {
        let health = DataplaneHealth::new_healthy();
        health.set_peers_pending();
        assert_eq!(
            health.status(),
            DataplaneHealthStatus::Unavailable {
                reason: WAITING_FOR_PEERS.to_string(),
            }
        );
    }

    #[test]
    fn peers_connected_make_node_healthy() {
        let health = DataplaneHealth::new_healthy();
        health.set_peers_pending();
        assert!(!health.status().is_healthy());
        health.set_peers_connected();
        assert!(health.status().is_healthy());
    }

    #[test]
    fn peers_disconnected_report_reason() {
        let health = DataplaneHealth::new_healthy();
        health.set_peers_connected();
        health.set_peers_disconnected("1 of 1 ready peer unreachable".to_string());
        assert_eq!(
            health.status(),
            DataplaneHealthStatus::Unavailable {
                reason: "1 of 1 ready peer unreachable".to_string(),
            }
        );
        // Recover when the peer becomes reachable again.
        health.set_peers_connected();
        assert!(health.status().is_healthy());
    }

    #[test]
    fn local_failure_dominates_connected_peers() {
        let health = DataplaneHealth::new_healthy();
        health.set_peers_connected();
        health.set_unavailable("WireGuard key not found".to_string());
        assert_eq!(
            health.status(),
            DataplaneHealthStatus::Unavailable {
                reason: "WireGuard key not found".to_string(),
            },
            "a hard local failure must dominate peer connectivity"
        );
    }
}
