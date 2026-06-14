//! Multiplex backend — single-actor, multi-pod transport.
//!
//! In R1 this is a placeholder that returns NotAvailable. Will be
//! implemented to route all pod lifecycle messages through a single
//! multiplexed channel.

use async_trait::async_trait;

use super::PodLifecycleDiagnostics;
use super::PodLifecycleRouteBackend;
use super::PodLifecycleRouteError;
use super::PodLifecycleRouteMode;
use crate::kubelet::pod_lifecycle_actor::message::{LifecycleMessage, PodLifecycleKey};

/// Multiplex backend placeholder.
#[derive(Debug)]
pub struct MultiplexPodLifecycleBackend;

#[async_trait]
impl PodLifecycleRouteBackend for MultiplexPodLifecycleBackend {
    async fn route(&self, _message: LifecycleMessage) -> Result<(), PodLifecycleRouteError> {
        Err(PodLifecycleRouteError::NotAvailable(
            "multiplex backend not yet implemented".to_string(),
        ))
    }

    fn try_route_nonblocking(&self, _message: LifecycleMessage) {
        // Multiplex not yet implemented — silently drop.
    }

    fn mode(&self) -> PodLifecycleRouteMode {
        PodLifecycleRouteMode::Multiplex
    }

    async fn remove_pod_state(&self, _key: &PodLifecycleKey) -> bool {
        false
    }

    async fn diagnostics(&self) -> PodLifecycleDiagnostics {
        PodLifecycleDiagnostics {
            mode: PodLifecycleRouteMode::Multiplex,
            actor_states: Vec::new(),
            recent_trace: Vec::new(),
            active_pod_count: 0,
        }
    }

    async fn active_pod_count(&self) -> usize {
        0
    }
}
