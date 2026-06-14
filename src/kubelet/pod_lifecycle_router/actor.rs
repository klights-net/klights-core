//! Actor backend — wraps the existing per-pod actor transport.
//!
//! Delegates to `PodLifecycleRegistry` for all lifecycle routing.
//! After the actor processes each message, the backend dispatches
//! the returned `PodAction` through the actor-mode `PodWorkExecutor`.

use async_trait::async_trait;
use std::sync::{Arc, Mutex};

use super::PodLifecycleDiagnostics;
use super::PodLifecycleRouteBackend;
use super::PodLifecycleRouteError;
use super::PodLifecycleRouteMode;
use super::executor::PodWorkExecutor;
use crate::kubelet::pod_lifecycle_actor::message::{LifecycleMessage, PodLifecycleKey};
use crate::kubelet::pod_lifecycle_actor::registry::PodLifecycleRegistry;

/// Actor backend wrapping the real `PodLifecycleRegistry`.
pub struct ActorPodLifecycleBackend {
    registry: Arc<PodLifecycleRegistry>,
    executor_holder: Arc<Mutex<Arc<dyn PodWorkExecutor>>>,
}

impl std::fmt::Debug for ActorPodLifecycleBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorPodLifecycleBackend")
            .finish_non_exhaustive()
    }
}

impl ActorPodLifecycleBackend {
    pub fn new(
        registry: Arc<PodLifecycleRegistry>,
        executor_holder: Arc<Mutex<Arc<dyn PodWorkExecutor>>>,
    ) -> Self {
        Self {
            registry,
            executor_holder,
        }
    }

    pub fn executor_holder(&self) -> Arc<Mutex<Arc<dyn PodWorkExecutor>>> {
        self.executor_holder.clone()
    }
}

#[async_trait]
impl PodLifecycleRouteBackend for ActorPodLifecycleBackend {
    async fn route(&self, message: LifecycleMessage) -> Result<(), PodLifecycleRouteError> {
        let key = message.key().clone();
        let sender = self.registry.sender_for(key).await.map_err(|err| {
            PodLifecycleRouteError::SendError(format!("failed to get actor sender: {err}"))
        })?;
        sender.send(message).await.map_err(|err| {
            PodLifecycleRouteError::SendError(format!("failed to send to actor: {err}"))
        })
    }

    fn try_route_nonblocking(&self, message: LifecycleMessage) {
        let key = message.key();
        let Some(sender) = self.registry.try_sender_for(key) else {
            return;
        };
        sender.try_send_nonblocking(message);
    }

    fn mode(&self) -> PodLifecycleRouteMode {
        PodLifecycleRouteMode::Actor
    }

    async fn remove_pod_state(&self, key: &PodLifecycleKey) -> bool {
        self.registry.remove_actor(key).await
    }

    async fn diagnostics(&self) -> PodLifecycleDiagnostics {
        PodLifecycleDiagnostics {
            mode: PodLifecycleRouteMode::Actor,
            actor_states: self.registry.actor_states_snapshot().await,
            recent_trace: self.registry.recent_trace(200).await,
            active_pod_count: self.registry.actor_count().await,
        }
    }

    async fn active_pod_count(&self) -> usize {
        self.registry.actor_count().await
    }

    fn set_work_executor(&self, executor: Arc<dyn PodWorkExecutor>) {
        *self.executor_holder.lock().unwrap() = executor;
    }

    fn executor_holder(&self) -> Option<Arc<std::sync::Mutex<Arc<dyn PodWorkExecutor>>>> {
        Some(self.executor_holder.clone())
    }
}
