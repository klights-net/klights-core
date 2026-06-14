//! Pod lifecycle router — facade that selects transport backend by environment.
//!
//! Reads `KLIGHTS_POD_LIFECYCLE_MODE` once at construction:
//! - unset or "actor" → actor backend (default)
//! - "multiplex" → multiplex backend
//! - invalid → actor backend with a warning

pub mod actor;
pub mod executor;
pub mod multiplex;

use std::fmt;
use std::sync::Arc;

use actor::ActorPodLifecycleBackend;
use executor::NoopExecutor;
use multiplex::MultiplexPodLifecycleBackend;
use tokio::sync::mpsc;

use crate::kubelet::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleKey};
use crate::kubelet::pod_lifecycle_core::trace::LifecycleTraceEntry;

use super::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig;
use super::pod_lifecycle_actor::registry::{PodLifecycleActorState, PodLifecycleRegistry};

/// Parsed mode for pod lifecycle routing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodLifecycleRouteMode {
    Actor,
    Multiplex,
}

/// Error returned by lifecycle route backends.
#[derive(Debug)]
pub enum PodLifecycleRouteError {
    SendError(String),
    NotAvailable(String),
}

impl fmt::Display for PodLifecycleRouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SendError(msg) => write!(f, "pod lifecycle send error: {msg}"),
            Self::NotAvailable(msg) => write!(f, "pod lifecycle backend not available: {msg}"),
        }
    }
}

impl std::error::Error for PodLifecycleRouteError {}

/// UID-qualified reason for local orphan cleanup requested by reconcilers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrphanReason {
    NodeLost,
    LeaderDeletedWhileDown,
    UidChangedWhileDown,
    ColdCriOrphan,
    StaleNetworkRow,
    StaleEndpointRow,
}

/// External reconciler entry point for actor-owned orphan cleanup.
pub async fn enqueue_orphan_finalize(
    router: &PodLifecycleRouter,
    key: PodLifecycleKey,
    reason: OrphanReason,
) -> Result<(), PodLifecycleRouteError> {
    router
        .route(LifecycleMessage::OrphanFinalize { key, reason })
        .await
}

/// Diagnostics snapshot for the debug endpoint.
#[derive(Clone, Debug)]
pub struct PodLifecycleDiagnostics {
    pub mode: PodLifecycleRouteMode,
    pub actor_states: Vec<PodLifecycleActorState>,
    pub recent_trace: Vec<LifecycleTraceEntry>,
    pub active_pod_count: usize,
}

/// Backend-neutral reply handle: cloneable, routes completion/failure
/// messages back through the selected lifecycle transport.
#[derive(Clone)]
pub struct LifecycleReplyHandle {
    target: LifecycleReplyTarget,
}

#[derive(Clone)]
enum LifecycleReplyTarget {
    Backend(Arc<dyn PodLifecycleRouteBackend>),
    Direct(mpsc::Sender<LifecycleMessage>),
}

impl LifecycleReplyHandle {
    pub fn new(backend: Arc<dyn PodLifecycleRouteBackend>) -> Self {
        Self {
            target: LifecycleReplyTarget::Backend(backend),
        }
    }

    pub fn direct(tx: mpsc::Sender<LifecycleMessage>) -> Self {
        Self {
            target: LifecycleReplyTarget::Direct(tx),
        }
    }

    pub async fn route(&self, message: LifecycleMessage) -> Result<(), PodLifecycleRouteError> {
        match &self.target {
            LifecycleReplyTarget::Backend(backend) => backend.route(message).await,
            LifecycleReplyTarget::Direct(tx) => tx.send(message).await.map_err(|err| {
                PodLifecycleRouteError::SendError(format!(
                    "failed to send direct lifecycle reply: {err}"
                ))
            }),
        }
    }
}

/// Internal trait for lifecycle route backends.
#[async_trait::async_trait]
pub trait PodLifecycleRouteBackend: Send + Sync {
    /// Route a message asynchronously (may wait for backpressure).
    async fn route(&self, message: LifecycleMessage) -> Result<(), PodLifecycleRouteError>;

    /// Fire-and-forget nonblocking route. Drops the message on failure.
    fn try_route_nonblocking(&self, message: LifecycleMessage);

    fn mode(&self) -> PodLifecycleRouteMode;

    /// Remove per-pod state for a deleted pod. Returns true if state was removed.
    /// Backend-neutral replacement for the old `remove_actor` name.
    async fn remove_pod_state(&self, key: &PodLifecycleKey) -> bool;

    /// Diagnostics snapshot for debug endpoints.
    async fn diagnostics(&self) -> PodLifecycleDiagnostics;

    /// Number of active pod lifecycle actors/entries.
    async fn active_pod_count(&self) -> usize;

    /// Replace the work executor at runtime. Default no-op; actor backend
    /// swaps the executor used by per-pod actors for dispatching `PodAction`s.
    fn set_work_executor(&self, _executor: Arc<dyn executor::PodWorkExecutor>) {}

    /// Shared executor handle for actors to read the current executor.
    fn executor_holder(&self) -> Option<Arc<std::sync::Mutex<Arc<dyn executor::PodWorkExecutor>>>> {
        None
    }
}

/// Pod lifecycle router: selects transport backend from environment at construction.
pub struct PodLifecycleRouter {
    backend: Arc<dyn PodLifecycleRouteBackend>,
}

impl std::fmt::Debug for PodLifecycleRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PodLifecycleRouter")
            .field("mode", &self.mode())
            .finish_non_exhaustive()
    }
}

impl PodLifecycleRouter {
    /// Create a router for actor mode backed by a real `PodLifecycleRegistry`
    /// and the default no-op executor.
    pub fn new_actor(registry: Arc<PodLifecycleRegistry>) -> Self {
        let executor_holder = registry.executor_holder();
        let backend: Arc<dyn PodLifecycleRouteBackend> = Arc::new(ActorPodLifecycleBackend::new(
            registry.clone(),
            executor_holder,
        ));
        registry.set_reply_handle(LifecycleReplyHandle::new(backend.clone()));
        Self { backend }
    }

    /// Create a router with a pre-built executor holder (for production wiring).
    #[cfg(test)]
    pub fn new_actor_with_executor(
        registry: Arc<PodLifecycleRegistry>,
        executor: Arc<dyn executor::PodWorkExecutor>,
    ) -> Self {
        let executor_holder = Arc::new(std::sync::Mutex::new(executor));
        let backend: Arc<dyn PodLifecycleRouteBackend> = Arc::new(ActorPodLifecycleBackend::new(
            registry.clone(),
            executor_holder,
        ));
        registry.set_reply_handle(LifecycleReplyHandle::new(backend.clone()));
        Self { backend }
    }

    /// Create a router from env-selected mode (for bootstrap).
    pub fn from_env(
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
        config: PodLifecycleConcurrencyConfig,
    ) -> Self {
        Self::from_env_impl(supervisor, config, |key| std::env::var(key))
    }

    /// Construct with injectable env reader (for testing).
    fn from_env_impl(
        supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
        config: PodLifecycleConcurrencyConfig,
        get_env: impl Fn(&str) -> Result<String, std::env::VarError>,
    ) -> Self {
        let mode = match get_env("KLIGHTS_POD_LIFECYCLE_MODE") {
            Ok(val) => match val.to_lowercase().as_str() {
                "actor" => PodLifecycleRouteMode::Actor,
                "multiplex" => PodLifecycleRouteMode::Multiplex,
                other => {
                    tracing::warn!(
                        mode = %other,
                        "unknown KLIGHTS_POD_LIFECYCLE_MODE value; falling back to actor mode"
                    );
                    PodLifecycleRouteMode::Actor
                }
            },
            Err(std::env::VarError::NotPresent) => PodLifecycleRouteMode::Actor,
            Err(std::env::VarError::NotUnicode(_)) => {
                tracing::warn!(
                    "KLIGHTS_POD_LIFECYCLE_MODE is not valid Unicode; falling back to actor mode"
                );
                PodLifecycleRouteMode::Actor
            }
        };

        let backend: Arc<dyn PodLifecycleRouteBackend> = match mode {
            PodLifecycleRouteMode::Actor => {
                let executor_holder = Arc::new(std::sync::Mutex::new(
                    Arc::new(NoopExecutor) as Arc<dyn executor::PodWorkExecutor>
                ));
                let registry = Arc::new(PodLifecycleRegistry::new(
                    supervisor,
                    config,
                    executor_holder.clone(),
                ));
                let backend: Arc<dyn PodLifecycleRouteBackend> = Arc::new(
                    ActorPodLifecycleBackend::new(registry.clone(), executor_holder),
                );
                registry.set_reply_handle(LifecycleReplyHandle::new(backend.clone()));
                backend
            }
            PodLifecycleRouteMode::Multiplex => Arc::new(MultiplexPodLifecycleBackend),
        };

        Self { backend }
    }

    /// Route a lifecycle message to the selected backend.
    pub async fn route(&self, message: LifecycleMessage) -> Result<(), PodLifecycleRouteError> {
        self.backend.route(message).await
    }

    /// Fire-and-forget nonblocking route. Drops the message on failure.
    pub fn try_route_nonblocking(&self, message: LifecycleMessage) {
        self.backend.try_route_nonblocking(message);
    }

    /// Remove per-pod state for a deleted pod (backend-neutral name).
    pub async fn remove_pod_state(&self, key: &PodLifecycleKey) -> bool {
        self.backend.remove_pod_state(key).await
    }

    /// Get a cloneable reply handle for use by executor futures.
    pub fn reply_handle(&self) -> LifecycleReplyHandle {
        LifecycleReplyHandle::new(self.backend.clone())
    }

    /// Replace the work executor at runtime. Called in `run_pod_watcher`
    /// after the real executor context is available.
    pub fn set_work_executor(&self, executor: Arc<dyn executor::PodWorkExecutor>) {
        self.backend.set_work_executor(executor);
    }

    /// Diagnostics for the debug endpoint.
    pub async fn diagnostics(&self) -> PodLifecycleDiagnostics {
        self.backend.diagnostics().await
    }

    /// Active pod lifecycle actor count.
    pub async fn active_pod_count(&self) -> usize {
        self.backend.active_pod_count().await
    }

    /// Selected mode for diagnostics.
    pub fn mode(&self) -> PodLifecycleRouteMode {
        self.backend.mode()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kubelet::pod_lifecycle_actor::config::PodLifecycleConcurrencyConfig;
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use std::collections::HashMap;

    fn test_supervisor() -> Arc<TaskSupervisor> {
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
    }

    fn test_registry(_capacity: usize) -> Arc<PodLifecycleRegistry> {
        let holder = Arc::new(std::sync::Mutex::new(
            Arc::new(NoopExecutor) as Arc<dyn executor::PodWorkExecutor>
        ));
        Arc::new(PodLifecycleRegistry::new(
            test_supervisor(),
            PodLifecycleConcurrencyConfig::production_default(),
            holder,
        ))
    }

    fn test_registry_with_executor(
        executor: Arc<dyn executor::PodWorkExecutor>,
    ) -> Arc<PodLifecycleRegistry> {
        let holder = Arc::new(std::sync::Mutex::new(executor));
        Arc::new(PodLifecycleRegistry::new(
            test_supervisor(),
            PodLifecycleConcurrencyConfig::production_default(),
            holder,
        ))
    }

    fn test_env<'a>(
        vars: &'a [(&'a str, &'a str)],
    ) -> impl Fn(&str) -> Result<String, std::env::VarError> + 'a {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key: &str| map.get(key).cloned().ok_or(std::env::VarError::NotPresent)
    }

    // ── env parsing ──

    #[test]
    fn env_unset_defaults_to_actor() {
        let env = test_env(&[]);
        let router = PodLifecycleRouter::from_env_impl(
            test_supervisor(),
            PodLifecycleConcurrencyConfig::production_default(),
            env,
        );
        assert_eq!(router.mode(), PodLifecycleRouteMode::Actor);
    }

    #[test]
    fn env_actor_selects_actor() {
        let env = test_env(&[("KLIGHTS_POD_LIFECYCLE_MODE", "actor")]);
        let router = PodLifecycleRouter::from_env_impl(
            test_supervisor(),
            PodLifecycleConcurrencyConfig::production_default(),
            env,
        );
        assert_eq!(router.mode(), PodLifecycleRouteMode::Actor);
    }

    #[test]
    fn env_actor_case_insensitive() {
        let env = test_env(&[("KLIGHTS_POD_LIFECYCLE_MODE", "ACTOR")]);
        let router = PodLifecycleRouter::from_env_impl(
            test_supervisor(),
            PodLifecycleConcurrencyConfig::production_default(),
            env,
        );
        assert_eq!(router.mode(), PodLifecycleRouteMode::Actor);
    }

    #[test]
    fn env_multiplex_selects_multiplex() {
        let env = test_env(&[("KLIGHTS_POD_LIFECYCLE_MODE", "multiplex")]);
        let router = PodLifecycleRouter::from_env_impl(
            test_supervisor(),
            PodLifecycleConcurrencyConfig::production_default(),
            env,
        );
        assert_eq!(router.mode(), PodLifecycleRouteMode::Multiplex);
    }

    #[test]
    fn env_multiplex_case_insensitive() {
        let env = test_env(&[("KLIGHTS_POD_LIFECYCLE_MODE", "MULTIPLEX")]);
        let router = PodLifecycleRouter::from_env_impl(
            test_supervisor(),
            PodLifecycleConcurrencyConfig::production_default(),
            env,
        );
        assert_eq!(router.mode(), PodLifecycleRouteMode::Multiplex);
    }

    #[test]
    fn env_invalid_falls_back_to_actor() {
        let env = test_env(&[("KLIGHTS_POD_LIFECYCLE_MODE", "foobar")]);
        let router = PodLifecycleRouter::from_env_impl(
            test_supervisor(),
            PodLifecycleConcurrencyConfig::production_default(),
            env,
        );
        assert_eq!(router.mode(), PodLifecycleRouteMode::Actor);
    }

    // ── actor backend integration ──

    #[tokio::test]
    async fn new_actor_router_routes_message_to_registry() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        router
            .route(LifecycleMessage::RetryDue { key })
            .await
            .unwrap();

        assert_eq!(router.active_pod_count().await, 1);
    }

    #[tokio::test]
    async fn actor_router_try_route_nonblocking_does_not_block() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

        router
            .route(LifecycleMessage::RetryDue { key: key.clone() })
            .await
            .unwrap();

        for _ in 0..10 {
            router.try_route_nonblocking(LifecycleMessage::RetryDue { key: key.clone() });
        }

        assert_eq!(router.active_pod_count().await, 1);
    }

    #[tokio::test]
    async fn actor_router_remove_pod_state_cleans_up() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        router
            .route(LifecycleMessage::RetryDue { key: key.clone() })
            .await
            .unwrap();
        assert_eq!(router.active_pod_count().await, 1);

        assert!(router.remove_pod_state(&key).await);
        assert_eq!(router.active_pod_count().await, 0);
    }

    #[tokio::test]
    async fn actor_router_remove_pod_state_is_idempotent() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        router
            .route(LifecycleMessage::RetryDue { key: key.clone() })
            .await
            .unwrap();

        assert!(router.remove_pod_state(&key).await);
        assert!(!router.remove_pod_state(&key).await);
    }

    #[tokio::test]
    async fn actor_router_diagnostics_returns_mode_and_states() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        router
            .route(LifecycleMessage::RetryDue { key: key.clone() })
            .await
            .unwrap();

        let diag = router.diagnostics().await;
        assert_eq!(diag.mode, PodLifecycleRouteMode::Actor);
        assert_eq!(diag.active_pod_count, 1);
        assert!(!diag.actor_states.is_empty());
        assert_eq!(diag.actor_states[0].namespace, "default");
        assert_eq!(diag.actor_states[0].name, "pod-a");
    }

    // ── multiplex backend (placeholder) ──

    #[tokio::test]
    async fn multiplex_router_returns_not_available_on_route() {
        let env = test_env(&[("KLIGHTS_POD_LIFECYCLE_MODE", "multiplex")]);
        let router = PodLifecycleRouter::from_env_impl(
            test_supervisor(),
            PodLifecycleConcurrencyConfig::production_default(),
            env,
        );

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        let result = router.route(LifecycleMessage::RetryDue { key }).await;
        assert!(result.is_err());
        assert_eq!(router.mode(), PodLifecycleRouteMode::Multiplex);
    }

    #[tokio::test]
    async fn multiplex_router_diagnostics_returns_empty() {
        let env = test_env(&[("KLIGHTS_POD_LIFECYCLE_MODE", "multiplex")]);
        let router = PodLifecycleRouter::from_env_impl(
            test_supervisor(),
            PodLifecycleConcurrencyConfig::production_default(),
            env,
        );

        let diag = router.diagnostics().await;
        assert_eq!(diag.mode, PodLifecycleRouteMode::Multiplex);
        assert!(diag.actor_states.is_empty());
        assert!(diag.recent_trace.is_empty());
    }

    /// Full contract: multiplex backend is a deliberate placeholder that
    /// returns `NotAvailable` for every route, never panics on nonblocking
    /// send, returns `false` for `remove_pod_state` (safe no-op), reports
    /// zero active pods, and outputs `Multiplex` in mode + diagnostics.
    #[tokio::test]
    async fn multiplex_backend_contract_reports_notavailable_without_runtime_calls() {
        let env = test_env(&[("KLIGHTS_POD_LIFECYCLE_MODE", "multiplex")]);
        let router = PodLifecycleRouter::from_env_impl(
            test_supervisor(),
            PodLifecycleConcurrencyConfig::production_default(),
            env,
        );

        // Mode selection works.
        assert_eq!(router.mode(), PodLifecycleRouteMode::Multiplex);

        // Route returns NotAvailable.
        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        let result = router
            .route(LifecycleMessage::WatchAdded {
                key: key.clone(),
                resource_version: Some(1),
                pod: serde_json::json!({"kind": "Pod"}),
            })
            .await;
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.to_ascii_lowercase().contains("not available"),
            "expected NotAvailable, got: {err_msg}"
        );

        // Nonblocking route does not panic.
        router.try_route_nonblocking(LifecycleMessage::RetryDue { key: key.clone() });

        // remove_pod_state is a safe no-op.
        assert!(!router.remove_pod_state(&key).await);

        // Active pod count and diagnostics report empty.
        assert_eq!(router.active_pod_count().await, 0);
        let diag = router.diagnostics().await;
        assert_eq!(diag.mode, PodLifecycleRouteMode::Multiplex);
        assert!(diag.actor_states.is_empty());
        assert!(diag.recent_trace.is_empty());
        assert_eq!(diag.active_pod_count, 0);
    }

    // ── router api does not expose backend types ──

    #[tokio::test]
    async fn router_api_does_not_expose_backend_types() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry);

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        let result = router.route(LifecycleMessage::RetryDue { key }).await;

        assert!(result.is_ok());
        assert_eq!(router.mode(), PodLifecycleRouteMode::Actor);
    }

    // ── lifecycle event type routing ──

    use crate::kubelet::cri_events::KubeletEventKind;
    use crate::kubelet::pod_lifecycle_core::message::{
        PodLifecycleWorkKind, PodProbeKind, PodProbeResult,
    };
    use serde_json::json;

    #[tokio::test]
    async fn router_routes_watch_added_event() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        let result = router
            .route(LifecycleMessage::WatchAdded {
                key,
                resource_version: Some(1),
                pod: json!({"kind": "Pod", "metadata": {"name": "pod-a", "namespace": "default", "uid": "uid-a"}}),
            })
            .await;
        assert!(result.is_ok());
        assert_eq!(router.active_pod_count().await, 1);
    }

    #[tokio::test]
    async fn router_routes_watch_modified_event() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        let result = router
            .route(LifecycleMessage::WatchModified {
                key,
                resource_version: Some(2),
                pod: json!({"kind": "Pod", "metadata": {"name": "pod-a", "namespace": "default", "uid": "uid-a"}}),
            })
            .await;
        assert!(result.is_ok());
        assert_eq!(router.active_pod_count().await, 1);
    }

    #[tokio::test]
    async fn router_routes_watch_deleted_with_cleanup() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

        router
            .route(LifecycleMessage::WatchAdded {
                key: key.clone(),
                resource_version: Some(1),
                pod: json!({"kind": "Pod"}),
            })
            .await
            .unwrap();
        assert_eq!(router.active_pod_count().await, 1);

        router
            .route(LifecycleMessage::WatchDeleted {
                key: key.clone(),
                resource_version: Some(2),
                pod: json!({"kind": "Pod"}),
            })
            .await
            .unwrap();

        assert!(router.remove_pod_state(&key).await);
        assert_eq!(router.active_pod_count().await, 0);
    }

    #[tokio::test]
    async fn router_routes_cri_event() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        let result = router
            .route(LifecycleMessage::CriEvent {
                key,
                container_id: "container-a".to_string(),
                kind: KubeletEventKind::Started,
            })
            .await;
        assert!(result.is_ok());
        assert_eq!(router.active_pod_count().await, 1);
    }

    #[tokio::test]
    async fn router_routes_lifecycle_command() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        let result = router
            .route(LifecycleMessage::LifecycleCommand {
                key,
                command: crate::kubelet::lifecycle::LifecycleCommand::ReadinessChanged {
                    pod_uid: "uid-a".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "pod-a".to_string(),
                    container_name: "container-a".to_string(),
                    ready: true,
                },
            })
            .await;
        assert!(result.is_ok());
        assert_eq!(router.active_pod_count().await, 1);
    }

    #[tokio::test]
    async fn router_routes_active_deadline_due() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        let result = router
            .route(LifecycleMessage::ActiveDeadlineDue { key })
            .await;
        assert!(result.is_ok());
        assert_eq!(router.active_pod_count().await, 1);
    }

    #[tokio::test]
    async fn router_routes_network_assigned() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        let result = router
            .route(LifecycleMessage::NetworkAssigned {
                key,
                sandbox_id: "sandbox-a".to_string(),
                pod_ip: "10.43.0.4".to_string(),
            })
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn router_routes_pod_work_completed() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        let result = router
            .route(LifecycleMessage::PodWorkCompleted {
                key,
                operation_id: 1,
                kind: PodLifecycleWorkKind::StartPod,
                sandbox_id: None,
            })
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn router_routes_probe_result() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        let result = router
            .route(LifecycleMessage::ProbeResult {
                key,
                probe_id: 1,
                container_name: "container-a".to_string(),
                kind: PodProbeKind::Readiness,
                result: PodProbeResult::Success,
            })
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn router_routes_multiple_distinct_pods_concurrently() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        for i in 0..5 {
            let key = PodLifecycleKey::new("default", &format!("pod-{i}"), &format!("uid-{i}"));
            router
                .route(LifecycleMessage::RetryDue { key })
                .await
                .unwrap();
        }

        assert_eq!(router.active_pod_count().await, 5);
    }

    #[tokio::test]
    async fn router_diagnostics_after_watch_deleted_events() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        router
            .route(LifecycleMessage::WatchAdded {
                key: key.clone(),
                resource_version: Some(1),
                pod: json!({"kind": "Pod"}),
            })
            .await
            .unwrap();
        router
            .route(LifecycleMessage::WatchDeleted {
                key: key.clone(),
                resource_version: Some(2),
                pod: json!({"kind": "Pod"}),
            })
            .await
            .unwrap();

        let diag = router.diagnostics().await;
        assert_eq!(diag.mode, PodLifecycleRouteMode::Actor);
        assert!(!diag.actor_states.is_empty());
    }

    // ── compile-time guard: actor and router import lifecycle types from core ──

    #[test]
    fn router_imports_lifecycle_types_from_core_not_actor() {
        // Compile-time check: this test module uses types from pod_lifecycle_core,
        // not pod_lifecycle_actor. Verify via static assertions on type paths.
        let _: PodLifecycleKey = PodLifecycleKey::new("ns", "pod", "uid");
        let _msg = LifecycleMessage::RetryDue {
            key: PodLifecycleKey::new("ns", "pod", "uid"),
        };
    }

    // ── reply handle ──

    #[tokio::test]
    async fn reply_handle_routes_through_backend() {
        let registry = test_registry(4);
        let router = PodLifecycleRouter::new_actor(registry.clone());

        let handle = router.reply_handle();
        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        let result = handle.route(LifecycleMessage::RetryDue { key }).await;
        assert!(result.is_ok());
        assert_eq!(router.active_pod_count().await, 1);
    }

    // ── Task 13.2: actor backend contract tests ──

    /// Routing `WatchAdded` through the actor backend dispatches a
    /// `CheckSlotAdmission` action to the configured executor with the
    /// correct namespace, name, and UID.
    #[tokio::test]
    async fn actor_backend_routes_uid_keyed_watch_added_to_configured_executor() {
        use crate::kubelet::pod_lifecycle_core::action::PodAction;
        use crate::kubelet::pod_lifecycle_router::executor::RecordingExecutor;

        let recorder = RecordingExecutor::new();
        let registry = test_registry_with_executor(recorder.clone());
        let router =
            PodLifecycleRouter::new_actor_with_executor(registry.clone(), recorder.clone());

        let test_uid = "uid-contract-1";
        let key = PodLifecycleKey::new("ns", "pod-contract", test_uid);
        let pod = serde_json::json!({
            "kind": "Pod",
            "metadata": {"name": "pod-contract", "namespace": "ns", "uid": test_uid},
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {}
        });

        router
            .route(LifecycleMessage::WatchAdded {
                key: key.clone(),
                resource_version: Some(1),
                pod,
            })
            .await
            .unwrap();

        // Wait for the actor to dispatch both ReconcileCriLeftovers and
        // CheckSlotAdmission (CheckSlotAdmission comes second, after the
        // restart-reconcile gate defers the WatchAdded).
        for _ in 0..1000 {
            if recorder.action_count() >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }

        let actions = recorder.take_actions();
        assert!(
            !actions.is_empty(),
            "executor must receive at least one action"
        );

        // The actor first dispatches ReconcileCriLeftovers (restart reconcile
        // gate), then CheckSlotAdmission (WatchAdded handler). Verify at least
        // one action carries the correct UID.
        let check = actions
            .iter()
            .find(|a| matches!(a, PodAction::CheckSlotAdmission { .. }));
        assert!(
            check.is_some(),
            "expected CheckSlotAdmission among recorded actions: {:?}",
            actions.iter().map(|a| a.task_name()).collect::<Vec<_>>()
        );
        let action_key = check.unwrap().key().expect("action must have key");
        assert_eq!(action_key.namespace, "ns");
        assert_eq!(action_key.name, "pod-contract");
        assert_eq!(
            action_key.uid, test_uid,
            "UID must be preserved through actor backend"
        );
    }

    #[tokio::test]
    async fn enqueue_orphan_finalize_is_idempotent_on_uid() {
        use crate::kubelet::pod_lifecycle_core::action::PodAction;
        use crate::kubelet::pod_lifecycle_router::executor::RecordingExecutor;

        let recorder = RecordingExecutor::new();
        let registry = test_registry_with_executor(recorder.clone());
        let router =
            PodLifecycleRouter::new_actor_with_executor(registry.clone(), recorder.clone());

        let key = PodLifecycleKey::new("ns", "orphan-pod", "uid-orphan");
        enqueue_orphan_finalize(&router, key.clone(), OrphanReason::LeaderDeletedWhileDown)
            .await
            .unwrap();
        enqueue_orphan_finalize(&router, key.clone(), OrphanReason::LeaderDeletedWhileDown)
            .await
            .unwrap();

        for _ in 0..1000 {
            if recorder.action_count() >= 1 {
                break;
            }
            tokio::task::yield_now().await;
        }

        let actions = recorder.take_actions();
        let stops = actions
            .iter()
            .filter(|action| {
                matches!(
                    action,
                    PodAction::StopPod {
                        key: action_key,
                        ..
                    } if action_key == &key
                )
            })
            .count();
        assert_eq!(
            stops, 1,
            "duplicate orphan finalization enqueue must not dispatch duplicate StopPod actions: {actions:?}"
        );
    }

    /// Routing `WatchAdded` through the actor backend sets up the actor
    /// so diagnostics and `active_pod_count` reflect the routed pod.
    /// `remove_pod_state` cleans up actor state and returns true.
    #[tokio::test]
    async fn actor_backend_routes_watch_added_to_configured_executor() {
        use crate::kubelet::pod_lifecycle_router::executor::RecordingExecutor;

        let recorder = RecordingExecutor::new();
        let registry = test_registry_with_executor(recorder.clone());
        let router =
            PodLifecycleRouter::new_actor_with_executor(registry.clone(), recorder.clone());

        let key = PodLifecycleKey::new("default", "pod-diag", "uid-diag");
        let pod = serde_json::json!({
            "kind": "Pod",
            "metadata": {"name": "pod-diag", "namespace": "default", "uid": "uid-diag"},
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {}
        });

        router
            .route(LifecycleMessage::WatchAdded {
                key: key.clone(),
                resource_version: Some(1),
                pod,
            })
            .await
            .unwrap();

        // Wait for the actor to process.
        for _ in 0..1000 {
            if recorder.action_count() > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }

        assert!(recorder.action_count() > 0, "executor must receive actions");

        // Diagnostics reflect the routed pod.
        assert_eq!(router.active_pod_count().await, 1);

        let diag = router.diagnostics().await;
        assert_eq!(diag.mode, PodLifecycleRouteMode::Actor);
        assert_eq!(diag.active_pod_count, 1);
        assert!(!diag.actor_states.is_empty());
        assert_eq!(diag.actor_states[0].namespace, "default");
        assert_eq!(diag.actor_states[0].name, "pod-diag");

        // remove_pod_state cleans up.
        assert!(router.remove_pod_state(&key).await);
        assert_eq!(router.active_pod_count().await, 0);
    }

    // ── Task 13.4: backend pluggability seam ──

    use crate::kubelet::pod_lifecycle_core::action::PodAction;
    use crate::kubelet::pod_runtime::service::{PodRuntimeKey, PodRuntimeService};
    use crate::kubelet::pod_runtime::test_support::MockPodRuntimeService;
    use tokio_util::sync::CancellationToken;

    /// Test-only backend that proves a third backend can be added without
    /// changing `PodWorkExecutor` or `PodRuntimeService`. On every
    /// `WatchAdded`, it immediately dispatches `StartPod` through the
    /// configured executor so the runtime port is exercised end-to-end.
    struct FakePodLifecycleBackend {
        executor_holder:
            std::sync::Arc<std::sync::Mutex<std::sync::Arc<dyn executor::PodWorkExecutor>>>,
    }

    impl FakePodLifecycleBackend {
        fn with_executor(
            executor: std::sync::Arc<dyn executor::PodWorkExecutor>,
        ) -> std::sync::Arc<dyn PodLifecycleRouteBackend> {
            std::sync::Arc::new(Self {
                executor_holder: std::sync::Arc::new(std::sync::Mutex::new(executor)),
            })
        }
    }

    #[async_trait::async_trait]
    impl PodLifecycleRouteBackend for FakePodLifecycleBackend {
        async fn route(&self, message: LifecycleMessage) -> Result<(), PodLifecycleRouteError> {
            if let LifecycleMessage::WatchAdded { key, pod, .. } = message {
                let executor = self.executor_holder.lock().unwrap().clone();
                let (tx, _rx) = tokio::sync::mpsc::channel(64);
                let reply_to = LifecycleReplyHandle::direct(tx);
                let action = PodAction::StartPod {
                    key,
                    pod: Some(pod),
                    operation_id: 1,
                    permit: None,
                };
                let _ = executor.dispatch(action, reply_to).await;
            }
            Ok(())
        }

        fn try_route_nonblocking(&self, _message: LifecycleMessage) {}

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

        fn set_work_executor(&self, executor: std::sync::Arc<dyn executor::PodWorkExecutor>) {
            *self.executor_holder.lock().unwrap() = executor;
        }

        fn executor_holder(
            &self,
        ) -> Option<std::sync::Arc<std::sync::Mutex<std::sync::Arc<dyn executor::PodWorkExecutor>>>>
        {
            Some(self.executor_holder.clone())
        }
    }

    /// Test-only executor that delegates `StartPod` to a `PodRuntimeService`.
    /// Proves the executor/runtime port contract is backend-agnostic.
    struct RuntimeDelegatingExecutor {
        runtime: std::sync::Arc<MockPodRuntimeService>,
    }

    #[async_trait::async_trait]
    impl executor::PodWorkExecutor for RuntimeDelegatingExecutor {
        async fn dispatch(
            &self,
            action: PodAction,
            _reply_to: LifecycleReplyHandle,
        ) -> Result<(), executor::ExecutorError> {
            if let PodAction::StartPod { key, pod, .. } = action {
                let runtime_key = PodRuntimeKey::from(&key);
                let _ = self
                    .runtime
                    .start_pod(runtime_key, pod, CancellationToken::new())
                    .await;
            }
            Ok(())
        }
    }

    /// Proves the `PodWorkExecutor` and `PodRuntimeService` traits can be
    /// driven by a non-actor backend without any trait signature changes.
    /// A hand-rolled `FakePodLifecycleBackend` routes a `WatchAdded` through
    /// a `RuntimeDelegatingExecutor` that delegates `StartPod` to a
    /// `MockPodRuntimeService`, and the mock must record the call with the
    /// same namespace, name, and UID.
    #[tokio::test]
    async fn lifecycle_backend_pluggability_seam() {
        let mock_runtime = std::sync::Arc::new(MockPodRuntimeService::new());
        let executor: std::sync::Arc<dyn executor::PodWorkExecutor> =
            std::sync::Arc::new(RuntimeDelegatingExecutor {
                runtime: mock_runtime.clone(),
            });
        let backend = FakePodLifecycleBackend::with_executor(executor);
        let router = PodLifecycleRouter { backend };

        let test_uid = "plug-uid-1";
        let key = PodLifecycleKey::new("ns", "plug-pod", test_uid);
        let pod = serde_json::json!({
            "kind": "Pod",
            "metadata": {"name": "plug-pod", "namespace": "ns", "uid": test_uid},
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {}
        });

        router
            .route(LifecycleMessage::WatchAdded {
                key: key.clone(),
                resource_version: Some(1),
                pod,
            })
            .await
            .unwrap();

        let calls = mock_runtime.recorded_calls();
        assert_eq!(calls.len(), 1, "runtime must receive exactly one call");
        match &calls[0] {
            super::super::pod_runtime::test_support::MockRuntimeCall::StartPod {
                namespace,
                name,
                uid,
                ..
            } => {
                assert_eq!(namespace, "ns");
                assert_eq!(name, "plug-pod");
                assert_eq!(uid, test_uid);
            }
            other => panic!("expected StartPod call, got {:?}", other),
        }
    }

    /// Contract matrix: actor backend handles lifecycle inputs through the
    /// registry; multiplex backend returns NotAvailable for all routes.
    /// Verifies both backends conform to the `PodLifecycleRouteBackend` trait
    /// with correct mode reporting and message acceptance/rejection.
    #[tokio::test]
    async fn lifecycle_backend_runtime_contract_matrix() {
        // ── Actor backend: verify message routing to registry ──
        let registry = test_registry(4);
        let actor_router = PodLifecycleRouter::new_actor(registry.clone());

        assert_eq!(actor_router.mode(), PodLifecycleRouteMode::Actor);

        let key = PodLifecycleKey::new("ns", "matrix-pod", "uid-matrix");
        let pod = serde_json::json!({
            "kind": "Pod",
            "metadata": {"namespace": "ns", "name": "matrix-pod", "uid": "uid-matrix"},
            "spec": {"containers": [{"name": "app", "image": "nginx"}]},
            "status": {"phase": "Pending"}
        });

        // WatchAdded routes successfully through actor backend.
        let result = actor_router
            .route(LifecycleMessage::WatchAdded {
                key: key.clone(),
                resource_version: Some(1),
                pod: pod.clone(),
            })
            .await;
        assert!(result.is_ok(), "actor must accept WatchAdded");

        // WatchModified routes successfully.
        let result = actor_router
            .route(LifecycleMessage::WatchModified {
                key: key.clone(),
                pod: pod.clone(),
                resource_version: Some(2),
            })
            .await;
        assert!(result.is_ok(), "actor must accept WatchModified");

        // RetryDue routes successfully.
        let result = actor_router
            .route(LifecycleMessage::RetryDue { key: key.clone() })
            .await;
        assert!(result.is_ok(), "actor must accept RetryDue");

        // Actor diagnostics report Actor mode.
        let diag = actor_router.diagnostics().await;
        assert_eq!(diag.mode, PodLifecycleRouteMode::Actor);

        // ── Multiplex backend: NotAvailable contract ──
        let env = test_env(&[("KLIGHTS_POD_LIFECYCLE_MODE", "multiplex")]);
        let mux_router = PodLifecycleRouter::from_env_impl(
            test_supervisor(),
            PodLifecycleConcurrencyConfig::production_default(),
            env,
        );

        assert_eq!(mux_router.mode(), PodLifecycleRouteMode::Multiplex);
        let mux_result = mux_router
            .route(LifecycleMessage::WatchAdded {
                key: key.clone(),
                resource_version: Some(1),
                pod,
            })
            .await;
        assert!(mux_result.is_err());
        let err_msg = format!("{}", mux_result.unwrap_err());
        assert!(
            err_msg.to_ascii_lowercase().contains("not available"),
            "multiplex must return NotAvailable, got: {err_msg}"
        );

        // Multiplex diagnostics report Multiplex mode with zero active pods.
        let diag = mux_router.diagnostics().await;
        assert_eq!(diag.mode, PodLifecycleRouteMode::Multiplex);
        assert_eq!(diag.active_pod_count, 0);
    }
}
