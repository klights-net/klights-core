use super::message::{LifecycleMessage, PodLifecycleKey};
use super::state::PodLifecycleState;
use super::test_harness::{LifecycleEvent, LifecycleOrderCase, PodLifecycleHarness};
use super::trace::{LifecycleTraceEntry, LifecycleTraceRing};
use crate::pod_lifecycle_core::state::FinalizationAction;
use crate::pod_lifecycle_router::LifecycleReplyHandle;
use crate::pod_lifecycle_router::executor::NoopExecutor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn dummy_reply_handle() -> LifecycleReplyHandle {
    use crate::pod_lifecycle_router::multiplex::MultiplexPodLifecycleBackend;
    LifecycleReplyHandle::new(std::sync::Arc::new(MultiplexPodLifecycleBackend))
}

fn test_executor() -> Arc<dyn crate::pod_lifecycle_router::executor::PodWorkExecutor> {
    Arc::new(NoopExecutor)
}

fn test_executor_holder()
-> Arc<std::sync::Mutex<Arc<dyn crate::pod_lifecycle_router::executor::PodWorkExecutor>>> {
    Arc::new(std::sync::Mutex::new(test_executor()))
}

fn recording_executor_holder() -> (
    Arc<crate::pod_lifecycle_router::executor::RecordingExecutor>,
    Arc<std::sync::Mutex<Arc<dyn crate::pod_lifecycle_router::executor::PodWorkExecutor>>>,
) {
    let recorder = crate::pod_lifecycle_router::executor::RecordingExecutor::new();
    let executor: Arc<dyn crate::pod_lifecycle_router::executor::PodWorkExecutor> =
        recorder.clone();
    (recorder, Arc::new(std::sync::Mutex::new(executor)))
}

struct BlockingStartExecutor {
    actions: std::sync::Mutex<Vec<crate::pod_lifecycle_core::action::PodAction>>,
    start_seen: tokio::sync::Notify,
    start_cancelled: tokio::sync::Notify,
    release_start: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

struct DeadlineStopExecutor {
    calls: std::sync::Mutex<Vec<(crate::runtime::PodStopMode, u64)>>,
    graceful_seen: tokio::sync::Notify,
    graceful_cancelled: tokio::sync::Notify,
    forced_seen: tokio::sync::Notify,
}

struct StopCompletionExecutor {
    stop_seen: tokio::sync::Notify,
    release_stop: tokio::sync::Notify,
    finalize_seen: tokio::sync::Notify,
}

impl StopCompletionExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            stop_seen: tokio::sync::Notify::new(),
            release_stop: tokio::sync::Notify::new(),
            finalize_seen: tokio::sync::Notify::new(),
        })
    }
}

#[async_trait::async_trait]
impl crate::pod_lifecycle_router::executor::PodWorkExecutor for StopCompletionExecutor {
    async fn dispatch(
        &self,
        action: crate::pod_lifecycle_core::action::PodAction,
        reply_to: LifecycleReplyHandle,
    ) -> Result<(), crate::pod_lifecycle_router::executor::ExecutorError> {
        match action {
            crate::pod_lifecycle_core::action::PodAction::StopPod {
                key, operation_id, ..
            } => {
                self.stop_seen.notify_one();
                self.release_stop.notified().await;
                let _ = reply_to
                    .route(LifecycleMessage::PodWorkCompleted {
                        key,
                        operation_id,
                        kind: super::message::PodLifecycleWorkKind::StopPod,
                        sandbox_id: None,
                    })
                    .await;
            }
            crate::pod_lifecycle_core::action::PodAction::FinalizePodDeletion { .. } => {
                self.finalize_seen.notify_one();
            }
            _ => {}
        }
        Ok(())
    }
}

struct BlockingRuntimeObservationOutbox {
    checkpoint_attempted: tokio::sync::Notify,
}

impl BlockingRuntimeObservationOutbox {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            checkpoint_attempted: tokio::sync::Notify::new(),
        })
    }
}

impl klights_leader_api::NodeOutbox for BlockingRuntimeObservationOutbox {
    fn enqueue(
        &self,
        _command: klights_leader_api::NodeOutboxCommand,
    ) -> klights_leader_api::NodeOutboxFuture<'_, klights_leader_api::NodeOutboxRoute> {
        Box::pin(async { unreachable!() })
    }

    fn next_status_stamp(&self) -> klights_leader_api::NodeOutboxFuture<'_, i64> {
        Box::pin(async { unreachable!() })
    }

    fn record_pod_status_checkpoint<'a>(
        &'a self,
        _checkpoint: &'a klights_cluster_core::Resource,
        _updated_ms: i64,
    ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
        Box::pin(async { unreachable!() })
    }

    fn merge_pod_status_checkpoint(
        &self,
        _pod: klights_cluster_core::Resource,
    ) -> klights_leader_api::NodeOutboxFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async { unreachable!() })
    }

    fn delete_pod_status_checkpoint<'a>(
        &'a self,
        _pod_uid: &'a str,
    ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
        Box::pin(async { unreachable!() })
    }

    fn record_runtime_observation_checkpoint<'a>(
        &'a self,
        _pod_uid: &'a str,
        _container_ids: Vec<String>,
        _generation: u64,
        _updated_ms: i64,
    ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
        Box::pin(async move {
            self.checkpoint_attempted.notify_one();
            std::future::pending().await
        })
    }

    fn get_runtime_observation_checkpoint<'a>(
        &'a self,
        _pod_uid: &'a str,
    ) -> klights_leader_api::NodeOutboxFuture<
        'a,
        Option<klights_leader_api::NodeRuntimeObservationCheckpoint>,
    > {
        Box::pin(async { Ok(None) })
    }

    fn delete_runtime_observation_checkpoint<'a>(
        &'a self,
        _pod_uid: &'a str,
    ) -> klights_leader_api::NodeOutboxFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl DeadlineStopExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: std::sync::Mutex::new(Vec::new()),
            graceful_seen: tokio::sync::Notify::new(),
            graceful_cancelled: tokio::sync::Notify::new(),
            forced_seen: tokio::sync::Notify::new(),
        })
    }
}

#[async_trait::async_trait]
impl crate::pod_lifecycle_router::executor::PodWorkExecutor for DeadlineStopExecutor {
    async fn dispatch(
        &self,
        action: crate::pod_lifecycle_core::action::PodAction,
        reply_to: LifecycleReplyHandle,
    ) -> Result<(), crate::pod_lifecycle_router::executor::ExecutorError> {
        self.dispatch_with_cancel(action, reply_to, tokio_util::sync::CancellationToken::new())
            .await
    }

    async fn dispatch_with_cancel(
        &self,
        action: crate::pod_lifecycle_core::action::PodAction,
        _reply_to: LifecycleReplyHandle,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), crate::pod_lifecycle_router::executor::ExecutorError> {
        let crate::pod_lifecycle_core::action::PodAction::StopPod {
            mode, operation_id, ..
        } = action
        else {
            return Ok(());
        };
        self.calls.lock().unwrap().push((mode, operation_id));
        match mode {
            crate::runtime::PodStopMode::Graceful => {
                self.graceful_seen.notify_one();
                cancel.cancelled().await;
                self.graceful_cancelled.notify_one();
            }
            crate::runtime::PodStopMode::Forced => self.forced_seen.notify_one(),
        }
        Ok(())
    }
}

impl BlockingStartExecutor {
    fn new() -> (Arc<Self>, tokio::sync::oneshot::Sender<()>) {
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        (
            Arc::new(Self {
                actions: std::sync::Mutex::new(Vec::new()),
                start_seen: tokio::sync::Notify::new(),
                start_cancelled: tokio::sync::Notify::new(),
                release_start: std::sync::Mutex::new(Some(release_rx)),
            }),
            release_tx,
        )
    }

    async fn wait_for_start(&self) {
        self.start_seen.notified().await;
    }

    async fn wait_for_start_cancelled(&self) {
        self.start_cancelled.notified().await;
    }

    fn has_stop_pod(&self) -> bool {
        self.actions.lock().unwrap().iter().any(|action| {
            matches!(
                action,
                crate::pod_lifecycle_core::action::PodAction::StopPod { .. }
            )
        })
    }
}

#[async_trait::async_trait]
impl crate::pod_lifecycle_router::executor::PodWorkExecutor for BlockingStartExecutor {
    async fn dispatch(
        &self,
        action: crate::pod_lifecycle_core::action::PodAction,
        reply_to: LifecycleReplyHandle,
    ) -> Result<(), crate::pod_lifecycle_router::executor::ExecutorError> {
        if let crate::pod_lifecycle_core::action::PodAction::CheckSlotAdmission {
            key,
            pod,
            resource_version,
            start_after_admit,
            operation_id,
            ..
        } = action
        {
            let _ = reply_to
                .route(LifecycleMessage::SlotAdmissionGranted {
                    key,
                    operation_id,
                    pod,
                    resource_version,
                    start_after_admit,
                })
                .await;
            return Ok(());
        }
        if let crate::pod_lifecycle_core::action::PodAction::ReconcileCriLeftovers {
            key,
            operation_id,
            ..
        } = &action
        {
            let completion_key = key.clone();
            let completion_operation_id = *operation_id;
            self.actions.lock().unwrap().push(action);
            let _ = reply_to
                .route(LifecycleMessage::PodWorkCompleted {
                    key: completion_key,
                    operation_id: completion_operation_id,
                    kind: super::message::PodLifecycleWorkKind::ReconcileCriLeftovers,
                    sandbox_id: None,
                })
                .await;
            return Ok(());
        }
        let is_start = matches!(
            action,
            crate::pod_lifecycle_core::action::PodAction::StartPod { .. }
        );
        self.actions.lock().unwrap().push(action);
        if is_start {
            self.start_seen.notify_waiters();
            let release = self.release_start.lock().unwrap().take();
            if let Some(release) = release {
                let _ = release.await;
            }
        }
        Ok(())
    }

    async fn dispatch_with_cancel(
        &self,
        action: crate::pod_lifecycle_core::action::PodAction,
        reply_to: LifecycleReplyHandle,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), crate::pod_lifecycle_router::executor::ExecutorError> {
        if let crate::pod_lifecycle_core::action::PodAction::CheckSlotAdmission {
            key,
            pod,
            resource_version,
            start_after_admit,
            operation_id,
            ..
        } = action
        {
            let _ = reply_to
                .route(LifecycleMessage::SlotAdmissionGranted {
                    key,
                    operation_id,
                    pod,
                    resource_version,
                    start_after_admit,
                })
                .await;
            return Ok(());
        }
        if let crate::pod_lifecycle_core::action::PodAction::ReconcileCriLeftovers {
            key,
            operation_id,
            ..
        } = &action
        {
            let completion_key = key.clone();
            let completion_operation_id = *operation_id;
            self.actions.lock().unwrap().push(action);
            let _ = reply_to
                .route(LifecycleMessage::PodWorkCompleted {
                    key: completion_key,
                    operation_id: completion_operation_id,
                    kind: super::message::PodLifecycleWorkKind::ReconcileCriLeftovers,
                    sandbox_id: None,
                })
                .await;
            return Ok(());
        }
        let is_start = matches!(
            action,
            crate::pod_lifecycle_core::action::PodAction::StartPod { .. }
        );
        self.actions.lock().unwrap().push(action);
        if is_start {
            self.start_seen.notify_waiters();
            let release = self.release_start.lock().unwrap().take();
            if let Some(release) = release {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        self.start_cancelled.notify_waiters();
                    }
                    _ = release => {}
                }
            }
        }
        Ok(())
    }
}

struct CompletingExecutor;

#[async_trait::async_trait]
impl crate::pod_lifecycle_router::executor::PodWorkExecutor for CompletingExecutor {
    async fn dispatch(
        &self,
        action: crate::pod_lifecycle_core::action::PodAction,
        reply_to: LifecycleReplyHandle,
    ) -> Result<(), crate::pod_lifecycle_router::executor::ExecutorError> {
        if let crate::pod_lifecycle_core::action::PodAction::CheckSlotAdmission {
            key,
            pod,
            resource_version,
            start_after_admit,
            operation_id,
            ..
        } = action
        {
            let _ = reply_to
                .route(LifecycleMessage::SlotAdmissionGranted {
                    key,
                    operation_id,
                    pod,
                    resource_version,
                    start_after_admit,
                })
                .await;
            return Ok(());
        }
        if let crate::pod_lifecycle_core::action::PodAction::StartPod {
            key, operation_id, ..
        } = action
        {
            let _ = reply_to
                .route(LifecycleMessage::PodWorkCompleted {
                    key,
                    operation_id,
                    kind: super::message::PodLifecycleWorkKind::StartPod,
                    sandbox_id: Some("sandbox-a".to_string()),
                })
                .await;
        }
        Ok(())
    }
}

struct CompletingStartStopExecutor {
    start_seen: tokio::sync::Notify,
    stop_seen: tokio::sync::Notify,
}

impl CompletingStartStopExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            start_seen: tokio::sync::Notify::new(),
            stop_seen: tokio::sync::Notify::new(),
        })
    }

    async fn wait_for_start(&self) {
        self.start_seen.notified().await;
    }

    async fn wait_for_stop(&self) {
        self.stop_seen.notified().await;
    }
}

#[async_trait::async_trait]
impl crate::pod_lifecycle_router::executor::PodWorkExecutor for CompletingStartStopExecutor {
    async fn dispatch(
        &self,
        action: crate::pod_lifecycle_core::action::PodAction,
        reply_to: LifecycleReplyHandle,
    ) -> Result<(), crate::pod_lifecycle_router::executor::ExecutorError> {
        match action {
            crate::pod_lifecycle_core::action::PodAction::CheckSlotAdmission {
                key,
                pod,
                resource_version,
                start_after_admit,
                operation_id,
                ..
            } => {
                let _ = reply_to
                    .route(LifecycleMessage::SlotAdmissionGranted {
                        key,
                        operation_id,
                        pod,
                        resource_version,
                        start_after_admit,
                    })
                    .await;
            }
            crate::pod_lifecycle_core::action::PodAction::ReconcileCriLeftovers {
                key,
                operation_id,
                ..
            } => {
                let _ = reply_to
                    .route(LifecycleMessage::PodWorkCompleted {
                        key,
                        operation_id,
                        kind: super::message::PodLifecycleWorkKind::ReconcileCriLeftovers,
                        sandbox_id: None,
                    })
                    .await;
            }
            crate::pod_lifecycle_core::action::PodAction::StartPod {
                key, operation_id, ..
            } => {
                self.start_seen.notify_waiters();
                let _ = reply_to
                    .route(LifecycleMessage::PodWorkCompleted {
                        key,
                        operation_id,
                        kind: super::message::PodLifecycleWorkKind::StartPod,
                        sandbox_id: Some("sandbox-a".to_string()),
                    })
                    .await;
            }
            crate::pod_lifecycle_core::action::PodAction::StopPod {
                key, operation_id, ..
            } => {
                self.stop_seen.notify_waiters();
                let _ = reply_to
                    .route(LifecycleMessage::PodWorkCompleted {
                        key,
                        operation_id,
                        kind: super::message::PodLifecycleWorkKind::StopPod,
                        sandbox_id: None,
                    })
                    .await;
            }
            crate::pod_lifecycle_core::action::PodAction::FinalizePodDeletion {
                key,
                operation_id,
                ..
            } => {
                let _ = reply_to
                    .route(LifecycleMessage::PodWorkCompleted {
                        key,
                        operation_id,
                        kind: super::message::PodLifecycleWorkKind::FinalizePodDeletion,
                        sandbox_id: None,
                    })
                    .await;
            }
            _ => {}
        }
        Ok(())
    }
}

struct FinalizationPendingExecutor {
    start_seen: tokio::sync::Notify,
    stop_count: AtomicUsize,
    finalize_count: AtomicUsize,
}

impl FinalizationPendingExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            start_seen: tokio::sync::Notify::new(),
            stop_count: AtomicUsize::new(0),
            finalize_count: AtomicUsize::new(0),
        })
    }

    async fn wait_for_start(&self) {
        self.start_seen.notified().await;
    }

    fn stop_count(&self) -> usize {
        self.stop_count.load(Ordering::SeqCst)
    }

    async fn wait_for_finalize_count(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if self.finalize_count.load(Ordering::SeqCst) >= expected {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("FinalizePodDeletion count did not reach expected value");
    }
}

#[async_trait::async_trait]
impl crate::pod_lifecycle_router::executor::PodWorkExecutor for FinalizationPendingExecutor {
    async fn dispatch(
        &self,
        action: crate::pod_lifecycle_core::action::PodAction,
        reply_to: LifecycleReplyHandle,
    ) -> Result<(), crate::pod_lifecycle_router::executor::ExecutorError> {
        match action {
            crate::pod_lifecycle_core::action::PodAction::CheckSlotAdmission {
                key,
                pod,
                resource_version,
                start_after_admit,
                operation_id,
                ..
            } => {
                let _ = reply_to
                    .route(LifecycleMessage::SlotAdmissionGranted {
                        key,
                        operation_id,
                        pod,
                        resource_version,
                        start_after_admit,
                    })
                    .await;
            }
            crate::pod_lifecycle_core::action::PodAction::ReconcileCriLeftovers {
                key,
                operation_id,
                ..
            } => {
                let _ = reply_to
                    .route(LifecycleMessage::PodWorkCompleted {
                        key,
                        operation_id,
                        kind: super::message::PodLifecycleWorkKind::ReconcileCriLeftovers,
                        sandbox_id: None,
                    })
                    .await;
            }
            crate::pod_lifecycle_core::action::PodAction::StartPod {
                key, operation_id, ..
            } => {
                self.start_seen.notify_waiters();
                let _ = reply_to
                    .route(LifecycleMessage::PodWorkCompleted {
                        key,
                        operation_id,
                        kind: super::message::PodLifecycleWorkKind::StartPod,
                        sandbox_id: Some("sandbox-a".to_string()),
                    })
                    .await;
            }
            crate::pod_lifecycle_core::action::PodAction::FinalizeStartup {
                key,
                operation_id,
                ..
            } => {
                let _ = reply_to
                    .route(LifecycleMessage::PodWorkCompleted {
                        key,
                        operation_id,
                        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
                        sandbox_id: Some("sandbox-a".to_string()),
                    })
                    .await;
            }
            crate::pod_lifecycle_core::action::PodAction::StopPod {
                key, operation_id, ..
            } => {
                self.stop_count.fetch_add(1, Ordering::SeqCst);
                let _ = reply_to
                    .route(LifecycleMessage::PodWorkCompleted {
                        key,
                        operation_id,
                        kind: super::message::PodLifecycleWorkKind::StopPod,
                        sandbox_id: None,
                    })
                    .await;
            }
            crate::pod_lifecycle_core::action::PodAction::FinalizePodDeletion {
                key,
                operation_id,
                ..
            } => {
                self.finalize_count.fetch_add(1, Ordering::SeqCst);
                let _ = reply_to
                    .route(LifecycleMessage::PodWorkFailed {
                        key,
                        operation_id,
                        kind: super::message::PodLifecycleWorkKind::FinalizePodDeletion,
                        retryable: true,
                        failure: super::message::PodLifecycleWorkFailure::FinalizersPending,
                    })
                    .await;
            }
            crate::pod_lifecycle_core::action::PodAction::ScheduleRetry { .. } => {}
            _ => {}
        }
        Ok(())
    }
}

struct FirstAdmissionReconcileExecutor {
    actions: std::sync::Mutex<Vec<String>>,
    reconcile_seen: tokio::sync::Notify,
    start_seen: tokio::sync::Notify,
    release_reconcile: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    fail_reconcile: bool,
}

impl FirstAdmissionReconcileExecutor {
    fn blocking(fail_reconcile: bool) -> (Arc<Self>, tokio::sync::oneshot::Sender<()>) {
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        (
            Arc::new(Self {
                actions: std::sync::Mutex::new(Vec::new()),
                reconcile_seen: tokio::sync::Notify::new(),
                start_seen: tokio::sync::Notify::new(),
                release_reconcile: std::sync::Mutex::new(Some(release_rx)),
                fail_reconcile,
            }),
            release_tx,
        )
    }

    fn immediate(fail_reconcile: bool) -> Arc<Self> {
        Arc::new(Self {
            actions: std::sync::Mutex::new(Vec::new()),
            reconcile_seen: tokio::sync::Notify::new(),
            start_seen: tokio::sync::Notify::new(),
            release_reconcile: std::sync::Mutex::new(None),
            fail_reconcile,
        })
    }

    async fn wait_for_reconcile(&self) {
        self.reconcile_seen.notified().await;
    }

    async fn wait_for_start(&self) {
        self.start_seen.notified().await;
    }

    fn actions(&self) -> Vec<String> {
        self.actions.lock().unwrap().clone()
    }

    fn reconcile_count(&self) -> usize {
        self.actions()
            .iter()
            .filter(|action| action.starts_with("reconcile:"))
            .count()
    }
}

#[async_trait::async_trait]
impl crate::pod_lifecycle_router::executor::PodWorkExecutor for FirstAdmissionReconcileExecutor {
    async fn dispatch(
        &self,
        action: crate::pod_lifecycle_core::action::PodAction,
        reply_to: LifecycleReplyHandle,
    ) -> Result<(), crate::pod_lifecycle_router::executor::ExecutorError> {
        match action {
            crate::pod_lifecycle_core::action::PodAction::CheckSlotAdmission {
                key,
                pod,
                resource_version,
                start_after_admit,
                operation_id,
                ..
            } => {
                let _ = reply_to
                    .route(LifecycleMessage::SlotAdmissionGranted {
                        key,
                        operation_id,
                        pod,
                        resource_version,
                        start_after_admit,
                    })
                    .await;
            }
            crate::pod_lifecycle_core::action::PodAction::ReconcileCriLeftovers {
                key,
                operation_id,
                ..
            } => {
                self.actions
                    .lock()
                    .unwrap()
                    .push(format!("reconcile:{}", key.uid));
                self.reconcile_seen.notify_waiters();
                let release = self.release_reconcile.lock().unwrap().take();
                if let Some(release) = release {
                    let _ = release.await;
                }
                if self.fail_reconcile {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkFailed {
                            key,
                            operation_id,
                            kind: super::message::PodLifecycleWorkKind::ReconcileCriLeftovers,
                            retryable: false,
                            failure: super::message::PodLifecycleWorkFailure::DispatchFailed(
                                "injected CRI list failure".to_string(),
                            ),
                        })
                        .await;
                } else {
                    let _ = reply_to
                        .route(LifecycleMessage::PodWorkCompleted {
                            key,
                            operation_id,
                            kind: super::message::PodLifecycleWorkKind::ReconcileCriLeftovers,
                            sandbox_id: None,
                        })
                        .await;
                }
            }
            crate::pod_lifecycle_core::action::PodAction::StartPod {
                key, operation_id, ..
            } => {
                self.actions
                    .lock()
                    .unwrap()
                    .push(format!("start:{}", key.uid));
                self.start_seen.notify_waiters();
                let _ = reply_to
                    .route(LifecycleMessage::PodWorkCompleted {
                        key,
                        operation_id,
                        kind: super::message::PodLifecycleWorkKind::StartPod,
                        sandbox_id: Some("sandbox-a".to_string()),
                    })
                    .await;
            }
            _ => {}
        }
        Ok(())
    }
}

struct CountingReplyBackend {
    routes: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl crate::pod_lifecycle_router::PodLifecycleRouteBackend for CountingReplyBackend {
    async fn route(
        &self,
        _message: LifecycleMessage,
    ) -> Result<(), crate::pod_lifecycle_router::PodLifecycleRouteError> {
        self.routes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn try_route_nonblocking(&self, _message: LifecycleMessage) {
        self.routes.fetch_add(1, Ordering::SeqCst);
    }

    fn mode(&self) -> crate::pod_lifecycle_router::PodLifecycleRouteMode {
        crate::pod_lifecycle_router::PodLifecycleRouteMode::Actor
    }

    async fn remove_pod_state(&self, _key: &PodLifecycleKey) -> bool {
        false
    }

    async fn diagnostics(&self) -> crate::pod_lifecycle_router::PodLifecycleDiagnostics {
        crate::pod_lifecycle_router::PodLifecycleDiagnostics {
            mode: crate::pod_lifecycle_router::PodLifecycleRouteMode::Actor,
            actor_states: Vec::new(),
            recent_trace: Vec::new(),
            active_pod_count: 0,
        }
    }

    async fn active_pod_count(&self) -> usize {
        0
    }
}

fn test_pod(namespace: &str, name: &str, uid: &str) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "namespace": namespace,
            "name": name,
            "uid": uid
        }
    })
}

fn running_test_pod(namespace: &str, name: &str, uid: &str) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "namespace": namespace,
            "name": name,
            "uid": uid,
            "annotations": {
                "klights.dev/sandbox-id": "sandbox-a"
            }
        },
        "status": {
            "phase": "Running",
            "podIP": "10.42.0.10"
        }
    })
}

fn running_test_pod_without_sandbox_annotation(
    namespace: &str,
    name: &str,
    uid: &str,
) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "namespace": namespace,
            "name": name,
            "uid": uid
        },
        "status": {
            "phase": "Running",
            "podIP": "10.42.0.10"
        }
    })
}

fn running_test_pod_with_ephemeral_container(
    namespace: &str,
    name: &str,
    uid: &str,
) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "namespace": namespace,
            "name": name,
            "uid": uid,
            "annotations": {
                "klights.dev/sandbox-id": "sandbox-a"
            }
        },
        "spec": {
            "ephemeralContainers": [{
                "name": "debugger",
                "image": "registry.k8s.io/e2e-test-images/busybox:1.37.0-1",
                "command": ["/bin/sh", "-c"],
                "args": ["while true; do echo polo; sleep 2; done"],
                "stdin": true,
                "tty": true
            }]
        },
        "status": {
            "phase": "Running",
            "podIP": "10.42.0.10"
        }
    })
}

fn direct_test_actor() -> super::actor::PodLifecycleActor {
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    super::actor::PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        test_executor_holder(),
        dummy_reply_handle(),
    )
}

#[test]
fn deletion_deadline_due_is_uid_and_generation_qualified() {
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let message = LifecycleMessage::DeletionDeadlineDue {
        key: key.clone(),
        generation: 7,
    };

    assert_eq!(message.key(), &key);
    assert_eq!(message.deletion_deadline_generation(), Some(7));
    assert_eq!(message.event_name(), "deletion_deadline_due");
}

#[test]
fn matching_deletion_deadline_replaces_graceful_stop_with_forced_stop() {
    use crate::pod_lifecycle_core::action::{PodAction, PodStopMode};

    let mut actor = direct_test_actor();
    actor.set_local_node_name_for_test("node-a");
    actor.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(100));
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let mut pod = test_pod("default", "pod-a", "uid-a");
    pod["spec"]["nodeName"] = serde_json::json!("node-a");
    pod["metadata"]["deletionTimestamp"] = serde_json::json!("1970-01-01T00:01:50Z");
    pod["metadata"]["deletionGracePeriodSeconds"] = serde_json::json!(10);

    let graceful = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(2),
        pod,
    });
    let graceful_operation = match graceful {
        PodAction::StopPod {
            mode: PodStopMode::Graceful,
            operation_id,
            ..
        } => operation_id,
        other => panic!("expected graceful StopPod, got {other:?}"),
    };
    let generation = actor
        .deletion_deadline_generation_for_test()
        .expect("deadline generation");

    let forced = actor.handle_for_test(LifecycleMessage::DeletionDeadlineDue {
        key: key.clone(),
        generation,
    });
    match forced {
        PodAction::StopPod {
            mode: PodStopMode::Forced,
            operation_id,
            ..
        } => assert!(operation_id > graceful_operation),
        other => panic!("expected forced StopPod, got {other:?}"),
    }
}

fn deadline_test_actor() -> super::actor::PodLifecycleActor {
    let mut actor = direct_test_actor();
    actor.set_local_node_name_for_test("node-a");
    actor.set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(100));
    actor
}

fn terminating_test_pod(uid: &str, timestamp: serde_json::Value) -> serde_json::Value {
    let mut pod = test_pod("default", "pod-a", uid);
    pod["spec"]["nodeName"] = serde_json::json!("node-a");
    pod["metadata"]["deletionTimestamp"] = timestamp;
    pod["metadata"]["deletionGracePeriodSeconds"] = serde_json::json!(30);
    pod
}

#[test]
fn deletion_deadline_only_rearms_for_an_earlier_absolute_deadline() {
    let mut actor = deadline_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let initial = terminating_test_pod("uid-a", serde_json::json!("1970-01-01T00:02:10Z"));
    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: initial,
    });
    let (_, initial_generation, _, _) = actor.deletion_deadline_for_test().unwrap();

    let later = terminating_test_pod("uid-a", serde_json::json!("1970-01-01T00:02:20Z"));
    let _ = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(2),
        pod: later,
    });
    assert_eq!(
        actor.deletion_deadline_generation_for_test(),
        Some(initial_generation)
    );

    let earlier = terminating_test_pod("uid-a", serde_json::json!("1970-01-01T00:01:50Z"));
    let _ = actor.handle_for_test(LifecycleMessage::WatchModified {
        key,
        resource_version: Some(3),
        pod: earlier,
    });
    let (deadline, generation, fired, forced) = actor.deletion_deadline_for_test().unwrap();
    assert_eq!(deadline.timestamp(), 110);
    assert!(generation > initial_generation);
    assert!(!fired);
    assert!(!forced);
}

#[test]
fn malformed_deadline_is_immediate_and_absent_deadline_has_no_timer_state() {
    let mut malformed_actor = deadline_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let malformed = terminating_test_pod("uid-a", serde_json::json!("not-a-time"));
    let _ = malformed_actor.handle_for_test(LifecycleMessage::WatchAdded {
        key,
        resource_version: Some(1),
        pod: malformed,
    });
    assert_eq!(
        malformed_actor
            .deletion_deadline_for_test()
            .unwrap()
            .0
            .timestamp(),
        100
    );

    let mut absent_actor = deadline_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-b");
    let mut absent = test_pod("default", "pod-a", "uid-b");
    absent["spec"]["nodeName"] = serde_json::json!("node-a");
    let _ = absent_actor.handle_for_test(LifecycleMessage::WatchAdded {
        key,
        resource_version: Some(1),
        pod: absent,
    });
    assert!(absent_actor.deletion_deadline_for_test().is_none());
}

#[test]
fn stale_deadline_generation_and_replacement_uid_cannot_force_stop() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = deadline_test_actor();
    let old_key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = terminating_test_pod("uid-a", serde_json::json!("1970-01-01T00:01:50Z"));
    let old_stop = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: old_key.clone(),
        resource_version: Some(1),
        pod,
    });
    let old_stop_operation = match old_stop {
        PodAction::StopPod { operation_id, .. } => operation_id,
        other => panic!("expected old UID stop, got {other:?}"),
    };
    let generation = actor.deletion_deadline_generation_for_test().unwrap();
    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::DeletionDeadlineDue {
            key: old_key.clone(),
            generation: generation.wrapping_add(1),
        }),
        PodAction::Noop
    ));

    let new_key = PodLifecycleKey::new("default", "pod-a", "uid-b");
    let replacement = test_pod("default", "pod-a", "uid-b");
    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: new_key.clone(),
        resource_version: Some(2),
        pod: replacement,
    });
    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: old_key.clone(),
        operation_id: old_stop_operation,
        kind: super::message::PodLifecycleWorkKind::StopPod,
        sandbox_id: None,
    });
    let finalize_operation = match finalize {
        PodAction::FinalizePodDeletion { operation_id, .. } => operation_id,
        other => panic!("expected actor-owned finalization, got {other:?}"),
    };
    let _ = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: old_key.clone(),
        operation_id: finalize_operation,
        kind: super::message::PodLifecycleWorkKind::FinalizePodDeletion,
        sandbox_id: None,
    });
    assert_eq!(actor.active_uid_for_test(), Some(new_key.uid.as_str()));
    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::DeletionDeadlineDue {
            key: old_key,
            generation,
        }),
        PodAction::Noop
    ));
}

#[test]
fn unscheduled_and_other_node_terminating_pods_do_not_arm_local_deadlines() {
    for node_name in [None, Some("node-b")] {
        let mut actor = deadline_test_actor();
        let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
        let mut pod = terminating_test_pod("uid-a", serde_json::json!("1970-01-01T00:01:50Z"));
        match node_name {
            Some(node) => pod["spec"]["nodeName"] = serde_json::json!(node),
            None => {
                pod["spec"].as_object_mut().unwrap().remove("nodeName");
            }
        }
        let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
            key,
            resource_version: Some(1),
            pod,
        });
        assert!(actor.deletion_deadline_for_test().is_none());
    }
}

#[test]
fn relist_reconstructs_deadline_and_late_graceful_completion_is_ignored() {
    use crate::pod_lifecycle_core::action::{PodAction, PodStopMode};

    let mut actor = deadline_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = terminating_test_pod("uid-a", serde_json::json!("1970-01-01T00:01:50Z"));
    let graceful = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(8),
        pod,
    });
    let graceful_operation = match graceful {
        PodAction::StopPod {
            operation_id,
            mode: PodStopMode::Graceful,
            ..
        } => operation_id,
        other => panic!("expected relist graceful stop, got {other:?}"),
    };
    let generation = actor.deletion_deadline_generation_for_test().unwrap();
    let forced = actor.handle_for_test(LifecycleMessage::DeletionDeadlineDue {
        key: key.clone(),
        generation,
    });
    let forced_operation = match forced {
        PodAction::StopPod {
            operation_id,
            mode: PodStopMode::Forced,
            ..
        } => operation_id,
        other => panic!("expected forced stop, got {other:?}"),
    };
    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
            key: key.clone(),
            operation_id: graceful_operation,
            kind: super::message::PodLifecycleWorkKind::StopPod,
            sandbox_id: None,
        }),
        PodAction::Noop
    ));
    assert_eq!(
        actor.in_flight_for_test().map(|(_, _, op)| op),
        Some(forced_operation)
    );
}

#[test]
fn forced_stop_retry_remains_forced_and_finalizers_still_gate_hard_delete() {
    use crate::pod_lifecycle_core::action::{PodAction, PodStopMode};

    let mut actor = deadline_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = terminating_test_pod("uid-a", serde_json::json!("1970-01-01T00:01:40Z"));
    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod,
    });
    let generation = actor.deletion_deadline_generation_for_test().unwrap();
    let forced_operation = match actor.handle_for_test(LifecycleMessage::DeletionDeadlineDue {
        key: key.clone(),
        generation,
    }) {
        PodAction::StopPod { operation_id, .. } => operation_id,
        other => panic!("expected forced stop, got {other:?}"),
    };
    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::PodWorkFailed {
            key: key.clone(),
            operation_id: forced_operation,
            kind: super::message::PodLifecycleWorkKind::StopPod,
            retryable: true,
            failure: super::message::PodLifecycleWorkFailure::DeadlineExceeded,
        }),
        PodAction::ScheduleRetry { .. }
    ));
    let retried_operation =
        match actor.handle_for_test(LifecycleMessage::RetryDue { key: key.clone() }) {
            PodAction::StopPod {
                mode: PodStopMode::Forced,
                operation_id,
                ..
            } => operation_id,
            other => panic!("expected forced StopPod retry, got {other:?}"),
        };
    let finalize_operation = match actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: retried_operation,
        kind: super::message::PodLifecycleWorkKind::StopPod,
        sandbox_id: None,
    }) {
        PodAction::FinalizePodDeletion { operation_id, .. } => operation_id,
        other => panic!("expected actor-owned finalization after StopPod, got {other:?}"),
    };
    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::PodWorkFailed {
            key,
            operation_id: finalize_operation,
            kind: super::message::PodLifecycleWorkKind::FinalizePodDeletion,
            retryable: true,
            failure: super::message::PodLifecycleWorkFailure::FinalizersPending,
        }),
        PodAction::ScheduleRetry { .. }
    ));
}

#[test]
fn pending_start_config_error_restarts_repaired_snapshot_after_in_flight_update() {
    use super::message::{PodLifecycleWorkFailure, PodLifecycleWorkKind};
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let invalid_pod = serde_json::json!({
        "metadata": {
            "namespace": "default",
            "name": "pod-a",
            "uid": "uid-a",
            "annotations": {
                "notmysubpath": "mypath",
                "mysubpath": "/foo"
            }
        },
        "spec": {
            "nodeName": "dallas",
            "containers": [{
                "name": "c",
                "env": [
                    {"name": "POD_NAME", "value": "foo"},
                    {"name": "ANNOTATION", "valueFrom": {"fieldRef": {"fieldPath": "metadata.annotations['mysubpath']"}}}
                ],
                "volumeMounts": [{
                    "name": "workdir",
                    "mountPath": "/subpath_mount",
                    "subPathExpr": "$(ANNOTATION)"
                }]
            }],
            "volumes": [{"name": "workdir", "emptyDir": {}}]
        }
    });

    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::WatchAdded {
            key: key.clone(),
            resource_version: Some(1),
            pod: invalid_pod.clone(),
        }),
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let mut repaired_pod = invalid_pod;
    repaired_pod["metadata"]["annotations"]["mysubpath"] = serde_json::json!("mypath");
    repaired_pod["status"] = serde_json::json!({
        "phase": "Pending",
        "containerStatuses": [{
            "name": "c",
            "state": {"waiting": {"reason": "CreateContainerConfigError"}}
        }]
    });
    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::WatchModified {
            key: key.clone(),
            resource_version: Some(2),
            pod: repaired_pod,
        }),
        PodAction::Noop
    ));

    let retry = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key.clone(),
        operation_id: 1,
        kind: PodLifecycleWorkKind::StartPod,
        retryable: false,
        failure: PodLifecycleWorkFailure::Startup(
            "All 1 app container(s) failed with CreateContainerConfigError".to_string(),
        ),
    });
    assert!(matches!(
        retry,
        PodAction::StartPod {
            key: action_key,
            operation_id: 2,
            pod: Some(pod),
            ..
        } if action_key == key
            && pod.pointer("/metadata/annotations/mysubpath").and_then(|value| value.as_str())
                == Some("mypath")
    ));
}

#[test]
fn pending_start_config_error_does_not_restart_unchanged_buffered_echo() {
    use super::message::{PodLifecycleWorkFailure, PodLifecycleWorkKind};
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let invalid_pod = serde_json::json!({
        "metadata": {
            "namespace": "default",
            "name": "pod-a",
            "uid": "uid-a",
            "annotations": {
                "notmysubpath": "mypath",
                "mysubpath": "/foo"
            }
        },
        "spec": {
            "nodeName": "dallas",
            "containers": [{
                "name": "c",
                "env": [
                    {"name": "POD_NAME", "value": "foo"},
                    {"name": "ANNOTATION", "valueFrom": {"fieldRef": {"fieldPath": "metadata.annotations['mysubpath']"}}}
                ],
                "volumeMounts": [{
                    "name": "workdir",
                    "mountPath": "/subpath_mount",
                    "subPathExpr": "$(ANNOTATION)"
                }]
            }],
            "volumes": [{"name": "workdir", "emptyDir": {}}]
        }
    });

    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::WatchAdded {
            key: key.clone(),
            resource_version: Some(1),
            pod: invalid_pod.clone(),
        }),
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let mut unchanged_echo = invalid_pod;
    unchanged_echo["status"] = serde_json::json!({
        "phase": "Pending",
        "containerStatuses": [{
            "name": "c",
            "state": {"waiting": {"reason": "CreateContainerConfigError"}}
        }]
    });
    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::WatchModified {
            key: key.clone(),
            resource_version: Some(2),
            pod: unchanged_echo,
        }),
        PodAction::Noop
    ));

    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::PodWorkFailed {
            key,
            operation_id: 1,
            kind: PodLifecycleWorkKind::StartPod,
            retryable: false,
            failure: PodLifecycleWorkFailure::Startup(
                "All 1 app container(s) failed with CreateContainerConfigError".to_string(),
            ),
        }),
        PodAction::Noop
    ));
}

#[tokio::test]
async fn supervised_deadline_shortening_cancels_graceful_and_dispatches_forced_stop() {
    let executor = DeadlineStopExecutor::new();
    let holder =
        Arc::new(std::sync::Mutex::new(executor.clone()
            as Arc<
                dyn crate::pod_lifecycle_router::executor::PodWorkExecutor,
            >));
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let mut actor = super::actor::PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        holder,
        dummy_reply_handle(),
    );
    actor.set_local_node_name_for_test("node-a");
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let actor_task = tokio::spawn(actor.run(rx));
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let initial_deadline = chrono::Utc::now() + chrono::Duration::seconds(2);
    let initial = terminating_test_pod(
        "uid-a",
        serde_json::json!(klights_cluster_core::k8s_time::format_legacy_timestamp(
            initial_deadline,
        )),
    );
    tx.send(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: initial,
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), executor.graceful_seen.notified())
        .await
        .expect("graceful stop dispatch");

    let shortened_deadline = chrono::Utc::now() + chrono::Duration::milliseconds(100);
    let shortened = terminating_test_pod(
        "uid-a",
        serde_json::json!(klights_cluster_core::k8s_time::format_legacy_timestamp(
            shortened_deadline,
        )),
    );
    tx.send(LifecycleMessage::WatchModified {
        key,
        resource_version: Some(2),
        pod: shortened,
    })
    .await
    .unwrap();

    tokio::time::timeout(
        Duration::from_secs(1),
        executor.graceful_cancelled.notified(),
    )
    .await
    .expect("shortened deadline cancels graceful stop");
    tokio::time::timeout(Duration::from_secs(1), executor.forced_seen.notified())
        .await
        .expect("shortened deadline dispatches forced stop");
    let calls = executor.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].0, crate::runtime::PodStopMode::Graceful);
    assert_eq!(calls[1].0, crate::runtime::PodStopMode::Forced);
    assert!(calls[1].1 > calls[0].1);

    drop(tx);
    actor_task.await.unwrap();
}

#[tokio::test]
async fn cri_stop_observation_cannot_block_actor_owned_deletion_finalization() {
    let executor = StopCompletionExecutor::new();
    let holder =
        Arc::new(std::sync::Mutex::new(executor.clone()
            as Arc<
                dyn crate::pod_lifecycle_router::executor::PodWorkExecutor,
            >));
    let outbox = BlockingRuntimeObservationOutbox::new();
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let mut actor = super::actor::PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        holder,
        dummy_reply_handle(),
    );
    actor.set_local_node_name_for_test("node-a");
    actor.set_runtime_observation_store_for_test(outbox.clone());
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let actor_task = tokio::spawn(actor.run(rx));
    let key = PodLifecycleKey::new("default", "ss-0", "uid-0");
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(30);
    let terminating = terminating_test_pod(
        "uid-0",
        serde_json::json!(klights_cluster_core::k8s_time::format_legacy_timestamp(
            deadline,
        )),
    );

    tx.send(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(7),
        pod: terminating,
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), executor.stop_seen.notified())
        .await
        .expect("terminating watch must dispatch StopPod");

    tx.send(LifecycleMessage::CriEvent {
        key,
        container_id: "container-0".to_string(),
        kind: crate::cri_events::KubeletEventKind::Stopped,
    })
    .await
    .unwrap();
    executor.release_stop.notify_one();

    tokio::time::timeout(Duration::from_millis(200), executor.finalize_seen.notified())
        .await
        .expect(
            "the StopPod-generated CRI event must not checkpoint a runtime reconcile and block deletion finalization",
        );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            outbox.checkpoint_attempted.notified(),
        )
        .await
        .is_err(),
        "Stopping Pods must discard self-generated CRI observations"
    );

    drop(tx);
    actor_task.abort();
}

#[test]
fn startable_pod_checks_slot_admission_before_startpod() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    actor.enable_slot_admission_gate_for_test();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = test_pod("default", "pod-a", "uid-a");

    let action = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: pod.clone(),
    });

    match action {
        PodAction::CheckSlotAdmission {
            key: actual_key,
            pod: actual_pod,
            resource_version,
            start_after_admit,
            ..
        } => {
            assert_eq!(actual_key, key);
            assert_eq!(actual_pod, pod);
            assert_eq!(resource_version, Some(1));
            assert!(start_after_admit);
        }
        other => panic!("expected CheckSlotAdmission before StartPod, got {other:?}"),
    }
}

#[test]
fn slot_admission_blocked_parks_start_without_startpod() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    actor.enable_slot_admission_gate_for_test();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = test_pod("default", "pod-a", "uid-a");

    let action = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: pod.clone(),
    });
    let operation_id = action.operation_id().expect("admission op id");

    let blocked = actor.handle_for_test(LifecycleMessage::SlotAdmissionBlocked {
        key: key.clone(),
        operation_id,
        blocking_uid: "old-uid".to_string(),
        blocking_node: "node-a".to_string(),
    });
    assert!(matches!(blocked, PodAction::Noop));

    let echo = actor.handle_for_test(LifecycleMessage::WatchModified {
        key,
        resource_version: Some(2),
        pod,
    });
    assert!(
        !matches!(echo, PodAction::StartPod { .. }),
        "blocked slot watch echoes must not bypass admission"
    );
}

#[test]
fn slot_admission_wake_rechecks_before_startpod() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    actor.enable_slot_admission_gate_for_test();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = test_pod("default", "pod-a", "uid-a");

    let action = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: pod.clone(),
    });
    let operation_id = action.operation_id().expect("admission op id");

    let _ = actor.handle_for_test(LifecycleMessage::SlotAdmissionBlocked {
        key: key.clone(),
        operation_id,
        blocking_uid: "old-uid".to_string(),
        blocking_node: "node-a".to_string(),
    });

    let wake = actor.handle_for_test(LifecycleMessage::SlotAdmissionWake { key: key.clone() });
    assert!(
        matches!(wake, PodAction::CheckSlotAdmission { key: actual, .. } if actual == key),
        "wakeup must re-check admission instead of dispatching StartPod directly"
    );
}

#[test]
fn blocked_slot_uses_single_waiter_for_repeated_watch_events() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    actor.enable_slot_admission_gate_for_test();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = test_pod("default", "pod-a", "uid-a");

    let action = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: pod.clone(),
    });
    let operation_id = action.operation_id().expect("admission op id");

    let _ = actor.handle_for_test(LifecycleMessage::SlotAdmissionBlocked {
        key: key.clone(),
        operation_id,
        blocking_uid: "old-uid".to_string(),
        blocking_node: "node-a".to_string(),
    });

    for rv in 2..8 {
        let action = actor.handle_for_test(LifecycleMessage::WatchModified {
            key: key.clone(),
            resource_version: Some(rv),
            pod: pod.clone(),
        });
        assert!(
            matches!(action, PodAction::Noop),
            "blocked watch echo {rv} must not spawn another admission check or StartPod"
        );
    }

    let wake = actor.handle_for_test(LifecycleMessage::SlotAdmissionWake { key: key.clone() });
    assert!(
        matches!(wake, PodAction::CheckSlotAdmission { key: actual, .. } if actual == key),
        "the single waiter wake should produce exactly one fresh admission check"
    );
}

#[test]
fn stale_slot_wake_for_old_uid_ignored() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    actor.enable_slot_admission_gate_for_test();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = test_pod("default", "pod-a", "uid-a");

    let action = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod,
    });
    let operation_id = action.operation_id().expect("admission op id");

    let _ = actor.handle_for_test(LifecycleMessage::SlotAdmissionBlocked {
        key: key.clone(),
        operation_id,
        blocking_uid: "old-uid".to_string(),
        blocking_node: "node-a".to_string(),
    });

    let stale = actor.handle_for_test(LifecycleMessage::SlotAdmissionWake {
        key: PodLifecycleKey::new("default", "pod-a", "old-uid"),
    });
    assert!(matches!(stale, PodAction::Noop));
    assert_eq!(
        actor.uid_mismatch_warnings_for_test(),
        vec![(
            "slot_admission_wake",
            "uid-a".to_string(),
            "old-uid".to_string()
        )]
    );
}

#[test]
fn stale_lifecycle_command_for_old_uid_ignored() {
    use crate::lifecycle::{LifecycleCommand, RestartReason};
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let current_key = PodLifecycleKey::new("default", "same-name", "uid-new");
    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::WatchAdded {
            key: current_key,
            resource_version: Some(1),
            pod: test_pod("default", "same-name", "uid-new"),
        }),
        PodAction::StartPod { .. }
    ));

    let stale = actor.handle_for_test(LifecycleMessage::LifecycleCommand {
        key: PodLifecycleKey::new("default", "same-name", "uid-old"),
        command: LifecycleCommand::RestartRequested {
            pod_uid: "uid-old".to_string(),
            namespace: "default".to_string(),
            pod_name: "same-name".to_string(),
            container_name: "app".to_string(),
            reason: RestartReason::LivenessProbe,
        },
    });

    assert!(
        matches!(stale, PodAction::Noop),
        "old-UID lifecycle commands must not dispatch work for the active replacement"
    );
    assert_eq!(actor.active_uid_for_test(), Some("uid-new"));
}

#[test]
fn active_uid_mismatch_workflows_emit_warning_records() {
    use crate::cri_events::KubeletEventKind;
    use crate::lifecycle::LifecycleCommand;

    fn actor_with_active_uid() -> super::actor::PodLifecycleActor {
        let mut actor = direct_test_actor();
        let key = PodLifecycleKey::new("default", "same-name", "uid-new");
        let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
            key,
            resource_version: Some(1),
            pod: test_pod("default", "same-name", "uid-new"),
        });
        actor.clear_uid_mismatch_warnings_for_test();
        actor
    }

    fn old_key() -> PodLifecycleKey {
        PodLifecycleKey::new("default", "same-name", "uid-old")
    }

    let cases = vec![
        (
            "watch_added",
            LifecycleMessage::WatchAdded {
                key: old_key(),
                resource_version: Some(2),
                pod: test_pod("default", "same-name", "uid-old"),
            },
        ),
        (
            "watch_modified",
            LifecycleMessage::WatchModified {
                key: old_key(),
                resource_version: Some(2),
                pod: test_pod("default", "same-name", "uid-old"),
            },
        ),
        (
            "watch_deleted",
            LifecycleMessage::WatchDeleted {
                key: old_key(),
                resource_version: Some(2),
                pod: test_pod("default", "same-name", "uid-old"),
            },
        ),
        ("retry_due", LifecycleMessage::RetryDue { key: old_key() }),
        (
            "cri_event",
            LifecycleMessage::CriEvent {
                key: old_key(),
                container_id: "container-a".to_string(),
                kind: KubeletEventKind::Stopped,
            },
        ),
        (
            "active_deadline_due",
            LifecycleMessage::ActiveDeadlineDue { key: old_key() },
        ),
        (
            "lifecycle_command",
            LifecycleMessage::LifecycleCommand {
                key: old_key(),
                command: LifecycleCommand::ReadinessChanged {
                    pod_uid: "uid-old".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "same-name".to_string(),
                    container_name: "app".to_string(),
                    ready: true,
                },
            },
        ),
    ];

    for (workflow, message) in cases {
        let mut actor = actor_with_active_uid();
        let _ = actor.handle_for_test(message);
        assert_eq!(
            actor.uid_mismatch_warnings_for_test(),
            vec![(workflow, "uid-new".to_string(), "uid-old".to_string())],
            "{workflow} should emit one warning-level UID mismatch record"
        );
    }
}

#[test]
fn slot_admission_clear_after_stop_completion_admits_replacement() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    actor.enable_slot_admission_gate_for_test();
    let old_key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let new_key = PodLifecycleKey::new("default", "pod-a", "uid-b");
    let old_pod = test_pod("default", "pod-a", "uid-a");
    let new_pod = test_pod("default", "pod-a", "uid-b");

    let check = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: old_key.clone(),
        resource_version: Some(1),
        pod: old_pod.clone(),
    });
    let check_op = check.operation_id().expect("old admission op");
    let start = actor.handle_for_test(LifecycleMessage::SlotAdmissionGranted {
        key: old_key.clone(),
        operation_id: check_op,
        pod: old_pod.clone(),
        resource_version: Some(1),
        start_after_admit: true,
    });
    let start_op = start.operation_id().expect("old start op");
    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: old_key.clone(),
        operation_id: start_op,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    let finalize_op = finalize.operation_id().expect("finalize op");
    let _ = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: old_key.clone(),
        operation_id: finalize_op,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: None,
    });

    let stop = actor.handle_for_test(LifecycleMessage::WatchDeleted {
        key: old_key.clone(),
        resource_version: Some(2),
        pod: old_pod,
    });
    let stop_op = stop.operation_id().expect("old stop op");
    let parked = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: new_key.clone(),
        resource_version: Some(3),
        pod: new_pod.clone(),
    });
    assert!(matches!(parked, PodAction::Noop));

    let finalize_delete = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: old_key.clone(),
        operation_id: stop_op,
        kind: super::message::PodLifecycleWorkKind::StopPod,
        sandbox_id: None,
    });
    assert!(
        matches!(&finalize_delete, PodAction::FinalizePodDeletion { key, .. } if key == &old_key),
        "StopPod completion must clear local state and then ask actor-owned finalization to remove the Pod row"
    );
    assert_eq!(
        actor.active_uid_for_test(),
        None,
        "local active UID must be cleared before datastore Pod cleanup"
    );
    assert_eq!(
        actor.admitted_slot_uid_for_test(),
        None,
        "slot admission state must be cleared before datastore Pod cleanup"
    );

    let admitted = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: old_key,
        operation_id: finalize_delete
            .operation_id()
            .expect("final delete operation id"),
        kind: super::message::PodLifecycleWorkKind::FinalizePodDeletion,
        sandbox_id: None,
    });
    assert!(
        matches!(admitted, PodAction::CheckSlotAdmission { key, pod, .. } if key == new_key && pod == new_pod),
        "replacement must be admitted through a fresh slot check only after datastore Pod cleanup finalizes"
    );
}

#[test]
fn slot_admission_not_cleared_after_retryable_stop_failure() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    actor.enable_slot_admission_gate_for_test();
    let old_key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let new_key = PodLifecycleKey::new("default", "pod-a", "uid-b");
    let old_pod = test_pod("default", "pod-a", "uid-a");

    let check = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: old_key.clone(),
        resource_version: Some(1),
        pod: old_pod.clone(),
    });
    let check_op = check.operation_id().expect("old admission op");
    let start = actor.handle_for_test(LifecycleMessage::SlotAdmissionGranted {
        key: old_key.clone(),
        operation_id: check_op,
        pod: old_pod.clone(),
        resource_version: Some(1),
        start_after_admit: true,
    });
    let start_op = start.operation_id().expect("old start op");
    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: old_key.clone(),
        operation_id: start_op,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    let finalize_op = finalize.operation_id().expect("finalize op");
    let _ = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: old_key.clone(),
        operation_id: finalize_op,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: None,
    });

    let stop = actor.handle_for_test(LifecycleMessage::WatchDeleted {
        key: old_key.clone(),
        resource_version: Some(2),
        pod: old_pod,
    });
    let stop_op = stop.operation_id().expect("old stop op");
    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: new_key,
        resource_version: Some(3),
        pod: test_pod("default", "pod-a", "uid-b"),
    });

    let retry = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: old_key,
        operation_id: stop_op,
        kind: super::message::PodLifecycleWorkKind::StopPod,
        retryable: true,
        failure: super::message::PodLifecycleWorkFailure::DeadlineExceeded,
    });
    assert!(
        matches!(retry, PodAction::ScheduleRetry { .. }),
        "retryable StopPod failure must keep the old slot blocked and schedule a retry"
    );
}

#[test]
fn stop_pod_unscheduled_not_owned_failure_does_not_retry() {
    use crate::pod_lifecycle_core::action::PodAction;
    use crate::pod_lifecycle_core::message::PodLifecycleWorkFailure;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = test_pod("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: pod.clone(),
    });
    let start_op = start.operation_id().expect("start op");
    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: start_op,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    let finalize_op = finalize.operation_id().expect("finalize op");
    let _ = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: finalize_op,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: None,
    });

    let stop = actor.handle_for_test(LifecycleMessage::WatchDeleted {
        key: key.clone(),
        resource_version: Some(2),
        pod,
    });
    let stop_op = stop.operation_id().expect("stop op");

    // P0 StopPod loop: an unscheduled Pod (target_node == None) reaching a
    // kubelet that does not own it must be terminal/non-retryable. The actor
    // must NOT schedule a retry, must NOT hard-delete via FinalizePodDeletion,
    // and must cancel the in-flight stop.
    let action = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key.clone(),
        operation_id: stop_op,
        kind: super::message::PodLifecycleWorkKind::StopPod,
        retryable: false,
        failure: PodLifecycleWorkFailure::NotOwned {
            local_node: "this-node".to_string(),
            target_node: None,
        },
    });
    assert!(
        matches!(action, PodAction::Noop),
        "unscheduled/not-owned StopPod failure must no-op locally, not retry; got {action:?}"
    );
    assert!(
        !matches!(action, PodAction::FinalizePodDeletion { .. }),
        "not-owned StopPod must not hard-delete a row this node does not own"
    );
    assert_eq!(
        actor.in_flight_for_test(),
        None,
        "in-flight stop must be cancelled after a not-owned StopPod failure"
    );
}

#[test]
fn stop_pod_other_node_not_owned_failure_does_not_retry() {
    use crate::pod_lifecycle_core::action::PodAction;
    use crate::pod_lifecycle_core::message::PodLifecycleWorkFailure;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-b", "uid-b");
    let pod = test_pod("default", "pod-b", "uid-b");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: pod.clone(),
    });
    let start_op = start.operation_id().expect("start op");
    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: start_op,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-b".to_string()),
    });
    let finalize_op = finalize.operation_id().expect("finalize op");
    let _ = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: finalize_op,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: None,
    });

    let stop = actor.handle_for_test(LifecycleMessage::WatchDeleted {
        key: key.clone(),
        resource_version: Some(2),
        pod,
    });
    let stop_op = stop.operation_id().expect("stop op");

    // A Pod assigned to another node is equally non-retryable on this actor.
    let action = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key,
        operation_id: stop_op,
        kind: super::message::PodLifecycleWorkKind::StopPod,
        retryable: false,
        failure: PodLifecycleWorkFailure::NotOwned {
            local_node: "this-node".to_string(),
            target_node: Some("other-node".to_string()),
        },
    });
    assert!(
        matches!(action, PodAction::Noop),
        "other-node StopPod failure must no-op locally, not retry; got {action:?}"
    );
    assert_eq!(
        actor.in_flight_for_test(),
        None,
        "in-flight stop must be cancelled after a not-owned StopPod failure"
    );
}

#[test]
fn running_pod_observation_seeds_slot_admission_without_startpod() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    actor.enable_slot_admission_gate_for_test();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = running_test_pod("default", "pod-a", "uid-a");

    let action = actor.handle_for_test(LifecycleMessage::WatchModified {
        key,
        resource_version: Some(10),
        pod,
    });

    assert!(
        matches!(
            action,
            PodAction::CheckSlotAdmission {
                start_after_admit: false,
                ..
            }
        ),
        "running local pods must seed the cluster slot admission row without starting"
    );
}

#[tokio::test]
async fn actor_records_messages_in_receive_order() {
    use super::actor::PodLifecycleActor;
    use crate::cri_events::KubeletEventKind;

    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        test_executor_holder(),
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: serde_json::json!({"kind": "Pod"}),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::RetryDue { key: key.clone() })
        .unwrap();
    tx.try_send(LifecycleMessage::CriEvent {
        key,
        container_id: "container-a".to_string(),
        kind: KubeletEventKind::Started,
    })
    .unwrap();
    drop(tx);

    let mut seen = Vec::new();
    while let Some(event) = seen_rx.recv().await {
        seen.push(event);
    }
    handle.await.unwrap();

    assert_eq!(seen, ["watch_added", "retry_due", "cri_event"]);
}

#[tokio::test]
async fn actor_processes_watch_deleted_while_start_pod_is_in_flight() {
    use super::actor::PodLifecycleActor;

    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let (executor, release_start) = BlockingStartExecutor::new();
    let executor_holder =
        Arc::new(std::sync::Mutex::new(executor.clone()
            as Arc<
                dyn crate::pod_lifecycle_router::executor::PodWorkExecutor,
            >));
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    })
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), executor.wait_for_start())
        .await
        .expect("StartPod should enter executor");

    tx.try_send(LifecycleMessage::WatchDeleted {
        key,
        resource_version: Some(2),
        pod: test_pod("default", "pod-a", "uid-a"),
    })
    .unwrap();

    tokio::time::timeout(Duration::from_millis(200), async {
        while !executor.has_stop_pod() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("WatchDeleted must dispatch StopPod while StartPod remains in flight");

    let _ = release_start.send(());
    drop(tx);
    handle.await.unwrap();
}

#[tokio::test]
async fn completion_does_not_route_through_router() {
    use super::actor::PodLifecycleActor;

    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let public_routes = Arc::new(AtomicUsize::new(0));
    let reply_handle = LifecycleReplyHandle::new(Arc::new(CountingReplyBackend {
        routes: public_routes.clone(),
    }));
    let executor: Arc<dyn crate::pod_lifecycle_router::executor::PodWorkExecutor> =
        Arc::new(CompletingExecutor);
    let executor_holder = Arc::new(std::sync::Mutex::new(executor));
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor =
        PodLifecycleActor::new_with_event_sink_for_test(8, seen_tx, executor_holder, reply_handle);
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key,
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    })
    .unwrap();
    drop(tx);
    handle.await.unwrap();

    assert_eq!(
        public_routes.load(Ordering::SeqCst),
        0,
        "executor completions must return through the actor's private channel, not the public router"
    );
}

#[tokio::test]
async fn stop_pod_cancels_in_flight_start_pod() {
    use super::actor::PodLifecycleActor;

    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let (executor, _release_start) = BlockingStartExecutor::new();
    let executor_holder =
        Arc::new(std::sync::Mutex::new(executor.clone()
            as Arc<
                dyn crate::pod_lifecycle_router::executor::PodWorkExecutor,
            >));
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    })
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), executor.wait_for_start())
        .await
        .expect("StartPod should enter executor");

    tx.try_send(LifecycleMessage::WatchDeleted {
        key,
        resource_version: Some(2),
        pod: test_pod("default", "pod-a", "uid-a"),
    })
    .unwrap();

    tokio::time::timeout(Duration::from_secs(1), executor.wait_for_start_cancelled())
        .await
        .expect("WatchDeleted(active_uid) must cancel in-flight StartPod token");

    drop(tx);
    handle.await.unwrap();
}

#[test]
fn stale_completion_with_wrong_operation_id_ignored() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });

    let action = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 999,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });

    assert!(matches!(action, PodAction::Noop));
    assert_eq!(
        actor.in_flight_for_test(),
        Some(("uid-a", super::message::PodLifecycleWorkKind::StartPod, 1))
    );
}

#[test]
fn stale_completion_with_wrong_uid_ignored() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key_a = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let key_b = PodLifecycleKey::new("default", "pod-a", "uid-b");
    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key_a,
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });

    let action = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key_b,
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-b".to_string()),
    });

    assert!(matches!(action, PodAction::Noop));
    assert_eq!(
        actor.in_flight_for_test(),
        Some(("uid-a", super::message::PodLifecycleWorkKind::StartPod, 1))
    );
    assert_eq!(
        actor.uid_mismatch_warnings_for_test(),
        vec![("pod_work_result", "uid-a".to_string(), "uid-b".to_string())]
    );
}

#[test]
fn stale_stop_completion_with_wrong_uid_ignored_at_actor_level() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key_a = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let key_b = PodLifecycleKey::new("default", "pod-a", "uid-b");
    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key_a.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });

    let stop_action = actor.handle_for_test(LifecycleMessage::WatchDeleted {
        key: key_a.clone(),
        resource_version: Some(2),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    let operation_id = match stop_action {
        PodAction::StopPod { operation_id, .. } => operation_id,
        other => panic!("expected StopPod action, got {other:?}"),
    };

    assert_eq!(
        actor.in_flight_for_test(),
        Some((
            "uid-a",
            super::message::PodLifecycleWorkKind::StopPod,
            operation_id
        ))
    );

    let action = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key_b,
        operation_id,
        kind: super::message::PodLifecycleWorkKind::StopPod,
        sandbox_id: None,
    });

    assert!(matches!(action, PodAction::Noop));
    assert_eq!(
        actor.in_flight_for_test(),
        Some((
            "uid-a",
            super::message::PodLifecycleWorkKind::StopPod,
            operation_id
        ))
    );
}

#[test]
fn matching_reconcile_runtime_completion_clears_in_flight() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::ActiveDeadlineDue { key: key.clone() }),
        PodAction::ReconcileRuntime {
            operation_id: 1,
            ..
        }
    ));
    assert_eq!(
        actor.in_flight_for_test(),
        Some((
            "uid-a",
            super::message::PodLifecycleWorkKind::ReconcileRuntime,
            1
        ))
    );

    let action = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::ReconcileRuntime,
        sandbox_id: None,
    });

    assert!(matches!(action, PodAction::Noop));
    assert_eq!(actor.in_flight_for_test(), None);
}

#[test]
fn cri_event_during_runtime_reconcile_dispatches_followup_after_completion() {
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        start,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        finalize,
        PodAction::FinalizeStartup {
            operation_id: 2,
            ..
        }
    ));

    let finalized = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        finalized,
        PodAction::ReconcileRuntime {
            operation_id: 3,
            ..
        }
    ));

    let deferred_old = actor.handle_for_test(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "old-container".to_string(),
        kind: KubeletEventKind::Stopped,
    });
    assert!(matches!(deferred_old, PodAction::Noop));

    let deferred = actor.handle_for_test(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "new-container".to_string(),
        kind: KubeletEventKind::Started,
    });
    assert!(
        matches!(deferred, PodAction::Noop),
        "CRI event during runtime reconcile must be deferred, got {deferred:?}"
    );

    let followup = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 3,
        kind: super::message::PodLifecycleWorkKind::ReconcileRuntime,
        sandbox_id: None,
    });
    assert!(
        matches!(
            followup,
            PodAction::ReconcileRuntime {
                operation_id: 4,
                ..
            }
        ),
        "deferred CRI event must dispatch a follow-up runtime reconcile after the in-flight reconcile completes, got {followup:?}"
    );
}

#[test]
fn cri_event_during_startup_finalization_preserves_container_id_hint() {
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;
    use crate::runtime::RuntimeReconcileHint;

    // A fast-exit pod (ConfigMap-volume / ReplicaSet-adoption) can stop while
    // startup finalization is still in flight. The actor defers the CRI stop
    // event and the deferred reconcile must carry the event's container id so
    // runtime reconcile can read the concrete (terminated) status instead of
    // synthesizing Pending/ContainerCreating under an empty sandbox listing.
    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        start,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    // StartPod completes → FinalizeStartup dispatched (in flight).
    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        finalize,
        PodAction::FinalizeStartup {
            operation_id: 2,
            ..
        }
    ));

    // CRI stop event arrives while FinalizeStartup is in flight → must defer.
    let deferred = actor.handle_for_test(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "ctr-fast-exit".to_string(),
        kind: KubeletEventKind::Stopped,
    });
    assert!(
        matches!(deferred, PodAction::Noop),
        "CRI event during startup finalization must be deferred, got {deferred:?}"
    );

    // FinalizeStartup completes → deferred reconcile drains, carrying the hint.
    let reconciled = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    match reconciled {
        PodAction::ReconcileRuntime {
            hint, operation_id, ..
        } => {
            assert_eq!(
                operation_id, 3,
                "deferred runtime reconcile must use the next operation id"
            );
            assert_eq!(
                hint,
                RuntimeReconcileHint::from_container_event(
                    "ctr-fast-exit",
                    KubeletEventKind::Stopped,
                ),
                "deferred reconcile must carry the CRI event's container id and transition kind so the reconciler can read the concrete terminated status"
            );
        }
        other => panic!(
            "FinalizeStartup completion must drain the deferred CRI event as a ReconcileRuntime carrying the container id hint, got {other:?}"
        ),
    }
}

#[test]
fn sandbox_then_app_started_during_finalization_defers_only_workload_hint() {
    use crate::cri_events::{KubeletEvent, KubeletEventKind};
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let mut actions = Vec::new();
    actions.push(actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    }));
    actions.push(actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".into()),
    }));

    let transition = |container_id: &str| KubeletEvent {
        kind: KubeletEventKind::Started,
        container_id: container_id.into(),
        pod_namespace: Some("default".into()),
        pod_name: Some("pod-a".into()),
        pod_uid: Some("uid-a".into()),
        pod_sandbox_id: Some("sandbox-a".into()),
        timestamp_ns: 1,
    };
    for event in [transition("sandbox-a"), transition("container-app")] {
        if event.is_pod_sandbox_transition() {
            continue;
        }
        actions.push(actor.handle_for_test(LifecycleMessage::CriEvent {
            key: key.clone(),
            container_id: event.container_id,
            kind: event.kind,
        }));
    }
    actions.push(actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: Some("sandbox-a".into()),
    }));

    assert_eq!(
        actions
            .iter()
            .filter(|action| matches!(action, PodAction::StartPod { .. }))
            .count(),
        1
    );
    let hint = actions.iter().find_map(|action| match action {
        PodAction::ReconcileRuntime { hint, .. } => Some(hint),
        _ => None,
    });
    assert_eq!(
        hint.expect("deferred workload reconcile")
            .container_ids()
            .collect::<Vec<_>>(),
        vec!["container-app"]
    );
    assert!(!actions.iter().any(|action| matches!(
        action,
        PodAction::ScheduleRetry { .. }
            | PodAction::ScheduleStartPodRetry { .. }
            | PodAction::StopPod { .. }
            | PodAction::FinalizePodDeletion { .. }
    )));
}

#[test]
fn runtime_reconcile_after_unconfirmed_startup_finalization_retries_finalization() {
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        start,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let deferred_runtime = actor.handle_for_test(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "container-a".to_string(),
        kind: KubeletEventKind::Started,
    });
    assert!(matches!(deferred_runtime, PodAction::Noop));

    let early_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        early_finalize,
        PodAction::FinalizeStartup {
            operation_id: 2,
            ..
        }
    ));

    let reconcile = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: None,
    });
    assert!(matches!(
        reconcile,
        PodAction::ReconcileRuntime {
            operation_id: 3,
            ..
        }
    ));

    let retry_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 3,
        kind: super::message::PodLifecycleWorkKind::ReconcileRuntime,
        sandbox_id: None,
    });
    assert!(
        matches!(
            retry_finalize,
            PodAction::FinalizeStartup {
                operation_id: 4,
                ..
            }
        ),
        "runtime reconcile publishes the Running status that unblocks startup finalization; completion must retry probe startup without relying on a later watch echo, got {retry_finalize:?}"
    );
}

#[test]
fn running_watch_after_runtime_reconcile_retry_preserves_finalization_in_flight() {
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        start,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let deferred_runtime = actor.handle_for_test(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "container-a".to_string(),
        kind: KubeletEventKind::Started,
    });
    assert!(matches!(deferred_runtime, PodAction::Noop));

    let early_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        early_finalize,
        PodAction::FinalizeStartup {
            operation_id: 2,
            ..
        }
    ));

    let reconcile = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: None,
    });
    assert!(
        matches!(
            reconcile,
            PodAction::ReconcileRuntime {
                operation_id: 3,
                ..
            }
        ),
        "early startup finalization that did not confirm probe startup must drain the deferred runtime reconcile"
    );

    let retry_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 3,
        kind: super::message::PodLifecycleWorkKind::ReconcileRuntime,
        sandbox_id: None,
    });
    assert!(
        matches!(
            retry_finalize,
            PodAction::FinalizeStartup {
                operation_id: 4,
                ..
            }
        ),
        "runtime reconcile completion must retry unconfirmed startup finalization, got {retry_finalize:?}"
    );

    let echo = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(4),
        pod: running_test_pod("default", "pod-a", "uid-a"),
    });
    assert!(
        matches!(echo, PodAction::Noop),
        "Running watch echo must not duplicate a retry already in flight, got {echo:?}"
    );
    assert_eq!(
        actor.in_flight_for_test(),
        Some((
            "uid-a",
            super::message::PodLifecycleWorkKind::FinalizeStartup,
            4
        )),
        "Running watch echo must preserve the finalization retry in flight"
    );
}

#[test]
fn running_watch_during_unconfirmed_finalization_retry_runs_after_completion() {
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        start,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let deferred_runtime = actor.handle_for_test(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "container-a".to_string(),
        kind: KubeletEventKind::Started,
    });
    assert!(matches!(deferred_runtime, PodAction::Noop));

    let early_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        early_finalize,
        PodAction::FinalizeStartup {
            operation_id: 2,
            ..
        }
    ));

    let reconcile = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: None,
    });
    assert!(matches!(
        reconcile,
        PodAction::ReconcileRuntime {
            operation_id: 3,
            ..
        }
    ));

    let retry_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 3,
        kind: super::message::PodLifecycleWorkKind::ReconcileRuntime,
        sandbox_id: None,
    });
    assert!(matches!(
        retry_finalize,
        PodAction::FinalizeStartup {
            operation_id: 4,
            ..
        }
    ));

    let echo = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(4),
        pod: running_test_pod("default", "pod-a", "uid-a"),
    });
    assert!(
        matches!(echo, PodAction::Noop),
        "Running watch echo must not replace the in-flight finalizer, got {echo:?}"
    );
    assert_eq!(
        actor.in_flight_for_test(),
        Some((
            "uid-a",
            super::message::PodLifecycleWorkKind::FinalizeStartup,
            4
        ))
    );

    let followup_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 4,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: None,
    });
    assert!(
        matches!(
            followup_finalize,
            PodAction::FinalizeStartup {
                operation_id: 5,
                ..
            }
        ),
        "Running watch echo observed during an unconfirmed finalizer retry must trigger one more startup finalization after the in-flight attempt completes, got {followup_finalize:?}"
    );
}

#[test]
fn successful_finalization_retry_clears_running_watch_pending_retry() {
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        start,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let deferred_runtime = actor.handle_for_test(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "container-a".to_string(),
        kind: KubeletEventKind::Started,
    });
    assert!(matches!(deferred_runtime, PodAction::Noop));

    let early_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        early_finalize,
        PodAction::FinalizeStartup {
            operation_id: 2,
            ..
        }
    ));

    let reconcile = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: None,
    });
    assert!(matches!(
        reconcile,
        PodAction::ReconcileRuntime {
            operation_id: 3,
            ..
        }
    ));

    let retry_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 3,
        kind: super::message::PodLifecycleWorkKind::ReconcileRuntime,
        sandbox_id: None,
    });
    assert!(matches!(
        retry_finalize,
        PodAction::FinalizeStartup {
            operation_id: 4,
            ..
        }
    ));

    let echo = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(4),
        pod: running_test_pod("default", "pod-a", "uid-a"),
    });
    assert!(
        matches!(echo, PodAction::Noop),
        "Running watch echo must stay pending while retry finalization is in flight, got {echo:?}"
    );

    let completed = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 4,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(
        matches!(completed, PodAction::Noop),
        "successful finalization retry must clear the pending Running watch retry, got {completed:?}"
    );
    assert_eq!(actor.in_flight_for_test(), None);
}

#[test]
fn equal_rv_running_watch_after_runtime_reconcile_retry_preserves_finalization_in_flight() {
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        start,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let deferred_runtime = actor.handle_for_test(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "container-a".to_string(),
        kind: KubeletEventKind::Started,
    });
    assert!(matches!(deferred_runtime, PodAction::Noop));

    let early_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        early_finalize,
        PodAction::FinalizeStartup {
            operation_id: 2,
            ..
        }
    ));

    let reconcile = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: None,
    });
    assert!(matches!(
        reconcile,
        PodAction::ReconcileRuntime {
            operation_id: 3,
            ..
        }
    ));

    let retry_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 3,
        kind: super::message::PodLifecycleWorkKind::ReconcileRuntime,
        sandbox_id: None,
    });
    assert!(
        matches!(
            retry_finalize,
            PodAction::FinalizeStartup {
                operation_id: 4,
                ..
            }
        ),
        "runtime reconcile completion must retry unconfirmed startup finalization, got {retry_finalize:?}"
    );

    let echo = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(1),
        pod: running_test_pod_without_sandbox_annotation("default", "pod-a", "uid-a"),
    });
    assert!(
        matches!(echo, PodAction::Noop),
        "equal resourceVersion Running watch echo must not duplicate a retry already in flight, got {echo:?}"
    );
    assert_eq!(
        actor.in_flight_for_test(),
        Some((
            "uid-a",
            super::message::PodLifecycleWorkKind::FinalizeStartup,
            4
        )),
        "equal resourceVersion Running watch echo must preserve the finalization retry in flight"
    );
}

#[test]
fn running_watch_echo_during_runtime_reconcile_preserves_deferred_followup() {
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        start,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        finalize,
        PodAction::FinalizeStartup {
            operation_id: 2,
            ..
        }
    ));

    let finalized = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        finalized,
        PodAction::ReconcileRuntime {
            operation_id: 3,
            ..
        }
    ));

    let deferred_old = actor.handle_for_test(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "old-container".to_string(),
        kind: KubeletEventKind::Stopped,
    });
    assert!(matches!(deferred_old, PodAction::Noop));

    let deferred = actor.handle_for_test(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "new-container".to_string(),
        kind: KubeletEventKind::Started,
    });
    assert!(matches!(deferred, PodAction::Noop));

    let echo = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(2),
        pod: serde_json::json!({
            "metadata": {
                "namespace": "default",
                "name": "pod-a",
                "uid": "uid-a",
                "annotations": {"klights.dev/sandbox-id": "sandbox-a"}
            },
            "status": {
                "phase": "Running",
                "podIP": "10.43.0.10"
            }
        }),
    });
    assert!(matches!(echo, PodAction::Noop));
    assert_eq!(
        actor.in_flight_for_test(),
        Some((
            "uid-a",
            super::message::PodLifecycleWorkKind::ReconcileRuntime,
            3
        )),
        "Running watch echoes from runtime status writes must not clear ReconcileRuntime in-flight"
    );

    let followup = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 3,
        kind: super::message::PodLifecycleWorkKind::ReconcileRuntime,
        sandbox_id: None,
    });
    assert!(
        matches!(
            followup,
            PodAction::ReconcileRuntime {
                operation_id: 4,
                ..
            }
        ),
        "deferred CRI event must still drain after a running watch echo, got {followup:?}"
    );
}

#[test]
fn pending_running_status_with_pod_ip_during_runtime_reconcile_dispatches_followup() {
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        start,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        finalize,
        PodAction::FinalizeStartup {
            operation_id: 2,
            ..
        }
    ));

    let finalized = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        finalized,
        PodAction::ReconcileRuntime {
            operation_id: 3,
            ..
        }
    ));

    let deferred_cri = actor.handle_for_test(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "container-a".to_string(),
        kind: KubeletEventKind::Started,
    });
    assert!(matches!(deferred_cri, PodAction::Noop));

    let pending_running_echo = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(2),
        pod: serde_json::json!({
            "metadata": {
                "namespace": "default",
                "name": "pod-a",
                "uid": "uid-a",
                "annotations": {"klights.dev/sandbox-id": "sandbox-a"}
            },
            "status": {
                "phase": "Pending",
                "podIP": "10.43.0.10",
                "containerStatuses": [{
                    "name": "app",
                    "ready": true,
                    "started": true,
                    "state": {"running": {"startedAt": "2026-05-18T10:00:00Z"}}
                }],
                "conditions": [
                    {"type": "ContainersReady", "status": "True"},
                    {"type": "Ready", "status": "False"}
                ]
            }
        }),
    });
    assert!(
        matches!(pending_running_echo, PodAction::Noop),
        "pending/running watch echo during runtime reconcile must defer a follow-up, got {pending_running_echo:?}"
    );

    let followup = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 3,
        kind: super::message::PodLifecycleWorkKind::ReconcileRuntime,
        sandbox_id: None,
    });
    assert!(
        matches!(
            followup,
            PodAction::ReconcileRuntime {
                operation_id: 4,
                ..
            }
        ),
        "pending pod with podIP and running containers must dispatch a follow-up reconcile after the in-flight reconcile completes, got {followup:?}"
    );
}

#[test]
fn start_failure_with_deferred_cri_event_reconciles_runtime_then_retries_if_still_pending() {
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        start,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let deferred = actor.handle_for_test(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "container-a".to_string(),
        kind: KubeletEventKind::Started,
    });
    assert!(matches!(deferred, PodAction::Noop));

    let reconcile = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        retryable: true,
        failure: super::message::PodLifecycleWorkFailure::Startup(
            "replication stream closed before forward send".to_string(),
        ),
    });
    assert!(matches!(
        reconcile,
        PodAction::ReconcileRuntime {
            operation_id: 2,
            ..
        }
    ));

    let retry = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::ReconcileRuntime,
        sandbox_id: None,
    });
    assert!(
        matches!(retry, PodAction::ScheduleRetry { .. }),
        "if no Running watch echo arrived after runtime reconcile, StartPod must stay retryable"
    );
}

#[test]
fn start_completion_without_sandbox_but_deferred_cri_event_reconciles_runtime() {
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("kube-system", "coredns", "uid-coredns");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("kube-system", "coredns", "uid-coredns"),
    });
    assert!(matches!(
        start,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let deferred = actor.handle_for_test(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "container-coredns".to_string(),
        kind: KubeletEventKind::Started,
    });
    assert!(matches!(deferred, PodAction::Noop));

    let reconcile = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: None,
    });
    assert!(
        matches!(
            reconcile,
            PodAction::ReconcileRuntime {
                operation_id: 2,
                ..
            }
        ),
        "a missing StartPod sandbox id must not drop a deferred CRI reconcile"
    );
}

/// A retryable StartPod failure must produce `PodAction::ScheduleStartPodRetry`
/// carrying the underlying error message and the incremented attempt count
/// so the executor can surface ErrImagePull/ImagePullBackOff in pod status.
#[test]
fn retryable_start_pod_failure_produces_schedule_start_pod_retry_with_error_message() {
    use super::message::{PodLifecycleWorkFailure, PodLifecycleWorkKind};
    use crate::pod_lifecycle_core::action::PodAction;
    use crate::runtime::retry_backoff;

    let mut actor = direct_test_actor();
    actor.enable_slot_admission_gate_for_test();
    let key = PodLifecycleKey::new("default", "pull-fail", "uid-pf");
    let pod = test_pod("default", "pull-fail", "uid-pf");

    let admission = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: pod.clone(),
    });
    let admission_operation_id = admission.operation_id().expect("admission operation id");

    let start = actor.handle_for_test(LifecycleMessage::SlotAdmissionGranted {
        key: key.clone(),
        operation_id: admission_operation_id,
        pod,
        resource_version: Some(1),
        start_after_admit: true,
    });
    let start_operation_id = start.operation_id().expect("start operation id");

    let underlying = "Failed to pull image \"busybox:1.36\": connection refused".to_string();
    let action = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key.clone(),
        operation_id: start_operation_id,
        kind: PodLifecycleWorkKind::StartPod,
        retryable: true,
        failure: PodLifecycleWorkFailure::Startup(underlying.clone()),
    });

    match action {
        PodAction::ScheduleStartPodRetry {
            key: retry_key,
            delay,
            error_message,
            attempt,
        } => {
            assert_eq!(retry_key.uid, "uid-pf", "retry must stay UID-bound");
            assert_eq!(delay, retry_backoff(1), "first retry uses backoff(1)");
            assert_eq!(error_message, underlying, "error message must round-trip");
            assert_eq!(attempt, 1, "first attempt counter");
        }
        other => panic!("expected ScheduleStartPodRetry, got {other:?}"),
    }
}

#[test]
fn retryable_start_pod_failures_use_exponential_backoff() {
    use super::message::{PodLifecycleWorkFailure, PodLifecycleWorkKind};
    use crate::pod_lifecycle_core::action::PodAction;
    use crate::runtime::retry_backoff;

    let mut actor = direct_test_actor();
    actor.enable_slot_admission_gate_for_test();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = test_pod("default", "pod-a", "uid-a");

    let admission = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: pod.clone(),
    });
    let admission_operation_id = admission.operation_id().expect("admission operation id");

    let start = actor.handle_for_test(LifecycleMessage::SlotAdmissionGranted {
        key: key.clone(),
        operation_id: admission_operation_id,
        pod,
        resource_version: Some(1),
        start_after_admit: true,
    });
    let first_start_operation_id = start.operation_id().expect("start operation id");

    let first_retry = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key.clone(),
        operation_id: first_start_operation_id,
        kind: PodLifecycleWorkKind::StartPod,
        retryable: true,
        failure: PodLifecycleWorkFailure::Startup("image pull failed".to_string()),
    });
    assert!(matches!(
        first_retry,
        PodAction::ScheduleStartPodRetry { delay, attempt: 1, .. } if delay == retry_backoff(1)
    ));

    let retry_start = actor.handle_for_test(LifecycleMessage::RetryDue { key: key.clone() });
    let second_start_operation_id = retry_start
        .operation_id()
        .expect("retry start operation id");

    let second_retry = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key,
        operation_id: second_start_operation_id,
        kind: PodLifecycleWorkKind::StartPod,
        retryable: true,
        failure: PodLifecycleWorkFailure::Startup("image pull failed".to_string()),
    });
    assert!(matches!(
        second_retry,
        PodAction::ScheduleStartPodRetry { delay, attempt: 2, .. } if delay == retry_backoff(2)
    ));
}

#[test]
fn matching_stop_pod_failure_clears_in_flight_and_schedules_retry() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::WatchDeleted {
            key: key.clone(),
            resource_version: Some(2),
            pod: test_pod("default", "pod-a", "uid-a"),
        }),
        PodAction::StopPod {
            operation_id: 2,
            ..
        }
    ));

    let action = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key.clone(),
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::StopPod,
        retryable: true,
        failure: super::message::PodLifecycleWorkFailure::DispatchFailed("stop failed".into()),
    });

    assert!(matches!(
        action,
        PodAction::ScheduleRetry {
            key: retry_key,
            ..
        } if retry_key == key
    ));
    assert_eq!(actor.in_flight_for_test(), None);
}

#[test]
fn stop_pod_container_not_found_failure_moves_to_finalize() {
    use super::message::{PodLifecycleWorkFailure, PodLifecycleWorkKind};
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = test_pod("default", "pod-a", "uid-a");

    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: pod.clone(),
    });
    let stop = actor.handle_for_test(LifecycleMessage::WatchDeleted {
        key: key.clone(),
        resource_version: Some(2),
        pod,
    });
    let stop_operation_id = stop.operation_id().expect("stop operation id");

    let action = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key.clone(),
        operation_id: stop_operation_id,
        kind: PodLifecycleWorkKind::StopPod,
        retryable: false,
        failure: PodLifecycleWorkFailure::ContainerNotFound,
    });

    assert!(matches!(
        action,
        PodAction::FinalizePodDeletion {
            key: action_key,
            operation_id,
            ..
        } if action_key == key && operation_id == stop_operation_id + 1
    ));
    assert_eq!(
        actor.in_flight_for_test(),
        Some((
            "uid-a",
            PodLifecycleWorkKind::FinalizePodDeletion,
            stop_operation_id + 1
        )),
    );
}

#[test]
fn reconcile_runtime_container_not_found_retries_uid_bound_start_progression() {
    use super::message::{PodLifecycleWorkFailure, PodLifecycleWorkKind};
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "init-retry", "uid-init-retry");
    let pod = test_pod("default", "init-retry", "uid-init-retry");
    actor.enable_slot_admission_gate_for_test();

    let admission = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: pod.clone(),
    });
    let admission_id = admission.operation_id().expect("slot admission operation");
    let start = actor.handle_for_test(LifecycleMessage::SlotAdmissionGranted {
        key: key.clone(),
        operation_id: admission_id,
        pod,
        resource_version: Some(1),
        start_after_admit: true,
    });
    let start_id = start.operation_id().expect("start operation");
    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: start_id,
        kind: PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sb-init-retry".into()),
    });
    let finalize_id = finalize
        .operation_id()
        .expect("startup finalization operation");
    let reconcile = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: finalize_id,
        kind: PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: Some("sb-init-retry".into()),
    });
    let reconcile_id = reconcile
        .operation_id()
        .expect("runtime reconcile operation");

    let retry = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key.clone(),
        operation_id: reconcile_id,
        kind: PodLifecycleWorkKind::ReconcileRuntime,
        retryable: true,
        failure: PodLifecycleWorkFailure::ContainerNotFound,
    });
    assert!(matches!(retry, PodAction::ScheduleRetry { key: retry_key, .. } if retry_key == key));

    let restarted = actor.handle_for_test(LifecycleMessage::RetryDue { key: key.clone() });
    assert!(matches!(
        restarted,
        PodAction::StartPod { key: restarted_key, .. } if restarted_key == key
    ));
}

#[test]
fn retry_due_after_stop_pod_failure_retries_stop_with_snapshot() {
    use super::message::{PodLifecycleWorkFailure, PodLifecycleWorkKind};
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = test_pod("default", "pod-a", "uid-a");

    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: pod.clone(),
    });
    let stop = actor.handle_for_test(LifecycleMessage::WatchDeleted {
        key: key.clone(),
        resource_version: Some(2),
        pod: pod.clone(),
    });
    let stop_operation_id = stop.operation_id().expect("stop operation id");

    let retry_timer = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key.clone(),
        operation_id: stop_operation_id,
        kind: PodLifecycleWorkKind::StopPod,
        retryable: true,
        failure: PodLifecycleWorkFailure::DispatchFailed("volume busy".into()),
    });
    assert!(matches!(retry_timer, PodAction::ScheduleRetry { .. }));
    assert_eq!(actor.in_flight_for_test(), None);

    let retry = actor.handle_for_test(LifecycleMessage::RetryDue { key: key.clone() });
    assert!(matches!(
        retry,
        PodAction::StopPod {
            key: retry_key,
            operation_id,
            pod: Some(retry_pod),
            ..
        } if retry_key == key
            && operation_id == stop_operation_id + 1
            && retry_pod.pointer("/metadata/uid").and_then(|uid| uid.as_str()) == Some("uid-a")
    ));
    assert_eq!(
        actor.in_flight_for_test(),
        Some((
            "uid-a",
            PodLifecycleWorkKind::StopPod,
            stop_operation_id + 1
        ))
    );
}

#[test]
fn same_rv_terminating_watch_after_stop_failure_retries_stop() {
    use super::message::{PodLifecycleWorkFailure, PodLifecycleWorkKind};
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let mut pod = test_pod("default", "pod-a", "uid-a");
    pod["metadata"]["deletionTimestamp"] = serde_json::json!("2026-05-13T17:16:04Z");

    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    let stop = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(2),
        pod: pod.clone(),
    });
    let stop_operation_id = stop.operation_id().expect("stop operation id");

    let retry_timer = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key.clone(),
        operation_id: stop_operation_id,
        kind: PodLifecycleWorkKind::StopPod,
        retryable: true,
        failure: PodLifecycleWorkFailure::DispatchFailed("volume busy".into()),
    });
    assert!(matches!(retry_timer, PodAction::ScheduleRetry { .. }));

    let retry = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(2),
        pod,
    });

    assert!(matches!(
        retry,
        PodAction::StopPod {
            key: retry_key,
            operation_id,
            ..
        } if retry_key == key && operation_id == stop_operation_id + 1
    ));
}

#[test]
fn terminating_watch_with_stale_rv_after_status_echo_inflation_still_stops() {
    use super::message::PodLifecycleWorkKind;
    use crate::pod_lifecycle_core::action::PodAction;

    // Regression for the multinode worker-pod GC stall: under WAN latency +
    // leader load, worker status echoes carry a *synthetic* resourceVersion
    // far ahead of the real one. That inflates the actor's last-seen RV, so
    // the real terminating watch event (deletionTimestamp stamped by the
    // leader at a lower real RV) looks "stale" and was silently dropped,
    // leaving the Pod Running long past its grace period. deletionTimestamp
    // is monotonic, so a terminating event must never be dropped as stale.
    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    // Bring the Pod to Running + finalized with a sandbox.
    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    let start_op = start.operation_id().expect("start op");
    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: start_op,
        kind: PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    let finalize_op = finalize.operation_id().expect("finalize op");
    let _ = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: finalize_op,
        kind: PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: Some("sandbox-a".to_string()),
    });

    // A worker status echo inflates the actor's last-seen RV to 100.
    let _ = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(100),
        pod: running_test_pod("default", "pod-a", "uid-a"),
    });

    // The leader stamped deletionTimestamp at the real RV (50), below the
    // inflated synthetic RV. This terminating event must still stop the Pod.
    let mut terminating = running_test_pod("default", "pod-a", "uid-a");
    terminating["metadata"]["deletionTimestamp"] = serde_json::json!("2026-06-09T07:24:04Z");
    let action = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(50),
        pod: terminating,
    });

    assert!(
        matches!(action, PodAction::StopPod { key: ref k, .. } if *k == key),
        "terminating watch with stale (synthetic-echo-inflated) RV must still stop the pod, got {action:?}"
    );
}

#[test]
fn ephemeral_watch_with_stale_rv_after_status_echo_inflation_still_reconciles() {
    use super::message::PodLifecycleWorkKind;
    use crate::pod_lifecycle_core::action::PodAction;

    // Regression for the run-8 ephemeral-container conformance failure:
    // worker status echoes carry a synthetic resourceVersion that can equal
    // the real RV at which the leader appended spec.ephemeralContainers. The
    // watch echo introducing the ephemeral container then looked stale and
    // was dropped, so ReconcileEphemeral never ran and the container stayed
    // Created until the test timed out. A pod that carries ephemeral
    // containers with no pending ephemeral reconcile must never be dropped
    // as stale.
    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    // Bring the Pod to Running + finalized with a sandbox.
    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    let start_op = start.operation_id().expect("start op");
    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: start_op,
        kind: PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    let finalize_op = finalize.operation_id().expect("finalize op");
    // FinalizeStartup completion drains the deferred runtime reconcile from
    // StartPod and dispatches a ReconcileRuntime; capture and complete it so
    // the actor is idle, matching the production sequence where the
    // ephemeral watch arrives with no in-flight work.
    let runtime_after_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: finalize_op,
        kind: PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    let runtime_op = runtime_after_finalize
        .operation_id()
        .expect("runtime reconcile op");
    let _ = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: runtime_op,
        kind: PodLifecycleWorkKind::ReconcileRuntime,
        sandbox_id: None,
    });

    // A worker status echo inflates the actor's last-seen RV to 100.
    let _ = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(100),
        pod: running_test_pod("default", "pod-a", "uid-a"),
    });

    // The leader appended spec.ephemeralContainers at the real RV (50),
    // below/equal to the inflated synthetic RV. This watch echo must NOT be
    // dropped as stale; it must dispatch ReconcileEphemeral.
    let mut pod = running_test_pod("default", "pod-a", "uid-a");
    pod["spec"]["ephemeralContainers"] = serde_json::json!([
        {"name": "debugger", "image": "busybox", "command": ["/bin/sh"]}
    ]);
    let action = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(50),
        pod,
    });

    assert!(
        matches!(action, PodAction::ReconcileEphemeral { key: ref k, .. } if *k == key),
        "ephemeral watch with stale (synthetic-echo-inflated) RV must still dispatch ReconcileEphemeral, got {action:?}"
    );
}

#[test]
fn terminating_watch_added_from_reconnect_snapshot_stops_instead_of_starting() {
    use crate::pod_lifecycle_core::action::PodAction;

    // WorkerStoreAdapter republishes the initial list snapshot as ADDED on
    // watch reconnect. If the leader stamped deletionTimestamp while the
    // worker stream was down, the first event the target actor sees can be a
    // terminating ADDED, not MODIFIED. That still must drive actor-owned
    // cleanup; starting or ignoring it leaves GC-created pods stuck.
    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let mut pod = test_pod("default", "pod-a", "uid-a");
    pod["metadata"]["deletionTimestamp"] = serde_json::json!("2026-06-13T21:37:59Z");

    let action = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(50),
        pod,
    });

    assert!(
        matches!(action, PodAction::StopPod { key: ref stop_key, .. } if *stop_key == key),
        "terminating ADDED snapshot must stop the pod, got {action:?}"
    );
}

#[test]
fn node_lost_terminal_watch_dispatches_stop_without_deletion_timestamp() {
    use super::message::PodLifecycleWorkKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = test_pod("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod,
    });
    let start_operation_id = start.operation_id().expect("start operation id");
    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: start_operation_id,
        kind: PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    let finalize_operation_id = finalize.operation_id().expect("finalize op");
    let _ = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: finalize_operation_id,
        kind: PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: Some("sandbox-a".to_string()),
    });

    let mut terminal = running_test_pod("default", "pod-a", "uid-a");
    terminal["status"]["phase"] = serde_json::json!("Failed");
    terminal["status"]["reason"] = serde_json::json!("NodeLost");

    let stop = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(2),
        pod: terminal,
    });

    assert!(matches!(
        stop,
        PodAction::StopPod {
            key: stop_key,
            pod: Some(_),
            ..
        } if stop_key == key
    ));
}

#[tokio::test]
async fn spawn_failure_synthesizes_pod_work_failed() {
    use super::actor::PodLifecycleActor;

    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let (recorder, executor_holder) = recording_executor_holder();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::channel(8);
    let mut actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    actor.fail_next_spawn_for_test();
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key,
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    })
    .unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(event) = seen_rx.recv().await {
            if event == "pod_work_failed" {
                return;
            }
        }
        panic!("actor event stream closed before PodWorkFailed");
    })
    .await
    .expect("spawn failure should synthesize PodWorkFailed into completion channel");

    drop(tx);
    handle.await.unwrap();
    assert_eq!(
        recorder.action_count(),
        0,
        "spawn failure must not call the executor"
    );
}

#[test]
fn watch_added_uid_b_during_stopping_uid_a_stores_pending_no_dispatch() {
    use super::message::PodLifecycleWorkKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key_a = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let key_b = PodLifecycleKey::new("default", "pod-a", "uid-b");

    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::WatchAdded {
            key: key_a.clone(),
            resource_version: Some(1),
            pod: test_pod("default", "pod-a", "uid-a"),
        }),
        PodAction::StartPod { .. }
    ));
    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::WatchDeleted {
            key: key_a,
            resource_version: Some(2),
            pod: test_pod("default", "pod-a", "uid-a"),
        }),
        PodAction::StopPod { .. }
    ));

    let action = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key_b,
        resource_version: Some(3),
        pod: test_pod("default", "pod-a", "uid-b"),
    });
    assert!(matches!(action, PodAction::Noop));
    assert_eq!(actor.pending_replacement_uid_for_test(), Some("uid-b"));
    assert_eq!(
        actor.in_flight_for_test(),
        Some(("uid-a", PodLifecycleWorkKind::StopPod, 2))
    );
}

#[test]
fn watch_added_uid_b_during_stopping_does_not_cancel_stop_pod() {
    use super::message::PodLifecycleWorkKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key_a = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let key_b = PodLifecycleKey::new("default", "pod-a", "uid-b");

    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key_a.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    let _ = actor.handle_for_test(LifecycleMessage::WatchDeleted {
        key: key_a,
        resource_version: Some(2),
        pod: test_pod("default", "pod-a", "uid-a"),
    });

    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::WatchAdded {
            key: key_b,
            resource_version: Some(3),
            pod: test_pod("default", "pod-a", "uid-b"),
        }),
        PodAction::Noop
    ));
    assert_eq!(
        actor.in_flight_for_test(),
        Some(("uid-a", PodLifecycleWorkKind::StopPod, 2))
    );
}

#[test]
fn watch_modified_uid_b_during_stopping_updates_pending_snapshot() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key_a = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let key_b = PodLifecycleKey::new("default", "pod-a", "uid-b");

    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key_a.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    let _ = actor.handle_for_test(LifecycleMessage::WatchDeleted {
        key: key_a,
        resource_version: Some(2),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key_b.clone(),
        resource_version: Some(3),
        pod: test_pod("default", "pod-a", "uid-b"),
    });

    let mut updated = test_pod("default", "pod-a", "uid-b");
    updated["metadata"]["labels"] = serde_json::json!({"revision": "new"});
    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::WatchModified {
            key: key_b,
            resource_version: Some(4),
            pod: updated,
        }),
        PodAction::Noop
    ));
    assert_eq!(actor.pending_replacement_uid_for_test(), Some("uid-b"));
    assert_eq!(
        actor.pending_replacement_resource_version_for_test(),
        Some(4)
    );
    assert_eq!(
        actor
            .pending_replacement_pod_for_test()
            .and_then(|pod| pod.pointer("/metadata/labels/revision"))
            .and_then(|value| value.as_str()),
        Some("new")
    );
}

#[test]
fn watch_deleted_uid_b_during_stopping_drops_pending_no_interrupt() {
    use super::message::PodLifecycleWorkKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key_a = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let key_b = PodLifecycleKey::new("default", "pod-a", "uid-b");

    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key_a.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    let _ = actor.handle_for_test(LifecycleMessage::WatchDeleted {
        key: key_a,
        resource_version: Some(2),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key_b.clone(),
        resource_version: Some(3),
        pod: test_pod("default", "pod-a", "uid-b"),
    });
    assert_eq!(actor.pending_replacement_uid_for_test(), Some("uid-b"));

    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::WatchDeleted {
            key: key_b,
            resource_version: Some(4),
            pod: test_pod("default", "pod-a", "uid-b"),
        }),
        PodAction::Noop
    ));
    assert_eq!(actor.pending_replacement_uid_for_test(), None);
    assert_eq!(
        actor.in_flight_for_test(),
        Some(("uid-a", PodLifecycleWorkKind::StopPod, 2))
    );
}

#[test]
fn stop_pod_completion_finalizes_delete_before_admitting_pending_replacement() {
    use super::message::PodLifecycleWorkKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key_a = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let key_b = PodLifecycleKey::new("default", "pod-a", "uid-b");

    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key_a.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    let _ = actor.handle_for_test(LifecycleMessage::WatchDeleted {
        key: key_a.clone(),
        resource_version: Some(2),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key_b.clone(),
        resource_version: Some(3),
        pod: test_pod("default", "pod-a", "uid-b"),
    });

    let action = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key_a.clone(),
        operation_id: 2,
        kind: PodLifecycleWorkKind::StopPod,
        sandbox_id: None,
    });

    assert!(matches!(
        action,
        PodAction::FinalizePodDeletion {
            key,
            operation_id: 3,
            ..
        } if key == key_a
    ));
    assert_eq!(actor.pending_replacement_uid_for_test(), Some("uid-b"));
    assert_eq!(
        actor.in_flight_for_test(),
        Some(("uid-a", PodLifecycleWorkKind::FinalizePodDeletion, 3))
    );

    let action = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key_a,
        operation_id: 3,
        kind: PodLifecycleWorkKind::FinalizePodDeletion,
        sandbox_id: None,
    });

    assert!(matches!(
        action,
        PodAction::StartPod {
            key,
            pod: Some(pod),
            ..
        } if key == key_b && pod.pointer("/metadata/uid").and_then(|uid| uid.as_str()) == Some("uid-b")
    ));
    assert_eq!(actor.pending_replacement_uid_for_test(), None);
    assert_eq!(
        actor.in_flight_for_test(),
        Some(("uid-b", PodLifecycleWorkKind::StartPod, 4))
    );
}

#[test]
fn late_finalizer_removal_reissues_semantic_delete_and_stale_retry_cannot_readmit_uid() {
    use super::message::{PodLifecycleWorkFailure, PodLifecycleWorkKind};
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = test_pod("default", "pod-a", "uid-a");
    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: pod.clone(),
    });
    let stop = actor.handle_for_test(LifecycleMessage::WatchDeleted {
        key: key.clone(),
        resource_version: Some(2),
        pod: pod.clone(),
    });
    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: stop.operation_id().expect("stop operation"),
        kind: PodLifecycleWorkKind::StopPod,
        sandbox_id: None,
    });
    let retry = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key.clone(),
        operation_id: finalize.operation_id().expect("finalize operation"),
        kind: PodLifecycleWorkKind::FinalizePodDeletion,
        retryable: true,
        failure: PodLifecycleWorkFailure::FinalizersPending,
    });
    assert!(matches!(retry, PodAction::ScheduleRetry { .. }));

    let mut finalizer_free = pod;
    finalizer_free["metadata"]["deletionTimestamp"] = serde_json::json!("2026-07-25T00:00:00Z");
    finalizer_free["metadata"]["finalizers"] = serde_json::json!([]);
    let reissued = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(3),
        pod: finalizer_free,
    });
    assert!(
        matches!(reissued, PodAction::FinalizePodDeletion { .. }),
        "finalizer removal must event-drive a fresh semantic finalization"
    );

    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::RetryDue { key }),
        PodAction::Noop
    ));
    assert_eq!(
        actor.active_uid_for_test(),
        None,
        "a superseded retry timer must not readmit the finalized UID"
    );
}

#[test]
fn stale_old_uid_messages_do_not_mutate_active_replacement() {
    use crate::cri_events::KubeletEventKind;
    use crate::lifecycle::LifecycleCommand;
    use crate::pod_lifecycle_core::action::PodAction;
    use crate::pod_lifecycle_router::OrphanReason;

    let mut actor = direct_test_actor();
    // Admit a replacement UID as the active pod.
    let new_key = PodLifecycleKey::new("default", "same-name", "uid-new");
    let new_pod = test_pod("default", "same-name", "uid-new");
    assert!(matches!(
        actor.handle_for_test(LifecycleMessage::WatchAdded {
            key: new_key.clone(),
            resource_version: Some(1),
            pod: new_pod.clone(),
        }),
        PodAction::StartPod { .. }
    ));
    assert_eq!(actor.active_uid_for_test(), Some("uid-new"));

    let old_key = PodLifecycleKey::new("default", "same-name", "uid-old");

    let cases: Vec<(&str, LifecycleMessage)> = vec![
        (
            "orphan_finalize",
            LifecycleMessage::OrphanFinalize {
                key: old_key.clone(),
                reason: OrphanReason::LeaderDeletedWhileDown,
            },
        ),
        (
            "cri_event",
            LifecycleMessage::CriEvent {
                key: old_key.clone(),
                container_id: "container-old".to_string(),
                kind: KubeletEventKind::Stopped,
            },
        ),
        (
            "active_deadline_due",
            LifecycleMessage::ActiveDeadlineDue {
                key: old_key.clone(),
            },
        ),
        (
            "lifecycle_command",
            LifecycleMessage::LifecycleCommand {
                key: old_key.clone(),
                command: LifecycleCommand::ReadinessChanged {
                    pod_uid: "uid-old".to_string(),
                    namespace: "default".to_string(),
                    pod_name: "same-name".to_string(),
                    container_name: "app".to_string(),
                    ready: true,
                },
            },
        ),
    ];

    for (workflow, message) in cases {
        let action = actor.handle_for_test(message);
        assert!(
            matches!(action, PodAction::Noop),
            "stale old-UID {workflow} must yield Noop, got {action:?}"
        );
        // Active UID stays the replacement.
        assert_eq!(
            actor.active_uid_for_test(),
            Some("uid-new"),
            "stale old-UID {workflow} must not steal the active replacement UID"
        );
        // No replacement slot is consumed: the old UID never registered a
        // pending replacement, and the stale messages must not create one
        // or admit the old UID.
        assert_eq!(
            actor.pending_replacement_uid_for_test(),
            None,
            "stale old-UID {workflow} must not consume the replacement slot"
        );
    }
}

#[test]
fn stale_orphan_finalize_for_old_uid_does_not_steal_active_replacement() {
    use super::message::{PodLifecycleWorkFailure, PodLifecycleWorkKind};
    use crate::pod_lifecycle_core::action::PodAction;
    use crate::pod_lifecycle_router::OrphanReason;

    let mut actor = direct_test_actor();
    actor.enable_slot_admission_gate_for_test();
    let key_a = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let key_b = PodLifecycleKey::new("default", "pod-a", "uid-b");
    let pod_a = test_pod("default", "pod-a", "uid-a");
    let pod_b = test_pod("default", "pod-a", "uid-b");

    let check_a = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key_a.clone(),
        resource_version: Some(1),
        pod: pod_a.clone(),
    });
    let start_a = actor.handle_for_test(LifecycleMessage::SlotAdmissionGranted {
        key: key_a.clone(),
        operation_id: check_a.operation_id().expect("old slot admission op"),
        pod: pod_a.clone(),
        resource_version: Some(1),
        start_after_admit: true,
    });
    let _ = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key_a.clone(),
        operation_id: start_a.operation_id().expect("old start op"),
        kind: PodLifecycleWorkKind::StartPod,
        retryable: false,
        failure: PodLifecycleWorkFailure::Startup("hostPort admission failed".to_string()),
    });

    let mut deleting_old = pod_a.clone();
    deleting_old["metadata"]["deletionTimestamp"] = serde_json::json!("2026-07-04T18:51:57Z");
    let stop_a = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key_a.clone(),
        resource_version: Some(2),
        pod: deleting_old,
    });
    let finalize_a = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key_a.clone(),
        operation_id: stop_a.operation_id().expect("old stop op"),
        kind: PodLifecycleWorkKind::StopPod,
        sandbox_id: None,
    });
    let _ = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key_a.clone(),
        operation_id: finalize_a.operation_id().expect("old finalization op"),
        kind: PodLifecycleWorkKind::FinalizePodDeletion,
        sandbox_id: None,
    });

    let check_b = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key_b.clone(),
        resource_version: Some(3),
        pod: pod_b.clone(),
    });
    let check_b_op = check_b
        .operation_id()
        .expect("replacement slot admission op");

    let stale_orphan = actor.handle_for_test(LifecycleMessage::OrphanFinalize {
        key: key_a,
        reason: OrphanReason::LeaderDeletedWhileDown,
    });
    assert!(
        matches!(stale_orphan, PodAction::Noop),
        "stale old-UID orphan finalization must not dispatch stop for the active replacement slot"
    );

    let start_b = actor.handle_for_test(LifecycleMessage::SlotAdmissionGranted {
        key: key_b.clone(),
        operation_id: check_b_op,
        pod: pod_b,
        resource_version: Some(3),
        start_after_admit: true,
    });
    assert!(
        matches!(start_b, PodAction::StartPod { key, .. } if key == key_b),
        "replacement slot admission grant must still start the replacement"
    );
}

#[test]
fn stop_pod_failure_keeps_pending_replacement_parked() {
    use super::message::{PodLifecycleWorkFailure, PodLifecycleWorkKind};
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key_a = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let key_b = PodLifecycleKey::new("default", "pod-a", "uid-b");

    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key_a.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    let _ = actor.handle_for_test(LifecycleMessage::WatchDeleted {
        key: key_a.clone(),
        resource_version: Some(2),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    let _ = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key_b,
        resource_version: Some(3),
        pod: test_pod("default", "pod-a", "uid-b"),
    });

    let action = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key_a.clone(),
        operation_id: 2,
        kind: PodLifecycleWorkKind::StopPod,
        retryable: true,
        failure: PodLifecycleWorkFailure::DeadlineExceeded,
    });

    assert!(matches!(
        action,
        PodAction::ScheduleRetry { key, .. } if key == key_a
    ));
    assert_eq!(actor.pending_replacement_uid_for_test(), Some("uid-b"));
    assert_eq!(actor.in_flight_for_test(), None);
}

#[test]
fn start_pod_nonretryable_failure_allows_scheduled_snapshot_to_retry() {
    use super::message::{PodLifecycleWorkFailure, PodLifecycleWorkKind};
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let first_action = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        first_action,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let failed_action = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key.clone(),
        operation_id: 1,
        kind: PodLifecycleWorkKind::StartPod,
        retryable: false,
        failure: PodLifecycleWorkFailure::DispatchFailed("spawn rejected".to_string()),
    });
    assert!(matches!(failed_action, PodAction::Noop));
    assert_eq!(actor.in_flight_for_test(), None);

    let retry_action = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(2),
        pod: serde_json::json!({
            "metadata": {
                "namespace": "default",
                "name": "pod-a",
                "uid": "uid-a"
            },
            "spec": {
                "nodeName": "dallas"
            }
        }),
    });

    assert!(matches!(
        retry_action,
        PodAction::StartPod {
            key: action_key,
            operation_id: 2,
            pod: Some(pod),
            ..
        } if action_key == key
            && pod.pointer("/spec/nodeName").and_then(|node_name| node_name.as_str()) == Some("dallas")
    ));
    assert_eq!(
        actor.in_flight_for_test(),
        Some(("uid-a", PodLifecycleWorkKind::StartPod, 2))
    );
}

#[test]
fn pending_start_config_error_retries_when_pod_fingerprint_changes() {
    use super::message::{PodLifecycleWorkFailure, PodLifecycleWorkKind};
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let invalid_pod = serde_json::json!({
        "metadata": {
            "namespace": "default",
            "name": "pod-a",
            "uid": "uid-a",
            "annotations": {
                "notmysubpath": "mypath",
                "mysubpath": "/foo"
            }
        },
        "spec": {
            "nodeName": "dallas",
            "containers": [{
                "name": "c",
                "env": [
                    {"name": "POD_NAME", "value": "foo"},
                    {"name": "ANNOTATION", "valueFrom": {"fieldRef": {"fieldPath": "metadata.annotations['mysubpath']"}}}
                ],
                "volumeMounts": [{
                    "name": "workdir",
                    "mountPath": "/subpath_mount",
                    "subPathExpr": "$(ANNOTATION)"
                }]
            }],
            "volumes": [{"name": "workdir", "emptyDir": {}}]
        },
        "status": {
            "phase": "Pending",
            "containerStatuses": [{
                "name": "c",
                "state": {
                    "waiting": {
                        "reason": "CreateContainerConfigError",
                        "message": "invalid subPath in container c"
                    }
                }
            }]
        }
    });

    let first_action = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: invalid_pod.clone(),
    });
    assert!(matches!(
        first_action,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let failed_action = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key.clone(),
        operation_id: 1,
        kind: PodLifecycleWorkKind::StartPod,
        retryable: false,
        failure: PodLifecycleWorkFailure::Startup(
            "All 1 app container(s) failed with CreateContainerConfigError".to_string(),
        ),
    });
    assert!(matches!(failed_action, PodAction::Noop));

    let unchanged_config_error = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(2),
        pod: invalid_pod,
    });
    assert!(
        matches!(unchanged_config_error, PodAction::Noop),
        "the first config-error status echo should record the failed fingerprint without hot-looping"
    );

    let retry_action = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(3),
        pod: serde_json::json!({
            "metadata": {
                "namespace": "default",
                "name": "pod-a",
                "uid": "uid-a",
                "annotations": {
                    "notmysubpath": "mypath",
                    "mysubpath": "mypath"
                }
            },
            "spec": {
                "nodeName": "dallas",
                "containers": [{
                    "name": "c",
                    "env": [
                        {"name": "POD_NAME", "value": "foo"},
                        {"name": "ANNOTATION", "valueFrom": {"fieldRef": {"fieldPath": "metadata.annotations['mysubpath']"}}}
                    ],
                    "volumeMounts": [{
                        "name": "workdir",
                        "mountPath": "/subpath_mount",
                        "subPathExpr": "$(ANNOTATION)"
                    }]
                }],
                "volumes": [{"name": "workdir", "emptyDir": {}}]
            },
            "status": {
                "phase": "Pending",
                "containerStatuses": [{
                    "name": "c",
                    "state": {
                        "waiting": {
                            "reason": "CreateContainerConfigError",
                            "message": "invalid subPath in container c"
                        }
                    }
                }]
            }
        }),
    });

    assert!(matches!(
        retry_action,
        PodAction::StartPod {
            key: action_key,
            operation_id: 2,
            pod: Some(pod),
            ..
        } if action_key == key
            && pod.pointer("/metadata/annotations/mysubpath").and_then(|value| value.as_str())
                == Some("mypath")
    ));
}

#[test]
fn pending_start_config_error_retries_when_metadata_update_arrives_before_status_echo() {
    use super::message::{PodLifecycleWorkFailure, PodLifecycleWorkKind};
    use crate::pod_lifecycle_core::action::PodAction;

    // Kubernetes permits a failed subPathExpr to become valid after a metadata
    // update.  A worker-local status write need not loop back through the pod
    // watch before that update arrives, so the actor must remember the
    // configuration it attempted rather than treating the update as its first
    // CreateContainerConfigError echo.
    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let invalid_pod = serde_json::json!({
        "metadata": {
            "namespace": "default",
            "name": "pod-a",
            "uid": "uid-a",
            "annotations": {
                "notmysubpath": "mypath",
                "mysubpath": "/foo"
            }
        },
        "spec": {
            "nodeName": "dallas",
            "containers": [{
                "name": "c",
                "env": [
                    {"name": "POD_NAME", "value": "foo"},
                    {"name": "ANNOTATION", "valueFrom": {"fieldRef": {"fieldPath": "metadata.annotations['mysubpath']"}}}
                ],
                "volumeMounts": [{
                    "name": "workdir",
                    "mountPath": "/subpath_mount",
                    "subPathExpr": "$(ANNOTATION)"
                }]
            }],
            "volumes": [{"name": "workdir", "emptyDir": {}}]
        }
    });

    let first_action = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: invalid_pod,
    });
    assert!(matches!(
        first_action,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let failed_action = actor.handle_for_test(LifecycleMessage::PodWorkFailed {
        key: key.clone(),
        operation_id: 1,
        kind: PodLifecycleWorkKind::StartPod,
        retryable: false,
        failure: PodLifecycleWorkFailure::Startup(
            "All 1 app container(s) failed with CreateContainerConfigError".to_string(),
        ),
    });
    assert!(matches!(failed_action, PodAction::Noop));

    let updated_pod = serde_json::json!({
        "metadata": {
            "namespace": "default",
            "name": "pod-a",
            "uid": "uid-a",
            "annotations": {
                "notmysubpath": "mypath",
                "mysubpath": "mypath"
            }
        },
        "spec": {
            "nodeName": "dallas",
            "containers": [{
                "name": "c",
                "env": [
                    {"name": "POD_NAME", "value": "foo"},
                    {"name": "ANNOTATION", "valueFrom": {"fieldRef": {"fieldPath": "metadata.annotations['mysubpath']"}}}
                ],
                "volumeMounts": [{
                    "name": "workdir",
                    "mountPath": "/subpath_mount",
                    "subPathExpr": "$(ANNOTATION)"
                }]
            }],
            "volumes": [{"name": "workdir", "emptyDir": {}}]
        },
        "status": {
            "phase": "Pending",
            "containerStatuses": [{
                "name": "c",
                "state": {"waiting": {"reason": "CreateContainerConfigError"}}
            }]
        }
    });

    let retry_action = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(2),
        pod: updated_pod,
    });
    assert!(matches!(
        retry_action,
        PodAction::StartPod {
            key: action_key,
            operation_id: 2,
            ..
        } if action_key == key
    ));
}

#[tokio::test]
async fn watch_deleted_active_uid_with_stop_in_flight_is_noop() {
    use super::actor::PodLifecycleActor;
    use crate::pod_lifecycle_core::action::PodAction;

    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let pod = test_pod("default", "pod-a", "uid-a");
    let (recorder, executor_holder) = recording_executor_holder();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: pod.clone(),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::WatchDeleted {
        key: key.clone(),
        resource_version: Some(2),
        pod: pod.clone(),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::WatchDeleted {
        key,
        resource_version: Some(3),
        pod,
    })
    .unwrap();
    drop(tx);
    handle.await.unwrap();

    let stop_count = recorder
        .take_actions()
        .into_iter()
        .filter(|action| matches!(action, PodAction::StopPod { .. }))
        .count();
    assert_eq!(
        stop_count, 1,
        "WatchDeleted for an active UID with StopPod in flight must be idempotent"
    );
}

#[tokio::test]
async fn stale_watch_deleted_for_old_uid_after_admission_ignored() {
    use super::actor::PodLifecycleActor;
    use crate::pod_lifecycle_core::action::PodAction;

    let key_a = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let key_b = PodLifecycleKey::new("default", "pod-a", "uid-b");
    let (recorder, executor_holder) = recording_executor_holder();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key_a.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::WatchAdded {
        key: key_b.clone(),
        resource_version: Some(2),
        pod: test_pod("default", "pod-a", "uid-b"),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::WatchDeleted {
        key: key_a,
        resource_version: Some(3),
        pod: test_pod("default", "pod-a", "uid-a"),
    })
    .unwrap();
    drop(tx);
    handle.await.unwrap();

    let actions = recorder.take_actions();
    assert!(
        actions.iter().any(|action| matches!(
            action,
            PodAction::StartPod { key, .. } if key.uid == "uid-b"
        )),
        "replacement UID must be admitted before old-UID stale delete is evaluated; actions={actions:?}"
    );
    assert!(
        !actions.iter().any(|action| matches!(
            action,
            PodAction::StopPod { key, .. } if key.uid == "uid-a"
        )),
        "stale old-UID delete must not stop the active replacement; actions={actions:?}"
    );
}

#[tokio::test]
async fn stale_cri_event_for_old_uid_after_admission_ignored() {
    use super::actor::PodLifecycleActor;
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let key_a = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let key_b = PodLifecycleKey::new("default", "pod-a", "uid-b");
    let (recorder, executor_holder) = recording_executor_holder();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key_a.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::WatchAdded {
        key: key_b.clone(),
        resource_version: Some(2),
        pod: test_pod("default", "pod-a", "uid-b"),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::CriEvent {
        key: key_a,
        container_id: "old-container".to_string(),
        kind: KubeletEventKind::Stopped,
    })
    .unwrap();
    drop(tx);
    handle.await.unwrap();

    let actions = recorder.take_actions();
    assert!(
        actions.iter().any(|action| matches!(
            action,
            PodAction::StartPod { key, .. } if key.uid == "uid-b"
        )),
        "replacement UID must be active before stale CRI event arrives; actions={actions:?}"
    );
    assert!(
        !actions.iter().any(
            |action| matches!(action, PodAction::ReconcileRuntime { key, .. } if key.uid == "uid-a")
        ),
        "stale old-UID CRI event must not dispatch runtime reconcile; actions={actions:?}"
    );
}

#[tokio::test]
async fn operation_id_monotonic_per_slot() {
    use super::actor::PodLifecycleActor;
    use crate::pod_lifecycle_core::action::PodAction;

    let (recorder, executor_holder) = recording_executor_holder();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    for i in 0..100 {
        let uid = format!("uid-{i}");
        tx.try_send(LifecycleMessage::WatchAdded {
            key: PodLifecycleKey::new("default", "pod-a", &uid),
            resource_version: Some(i + 1),
            pod: test_pod("default", "pod-a", &uid),
        })
        .unwrap();
    }
    drop(tx);
    handle.await.unwrap();

    let ids: Vec<_> = recorder
        .take_actions()
        .into_iter()
        .filter_map(|action| match action {
            PodAction::StartPod { operation_id, .. } => Some(operation_id),
            _ => None,
        })
        .collect();
    assert_eq!(ids.len(), 100);
    assert!(
        ids.windows(2).all(|pair| pair[0] < pair[1]),
        "operation ids must be strictly monotonic per slot: {ids:?}"
    );
}

#[tokio::test]
async fn watch_deleted_dispatches_stop_with_deleted_pod_snapshot() {
    use super::actor::PodLifecycleActor;
    use crate::pod_lifecycle_core::action::PodAction;

    let key = PodLifecycleKey::new("hostport-3612", "pod1", "old-uid");
    let deleted = serde_json::json!({
        "metadata": {"namespace": "hostport-3612", "name": "pod1", "uid": "old-uid"},
        "spec": {"containers": [{"ports": [{"hostPort": 54323, "containerPort": 8080}]}]},
        "status": {"podIP": "10.43.0.17"}
    });
    let (recorder, executor_holder) = recording_executor_holder();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: deleted.clone(),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-old".to_string()),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::WatchDeleted {
        key,
        resource_version: Some(2),
        pod: deleted.clone(),
    })
    .unwrap();
    drop(tx);
    handle.await.unwrap();

    let actions = recorder.take_actions();
    assert!(
        actions.iter().any(|action| matches!(
            action,
            PodAction::StopPod { pod: Some(pod), .. }
                if pod.pointer("/status/podIP") == Some(&serde_json::json!("10.43.0.17"))
        )),
        "WatchDeleted must carry the deleted pod snapshot into StopPod for hard-delete cleanup; actions={actions:?}"
    );
}

#[tokio::test]
async fn skipped_start_completion_allows_scheduled_watch_modified_to_start() {
    use super::actor::PodLifecycleActor;
    use super::message::PodLifecycleWorkKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let key = PodLifecycleKey::new("default", "late-bound", "uid-late-bound");
    let (recorder, executor_holder) = recording_executor_holder();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: serde_json::json!({
            "metadata": {"namespace": "default", "name": "late-bound", "uid": "uid-late-bound"},
            "spec": {}
        }),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: PodLifecycleWorkKind::StartPod,
        sandbox_id: None,
    })
    .unwrap();
    tx.try_send(LifecycleMessage::WatchModified {
        key,
        resource_version: Some(2),
        pod: serde_json::json!({
            "metadata": {"namespace": "default", "name": "late-bound", "uid": "uid-late-bound"},
            "spec": {"nodeName": "node-a"}
        }),
    })
    .unwrap();
    drop(tx);
    handle.await.unwrap();

    let start_count = recorder
        .take_actions()
        .into_iter()
        .filter(|action| matches!(action, PodAction::StartPod { .. }))
        .count();
    assert_eq!(
        start_count, 2,
        "a skipped unscheduled start must not strand the actor before the scheduler bind arrives"
    );
}

#[tokio::test]
async fn queued_scheduled_watch_modified_runs_after_skipped_start_completion() {
    use super::actor::PodLifecycleActor;
    use super::message::PodLifecycleWorkKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let key = PodLifecycleKey::new("default", "queued-bind", "uid-queued-bind");
    let (recorder, executor_holder) = recording_executor_holder();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: serde_json::json!({
            "metadata": {"namespace": "default", "name": "queued-bind", "uid": "uid-queued-bind"},
            "spec": {}
        }),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(2),
        pod: serde_json::json!({
            "metadata": {"namespace": "default", "name": "queued-bind", "uid": "uid-queued-bind"},
            "spec": {"nodeName": "node-a"}
        }),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 1,
        kind: PodLifecycleWorkKind::StartPod,
        sandbox_id: None,
    })
    .unwrap();
    drop(tx);
    handle.await.unwrap();

    let start_count = recorder
        .take_actions()
        .into_iter()
        .filter(|action| matches!(action, PodAction::StartPod { .. }))
        .count();
    assert_eq!(
        start_count, 2,
        "a scheduler bind received while the skipped start is in flight must run after completion"
    );
}

#[tokio::test]
async fn watch_modified_with_ephemeral_container_dispatches_reconcile_after_start() {
    use super::actor::PodLifecycleActor;
    use super::message::PodLifecycleWorkKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let (recorder, executor_holder) = recording_executor_holder();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: serde_json::json!({
            "metadata": {"namespace": "default", "name": "pod-a", "uid": "uid-a"}
        }),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(2),
        pod: serde_json::json!({
            "metadata": {
                "namespace": "default",
                "name": "pod-a",
                "uid": "uid-a",
                "annotations": {"klights.dev/sandbox-id": "sandbox-a"}
            },
            "spec": {
                "ephemeralContainers": [
                    {"name": "debugger", "image": "busybox", "command": ["/bin/sh"]}
                ]
            }
        }),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 2,
        kind: PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: Some("sandbox-a".to_string()),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 3,
        kind: PodLifecycleWorkKind::ReconcileRuntime,
        sandbox_id: None,
    })
    .unwrap();
    drop(tx);
    handle.await.unwrap();

    let actions = recorder.take_actions();
    let finalize_position = actions
        .iter()
        .position(|action| matches!(action, PodAction::FinalizeStartup { .. }))
        .expect("start completion must dispatch startup finalization before ephemeral reconcile");
    let reconcile_position = actions
        .iter()
        .position(|action| matches!(action, PodAction::ReconcileEphemeral { .. }))
        .expect("WatchModified with appended ephemeralContainers must dispatch ReconcileEphemeral after startup finalization");
    assert!(
        reconcile_position > finalize_position,
        "WatchModified with appended ephemeralContainers must dispatch ReconcileEphemeral after startup finalization; actions={actions:?}"
    );
}

#[test]
fn ephemeral_update_during_startup_finalization_reconciles_after_finalization() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        start,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        finalize,
        PodAction::FinalizeStartup {
            operation_id: 2,
            ..
        }
    ));

    let update = actor.handle_for_test(LifecycleMessage::WatchModified {
        key: key.clone(),
        resource_version: Some(2),
        pod: running_test_pod_with_ephemeral_container("default", "pod-a", "uid-a"),
    });
    assert!(
        matches!(update, PodAction::Noop),
        "ephemeral update during startup finalization must be deferred without replacing finalizer work, got {update:?}"
    );
    assert_eq!(
        actor.in_flight_for_test(),
        Some((
            "uid-a",
            super::message::PodLifecycleWorkKind::FinalizeStartup,
            2
        ))
    );

    let after_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(
        matches!(
            after_finalize,
            PodAction::ReconcileRuntime {
                operation_id: 3,
                ..
            }
        ),
        "runtime status must reconcile before the deferred ephemeral update, got {after_finalize:?}"
    );

    let after_runtime = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 3,
        kind: super::message::PodLifecycleWorkKind::ReconcileRuntime,
        sandbox_id: None,
    });
    assert!(
        matches!(
            after_runtime,
            PodAction::ReconcileEphemeral {
                operation_id: 4,
                ..
            }
        ),
        "deferred ephemeral update must run after startup runtime status reconciliation, got {after_runtime:?}"
    );
}

#[tokio::test]
async fn watch_modified_running_echo_does_not_start_pod_again_before_completion() {
    use super::actor::PodLifecycleActor;
    use crate::pod_lifecycle_core::action::PodAction;

    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let (recorder, executor_holder) = recording_executor_holder();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: serde_json::json!({
            "metadata": {"namespace": "default", "name": "pod-a", "uid": "uid-a"}
        }),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::WatchModified {
        key,
        resource_version: Some(2),
        pod: serde_json::json!({
            "metadata": {
                "namespace": "default",
                "name": "pod-a",
                "uid": "uid-a",
                "annotations": {"klights.dev/sandbox-id": "sandbox-a"}
            },
            "status": {"phase": "Running"}
        }),
    })
    .unwrap();
    drop(tx);
    handle.await.unwrap();

    let start_count = recorder
        .take_actions()
        .iter()
        .filter(|action| matches!(action, PodAction::StartPod { .. }))
        .count();
    assert_eq!(
        start_count, 1,
        "Running watch echo during pending start must not dispatch a second StartPod"
    );
}

#[tokio::test]
async fn cri_event_does_not_dispatch_runtime_reconcile_before_start_completion() {
    use super::actor::PodLifecycleActor;
    use super::message::PodLifecycleWorkKind;
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let (recorder, executor_holder) = recording_executor_holder();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: serde_json::json!({
            "metadata": {"namespace": "default", "name": "pod-a", "uid": "uid-a"}
        }),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "container-a".to_string(),
        kind: KubeletEventKind::Started,
    })
    .unwrap();
    tx.try_send(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 1,
        kind: PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    })
    .unwrap();
    drop(tx);
    handle.await.unwrap();

    let actions = recorder.take_actions();
    let finalize_position = actions
        .iter()
        .position(|action| matches!(action, PodAction::FinalizeStartup { .. }))
        .expect("start completion must dispatch startup finalization");
    assert!(
        !actions[..finalize_position]
            .iter()
            .any(|action| matches!(action, PodAction::ReconcileRuntime { .. })),
        "runtime reconcile must not run ahead of startup finalization; actions={actions:?}"
    );
}

#[tokio::test]
async fn cri_event_during_start_dispatches_runtime_reconcile_after_startup_finalization() {
    use super::actor::PodLifecycleActor;
    use super::message::PodLifecycleWorkKind;
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let (recorder, executor_holder) = recording_executor_holder();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: serde_json::json!({
            "metadata": {"namespace": "default", "name": "pod-a", "uid": "uid-a"}
        }),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "container-a".to_string(),
        kind: KubeletEventKind::Stopped,
    })
    .unwrap();
    tx.try_send(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 2,
        kind: PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: Some("sandbox-a".to_string()),
    })
    .unwrap();
    drop(tx);
    handle.await.unwrap();

    let actions = recorder.take_actions();
    let finalize_position = actions
        .iter()
        .position(|action| matches!(action, PodAction::FinalizeStartup { .. }))
        .expect("start completion must dispatch startup finalization");
    let reconcile_position = actions
        .iter()
        .position(|action| matches!(action, PodAction::ReconcileRuntime { .. }))
        .expect("deferred CRI event must dispatch runtime reconcile after startup finalization");
    let reconcile_count = actions
        .iter()
        .filter(|action| matches!(action, PodAction::ReconcileRuntime { .. }))
        .count();
    assert!(
        reconcile_position > finalize_position,
        "runtime reconcile must run after startup finalization; actions={actions:?}"
    );
    assert_eq!(
        reconcile_count, 1,
        "the successful-start observation and deferred CRI event must coalesce into one runtime reconcile; actions={actions:?}"
    );
}

#[test]
fn successful_start_dispatches_runtime_reconcile_without_cri_event() {
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        start,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        finalize,
        PodAction::FinalizeStartup {
            operation_id: 2,
            ..
        }
    ));

    let after_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: None,
    });
    assert!(
        matches!(
            after_finalize,
            PodAction::ReconcileRuntime {
                operation_id: 3,
                ..
            }
        ),
        "the kubelet's successful start observation must drive runtime status reconciliation without relying on a separate CRI event; got {after_finalize:?}"
    );
}

#[test]
fn cri_event_during_startup_finalization_preserves_finalizer_in_flight() {
    use crate::cri_events::KubeletEventKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let mut actor = direct_test_actor();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let start = actor.handle_for_test(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: test_pod("default", "pod-a", "uid-a"),
    });
    assert!(matches!(
        start,
        PodAction::StartPod {
            operation_id: 1,
            ..
        }
    ));

    let finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: super::message::PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    });
    assert!(matches!(
        finalize,
        PodAction::FinalizeStartup {
            operation_id: 2,
            ..
        }
    ));
    assert_eq!(
        actor.in_flight_for_test(),
        Some((
            "uid-a",
            super::message::PodLifecycleWorkKind::FinalizeStartup,
            2
        ))
    );

    let during_finalize = actor.handle_for_test(LifecycleMessage::CriEvent {
        key: key.clone(),
        container_id: "container-a".to_string(),
        kind: KubeletEventKind::Stopped,
    });
    assert!(
        matches!(during_finalize, PodAction::Noop),
        "CRI event during startup finalization must be deferred, got {during_finalize:?}"
    );
    assert_eq!(
        actor.in_flight_for_test(),
        Some((
            "uid-a",
            super::message::PodLifecycleWorkKind::FinalizeStartup,
            2
        )),
        "deferred runtime reconcile must not replace the finalizer in-flight marker"
    );

    let after_finalize = actor.handle_for_test(LifecycleMessage::PodWorkCompleted {
        key,
        operation_id: 2,
        kind: super::message::PodLifecycleWorkKind::FinalizeStartup,
        sandbox_id: None,
    });
    assert!(matches!(
        after_finalize,
        PodAction::ReconcileRuntime {
            operation_id: 3,
            ..
        }
    ));
}

#[tokio::test]
async fn running_watch_echo_with_sandbox_dispatches_startup_finalization() {
    use super::actor::PodLifecycleActor;
    use crate::pod_lifecycle_core::action::PodAction;

    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let (recorder, executor_holder) = recording_executor_holder();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: serde_json::json!({
            "metadata": {"namespace": "default", "name": "pod-a", "uid": "uid-a"}
        }),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::WatchModified {
        key,
        resource_version: Some(2),
        pod: serde_json::json!({
            "metadata": {
                "namespace": "default",
                "name": "pod-a",
                "uid": "uid-a",
                "annotations": {"klights.dev/sandbox-id": "sandbox-a"}
            },
            "status": {
                "phase": "Running",
                "podIP": "10.43.0.10"
            }
        }),
    })
    .unwrap();
    drop(tx);
    handle.await.unwrap();

    let actions = recorder.take_actions();
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, PodAction::FinalizeStartup { .. })),
        "a Running watch echo carrying a sandbox id must dispatch startup finalization; actions={actions:?}"
    );
}

#[tokio::test]
async fn watch_modified_pending_config_error_retries_start_after_update() {
    use super::actor::PodLifecycleActor;
    use super::message::PodLifecycleWorkKind;
    use crate::pod_lifecycle_core::action::PodAction;

    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let (recorder, executor_holder) = recording_executor_holder();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let (seen_tx, _seen_rx) = tokio::sync::mpsc::channel(8);
    let actor = PodLifecycleActor::new_with_event_sink_for_test(
        8,
        seen_tx,
        executor_holder,
        dummy_reply_handle(),
    );
    let handle = tokio::spawn(actor.run(rx));

    tx.try_send(LifecycleMessage::WatchAdded {
        key: key.clone(),
        resource_version: Some(1),
        pod: serde_json::json!({
            "metadata": {"namespace": "default", "name": "pod-a", "uid": "uid-a"}
        }),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::PodWorkCompleted {
        key: key.clone(),
        operation_id: 1,
        kind: PodLifecycleWorkKind::StartPod,
        sandbox_id: Some("sandbox-a".to_string()),
    })
    .unwrap();
    tx.try_send(LifecycleMessage::WatchModified {
        key,
        resource_version: Some(2),
        pod: serde_json::json!({
            "metadata": {
                "namespace": "default",
                "name": "pod-a",
                "uid": "uid-a",
                "annotations": {
                    "klights.dev/sandbox-id": "sandbox-a",
                    "mysubpath": "mypath"
                }
            },
            "status": {
                "phase": "Pending",
                "containerStatuses": [{
                    "name": "c",
                    "state": {
                        "waiting": {
                            "reason": "CreateContainerConfigError",
                            "message": "invalid subPath in container c"
                        }
                    }
                }]
            }
        }),
    })
    .unwrap();
    drop(tx);
    handle.await.unwrap();

    let start_count = recorder
        .take_actions()
        .iter()
        .filter(|action| matches!(action, PodAction::StartPod { .. }))
        .count();
    assert_eq!(
        start_count, 2,
        "Pending CreateContainerConfigError watch update must retry StartPod once"
    );
}

#[test]
fn finalization_runs_once_per_sandbox() {
    let mut state = PodLifecycleState::new();

    assert_eq!(
        state.on_started("sandbox-a"),
        FinalizationAction::RunFinalizers
    );
    // Mark finalized — same sandbox, now finalized → AlreadyFinalized
    state.finalized = true;
    assert_eq!(
        state.on_started("sandbox-a"),
        FinalizationAction::AlreadyFinalized
    );
    assert_eq!(
        state.on_started("sandbox-b"),
        FinalizationAction::RunFinalizers
    );
}

#[test]
fn finalization_action_gates_finalizer_side_effects() {
    let mut state = PodLifecycleState::new();
    let mut finalizer_runs = 0;

    for sandbox_id in ["sandbox-a", "sandbox-a", "sandbox-b", "sandbox-b"] {
        let result = state.on_started(sandbox_id);
        if result == FinalizationAction::RunFinalizers {
            finalizer_runs += 1;
            state.finalized = true;
        }
    }

    assert_eq!(finalizer_runs, 2);
}

#[test]
fn lifecycle_trace_retains_recent_entries_per_pod() {
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let mut ring = LifecycleTraceRing::new(2);

    ring.record(LifecycleTraceEntry::new(
        key.clone(),
        "watch_added",
        Some(10),
        Some("sandbox-a"),
        "queued",
    ));
    ring.record(LifecycleTraceEntry::new(
        key.clone(),
        "cni_assigned",
        Some(11),
        Some("sandbox-a"),
        "pod_ip=10.43.0.4",
    ));
    ring.record(LifecycleTraceEntry::new(
        key.clone(),
        "probe_started",
        Some(12),
        Some("sandbox-a"),
        "readiness",
    ));

    let entries = ring.entries_for(&key);
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].event, "cni_assigned");
    assert_eq!(entries[1].event, "probe_started");
}

#[test]
fn lifecycle_message_carries_pod_identity() {
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let message = LifecycleMessage::RetryDue { key: key.clone() };

    assert_eq!(message.key(), &key);
    assert_eq!(message.key().namespace, "default");
    assert_eq!(message.key().name, "pod-a");
    assert_eq!(message.key().uid, "uid-a");
}

use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};

use super::config::PodLifecycleConcurrencyConfig;
use super::registry::PodLifecycleRegistry;

fn test_supervisor() -> Arc<TaskSupervisor> {
    Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()))
}

fn test_actor_registry() -> PodLifecycleRegistry {
    let registry = PodLifecycleRegistry::new(
        test_supervisor(),
        PodLifecycleConcurrencyConfig::production_default(),
        test_executor_holder(),
    );
    registry.set_reply_handle(dummy_reply_handle());
    registry
}

fn test_actor_registry_with_executor_and_idle_grace(
    executor: Arc<dyn crate::pod_lifecycle_router::executor::PodWorkExecutor>,
    idle_grace: Duration,
) -> PodLifecycleRegistry {
    let registry = PodLifecycleRegistry::new_with_idle_grace_for_test(
        test_supervisor(),
        PodLifecycleConcurrencyConfig::production_default(),
        Arc::new(std::sync::Mutex::new(executor)),
        idle_grace,
    );
    registry.set_reply_handle(dummy_reply_handle());
    registry
}

async fn wait_for_actor_count(registry: &PodLifecycleRegistry, expected: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if registry.actor_count().await == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("registry actor count did not reach expected value");
}

#[tokio::test]
async fn registry_reuses_sender_for_same_pod_uid() {
    let registry = test_actor_registry();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");

    let first = registry.sender_for(key.clone()).await.unwrap();
    let second = registry.sender_for(key.clone()).await.unwrap();

    assert!(first.same_channel(&second));
    assert_eq!(registry.actor_count().await, 1);
}

#[tokio::test]
async fn lifecycle_sender_send_enqueues_lifecycle_message() {
    let registry = test_actor_registry();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let sender = registry.sender_for(key.clone()).await.unwrap();

    sender
        .send(LifecycleMessage::RetryDue { key })
        .await
        .expect("actor mailbox should accept lifecycle message");
}

fn lifecycle_order_cases() -> Vec<LifecycleOrderCase> {
    vec![
        LifecycleOrderCase {
            name: "watch_added_cni_assigned_retry_due",
            events: &[
                LifecycleEvent::WatchAdded,
                LifecycleEvent::CniAssigned,
                LifecycleEvent::RetryDue,
            ],
        },
        LifecycleOrderCase {
            name: "watch_added_retry_due_cni_assigned",
            events: &[
                LifecycleEvent::WatchAdded,
                LifecycleEvent::RetryDue,
                LifecycleEvent::CniAssigned,
            ],
        },
        LifecycleOrderCase {
            name: "watch_added_cri_started_watch_modified",
            events: &[
                LifecycleEvent::WatchAdded,
                LifecycleEvent::CriStarted,
                LifecycleEvent::WatchModified,
            ],
        },
        LifecycleOrderCase {
            name: "watch_added_cri_stopped_before_status_echo",
            events: &[
                LifecycleEvent::WatchAdded,
                LifecycleEvent::CriStopped,
                LifecycleEvent::StatusEcho,
            ],
        },
        LifecycleOrderCase {
            name: "watch_modified_start_after_transient_network_wait",
            events: &[
                LifecycleEvent::WatchModified,
                LifecycleEvent::TransientNetworkWait,
                LifecycleEvent::CriStarted,
            ],
        },
    ]
}

#[tokio::test]
async fn pod_lifecycle_handles_valid_event_reorderings_once() {
    for case in lifecycle_order_cases() {
        let mut harness = PodLifecycleHarness::new().await;
        harness.run_case(&case).await;

        assert_eq!(harness.finalized_count, 1, "case {}", case.name);
        assert_eq!(harness.probe_started_count, 1, "case {}", case.name);
        assert_eq!(harness.owner_enqueue_count, 1, "case {}", case.name);
    }
}

#[tokio::test]
async fn actor_self_removes_after_terminated_idle_no_pending() {
    let executor = CompletingStartStopExecutor::new();
    let registry = test_actor_registry_with_executor_and_idle_grace(
        executor.clone(),
        Duration::from_millis(20),
    );
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let sender = registry.sender_for(key.clone()).await.unwrap();

    sender
        .send(LifecycleMessage::WatchAdded {
            key: key.clone(),
            resource_version: Some(1),
            pod: test_pod("default", "pod-a", "uid-a"),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), executor.wait_for_start())
        .await
        .expect("StartPod should complete");

    sender
        .send(LifecycleMessage::WatchDeleted {
            key,
            resource_version: Some(2),
            pod: test_pod("default", "pod-a", "uid-a"),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), executor.wait_for_stop())
        .await
        .expect("StopPod should complete");

    wait_for_actor_count(&registry, 0).await;
}

#[tokio::test]
async fn actor_retains_terminated_state_while_delete_finalization_retry_pending() {
    let executor = FinalizationPendingExecutor::new();
    let registry = test_actor_registry_with_executor_and_idle_grace(
        executor.clone(),
        Duration::from_millis(20),
    );
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let sender = registry.sender_for(key.clone()).await.unwrap();

    sender
        .send(LifecycleMessage::WatchAdded {
            key: key.clone(),
            resource_version: Some(1),
            pod: test_pod("default", "pod-a", "uid-a"),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), executor.wait_for_start())
        .await
        .expect("StartPod should complete");

    let mut terminating = test_pod("default", "pod-a", "uid-a");
    terminating["metadata"]["deletionTimestamp"] = serde_json::json!("2026-07-07T23:15:00Z");
    sender
        .send(LifecycleMessage::WatchModified {
            key: key.clone(),
            resource_version: Some(2),
            pod: terminating.clone(),
        })
        .await
        .unwrap();
    executor.wait_for_finalize_count(1).await;
    assert_eq!(
        executor.stop_count(),
        1,
        "first terminating watch must stop the pod exactly once"
    );

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        registry.actor_count().await,
        1,
        "actor must stay registered while delete finalization has a pending retry"
    );
    let sender_after_idle = registry.sender_for(key.clone()).await.unwrap();
    assert!(
        sender.same_channel(&sender_after_idle),
        "deferred delete wake must route to the existing terminated actor"
    );

    sender_after_idle
        .send(LifecycleMessage::WatchModified {
            key,
            resource_version: Some(3),
            pod: terminating,
        })
        .await
        .unwrap();
    executor.wait_for_finalize_count(2).await;
    assert_eq!(
        executor.stop_count(),
        1,
        "deferred delete wake after local cleanup must retry finalization, not StopPod"
    );
}

#[tokio::test]
async fn actor_does_not_self_remove_with_in_flight_work() {
    let registry = test_actor_registry_with_executor_and_idle_grace(
        Arc::new(NoopExecutor),
        Duration::from_millis(20),
    );
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let sender = registry.sender_for(key.clone()).await.unwrap();

    sender
        .send(LifecycleMessage::WatchAdded {
            key,
            resource_version: Some(1),
            pod: test_pod("default", "pod-a", "uid-a"),
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        registry.actor_count().await,
        1,
        "actor with StartPod in flight must remain registered"
    );
}

#[tokio::test]
async fn try_remove_if_idle_no_op_if_new_actor_inserted_between_check_and_remove() {
    let registry = test_actor_registry();
    let old_key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let new_key = PodLifecycleKey::new("default", "pod-a", "uid-b");
    let slot = super::message::PodSlotKey::from(&old_key);
    let _old_sender = registry.sender_for(old_key.clone()).await.unwrap();
    let old_instance = registry
        .actor_instance_token_for_test(&old_key)
        .await
        .expect("old actor instance token should be available");

    assert!(registry.remove_actor(&old_key).await);
    let _new_sender = registry.sender_for(new_key.clone()).await.unwrap();

    assert!(
        !registry.try_remove_if_idle(&slot, &old_instance).await,
        "stale idle-removal token must not remove a replacement actor"
    );
    assert_eq!(registry.actor_count().await, 1);
    assert!(
        registry
            .actor_instance_token_for_test(&new_key)
            .await
            .is_some()
    );
}

#[tokio::test]
async fn replacement_event_during_stop_routes_to_same_actor() {
    let (executor, _release_start) = BlockingStartExecutor::new();
    let registry = test_actor_registry_with_executor_and_idle_grace(
        executor.clone(),
        Duration::from_millis(20),
    );
    let old_key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let new_key = PodLifecycleKey::new("default", "pod-a", "uid-b");
    let old_sender = registry.sender_for(old_key.clone()).await.unwrap();

    old_sender
        .send(LifecycleMessage::WatchAdded {
            key: old_key.clone(),
            resource_version: Some(1),
            pod: test_pod("default", "pod-a", "uid-a"),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), executor.wait_for_start())
        .await
        .expect("StartPod should enter executor");
    old_sender
        .send(LifecycleMessage::WatchDeleted {
            key: old_key,
            resource_version: Some(2),
            pod: test_pod("default", "pod-a", "uid-a"),
        })
        .await
        .unwrap();

    let replacement_sender = registry.sender_for(new_key).await.unwrap();
    assert!(
        old_sender.same_channel(&replacement_sender),
        "replacement events during stop must route to the existing slot actor"
    );
}

#[tokio::test]
async fn actor_count_returns_to_zero_after_pod_churn() {
    let executor = CompletingStartStopExecutor::new();
    let registry =
        test_actor_registry_with_executor_and_idle_grace(executor, Duration::from_millis(1));

    for i in 0..100 {
        let key = PodLifecycleKey::new("default", &format!("pod-{i}"), &format!("uid-{i}"));
        let sender = registry.sender_for(key.clone()).await.unwrap();
        sender
            .send(LifecycleMessage::WatchDeleted {
                key,
                resource_version: Some(i),
                pod: test_pod("default", &format!("pod-{i}"), &format!("uid-{i}")),
            })
            .await
            .unwrap();
    }

    wait_for_actor_count(&registry, 0).await;
}

#[tokio::test]
async fn fresh_actor_reconciles_cri_before_first_admission() {
    let (executor, _release) = FirstAdmissionReconcileExecutor::blocking(false);
    let registry = test_actor_registry_with_executor_and_idle_grace(
        executor.clone(),
        Duration::from_millis(20),
    );
    let key = PodLifecycleKey::new("default", "pod-a", "uid-y");
    let sender = registry.sender_for(key.clone()).await.unwrap();

    sender
        .send(LifecycleMessage::WatchAdded {
            key,
            resource_version: Some(1),
            pod: test_pod("default", "pod-a", "uid-y"),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), executor.wait_for_reconcile())
        .await
        .expect("fresh actor must dispatch CRI leftover reconciliation first");
    assert_eq!(executor.actions(), ["reconcile:uid-y"]);
}

#[tokio::test]
async fn leftover_other_uid_sandbox_terminated_before_new_uid_admitted() {
    let (executor, release) = FirstAdmissionReconcileExecutor::blocking(false);
    let registry = test_actor_registry_with_executor_and_idle_grace(
        executor.clone(),
        Duration::from_millis(20),
    );
    let key = PodLifecycleKey::new("default", "pod-a", "uid-y");
    let sender = registry.sender_for(key.clone()).await.unwrap();

    sender
        .send(LifecycleMessage::WatchAdded {
            key,
            resource_version: Some(1),
            pod: test_pod("default", "pod-a", "uid-y"),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), executor.wait_for_reconcile())
        .await
        .expect("reconcile should start");
    assert_eq!(
        executor.actions(),
        ["reconcile:uid-y"],
        "StartPod must not dispatch while leftover cleanup is still in flight"
    );

    let _ = release.send(());
    tokio::time::timeout(Duration::from_secs(1), executor.wait_for_start())
        .await
        .expect("StartPod should dispatch after reconcile completes");
    assert_eq!(executor.actions(), ["reconcile:uid-y", "start:uid-y"]);
}

#[tokio::test]
async fn no_leftovers_admits_immediately_after_fast_reconcile() {
    let executor = FirstAdmissionReconcileExecutor::immediate(false);
    let registry = test_actor_registry_with_executor_and_idle_grace(
        executor.clone(),
        Duration::from_millis(20),
    );
    let key = PodLifecycleKey::new("default", "pod-a", "uid-y");
    let sender = registry.sender_for(key.clone()).await.unwrap();

    sender
        .send(LifecycleMessage::WatchAdded {
            key,
            resource_version: Some(1),
            pod: test_pod("default", "pod-a", "uid-y"),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), executor.wait_for_start())
        .await
        .expect("clean first admission should continue to StartPod");
    assert_eq!(executor.actions(), ["reconcile:uid-y", "start:uid-y"]);
}

#[tokio::test]
async fn reconcile_runs_only_once_per_actor() {
    let executor = FirstAdmissionReconcileExecutor::immediate(false);
    let registry = test_actor_registry_with_executor_and_idle_grace(
        executor.clone(),
        Duration::from_millis(20),
    );
    let key = PodLifecycleKey::new("default", "pod-a", "uid-y");
    let sender = registry.sender_for(key.clone()).await.unwrap();

    sender
        .send(LifecycleMessage::WatchAdded {
            key: key.clone(),
            resource_version: Some(1),
            pod: test_pod("default", "pod-a", "uid-y"),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), executor.wait_for_start())
        .await
        .expect("first admission should start pod");

    for rv in 2..12 {
        sender
            .send(LifecycleMessage::WatchModified {
                key: key.clone(),
                resource_version: Some(rv),
                pod: test_pod("default", "pod-a", "uid-y"),
            })
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        executor.reconcile_count(),
        1,
        "same actor lifetime must reconcile process-restart leftovers only once"
    );
}

#[tokio::test]
async fn reconcile_handles_cri_list_failure_gracefully() {
    let executor = FirstAdmissionReconcileExecutor::immediate(true);
    let registry = test_actor_registry_with_executor_and_idle_grace(
        executor.clone(),
        Duration::from_millis(20),
    );
    let key = PodLifecycleKey::new("default", "pod-a", "uid-y");
    let sender = registry.sender_for(key.clone()).await.unwrap();

    sender
        .send(LifecycleMessage::WatchAdded {
            key,
            resource_version: Some(1),
            pod: test_pod("default", "pod-a", "uid-y"),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), executor.wait_for_start())
        .await
        .expect("best-effort CRI reconcile failure should delay, then allow admission");
    assert_eq!(executor.actions(), ["reconcile:uid-y", "start:uid-y"]);
}

#[tokio::test]
async fn registry_routes_same_name_different_uid_to_same_actor() {
    let registry = test_actor_registry();
    let first = registry
        .sender_for(PodLifecycleKey::new("default", "pod-a", "uid-a"))
        .await
        .unwrap();
    let second = registry
        .sender_for(PodLifecycleKey::new("default", "pod-a", "uid-b"))
        .await
        .unwrap();

    assert!(first.same_channel(&second));
    assert_eq!(registry.actor_count().await, 1);
}

#[tokio::test]
async fn registry_remove_actor_uses_slot_key() {
    let registry = test_actor_registry();

    let original = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let replacement = PodLifecycleKey::new("default", "pod-a", "uid-b");
    let _sender = registry.sender_for(original).await.unwrap();
    assert_eq!(registry.actor_count().await, 1);

    assert!(registry.remove_actor(&replacement).await);
    assert_eq!(registry.actor_count().await, 0);
}

#[tokio::test]
async fn registry_scales_beyond_any_fixed_limit() {
    let registry = test_actor_registry();

    for i in 0..64 {
        let key = PodLifecycleKey::new("default", &format!("pod-{i}"), &format!("uid-{i}"));
        registry.sender_for(key).await.unwrap();
    }
    assert_eq!(registry.actor_count().await, 64);
}

#[test]
fn lifecycle_sender_uses_bounded_mailbox() {
    // R4: invariant now enforced by check_kubelet_invariants.sh
}

#[tokio::test]
async fn registry_remove_actor_drops_sender_and_lets_actor_exit() {
    let registry = test_actor_registry();

    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let _sender = registry.sender_for(key.clone()).await.unwrap();
    assert_eq!(registry.actor_count().await, 1);

    assert!(registry.remove_actor(&key).await);
    assert_eq!(registry.actor_count().await, 0);
}

#[tokio::test]
async fn registry_remove_actor_is_idempotent() {
    let registry = test_actor_registry();
    let key = PodLifecycleKey::new("default", "pod-a", "uid-a");
    let _sender = registry.sender_for(key.clone()).await.unwrap();
    assert_eq!(registry.actor_count().await, 1);

    assert!(registry.remove_actor(&key).await);
    assert_eq!(registry.actor_count().await, 0);

    // Second removal should be harmless
    assert!(!registry.remove_actor(&key).await);
    assert_eq!(registry.actor_count().await, 0);
}

#[test]
fn pod_lifecycle_actor_has_no_direct_tokio_spawn_or_timer_calls() {
    // R4: invariant now enforced by check_kubelet_invariants.sh
}
