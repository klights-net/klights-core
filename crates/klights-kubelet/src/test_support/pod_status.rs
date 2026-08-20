//! Focused Pod status support for local and worker integration tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use klights_cluster_core::Resource;

use crate::pod_repository::status::PodStatusWriter;
use crate::pod_repository::{PodStatusUpdate, RuntimeReconcileStatus};

/// Test-only adapter exposing exactly the kubelet-owned status writer.
#[derive(Clone)]
pub struct PodStatusTestPorts {
    writer: Arc<dyn PodStatusWriter>,
}

impl PodStatusTestPorts {
    pub fn new(writer: Arc<dyn PodStatusWriter>) -> Self {
        Self { writer }
    }

    pub async fn set_pod_status(
        &self,
        namespace: &str,
        name: &str,
        update: PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        self.writer
            .set_pod_status(namespace, name, update, expected_rv)
            .await
    }

    pub async fn set_pod_status_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        update: PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        self.writer
            .set_pod_status_for_uid(namespace, name, uid, update, expected_rv)
            .await
    }

    pub async fn apply_runtime_reconcile_status(
        &self,
        namespace: &str,
        name: &str,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        self.writer
            .apply_runtime_reconcile_status(namespace, name, update, expected_rv)
            .await
    }

    pub async fn apply_runtime_reconcile_status_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        self.writer
            .apply_runtime_reconcile_status_for_uid(namespace, name, uid, update, expected_rv)
            .await
    }

    pub async fn mark_start_pending_for_retry_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        error_message: &str,
    ) -> anyhow::Result<Resource> {
        self.writer
            .mark_start_pending_for_retry_for_uid(namespace, name, uid, error_message)
            .await
    }

    pub async fn set_probe_readiness(
        &self,
        namespace: &str,
        name: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        self.writer
            .set_probe_readiness(namespace, name, container_name, ready, expected_rv)
            .await
            .map(|result| result.resource)
    }

    pub async fn set_probe_readiness_with_result(
        &self,
        namespace: &str,
        name: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::pod_repository::status::PodStatusWriteResult> {
        self.writer
            .set_probe_readiness(namespace, name, container_name, ready, expected_rv)
            .await
    }

    pub async fn set_probe_readiness_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        self.writer
            .set_probe_readiness_for_uid(namespace, name, uid, container_name, ready, expected_rv)
            .await
            .map(|result| result.resource)
    }

    pub async fn set_probe_readiness_for_uid_with_result(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<crate::pod_repository::status::PodStatusWriteResult> {
        self.writer
            .set_probe_readiness_for_uid(namespace, name, uid, container_name, ready, expected_rv)
            .await
    }

    pub async fn set_deadline_exceeded(
        &self,
        namespace: &str,
        name: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        self.writer
            .set_deadline_exceeded(namespace, name, message, expected_rv)
            .await
    }

    /// Read this Pod with the kubelet's own pending outbox writes applied.
    ///
    /// Exposes the reconcile-path read (`LeaderFresh` + node-local checkpoint
    /// overlay) so tests can assert read-your-own-writes freshness without
    /// going through the full runtime service.
    pub async fn read_pod_with_own_writes(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<Option<Resource>> {
        self.writer
            .read_pod_with_own_writes(namespace, name, uid)
            .await
    }

    pub async fn set_deadline_exceeded_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        self.writer
            .set_deadline_exceeded_for_uid(namespace, name, uid, message, expected_rv)
            .await
    }

    pub async fn apply_ephemeral_container_statuses_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        statuses: Vec<serde_json::Value>,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        self.writer
            .apply_ephemeral_container_statuses_for_uid(namespace, name, uid, statuses, expected_rv)
            .await
    }
}

/// Observable result of a status CAS race.
pub struct StatusRaceOutcome {
    pub attempts: usize,
    pub resource: Option<Resource>,
    pub conflict: bool,
}

/// Observable result when a status write races a same-name replacement Pod.
pub struct SameNameStatusRaceOutcome {
    pub old_uid: String,
    pub replacement: Resource,
    pub persisted_after: Resource,
    pub persistence_attempts: usize,
    pub reconcile_effects: usize,
    pub outbox_enqueues: usize,
    pub conflict: bool,
}

/// Focused hook used to inject a deterministic status-write race.
pub trait StatusWriteRaceHook: Send + Sync {
    fn before_write<'a>(
        &'a self,
        attempt: usize,
        request: &'a klights_pod_api::PodStatusWriteRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'a, ()>;
}

/// Status persistence fake that counts attempts and invokes an exact race hook.
pub struct StatusRacePersistence {
    delegate: Arc<dyn klights_pod_api::PodStatusPersistence>,
    hook: Arc<dyn StatusWriteRaceHook>,
    attempts: AtomicUsize,
}

impl StatusRacePersistence {
    pub fn new(
        delegate: Arc<dyn klights_pod_api::PodStatusPersistence>,
        hook: Arc<dyn StatusWriteRaceHook>,
    ) -> Self {
        Self {
            delegate,
            hook,
            attempts: AtomicUsize::new(0),
        }
    }

    pub fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

impl klights_pod_api::PodStatusPersistence for StatusRacePersistence {
    fn write_pod_status(
        &self,
        request: klights_pod_api::PodStatusWriteRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            self.hook.before_write(attempt, &request).await?;
            self.delegate.write_pod_status(request).await
        })
    }
}

/// Status persistence fake that pauses exactly between request capture and CAS.
pub struct PausedStatusPersistence {
    delegate: Arc<dyn klights_pod_api::PodStatusPersistence>,
    entered: Arc<tokio::sync::Barrier>,
    release: Arc<tokio::sync::Barrier>,
    requested_status: std::sync::Mutex<Option<serde_json::Value>>,
    attempts: AtomicUsize,
}

impl PausedStatusPersistence {
    pub fn new(
        delegate: Arc<dyn klights_pod_api::PodStatusPersistence>,
        entered: Arc<tokio::sync::Barrier>,
        release: Arc<tokio::sync::Barrier>,
    ) -> Self {
        Self {
            delegate,
            entered,
            release,
            requested_status: std::sync::Mutex::new(None),
            attempts: AtomicUsize::new(0),
        }
    }

    pub fn requested_status(&self) -> Option<serde_json::Value> {
        self.requested_status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

impl klights_pod_api::PodStatusPersistence for PausedStatusPersistence {
    fn write_pod_status(
        &self,
        request: klights_pod_api::PodStatusWriteRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            self.requested_status
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .replace(request.status.clone());
            self.entered.wait().await;
            self.release.wait().await;
            self.delegate.write_pod_status(request).await
        })
    }
}
