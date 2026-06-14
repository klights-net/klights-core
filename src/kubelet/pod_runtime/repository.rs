use crate::datastore::Resource;
use crate::kubelet::pod_repository::{PodRepository, PodStatusUpdate, RuntimeReconcileStatus};
use crate::kubelet::pod_startup_error::PodStartupErrorKind;

#[async_trait::async_trait]
pub trait PodRuntimeRepository: Send + Sync {
    async fn get_pod_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
    ) -> anyhow::Result<Option<Resource>>;

    async fn set_pod_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource>;

    async fn apply_runtime_reconcile_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource>;

    async fn mark_start_pending_for_retry_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        error_message: &str,
    ) -> anyhow::Result<Resource>;

    async fn set_probe_readiness_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource>;

    async fn set_deadline_exceeded_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource>;

    async fn apply_ephemeral_container_statuses_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        statuses: Vec<serde_json::Value>,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource>;

    async fn note_container_restart_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        terminated: serde_json::Value,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Option<Resource>>;
}

#[async_trait::async_trait]
impl PodRuntimeRepository for PodRepository {
    async fn get_pod_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
    ) -> anyhow::Result<Option<Resource>> {
        crate::kubelet::pod_repository::PodReader::get_pod_for_uid(self, ns, name, pod_uid).await
    }

    async fn set_pod_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: PodStatusUpdate,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_pod_status_for_uid(
            self,
            ns,
            name,
            pod_uid,
            update,
            expected_rv,
        )
        .await
    }

    async fn apply_runtime_reconcile_status_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        update: RuntimeReconcileStatus,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::apply_runtime_reconcile_status_for_uid(
            self,
            ns,
            name,
            pod_uid,
            update,
            expected_rv,
        )
        .await
    }

    async fn mark_start_pending_for_retry_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        error_message: &str,
    ) -> anyhow::Result<Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::mark_start_pending_for_retry_for_uid(
            self,
            ns,
            name,
            pod_uid,
            error_message,
        )
        .await
    }

    async fn set_probe_readiness_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        ready: bool,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_probe_readiness_for_uid(
            self,
            ns,
            name,
            pod_uid,
            container_name,
            ready,
            expected_rv,
        )
        .await
    }

    async fn set_deadline_exceeded_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        message: String,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::set_deadline_exceeded_for_uid(
            self,
            ns,
            name,
            pod_uid,
            message,
            expected_rv,
        )
        .await
    }

    async fn apply_ephemeral_container_statuses_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        statuses: Vec<serde_json::Value>,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Resource> {
        crate::kubelet::pod_repository::PodStatusWriter::apply_ephemeral_container_statuses_for_uid(
            self,
            ns,
            name,
            pod_uid,
            statuses,
            expected_rv,
        )
        .await
    }

    async fn note_container_restart_for_uid(
        &self,
        ns: &str,
        name: &str,
        pod_uid: &str,
        container_name: &str,
        terminated: serde_json::Value,
        expected_rv: Option<i64>,
    ) -> anyhow::Result<Option<Resource>> {
        crate::kubelet::pod_repository::PodStatusWriter::note_container_restart_for_uid(
            self,
            ns,
            name,
            pod_uid,
            container_name,
            terminated,
            expected_rv,
        )
        .await
    }
}

pub async fn ensure_live_pod_uid(
    repository: &dyn PodRuntimeRepository,
    namespace: &str,
    name: &str,
    pod_uid: &str,
) -> anyhow::Result<()> {
    if repository
        .get_pod_for_uid(namespace, name, pod_uid)
        .await?
        .is_some()
    {
        return Ok(());
    }

    Err(anyhow::Error::new(PodStartupErrorKind::PodDisappeared))
}
