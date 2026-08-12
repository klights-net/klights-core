//! Actor-owned Pod deletion fixtures.

use std::sync::Arc;

use crate::pod_deletion_finalizer::PodDeletionFinalizer;
use crate::runtime_types::{PodDeletionFinalizeResult, PodRuntimeKey};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodDeleteCasRaceKind {
    SchedulerBind,
    StatusUpdate,
}

pub struct UnscheduledPodDeleteCasRaceOutcome {
    pub disposition: klights_pod_api::UnscheduledPodDeletionOutcome,
    pub raced: bool,
    pub created_resource_version: i64,
    pub live: klights_cluster_core::Resource,
}

pub struct BoundPodDeleteCasRaceOutcome {
    pub disposition: BoundPodDeleteOutcome,
    pub raced: bool,
    pub created_resource_version: i64,
    pub live: klights_cluster_core::Resource,
}

pub struct WorkerFinalizationRaceOutcome {
    pub initially_pending: bool,
    pub resource_version_advanced: bool,
    pub dispatched: bool,
    pub removed_after_dispatch: bool,
    pub completed_after_committed_absence: bool,
    pub node_mismatch_rejected: bool,
}

pub struct WorkerFinalizationDeliveryOutcome {
    pub queued: bool,
    pub exact_uid_bound_command: bool,
    pub committed_resource_receipt: bool,
    pub authoritative_pod_removed: bool,
}

#[derive(Debug, PartialEq)]
pub enum PodOutboxCommand {
    SandboxAnnotationPatch {
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        patch_kind: klights_cluster_core::PatchKind,
        pod_uid: String,
        resource_version: i64,
        strict_resource_version: bool,
        sandbox_id: String,
    },
    DeleteMarkPatch {
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        patch_kind: klights_cluster_core::PatchKind,
        pod_uid: String,
        resource_version: Option<i64>,
        strict_resource_version: bool,
        grace_period_seconds: i64,
        has_deletion_timestamp: bool,
    },
    FinalizeBoundPod {
        namespace: String,
        name: String,
        pod_uid: String,
        node_name: String,
        observed_resource_version: i64,
    },
    Other,
}

pub struct ClaimedPodOutbox {
    pub operation: String,
    pub pod_uid: String,
    pub command: PodOutboxCommand,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PodFinalizationOutcome {
    DeletedOrAlreadyGone,
    Queued,
    FinalizersPending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BoundPodDeleteOutcome {
    Removed,
    IdentityChanged,
    FinalizersPending,
    Retry,
}

impl From<klights_pod_api::BoundPodFinalizationOutcome> for BoundPodDeleteOutcome {
    fn from(outcome: klights_pod_api::BoundPodFinalizationOutcome) -> Self {
        match outcome {
            klights_pod_api::BoundPodFinalizationOutcome::Removed
            | klights_pod_api::BoundPodFinalizationOutcome::Accepted => Self::Removed,
            klights_pod_api::BoundPodFinalizationOutcome::IdentityChanged => Self::IdentityChanged,
            klights_pod_api::BoundPodFinalizationOutcome::FinalizersPending => {
                Self::FinalizersPending
            }
            klights_pod_api::BoundPodFinalizationOutcome::Retry => Self::Retry,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeferredRuntimeFinalizerOutcome {
    Deleted,
    Pending,
    Error,
}

struct FixedDeletionFinalizer {
    outcome: DeferredRuntimeFinalizerOutcome,
}

#[async_trait::async_trait]
impl PodDeletionFinalizer for FixedDeletionFinalizer {
    async fn finalize_after_actor_cleanup(
        &self,
        _key: &PodRuntimeKey,
    ) -> anyhow::Result<PodDeletionFinalizeResult> {
        match self.outcome {
            DeferredRuntimeFinalizerOutcome::Deleted => {
                Ok(PodDeletionFinalizeResult::DeletedOrAlreadyGone)
            }
            DeferredRuntimeFinalizerOutcome::Pending => {
                Ok(PodDeletionFinalizeResult::FinalizersPending)
            }
            DeferredRuntimeFinalizerOutcome::Error => anyhow::bail!("injected finalizer error"),
        }
    }
}

#[derive(Clone)]
pub struct PodDeletionTestPorts {
    finalizer: Arc<dyn PodDeletionFinalizer>,
}

#[derive(Clone)]
pub struct PodDeletionApiTestPorts {
    api: Arc<dyn klights_pod_api::PodApiMutation>,
}

impl PodDeletionApiTestPorts {
    pub fn new(api: Arc<dyn klights_pod_api::PodApiMutation>) -> Self {
        Self { api }
    }

    pub async fn delete(
        &self,
        request: klights_pod_api::PodApiDeleteRequest,
    ) -> Result<klights_pod_api::PodApiDeleteOutcome, klights_pod_api::PodRepositoryError> {
        self.api.delete_pod(request).await
    }

    pub async fn delete_pod<O>(
        &self,
        namespace: &str,
        name: &str,
        options: O,
        dry_run: bool,
    ) -> Result<klights_pod_api::PodApiDeleteOutcome, klights_pod_api::PodRepositoryError>
    where
        O: Into<klights_pod_api::PodDeleteOptions> + Send,
    {
        self.delete(klights_pod_api::PodApiDeleteRequest {
            namespace: namespace.to_string(),
            name: name.to_string(),
            options: options.into(),
            dry_run,
        })
        .await
    }

    pub async fn ordinary_mark_pod_terminating(
        &self,
        request: klights_pod_api::PodMarkTerminatingRequest,
    ) -> Result<klights_cluster_core::Resource, klights_pod_api::PodRepositoryError> {
        let target = request.into_target();
        let options = target
            .uid()
            .map(klights_pod_api::PodDeleteOptions::with_uid_precondition)
            .unwrap_or_default();
        match self
            .delete_pod(target.namespace(), target.name(), options, false)
            .await?
        {
            klights_pod_api::PodApiDeleteOutcome::GracefulSet(resource) => Ok(resource),
            klights_pod_api::PodApiDeleteOutcome::DryRun(_) => {
                unreachable!("ordinary mark is never dry-run")
            }
        }
    }

    pub async fn delete_collection(
        &self,
        request: klights_pod_api::PodApiDeleteCollectionRequest,
    ) -> Result<(), klights_pod_api::PodRepositoryError> {
        self.api.delete_collection_pods(request).await
    }

    pub async fn delete_collection_pods(
        &self,
        namespace: &str,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        dry_run: bool,
    ) -> Result<(), klights_pod_api::PodRepositoryError> {
        self.delete_collection(klights_pod_api::PodApiDeleteCollectionRequest {
            namespace: namespace.to_string(),
            label_selector: label_selector.map(str::to_string),
            field_selector: field_selector.map(str::to_string),
            dry_run,
        })
        .await
    }
}

impl PodDeletionTestPorts {
    pub fn new(finalizer: Arc<dyn PodDeletionFinalizer>) -> Self {
        Self { finalizer }
    }

    pub fn fixed(outcome: DeferredRuntimeFinalizerOutcome) -> Self {
        Self::new(Arc::new(FixedDeletionFinalizer { outcome }))
    }

    pub fn finalizer(&self) -> Arc<dyn PodDeletionFinalizer> {
        self.finalizer.clone()
    }

    pub async fn finalize_after_actor_cleanup(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<PodFinalizationOutcome> {
        let key = PodRuntimeKey::new(namespace, name, uid);
        Ok(
            match self.finalizer.finalize_after_actor_cleanup(&key).await? {
                PodDeletionFinalizeResult::DeletedOrAlreadyGone => {
                    PodFinalizationOutcome::DeletedOrAlreadyGone
                }
                PodDeletionFinalizeResult::Queued => PodFinalizationOutcome::Queued,
                PodDeletionFinalizeResult::FinalizersPending => {
                    PodFinalizationOutcome::FinalizersPending
                }
            },
        )
    }
}

// Fail-closed vocabulary pinned by the P12.1c guard: wrong_uid,
// finalizers_pending, actor_cleanup_confirmed, observed_resource_version.
