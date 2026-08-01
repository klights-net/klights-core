//! Neutral policy for the leader-only unscheduled-Pod deletion exception.
//!
//! The real persistence adapter remains in root composition. This module can
//! authorize only one exact UID/resourceVersion compare-and-swap after a fresh
//! observation proves that the Pod is terminating, finalizer-free, and still
//! unbound.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use klights_pod_api::{
    UnscheduledPodDeletion, UnscheduledPodDeletionError, UnscheduledPodDeletionFuture,
    UnscheduledPodDeletionOutcome, UnscheduledPodDeletionRequest,
};
use klights_types::PodIdentity;

pub type UnscheduledPodDeletionPortFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, UnscheduledPodDeletionError>> + Send + 'a>>;

/// Fresh, narrow Pod facts needed to decide unscheduled deletion eligibility.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnscheduledPodDeletionObservation {
    identity: PodIdentity,
    resource_version: i64,
    node_name: Option<String>,
    terminating: bool,
    has_finalizers: bool,
}

impl UnscheduledPodDeletionObservation {
    pub fn try_new(
        identity: PodIdentity,
        resource_version: i64,
        node_name: Option<String>,
        terminating: bool,
        has_finalizers: bool,
    ) -> Result<Self, UnscheduledPodDeletionError> {
        UnscheduledPodDeletionRequest::try_new(identity.clone(), resource_version)?;
        Ok(Self {
            identity,
            resource_version,
            node_name: node_name.filter(|node| !node.trim().is_empty()),
            terminating,
            has_finalizers,
        })
    }

    pub fn identity(&self) -> &PodIdentity {
        &self.identity
    }

    pub fn resource_version(&self) -> i64 {
        self.resource_version
    }

    pub fn node_name(&self) -> Option<&str> {
        self.node_name.as_deref()
    }

    pub fn is_terminating(&self) -> bool {
        self.terminating
    }

    pub fn has_finalizers(&self) -> bool {
        self.has_finalizers
    }
}

/// Opaque proof that the policy observed the requested UID/RV as eligible.
///
/// External adapters can inspect this exact CAS target but cannot construct
/// one. The only constructor is below the full eligibility decision in
/// [`UnscheduledPodDeletionService`].
#[derive(Debug, Eq, PartialEq)]
pub struct EligibleUnscheduledPodDeletion {
    identity: PodIdentity,
    observed_resource_version: i64,
}

impl EligibleUnscheduledPodDeletion {
    fn new(identity: PodIdentity, observed_resource_version: i64) -> Self {
        Self {
            identity,
            observed_resource_version,
        }
    }

    pub fn identity(&self) -> &PodIdentity {
        &self.identity
    }

    pub fn observed_resource_version(&self) -> i64 {
        self.observed_resource_version
    }
}

/// Result from the root persistence adapter's exact UID/RV CAS.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnscheduledPodDeleteCasOutcome {
    Removed,
    Conflict,
    Gone,
}

/// Focused lower port used by the neutral deletion policy.
///
/// `compare_and_swap_delete` accepts only the opaque eligibility token, so
/// root persistence cannot be reached through a caller-supplied UID/RV pair.
pub trait UnscheduledPodDeletionPort: Send + Sync {
    fn observe_pod<'a>(
        &'a self,
        identity: &'a PodIdentity,
    ) -> UnscheduledPodDeletionPortFuture<'a, Option<UnscheduledPodDeletionObservation>>;

    fn compare_and_swap_delete(
        &self,
        eligible: EligibleUnscheduledPodDeletion,
    ) -> UnscheduledPodDeletionPortFuture<'_, UnscheduledPodDeleteCasOutcome>;
}

/// Kubelet-owned policy implementation of the Pod API's narrow capability.
pub struct UnscheduledPodDeletionService {
    port: Arc<dyn UnscheduledPodDeletionPort>,
}

impl UnscheduledPodDeletionService {
    pub fn new(port: Arc<dyn UnscheduledPodDeletionPort>) -> Self {
        Self { port }
    }
}

impl UnscheduledPodDeletion for UnscheduledPodDeletionService {
    fn delete_unscheduled_pod(
        &self,
        request: UnscheduledPodDeletionRequest,
    ) -> UnscheduledPodDeletionFuture<'_> {
        Box::pin(async move {
            let (requested_identity, observed_resource_version) = request.into_parts();
            let Some(current) = self.port.observe_pod(&requested_identity).await? else {
                return Ok(UnscheduledPodDeletionOutcome::Removed);
            };

            if current.identity() != &requested_identity {
                return Ok(UnscheduledPodDeletionOutcome::Removed);
            }
            if current.resource_version() != observed_resource_version {
                return Ok(UnscheduledPodDeletionOutcome::Retry);
            }
            if current.node_name().is_some() {
                return Ok(UnscheduledPodDeletionOutcome::DeferToActor);
            }
            if !current.is_terminating() {
                return Ok(UnscheduledPodDeletionOutcome::Retry);
            }
            if current.has_finalizers() {
                return Ok(UnscheduledPodDeletionOutcome::FinalizersPending);
            }

            let eligible =
                EligibleUnscheduledPodDeletion::new(requested_identity, observed_resource_version);
            match self.port.compare_and_swap_delete(eligible).await? {
                UnscheduledPodDeleteCasOutcome::Removed | UnscheduledPodDeleteCasOutcome::Gone => {
                    Ok(UnscheduledPodDeletionOutcome::Removed)
                }
                UnscheduledPodDeleteCasOutcome::Conflict => {
                    Ok(UnscheduledPodDeletionOutcome::Retry)
                }
            }
        })
    }
}
