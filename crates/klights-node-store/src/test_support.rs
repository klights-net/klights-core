//! Focused node-store lease support for integration tests.

use std::sync::Arc;

use crate::{
    PodWorkIdentity, PodWorkqueueClaimRequest, PodWorkqueueLease, PodWorkqueueLeaseToken,
    PodWorkqueueMutationOutcome, PodWorkqueueStore, RuntimeWorkError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedPodWork {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub payload: Vec<u8>,
    lease_token: PodWorkqueueLeaseToken,
}

impl ClaimedPodWork {
    pub fn lease_token(&self) -> &PodWorkqueueLeaseToken {
        &self.lease_token
    }
}

#[derive(Clone)]
pub struct PodWorkqueueTestPorts {
    store: Arc<dyn PodWorkqueueStore>,
}

impl PodWorkqueueTestPorts {
    pub fn new(store: Arc<dyn PodWorkqueueStore>) -> Self {
        Self { store }
    }

    pub async fn claim_uid_bound_pod_work(
        &self,
        now_ms: i64,
        lease_ms: i64,
    ) -> Result<Option<ClaimedPodWork>, RuntimeWorkError> {
        let lease = self
            .store
            .claim_due_work_with_lease(PodWorkqueueClaimRequest::try_new(now_ms, lease_ms)?)
            .await?;
        Ok(lease.and_then(Self::pod_claim))
    }

    fn pod_claim(lease: PodWorkqueueLease) -> Option<ClaimedPodWork> {
        let (entry, lease_token) = lease.into_parts();
        let identity = match entry.identity() {
            PodWorkIdentity::Pod(identity) => identity.clone(),
            PodWorkIdentity::Namespace { .. } => return None,
        };
        Some(ClaimedPodWork {
            namespace: identity.namespace,
            name: identity.name,
            uid: identity.uid,
            payload: entry.payload().to_vec(),
            lease_token,
        })
    }

    pub async fn acknowledge_claim(
        &self,
        claim: ClaimedPodWork,
    ) -> Result<PodWorkqueueMutationOutcome, RuntimeWorkError> {
        self.acknowledge_token(claim.lease_token).await
    }

    pub async fn acknowledge_token(
        &self,
        lease_token: PodWorkqueueLeaseToken,
    ) -> Result<PodWorkqueueMutationOutcome, RuntimeWorkError> {
        self.store.acknowledge_work(lease_token).await
    }
}

// A wrong UID or stale lease_token must remain
// PodWorkqueueMutationOutcome::Stale; the focused port never broad-deletes.
