// Flow-control gate for raft proposals. Bounds the number of unacknowledged proposals
// so the leader cannot build an RV backlog ahead of raft progress under loss (finding.md
// flow-control plan). The gate is a fair semaphore: proposals queue in FIFO order rather
// than racing, which avoids RV-ordering inversions under load.
//
// A permit is acquired BEFORE build_log_apply_commit_for_outbox reserves the next
// resourceVersion. This is the core ordering guarantee: no RV is reserved until a
// slot opens in the in-flight window.

use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Leader-owned flow-control gate for raft proposal concurrency.
///
/// Backed by a fair `tokio::sync::Semaphore`. The in-flight cap matches
/// `max_payload_entries` in the raft config so a single retry cannot resend
/// a logical batch larger than the permit budget.
pub struct RaftCommitFlowControl {
    semaphore: Arc<Semaphore>,
    max_in_flight: usize,
}

impl RaftCommitFlowControl {
    pub fn new(max_in_flight: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_in_flight)),
            max_in_flight,
        }
    }

    /// The configured maximum number of in-flight proposals.
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    /// Acquire a permit, waiting asynchronously until one is available.
    /// The returned permit is an RAII guard — dropping it returns the slot to the pool.
    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("flow-control semaphore must not be closed")
    }

    /// Attempt to acquire a permit without waiting. Returns None if all permits are in use.
    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        match Arc::clone(&self.semaphore).try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(TryAcquireError::NoPermits) => None,
            Err(TryAcquireError::Closed) => {
                panic!("flow-control semaphore must not be closed")
            }
        }
    }

    /// Number of permits currently available (i.e. `max_in_flight - in_flight`).
    /// Used by integration tests to verify the RAII guard releases on every exit path
    /// of `propose_command`.
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_has_expected_capacity() {
        let fc = RaftCommitFlowControl::new(3);
        assert_eq!(fc.max_in_flight(), 3);
    }

    #[tokio::test]
    async fn permits_are_returned_on_drop() {
        let fc = RaftCommitFlowControl::new(2);
        {
            let _p1 = fc.acquire().await;
            let _p2 = fc.acquire().await;
            assert!(fc.try_acquire().is_none(), "capacity exhausted");
        }
        assert!(fc.try_acquire().is_some(), "permits returned after drop");
    }
}
