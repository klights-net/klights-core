// Flow-control gate for raft proposals. Bounds the number of unacknowledged proposals
// so the leader cannot build an RV backlog ahead of raft progress under loss (finding.md
// flow-control plan). The general gate is a fair semaphore: proposals queue in FIFO
// order rather than racing, which avoids RV-ordering inversions under load. A separate
// one-permit reserved gate is available only to control-critical outbox writes.
//
// A permit is acquired BEFORE build_log_apply_commit_for_outbox reserves the next
// resourceVersion. This is the core ordering guarantee: no RV is reserved until a
// slot opens in the in-flight window.

use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Leader-owned flow-control gate for raft proposal concurrency.
///
/// Backed by a fair `tokio::sync::Semaphore` for general writes plus a single
/// reserved permit for control-critical outbox writes. General proposal
/// concurrency is `max_in_flight`; the absolute critical-path concurrency is
/// `max_in_flight + reserved_in_flight()`.
pub struct RaftCommitFlowControl {
    semaphore: Arc<Semaphore>,
    priority_semaphore: Arc<Semaphore>,
    max_in_flight: usize,
    priority_in_flight: usize,
}

impl RaftCommitFlowControl {
    pub fn new(max_in_flight: usize) -> Self {
        let priority_in_flight = usize::from(max_in_flight > 0);
        Self {
            semaphore: Arc::new(Semaphore::new(max_in_flight)),
            priority_semaphore: Arc::new(Semaphore::new(priority_in_flight)),
            max_in_flight,
            priority_in_flight,
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

    /// Attempt to acquire a permit for control-critical outbox work.
    ///
    /// Critical outbox writes first use the regular proposal pool when capacity is
    /// available. If status traffic has filled that pool, they may draw from a
    /// single reserved permit so node liveness/control work cannot be starved by a
    /// synchronized status retry storm.
    pub fn try_acquire_priority(&self) -> Option<OwnedSemaphorePermit> {
        self.try_acquire().or_else(|| {
            match Arc::clone(&self.priority_semaphore).try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(TryAcquireError::NoPermits) => None,
                Err(TryAcquireError::Closed) => {
                    panic!("priority flow-control semaphore must not be closed")
                }
            }
        })
    }

    /// Number of permits currently available (i.e. `max_in_flight - in_flight`).
    /// Used by integration tests to verify the RAII guard releases on every exit path
    /// of `propose_command`.
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Number of reserved control-critical permits currently available.
    pub fn available_reserved_permits(&self) -> usize {
        self.priority_semaphore.available_permits()
    }

    pub fn reserved_in_flight(&self) -> usize {
        self.priority_in_flight
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_has_expected_capacity() {
        let fc = RaftCommitFlowControl::new(3);
        assert_eq!(fc.max_in_flight(), 3);
        assert_eq!(fc.reserved_in_flight(), 1);
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

    #[tokio::test]
    async fn priority_permit_uses_reserved_capacity_when_general_is_full() {
        let fc = RaftCommitFlowControl::new(2);
        let _p1 = fc.try_acquire().expect("first general permit");
        let _p2 = fc.try_acquire().expect("second general permit");
        assert!(fc.try_acquire().is_none(), "general capacity exhausted");

        let priority = fc
            .try_acquire_priority()
            .expect("priority outbox writes must have reserved capacity");
        assert_eq!(fc.available_reserved_permits(), 0);
        assert!(
            fc.try_acquire_priority().is_none(),
            "reserved capacity is bounded to one extra critical write"
        );

        drop(priority);
        assert_eq!(fc.available_reserved_permits(), 1);
    }
}
