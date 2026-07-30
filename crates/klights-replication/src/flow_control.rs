//! Fair, bounded flow control for embedded Raft proposals.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Leader-owned flow-control gate for Raft proposal concurrency.
pub struct RaftCommitFlowControl {
    semaphore: Arc<Semaphore>,
    priority_semaphore: Arc<Semaphore>,
    max_in_flight: usize,
    priority_in_flight: usize,
}

/// RAII drain of every proposal lane.
pub struct RaftCommitFlowControlDrain {
    _normal: Vec<OwnedSemaphorePermit>,
    _priority: OwnedSemaphorePermit,
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

    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.semaphore)
            .acquire_owned()
            .await
            .expect("flow-control semaphore must not be closed")
    }

    pub async fn acquire_exclusive_drain(&self) -> RaftCommitFlowControlDrain {
        let mut normal = Vec::with_capacity(self.max_in_flight);
        for _ in 0..self.max_in_flight {
            normal.push(self.acquire().await);
        }
        let priority = Arc::clone(&self.priority_semaphore)
            .acquire_owned()
            .await
            .expect("priority flow-control semaphore must not be closed");
        RaftCommitFlowControlDrain {
            _normal: normal,
            _priority: priority,
        }
    }

    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        match Arc::clone(&self.semaphore).try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(TryAcquireError::NoPermits) => None,
            Err(TryAcquireError::Closed) => {
                panic!("flow-control semaphore must not be closed")
            }
        }
    }

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

    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

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
        let flow = RaftCommitFlowControl::new(3);
        assert_eq!(flow.max_in_flight(), 3);
        assert_eq!(flow.reserved_in_flight(), 1);
    }

    #[tokio::test]
    async fn permits_are_returned_on_drop() {
        let flow = RaftCommitFlowControl::new(2);
        {
            let _first = flow.acquire().await;
            let _second = flow.acquire().await;
            assert!(flow.try_acquire().is_none());
        }
        assert!(flow.try_acquire().is_some());
    }

    #[tokio::test]
    async fn priority_lane_is_bounded() {
        let flow = RaftCommitFlowControl::new(1);
        let _normal = flow.try_acquire().expect("normal permit");
        let priority = flow.try_acquire_priority().expect("reserved permit");
        assert_eq!(flow.available_reserved_permits(), 0);
        assert!(flow.try_acquire_priority().is_none());
        drop(priority);
        assert_eq!(flow.available_reserved_permits(), 1);
    }
}
