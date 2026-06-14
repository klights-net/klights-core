//! T2 step 2: lease-loop orchestrator.
//!
//! Runs `on_leader` each time this node acquires the cluster leader
//! lease. Re-acquires on every rising edge of `is_leader_rx` (false →
//! true), and tears `on_leader`'s tasks down via the
//! `LeaderLease::cancel` token when leadership is lost.
//!
//! Event-driven: awaits `is_leader_rx.changed()` and
//! `lease.cancel.cancelled()`. No polling, no sleeps.
//!
//! HR #9: the inputs are ports — `Arc<dyn LeaderElection>` plus a
//! `watch::Receiver<bool>` for the leader edge. Tests drive both via a
//! `MockLeaderElection` and a `watch::channel`, so leader→lost→regain
//! cycles are deterministic without a real raft cluster.
//!
//! HR #1, #2: callers spawn this loop via `TaskSupervisor::spawn_async`
//! and bind `on_leader`'s child tasks to the lease cancel token. No raw
//! Tokio timers.

use std::future::Future;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::{LeaderElection, LeaderScope};

pub async fn run_under_lease<F, Fut>(
    election: Arc<dyn LeaderElection>,
    scope: LeaderScope,
    mut is_leader_rx: tokio::sync::watch::Receiver<bool>,
    shutdown: CancellationToken,
    on_leader: F,
) where
    F: Fn(LeaderScope, CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    loop {
        // Wait for is_leader == true (or shutdown). `borrow_and_update`
        // marks the current version as seen so the next `changed()`
        // waits for an actual transition rather than resolving
        // immediately on a stale pending value.
        loop {
            if *is_leader_rx.borrow_and_update() {
                break;
            }
            tokio::select! {
                _ = shutdown.cancelled() => return,
                changed = is_leader_rx.changed() => {
                    if changed.is_err() {
                        // Watch sender dropped; treat as shutdown.
                        return;
                    }
                }
            }
        }

        // Try to acquire the lease. If the election backend says we
        // are not leader (raced with a step-down between the watch
        // edge and the call), drop back to waiting on the next change.
        let lease = match election.acquire(scope.clone()).await {
            Ok(lease) => lease,
            Err(_err) => {
                // Wait for the next leader-state change before retrying.
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    changed = is_leader_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        continue;
                    }
                }
            }
        };

        let lease_cancel = lease.cancel.clone();
        // Hand the lease cancel token to the controller starter. It MUST
        // bind every task it spawns to a child of this token so lease loss
        // tears them down.
        on_leader(scope.clone(), lease_cancel.clone()).await;

        // Wait for the lease to be revoked (RaftLeaderLease's internal
        // metrics watcher cancels on leadership loss) or shutdown.
        tokio::select! {
            _ = lease_cancel.cancelled() => {
                // Drop the lease handle so any per-lease resources release.
                drop(lease);
                // A lease token may be cancelled independently from the
                // leadership watch. Do not reacquire while the last observed
                // value is still `true`; wait until we observe a non-leader
                // state, then the outer loop will require the next true edge.
                loop {
                    if !*is_leader_rx.borrow_and_update() {
                        break;
                    }
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        changed = is_leader_rx.changed() => {
                            if changed.is_err() {
                                return;
                            }
                        }
                    }
                }
                continue;
            }
            _ = shutdown.cancelled() => {
                drop(lease);
                return;
            }
        }
    }
}

/// Deterministic [`LeaderElection`] for unit tests.
///
/// `acquire` always returns a fresh lease whose `cancel` is a child of
/// the supplied root cancellation token. Tests can fire the lease's
/// cancel token to simulate leadership loss, and inspect
/// `acquire_count` to count re-acquisitions.
#[cfg(test)]
pub struct MockLeaderElection {
    root_cancel: CancellationToken,
    acquire_count: std::sync::atomic::AtomicUsize,
    last_lease_cancel: tokio::sync::Mutex<Option<CancellationToken>>,
    fail_n: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl MockLeaderElection {
    pub fn new(root_cancel: CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            root_cancel,
            acquire_count: std::sync::atomic::AtomicUsize::new(0),
            last_lease_cancel: tokio::sync::Mutex::new(None),
            fail_n: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    pub fn acquire_count(&self) -> usize {
        self.acquire_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn cancel_current_lease(&self) {
        if let Some(c) = self.last_lease_cancel.lock().await.as_ref() {
            c.cancel();
        }
    }

    /// Cause the next `n` `acquire` calls to fail (simulating a
    /// step-down race between leader-edge and acquire).
    pub fn script_fail_next(&self, n: usize) {
        self.fail_n.store(n, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl LeaderElection for MockLeaderElection {
    async fn acquire(&self, scope: LeaderScope) -> Result<super::LeaderLease, super::LeaderError> {
        if self
            .fail_n
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |n| if n > 0 { Some(n - 1) } else { None },
            )
            .is_ok()
        {
            return Err(super::LeaderError::AcquireFailed(scope));
        }
        let cancel = self.root_cancel.child_token();
        *self.last_lease_cancel.lock().await = Some(cancel.clone());
        self.acquire_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(super::LeaderLease { scope, cancel })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Notify;

    #[tokio::test]
    async fn acquires_on_leader_edge_and_invokes_on_leader_once() {
        let root = CancellationToken::new();
        let election = MockLeaderElection::new(root.clone());
        let (tx, rx) = tokio::sync::watch::channel(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_closure = calls.clone();
        let on_leader_done = Arc::new(Notify::new());
        let on_leader_done_for_closure = on_leader_done.clone();

        let election_dyn: Arc<dyn LeaderElection> = election.clone();
        let shutdown = CancellationToken::new();
        let shutdown_for_loop = shutdown.clone();
        let handle = tokio::spawn(async move {
            run_under_lease(
                election_dyn,
                LeaderScope::Cluster,
                rx,
                shutdown_for_loop,
                move |_scope, _cancel| {
                    let calls = calls_for_closure.clone();
                    let notify = on_leader_done_for_closure.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        notify.notify_one();
                    }
                },
            )
            .await;
        });

        // Rising edge: false → true.
        tx.send(true).unwrap();
        on_leader_done.notified().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(election.acquire_count(), 1);

        // Tear down.
        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn reacquires_after_lease_loss_on_next_leader_edge() {
        let root = CancellationToken::new();
        let election = MockLeaderElection::new(root.clone());
        let (tx, rx) = tokio::sync::watch::channel(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_closure = calls.clone();
        let lease_seen = Arc::new(Notify::new());
        let lease_seen_for_closure = lease_seen.clone();

        let election_dyn: Arc<dyn LeaderElection> = election.clone();
        let shutdown = CancellationToken::new();
        let shutdown_for_loop = shutdown.clone();
        let handle = tokio::spawn(async move {
            run_under_lease(
                election_dyn,
                LeaderScope::Cluster,
                rx,
                shutdown_for_loop,
                move |_scope, _cancel| {
                    let calls = calls_for_closure.clone();
                    let notify = lease_seen_for_closure.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        notify.notify_one();
                    }
                },
            )
            .await;
        });

        // First acquisition.
        tx.send(true).unwrap();
        lease_seen.notified().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Simulate leadership loss: cancel the lease cancel token AND
        // drive is_leader_rx false → true to re-arm.
        election.cancel_current_lease().await;
        tx.send(false).unwrap();
        // Yield so the loop observes lease-cancel + waits for edge.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "no re-acquire while not leader"
        );

        // Re-arm.
        tx.send(true).unwrap();
        lease_seen.notified().await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(election.acquire_count(), 2);

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_lease_does_not_reacquire_without_new_leader_edge() {
        let root = CancellationToken::new();
        let election = MockLeaderElection::new(root.clone());
        let (tx, rx) = tokio::sync::watch::channel(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_closure = calls.clone();
        let lease_seen = Arc::new(Notify::new());
        let lease_seen_for_closure = lease_seen.clone();

        let election_dyn: Arc<dyn LeaderElection> = election.clone();
        let shutdown = CancellationToken::new();
        let shutdown_for_loop = shutdown.clone();
        let handle = tokio::spawn(async move {
            run_under_lease(
                election_dyn,
                LeaderScope::Cluster,
                rx,
                shutdown_for_loop,
                move |_scope, _cancel| {
                    let calls = calls_for_closure.clone();
                    let notify = lease_seen_for_closure.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        notify.notify_one();
                    }
                },
            )
            .await;
        });

        tx.send(true).unwrap();
        lease_seen.notified().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        election.cancel_current_lease().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "lease cancellation while the watch still says leader must not spin-reacquire"
        );
        assert_eq!(election.acquire_count(), 1);

        tx.send(false).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tx.send(true).unwrap();
        lease_seen.notified().await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(election.acquire_count(), 2);

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_exits_loop_without_acquiring() {
        let root = CancellationToken::new();
        let election = MockLeaderElection::new(root.clone());
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_closure = calls.clone();

        let election_dyn: Arc<dyn LeaderElection> = election.clone();
        let shutdown = CancellationToken::new();
        let shutdown_for_loop = shutdown.clone();
        let handle = tokio::spawn(async move {
            run_under_lease(
                election_dyn,
                LeaderScope::Cluster,
                rx,
                shutdown_for_loop,
                move |_scope, _cancel| {
                    let calls = calls_for_closure.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                    }
                },
            )
            .await;
        });

        // Immediately shut down before anyone becomes leader.
        shutdown.cancel();
        handle.await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(election.acquire_count(), 0);
    }

    #[tokio::test]
    async fn acquire_failure_retries_on_next_leader_edge() {
        let root = CancellationToken::new();
        let election = MockLeaderElection::new(root.clone());
        election.script_fail_next(1);
        let (tx, rx) = tokio::sync::watch::channel(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_closure = calls.clone();
        let lease_seen = Arc::new(Notify::new());
        let lease_seen_for_closure = lease_seen.clone();

        let election_dyn: Arc<dyn LeaderElection> = election.clone();
        let shutdown = CancellationToken::new();
        let shutdown_for_loop = shutdown.clone();
        let handle = tokio::spawn(async move {
            run_under_lease(
                election_dyn,
                LeaderScope::Cluster,
                rx,
                shutdown_for_loop,
                move |_scope, _cancel| {
                    let calls = calls_for_closure.clone();
                    let notify = lease_seen_for_closure.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        notify.notify_one();
                    }
                },
            )
            .await;
        });

        // First leader edge — acquire fails by script.
        tx.send(true).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        // Flap to retry: false → true; this time acquire succeeds.
        tx.send(false).unwrap();
        tx.send(true).unwrap();
        lease_seen.notified().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        shutdown.cancel();
        handle.await.unwrap();
    }
}
