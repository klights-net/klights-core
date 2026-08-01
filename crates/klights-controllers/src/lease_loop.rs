//! Event-driven controller lease orchestration owned by `klights-controllers`.
//!
//! The loop depends only on the neutral coordination capability. The embedded
//! adapter waits for authority changes and validates an opaque generation
//! fence; controller code never receives a boolean leader signal.

use std::future::Future;
use std::sync::Arc;

use klights_leader_api::{ControllerCoordination, ControllerLease, ControllerScope};
use tokio_util::sync::CancellationToken;

pub async fn run_under_lease<F, Fut>(
    coordination: Arc<dyn ControllerCoordination>,
    scope: ControllerScope,
    shutdown: CancellationToken,
    on_leader: F,
) where
    F: Fn(ControllerScope, ControllerLease, CancellationToken) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    loop {
        let lease = tokio::select! {
            _ = shutdown.cancelled() => return,
            result = coordination.acquire(scope.clone()) => {
                match result {
                    Ok(lease) => lease,
                    Err(error) => {
                        tracing::warn!(%error, ?scope, "controller coordination closed");
                        return;
                    }
                }
            }
        };
        if let Err(error) = coordination.validate(&lease) {
            tracing::debug!(%error, ?scope, "controller lease changed before startup");
            continue;
        }

        let lease_cancel = shutdown.child_token();
        let startup = klights_leader_api::scope_controller_lease(
            coordination.clone(),
            lease.clone(),
            on_leader(scope.clone(), lease.clone(), lease_cancel.clone()),
        );
        tokio::pin!(startup);
        let revocation = coordination.wait_for_revocation(&lease);
        tokio::pin!(revocation);
        tokio::select! {
            _ = shutdown.cancelled() => {
                lease_cancel.cancel();
                return;
            }
            _ = &mut revocation => {
                lease_cancel.cancel();
                continue;
            }
            _ = &mut startup => {}
        }

        tokio::select! {
            _ = shutdown.cancelled() => {
                lease_cancel.cancel();
                return;
            }
            _ = coordination.wait_for_revocation(&lease) => {
                lease_cancel.cancel();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_leader_api::{
        ControllerAcquireFuture, ControllerCoordinationError, ControllerLease,
        ControllerRevocationFuture,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[derive(Clone, Copy, Debug)]
    struct CoordinationState {
        local: bool,
        generation: u64,
    }

    struct FakeCoordination {
        receiver: tokio::sync::watch::Receiver<CoordinationState>,
    }

    struct FakeCoordinationFence(u64);

    impl ControllerCoordination for FakeCoordination {
        fn try_acquire(
            &self,
            scope: ControllerScope,
        ) -> Result<ControllerLease, ControllerCoordinationError> {
            let state = *self.receiver.borrow();
            if state.local {
                Ok(ControllerLease::issue(
                    scope,
                    FakeCoordinationFence(state.generation),
                ))
            } else {
                Err(ControllerCoordinationError::Unavailable)
            }
        }

        fn acquire(&self, scope: ControllerScope) -> ControllerAcquireFuture<'_> {
            let mut receiver = self.receiver.clone();
            Box::pin(async move {
                loop {
                    let state = *receiver.borrow_and_update();
                    if state.local {
                        return Ok(ControllerLease::issue(
                            scope,
                            FakeCoordinationFence(state.generation),
                        ));
                    }
                    receiver
                        .changed()
                        .await
                        .map_err(|_| ControllerCoordinationError::Closed)?;
                }
            })
        }

        fn validate(&self, lease: &ControllerLease) -> Result<(), ControllerCoordinationError> {
            let state = *self.receiver.borrow();
            if !state.local {
                Err(ControllerCoordinationError::Unavailable)
            } else if lease
                .adapter_fence::<FakeCoordinationFence>()
                .is_none_or(|fence| fence.0 != state.generation)
            {
                Err(ControllerCoordinationError::StalePermit)
            } else {
                Ok(())
            }
        }

        fn wait_for_revocation<'a>(
            &'a self,
            lease: &'a ControllerLease,
        ) -> ControllerRevocationFuture<'a> {
            let mut receiver = self.receiver.clone();
            let generation = lease
                .adapter_fence::<FakeCoordinationFence>()
                .map_or(0, |fence| fence.0);
            Box::pin(async move {
                loop {
                    let state = *receiver.borrow_and_update();
                    if !state.local || state.generation != generation {
                        return;
                    }
                    if receiver.changed().await.is_err() {
                        return;
                    }
                }
            })
        }
    }

    fn coordination(
        local: bool,
    ) -> (
        Arc<dyn ControllerCoordination>,
        tokio::sync::watch::Sender<CoordinationState>,
    ) {
        let (sender, receiver) = tokio::sync::watch::channel(CoordinationState {
            local,
            generation: 1,
        });
        (Arc::new(FakeCoordination { receiver }), sender)
    }

    #[tokio::test]
    async fn waits_for_authority_then_starts_once() {
        let (coordination, publisher) = coordination(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_task = calls.clone();
        let started = Arc::new(Notify::new());
        let started_for_task = started.clone();
        let shutdown = CancellationToken::new();
        let shutdown_for_task = shutdown.clone();

        let task = tokio::spawn(async move {
            run_under_lease(
                coordination,
                ControllerScope::Cluster,
                shutdown_for_task,
                move |_scope, _lease, _cancel| {
                    let calls = calls_for_task.clone();
                    let started = started_for_task.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        started.notify_one();
                    }
                },
            )
            .await;
        });
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        publisher.send_replace(CoordinationState {
            local: true,
            generation: 2,
        });
        started.notified().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        shutdown.cancel();
        task.await.expect("join");
    }

    #[tokio::test]
    async fn revocation_cancels_tasks_and_reacquires_after_promotion() {
        let (coordination, publisher) = coordination(true);
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_task = calls.clone();
        let (lease_tx, mut lease_rx) = tokio::sync::mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        let shutdown_for_task = shutdown.clone();

        let task = tokio::spawn(async move {
            run_under_lease(
                coordination,
                ControllerScope::Cluster,
                shutdown_for_task,
                move |_scope, _lease, cancel| {
                    let calls = calls_for_task.clone();
                    let lease_tx = lease_tx.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        lease_tx.send(cancel).expect("lease observer");
                    }
                },
            )
            .await;
        });

        let first = lease_rx.recv().await.expect("first lease");
        publisher.send_replace(CoordinationState {
            local: false,
            generation: 2,
        });
        first.cancelled().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        publisher.send_replace(CoordinationState {
            local: true,
            generation: 3,
        });
        let second = lease_rx.recv().await.expect("second lease");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!second.is_cancelled());

        shutdown.cancel();
        task.await.expect("join");
        assert!(second.is_cancelled());
    }

    #[tokio::test]
    async fn shutdown_while_standby_exits_without_starting() {
        let (coordination, _publisher) = coordination(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_task = calls.clone();
        let shutdown = CancellationToken::new();
        let shutdown_for_task = shutdown.clone();
        let task = tokio::spawn(async move {
            run_under_lease(
                coordination,
                ControllerScope::Cluster,
                shutdown_for_task,
                move |_scope, _lease, _cancel| {
                    let calls = calls_for_task.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                    }
                },
            )
            .await;
        });
        shutdown.cancel();
        task.await.expect("join");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
