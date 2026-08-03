//! Generic event-driven controller dispatch coordination.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::workqueue::{Key, MAX_RETRY_ATTEMPTS, WorkQueue, backoff_for};

#[derive(Default)]
struct ActiveReconciles {
    in_flight: HashSet<Key>,
    pending_followup: HashSet<Key>,
}

/// Queue, retry, cancellation, and same-key serialization shared by all
/// resource-specific controller dispatch adapters.
pub struct DispatcherRuntime {
    queue: WorkQueue,
    retry_count: Mutex<HashMap<Key, u32>>,
    worker_running: AtomicBool,
    active_reconciles: Mutex<ActiveReconciles>,
    active_reconciles_changed: Notify,
}

impl DispatcherRuntime {
    pub fn new(task_supervisor: Arc<klights_supervisor::TaskSupervisor>) -> Self {
        Self {
            queue: WorkQueue::with_task_supervisor(task_supervisor),
            retry_count: Mutex::new(HashMap::new()),
            worker_running: AtomicBool::new(false),
            active_reconciles: Mutex::new(ActiveReconciles::default()),
            active_reconciles_changed: Notify::new(),
        }
    }

    pub fn worker_running(&self) -> bool {
        self.worker_running.load(Ordering::Acquire)
    }

    pub async fn enqueue(&self, key: Key) {
        self.queue.add(key).await;
    }

    pub async fn enqueue_batch(&self, keys: Vec<Key>) {
        self.queue.add_batch(keys).await;
    }

    pub async fn enqueue_after(&self, key: Key, delay: std::time::Duration) {
        if delay.is_zero() {
            self.queue.add(key).await;
        } else {
            self.queue.add_after(key, delay).await;
        }
    }

    pub async fn pending_keys(&self) -> Vec<Key> {
        self.queue.ready_keys_snapshot().await
    }

    /// Take the next ready key. Intended for deterministic, externally-driven
    /// dispatchers; production worker pools call the same queue operation.
    pub async fn take_next(&self) -> Key {
        self.queue.take().await
    }

    pub async fn record_success(&self, key: &Key) {
        self.retry_count.lock().await.remove(key);
    }

    pub async fn requeue_with_backoff(&self, key: Key) {
        let mut counts = self.retry_count.lock().await;
        let attempt = counts.entry(key.clone()).or_insert(0);
        if *attempt >= MAX_RETRY_ATTEMPTS {
            tracing::error!(
                workqueue_key = %key,
                attempts = *attempt,
                "workqueue: dropping key after MAX_RETRY_ATTEMPTS — will only retry on next mutation or watch event"
            );
            counts.remove(&key);
            return;
        }
        let backoff = backoff_for(*attempt);
        *attempt += 1;
        drop(counts);
        self.queue.add_after(key, backoff).await;
    }

    /// Run an event-driven worker pool through a deterministic dispatch seam.
    pub async fn run_worker_pool<F, Fut>(
        self: Arc<Self>,
        worker_count: usize,
        cancel: CancellationToken,
        dispatch: F,
    ) where
        F: Fn(Key) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.worker_running.store(true, Ordering::Release);
        let dispatch = Arc::new(dispatch);
        let workers = (0..worker_count.max(1)).map(|worker_id| {
            let runtime = self.clone();
            let cancel = cancel.clone();
            let dispatch = dispatch.clone();
            async move {
                runtime.run_worker_loop(worker_id, cancel, dispatch).await;
            }
        });
        futures::future::join_all(workers).await;
        self.worker_running.store(false, Ordering::Release);
    }

    async fn run_worker_loop<F, Fut>(
        self: Arc<Self>,
        worker_id: usize,
        cancel: CancellationToken,
        dispatch: Arc<F>,
    ) where
        F: Fn(Key) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!(worker_id, "controller workqueue worker shutting down");
                    return;
                }
                key = self.queue.take() => {
                    if !self.begin_key_dispatch(&key).await {
                        continue;
                    }
                    dispatch(key.clone()).await;
                    self.finish_key_dispatch(key).await;
                }
            }
        }
    }

    pub async fn begin_key_dispatch(&self, key: &Key) -> bool {
        let mut active = self.active_reconciles.lock().await;
        if active.in_flight.contains(key) {
            active.pending_followup.insert(key.clone());
            return false;
        }
        active.in_flight.insert(key.clone());
        true
    }

    pub async fn wait_for_key_dispatch_slot(&self, key: &Key) {
        loop {
            let notified = self.active_reconciles_changed.notified();
            {
                let mut active = self.active_reconciles.lock().await;
                if !active.in_flight.contains(key) {
                    active.in_flight.insert(key.clone());
                    return;
                }
            }
            notified.await;
        }
    }

    pub async fn finish_key_dispatch(&self, key: Key) {
        let should_requeue = {
            let mut active = self.active_reconciles.lock().await;
            active.in_flight.remove(&key);
            active.pending_followup.remove(&key)
        };
        self.active_reconciles_changed.notify_waiters();
        if should_requeue {
            self.queue.add(key).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workqueue::key_for_test;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn same_key_is_serialized_and_gets_one_followup() {
        let runtime = DispatcherRuntime::new(Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        )));
        let key = key_for_test("apps/v1", "Deployment", "default", "web");
        assert!(runtime.begin_key_dispatch(&key).await);
        assert!(!runtime.begin_key_dispatch(&key).await);
        runtime.finish_key_dispatch(key.clone()).await;
        assert_eq!(runtime.take_next().await, key);
    }

    #[tokio::test]
    async fn worker_pool_stops_on_cancellation_without_polling() {
        let runtime = Arc::new(DispatcherRuntime::new(Arc::new(
            klights_supervisor::TaskSupervisor::new(
                klights_supervisor::TaskCategoryConfig::default(),
            ),
        )));
        let cancel = CancellationToken::new();
        cancel.cancel();
        let calls = Arc::new(AtomicUsize::new(0));
        runtime
            .run_worker_pool(2, cancel, {
                let calls = calls.clone();
                move |_| {
                    calls.fetch_add(1, Ordering::Relaxed);
                    std::future::ready(())
                }
            })
            .await;
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}
