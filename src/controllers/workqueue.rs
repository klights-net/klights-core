//! Generic controller workqueue with deduplication, delayed re-enqueue, and
//! event-driven wake-up. Replaces the synchronous reconcile path that returned
//! 2xx to the client even when reconciliation failed and *nothing* was going to
//! retry it later (P0-LEAK-02).
//!
//! Design (HR1/HR2 compliant):
//!   * `HashMap<ReconcileKey, ReadyEntry>` for ready keys — last-write-wins
//!     dedup with priority upgrade. High-priority upgrades preserve one
//!     follow-up reconcile so terminal owner events cannot be swallowed by
//!     an older already-queued reconcile.
//!   * `tokio::sync::Notify` for "ready item available" — zero cost when nobody
//!     is waiting; not a polling loop.
//!   * `add_after` schedules a one-shot supervised timer task that re-enqueues
//!     the key after the delay; the delay is a timer-wheel event
//!     (HR2-sanctioned), not a polling loop.
//!
//! Per-key retry uses exponential backoff capped at 30s; after 7 consecutive
//! failures the key is dropped with a structured error log (the next
//! mutation/watch event will re-enqueue it).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, Notify};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ReconcileKey {
    pub api_version: &'static str,
    pub kind: &'static str,
    pub namespace: Option<String>,
    pub name: String,
}

impl ReconcileKey {
    pub fn namespaced(
        api_version: &'static str,
        kind: &'static str,
        namespace: &str,
        name: &str,
    ) -> Self {
        Self {
            api_version,
            kind,
            namespace: Some(namespace.to_string()),
            name: name.to_string(),
        }
    }

    pub fn cluster(api_version: &'static str, kind: &'static str, name: &str) -> Self {
        Self {
            api_version,
            kind,
            namespace: None,
            name: name.to_string(),
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Key {
    pub api_version: &'static str,
    pub kind: &'static str,
    pub namespace: Option<String>,
    pub name: String,
}

impl Key {
    #[cfg(test)]
    pub fn new(api_version: &str, kind: &str, namespace: &str, name: &str) -> Self {
        let (api_version, kind) = controller_kind_static(api_version, kind)
            .unwrap_or_else(|| panic!("unsupported controller key {api_version}/{kind}"));
        Self {
            api_version,
            kind,
            namespace: if namespace.is_empty() {
                None
            } else {
                Some(namespace.to_string())
            },
            name: name.to_string(),
        }
    }
}

impl From<ReconcileKey> for Key {
    fn from(value: ReconcileKey) -> Self {
        Self {
            api_version: value.api_version,
            kind: value.kind,
            namespace: value.namespace,
            name: value.name,
        }
    }
}

impl From<Key> for ReconcileKey {
    fn from(value: Key) -> Self {
        Self {
            api_version: value.api_version,
            kind: value.kind,
            namespace: value.namespace,
            name: value.name,
        }
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(namespace) = &self.namespace {
            write!(
                f,
                "{}/{} {}/{}",
                self.api_version, self.kind, namespace, self.name
            )
        } else {
            write!(f, "{}/{} {}", self.api_version, self.kind, self.name)
        }
    }
}

impl std::fmt::Display for ReconcileKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(namespace) = &self.namespace {
            write!(
                f,
                "{}/{} {}/{}",
                self.api_version, self.kind, namespace, self.name
            )
        } else {
            write!(f, "{}/{} {}", self.api_version, self.kind, self.name)
        }
    }
}

pub fn controller_kind_static(
    api_version: &str,
    kind: &str,
) -> Option<(&'static str, &'static str)> {
    match (api_version, kind) {
        ("apps/v1", "Deployment") => Some(("apps/v1", "Deployment")),
        ("apps/v1", "ReplicaSet") => Some(("apps/v1", "ReplicaSet")),
        ("apps/v1", "StatefulSet") => Some(("apps/v1", "StatefulSet")),
        ("apps/v1", "DaemonSet") => Some(("apps/v1", "DaemonSet")),
        ("batch/v1", "Job") => Some(("batch/v1", "Job")),
        ("v1", "Service") => Some(("v1", "Service")),
        ("v1", "Endpoints") => Some(("v1", "Endpoints")),
        ("v1", "PersistentVolumeClaim") => Some(("v1", "PersistentVolumeClaim")),
        ("v1", "ReplicationController") => Some(("v1", "ReplicationController")),
        ("policy/v1", "PodDisruptionBudget") => Some(("policy/v1", "PodDisruptionBudget")),
        ("certificates.k8s.io/v1", "CertificateSigningRequest") => {
            Some(("certificates.k8s.io/v1", "CertificateSigningRequest"))
        }
        _ => None,
    }
}

/// Backoff sequence for retries. Steps: 250ms, 500ms, 1s, 2s, 5s, 10s, 30s.
/// Index past the end clamps to the final entry.
pub fn backoff_for(attempt: u32) -> Duration {
    const STEPS_MS: &[u64] = &[250, 500, 1_000, 2_000, 5_000, 10_000, 30_000];
    let idx = (attempt as usize).min(STEPS_MS.len() - 1);
    Duration::from_millis(STEPS_MS[idx])
}

pub const MAX_RETRY_ATTEMPTS: u32 = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueuePriority {
    Normal,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadyEntry {
    priority: QueuePriority,
    rerun_after_take: bool,
}

impl ReadyEntry {
    fn new(priority: QueuePriority) -> Self {
        Self {
            priority,
            rerun_after_take: false,
        }
    }
}

/// A workqueue: dedup'ed `add()`, delayed `add_after()`, and a `take()` that
/// waits on either. Cheap to clone; designed to be shared across mutation
/// handlers (producers) and a worker task (consumer).
///
/// Each key carries a monotonic generation so stale `add_after` timers
/// self-extinguish: if a fresh mutation enqueues the key before a delayed
/// retry fires, the delayed timer sees a newer generation and becomes a
/// no-op (HR1 idle-silent — no polling, no unnecessary reconciles).
#[derive(Clone)]
pub struct WorkQueue {
    ready: Arc<Mutex<HashMap<Key, ReadyEntry>>>,
    /// Tracks the latest generation for each key that has a pending delayed
    /// retry. Fresh `add()` calls bump the generation here so stale timers
    /// self-extinguish.
    delayed_generations: Arc<Mutex<HashMap<Key, u64>>>,
    notify: Arc<Notify>,
    next_gen: Arc<AtomicU64>,
    task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
}

impl WorkQueue {
    #[cfg(test)]
    pub fn new() -> Self {
        Self::with_task_supervisor(Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        )))
    }

    pub fn with_task_supervisor(
        task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            ready: Arc::new(Mutex::new(HashMap::new())),
            delayed_generations: Arc::new(Mutex::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
            next_gen: Arc::new(AtomicU64::new(1)),
            task_supervisor,
        }
    }

    /// Insert (or refresh) a key in the ready set and wake any waiter.
    /// Multiple bursts of the same key collapse to a single reconcile.
    ///
    /// Also bumps the delayed-generation counter for this key so any
    /// in-flight `add_after` timer sees a stale generation and
    /// self-extinguishes.
    pub async fn add(&self, key: Key) {
        self.add_with_priority(key, QueuePriority::Normal).await;
    }

    pub async fn add_with_priority(&self, key: Key, priority: QueuePriority) {
        {
            let mut ready = self.ready.lock().await;
            ready
                .entry(key.clone())
                .and_modify(|current| {
                    if priority > current.priority {
                        current.priority = priority;
                    }
                    if priority == QueuePriority::High {
                        current.rerun_after_take = true;
                    }
                })
                .or_insert_with(|| ReadyEntry::new(priority));
        }
        {
            let mut gens = self.delayed_generations.lock().await;
            gens.insert(key, self.next_gen.fetch_add(1, Ordering::Relaxed));
        }
        self.notify.notify_one();
    }

    /// Schedule `key` to become ready after `dur`. Used by the worker after a
    /// reconcile failure to honor exponential backoff.
    ///
    /// A generation is captured; when the timer fires it becomes a no-op if a
    /// fresher `add` already bumped the generation for this key.
    pub async fn add_after(&self, key: Key, dur: Duration) {
        let r#gen = self.next_gen.fetch_add(1, Ordering::Relaxed);
        {
            let mut gens = self.delayed_generations.lock().await;
            gens.insert(key.clone(), r#gen);
        }

        let q = self.clone();
        if let Err(err) = self
            .task_supervisor
            .spawn_delay("workqueue_add_after", dur, async move {
                let mut gens = q.delayed_generations.lock().await;
                if gens.get(&key).copied() == Some(r#gen) {
                    gens.remove(&key);
                    drop(gens);
                    q.add(key).await;
                }
            })
            .await
        {
            tracing::warn!("failed to schedule workqueue delayed add: {}", err);
        }
    }

    /// Wait for and return the next ready key. Cancellation-safe under the
    /// ready-map lock — the key is only removed from the map after the lock is
    /// re-acquired post-wait, so dropping the future before that point loses
    /// nothing.
    pub async fn take(&self) -> Key {
        loop {
            // Subscribe to notifications BEFORE checking the map so we can't
            // race-lose a notify that fires between the check and the wait.
            let notified = self.notify.notified();
            tokio::pin!(notified);

            {
                let mut ready = self.ready.lock().await;
                let key = ready
                    .iter()
                    .find_map(|(key, entry)| {
                        (entry.priority == QueuePriority::High).then(|| key.clone())
                    })
                    .or_else(|| ready.keys().next().cloned());
                if let Some(k) = key {
                    if let Some(entry) = ready.remove(&k)
                        && entry.rerun_after_take
                    {
                        ready.insert(k.clone(), ReadyEntry::new(entry.priority));
                    }
                    return k;
                }
            }

            notified.await;
        }
    }

    #[cfg(test)]
    pub async fn ready_len(&self) -> usize {
        self.ready.lock().await.len()
    }

    #[cfg(test)]
    pub async fn ready_keys(&self) -> Vec<ReconcileKey> {
        self.ready_keys_snapshot().await
    }

    pub async fn ready_keys_snapshot(&self) -> Vec<ReconcileKey> {
        let mut keys: Vec<_> = self
            .ready
            .lock()
            .await
            .keys()
            .cloned()
            .map(ReconcileKey::from)
            .collect();
        keys.sort_by(|a, b| {
            (
                a.api_version,
                a.kind,
                a.namespace.as_deref().unwrap_or(""),
                a.name.as_str(),
            )
                .cmp(&(
                    b.api_version,
                    b.kind,
                    b.namespace.as_deref().unwrap_or(""),
                    b.name.as_str(),
                ))
        });
        keys
    }
}

#[cfg(test)]
impl Default for WorkQueue {
    fn default() -> Self {
        Self::with_task_supervisor(Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration as TDur, timeout};

    fn k(name: &str) -> Key {
        Key::new("apps/v1", "Deployment", "default", name)
    }

    #[tokio::test]
    async fn add_then_take_returns_the_same_key() {
        let q = WorkQueue::new();
        q.add(k("nginx")).await;
        let got = timeout(TDur::from_millis(100), q.take()).await.unwrap();
        assert_eq!(got, k("nginx"));
        assert_eq!(q.ready_len().await, 0);
    }

    #[tokio::test]
    async fn add_dedups_repeated_keys() {
        let q = WorkQueue::new();
        for _ in 0..10 {
            q.add(k("nginx")).await;
        }
        assert_eq!(q.ready_len().await, 1);
        // safe-to-ignore: test-only drain; we only care about the queue length afterwards
        let _ = timeout(TDur::from_millis(100), q.take()).await.unwrap();
        assert_eq!(q.ready_len().await, 0);
    }

    #[tokio::test]
    async fn high_priority_add_preempts_normal_backlog_and_upgrades_existing_key() {
        let q = WorkQueue::new();
        q.add(k("normal-a")).await;
        q.add(k("normal-b")).await;

        q.add_with_priority(k("urgent"), QueuePriority::High).await;
        let first = timeout(TDur::from_millis(100), q.take()).await.unwrap();
        assert_eq!(
            first,
            k("urgent"),
            "high-priority reconciles must not sit behind normal controller backlog"
        );

        q.add(k("upgrade")).await;
        q.add_with_priority(k("upgrade"), QueuePriority::High).await;
        let second = timeout(TDur::from_millis(100), q.take()).await.unwrap();
        assert_eq!(
            second,
            k("upgrade"),
            "a high-priority add must upgrade an already queued normal key"
        );
    }

    #[tokio::test]
    async fn high_priority_same_key_upgrade_preserves_followup_reconcile() {
        let q = WorkQueue::new();

        q.add(k("job")).await;
        q.add_with_priority(k("job"), QueuePriority::High).await;

        let first = timeout(TDur::from_millis(100), q.take()).await.unwrap();
        assert_eq!(first, k("job"));

        let second = timeout(TDur::from_millis(100), q.take())
            .await
            .expect("high-priority terminal enqueue must not collapse into the stale queued item");
        assert_eq!(second, k("job"));
    }

    #[tokio::test]
    async fn take_returns_immediately_when_queue_non_empty() {
        let q = WorkQueue::new();
        q.add(k("nginx")).await;
        let start = tokio::time::Instant::now();
        // safe-to-ignore: test-only drain; we only care about the elapsed time
        let _ = q.take().await;
        assert!(
            start.elapsed() < TDur::from_millis(20),
            "take() must return immediately when queue non-empty"
        );
    }

    #[tokio::test]
    async fn add_after_fires_within_window() {
        let q = WorkQueue::new();
        q.add_after(k("retry"), TDur::from_millis(40)).await;
        let start = tokio::time::Instant::now();
        let got = timeout(TDur::from_millis(500), q.take()).await.unwrap();
        let elapsed = start.elapsed();
        assert_eq!(got, k("retry"));
        assert!(
            elapsed >= TDur::from_millis(35) && elapsed < TDur::from_millis(250),
            "add_after must fire after ~40ms, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn add_wakes_a_waiting_take() {
        let q = WorkQueue::new();
        let q2 = q.clone();
        let join = tokio::spawn(async move { q2.take().await });
        // Give the spawned take() a chance to start waiting.
        tokio::time::sleep(TDur::from_millis(20)).await;
        q.add(k("woken")).await;
        let got = timeout(TDur::from_millis(200), join)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, k("woken"));
    }

    #[tokio::test]
    async fn delayed_re_enqueue_dedups_against_concurrent_burst() {
        let q = WorkQueue::new();
        // Schedule a delayed key, then have a fresh mutation enqueue the same
        // key immediately. Only one reconcile should result — the delayed
        // timer must self-extinguish because the fresh add bumped the
        // generation.
        q.add_after(k("nginx"), TDur::from_millis(80)).await;
        q.add(k("nginx")).await;

        let _first = timeout(TDur::from_millis(100), q.take()).await.unwrap();
        // Wait for the stale timer to fire.
        tokio::time::sleep(TDur::from_millis(120)).await;
        // Queue must be empty — stale timer self-extinguished.
        assert_eq!(q.ready_len().await, 0);
    }

    #[test]
    fn backoff_sequence_is_capped() {
        let expected = [
            250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 30_000, 30_000,
        ];
        for (i, &ms) in expected.iter().enumerate() {
            assert_eq!(backoff_for(i as u32), TDur::from_millis(ms));
        }
    }

    #[test]
    fn key_display_handles_namespaced_and_cluster_scope() {
        assert_eq!(
            format!("{}", Key::new("apps/v1", "Deployment", "default", "nginx")),
            "apps/v1/Deployment default/nginx"
        );
        assert_eq!(
            format!("{}", Key::new("v1", "Service", "", "kubernetes")),
            "v1/Service kubernetes"
        );
    }

    #[test]
    fn controller_kind_static_routes_certificate_signing_requests() {
        assert_eq!(
            controller_kind_static("certificates.k8s.io/v1", "CertificateSigningRequest"),
            Some(("certificates.k8s.io/v1", "CertificateSigningRequest")),
            "CSR create events must reach CsrSignerController for worker TLS bootstrap"
        );
    }

    #[test]
    fn reconcile_key_constructors_preserve_scope() {
        assert_eq!(
            ReconcileKey::namespaced("apps/v1", "DaemonSet", "default", "daemon"),
            Key::new("apps/v1", "DaemonSet", "default", "daemon").into()
        );
        assert_eq!(
            ReconcileKey::cluster("v1", "Service", "kubernetes").namespace,
            None
        );
    }

    /// When a fresh mutation enqueues a key before a stale `add_after` timer
    /// fires, the timer must self-extinguish and not produce a second
    /// reconcile.
    #[tokio::test]
    async fn stale_add_after_self_extinguishes_after_fresh_add() {
        let q = WorkQueue::new();

        // Schedule a delayed retry.
        q.add_after(k("deploy"), TDur::from_millis(120)).await;

        // Before the timer fires, a fresh mutation enqueues the same key.
        tokio::time::sleep(TDur::from_millis(30)).await;
        q.add(k("deploy")).await;

        // The fresh add should be immediately available.
        let first = timeout(TDur::from_millis(100), q.take()).await.unwrap();
        assert_eq!(first, k("deploy"));

        // Ensure queue is empty after the first take.
        assert_eq!(q.ready_len().await, 0);

        // Wait for the stale timer to fire (120ms total).
        tokio::time::sleep(TDur::from_millis(150)).await;

        // The queue must still be empty — the stale timer self-extinguished.
        assert_eq!(
            q.ready_len().await,
            0,
            "stale add_after must not re-enqueue after fresh add"
        );
    }
}
