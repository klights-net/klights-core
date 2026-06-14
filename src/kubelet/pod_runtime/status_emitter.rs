use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::kubelet::pod_runtime::service::PodRuntimeKey;

#[derive(Clone, Debug, Eq, PartialEq)]
struct PodStatusEmissionEntry {
    last_success: Option<Value>,
    in_flight: Option<Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodStatusEmissionPermit {
    key: PodRuntimeKey,
    status: Value,
}

/// Per-container readiness dedupe state. Tracked separately from the full
/// status payloads so a periodic readiness emission cannot ping-pong against
/// the differently-shaped `write_pod_status` / `reconcile_runtime` payloads
/// that share the status cache.
#[derive(Clone, Debug, Eq, PartialEq)]
struct PodReadinessEmissionEntry {
    last_success: Option<bool>,
    in_flight: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodReadinessEmissionPermit {
    key: PodRuntimeKey,
    container: String,
    ready: bool,
}

#[derive(Default)]
pub struct PodStatusEmissionCache {
    entries: HashMap<PodRuntimeKey, PodStatusEmissionEntry>,
    readiness: HashMap<(PodRuntimeKey, String), PodReadinessEmissionEntry>,
}

impl PodStatusEmissionCache {
    pub fn begin_emit(
        &mut self,
        key: &PodRuntimeKey,
        status: &Value,
    ) -> Option<PodStatusEmissionPermit> {
        if let Some(entry) = self.entries.get_mut(key) {
            if entry.last_success.as_ref() == Some(status)
                || entry.in_flight.as_ref() == Some(status)
            {
                return None;
            }
            entry.in_flight = Some(status.clone());
        } else {
            self.entries.insert(
                key.clone(),
                PodStatusEmissionEntry {
                    last_success: None,
                    in_flight: Some(status.clone()),
                },
            );
        }

        Some(PodStatusEmissionPermit {
            key: key.clone(),
            status: status.clone(),
        })
    }

    pub fn record_success(&mut self, permit: &PodStatusEmissionPermit) {
        let entry =
            self.entries
                .entry(permit.key.clone())
                .or_insert_with(|| PodStatusEmissionEntry {
                    last_success: None,
                    in_flight: None,
                });
        entry.last_success = Some(permit.status.clone());
        if entry.in_flight.as_ref() == Some(&permit.status) {
            entry.in_flight = None;
        }
    }

    pub fn record_failure(&mut self, permit: &PodStatusEmissionPermit) {
        if let Some(entry) = self.entries.get_mut(&permit.key)
            && entry.in_flight.as_ref() == Some(&permit.status)
        {
            entry.in_flight = None;
        }
    }

    pub fn begin_readiness(
        &mut self,
        key: &PodRuntimeKey,
        container: &str,
        ready: bool,
    ) -> Option<PodReadinessEmissionPermit> {
        let entry_key = (key.clone(), container.to_string());
        if let Some(entry) = self.readiness.get_mut(&entry_key) {
            if entry.last_success == Some(ready) || entry.in_flight == Some(ready) {
                return None;
            }
            entry.in_flight = Some(ready);
        } else {
            self.readiness.insert(
                entry_key,
                PodReadinessEmissionEntry {
                    last_success: None,
                    in_flight: Some(ready),
                },
            );
        }

        Some(PodReadinessEmissionPermit {
            key: key.clone(),
            container: container.to_string(),
            ready,
        })
    }

    pub fn record_readiness_success(&mut self, permit: &PodReadinessEmissionPermit) {
        let entry = self
            .readiness
            .entry((permit.key.clone(), permit.container.clone()))
            .or_insert_with(|| PodReadinessEmissionEntry {
                last_success: None,
                in_flight: None,
            });
        entry.last_success = Some(permit.ready);
        if entry.in_flight == Some(permit.ready) {
            entry.in_flight = None;
        }
    }

    pub fn record_readiness_failure(&mut self, permit: &PodReadinessEmissionPermit) {
        if let Some(entry) = self
            .readiness
            .get_mut(&(permit.key.clone(), permit.container.clone()))
            && entry.in_flight == Some(permit.ready)
        {
            entry.in_flight = None;
        }
    }

    pub fn forget(&mut self, key: &PodRuntimeKey) {
        self.entries.remove(key);
        self.readiness.retain(|(entry_key, _), _| entry_key != key);
    }
}

#[derive(Clone, Default)]
pub struct PodStatusEmitter {
    cache: Arc<Mutex<PodStatusEmissionCache>>,
}

impl PodStatusEmitter {
    pub async fn emit_if_changed<F, Fut, E>(
        &self,
        key: &PodRuntimeKey,
        status: Value,
        emit: F,
    ) -> Result<bool, E>
    where
        F: FnOnce(Value) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let permit = {
            let mut cache = self
                .cache
                .lock()
                .expect("pod status emission cache mutex poisoned");
            cache.begin_emit(key, &status)
        };
        let Some(permit) = permit else {
            return Ok(false);
        };

        let result = emit(status).await;
        match result {
            Ok(()) => {
                self.cache
                    .lock()
                    .expect("pod status emission cache mutex poisoned")
                    .record_success(&permit);
                Ok(true)
            }
            Err(err) => {
                self.cache
                    .lock()
                    .expect("pod status emission cache mutex poisoned")
                    .record_failure(&permit);
                Err(err)
            }
        }
    }

    /// Dedupe a per-container readiness emission. The `emit` closure (the
    /// worker→leader forward / leader-local write) runs only when the
    /// `(container, ready)` pair differs from the last value this actor
    /// successfully emitted, so repeated identical `ReadinessChanged` signals
    /// — which the leader would no-op anyway — never re-cross the boundary.
    /// A genuine flip always re-emits and carries its downstream side effects;
    /// a failed emit leaves the prior success intact so it retries.
    pub async fn emit_readiness_if_changed<F, Fut, E>(
        &self,
        key: &PodRuntimeKey,
        container_name: &str,
        ready: bool,
        emit: F,
    ) -> Result<bool, E>
    where
        F: FnOnce(bool) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let permit = {
            let mut cache = self
                .cache
                .lock()
                .expect("pod status emission cache mutex poisoned");
            cache.begin_readiness(key, container_name, ready)
        };
        let Some(permit) = permit else {
            return Ok(false);
        };

        let result = emit(ready).await;
        match result {
            Ok(()) => {
                self.cache
                    .lock()
                    .expect("pod status emission cache mutex poisoned")
                    .record_readiness_success(&permit);
                Ok(true)
            }
            Err(err) => {
                self.cache
                    .lock()
                    .expect("pod status emission cache mutex poisoned")
                    .record_readiness_failure(&permit);
                Err(err)
            }
        }
    }

    pub fn forget(&self, key: &PodRuntimeKey) {
        self.cache
            .lock()
            .expect("pod status emission cache mutex poisoned")
            .forget(key);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::kubelet::pod_runtime::service::PodRuntimeKey;

    use super::{PodStatusEmissionCache, PodStatusEmitter};

    #[test]
    fn status_emission_cache_suppresses_only_identical_uid_status_payloads() {
        let mut cache = PodStatusEmissionCache::default();
        let key = PodRuntimeKey::new("default", "web", "uid-1");
        let same_name_new_uid = PodRuntimeKey::new("default", "web", "uid-2");
        let pending = serde_json::json!({
            "phase": "Pending",
            "podIP": "10.42.0.8",
            "containerStatuses": [{
                "name": "app",
                "ready": false,
                "state": {"waiting": {"reason": "ContainerCreating"}}
            }]
        });
        let running = serde_json::json!({
            "phase": "Running",
            "podIP": "10.42.0.8",
            "containerStatuses": [{
                "name": "app",
                "ready": true,
                "state": {"running": {"startedAt": "2026-06-11T00:00:00Z"}}
            }]
        });

        let first_pending = cache
            .begin_emit(&key, &pending)
            .expect("first status must emit");
        cache.record_success(&first_pending);
        assert!(cache.begin_emit(&key, &pending).is_none());

        let next_running = cache
            .begin_emit(&key, &running)
            .expect("changed status must emit");
        cache.record_success(&next_running);
        assert!(
            cache.begin_emit(&same_name_new_uid, &running).is_some(),
            "same-name replacement pod has a different UID and must not inherit the old UID cache"
        );
    }

    #[tokio::test]
    async fn emitter_does_not_run_side_effect_closure_for_unchanged_status() {
        let emitter = PodStatusEmitter::default();
        let key = PodRuntimeKey::new("default", "web", "uid-1");
        let status = serde_json::json!({"phase": "Pending"});
        let calls = Arc::new(AtomicUsize::new(0));

        let first_calls = calls.clone();
        assert!(
            emitter
                .emit_if_changed(&key, status.clone(), move |_| {
                    let first_calls = first_calls.clone();
                    async move {
                        first_calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), anyhow::Error>(())
                    }
                })
                .await
                .expect("first emit succeeds")
        );

        let second_calls = calls.clone();
        assert!(
            !emitter
                .emit_if_changed(&key, status, move |_| {
                    let second_calls = second_calls.clone();
                    async move {
                        second_calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), anyhow::Error>(())
                    }
                })
                .await
                .expect("suppressed emit succeeds")
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "suppressed duplicate status must not run repository/outbox side effects"
        );
    }

    #[tokio::test]
    async fn emitter_runs_side_effect_closure_for_distinct_status_payloads() {
        let emitter = PodStatusEmitter::default();
        let key = PodRuntimeKey::new("default", "web", "uid-1");
        let calls = Arc::new(AtomicUsize::new(0));

        for status in [
            serde_json::json!({"phase": "Pending", "podIP": "10.42.0.8"}),
            serde_json::json!({"phase": "Running", "podIP": "10.42.0.8"}),
        ] {
            let calls = calls.clone();
            assert!(
                emitter
                    .emit_if_changed(&key, status, move |_| {
                        let calls = calls.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            Ok::<(), anyhow::Error>(())
                        }
                    })
                    .await
                    .expect("changed status emit succeeds")
            );
        }

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "distinct status payloads must still run repository/outbox side effects"
        );
    }

    #[tokio::test]
    async fn emitter_suppresses_repeated_identical_readiness() {
        let emitter = PodStatusEmitter::default();
        let key = PodRuntimeKey::new("default", "web", "uid-1");
        let calls = Arc::new(AtomicUsize::new(0));

        for _ in 0..3 {
            let calls = calls.clone();
            let _ = emitter
                .emit_readiness_if_changed(&key, "app", true, move |_| {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), anyhow::Error>(())
                    }
                })
                .await
                .expect("readiness emit succeeds");
        }

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "repeated identical readiness must forward / write exactly once"
        );
    }

    #[tokio::test]
    async fn emitter_emits_on_readiness_flip() {
        let emitter = PodStatusEmitter::default();
        let key = PodRuntimeKey::new("default", "web", "uid-1");
        let calls = Arc::new(AtomicUsize::new(0));

        for ready in [true, true, false, false, true] {
            let calls = calls.clone();
            let _ = emitter
                .emit_readiness_if_changed(&key, "app", ready, move |_| {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), anyhow::Error>(())
                    }
                })
                .await
                .expect("readiness emit succeeds");
        }

        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "each genuine readiness flip must re-emit so downstream side effects fire"
        );
    }

    #[tokio::test]
    async fn emitter_tracks_readiness_per_container() {
        let emitter = PodStatusEmitter::default();
        let key = PodRuntimeKey::new("default", "web", "uid-1");
        let calls = Arc::new(AtomicUsize::new(0));

        for container in ["app", "sidecar", "app", "sidecar"] {
            let calls = calls.clone();
            let _ = emitter
                .emit_readiness_if_changed(&key, container, true, move |_| {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), anyhow::Error>(())
                    }
                })
                .await
                .expect("readiness emit succeeds");
        }

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "each container's readiness is deduped independently"
        );
    }

    #[tokio::test]
    async fn failed_readiness_emit_allows_retry() {
        let emitter = PodStatusEmitter::default();
        let key = PodRuntimeKey::new("default", "web", "uid-1");

        let failed = emitter
            .emit_readiness_if_changed(&key, "app", true, |_| async {
                Err::<(), &'static str>("forward failed")
            })
            .await;
        assert_eq!(failed, Err("forward failed"));

        assert!(
            emitter
                .emit_readiness_if_changed(&key, "app", true, |_| async {
                    Ok::<(), &'static str>(())
                })
                .await
                .expect("retry succeeds"),
            "a failed readiness emit must leave the same readiness eligible for retry"
        );
    }

    #[tokio::test]
    async fn forget_clears_readiness_so_new_uid_reemits() {
        let emitter = PodStatusEmitter::default();
        let key = PodRuntimeKey::new("default", "web", "uid-1");
        let calls = Arc::new(AtomicUsize::new(0));

        let run = || {
            let emitter = emitter.clone();
            let key = key.clone();
            let calls = calls.clone();
            async move {
                emitter
                    .emit_readiness_if_changed(&key, "app", true, move |_| {
                        let calls = calls.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            Ok::<(), anyhow::Error>(())
                        }
                    })
                    .await
                    .expect("readiness emit succeeds")
            }
        };

        run().await;
        emitter.forget(&key);
        run().await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "forgetting a pod (deletion / uid change) must let readiness re-emit"
        );
    }

    #[tokio::test]
    async fn failed_emit_does_not_poison_cache_for_retry() {
        let emitter = PodStatusEmitter::default();
        let key = PodRuntimeKey::new("default", "web", "uid-1");
        let status = serde_json::json!({"phase": "Pending"});

        let failed = emitter
            .emit_if_changed(&key, status.clone(), |_| async {
                Err::<(), &'static str>("write failed")
            })
            .await;
        assert_eq!(failed, Err("write failed"));

        assert!(
            emitter
                .emit_if_changed(&key, status, |_| async { Ok::<(), &'static str>(()) })
                .await
                .expect("retry succeeds"),
            "a failed emit must leave the same status eligible for retry"
        );
    }
}
