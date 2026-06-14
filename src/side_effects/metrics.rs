use std::collections::VecDeque;
use std::sync::{Arc, Mutex, atomic::AtomicU64, atomic::Ordering};

const RECENT_SIDE_EFFECT_FAILURES: usize = 64;

#[derive(Clone, Debug)]
pub struct SideEffectFailureEntry {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
    pub hook: String,
    pub context: String,
    pub error: String,
}

/// Counters for side-effect and cascade-delete failures observable via `/metrics`.
///
/// Failures are expected to be transient; a rising counter signals a persistent
/// divergence between the API response the client received and the cluster state.
pub struct SideEffectMetrics {
    /// Post-mutation hook failures (ResourceQuota recount, PDB sync, endpoint mirror, …).
    pub side_effect_failures_total: AtomicU64,
    /// GC cascade-delete failures (orphaned children, leaked owner refs).
    pub cascade_delete_failures_total: AtomicU64,
    /// Namespace hard-delete failures during namespace termination.
    pub namespace_delete_failures_total: AtomicU64,
    recent_failures: Arc<Mutex<VecDeque<SideEffectFailureEntry>>>,
}

impl SideEffectMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            side_effect_failures_total: AtomicU64::new(0),
            cascade_delete_failures_total: AtomicU64::new(0),
            namespace_delete_failures_total: AtomicU64::new(0),
            recent_failures: Arc::new(Mutex::new(VecDeque::new())),
        })
    }

    /// Render Prometheus-compatible text exposition.
    pub fn render_prometheus(&self) -> String {
        format!(
            "# HELP side_effect_failures_total Post-mutation hook failures\n\
             # TYPE side_effect_failures_total counter\n\
             side_effect_failures_total {}\n\
             # HELP cascade_delete_failures_total GC cascade-delete failures\n\
             # TYPE cascade_delete_failures_total counter\n\
             cascade_delete_failures_total {}\n\
             # HELP namespace_delete_failures_total Namespace hard-delete failures\n\
             # TYPE namespace_delete_failures_total counter\n\
             namespace_delete_failures_total {}\n",
            self.side_effect_failures_total.load(Ordering::Relaxed),
            self.cascade_delete_failures_total.load(Ordering::Relaxed),
            self.namespace_delete_failures_total.load(Ordering::Relaxed),
        )
    }

    pub fn record_recent_failure(&self, entry: SideEffectFailureEntry) {
        let mut recent_failures = self
            .recent_failures
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        while recent_failures.len() >= RECENT_SIDE_EFFECT_FAILURES {
            recent_failures.pop_front();
        }
        recent_failures.push_back(entry);
    }

    pub fn recent_failures(&self) -> Vec<SideEffectFailureEntry> {
        self.recent_failures
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .iter()
            .cloned()
            .collect()
    }
}

impl Default for SideEffectMetrics {
    fn default() -> Self {
        Self {
            side_effect_failures_total: AtomicU64::new(0),
            cascade_delete_failures_total: AtomicU64::new(0),
            namespace_delete_failures_total: AtomicU64::new(0),
            recent_failures: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}
