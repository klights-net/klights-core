//! T4 (latency-todo): passive RTT estimator fed by raft heartbeat /
//! AppendEntries round-trips. No extra RPCs — it consumes timing from RPCs
//! the raft transport already makes, so it is idle-silent when no traffic
//! flows (HR: zero idle CPU). The EWMA is stored in an atomic so a single
//! shared estimator can be read by deadline sizing and written by the raft
//! network without a lock on the hot path.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Default RTT (ms) reported before any sample is recorded. Matches the
/// fixed estimate the adaptive outbox backoff previously hard-coded.
pub const RTT_DEFAULT_MS: i64 = 200;
/// EWMA smoothing factor: `estimate = alpha * sample + (1 - alpha) * estimate`.
/// Low alpha = smooth/stable, less reactive to a single outlier. 0.1 means a
/// single pathological sample (a stalled RPC under loss) moves the estimate
/// by ~10%, so deadlines/backoff do not inflate from one bad round-trip.
const EWMA_ALPHA: f64 = 0.1;
/// The estimate is clamped to `[RTT_MIN_MS, RTT_MAX_MS]` so a pathological
/// sample (a stalled RPC under loss) cannot pin deadlines to hours.
const RTT_MIN_MS: i64 = 10;
const RTT_MAX_MS: i64 = 5_000;

/// Atomic backing store sentinel: `0` means "no sample recorded yet".
const NO_SAMPLE: u64 = 0;

/// Shared, lock-free EWMA of raft peer round-trip times in milliseconds.
///
/// Cloned copies share the same atomic estimate (cheap `Clone` for wiring into
/// multiple components — the raft network writes, the transport policy reads).
#[derive(Debug)]
pub struct RttEstimator {
    /// Milliseconds * 1000 stored as u64 for sub-ms resolution without floats
    /// in atomics. `NO_SAMPLE` (0) means uninitialized.
    estimate_micro_ms: AtomicU64,
}

impl RttEstimator {
    pub fn new() -> Self {
        Self {
            estimate_micro_ms: AtomicU64::new(NO_SAMPLE),
        }
    }

    /// Record a successful raft RPC round-trip duration and fold it into the
    /// EWMA. Called from the raft network after each completed append_entries
    /// (and optionally vote / install_snapshot). Failure durations are NOT
    /// sampled — a timed-out RPC tells us nothing about the true RTT.
    pub fn record_sample(&self, elapsed: Duration) {
        let sample_ms = elapsed.as_millis().max(1) as i64;
        loop {
            let prev = self.estimate_micro_ms.load(Ordering::Relaxed);
            let next_ms = if prev == NO_SAMPLE {
                sample_ms
            } else {
                let prev_ms = (prev as f64) / 1000.0;
                (EWMA_ALPHA * sample_ms as f64 + (1.0 - EWMA_ALPHA) * prev_ms) as i64
            };
            let clamped = next_ms.clamp(RTT_MIN_MS, RTT_MAX_MS);
            let encoded = (clamped as u64) * 1000;
            match self.estimate_micro_ms.compare_exchange(
                prev,
                encoded,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => self.estimate_micro_ms.store(actual, Ordering::Relaxed),
            }
        }
    }

    /// Current RTT estimate in ms, clamped. Returns [`RTT_DEFAULT_MS`] until
    /// the first sample is recorded so callers (deadline/backoff sizing) get a
    /// sane default before any raft traffic has flowed.
    pub fn estimate_ms(&self) -> i64 {
        let raw = self.estimate_micro_ms.load(Ordering::Relaxed);
        if raw == NO_SAMPLE {
            RTT_DEFAULT_MS
        } else {
            ((raw as f64) / 1000.0).round() as i64
        }
    }
}

impl Default for RttEstimator {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for RttEstimator {
    /// Clones share the SAME atomic estimate — this is intentional so the
    /// estimator can be wired into the writer (raft network) and reader
    /// (transport policy / outbox backoff) sides of the same peer transport.
    /// For an independent estimator, construct a new one.
    fn clone(&self) -> Self {
        Self {
            estimate_micro_ms: AtomicU64::new(self.estimate_micro_ms.load(Ordering::Relaxed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimate_is_default_until_first_sample() {
        let e = RttEstimator::new();
        assert_eq!(e.estimate_ms(), RTT_DEFAULT_MS);
    }

    #[test]
    fn first_sample_is_recorded_clamped() {
        let e = RttEstimator::new();
        e.record_sample(Duration::from_millis(350));
        assert_eq!(e.estimate_ms(), 350);
        // a below-floor sample still clamps to the minimum
        let e2 = RttEstimator::new();
        e2.record_sample(Duration::from_micros(500));
        assert_eq!(e2.estimate_ms(), RTT_MIN_MS);
    }

    #[test]
    fn ewma_converges_toward_steady_sample() {
        let e = RttEstimator::new();
        // Feed many ~200ms samples; estimate must converge near 200ms and stay
        // far from the initial 350ms outlier.
        e.record_sample(Duration::from_millis(350)); // initial
        for _ in 0..50 {
            e.record_sample(Duration::from_millis(200));
        }
        let est = e.estimate_ms();
        assert!(
            (190..=210).contains(&est),
            "EWMA must converge toward steady 200ms, got {est}"
        );
    }

    #[test]
    fn ewma_dampens_a_single_outlier() {
        let e = RttEstimator::new();
        for _ in 0..20 {
            e.record_sample(Duration::from_millis(100));
        }
        assert!(e.estimate_ms() <= 110, "settled near 100ms");
        // One pathological 4000ms sample (a stalled RPC under loss) must NOT
        // pin the estimate anywhere near 4000ms — it nudges, not jumps.
        e.record_sample(Duration::from_millis(4000));
        let est = e.estimate_ms();
        assert!(
            est < 500,
            "a single outlier must not dominate the EWMA, got {est}"
        );
    }

    #[test]
    fn pathological_sample_clamps_to_max() {
        let e = RttEstimator::new();
        // First sample is the entire estimate; a huge first sample clamps.
        e.record_sample(Duration::from_secs(60));
        assert_eq!(e.estimate_ms(), RTT_MAX_MS);
    }
}
