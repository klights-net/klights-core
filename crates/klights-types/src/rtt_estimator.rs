//! Passive, idle-silent RTT estimation shared by transport policy adapters.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Default RTT in milliseconds before the first successful sample.
pub const RTT_DEFAULT_MS: i64 = 200;
const EWMA_ALPHA: f64 = 0.1;
const RTT_MIN_MS: i64 = 10;
const RTT_MAX_MS: i64 = 5_000;
const NO_SAMPLE: u64 = 0;

/// Lock-free EWMA updated only by completed transport operations.
#[derive(Debug)]
pub struct RttEstimator {
    estimate_micro_ms: AtomicU64,
}

impl RttEstimator {
    pub fn new() -> Self {
        Self {
            estimate_micro_ms: AtomicU64::new(NO_SAMPLE),
        }
    }

    pub fn record_sample(&self, elapsed: Duration) {
        let sample_ms = elapsed.as_millis().max(1) as i64;
        loop {
            let previous = self.estimate_micro_ms.load(Ordering::Relaxed);
            let next_ms = if previous == NO_SAMPLE {
                sample_ms
            } else {
                let previous_ms = (previous as f64) / 1_000.0;
                (EWMA_ALPHA * sample_ms as f64 + (1.0 - EWMA_ALPHA) * previous_ms) as i64
            };
            let encoded = (next_ms.clamp(RTT_MIN_MS, RTT_MAX_MS) as u64) * 1_000;
            if self
                .estimate_micro_ms
                .compare_exchange(previous, encoded, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    pub fn estimate_ms(&self) -> i64 {
        let raw = self.estimate_micro_ms.load(Ordering::Relaxed);
        if raw == NO_SAMPLE {
            RTT_DEFAULT_MS
        } else {
            ((raw as f64) / 1_000.0).round() as i64
        }
    }
}

impl Default for RttEstimator {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for RttEstimator {
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
        assert_eq!(RttEstimator::new().estimate_ms(), RTT_DEFAULT_MS);
    }

    #[test]
    fn first_sample_is_clamped() {
        let estimator = RttEstimator::new();
        estimator.record_sample(Duration::from_millis(350));
        assert_eq!(estimator.estimate_ms(), 350);

        let estimator = RttEstimator::new();
        estimator.record_sample(Duration::from_micros(500));
        assert_eq!(estimator.estimate_ms(), RTT_MIN_MS);
    }

    #[test]
    fn ewma_converges_and_dampens_outliers() {
        let estimator = RttEstimator::new();
        estimator.record_sample(Duration::from_millis(350));
        for _ in 0..50 {
            estimator.record_sample(Duration::from_millis(200));
        }
        assert!((190..=210).contains(&estimator.estimate_ms()));

        let estimator = RttEstimator::new();
        for _ in 0..20 {
            estimator.record_sample(Duration::from_millis(100));
        }
        estimator.record_sample(Duration::from_millis(4_000));
        assert!(estimator.estimate_ms() < 500);
    }

    #[test]
    fn pathological_first_sample_clamps_to_maximum() {
        let estimator = RttEstimator::new();
        estimator.record_sample(Duration::from_secs(60));
        assert_eq!(estimator.estimate_ms(), RTT_MAX_MS);
    }
}
