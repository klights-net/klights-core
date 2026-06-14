//! Global allocator (memory hard-requirement #3).
//!
//! klights uses jemalloc instead of the system (glibc) allocator. glibc's
//! per-thread arenas (up to `8 × ncpu`) retain freed memory under high
//! allocation churn — conformance traffic allocates heavily (JSON `Value`s,
//! protobuf encode/decode, watch frames) — and never return it to the OS,
//! so RSS balloons to multiple GB and never shrinks. jemalloc fragments far
//! less and, with the background purge thread enabled, proactively returns
//! dirty pages to the OS so RSS falls back toward the idle baseline after a
//! load spike.
//!
//! Zero-idle-CPU (hard requirement #1) still holds: the purge thread is
//! decay-driven and sleeps once its decay queue drains, so a steady idle
//! heap produces no wakeups. It is only enabled for long-lived runtime
//! roles, never for short-lived CNI / exec / wrapper invocations.

use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Enable jemalloc's background purge thread so dirty pages are returned to
/// the OS proactively (decay-timer driven) rather than only during later
/// allocator activity. Best-effort and idempotent: a failure to enable it
/// (e.g. a platform without background-thread support) is logged, not fatal —
/// jemalloc still purges on the decay timer during allocation activity.
pub fn enable_background_purge() {
    match tikv_jemalloc_ctl::background_thread::write(true) {
        Ok(()) => tracing::debug!("jemalloc background purge thread enabled"),
        Err(err) => {
            tracing::warn!(error = %err, "failed to enable jemalloc background purge thread")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jemalloc_is_the_global_allocator() {
        // Proves the program's allocations route through jemalloc: its
        // in-use `stats.allocated` counter must grow when we retain a heap
        // buffer. If the system allocator were still in use, jemalloc's
        // stats would not reflect the allocation.
        use tikv_jemalloc_ctl::{epoch, stats};

        // `epoch::advance()` refreshes jemalloc's cached statistics before
        // each read (jemalloc must be the allocator for these ctls to work).
        epoch::advance().expect("advance epoch");
        let before = stats::allocated::read().expect("read allocated before");

        let buf: Vec<u8> = vec![7u8; 16 * 1024 * 1024];
        std::hint::black_box(&buf);

        epoch::advance().expect("advance epoch");
        let after = stats::allocated::read().expect("read allocated after");

        assert!(
            after > before,
            "jemalloc allocated stat must grow when the program allocates \
             ({before} -> {after}); is jemalloc the #[global_allocator]?"
        );
        drop(buf);
    }

    #[test]
    fn background_purge_enable_is_idempotent_and_nonfatal() {
        // Must be safe to call repeatedly and must never panic.
        enable_background_purge();
        enable_background_purge();
    }
}
