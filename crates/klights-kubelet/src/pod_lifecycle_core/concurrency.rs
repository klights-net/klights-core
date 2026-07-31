//! Concurrency primitives shared by actor and multiplex backends.
//!
//! `WorkPermit` and `ProbePermit` enforce global and per-pod concurrency
//! limits. In actor mode (R2a), these are unused placeholders;
//! multiplex mode fills in the semaphore-backed implementation.

/// Permit that bounds the number of concurrent lifecycle work tasks
/// (start, stop, finalize, reconcile, restart, handle-command, ephemeral).
///
/// Actor mode sets this to `None` — per-pod actors provide natural
/// serialization. Multiplex mode wraps a semaphore permit that drops
/// when the work future exits.
#[derive(Debug)]
pub struct WorkPermit {
    // Placeholder: multiplex mode fills this in.
    pub _private: (),
}

/// Permit that bounds the number of concurrent probe tasks.
///
/// Actor mode sets this to `None`. Multiplex mode wraps a semaphore
/// permit.
#[derive(Debug)]
pub struct ProbePermit {
    pub _private: (),
}
