//! Bounded `applied_outbox` retention.
//!
//! The idempotency ledger is useful for retry/replay windows, but
//! Sonobuoy-scale churn can create thousands of rows per run. Node-local
//! outbox resend is bounded by the same twelve-hour ceiling, so rows older than
//! that are outside the resend window and can be pruned without an operation
//! allowlist.

use crate::datastore::DatastoreHandle;
use anyhow::Result;
use async_trait::async_trait;

pub const APPLIED_OUTBOX_GC_TTL_MS: i64 = 12 * 60 * 60 * 1000;
pub const APPLIED_OUTBOX_GC_INTERVAL_SECS: u64 = 60 * 60;

pub struct AppliedOutboxGc {
    db: DatastoreHandle,
}

impl AppliedOutboxGc {
    pub fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

#[async_trait]
impl super::GcTask for AppliedOutboxGc {
    fn name(&self) -> &'static str {
        "applied_outbox_gc"
    }

    async fn run(&self) -> Result<()> {
        let removed = self
            .db
            .gc_applied_outbox(now_ms(), APPLIED_OUTBOX_GC_TTL_MS)
            .await?;
        if removed > 0 {
            tracing::info!(
                applied_outbox_gc = true,
                removed,
                ttl_ms = APPLIED_OUTBOX_GC_TTL_MS,
                "applied_outbox_gc: tick complete"
            );
        }
        Ok(())
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
