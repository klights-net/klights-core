//! Bounded `watch_events` retention (P0-LEAK-04).
//!
//! Each tick deletes rows below `MAX(id) - MAX_WATCH_EVENTS`, capped at
//! `BATCH_CAP_PER_TICK` rows per tick to keep the SQLite write small.
//! The SQLite GC also preserves a small per resource-scope floor so unrelated
//! write bursts cannot erase rare-kind history needed by lagged watches.
//!
//! The 100k-row cap is configurable via `KLIGHTS_MAX_WATCH_EVENTS`.

use crate::datastore::DatastoreHandle;
use anyhow::Result;
use async_trait::async_trait;

/// Default sliding-window size. Override with `KLIGHTS_MAX_WATCH_EVENTS`.
pub const DEFAULT_MAX_WATCH_EVENTS: i64 = 100_000;

/// Maximum rows deleted per tick. Bounds the SQLite write so the GC stays
/// snappy under sustained load (a backlog drains over several ticks).
pub const BATCH_CAP_PER_TICK: i64 = 5_000;

pub struct WatchEventsGc {
    db: DatastoreHandle,
    max_rows: i64,
    batch_cap: i64,
}

impl WatchEventsGc {
    pub fn new(db: DatastoreHandle) -> Self {
        let max_rows = std::env::var("KLIGHTS_MAX_WATCH_EVENTS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_MAX_WATCH_EVENTS);
        Self {
            db,
            max_rows,
            batch_cap: BATCH_CAP_PER_TICK,
        }
    }
}

#[async_trait]
impl super::GcTask for WatchEventsGc {
    fn name(&self) -> &'static str {
        "watch_events_gc"
    }
    async fn run(&self) -> Result<()> {
        let removed = self
            .db
            .gc_watch_events(self.max_rows, self.batch_cap)
            .await?;
        if removed > 0 {
            tracing::info!(
                watch_events_gc = true,
                removed,
                max_rows = self.max_rows,
                "watch_events_gc: tick complete"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::sqlite::Datastore;

    /// Insert N rows into watch_events directly, then GC and assert COUNT(*).
    async fn insert_n_events(db: &Datastore, n: i64) {
        // Borrow the underlying connection by going through create_resource —
        // each create_resource pushes one row to watch_events with a new RV.
        for i in 0..n {
            db.create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                &format!("cm-{}", i),
                serde_json::json!({"data": {"k": "v"}}),
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn gc_keeps_most_recent_rows_within_cap() {
        let db = crate::datastore::test_support::in_memory().await;
        // Insert more rows than the cap.
        let cap: i64 = 50;
        let total: i64 = 130;
        insert_n_events(&db, total).await;

        // Run GC with the small cap and a generous batch.
        let removed = db.gc_watch_events(cap, total).await.unwrap() as i64;
        assert!(
            removed >= total - cap,
            "GC must drop at least {} rows, removed only {}",
            total - cap,
            removed
        );

        // Verify the survivors fit within the cap (allow off-by-one for the
        // bound formula `id <= MAX(id) - cap`).
        let count = db.count_watch_events().await.unwrap();
        assert!(
            count <= cap + 1,
            "watch_events count {} must be within cap+1 ({})",
            count,
            cap + 1
        );
    }

    #[tokio::test]
    async fn gc_is_idempotent_when_table_within_cap() {
        let db = crate::datastore::test_support::in_memory().await;
        insert_n_events(&db, 10).await;
        let removed = db.gc_watch_events(100_000, 5_000).await.unwrap();
        assert_eq!(removed, 0, "GC must remove nothing when below cap");
    }

    #[tokio::test]
    async fn gc_respects_batch_cap() {
        let db = crate::datastore::test_support::in_memory().await;
        insert_n_events(&db, 200).await;
        let removed = db.gc_watch_events(50, 30).await.unwrap();
        assert!(
            removed <= 30usize,
            "GC must not exceed batch cap; removed {}",
            removed
        );
    }

    #[tokio::test]
    async fn gc_retains_rare_kind_history_despite_unrelated_churn() {
        let db = crate::datastore::test_support::in_memory().await;

        db.create_resource(
            "v1",
            "LimitRange",
            Some("default"),
            "limit-range",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "LimitRange",
                "metadata": {"name": "limit-range", "namespace": "default"},
                "spec": {
                    "limits": [{
                        "type": "Container",
                        "default": {"cpu": "500m"},
                        "defaultRequest": {"cpu": "100m"}
                    }]
                }
            }),
        )
        .await
        .unwrap();

        for i in 0..30 {
            db.create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                &format!("churn-{i}"),
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": format!("churn-{i}"), "namespace": "default"},
                    "data": {"k": "v"}
                }),
            )
            .await
            .unwrap();
        }

        let removed = db.gc_watch_events(5, 1000).await.unwrap();
        assert!(removed > 0, "GC must exercise the pruning path");

        let events = db
            .list_watch_events_since(
                &[crate::datastore::WatchTarget::namespaced_in_namespace(
                    "v1",
                    "LimitRange",
                    "default",
                )],
                0,
            )
            .await
            .unwrap();
        assert_eq!(
            events.len(),
            1,
            "rare-kind watch history must survive unrelated resource churn"
        );
    }

    #[tokio::test]
    async fn watch_events_gc_task_name() {
        let db = crate::datastore::test_support::in_memory().await;
        let task = WatchEventsGc::new(std::sync::Arc::new(db));
        assert_eq!(
            <WatchEventsGc as super::super::GcTask>::name(&task),
            "watch_events_gc"
        );
    }
}
