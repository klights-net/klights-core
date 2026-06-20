//! Watch replay abstraction.

use anyhow::Result;

#[async_trait::async_trait]
pub trait WatchReplaySource: Send + Sync {
    async fn replay_since(&self, since_rv: i64) -> Result<Vec<super::events::WatchEvent>>;

    async fn replay_since_checked(
        &self,
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<crate::datastore::WatchReplayRead<super::events::WatchEvent>> {
        let mut events = self.replay_since(since_rv).await?;
        events.truncate(limit.get());
        Ok(crate::datastore::WatchReplayRead::Events(events))
    }

    /// Lowest `resourceVersion` still retained in the durable watch-event
    /// window, or `None` when no events are retained. Used to detect when a
    /// requested resume point predates the window so the watch can return a
    /// `410 Gone` (Expired) instead of silently delivering a truncated replay.
    /// Defaults to `None` (never report a gap) so non-datastore sources and
    /// test doubles keep their existing behavior.
    async fn earliest_retained_rv(&self) -> Result<Option<i64>> {
        Ok(None)
    }
}

#[derive(Debug)]
pub enum WatchCursorError {
    Closed,
    Replay(anyhow::Error),
    /// The requested/resume `resourceVersion` is older than the oldest
    /// retained watch event, so the gap between them can no longer be
    /// replayed. The HTTP watch must surface this as `410 Gone` (Expired)
    /// so the client reflector performs a fresh list+watch. Mirrors the
    /// Kubernetes apiserver "too old resource version" semantics.
    Expired,
}
