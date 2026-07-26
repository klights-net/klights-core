use std::sync::Arc;

use crate::watch::WatchReplaySource;
use klights_watch::{WatchTarget, WatchTopic};

pub struct BoxedWatchReplaySource {
    inner: Arc<dyn WatchReplaySource>,
}

impl BoxedWatchReplaySource {
    pub fn new(inner: Arc<dyn WatchReplaySource>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl WatchReplaySource for BoxedWatchReplaySource {
    async fn replay_since(&self, since_rv: i64) -> anyhow::Result<Vec<crate::watch::WatchEvent>> {
        self.inner.replay_since(since_rv).await
    }

    async fn replay_since_checked(
        &self,
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> anyhow::Result<klights_watch::WatchReplayRead<crate::watch::WatchEvent>> {
        self.inner.replay_since_checked(since_rv, limit).await
    }

    async fn replay_after_checked(
        &self,
        position: klights_watch::WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> anyhow::Result<klights_watch::PositionedWatchReplayRead<crate::watch::WatchEvent>> {
        self.inner.replay_after_checked(position, limit).await
    }

    async fn earliest_retained_rv(&self) -> anyhow::Result<Option<i64>> {
        self.inner.earliest_retained_rv().await
    }
}

#[async_trait::async_trait]
pub trait PodWatchSource: Send + Sync {
    fn subscribe_watch_signals(&self, topic: WatchTopic) -> klights_watch::WatchSignalReceiver;

    fn replay_source(&self, targets: Vec<WatchTarget>) -> BoxedWatchReplaySource;

    async fn current_resource_version(&self) -> anyhow::Result<i64>;
}
