use anyhow::Result;

use async_trait::async_trait;

use crate::watch::WatchReplaySource;

use super::{CatchUpResource, DatastoreHandle, WatchEvent, WatchTarget};

pub struct DatastoreWatchReplaySource {
    db: DatastoreHandle,
    targets: Vec<WatchTarget>,
}

impl DatastoreWatchReplaySource {
    pub fn new(db: DatastoreHandle, targets: Vec<WatchTarget>) -> Self {
        Self { db, targets }
    }
}

#[async_trait]
impl WatchReplaySource for DatastoreWatchReplaySource {
    async fn replay_since(&self, since_rv: i64) -> Result<Vec<WatchEvent>> {
        let replay = self
            .db
            .list_watch_events_since(&self.targets, since_rv)
            .await?;
        Ok(replay
            .into_iter()
            .map(CatchUpResource::into_watch_event)
            .collect())
    }

    async fn earliest_retained_rv(&self) -> Result<Option<i64>> {
        self.db.earliest_watch_event_rv().await
    }
}
