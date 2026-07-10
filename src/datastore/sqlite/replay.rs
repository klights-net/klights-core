use anyhow::Result;

use async_trait::async_trait;

use crate::watch::{WatchEvent, WatchReplaySource};

use super::{CatchUpResource, DatastoreHandle, WatchReplayRead, WatchTarget};
use crate::datastore::{
    PositionedWatchEvent, PositionedWatchReplay, PositionedWatchReplayRead, WatchReplayPosition,
};

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

    async fn replay_since_checked(
        &self,
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead<WatchEvent>> {
        match self
            .db
            .list_watch_events_since_checked_bounded(&self.targets, since_rv, limit)
            .await?
        {
            WatchReplayRead::Events(events) => Ok(WatchReplayRead::Events(
                events
                    .into_iter()
                    .map(CatchUpResource::into_watch_event)
                    .collect(),
            )),
            WatchReplayRead::Expired => Ok(WatchReplayRead::Expired),
        }
    }

    async fn replay_after_checked(
        &self,
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<WatchEvent>> {
        match self
            .db
            .list_watch_events_after_position_checked_bounded(&self.targets, position, limit)
            .await?
        {
            PositionedWatchReplayRead::Events(replay) => {
                Ok(PositionedWatchReplayRead::Events(PositionedWatchReplay {
                    next_position: replay.next_position,
                    events: replay
                        .events
                        .into_iter()
                        .map(|positioned| PositionedWatchEvent {
                            position: positioned.position,
                            event: positioned.event.into_watch_event(),
                        })
                        .collect(),
                }))
            }
            PositionedWatchReplayRead::Expired => Ok(PositionedWatchReplayRead::Expired),
        }
    }

    async fn earliest_retained_rv(&self) -> Result<Option<i64>> {
        self.db.earliest_watch_event_rv().await
    }
}
