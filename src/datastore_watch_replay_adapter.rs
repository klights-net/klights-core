use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::datastore::{
    CatchUpResource, PositionedWatchReplayRead as StorePositionedWatchReplayRead,
    WatchReplayRead as StoreWatchReplayRead, WatchStore, WatchTarget,
};
use crate::watch::{WatchEvent, WatchReplaySource};

pub struct DatastoreWatchReplaySource {
    watch_store: Arc<dyn WatchStore>,
    targets: Vec<WatchTarget>,
}

impl DatastoreWatchReplaySource {
    pub fn new(watch_store: Arc<dyn WatchStore>, targets: Vec<WatchTarget>) -> Self {
        Self {
            watch_store,
            targets,
        }
    }
}

#[async_trait]
impl WatchReplaySource for DatastoreWatchReplaySource {
    async fn replay_since(&self, since_rv: i64) -> Result<Vec<WatchEvent>> {
        let replay = self
            .watch_store
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
    ) -> Result<klights_watch::WatchReplayRead<WatchEvent>> {
        match self
            .watch_store
            .list_watch_events_since_checked_bounded(&self.targets, since_rv, limit)
            .await?
        {
            StoreWatchReplayRead::Events(events) => Ok(klights_watch::WatchReplayRead::Events(
                events
                    .into_iter()
                    .map(CatchUpResource::into_watch_event)
                    .collect(),
            )),
            StoreWatchReplayRead::Expired => Ok(klights_watch::WatchReplayRead::Expired),
        }
    }

    async fn replay_after_checked(
        &self,
        position: klights_watch::WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<klights_watch::PositionedWatchReplayRead<WatchEvent>> {
        match self
            .watch_store
            .list_watch_events_after_position_checked_bounded(&self.targets, position, limit)
            .await?
        {
            StorePositionedWatchReplayRead::Events(replay) => {
                Ok(klights_watch::PositionedWatchReplayRead::Events(
                    klights_watch::PositionedWatchReplay::new(
                        replay
                            .events
                            .into_iter()
                            .map(|positioned| klights_watch::PositionedWatchEvent {
                                position: positioned.position,
                                event: positioned.event.into_watch_event(),
                            })
                            .collect(),
                        replay.next_position,
                    ),
                ))
            }
            StorePositionedWatchReplayRead::Expired => {
                Ok(klights_watch::PositionedWatchReplayRead::Expired)
            }
        }
    }

    async fn earliest_retained_rv(&self) -> Result<Option<i64>> {
        self.watch_store.earliest_watch_event_rv().await
    }
}
