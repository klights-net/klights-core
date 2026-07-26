use anyhow::Result;
use std::sync::Arc;

use crate::datastore::{RawWatchEvent, RawWatchReplayStore, WatchTarget};

use super::signal_replay_cursor_core::{SignalReplayCursorCore, SignalReplayCursorSource};
use klights_watch::{
    PositionedWatchReplay, PositionedWatchReplayRead, WatchReplayPosition, WatchSignalReceiver,
    WatchTopic,
};

use super::{WatchCursorError, WatchDeliveryScope, WindowPolicy};

pub struct RawSignalWatchCursor {
    core: SignalReplayCursorCore<RawWatchEvent, RawWatchReplaySource>,
}

impl RawSignalWatchCursor {
    pub fn new(
        signal_rx: impl Into<WatchSignalReceiver>,
        replay_store: Arc<dyn RawWatchReplayStore>,
        targets: Vec<WatchTarget>,
        topic: WatchTopic,
        scope: WatchDeliveryScope,
        accepted_rv: i64,
    ) -> Self {
        Self::new_at_position(
            signal_rx,
            replay_store,
            targets,
            topic,
            scope,
            accepted_rv,
            WatchReplayPosition::from_resource_version(accepted_rv),
        )
    }

    pub fn new_at_position(
        signal_rx: impl Into<WatchSignalReceiver>,
        replay_store: Arc<dyn RawWatchReplayStore>,
        targets: Vec<WatchTarget>,
        topic: WatchTopic,
        scope: WatchDeliveryScope,
        accepted_rv: i64,
        replay_position: WatchReplayPosition,
    ) -> Self {
        Self {
            core: SignalReplayCursorCore::new_at_position(
                signal_rx,
                RawWatchReplaySource {
                    replay_store,
                    targets,
                },
                vec![topic],
                scope,
                accepted_rv,
                replay_position,
                WindowPolicy::default_watch_delivery(),
            ),
        }
    }

    pub fn accepted_rv(&self) -> i64 {
        self.core.accepted_rv()
    }

    pub fn accept_event(&mut self, rv: i64) {
        self.core.accept_event(rv);
    }

    pub async fn prime_replay_or_expired(&mut self) -> Result<usize, WatchCursorError> {
        self.core.prime_replay_or_expired().await
    }

    pub async fn next_event(&mut self) -> Result<RawWatchEvent, WatchCursorError> {
        self.core.next_event().await
    }
}

struct RawWatchReplaySource {
    replay_store: Arc<dyn RawWatchReplayStore>,
    targets: Vec<WatchTarget>,
}

#[async_trait::async_trait]
impl SignalReplayCursorSource<RawWatchEvent> for RawWatchReplaySource {
    async fn replay_after_checked(
        &self,
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<RawWatchEvent>> {
        match self
            .replay_store
            .list_raw_watch_events_after_position_checked_bounded(&self.targets, position, limit)
            .await?
        {
            crate::datastore::PositionedWatchReplayRead::Events(replay) => {
                Ok(PositionedWatchReplayRead::Events(
                    PositionedWatchReplay::new(replay.events, replay.next_position),
                ))
            }
            crate::datastore::PositionedWatchReplayRead::Expired => {
                Ok(PositionedWatchReplayRead::Expired)
            }
        }
    }
}
