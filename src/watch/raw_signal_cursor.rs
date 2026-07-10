use anyhow::Result;

use crate::datastore::{
    DatastoreHandle, PositionedWatchReplayRead, RawWatchEvent, WatchReplayPosition, WatchTarget,
};

use super::signal_replay_cursor_core::{SignalReplayCursorCore, SignalReplayCursorSource};
use super::{WatchCursorError, WatchDeliveryScope, WatchSignalReceiver, WatchTopic, WindowPolicy};

pub struct RawSignalWatchCursor {
    core: SignalReplayCursorCore<RawWatchEvent, RawWatchReplaySource>,
}

impl RawSignalWatchCursor {
    pub fn new(
        signal_rx: impl Into<WatchSignalReceiver>,
        db: DatastoreHandle,
        targets: Vec<WatchTarget>,
        topic: WatchTopic,
        scope: WatchDeliveryScope,
        accepted_rv: i64,
    ) -> Self {
        Self {
            core: SignalReplayCursorCore::new(
                signal_rx,
                RawWatchReplaySource { db, targets },
                vec![topic],
                scope,
                accepted_rv,
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

    pub fn mark_delivered(&mut self, rv: i64) {
        self.core.mark_delivered(rv);
    }

    pub fn mark_delivered_for_key(&mut self, namespace: Option<String>, name: String, rv: i64) {
        self.core.mark_delivered_for_key(namespace, name, rv);
    }

    pub async fn prime_replay_or_expired(&mut self) -> Result<usize, WatchCursorError> {
        self.core.prime_replay_or_expired().await
    }

    pub async fn next_event(&mut self) -> Result<RawWatchEvent, WatchCursorError> {
        self.core.next_event().await
    }
}

struct RawWatchReplaySource {
    db: DatastoreHandle,
    targets: Vec<WatchTarget>,
}

#[async_trait::async_trait]
impl SignalReplayCursorSource<RawWatchEvent> for RawWatchReplaySource {
    async fn replay_after_checked(
        &self,
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<RawWatchEvent>> {
        self.db
            .list_raw_watch_events_after_position_checked_bounded(&self.targets, position, limit)
            .await
    }
}
