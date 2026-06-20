use std::collections::VecDeque;

use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

use crate::datastore::WatchReplayRead;

use super::{
    WatchCursorError, WatchDeliveryScope, WatchEvent, WatchReplaySource, WatchSignal, WatchTopic,
    WindowPolicy,
};

pub struct SignalWatchCursor<S> {
    signal_rx: broadcast::Receiver<WatchSignal>,
    replay_source: S,
    topic: WatchTopic,
    scope: WatchDeliveryScope,
    accepted_rv: i64,
    pending: VecDeque<WatchEvent>,
    window: WindowPolicy,
    replay_needed: bool,
}

impl<S: WatchReplaySource> SignalWatchCursor<S> {
    pub fn new(
        signal_rx: broadcast::Receiver<WatchSignal>,
        replay_source: S,
        topic: WatchTopic,
        scope: WatchDeliveryScope,
        accepted_rv: i64,
        window: WindowPolicy,
    ) -> Self {
        Self {
            signal_rx,
            replay_source,
            topic,
            scope,
            accepted_rv,
            pending: VecDeque::new(),
            window,
            replay_needed: false,
        }
    }

    pub fn accepted_rv(&self) -> i64 {
        self.accepted_rv
    }

    pub fn accept_event(&mut self, rv: i64) {
        if rv > self.accepted_rv {
            self.accepted_rv = rv;
        }
    }

    pub async fn prime_replay_or_expired(&mut self) -> Result<usize, WatchCursorError> {
        self.replay_once().await
    }

    pub async fn next_event(&mut self) -> Result<WatchEvent, WatchCursorError> {
        loop {
            if let Some(event) = self.pop_pending_event() {
                return Ok(event);
            }

            if self.replay_needed {
                self.replay_needed = false;
                self.replay_once().await?;
                continue;
            }

            match self.signal_rx.recv().await {
                Ok(signal) => {
                    if self.matching_signal_high_rv(&signal).is_none() {
                        continue;
                    }
                    self.replay_once().await?;
                }
                Err(RecvError::Lagged(_)) => {
                    self.replay_needed = true;
                }
                Err(RecvError::Closed) => return Err(WatchCursorError::Closed),
            }
        }
    }

    async fn replay_once(&mut self) -> Result<usize, WatchCursorError> {
        let limit = self.window.limit();
        let replay = self
            .replay_source
            .replay_since_checked(self.accepted_rv, limit)
            .await
            .map_err(WatchCursorError::Replay)?;
        match replay {
            WatchReplayRead::Events(events) => {
                let event_count = events.len();
                self.replay_needed = event_count == limit.get();
                self.pending.extend(events);
                Ok(event_count)
            }
            WatchReplayRead::Expired => Err(WatchCursorError::Expired),
        }
    }

    fn pop_pending_event(&mut self) -> Option<WatchEvent> {
        while let Some(event) = self.pending.pop_front() {
            let Some(rv) = event.resource_version() else {
                continue;
            };
            if rv <= self.accepted_rv {
                continue;
            }
            if !self.event_matches(&event) {
                self.accept_event(rv);
                continue;
            }
            self.accept_event(rv);
            return Some(event);
        }
        None
    }

    fn matching_signal_high_rv(&self, signal: &WatchSignal) -> Option<i64> {
        if signal.topic != self.topic {
            return None;
        }
        signal
            .advances
            .iter()
            .filter(|advance| {
                advance.high_rv > self.accepted_rv
                    && self.scope.matches_namespace(advance.namespace.as_deref())
            })
            .map(|advance| advance.high_rv)
            .max()
    }

    fn event_matches(&self, event: &WatchEvent) -> bool {
        let api_version = event
            .object
            .get("apiVersion")
            .and_then(|value| value.as_str());
        let kind = event.object.get("kind").and_then(|value| value.as_str());
        if api_version != Some(self.topic.api_version()) || kind != Some(self.topic.kind()) {
            return false;
        }
        self.scope.matches_namespace(event_namespace(event))
    }
}

fn event_namespace(event: &WatchEvent) -> Option<&str> {
    event
        .object
        .get("metadata")
        .and_then(|metadata| metadata.get("namespace"))
        .and_then(|namespace| namespace.as_str())
}
