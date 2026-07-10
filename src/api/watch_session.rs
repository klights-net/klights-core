use crate::watch::{
    EventType, RawSignalWatchCursor, SignalWatchCursor, WatchCursorError, WatchDeliveryScope,
    WatchEvent, WatchReplaySource, WatchSignalReceiver, WatchTopic, WindowPolicy,
};
use std::collections::HashSet;

pub(crate) type WatchObjectKey = (Option<String>, String);

#[derive(Clone, Copy, Debug)]
pub(crate) struct WatchSessionConfig {
    pub requested_rv: i64,
    pub has_selector: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BaselineDisposition {
    Emitted,
    Retained,
}

#[derive(Debug)]
pub(crate) enum WatchSessionEvent {
    Deliver(WatchEvent),
    Filtered { key: WatchObjectKey, rv: i64 },
    Ignored,
}

impl WatchSessionEvent {
    pub(crate) fn into_deliverable(self) -> Option<WatchEvent> {
        match self {
            Self::Deliver(event) => Some(event),
            Self::Filtered { .. } | Self::Ignored => None,
        }
    }
}

/// Stateful watch establishment shared by built-in and custom-resource
/// adapters. It deliberately owns no datastore lookup, conversion, or wire
/// encoding: callers supply already selected resources/events and retain their
/// resource-specific responsibilities.
pub(crate) struct WatchSessionBootstrap {
    config: WatchSessionConfig,
    initial_list_rv: i64,
    last_delivered_scoped_rv: i64,
    matched_keys: HashSet<WatchObjectKey>,
    baseline_delivered: Vec<(WatchObjectKey, i64)>,
    baseline_low_rv_allowlist: Vec<(WatchObjectKey, i64)>,
}

impl WatchSessionBootstrap {
    pub(crate) fn new(config: WatchSessionConfig) -> Self {
        Self {
            initial_list_rv: config.requested_rv,
            last_delivered_scoped_rv: config.requested_rv,
            config,
            matched_keys: HashSet::new(),
            baseline_delivered: Vec::new(),
            baseline_low_rv_allowlist: Vec::new(),
        }
    }

    pub(crate) fn observe_catch_up_frontier(&mut self, rv: i64) {
        self.initial_list_rv = self.initial_list_rv.max(rv);
    }

    pub(crate) fn observe_snapshot_rv(&mut self, rv: i64) {
        self.observe_catch_up_frontier(rv);
        self.observe_delivered_rv(rv);
    }

    pub(crate) fn observe_delivered_event(&mut self, event: &WatchEvent) {
        if let Some(rv) = event.resource_version() {
            self.observe_delivered_rv(rv);
        }
    }

    pub(crate) fn observe_delivered_rv(&mut self, rv: i64) {
        self.last_delivered_scoped_rv = self.last_delivered_scoped_rv.max(rv);
    }

    pub(crate) fn record_baseline_member(
        &mut self,
        key: WatchObjectKey,
        rv: i64,
        disposition: BaselineDisposition,
    ) {
        self.matched_keys.insert(key.clone());
        match disposition {
            BaselineDisposition::Emitted => {
                self.baseline_delivered.push((key, rv));
                self.observe_delivered_rv(rv);
            }
            BaselineDisposition::Retained => {
                self.baseline_low_rv_allowlist.push((key, rv));
            }
        }
    }

    pub(crate) fn record_member(&mut self, key: WatchObjectKey) {
        self.matched_keys.insert(key);
    }

    pub(crate) fn classify_event(
        &mut self,
        event: WatchEvent,
        matches_selector: bool,
    ) -> WatchSessionEvent {
        classify_event(
            event,
            matches_selector,
            self.config.has_selector,
            &mut self.matched_keys,
        )
    }

    pub(crate) fn cursor_floor(&self) -> i64 {
        if self.config.has_selector {
            self.config.requested_rv.max(self.last_delivered_scoped_rv)
        } else {
            self.config.requested_rv.max(self.initial_list_rv)
        }
    }

    pub(crate) fn last_delivered_scoped_rv(&self) -> i64 {
        self.last_delivered_scoped_rv
    }

    pub(crate) fn seed_raw_cursor(&self, cursor: &mut RawSignalWatchCursor) {
        for ((namespace, name), rv) in &self.baseline_delivered {
            cursor.mark_delivered_for_key(namespace.clone(), name.clone(), *rv);
        }
    }

    #[cfg(test)]
    fn baseline_delivered(&self) -> &[(WatchObjectKey, i64)] {
        &self.baseline_delivered
    }

    #[cfg(test)]
    fn contains_member(&self, key: &WatchObjectKey) -> bool {
        self.matched_keys.contains(key)
    }

    pub(crate) fn establish_many<S: WatchReplaySource>(
        self,
        signal_rx: impl Into<WatchSignalReceiver>,
        replay_source: S,
        topics: Vec<WatchTopic>,
        delivery_scope: WatchDeliveryScope,
    ) -> WatchSession<S> {
        let mut cursor = SignalWatchCursor::new_many(
            signal_rx,
            replay_source,
            topics,
            delivery_scope,
            self.cursor_floor(),
            WindowPolicy::default_watch_delivery(),
        );
        for ((namespace, name), rv) in &self.baseline_delivered {
            cursor.mark_delivered_for_key(namespace.clone(), name.clone(), *rv);
        }
        for ((namespace, name), after_rv) in &self.baseline_low_rv_allowlist {
            cursor.allow_low_rv_for_key(namespace.clone(), name.clone(), *after_rv);
        }
        WatchSession {
            cursor,
            config: self.config,
            last_delivered_scoped_rv: self.last_delivered_scoped_rv,
            matched_keys: self.matched_keys,
        }
    }
}

pub(crate) struct WatchSession<S: WatchReplaySource> {
    cursor: SignalWatchCursor<S>,
    config: WatchSessionConfig,
    last_delivered_scoped_rv: i64,
    matched_keys: HashSet<WatchObjectKey>,
}

impl<S: WatchReplaySource> WatchSession<S> {
    pub(crate) async fn prime_replay_or_expired(&mut self) -> Result<usize, WatchCursorError> {
        self.cursor.prime_replay_or_expired().await
    }

    pub(crate) async fn next_event(&mut self) -> Result<WatchEvent, WatchCursorError> {
        self.cursor.next_event().await
    }

    pub(crate) fn classify_event(
        &mut self,
        event: WatchEvent,
        matches_selector: bool,
    ) -> WatchSessionEvent {
        classify_event(
            event,
            matches_selector,
            self.config.has_selector,
            &mut self.matched_keys,
        )
    }

    pub(crate) fn accept_delivered_rv(&mut self, rv: i64) {
        self.cursor.accept_event(rv);
        self.last_delivered_scoped_rv = self.last_delivered_scoped_rv.max(rv);
    }

    pub(crate) fn accept_filtered(&mut self, key: WatchObjectKey, rv: i64) {
        self.cursor.mark_filtered_for_key(key.0, key.1, rv);
    }

    pub(crate) fn accepted_rv(&self) -> i64 {
        self.cursor.accepted_rv()
    }

    pub(crate) fn last_delivered_scoped_rv(&self) -> i64 {
        self.last_delivered_scoped_rv
    }
}

fn classify_event(
    event: WatchEvent,
    matches_selector: bool,
    has_selector: bool,
    matched_keys: &mut HashSet<WatchObjectKey>,
) -> WatchSessionEvent {
    let event_rv = event.resource_version();
    let key = watch_event_key(&event);
    let event = if has_selector {
        apply_selector_transition_event(event, matches_selector, matched_keys)
    } else if matches_selector {
        Some(event)
    } else {
        None
    };
    match event {
        Some(event) => WatchSessionEvent::Deliver(event),
        None => match (key, event_rv) {
            (Some(key), Some(rv)) => WatchSessionEvent::Filtered { key, rv },
            _ => WatchSessionEvent::Ignored,
        },
    }
}

pub(crate) fn watch_event_key(event: &WatchEvent) -> Option<WatchObjectKey> {
    let name = event
        .object
        .pointer("/metadata/name")
        .and_then(|value| value.as_str())?
        .to_string();
    let namespace = event
        .object
        .pointer("/metadata/namespace")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    Some((namespace, name))
}

/// Extract baseline identity from JSON metadata, matching the live event path
/// even when storage classification differs from the Kubernetes scope.
pub(crate) fn resource_to_seen_key(resource: &crate::datastore::Resource) -> WatchObjectKey {
    let namespace = resource
        .data
        .pointer("/metadata/namespace")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    (namespace, resource.name.clone())
}

pub(crate) fn apply_selector_transition_event(
    mut event: WatchEvent,
    matches_selector: bool,
    matched_keys: &mut HashSet<WatchObjectKey>,
) -> Option<WatchEvent> {
    if event.event_type == EventType::Bookmark {
        return Some(event);
    }
    let key = watch_event_key(&event)?;
    let rewrite_event_type = |event: &mut WatchEvent, new_type| {
        if event.event_type != new_type {
            event.event_type = new_type;
            event.encoded_payload = None;
        }
    };
    match event.event_type {
        EventType::Deleted => {
            if matched_keys.remove(&key) || matches_selector {
                Some(event)
            } else {
                None
            }
        }
        EventType::Added | EventType::Modified => {
            if matches_selector {
                if matched_keys.insert(key) {
                    rewrite_event_type(&mut event, EventType::Added);
                }
                Some(event)
            } else if matched_keys.remove(&key) {
                rewrite_event_type(&mut event, EventType::Deleted);
                Some(event)
            } else {
                None
            }
        }
        _ if matches_selector => Some(event),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::{
        PositionedWatchEvent, PositionedWatchReplay, PositionedWatchReplayRead,
        WatchReplayPosition, WatchReplayRead,
    };
    use crate::watch::{EventType, WatchContentType, WatchEvent, encode_watch_payload};
    use async_trait::async_trait;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use tokio::sync::broadcast;

    struct ReplaySource {
        events: Vec<WatchEvent>,
        expired: bool,
    }

    #[async_trait]
    impl WatchReplaySource for ReplaySource {
        async fn replay_since(&self, since_rv: i64) -> anyhow::Result<Vec<WatchEvent>> {
            Ok(self
                .events
                .iter()
                .filter(|event| event.resource_version().is_some_and(|rv| rv > since_rv))
                .cloned()
                .collect())
        }

        async fn replay_since_checked(
            &self,
            since_rv: i64,
            _limit: NonZeroUsize,
        ) -> anyhow::Result<WatchReplayRead<WatchEvent>> {
            if self.expired {
                Ok(WatchReplayRead::Expired)
            } else {
                Ok(WatchReplayRead::Events(self.replay_since(since_rv).await?))
            }
        }

        async fn replay_after_checked(
            &self,
            position: WatchReplayPosition,
            limit: NonZeroUsize,
        ) -> anyhow::Result<PositionedWatchReplayRead<WatchEvent>> {
            if self.expired {
                return Ok(PositionedWatchReplayRead::Expired);
            }
            let events: Vec<_> = self
                .events
                .iter()
                .enumerate()
                .filter(|(index, event)| {
                    if position.event_id == 0 {
                        event.resource_version().unwrap_or_default() > position.resource_version
                    } else {
                        *index as i64 + 1 > position.event_id
                    }
                })
                .take(limit.get())
                .map(|(index, event)| PositionedWatchEvent {
                    position: WatchReplayPosition {
                        resource_version: event.resource_version().unwrap_or_default(),
                        event_id: index as i64 + 1,
                    },
                    event: event.clone(),
                })
                .collect();
            let next_position =
                WatchReplayPosition::after_page(position, &events, self.events.len() as i64, limit);
            Ok(PositionedWatchReplayRead::Events(PositionedWatchReplay {
                events,
                next_position,
            }))
        }
    }

    fn establish(
        bootstrap: WatchSessionBootstrap,
        source: ReplaySource,
    ) -> WatchSession<ReplaySource> {
        let (_tx, rx) = broadcast::channel(4);
        bootstrap.establish_many(
            rx,
            source,
            vec![WatchTopic::new("v1", "ConfigMap")],
            WatchDeliveryScope::NamespacedAll,
        )
    }

    fn event(event_type: EventType, namespace: &str, name: &str, rv: i64) -> WatchEvent {
        WatchEvent {
            event_type,
            object: Arc::new(serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": namespace,
                    "name": name,
                    "resourceVersion": rv.to_string(),
                }
            })),
            encoded_payload: None,
        }
    }

    #[test]
    fn built_in_and_cr_bootstrap_cases_share_cursor_floor_and_baseline_policy() {
        struct Case {
            name: &'static str,
            requested_rv: i64,
            has_selector: bool,
            catch_up_rv: i64,
            delivered_rv: i64,
            expected_floor: i64,
        }
        let cases = [
            Case {
                name: "rv omitted selector",
                requested_rv: 0,
                has_selector: true,
                catch_up_rv: 40,
                delivered_rv: 7,
                expected_floor: 7,
            },
            Case {
                name: "rv zero selector free",
                requested_rv: 0,
                has_selector: false,
                catch_up_rv: 40,
                delivered_rv: 7,
                expected_floor: 40,
            },
            Case {
                name: "rv positive selector",
                requested_rv: 20,
                has_selector: true,
                catch_up_rv: 40,
                delivered_rv: 27,
                expected_floor: 27,
            },
            Case {
                name: "rv positive selector no match",
                requested_rv: 20,
                has_selector: true,
                catch_up_rv: 40,
                delivered_rv: 0,
                expected_floor: 20,
            },
        ];

        for adapter in ["built-in", "custom-resource"] {
            for case in &cases {
                let mut bootstrap = WatchSessionBootstrap::new(WatchSessionConfig {
                    requested_rv: case.requested_rv,
                    has_selector: case.has_selector,
                });
                bootstrap.observe_catch_up_frontier(case.catch_up_rv);
                bootstrap.observe_delivered_rv(case.delivered_rv);
                assert_eq!(
                    bootstrap.cursor_floor(),
                    case.expected_floor,
                    "{adapter}: {}",
                    case.name
                );
            }
        }
    }

    #[test]
    fn built_in_and_cr_selector_transitions_have_wire_format_parity() {
        struct Step {
            event_type: EventType,
            matches: bool,
            expected: Option<EventType>,
        }
        let steps = [
            Step {
                event_type: EventType::Added,
                matches: true,
                expected: Some(EventType::Added),
            },
            Step {
                event_type: EventType::Modified,
                matches: true,
                expected: Some(EventType::Modified),
            },
            Step {
                event_type: EventType::Modified,
                matches: false,
                expected: Some(EventType::Deleted),
            },
            Step {
                event_type: EventType::Modified,
                matches: false,
                expected: None,
            },
            Step {
                event_type: EventType::Modified,
                matches: true,
                expected: Some(EventType::Added),
            },
            Step {
                event_type: EventType::Deleted,
                matches: true,
                expected: Some(EventType::Deleted),
            },
        ];

        for adapter in ["built-in", "custom-resource"] {
            for content_type in [WatchContentType::Json, WatchContentType::Protobuf] {
                let mut bootstrap = WatchSessionBootstrap::new(WatchSessionConfig {
                    requested_rv: 0,
                    has_selector: true,
                });
                for (index, step) in steps.iter().enumerate() {
                    let mut input = event(step.event_type, "default", "same", index as i64 + 1);
                    input.encoded_payload = (content_type == WatchContentType::Json)
                        .then(|| encode_watch_payload(&input, content_type).unwrap());
                    let actual = bootstrap
                        .classify_event(input, step.matches)
                        .into_deliverable();
                    assert_eq!(
                        actual.as_ref().map(|event| event.event_type),
                        step.expected,
                        "{adapter}/{content_type:?} step {index}"
                    );
                    if let Some(actual) = actual
                        && actual.event_type != step.event_type
                    {
                        assert!(
                            actual.encoded_payload.is_none(),
                            "{adapter}/{content_type:?}: rewritten events must invalidate cached bytes"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn baseline_dedupe_is_recorded_without_collapsing_same_rv_distinct_members() {
        for adapter in ["built-in", "custom-resource"] {
            let mut bootstrap = WatchSessionBootstrap::new(WatchSessionConfig {
                requested_rv: 0,
                has_selector: true,
            });
            bootstrap.record_baseline_member(
                (Some("a".into()), "shared".into()),
                9,
                BaselineDisposition::Emitted,
            );
            bootstrap.record_baseline_member(
                (Some("b".into()), "shared".into()),
                9,
                BaselineDisposition::Emitted,
            );
            assert_eq!(bootstrap.baseline_delivered().len(), 2, "{adapter}");
            assert!(
                bootstrap.contains_member(&(Some("a".into()), "shared".into())),
                "{adapter}"
            );
            assert!(
                bootstrap.contains_member(&(Some("b".into()), "shared".into())),
                "{adapter}"
            );
        }
    }

    #[test]
    fn filtered_events_and_delivered_events_progress_separate_frontiers() {
        let mut bootstrap = WatchSessionBootstrap::new(WatchSessionConfig {
            requested_rv: 10,
            has_selector: true,
        });
        let filtered =
            bootstrap.classify_event(event(EventType::Modified, "default", "filtered", 11), false);
        assert!(matches!(
            filtered,
            WatchSessionEvent::Filtered { rv: 11, .. }
        ));
        assert_eq!(bootstrap.last_delivered_scoped_rv(), 10);
        let delivered =
            bootstrap.classify_event(event(EventType::Added, "default", "visible", 11), true);
        let delivered = delivered.into_deliverable().unwrap();
        bootstrap.observe_delivered_event(&delivered);
        assert_eq!(bootstrap.last_delivered_scoped_rv(), 11);
    }

    #[tokio::test]
    async fn built_in_and_cr_identity_dedupe_preserves_same_rv_distinct_objects() {
        // A single positioned replay page carrying two distinct objects that
        // share one resourceVersion (a same-raft-revision pair) must deliver
        // both; the kernel must not collapse them by numeric RV. The
        // baseline-delivered dedup seed is covered by the bootstrap-level
        // `baseline_dedupe_is_recorded_*` unit tests.
        for adapter in ["built-in", "custom-resource"] {
            let bootstrap = WatchSessionBootstrap::new(WatchSessionConfig {
                requested_rv: 0,
                has_selector: true,
            });
            let source = ReplaySource {
                events: vec![
                    event(EventType::Added, "default", "first", 9),
                    event(EventType::Added, "default", "second", 9),
                ],
                expired: false,
            };
            let mut session = establish(bootstrap, source);
            session.prime_replay_or_expired().await.unwrap();
            let first = session.next_event().await.unwrap();
            assert_eq!(watch_event_key(&first).unwrap().1, "first", "{adapter}");
            let second = session.next_event().await.unwrap();
            assert_eq!(
                watch_event_key(&second).unwrap().1,
                "second",
                "{adapter}: same-rv distinct peer must not be collapsed"
            );
        }
    }

    #[tokio::test]
    async fn built_in_and_cr_filtered_same_rv_event_does_not_hide_visible_peer() {
        for adapter in ["built-in", "custom-resource"] {
            let bootstrap = WatchSessionBootstrap::new(WatchSessionConfig {
                requested_rv: 0,
                has_selector: true,
            });
            let source = ReplaySource {
                events: vec![
                    event(EventType::Added, "default", "filtered", 11),
                    event(EventType::Added, "default", "visible", 11),
                ],
                expired: false,
            };
            let mut session = establish(bootstrap, source);
            session.prime_replay_or_expired().await.unwrap();
            let filtered = session.next_event().await.unwrap();
            match session.classify_event(filtered, false) {
                WatchSessionEvent::Filtered { key, rv } => session.accept_filtered(key, rv),
                other => panic!("{adapter}: expected filtered event, got {other:?}"),
            }
            let visible = session.next_event().await.unwrap();
            assert_eq!(watch_event_key(&visible).unwrap().1, "visible", "{adapter}");
        }
    }

    #[tokio::test]
    async fn built_in_and_cr_prime_surface_expired_for_410_mapping() {
        for adapter in ["built-in", "custom-resource"] {
            let bootstrap = WatchSessionBootstrap::new(WatchSessionConfig {
                requested_rv: 10,
                has_selector: true,
            });
            let mut session = establish(
                bootstrap,
                ReplaySource {
                    events: Vec::new(),
                    expired: true,
                },
            );
            assert!(
                matches!(
                    session.prime_replay_or_expired().await,
                    Err(WatchCursorError::Expired)
                ),
                "{adapter} must map the shared expiry to a 410 watch status"
            );
        }
    }
}
