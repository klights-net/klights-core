//! Value-only durable watch replay position transitions.

/// Lossless durable watch-log cursor. `event_id == 0` denotes an external
/// Kubernetes resourceVersion boundary; once replay starts, `event_id` is the
/// authoritative insertion/apply-order position and permits later-applied rows
/// whose resourceVersion is lower than an already observed row.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WatchReplayPosition {
    pub resource_version: i64,
    pub event_id: i64,
    /// While non-zero, rows through this durable event ID are filtered by
    /// `resource_version`; later rows are selected solely by event ID.
    pub resource_version_filter_through_event_id: i64,
}

impl WatchReplayPosition {
    pub const fn from_resource_version(resource_version: i64) -> Self {
        Self {
            resource_version,
            event_id: 0,
            resource_version_filter_through_event_id: 0,
        }
    }

    pub const fn from_resource_version_through_event_id(
        resource_version: i64,
        event_id: i64,
    ) -> Self {
        Self {
            resource_version,
            event_id: 0,
            resource_version_filter_through_event_id: event_id,
        }
    }

    /// Advance through returned rows, or anchor an empty page at its high water.
    pub fn after_page<T>(
        current: Self,
        events: &[PositionedWatchEvent<T>],
        high_water_event_id: i64,
        _limit: std::num::NonZeroUsize,
    ) -> Self {
        if let Some(event) = events.last() {
            let mut next = event.position;
            if next.event_id < current.resource_version_filter_through_event_id {
                next.resource_version = current.resource_version;
                next.resource_version_filter_through_event_id =
                    current.resource_version_filter_through_event_id;
            }
            return next;
        }
        Self {
            resource_version: current.resource_version,
            event_id: high_water_event_id,
            resource_version_filter_through_event_id: if high_water_event_id
                < current.resource_version_filter_through_event_id
            {
                current.resource_version_filter_through_event_id
            } else {
                0
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct PositionedWatchEvent<T> {
    pub position: WatchReplayPosition,
    pub event: T,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_position_page_transition_is_table_driven() {
        let limit = std::num::NonZeroUsize::new(2).unwrap();
        struct Case {
            name: &'static str,
            current: WatchReplayPosition,
            positions: Vec<WatchReplayPosition>,
            high_water: i64,
            expected: WatchReplayPosition,
        }
        let boundary = WatchReplayPosition::from_resource_version_through_event_id(10, 5);
        let cases = [
            Case {
                name: "returned event before handoff boundary keeps RV filter",
                current: boundary,
                positions: vec![WatchReplayPosition {
                    resource_version: 11,
                    event_id: 4,
                    resource_version_filter_through_event_id: 0,
                }],
                high_water: 8,
                expected: WatchReplayPosition {
                    resource_version: 10,
                    event_id: 4,
                    resource_version_filter_through_event_id: 5,
                },
            },
            Case {
                name: "returned event after handoff boundary becomes exact",
                current: boundary,
                positions: vec![WatchReplayPosition {
                    resource_version: 9,
                    event_id: 6,
                    resource_version_filter_through_event_id: 0,
                }],
                high_water: 8,
                expected: WatchReplayPosition {
                    resource_version: 9,
                    event_id: 6,
                    resource_version_filter_through_event_id: 0,
                },
            },
            Case {
                name: "empty page anchors at high water",
                current: boundary,
                positions: vec![],
                high_water: 8,
                expected: WatchReplayPosition {
                    resource_version: 10,
                    event_id: 8,
                    resource_version_filter_through_event_id: 0,
                },
            },
        ];

        for case in cases {
            let events: Vec<_> = case
                .positions
                .into_iter()
                .map(|position| PositionedWatchEvent {
                    position,
                    event: (),
                })
                .collect();
            assert_eq!(
                WatchReplayPosition::after_page(case.current, &events, case.high_water, limit),
                case.expected,
                "{}",
                case.name
            );
        }
    }
}
