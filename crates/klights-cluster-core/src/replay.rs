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

    /// Whether an event's post-state is already represented by this cursor.
    ///
    /// A partially consumed composite cursor represents both rows explicitly
    /// consumed through `event_id` and rows at or below its resourceVersion
    /// filter through the establishment anchor. This is the exact complement
    /// of positioned replay.
    pub const fn represents_event(self, event_id: i64, resource_version: i64) -> bool {
        if event_id <= self.event_id {
            return true;
        }
        if self.resource_version_filter_through_event_id > 0 {
            return event_id <= self.resource_version_filter_through_event_id
                && resource_version <= self.resource_version;
        }
        self.event_id == 0 && resource_version <= self.resource_version
    }

    /// Validate the canonical composite cursor representation.
    pub fn validate(self) -> Result<(), String> {
        if self.resource_version < 0
            || self.event_id < 0
            || self.resource_version_filter_through_event_id < 0
        {
            return Err(format!("replay position must be non-negative: {self:?}"));
        }
        if self.resource_version_filter_through_event_id > 0
            && self.event_id >= self.resource_version_filter_through_event_id
        {
            return Err(format!(
                "replay position retained a completed resourceVersion filter boundary: {self:?}"
            ));
        }
        Ok(())
    }

    pub fn validate_exact(self) -> Result<(), String> {
        self.validate()?;
        if self.resource_version_filter_through_event_id != 0 {
            return Err(format!(
                "exact replay position must not carry a composite filter: {self:?}"
            ));
        }
        Ok(())
    }

    /// Whether `next` is a canonical non-regressing continuation of `self`.
    pub fn permits_successor(self, next: Self) -> bool {
        if self.validate().is_err() || next.validate().is_err() {
            return false;
        }
        if next.event_id < self.event_id {
            return false;
        }
        if next.event_id == self.event_id {
            return next == self;
        }
        let boundary = self.resource_version_filter_through_event_id;
        if boundary == 0 {
            return next.resource_version_filter_through_event_id == 0;
        }
        if next.event_id < boundary {
            next.resource_version == self.resource_version
                && next.resource_version_filter_through_event_id == boundary
        } else {
            next.resource_version_filter_through_event_id == 0
        }
    }

    /// Fold one exact durable event position through this composite cursor.
    pub fn advance_through_event(self, event: Self) -> Result<Self, String> {
        self.validate()?;
        event.validate_exact()?;
        if event.event_id <= self.event_id {
            return Err(format!(
                "event position {event:?} does not strictly follow cursor {self:?}"
            ));
        }
        if self.resource_version_filter_through_event_id > 0
            && event.event_id <= self.resource_version_filter_through_event_id
            && event.resource_version <= self.resource_version
        {
            return Err(format!(
                "event position {event:?} is excluded by composite cursor {self:?}"
            ));
        }
        let next = if self.resource_version_filter_through_event_id > 0
            && event.event_id < self.resource_version_filter_through_event_id
        {
            Self {
                resource_version: self.resource_version,
                event_id: event.event_id,
                resource_version_filter_through_event_id: self
                    .resource_version_filter_through_event_id,
            }
        } else {
            event
        };
        debug_assert!(self.permits_successor(next));
        Ok(next)
    }

    pub fn advance_to_high_water(self, high_water_event_id: i64) -> Result<Self, String> {
        self.validate()?;
        if high_water_event_id < self.event_id {
            return Err(format!(
                "high-water event ID {high_water_event_id} regresses cursor {self:?}"
            ));
        }
        if high_water_event_id == self.event_id {
            return Ok(self);
        }
        let next = Self {
            resource_version: self.resource_version,
            event_id: high_water_event_id,
            resource_version_filter_through_event_id: if high_water_event_id
                < self.resource_version_filter_through_event_id
            {
                self.resource_version_filter_through_event_id
            } else {
                0
            },
        };
        debug_assert!(self.permits_successor(next));
        Ok(next)
    }

    /// Advance through returned rows, or anchor an empty page at its high water.
    pub fn after_page<T>(
        current: Self,
        events: &[PositionedWatchEvent<T>],
        high_water_event_id: i64,
        _limit: std::num::NonZeroUsize,
    ) -> Self {
        if let Some(event) = events.last() {
            return current
                .advance_through_event(event.position)
                .expect("watch-history page positions must be validated before transition");
        }
        current
            .advance_to_high_water(high_water_event_id)
            .expect("watch-history high water must be validated before transition")
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

    #[test]
    fn represented_event_shapes_are_table_driven() {
        let exact = WatchReplayPosition {
            resource_version: 50,
            event_id: 10,
            resource_version_filter_through_event_id: 0,
        };
        let composite = WatchReplayPosition::from_resource_version_through_event_id(50, 10);
        let scalar = WatchReplayPosition::from_resource_version(50);
        let cases = [
            ("exact consumed event", exact, 10, 50, true),
            ("exact later lower RV", exact, 11, 40, false),
            ("composite filtered lower RV", composite, 10, 40, true),
            ("composite later lower RV", composite, 11, 40, false),
            ("scalar equal RV", scalar, 11, 50, true),
            ("scalar later RV", scalar, 11, 51, false),
        ];

        for (name, position, event_id, resource_version, expected) in cases {
            assert_eq!(
                position.represents_event(event_id, resource_version),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn composite_successor_validation_is_full_and_monotonic() {
        let current = WatchReplayPosition {
            resource_version: 10,
            event_id: 3,
            resource_version_filter_through_event_id: 5,
        };
        for successor in [
            current,
            WatchReplayPosition {
                resource_version: 10,
                event_id: 4,
                resource_version_filter_through_event_id: 5,
            },
            WatchReplayPosition {
                resource_version: 11,
                event_id: 5,
                resource_version_filter_through_event_id: 0,
            },
        ] {
            assert!(current.permits_successor(successor), "{successor:?}");
        }
        for rejected in [
            WatchReplayPosition {
                resource_version: 11,
                ..current
            },
            WatchReplayPosition {
                resource_version: 10,
                event_id: 4,
                resource_version_filter_through_event_id: 0,
            },
            WatchReplayPosition {
                resource_version: 10,
                event_id: 5,
                resource_version_filter_through_event_id: 6,
            },
            WatchReplayPosition {
                resource_version: 10,
                event_id: 2,
                resource_version_filter_through_event_id: 5,
            },
        ] {
            assert!(!current.permits_successor(rejected), "{rejected:?}");
        }
    }

    #[test]
    fn equal_event_id_requires_the_entire_cursor_to_match() {
        let current = WatchReplayPosition {
            resource_version: 10,
            event_id: 3,
            resource_version_filter_through_event_id: 5,
        };
        for changed in [
            WatchReplayPosition {
                resource_version: 11,
                ..current
            },
            WatchReplayPosition {
                resource_version_filter_through_event_id: 6,
                ..current
            },
        ] {
            assert!(!current.permits_successor(changed));
        }
    }

    #[test]
    fn composite_filter_enforces_rv_through_boundary_before_clearing() {
        let cursor = WatchReplayPosition::from_resource_version_through_event_id(10, 5);
        struct Case {
            name: &'static str,
            event: WatchReplayPosition,
            accepted: bool,
            expected: Option<WatchReplayPosition>,
        }
        let cases = [
            Case {
                name: "before boundary equal rv is filtered",
                event: WatchReplayPosition {
                    resource_version: 10,
                    event_id: 1,
                    resource_version_filter_through_event_id: 0,
                },
                accepted: false,
                expected: None,
            },
            Case {
                name: "before boundary lower rv is filtered",
                event: WatchReplayPosition {
                    resource_version: 9,
                    event_id: 2,
                    resource_version_filter_through_event_id: 0,
                },
                accepted: false,
                expected: None,
            },
            Case {
                name: "before boundary newer rv keeps filter",
                event: WatchReplayPosition {
                    resource_version: 11,
                    event_id: 3,
                    resource_version_filter_through_event_id: 0,
                },
                accepted: true,
                expected: Some(WatchReplayPosition {
                    resource_version: 10,
                    event_id: 3,
                    resource_version_filter_through_event_id: 5,
                }),
            },
            Case {
                name: "at boundary equal rv is filtered",
                event: WatchReplayPosition {
                    resource_version: 10,
                    event_id: 5,
                    resource_version_filter_through_event_id: 0,
                },
                accepted: false,
                expected: None,
            },
            Case {
                name: "at boundary newer rv clears filter",
                event: WatchReplayPosition {
                    resource_version: 11,
                    event_id: 5,
                    resource_version_filter_through_event_id: 0,
                },
                accepted: true,
                expected: Some(WatchReplayPosition {
                    resource_version: 11,
                    event_id: 5,
                    resource_version_filter_through_event_id: 0,
                }),
            },
            Case {
                name: "after boundary lower rv is allowed",
                event: WatchReplayPosition {
                    resource_version: 1,
                    event_id: 6,
                    resource_version_filter_through_event_id: 0,
                },
                accepted: true,
                expected: Some(WatchReplayPosition {
                    resource_version: 1,
                    event_id: 6,
                    resource_version_filter_through_event_id: 0,
                }),
            },
        ];
        for case in cases {
            let result = cursor.advance_through_event(case.event);
            assert_eq!(result.is_ok(), case.accepted, "{}: {result:?}", case.name);
            if let Some(expected) = case.expected {
                assert_eq!(result.unwrap(), expected, "{}", case.name);
            }
        }
    }
}
