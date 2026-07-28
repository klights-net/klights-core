//! Typed retained-history boundaries for durable watch replay.
//!
//! A resource-version-only boundary is deliberately distinct from an exact
//! event cursor. Legacy databases cannot prove that an arbitrary positioned
//! cursor is reconstructable, so that combination fails closed.

use klights_cluster_core::WatchReplayPosition;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayRetentionBoundary {
    Exact(WatchReplayPosition),
    LegacyRvOnly { resource_version: i64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayAvailability {
    Available,
    Expired,
}

impl ReplayRetentionBoundary {
    /// Classify one replay cursor against this actual retained-history
    /// boundary. A positioned cursor cannot be validated against a legacy
    /// resource-version-only boundary and must relist instead.
    pub const fn classify(self, cursor: WatchReplayPosition) -> ReplayAvailability {
        match self {
            Self::Exact(floor) => {
                // Three cursor shapes reach replay classification:
                //
                //  * positioned (`event_id > 0`): the cursor is a durable
                //    event position. It expires when that position precedes
                //    the retained floor.
                //  * resource-version-filtered-through (`event_id == 0` and
                //    `resource_version_filter_through_event_id > 0`): the
                //    client resumes from an RV and filters rows up to a later
                //    event id. Both the RV anchor and the event window must
                //    stay at or above the floor; otherwise a pruned row would
                //    drop out of the filtered prefix.
                //  * scalar resource-version only: expires when the RV precedes
                //    the floor, following Kubernetes compatibility rules.
                if cursor.event_id > 0 {
                    if cursor.event_id < floor.event_id {
                        ReplayAvailability::Expired
                    } else {
                        ReplayAvailability::Available
                    }
                } else if cursor.resource_version_filter_through_event_id > 0 {
                    if cursor.resource_version < floor.resource_version
                        || cursor.resource_version_filter_through_event_id < floor.event_id
                    {
                        ReplayAvailability::Expired
                    } else {
                        ReplayAvailability::Available
                    }
                } else if cursor.resource_version < floor.resource_version {
                    ReplayAvailability::Expired
                } else {
                    ReplayAvailability::Available
                }
            }
            Self::LegacyRvOnly { resource_version } => {
                if cursor.event_id > 0
                    || cursor.resource_version_filter_through_event_id > 0
                    || cursor.resource_version < resource_version
                {
                    ReplayAvailability::Expired
                } else {
                    ReplayAvailability::Available
                }
            }
        }
    }

    /// Intersect actual scope boundaries without inventing a synthetic pair
    /// from separately maximized resource-version and event-ID columns.
    pub fn classify_all(
        boundaries: impl IntoIterator<Item = Self>,
        cursor: WatchReplayPosition,
    ) -> ReplayAvailability {
        if boundaries
            .into_iter()
            .any(|boundary| boundary.classify(cursor) == ReplayAvailability::Expired)
        {
            ReplayAvailability::Expired
        } else {
            ReplayAvailability::Available
        }
    }

    /// Retain the two real exact boundaries needed for scalar and positioned
    /// compatibility. They can differ when a legacy lower-RV event is applied
    /// after a higher-RV event; keeping both avoids manufacturing a pair that
    /// never existed while keeping retention state bounded.
    pub fn retain_exact(boundaries: &mut Vec<Self>, candidate: WatchReplayPosition) {
        let mut exact = boundaries
            .iter()
            .copied()
            .filter_map(|boundary| match boundary {
                Self::Exact(position) => Some(position),
                Self::LegacyRvOnly { .. } => None,
            })
            .collect::<Vec<_>>();
        exact.push(candidate);
        let highest_resource_version = *exact
            .iter()
            .max_by_key(|position| position.resource_version)
            .expect("candidate keeps exact boundaries non-empty");
        let highest_event_id = *exact
            .iter()
            .max_by_key(|position| position.event_id)
            .expect("candidate keeps exact boundaries non-empty");
        boundaries.retain(|boundary| matches!(boundary, Self::LegacyRvOnly { .. }));
        boundaries.push(Self::Exact(highest_resource_version));
        if highest_event_id != highest_resource_version {
            boundaries.push(Self::Exact(highest_event_id));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_and_legacy_retention_boundaries_classify_positions() {
        let exact = ReplayRetentionBoundary::Exact(WatchReplayPosition {
            resource_version: 10,
            event_id: 40,
            resource_version_filter_through_event_id: 0,
        });
        let legacy = ReplayRetentionBoundary::LegacyRvOnly {
            resource_version: 10,
        };

        let cases = [
            (
                exact,
                WatchReplayPosition {
                    resource_version: 10,
                    event_id: 39,
                    resource_version_filter_through_event_id: 0,
                },
                ReplayAvailability::Expired,
            ),
            (
                exact,
                WatchReplayPosition {
                    resource_version: 10,
                    event_id: 40,
                    resource_version_filter_through_event_id: 0,
                },
                ReplayAvailability::Available,
            ),
            (
                exact,
                WatchReplayPosition::from_resource_version(10),
                ReplayAvailability::Available,
            ),
            (
                legacy,
                WatchReplayPosition::from_resource_version(9),
                ReplayAvailability::Expired,
            ),
            (
                legacy,
                WatchReplayPosition::from_resource_version(10),
                ReplayAvailability::Available,
            ),
            (
                legacy,
                WatchReplayPosition {
                    resource_version: 10,
                    event_id: 40,
                    resource_version_filter_through_event_id: 0,
                },
                ReplayAvailability::Expired,
            ),
            (
                exact,
                WatchReplayPosition::from_resource_version_through_event_id(9, 100),
                ReplayAvailability::Expired,
            ),
            (
                exact,
                WatchReplayPosition::from_resource_version_through_event_id(10, 39),
                ReplayAvailability::Expired,
            ),
            (
                exact,
                WatchReplayPosition::from_resource_version_through_event_id(10, 40),
                ReplayAvailability::Available,
            ),
        ];

        for (boundary, cursor, expected) in cases {
            assert_eq!(boundary.classify(cursor), expected);
        }

        let newer_rv = ReplayRetentionBoundary::Exact(WatchReplayPosition {
            resource_version: 20,
            event_id: 5,
            resource_version_filter_through_event_id: 0,
        });
        let newer_event = ReplayRetentionBoundary::Exact(WatchReplayPosition {
            resource_version: 10,
            event_id: 40,
            resource_version_filter_through_event_id: 0,
        });
        assert_eq!(
            ReplayRetentionBoundary::classify_all(
                [newer_rv, newer_event],
                WatchReplayPosition {
                    resource_version: 10,
                    event_id: 10,
                    resource_version_filter_through_event_id: 0,
                },
            ),
            ReplayAvailability::Expired,
            "scope composition must keep real boundaries instead of pairing max RV with max event ID"
        );
    }
}
