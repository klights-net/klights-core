//! Persisted resourceVersion assignment-mode metadata.

use anyhow::{Result, anyhow};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::datastore::MetaStore;
use crate::log_apply::ResourceVersionAssignment;

pub const KEY_RESOURCE_VERSION_ASSIGNMENT_MODE: &str = "resource_version_assignment_mode";

/// What a Raft snapshot actually said about its RV-assignment mode.
///
/// A missing field is not equivalent to an explicit legacy mode: a V1
/// destination must reject both rather than silently downgrading itself.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SnapshotAssignmentMode {
    #[default]
    AbsentLegacySnapshot,
    Explicit(ResourceVersionAssignment),
}

impl SnapshotAssignmentMode {
    pub const fn explicit(mode: ResourceVersionAssignment) -> Self {
        Self::Explicit(mode)
    }

    pub const fn is_absent_legacy(&self) -> bool {
        matches!(self, Self::AbsentLegacySnapshot)
    }
}

impl Serialize for SnapshotAssignmentMode {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Explicit(mode) => mode.serialize(serializer),
            Self::AbsentLegacySnapshot => serializer.serialize_unit(),
        }
    }
}

impl<'de> Deserialize<'de> for SnapshotAssignmentMode {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ResourceVersionAssignment::deserialize(deserializer).map(Self::Explicit)
    }
}

/// Decide a snapshot install mode from the destination's persisted mode and
/// the source envelope observation. This is evaluated inside the same SQLite
/// transaction that replaces replicated state.
pub fn decide_snapshot_assignment_mode(
    destination: ResourceVersionAssignment,
    source: SnapshotAssignmentMode,
) -> Result<ResourceVersionAssignment> {
    match (destination, source) {
        (
            ResourceVersionAssignment::CommittedApplyV1,
            SnapshotAssignmentMode::Explicit(ResourceVersionAssignment::CommittedApplyV1),
        ) => Ok(ResourceVersionAssignment::CommittedApplyV1),
        (ResourceVersionAssignment::CommittedApplyV1, source) => Err(anyhow!(
            "snapshot cannot downgrade resourceVersion assignment mode from committed_apply_v1 to {source:?}"
        )),
        (
            ResourceVersionAssignment::LegacyLeaderAssigned,
            SnapshotAssignmentMode::Explicit(ResourceVersionAssignment::CommittedApplyV1),
        ) => Ok(ResourceVersionAssignment::CommittedApplyV1),
        (
            ResourceVersionAssignment::LegacyLeaderAssigned,
            SnapshotAssignmentMode::AbsentLegacySnapshot
            | SnapshotAssignmentMode::Explicit(ResourceVersionAssignment::LegacyLeaderAssigned),
        ) => Ok(ResourceVersionAssignment::LegacyLeaderAssigned),
    }
}

#[cfg(test)]
mod snapshot_assignment_mode_tests {
    use super::{SnapshotAssignmentMode, decide_snapshot_assignment_mode};
    use crate::log_apply::ResourceVersionAssignment;

    #[test]
    fn snapshot_assignment_mode_is_monotonic() {
        struct Case {
            destination: ResourceVersionAssignment,
            source: SnapshotAssignmentMode,
            expected: Result<ResourceVersionAssignment, &'static str>,
        }

        let cases = [
            Case {
                destination: ResourceVersionAssignment::CommittedApplyV1,
                source: SnapshotAssignmentMode::Explicit(
                    ResourceVersionAssignment::CommittedApplyV1,
                ),
                expected: Ok(ResourceVersionAssignment::CommittedApplyV1),
            },
            Case {
                destination: ResourceVersionAssignment::CommittedApplyV1,
                source: SnapshotAssignmentMode::AbsentLegacySnapshot,
                expected: Err("cannot downgrade"),
            },
            Case {
                destination: ResourceVersionAssignment::CommittedApplyV1,
                source: SnapshotAssignmentMode::Explicit(
                    ResourceVersionAssignment::LegacyLeaderAssigned,
                ),
                expected: Err("cannot downgrade"),
            },
            Case {
                destination: ResourceVersionAssignment::LegacyLeaderAssigned,
                source: SnapshotAssignmentMode::Explicit(
                    ResourceVersionAssignment::CommittedApplyV1,
                ),
                expected: Ok(ResourceVersionAssignment::CommittedApplyV1),
            },
            Case {
                destination: ResourceVersionAssignment::LegacyLeaderAssigned,
                source: SnapshotAssignmentMode::AbsentLegacySnapshot,
                expected: Ok(ResourceVersionAssignment::LegacyLeaderAssigned),
            },
            Case {
                destination: ResourceVersionAssignment::LegacyLeaderAssigned,
                source: SnapshotAssignmentMode::Explicit(
                    ResourceVersionAssignment::LegacyLeaderAssigned,
                ),
                expected: Ok(ResourceVersionAssignment::LegacyLeaderAssigned),
            },
        ];

        for case in cases {
            match case.expected {
                Ok(expected) => assert_eq!(
                    decide_snapshot_assignment_mode(case.destination, case.source).unwrap(),
                    expected
                ),
                Err(message) => assert!(
                    decide_snapshot_assignment_mode(case.destination, case.source)
                        .unwrap_err()
                        .to_string()
                        .contains(message)
                ),
            }
        }
    }
}

pub async fn read_resource_version_assignment_mode(
    store: &(impl MetaStore + ?Sized),
) -> Result<ResourceVersionAssignment> {
    let Some(value) = store
        .get_klights_meta(KEY_RESOURCE_VERSION_ASSIGNMENT_MODE)
        .await?
    else {
        return Ok(ResourceVersionAssignment::LegacyLeaderAssigned);
    };
    parse_resource_version_assignment_mode(&value)
}

pub async fn write_resource_version_assignment_mode(
    store: &(impl MetaStore + ?Sized),
    mode: ResourceVersionAssignment,
) -> Result<()> {
    store
        .set_klights_meta(
            KEY_RESOURCE_VERSION_ASSIGNMENT_MODE,
            mode.as_metadata_value(),
        )
        .await
}

pub fn parse_resource_version_assignment_mode(value: &str) -> Result<ResourceVersionAssignment> {
    match value {
        "legacy_leader_assigned" => Ok(ResourceVersionAssignment::LegacyLeaderAssigned),
        "committed_apply_v1" => Ok(ResourceVersionAssignment::CommittedApplyV1),
        _ => Err(anyhow!("invalid resourceVersion assignment mode: {value}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn absent_metadata_defaults_to_legacy_and_v1_round_trips() {
        let db = crate::datastore::test_support::in_memory().await;
        assert_eq!(
            read_resource_version_assignment_mode(&db).await.unwrap(),
            ResourceVersionAssignment::LegacyLeaderAssigned
        );
        write_resource_version_assignment_mode(&db, ResourceVersionAssignment::CommittedApplyV1)
            .await
            .unwrap();
        assert_eq!(
            read_resource_version_assignment_mode(&db).await.unwrap(),
            ResourceVersionAssignment::CommittedApplyV1
        );
    }

    #[test]
    fn invalid_metadata_mode_is_rejected() {
        assert!(parse_resource_version_assignment_mode("not-a-mode").is_err());
    }
}
