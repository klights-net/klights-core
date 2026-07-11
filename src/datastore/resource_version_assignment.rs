//! Persisted resourceVersion assignment-mode metadata.

use anyhow::{Result, anyhow};

use crate::datastore::DatastoreBackend;
use crate::log_apply::ResourceVersionAssignment;

pub const KEY_RESOURCE_VERSION_ASSIGNMENT_MODE: &str = "resource_version_assignment_mode";

pub async fn read_resource_version_assignment_mode(
    store: &(impl DatastoreBackend + ?Sized),
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
    store: &(impl DatastoreBackend + ?Sized),
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
