use anyhow::Result;

use klights_cluster_datastore::redb::tables;

const LEGACY_WILDCARD: (&str, &str, &str) = ("*", "*", "*");

/// Legacy fail-closed replay boundary restored for scopes whose retained
/// history cannot be proven. All Redb replay consumers merge this object with
/// their scoped floor so replay and membership reconstruction cannot diverge.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct LegacyReplayFloor {
    resource_version: i64,
    event_id: i64,
}

impl LegacyReplayFloor {
    pub(super) fn read(read: &::redb::ReadTransaction) -> Result<Option<Self>> {
        let key = floor_key(LEGACY_WILDCARD.0, LEGACY_WILDCARD.1, LEGACY_WILDCARD.2);
        let rv_floors = read.open_table(tables::WATCH_REPLAY_FLOORS)?;
        let position_floors = read.open_table(tables::WATCH_REPLAY_POSITION_FLOORS)?;
        let resource_version = rv_floors
            .get(key.as_slice())?
            .map(|value| value.value() as i64);
        let event_id = position_floors
            .get(key.as_slice())?
            .and_then(|value| decode_position_floor(value.value()));
        Ok(resource_version
            .zip(event_id)
            .map(|(resource_version, event_id)| Self {
                resource_version,
                event_id,
            }))
    }

    pub(super) fn merge_resource_version(self, scoped: Option<i64>) -> Option<i64> {
        Some(scoped.map_or(self.resource_version, |value| {
            value.max(self.resource_version)
        }))
    }

    pub(super) fn merge_event_id(self, scoped: Option<i64>) -> Option<i64> {
        Some(scoped.map_or(self.event_id, |value| value.max(self.event_id)))
    }
}

fn decode_position_floor(encoded: &[u8]) -> Option<i64> {
    (encoded.len() == 16).then(|| {
        i64::try_from(u64::from_be_bytes(
            encoded[8..].try_into().expect("fixed replay-floor slice"),
        ))
        .unwrap_or(i64::MAX)
    })
}

fn floor_key(api_version: &str, kind: &str, namespace: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(api_version.len() + kind.len() + namespace.len() + 2);
    key.extend_from_slice(api_version.as_bytes());
    key.push(0);
    key.extend_from_slice(kind.as_bytes());
    key.push(0);
    key.extend_from_slice(namespace.as_bytes());
    key
}
