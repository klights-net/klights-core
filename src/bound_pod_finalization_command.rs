//! Root-private authoring capability for actor-owned bound Pod finalization.
//!
//! The wire enum stays public for replication decoding, but production
//! subsystems construct this authority-bearing command only through this
//! composition-root capability.

use crate::datastore::command::StorageCommand;

pub(crate) fn author(
    namespace: String,
    name: String,
    pod_uid: String,
    node_name: String,
    observed_resource_version: i64,
) -> StorageCommand {
    StorageCommand::FinalizeBoundPod {
        namespace,
        name,
        pod_uid,
        node_name,
        observed_resource_version,
    }
}
