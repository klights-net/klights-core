use super::*;

use klights_cluster_datastore::sqlite::mutation_helpers;
mod namespace_mutation;
mod namespace_read;
mod node_subnet;
mod ownership;
// TO-BE-CLEANUP: legacy replicated StorageCommand apply test support.
#[cfg(test)]
pub(in crate::datastore::sqlite) mod replicated_create;
pub(in crate::datastore::sqlite) mod resource_create;
pub(in crate::datastore::sqlite) mod resource_delete;
pub(in crate::datastore::sqlite) mod resource_read;
mod resource_status;
pub(in crate::datastore::sqlite) mod resource_update;
pub(in crate::datastore::sqlite) mod snapshot;
