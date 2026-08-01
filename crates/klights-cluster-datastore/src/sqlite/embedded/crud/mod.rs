use super::*;

use crate::sqlite::mutation_helpers;
mod namespace_mutation;
mod namespace_read;
mod node_subnet;
mod ownership;
// TO-BE-CLEANUP: legacy replicated StorageCommand apply test support.
#[cfg(any(test, feature = "test-support"))]
pub(in crate::sqlite::embedded) mod replicated_create;
pub(in crate::sqlite::embedded) mod resource_create;
pub(in crate::sqlite::embedded) mod resource_delete;
pub(in crate::sqlite::embedded) mod resource_read;
mod resource_status;
pub(in crate::sqlite::embedded) mod resource_update;
pub(in crate::sqlite::embedded) mod snapshot;
