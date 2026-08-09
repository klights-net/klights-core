#[cfg(test)]
mod delivery_composition;
pub mod redb;
#[cfg(test)]
mod runtime_work_composition;
pub mod selector;
#[cfg(test)]
mod sqlite;
#[cfg(test)]
mod test_delivery_compat;
#[cfg(any(test, feature = "pod-repository-test-support"))]
mod test_network_compat;
#[cfg(any(test, feature = "pod-repository-test-support"))]
mod test_runtime_work_compat;

#[cfg(test)]
pub(crate) use test_delivery_compat::LegacyDeliveryTestStore;
#[cfg(test)]
pub(crate) use test_runtime_work_compat::{
    DeadLetterRow, DeadLetterTestInsert, OutboxInsert, OutboxRow,
};
#[cfg(test)]
pub(crate) use test_runtime_work_compat::{PodWorkqueueEntry, PodWorkqueueKind};

pub use klights_node_datastore::schema;

#[cfg(test)]
mod tests;
