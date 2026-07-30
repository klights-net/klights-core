pub mod backend;
mod delivery_composition;
pub mod handle;
mod identity_adapter;
pub(crate) mod network_adapter;
pub(crate) mod raft_adapter;
pub mod redb;
mod runtime_work_composition;
pub mod selector;
pub mod sqlite;
#[cfg(test)]
mod test_delivery_compat;
#[cfg(test)]
pub mod test_runtime_work_compat;

pub use backend::NodeLocalBackend;
pub use handle::NodeLocalHandle;
#[cfg(test)]
pub use test_delivery_compat::LegacyDeliveryTestStore;
#[cfg(test)]
pub type KubeletTestStoreHandle = NodeLocalHandle;
pub use sqlite::SqliteNodeLocalDb;
#[cfg(test)]
pub use test_runtime_work_compat::{
    DeadLetterRow, DeadLetterTestInsert, OutboxFailureDisposition, OutboxInsert, OutboxRow,
    OutboxStats, PodRuntimeOwnershipError, PodSlotAdmissionEvent, PodSlotAdmissionResult,
    PodSlotAdmissionState, PodSlotClearResult, PodSlotMutationResult, PodStatusCheckpoint,
    PodWorkqueueEntry, PodWorkqueueKind, ReplicationCheckpoint, SandboxRef,
};

#[cfg(test)]
pub type NodeLocalDb = SqliteNodeLocalDb;

pub use klights_node_datastore::schema;

#[cfg(test)]
mod tests;
