pub mod backend;
pub(crate) mod delivery_adapter;
pub mod handle;
mod identity_adapter;
pub(crate) mod network_adapter;
pub(crate) mod raft_adapter;
pub mod redb;
pub mod selector;
pub mod sqlite;
pub mod types;

pub use backend::NodeLocalBackend;
pub use handle::NodeLocalHandle;
#[cfg(test)]
pub type KubeletTestStoreHandle = NodeLocalHandle;
pub use sqlite::SqliteNodeLocalDb;
#[cfg(test)]
pub use types::DeadLetterTestInsert;
pub use types::{
    DeadLetterRow, OutboxFailureDisposition, OutboxInsert, OutboxRow, OutboxStats,
    OwnedPodNetworkAllocationRequest, PodEndpointEvent, PodEndpointMode, PodEndpointRow,
    PodNetworkAllocationLink, PodNetworkAllocationPod, PodNetworkAllocationRequest,
    PodNetworkAllocationSubnet, PodNetworkAssignmentRow, PodNetworkEndpoint,
    PodNetworkReservationError, PodRuntimeOwnershipError, PodRuntimeRow, PodSlotAdmissionEvent,
    PodSlotAdmissionResult, PodSlotAdmissionState, PodSlotClearResult, PodSlotMutationResult,
    PodStatusCheckpoint, PodWorkqueueEntry, PodWorkqueueKind, ProbeStateRow, ReplicationCheckpoint,
    SandboxRef,
};

#[cfg(test)]
pub type NodeLocalDb = SqliteNodeLocalDb;

pub use sqlite::schema;

#[cfg(test)]
mod tests;
