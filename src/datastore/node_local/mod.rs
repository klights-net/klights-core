pub mod backend;
pub mod handle;
pub mod redb;
pub mod selector;
pub mod sqlite;
pub mod types;

pub use backend::NodeLocalBackend;
pub use handle::NodeLocalHandle;
pub use sqlite::SqliteNodeLocalDb;
#[cfg(test)]
pub use types::DeadLetterTestInsert;
pub use types::{
    DeadLetterRow, OutboxInsert, OutboxRow, OutboxStats, PodRuntimeRow, PodStatusCheckpoint,
    ProbeStateRow, ReplicationCheckpoint,
};

#[cfg(test)]
pub type NodeLocalDb = SqliteNodeLocalDb;

pub use sqlite::schema;

#[cfg(test)]
mod tests;
