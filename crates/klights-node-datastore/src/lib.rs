//! Passive node-local datastore adapters for klights.
//!
//! Backend selection and root composition remain outside this package. Raft
//! values are persisted as opaque bytes behind neutral node-store contracts.

pub mod delivery;
mod delivery_queries;
mod identity;
pub mod network_state;
pub mod open;
mod raft_durability;
mod runtime_work;
pub mod schema;

#[cfg(feature = "test-support")]
pub mod test_support;

pub use identity::SqliteNodeIdentity;
pub use network_state::SqliteNodeNetworkStateStore;
pub use raft_durability::SqliteRaftDurability;
pub use runtime_work::SqliteRuntimeWorkStore;
