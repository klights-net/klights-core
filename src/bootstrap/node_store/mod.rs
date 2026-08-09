//! Private root composition for node-local persistence capabilities.
//!
//! The concrete composition is intentionally unavailable to feature crates,
//! so it cannot become cluster CRUD authority or expose its implementations
//! through downcasting.
//!
//! ```compile_fail
//! use klights::bootstrap::node_store::NodeLocalStores;
//!
//! fn cluster_crud(stores: &NodeLocalStores) {
//!     stores.get_resource("v1", "Pod", "default", "example");
//! }
//! ```
//!
//! ```compile_fail
//! use klights::bootstrap::node_store::NodeLocalStores;
//!
//! fn downcast(stores: &NodeLocalStores) {
//!     let _ = stores.as_any().downcast_ref::<rusqlite::Connection>();
//! }
//! ```

mod stores;

#[cfg(test)]
mod selector_tests;

mod selector;

pub(crate) use selector::open_node_local;
pub(crate) use stores::NodeLocalStores;
