//! Transitional focused identity adapter for the broad node-local handle.
//!
//! Identity persistence is owned by `SqliteNodeIdentity`; this implementation
//! keeps existing root consumers source-compatible until Phase 11E removes the
//! broad handle.

use klights_node_store::{NodeIdentity, NodeIdentityFuture};

impl NodeIdentity for super::SqliteNodeLocalDb {
    fn backend_name(&self) -> &'static str {
        self.identity_ref().backend_name()
    }

    fn ensure_node_identity<'a>(
        &'a self,
        cluster_id: &'a str,
        node_uid: &'a str,
    ) -> NodeIdentityFuture<'a, ()> {
        self.identity_ref()
            .ensure_node_identity(cluster_id, node_uid)
    }

    fn get_node_meta<'a>(&'a self, key: &'a str) -> NodeIdentityFuture<'a, Option<String>> {
        self.identity_ref().get_node_meta(key)
    }

    fn set_node_meta<'a>(&'a self, key: &'a str, value: &'a str) -> NodeIdentityFuture<'a, ()> {
        self.identity_ref().set_node_meta(key, value)
    }
}
