use anyhow::Result;

use crate::datastore::DatastoreBackend;
use crate::kubelet::node_registration::{NodeRegistrationSnapshot, NodeRegistrationStore};

#[async_trait::async_trait]
impl NodeRegistrationStore for DatastoreNodeRegistrationStore<'_> {
    async fn get_node(&self, node_name: &str) -> Result<Option<klights_cluster_core::Resource>> {
        self.db.get_resource("v1", "Node", None, node_name).await
    }

    async fn stamp_routing_metadata(
        &self,
        node_name: &str,
        node: &mut serde_json::Value,
    ) -> Result<bool> {
        crate::node_routing_metadata::stamp_from_store(self.db, node_name, node).await
    }

    async fn update_node(
        &self,
        node_name: &str,
        node: serde_json::Value,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> Result<()> {
        self.db
            .update_resource_with_preconditions("v1", "Node", None, node_name, node, preconditions)
            .await
            .map(|_| ())
    }

    async fn create_node(&self, node_name: &str, node: serde_json::Value) -> Result<()> {
        self.db
            .create_resource("v1", "Node", None, node_name, node)
            .await
            .map(|_| ())
    }
}

struct DatastoreNodeRegistrationStore<'a> {
    db: &'a dyn DatastoreBackend,
}

pub(crate) async fn register_node_snapshot(
    db: &dyn DatastoreBackend,
    outbox: Option<&crate::node_outbox::Outbox>,
    dataplane_health: Option<&klights_network_api::DataplaneHealthSnapshot>,
    snapshot: &NodeRegistrationSnapshot,
) -> Result<()> {
    let store = DatastoreNodeRegistrationStore { db };
    crate::kubelet::node_registration::register_node_snapshot(
        &store,
        outbox,
        dataplane_health,
        snapshot,
    )
    .await
}
