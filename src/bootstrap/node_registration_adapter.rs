use anyhow::Result;

#[cfg(test)]
use crate::datastore::DatastoreBackend;
use klights_kubelet::node_registration::{NodeRegistrationSnapshot, NodeRegistrationStore};

#[cfg(test)]
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
        let mut changed = false;
        if let Some(subnet) = self.db.get_node_subnet(node_name).await? {
            changed |= klights_cluster_core::set_node_pod_cidr(node, &subnet.subnet().to_string());
        }
        if let Some(metadata) = self.db.get_node_dataplane(node_name).await? {
            changed |= klights_types::set_node_dataplane_annotations(
                node,
                &metadata.endpoint.to_string(),
                metadata.mode.as_str(),
                metadata.encryption.as_str(),
                metadata.public_key.as_ref().map(|key| key.as_str()),
                metadata.port,
            );
        }
        Ok(changed)
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

#[cfg(test)]
struct DatastoreNodeRegistrationStore<'a> {
    db: &'a dyn DatastoreBackend,
}

struct WorkerNodeRegistrationStore<'a> {
    store: &'a klights_kubelet::worker_store::WorkerStoreAdapter,
}

#[async_trait::async_trait]
impl NodeRegistrationStore for WorkerNodeRegistrationStore<'_> {
    async fn get_node(&self, node_name: &str) -> Result<Option<klights_cluster_core::Resource>> {
        self.store.get_resource("v1", "Node", None, node_name).await
    }

    async fn stamp_routing_metadata(
        &self,
        node_name: &str,
        node: &mut serde_json::Value,
    ) -> Result<bool> {
        crate::bootstrap::composition_adapters::node_routing_metadata::stamp_from_worker_store(
            self.store, node_name, node,
        )
        .await
    }

    async fn update_node(
        &self,
        _node_name: &str,
        _node: serde_json::Value,
        _preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> Result<()> {
        anyhow::bail!("worker Node registration updates must use the outbox")
    }

    async fn create_node(&self, _node_name: &str, _node: serde_json::Value) -> Result<()> {
        anyhow::bail!("worker Node registration creates must use the outbox")
    }
}

#[cfg(test)]
pub(crate) async fn register_node_snapshot(
    db: &dyn DatastoreBackend,
    outbox: Option<&klights_kubelet::node_outbox::Outbox>,
    dataplane_health: Option<&klights_network_api::DataplaneHealthSnapshot>,
    snapshot: &NodeRegistrationSnapshot,
) -> Result<()> {
    let store = DatastoreNodeRegistrationStore { db };
    klights_kubelet::node_registration::register_node_snapshot(
        &store,
        outbox.map(|outbox| outbox as &dyn klights_leader_api::NodeOutbox),
        dataplane_health,
        snapshot,
        klights_supervisor::SystemWallClock::now_utc(),
    )
    .await
}

pub(crate) async fn register_leader_node_snapshot(
    store: &crate::bootstrap::composition_adapters::leader_bootstrap_store_adapter::LeaderBootstrapStore,
    outbox: Option<&klights_kubelet::node_outbox::Outbox>,
    dataplane_health: Option<&klights_network_api::DataplaneHealthSnapshot>,
    snapshot: &NodeRegistrationSnapshot,
) -> Result<()> {
    klights_kubelet::node_registration::register_node_snapshot(
        store,
        outbox.map(|outbox| outbox as &dyn klights_leader_api::NodeOutbox),
        dataplane_health,
        snapshot,
        klights_supervisor::SystemWallClock::now_utc(),
    )
    .await
}

pub(crate) async fn register_worker_node_snapshot(
    store: &klights_kubelet::worker_store::WorkerStoreAdapter,
    outbox: &klights_kubelet::node_outbox::Outbox,
    dataplane_health: Option<&klights_network_api::DataplaneHealthSnapshot>,
    snapshot: &NodeRegistrationSnapshot,
) -> Result<()> {
    klights_kubelet::node_registration::register_node_snapshot(
        &WorkerNodeRegistrationStore { store },
        Some(outbox as &dyn klights_leader_api::NodeOutbox),
        dataplane_health,
        snapshot,
        klights_supervisor::SystemWallClock::now_utc(),
    )
    .await
}
