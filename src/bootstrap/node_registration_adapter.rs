use anyhow::Result;

use crate::bootstrap::{NodeMode, NodeRole};
use klights_kubelet::{
    node_config::KubeletNodeRole,
    node_registration::{NodeRegistrationSnapshot, NodeRegistrationStore},
    node_registration_profile::{NodeRegistrationProcessInputs, build_profile},
};

/// Map validated root process inputs to the kubelet-owned registration policy.
pub(crate) fn build_profile_from_process_inputs(
    node_mode: &NodeMode,
    node_role: &NodeRole,
) -> klights_kubelet::node_config::NodeRegistrationProfile {
    let peer_mode = match node_mode {
        NodeMode::Root => klights_network_api::NodePeerMode::Root,
        NodeMode::Rootless { .. } => klights_network_api::NodePeerMode::Rootless,
    };
    let (role, joins_existing_cluster) = match node_role {
        NodeRole::Leader {
            bootstrap:
                crate::bootstrap::node_role::LeaderBootstrap::Seed
                | crate::bootstrap::node_role::LeaderBootstrap::Bootstrap { .. },
        } => (KubeletNodeRole::Leader, false),
        NodeRole::Leader {
            bootstrap: crate::bootstrap::node_role::LeaderBootstrap::Join { .. },
        } => (KubeletNodeRole::Leader, true),
        NodeRole::Controlplane {
            leader_endpoints,
            as_learner,
            ..
        } => (
            KubeletNodeRole::Controlplane {
                as_learner: *as_learner,
            },
            !leader_endpoints.is_empty(),
        ),
        NodeRole::Worker { .. } => (KubeletNodeRole::Worker, true),
    };

    build_profile(NodeRegistrationProcessInputs::new(
        peer_mode,
        role,
        joins_existing_cluster,
        crate::version::build_identity(),
    ))
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
