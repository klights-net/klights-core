//! Root-owned real-adapter helpers for outbox integration tests.

use std::sync::Arc;

use anyhow::Result;
use klights_kubelet::node_outbox::{Outbox, OutboxDispatcher, OutboxStores};
use klights_leader_api::LeaderOutboxDelivery;
use tokio::sync::Notify;

use crate::datastore::node_local::NodeLocalStores;

pub(crate) trait NodeLocalStoresRef {
    fn node_local_stores(&self) -> &NodeLocalStores;
}

impl NodeLocalStoresRef for NodeLocalStores {
    fn node_local_stores(&self) -> &NodeLocalStores {
        self
    }
}

impl NodeLocalStoresRef for Arc<NodeLocalStores> {
    fn node_local_stores(&self) -> &NodeLocalStores {
        self.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutboxPayload {
    pub command: klights_cluster_core::StorageCommand,
}

impl OutboxPayload {
    pub fn from_command(command: klights_cluster_core::StorageCommand) -> Self {
        Self { command }
    }

    pub fn encode_protobuf(&self) -> Result<Vec<u8>> {
        Ok(
            klights_leader_rpc::storage_wire_codec::encode_outbox_payload_protobuf(
                &klights_cluster_core::OutboxPayload::new(self.command.clone()),
            )?,
        )
    }

    pub fn decode_protobuf(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            command: klights_leader_rpc::storage_wire_codec::decode_outbox_payload_protobuf(bytes)?
                .into_command(),
        })
    }
}

pub fn outbox_stores(node_db: &NodeLocalStores) -> OutboxStores {
    OutboxStores::new(
        node_db.outbox_producer(),
        node_db.outbox_dispatcher(),
        node_db.pod_status_checkpoints(),
        node_db.runtime_observation_checkpoints(),
        node_db.outbox_status_stamps(),
    )
}

pub fn outbox_from_node_db(node_db: impl NodeLocalStoresRef) -> Outbox {
    outbox_with_notify(node_db, Arc::new(Notify::new()))
}

pub fn outbox_with_notify(node_db: impl NodeLocalStoresRef, notify: Arc<Notify>) -> Outbox {
    let node_db = node_db.node_local_stores();
    Outbox::compose(
        outbox_stores(node_db),
        crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
        notify,
        Arc::new(klights_supervisor::SystemWallClock),
    )
}

pub fn dispatcher_for_tests(
    node_db: impl NodeLocalStoresRef,
    client: Arc<dyn LeaderOutboxDelivery>,
) -> OutboxDispatcher {
    dispatcher_with_notify(node_db, client, Arc::new(Notify::new()))
}

pub fn dispatcher_with_notify(
    node_db: impl NodeLocalStoresRef,
    client: Arc<dyn LeaderOutboxDelivery>,
    notify: Arc<Notify>,
) -> OutboxDispatcher {
    let node_db = node_db.node_local_stores();
    OutboxDispatcher::new(
        outbox_stores(node_db),
        crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
        client,
        notify,
        Arc::new(klights_supervisor::SystemWallClock),
    )
}

pub fn dispatcher_with_rtt_estimator(
    node_db: impl NodeLocalStoresRef,
    client: Arc<dyn LeaderOutboxDelivery>,
    rtt: Arc<klights_types::RttEstimator>,
) -> OutboxDispatcher {
    let node_db = node_db.node_local_stores();
    OutboxDispatcher::compose_with_rtt_estimator_for_test(
        outbox_stores(node_db),
        crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
        client,
        Arc::new(Notify::new()),
        rtt,
        Arc::new(klights_supervisor::SystemWallClock),
    )
}
