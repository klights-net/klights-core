//! Small root-private helpers shared by bootstrap composition tests.

use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OutboxPayload {
    pub(crate) command: klights_cluster_core::StorageCommand,
}

impl OutboxPayload {
    pub(crate) fn from_command(command: klights_cluster_core::StorageCommand) -> Self {
        Self { command }
    }

    pub(crate) fn encode_protobuf(&self) -> anyhow::Result<Vec<u8>> {
        Ok(
            klights_leader_rpc::storage_wire_codec::encode_outbox_payload_protobuf(
                &klights_cluster_core::OutboxPayload::new(self.command.clone()),
            )?,
        )
    }

    pub(crate) fn decode_protobuf(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            command: klights_leader_rpc::storage_wire_codec::decode_outbox_payload_protobuf(bytes)?
                .into_command(),
        })
    }
}

pub(crate) fn outbox_from_node_db(
    node_db: impl Into<Arc<crate::bootstrap::node_store::NodeLocalStores>>,
) -> klights_kubelet::node_outbox::Outbox {
    let node_db = node_db.into();
    let stores = klights_kubelet::node_outbox::OutboxStores::new(
        node_db.outbox_producer(),
        node_db.outbox_dispatcher(),
        node_db.pod_status_checkpoints(),
        node_db.runtime_observation_checkpoints(),
        node_db.outbox_status_stamps(),
    );
    klights_kubelet::node_outbox::Outbox::compose(
        stores,
        crate::bootstrap::composition_adapters::outbox_payload_codec_adapter::new_codec(),
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(klights_supervisor::SystemWallClock),
    )
}
