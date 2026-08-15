//! Small root-private helpers shared by bootstrap composition tests.

use std::sync::Arc;

pub(crate) fn outbox_from_node_db(
    node_db: impl Into<Arc<crate::bootstrap::composition::node_store::NodeLocalStores>>,
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
