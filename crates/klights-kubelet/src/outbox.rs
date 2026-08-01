//! Focused node-owned durable delivery port.
//!
//! Kubelet authors transport-neutral storage commands. The composition-owned
//! node outbox adapter is responsible for persistence, encoding, and delivery.

pub use klights_cluster_core::OutboxOperation;
pub use klights_leader_api::{
    NodeOutboxCommand as OutboxCommand, NodeOutboxRoute as OutboxSendRoute,
    NodeOutboxSubject as OutboxSubject,
};

pub type Outbox = dyn klights_leader_api::NodeOutbox;

pub struct OutboxSendPlanner<'a> {
    outbox: Option<&'a Outbox>,
}

impl<'a> OutboxSendPlanner<'a> {
    pub const fn new(outbox: Option<&'a Outbox>) -> Self {
        Self { outbox }
    }

    pub async fn route(&self, command: OutboxCommand) -> anyhow::Result<OutboxSendRoute> {
        klights_leader_api::route_node_outbox(self.outbox, command)
            .await
            .map_err(anyhow::Error::new)
    }
}
