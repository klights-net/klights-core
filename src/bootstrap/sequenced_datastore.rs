//! Root compatibility shell for legacy broad datastore consumers.
//!
//! Mutations delegate to existing focused leader capabilities. Passive cluster
//! persistence remains private to committed Raft apply, snapshot restore, and
//! focused read adapters.

use std::sync::Arc;

use anyhow::Result;
use klights_cluster_core::StorageCommand;
use klights_leader_api::{LeaderResourceCommand, ResourceCommandRequest, ResourceCommandResult};
use klights_replication::proposal::RaftProposal;

use crate::datastore::DatastoreBackend;

mod backend_impl;

pub(crate) struct SequencedDatastore {
    passive: Arc<dyn DatastoreBackend>,
    resource_command: Arc<klights_replication::leader_api::EmbeddedLeaderResourceCommand>,
    network: Arc<crate::control_plane::client::local::ClusterStoreLeaderNetwork>,
    pod_cleanup: Arc<crate::control_plane::client::local::ClusterStoreLeaderPodCleanup>,
    maintenance: Arc<crate::control_plane::client::local::ClusterStoreLeaderMaintenance>,
    outbox_delivery: Arc<klights_replication::leader_api::EmbeddedOutboxDelivery>,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

impl SequencedDatastore {
    pub(crate) fn new_with_clock(
        passive: Arc<dyn DatastoreBackend>,
        proposal: Arc<dyn RaftProposal>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
        is_leader_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        let resource_query = Arc::new(
            crate::control_plane::client::local::ClusterStoreLeaderResourceQuery::new(
                passive.clone(),
                is_leader_rx.clone(),
            ),
        );
        let resource_command = Arc::new(
            klights_replication::leader_api::EmbeddedLeaderResourceCommand::new(
                proposal.clone(),
                resource_query,
                is_leader_rx.clone(),
            ),
        );
        let network = Arc::new(
            crate::control_plane::client::local::ClusterStoreLeaderNetwork::new(
                passive.clone(),
                proposal.clone(),
                is_leader_rx.clone(),
            ),
        );
        let pod_cleanup = Arc::new(
            crate::control_plane::client::local::ClusterStoreLeaderPodCleanup::new(
                passive.clone(),
                proposal.clone(),
                is_leader_rx.clone(),
            ),
        );
        let maintenance = Arc::new(
            crate::control_plane::client::local::ClusterStoreLeaderMaintenance::new(
                passive.clone(),
                proposal.clone(),
                is_leader_rx.clone(),
            ),
        );
        let outbox_delivery = Arc::new(
            klights_replication::leader_api::EmbeddedOutboxDelivery::new(
                proposal,
                Arc::new(
                    crate::control_plane::client::local::ClusterStoreLeaderResourceQuery::new(
                        passive.clone(),
                        is_leader_rx.clone(),
                    ),
                ),
                is_leader_rx,
            ),
        );
        Self {
            passive,
            resource_command,
            network,
            pod_cleanup,
            maintenance,
            outbox_delivery,
            wall_clock,
        }
    }

    async fn submit_resource_command(
        &self,
        command: StorageCommand,
    ) -> Result<ResourceCommandResult> {
        let request = ResourceCommandRequest::try_new(command).map_err(anyhow::Error::new)?;
        self.resource_command
            .submit_resource_command(request)
            .await
            .map_err(anyhow::Error::new)
    }
}
