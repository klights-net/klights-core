use std::sync::Arc;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::DatastoreHandle;
use klights_controllers::node_subnet::{
    NodeReadinessPublishFuture, NodeReadinessPublishResult, NodeReadinessPublisher,
    PeerDataplaneHealth, PeerSyncOutcome, PeerTopologyProjection, PeerTopologyProjectionFuture,
};

pub struct DatastorePeerTopologyProjection {
    db: DatastoreHandle,
    my_node_name: String,
    cluster_cidr: String,
    authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
}

impl DatastorePeerTopologyProjection {
    pub fn new(
        db: DatastoreHandle,
        my_node_name: String,
        cluster_cidr: String,
        authority: Option<Arc<dyn klights_leader_api::LeaderAuthority>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            my_node_name,
            cluster_cidr,
            authority,
        })
    }
}

impl PeerTopologyProjection for DatastorePeerTopologyProjection {
    fn reconcile_node_event<'a>(
        &'a self,
        event: &'a klights_leader_api::ResourceEvent,
    ) -> PeerTopologyProjectionFuture<'a> {
        Box::pin(async move {
            let peer_name = event.resource().name.as_str();
            if peer_name == self.my_node_name
                || self.authority.as_ref().is_some_and(|authority| {
                    let klights_leader_api::AuthorityRoute::Local(permit) = authority.route()
                    else {
                        return true;
                    };
                    authority.validate(&permit).is_err()
                })
            {
                return Ok(());
            }

            match event.event_type() {
                klights_leader_api::WatchEventType::Deleted => {
                    self.db
                        .delete_node_subnet(peer_name)
                        .await
                        .map_err(map_controller_store_error)?;
                }
                klights_leader_api::WatchEventType::Added
                | klights_leader_api::WatchEventType::Modified => {
                    let node = event.resource().data.as_ref();
                    if node
                        .pointer("/metadata/deletionTimestamp")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|timestamp| !timestamp.is_empty())
                    {
                        self.db
                            .delete_node_subnet(peer_name)
                            .await
                            .map_err(map_controller_store_error)?;
                        return Ok(());
                    }
                    if let Some(node_ip) = klights_controllers::node_subnet::node_dataplane_ip(node)
                    {
                        self.db
                            .allocate_node_subnet(peer_name, &self.cluster_cidr, &node_ip)
                            .await
                            .map_err(map_controller_store_error)?;
                        let (mode, hostport_range) =
                            klights_controllers::node_subnet::project_node_peer_attributes(node);
                        self.db
                            .update_node_peer_attributes(peer_name, mode, hostport_range)
                            .await
                            .map_err(map_controller_store_error)?;
                    }
                }
                klights_leader_api::WatchEventType::Bookmark
                | klights_leader_api::WatchEventType::Error => {}
            }
            Ok(())
        })
    }
}

pub struct DataplaneHealthAdapter {
    inner: klights_networking::dataplane_health::DataplaneHealth,
}

impl DataplaneHealthAdapter {
    pub fn new(inner: klights_networking::dataplane_health::DataplaneHealth) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

impl PeerDataplaneHealth for DataplaneHealthAdapter {
    fn apply_peer_sync_outcome(
        &self,
        outcome: &PeerSyncOutcome,
    ) -> klights_network_api::DataplaneHealthSnapshot {
        if outcome.unreachable_ready_peers == 0 {
            self.inner.set_peers_connected();
        } else {
            self.inner.set_peers_disconnected(format!(
                "Waiting for WireGuard dataplane connectivity to {} of {} ready peer(s)",
                outcome.unreachable_ready_peers, outcome.ready_peers
            ));
        }
        self.inner.snapshot()
    }
}

pub struct KubeletNodeReadinessPublisher {
    query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    node_status: Arc<dyn klights_leader_api::LeaderNodeSelfStatus>,
}

impl KubeletNodeReadinessPublisher {
    pub fn new(
        query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
        node_status: Arc<dyn klights_leader_api::LeaderNodeSelfStatus>,
    ) -> Arc<Self> {
        Arc::new(Self { query, node_status })
    }
}

impl NodeReadinessPublisher for KubeletNodeReadinessPublisher {
    fn publish<'a>(
        &'a self,
        node_name: &'a str,
        health: &'a klights_network_api::DataplaneHealthSnapshot,
    ) -> NodeReadinessPublishFuture<'a> {
        Box::pin(async move {
            let result = klights_kubelet::node::publish_node_network_conditions(
                self.query.as_ref(),
                self.node_status.as_ref(),
                node_name,
                health,
                klights_supervisor::SystemWallClock::now_utc(),
            )
            .await
            .map_err(|error| {
                klights_reconcile_api::ControllerStoreError::unavailable(format!(
                    "publish Node readiness failed: {error}"
                ))
            })?;
            Ok(match result {
                klights_kubelet::node::NodeNetworkRefreshResult::Updated => {
                    NodeReadinessPublishResult::Updated
                }
                klights_kubelet::node::NodeNetworkRefreshResult::Unchanged => {
                    NodeReadinessPublishResult::Unchanged
                }
                klights_kubelet::node::NodeNetworkRefreshResult::Missing => {
                    NodeReadinessPublishResult::Missing
                }
            })
        })
    }
}
