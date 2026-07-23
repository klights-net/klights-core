use anyhow::{Context, Result};
use klights_leader_api::{
    LeaderNetworkTopologyQuery, LeaderNodeSubnetAllocation, NodeSubnetAllocationError,
    NodeSubnetAllocationRequest, NodeSubnetQuery,
};
use klights_supervisor::TaskSupervisor;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_MAX_ATTEMPTS: usize = 6;
const DEFAULT_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
pub(crate) struct NodeSubnetAllocationRetryPolicy {
    max_attempts: usize,
    backoff: Duration,
}

impl Default for NodeSubnetAllocationRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            backoff: DEFAULT_BACKOFF,
        }
    }
}

pub(crate) struct NodeSubnetAllocator {
    allocation: Arc<dyn LeaderNodeSubnetAllocation>,
    topology: Arc<dyn LeaderNetworkTopologyQuery>,
    supervisor: Arc<TaskSupervisor>,
    retry: NodeSubnetAllocationRetryPolicy,
}

impl NodeSubnetAllocator {
    pub(crate) fn new(
        allocation: Arc<dyn LeaderNodeSubnetAllocation>,
        topology: Arc<dyn LeaderNetworkTopologyQuery>,
        supervisor: Arc<TaskSupervisor>,
    ) -> Self {
        Self::with_policy(
            allocation,
            topology,
            supervisor,
            NodeSubnetAllocationRetryPolicy::default(),
        )
    }

    pub(crate) fn with_policy(
        allocation: Arc<dyn LeaderNodeSubnetAllocation>,
        topology: Arc<dyn LeaderNetworkTopologyQuery>,
        supervisor: Arc<TaskSupervisor>,
        retry: NodeSubnetAllocationRetryPolicy,
    ) -> Self {
        Self {
            allocation,
            topology,
            supervisor,
            retry,
        }
    }

    pub(crate) async fn allocate(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<crate::networking::PodSubnet> {
        let max_attempts = self.retry.max_attempts.max(1);
        let mut attempt = 1usize;

        loop {
            let request = NodeSubnetAllocationRequest::try_new(node_name, cluster_cidr, node_ip)
                .map_err(anyhow::Error::new)?;
            match self.allocation.allocate_node_subnet(request).await {
                Ok(result) => {
                    return crate::networking::PodSubnet::parse(result.into_subnet().subnet())
                        .map_err(anyhow::Error::msg);
                }
                Err(err) => {
                    if attempt >= max_attempts || !is_retryable_allocation_error(&err) {
                        return Err(anyhow::Error::new(err));
                    }

                    tracing::warn!(
                        node = node_name,
                        attempt,
                        max_attempts,
                        error = %err,
                        "node subnet allocation hit a transient leader RPC error; retrying"
                    );
                    self.supervisor
                        .sleep("node_subnet_allocation_retry", self.retry.backoff)
                        .await
                        .context("node subnet allocation retry timer failed")?;
                    attempt += 1;
                }
            }
        }
    }

    pub(crate) async fn allocate_or_reuse_existing(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<crate::networking::PodSubnet> {
        let query = NodeSubnetQuery::try_new(node_name).map_err(anyhow::Error::new)?;
        match self.topology.get_node_subnet(query).await {
            Ok(result) if result.as_ref().is_some() => {
                let subnet =
                    crate::networking::PodSubnet::parse(result.as_ref().expect("checked").subnet())
                        .map_err(anyhow::Error::msg)?;
                tracing::info!(
                    node = node_name,
                    subnet = %subnet,
                    "reusing existing local node subnet allocation"
                );
                return Ok(subnet);
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(
                    node = node_name,
                    error = %err,
                    "failed to read existing local node subnet; refusing to allocate a second subnet"
                );
                return Err(err).with_context(|| {
                    format!("failed to read existing local node subnet for {node_name}")
                });
            }
        }

        self.allocate(node_name, cluster_cidr, node_ip).await
    }
}

fn is_retryable_allocation_error(err: &NodeSubnetAllocationError) -> bool {
    matches!(
        err,
        NodeSubnetAllocationError::NotLeader
            | NodeSubnetAllocationError::Retryable { .. }
            | NodeSubnetAllocationError::Timeout
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::client::focused_node_subnet;
    use crate::datastore::NodeSubnet;
    use crate::networking::{NodeName, PodSubnet};
    use klights_leader_api::{
        NetworkTopologyError, NetworkTopologyFuture, NodeDataplaneQuery, NodeDataplaneResult,
        NodeSubnetAllocationFuture, NodeSubnetAllocationResult, NodeSubnetResult, PeerSubnetsQuery,
        PeerSubnetsResult,
    };
    use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use std::collections::VecDeque;
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[derive(Clone)]
    enum Outcome {
        Ok(NodeSubnet),
        Err(NodeSubnetAllocationError),
    }

    struct FakeAllocationClient {
        calls: AtomicUsize,
        outcomes: Mutex<VecDeque<Outcome>>,
    }

    impl FakeAllocationClient {
        fn new(outcomes: Vec<Outcome>) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                outcomes: Mutex::new(outcomes.into()),
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    struct FakeTopologyClient {
        calls: AtomicUsize,
        row: Mutex<Option<NodeSubnet>>,
        error: Option<&'static str>,
    }

    impl FakeTopologyClient {
        fn new(row: Option<NodeSubnet>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                row: Mutex::new(row),
                error: None,
            }
        }

        fn new_error(error: &'static str) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                row: Mutex::new(None),
                error: Some(error),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl LeaderNetworkTopologyQuery for FakeTopologyClient {
        fn get_node_subnet(
            &self,
            request: NodeSubnetQuery,
        ) -> NetworkTopologyFuture<'_, NodeSubnetResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let error = self.error;
            let row = self.row.lock().expect("row lock").clone();
            Box::pin(async move {
                if let Some(error) = error {
                    return Err(NetworkTopologyError::query_failed(error));
                }
                let focused = row.map(focused_node_subnet).transpose()?;
                NodeSubnetResult::try_from_wire(request.node_name(), focused.is_some(), focused)
            })
        }

        fn list_peer_subnets(
            &self,
            _request: PeerSubnetsQuery,
        ) -> NetworkTopologyFuture<'_, PeerSubnetsResult> {
            Box::pin(async {
                Err(NetworkTopologyError::query_failed(
                    "unexpected peer subnet query",
                ))
            })
        }

        fn get_node_dataplane(
            &self,
            _request: NodeDataplaneQuery,
        ) -> NetworkTopologyFuture<'_, NodeDataplaneResult> {
            Box::pin(async {
                Err(NetworkTopologyError::query_failed(
                    "unexpected node dataplane query",
                ))
            })
        }
    }

    impl LeaderNodeSubnetAllocation for FakeAllocationClient {
        fn allocate_node_subnet(
            &self,
            request: NodeSubnetAllocationRequest,
        ) -> NodeSubnetAllocationFuture<'_, NodeSubnetAllocationResult> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let outcome = self
                .outcomes
                .lock()
                .expect("outcomes lock")
                .pop_front()
                .expect("test must provide enough outcomes");
            Box::pin(async move {
                match outcome {
                    Outcome::Ok(row) => {
                        let subnet = focused_node_subnet(row).map_err(|error| {
                            NodeSubnetAllocationError::corrupt_response(error.to_string())
                        })?;
                        NodeSubnetAllocationResult::try_from_wire(request.node_name(), Some(subnet))
                    }
                    Outcome::Err(error) => Err(error),
                }
            })
        }
    }

    fn subnet_row() -> NodeSubnet {
        NodeSubnet {
            node_name: NodeName::parse("node-a").unwrap(),
            subnet: PodSubnet::parse("10.50.1.0/24").unwrap(),
            subnet_base_int: u32::from(Ipv4Addr::new(10, 50, 1, 0)),
            gateway_ip: Ipv4Addr::new(10, 50, 1, 0),
            node_ip: Ipv4Addr::new(192, 0, 2, 10),
            mode: crate::controllers::annotations::NodePeerMode::Root,
            hostport_range: None,
        }
    }

    fn test_allocator_with_topology(
        client: Arc<dyn LeaderNodeSubnetAllocation>,
        topology: Arc<dyn LeaderNetworkTopologyQuery>,
    ) -> NodeSubnetAllocator {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        NodeSubnetAllocator::with_policy(
            client,
            topology,
            supervisor,
            NodeSubnetAllocationRetryPolicy {
                max_attempts: 3,
                backoff: Duration::from_millis(1),
            },
        )
    }

    fn test_allocator(client: Arc<dyn LeaderNodeSubnetAllocation>) -> NodeSubnetAllocator {
        test_allocator_with_topology(client, Arc::new(FakeTopologyClient::new(None)))
    }

    #[tokio::test]
    async fn retries_retryable_deadline_and_returns_success() {
        let client = FakeAllocationClient::new(vec![
            Outcome::Err(NodeSubnetAllocationError::Timeout),
            Outcome::Ok(subnet_row()),
        ]);
        let allocator = test_allocator(client.clone());

        let row = allocator
            .allocate("node-a", "10.50.0.0/16", "192.0.2.10")
            .await
            .expect("retryable timeout must be retried");

        assert_eq!(row.to_string(), "10.50.1.0/24");
        assert_eq!(client.calls(), 2);
    }

    #[tokio::test]
    async fn does_not_retry_non_retryable_errors() {
        let client = FakeAllocationClient::new(vec![Outcome::Err(
            NodeSubnetAllocationError::conflict("node already owns a different subnet"),
        )]);
        let allocator = test_allocator(client.clone());

        let err = allocator
            .allocate("node-a", "10.50.0.0/16", "192.0.2.10")
            .await
            .expect_err("invalid input must fail immediately");

        assert!(err.to_string().contains("different subnet"));
        assert_eq!(client.calls(), 1);
    }

    #[tokio::test]
    async fn reuses_existing_local_subnet_without_leader_rpc() {
        let client = FakeAllocationClient::new(vec![]);
        let store = Arc::new(FakeTopologyClient::new(Some(subnet_row())));
        let allocator = test_allocator_with_topology(client.clone(), store.clone());

        let row = allocator
            .allocate_or_reuse_existing("node-a", "10.50.0.0/16", "192.0.2.10")
            .await
            .expect("existing local subnet must be reused");

        assert_eq!(row.to_string(), "10.50.1.0/24");
        assert_eq!(store.calls(), 1);
        assert_eq!(client.calls(), 0);
    }

    #[tokio::test]
    async fn local_subnet_read_errors_fail_closed_without_leader_rpc() {
        let client = FakeAllocationClient::new(vec![Outcome::Ok(subnet_row())]);
        let store = Arc::new(FakeTopologyClient::new_error(
            "node subnet store unavailable",
        ));
        let allocator = test_allocator_with_topology(client.clone(), store.clone());

        let err = allocator
            .allocate_or_reuse_existing("node-a", "10.50.0.0/16", "192.0.2.10")
            .await
            .expect_err("local subnet read errors must not allocate a second subnet");

        assert!(
            err.to_string()
                .contains("failed to read existing local node subnet for node-a"),
            "{err:#}"
        );
        assert_eq!(store.calls(), 1);
        assert_eq!(client.calls(), 0);
    }

    #[test]
    fn retryable_detection_uses_typed_error_variants() {
        assert!(is_retryable_allocation_error(
            &NodeSubnetAllocationError::NotLeader
        ));
        assert!(is_retryable_allocation_error(
            &NodeSubnetAllocationError::retryable("transport unavailable")
        ));
        assert!(!is_retryable_allocation_error(
            &NodeSubnetAllocationError::conflict("terminal conflict")
        ));
    }
}
