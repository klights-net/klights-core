use crate::control_plane::client::LeaderApiClient;
use crate::datastore::NodeSubnet;
use crate::task_supervisor::TaskSupervisor;
use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_MAX_ATTEMPTS: usize = 6;
const DEFAULT_BACKOFF: Duration = Duration::from_secs(5);

#[async_trait::async_trait]
pub(crate) trait NodeSubnetAllocationClient: Send + Sync {
    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet>;
}

#[async_trait::async_trait]
pub(crate) trait NodeSubnetAllocationStore: Send + Sync {
    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>>;
}

#[derive(Clone, Copy)]
pub(crate) struct DatastoreNodeSubnetAllocationStore<'a> {
    db: &'a dyn crate::datastore::DatastoreBackend,
}

impl<'a> DatastoreNodeSubnetAllocationStore<'a> {
    pub(crate) fn new(db: &'a dyn crate::datastore::DatastoreBackend) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl NodeSubnetAllocationStore for DatastoreNodeSubnetAllocationStore<'_> {
    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>> {
        crate::datastore::DatastoreBackend::get_node_subnet(self.db, node_name).await
    }
}

struct LeaderApiNodeSubnetAllocationClient {
    inner: Arc<dyn LeaderApiClient>,
}

#[async_trait::async_trait]
impl NodeSubnetAllocationClient for LeaderApiNodeSubnetAllocationClient {
    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        self.inner
            .allocate_node_subnet(node_name, cluster_cidr, node_ip)
            .await
    }
}

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
    client: Arc<dyn NodeSubnetAllocationClient>,
    supervisor: Arc<TaskSupervisor>,
    retry: NodeSubnetAllocationRetryPolicy,
}

impl NodeSubnetAllocator {
    pub(crate) fn new(
        leader_api: Arc<dyn LeaderApiClient>,
        supervisor: Arc<TaskSupervisor>,
    ) -> Self {
        Self::with_policy(
            Arc::new(LeaderApiNodeSubnetAllocationClient { inner: leader_api }),
            supervisor,
            NodeSubnetAllocationRetryPolicy::default(),
        )
    }

    pub(crate) fn with_policy(
        client: Arc<dyn NodeSubnetAllocationClient>,
        supervisor: Arc<TaskSupervisor>,
        retry: NodeSubnetAllocationRetryPolicy,
    ) -> Self {
        Self {
            client,
            supervisor,
            retry,
        }
    }

    pub(crate) async fn allocate(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet> {
        let max_attempts = self.retry.max_attempts.max(1);
        let mut attempt = 1usize;

        loop {
            match self
                .client
                .allocate_node_subnet(node_name, cluster_cidr, node_ip)
                .await
            {
                Ok(subnet) => return Ok(subnet),
                Err(err) => {
                    if attempt >= max_attempts || !is_retryable_allocation_error(&err) {
                        return Err(err);
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

    pub(crate) async fn allocate_or_reuse_existing<S>(
        &self,
        store: &S,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet>
    where
        S: NodeSubnetAllocationStore + ?Sized,
    {
        match store.get_node_subnet(node_name).await {
            Ok(Some(subnet)) => {
                tracing::info!(
                    node = node_name,
                    subnet = %subnet.subnet,
                    "reusing existing local node subnet allocation"
                );
                return Ok(subnet);
            }
            Ok(None) => {}
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

fn is_retryable_allocation_error(err: &anyhow::Error) -> bool {
    err.chain().any(is_retryable_allocation_message)
}

fn is_retryable_allocation_message(cause: &(dyn std::error::Error + 'static)) -> bool {
    let message = cause.to_string().to_ascii_lowercase();
    message.contains("retryable unary rpc error")
        || message.contains("deadline exceeded")
        || message.contains("not raft leader")
        || message.contains("transport")
        || message.contains("unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datastore::NodeSubnet;
    use crate::networking::{NodeName, PodSubnet};
    use crate::task_supervisor::{TaskCategoryConfig, TaskSupervisor};
    use std::collections::VecDeque;
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[derive(Clone)]
    enum Outcome {
        Ok(NodeSubnet),
        Err(&'static str),
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

    struct FakeNodeSubnetStore {
        calls: AtomicUsize,
        row: Mutex<Option<NodeSubnet>>,
        error: Option<&'static str>,
    }

    impl FakeNodeSubnetStore {
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

    #[async_trait::async_trait]
    impl NodeSubnetAllocationStore for FakeNodeSubnetStore {
        async fn get_node_subnet(&self, _node_name: &str) -> anyhow::Result<Option<NodeSubnet>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(error) = self.error {
                anyhow::bail!("{error}");
            }
            Ok(self.row.lock().expect("row lock").clone())
        }
    }

    #[async_trait::async_trait]
    impl NodeSubnetAllocationClient for FakeAllocationClient {
        async fn allocate_node_subnet(
            &self,
            _node_name: &str,
            _cluster_cidr: &str,
            _node_ip: &str,
        ) -> anyhow::Result<NodeSubnet> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self
                .outcomes
                .lock()
                .expect("outcomes lock")
                .pop_front()
                .expect("test must provide enough outcomes")
            {
                Outcome::Ok(row) => Ok(row),
                Outcome::Err(message) => anyhow::bail!("{message}"),
            }
        }
    }

    fn subnet_row() -> NodeSubnet {
        NodeSubnet {
            node_name: NodeName::parse("node-a").unwrap(),
            subnet: PodSubnet::parse("10.50.1.0/24").unwrap(),
            subnet_base_int: u32::from(Ipv4Addr::new(10, 50, 1, 0)),
            gateway_ip: Ipv4Addr::new(10, 50, 1, 1),
            node_ip: Ipv4Addr::new(192, 0, 2, 10),
            mode: crate::controllers::annotations::NodePeerMode::Root,
            hostport_range: None,
        }
    }

    fn test_allocator(client: Arc<dyn NodeSubnetAllocationClient>) -> NodeSubnetAllocator {
        let supervisor = Arc::new(TaskSupervisor::new(TaskCategoryConfig::default()));
        NodeSubnetAllocator::with_policy(
            client,
            supervisor,
            NodeSubnetAllocationRetryPolicy {
                max_attempts: 3,
                backoff: Duration::from_millis(1),
            },
        )
    }

    #[tokio::test]
    async fn retries_retryable_deadline_and_returns_success() {
        let client = FakeAllocationClient::new(vec![
            Outcome::Err(
                "gRPC AllocateNodeSubnet failed: retryable unary RPC error: \
                 grpc_allocate_node_subnet deadline exceeded after 15s",
            ),
            Outcome::Ok(subnet_row()),
        ]);
        let allocator = test_allocator(client.clone());

        let row = allocator
            .allocate("node-a", "10.50.0.0/16", "192.0.2.10")
            .await
            .expect("retryable timeout must be retried");

        assert_eq!(row.node_name.as_str(), "node-a");
        assert_eq!(client.calls(), 2);
    }

    #[tokio::test]
    async fn does_not_retry_non_retryable_errors() {
        let client = FakeAllocationClient::new(vec![Outcome::Err("invalid cluster cidr")]);
        let allocator = test_allocator(client.clone());

        let err = allocator
            .allocate("node-a", "not-a-cidr", "192.0.2.10")
            .await
            .expect_err("invalid input must fail immediately");

        assert!(err.to_string().contains("invalid cluster cidr"));
        assert_eq!(client.calls(), 1);
    }

    #[tokio::test]
    async fn reuses_existing_local_subnet_without_leader_rpc() {
        let client = FakeAllocationClient::new(vec![]);
        let store = FakeNodeSubnetStore::new(Some(subnet_row()));
        let allocator = test_allocator(client.clone());

        let row = allocator
            .allocate_or_reuse_existing(&store, "node-a", "10.50.0.0/16", "192.0.2.10")
            .await
            .expect("existing local subnet must be reused");

        assert_eq!(row.node_name.as_str(), "node-a");
        assert_eq!(store.calls(), 1);
        assert_eq!(client.calls(), 0);
    }

    #[tokio::test]
    async fn datastore_adapter_reuses_existing_subnet_without_allocating_new_one() {
        let db = crate::datastore::test_support::in_memory().await;
        let stored = db
            .allocate_node_subnet("node-a", "10.50.0.0/16", "192.0.2.10")
            .await
            .expect("node subnet allocator store should materialize row in test DB");

        let client = FakeAllocationClient::new(vec![Outcome::Ok(subnet_row())]);
        let allocator = test_allocator(client.clone());
        let store = DatastoreNodeSubnetAllocationStore::new(&db);

        let row = allocator
            .allocate_or_reuse_existing(&store, "node-a", "10.50.0.0/16", "192.0.2.10")
            .await
            .expect("existing local row must be reused");

        assert_eq!(row.subnet, stored.subnet);
        assert_eq!(client.calls(), 0);
    }

    #[tokio::test]
    async fn local_subnet_read_errors_fail_closed_without_leader_rpc() {
        let client = FakeAllocationClient::new(vec![Outcome::Ok(subnet_row())]);
        let store = FakeNodeSubnetStore::new_error("node subnet store unavailable");
        let allocator = test_allocator(client.clone());

        let err = allocator
            .allocate_or_reuse_existing(&store, "node-a", "10.50.0.0/16", "192.0.2.10")
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
    fn retryable_detection_walks_wrapped_error_chain() {
        let err = anyhow::anyhow!(
            "retryable unary RPC error: grpc_allocate_node_subnet deadline exceeded after 15s"
        )
        .context("gRPC AllocateNodeSubnet failed");

        assert!(is_retryable_allocation_error(&err));
    }
}
