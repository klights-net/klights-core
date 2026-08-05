use crate::outbox::OutboxOperation;
use crate::outbox::{Outbox, OutboxCommand, OutboxSendPlanner, OutboxSendRoute, OutboxSubject};
use anyhow::Result;
use klights_cluster_core::ResourcePreconditions;
use klights_cluster_core::StorageCommand;

pub use crate::node_heartbeat::run_heartbeat_with_lease_client;
pub use crate::node_leader_labels::clear_leader_label_from_other_nodes;
pub use crate::node_registration::{
    NodeRegistrationAddresses, NodeRegistrationHostFacts, NodeRegistrationSnapshot,
};
pub(crate) use crate::node_role_labels::role_label_keys_for_projection;
pub(crate) use crate::node_status_merge::preserve_existing_network_conditions;
pub use crate::node_status_merge::{
    merge_existing_node_mutable_fields, merge_node_status_for_update, set_node_external_ip,
};
#[cfg(feature = "test-support")]
pub(super) use crate::node_status_projection::stamp_git_commit_annotation;
pub(super) use crate::node_status_projection::{NodeNetworkConditions, apply_network_conditions};
pub use crate::node_status_projection::{
    set_node_dataplane_annotations, set_node_external_ip_from_dataplane_annotation,
    set_node_pod_cidr,
};

pub(super) fn parse_memory_ki(content: &str) -> Option<u64> {
    content
        .lines()
        .find(|line| line.starts_with("MemTotal:"))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u64>().ok())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeNetworkRefreshResult {
    Missing,
    Unchanged,
    Updated,
}

/// Worker-owned Node status publisher. It verifies the exact Node UID with a
/// current-leader read, then durably enqueues one status-only command. Remote
/// delivery and retries remain the outbox dispatcher's responsibility, so a
/// retry reuses the persisted row's idempotency and stream identity.
pub struct OutboxNodeSelfStatusPublisher {
    node_name: String,
    query: std::sync::Arc<dyn klights_leader_api::LeaderResourceQuery>,
    outbox: std::sync::Arc<Outbox>,
    wall_clock: std::sync::Arc<dyn crate::runtime_clock::RuntimeClock>,
}

impl OutboxNodeSelfStatusPublisher {
    pub fn new(
        node_name: impl Into<String>,
        query: std::sync::Arc<dyn klights_leader_api::LeaderResourceQuery>,
        outbox: std::sync::Arc<Outbox>,
        wall_clock: std::sync::Arc<dyn crate::runtime_clock::RuntimeClock>,
    ) -> Self {
        Self {
            node_name: node_name.into(),
            query,
            outbox,
            wall_clock,
        }
    }
}

impl klights_leader_api::LeaderNodeSelfStatus for OutboxNodeSelfStatusPublisher {
    fn submit_node_self_status(
        &self,
        request: klights_leader_api::NodeSelfStatusRequest,
    ) -> klights_leader_api::NodeSelfStatusFuture<'_, klights_leader_api::NodeSelfStatusResult>
    {
        Box::pin(async move {
            if request.node_name() != self.node_name {
                return Err(klights_leader_api::NodeSelfStatusError::unauthorized(
                    format!(
                        "node {} cannot publish Node status for {}",
                        self.node_name,
                        request.node_name()
                    ),
                ));
            }
            let get = klights_leader_api::node_get_request(
                &self.node_name,
                klights_leader_api::ResourceQueryConsistency::LeaderFresh,
            )
            .map_err(|error| {
                klights_leader_api::NodeSelfStatusError::retryable(error.to_string())
            })?;
            let current = self
                .query
                .get_resource(get)
                .await
                .map_err(|error| {
                    klights_leader_api::NodeSelfStatusError::retryable(error.to_string())
                })?
                .ok_or(klights_leader_api::NodeSelfStatusError::NotFound)?;
            if current.uid != request.node_uid() {
                return Err(klights_leader_api::NodeSelfStatusError::UidMismatch);
            }

            let node_uid = request.node_uid().to_string();
            let command = request.into_command();
            self.outbox
                .enqueue(OutboxCommand {
                    idempotency_key: format!(
                        "NodeStatus:v1/Node/{}/{}:{}",
                        self.node_name,
                        node_uid,
                        uuid::Uuid::new_v4()
                    ),
                    operation: OutboxOperation::NodeStatus,
                    subject: OutboxSubject {
                        key: format!("v1/Node/{}/{}", self.node_name, node_uid),
                        namespace: None,
                        name: self.node_name.clone(),
                        uid: Some(node_uid),
                    },
                    pod_uid: String::new(),
                    command,
                    now_ms: self.wall_clock.now_ms(),
                })
                .await
                .map_err(|error| {
                    klights_leader_api::NodeSelfStatusError::enqueue_failed(error.to_string())
                })?;
            Ok(klights_leader_api::NodeSelfStatusResult::Enqueued)
        })
    }
}

/// Re-evaluate and persist the local node's `Ready`/`NetworkUnavailable`
/// conditions from the current dataplane health. Event-driven: called by the
/// peer-route watcher when peer connectivity changes, so a node stops reporting
/// Ready as soon as a Ready peer becomes unreachable and recovers once the
/// WireGuard route is installed.
///
/// Writes go through the outbox when provided (mandatory on non-leader nodes,
/// which must not originate local cluster.db writes); the direct path is only
/// for the leader. Returns true if a write was issued.
pub async fn publish_node_network_conditions(
    query: &dyn klights_leader_api::LeaderResourceQuery,
    publisher: &dyn klights_leader_api::LeaderNodeSelfStatus,
    node_name: &str,
    dataplane_health: &klights_network_api::DataplaneHealthSnapshot,
    operation_now: chrono::DateTime<chrono::Utc>,
) -> Result<NodeNetworkRefreshResult> {
    let get = klights_leader_api::node_get_request(
        node_name,
        klights_leader_api::ResourceQueryConsistency::LeaderFresh,
    )?;
    let Some(existing) = query.get_resource(get).await? else {
        return Ok(NodeNetworkRefreshResult::Missing);
    };
    let conditions = NodeNetworkConditions::from_health(Some(dataplane_health));
    let mut node = existing.data.as_ref().clone();
    if !apply_network_conditions(&mut node, &conditions, operation_now) {
        return Ok(NodeNetworkRefreshResult::Unchanged);
    }
    let status = node
        .get("status")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let request =
        klights_leader_api::NodeSelfStatusRequest::try_new(StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: node_name.to_string(),
            status,
            expected_rv: None,
            preconditions: ResourcePreconditions::uid(existing.uid),
            observed_status_stamp: None,
        })?;
    publisher.submit_node_self_status(request).await?;
    Ok(NodeNetworkRefreshResult::Updated)
}

pub async fn refresh_current_git_commit_annotation_via_leader(
    query: &dyn klights_leader_api::LeaderResourceQuery,
    commands: &dyn klights_leader_api::LeaderResourceCommand,
    node_name: &str,
    git_commit: &str,
) -> Result<()> {
    let get = klights_leader_api::node_get_request(
        node_name,
        klights_leader_api::ResourceQueryConsistency::LeaderFresh,
    )?;
    let Some(current) = query.get_resource(get).await? else {
        return Ok(());
    };
    let command = current_git_commit_annotation_patch_command(&current, git_commit);
    let request = klights_leader_api::ResourceCommandRequest::try_new(command)?;
    commands
        .submit_resource_command(request)
        .await
        .map(|_| ())
        .map_err(|error| {
            anyhow::anyhow!("leader rejected Node git-commit annotation patch: {error}")
        })
}

fn current_git_commit_annotation_patch_command(
    node: &klights_cluster_core::Resource,
    git_commit: &str,
) -> StorageCommand {
    use klights_network_api::GIT_COMMIT_ANNOTATION;
    StorageCommand::PatchResource {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: node.name.clone(),
        patch_kind: klights_cluster_core::PatchKind::Merge,
        patch: serde_json::json!({
            "metadata": {
                "annotations": {
                    GIT_COMMIT_ANNOTATION: git_commit,
                }
            }
        }),
        preconditions: ResourcePreconditions::from_resource(node),
        strict_resource_version: true,
    }
}

pub(super) async fn send_node_command(
    outbox: Option<&Outbox>,
    operation: OutboxOperation,
    node_name: &str,
    node_uid: &str,
    command: StorageCommand,
    now_ms: i64,
) -> Result<OutboxSendRoute> {
    let subject_key = if node_uid.is_empty() {
        format!("v1/Node/{node_name}")
    } else {
        format!("v1/Node/{node_name}/{node_uid}")
    };
    OutboxSendPlanner::new(outbox)
        .route(OutboxCommand {
            idempotency_key: format!(
                "{}:{}:{}",
                operation.as_str(),
                subject_key,
                uuid::Uuid::new_v4()
            ),
            operation,
            subject: OutboxSubject {
                key: subject_key,
                namespace: None,
                name: node_name.to_string(),
                uid: (!node_uid.is_empty()).then(|| node_uid.to_string()),
            },
            pod_uid: String::new(),
            command,
            now_ms,
        })
        .await
}

pub async fn publish_node_external_ip_if_changed(
    query: &dyn klights_leader_api::LeaderResourceQuery,
    publisher: &dyn klights_leader_api::LeaderNodeSelfStatus,
    node_name: &str,
    external_ip: &str,
) -> Result<()> {
    let external_ip = external_ip.trim();
    if external_ip.is_empty() {
        return Ok(());
    }
    let get = klights_leader_api::node_get_request(
        node_name,
        klights_leader_api::ResourceQueryConsistency::LeaderFresh,
    )?;
    let Some(resource) = query.get_resource(get).await? else {
        return Ok(());
    };
    let mut data = (*resource.data).clone();
    if !set_node_external_ip(&mut data, external_ip) {
        return Ok(());
    }
    let status = data
        .get("status")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let request =
        klights_leader_api::NodeSelfStatusRequest::try_new(StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: node_name.to_string(),
            status,
            expected_rv: None,
            preconditions: ResourcePreconditions::uid(resource.uid),
            observed_status_stamp: None,
        })?;
    publisher.submit_node_self_status(request).await?;
    Ok(())
}

#[cfg(feature = "test-support")]
pub fn project_network_conditions_for_integration_test(
    node: &mut serde_json::Value,
    dataplane_health: &klights_network_api::DataplaneHealthSnapshot,
    operation_now: chrono::DateTime<chrono::Utc>,
) -> bool {
    let conditions = NodeNetworkConditions::from_health(Some(dataplane_health));
    apply_network_conditions(node, &conditions, operation_now)
}

#[cfg(feature = "test-support")]
pub fn stamp_git_commit_annotation_for_integration_test(
    node: &mut serde_json::Value,
    git_commit: &str,
) -> bool {
    stamp_git_commit_annotation(node, git_commit)
}
