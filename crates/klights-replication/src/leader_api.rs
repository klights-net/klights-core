//! Local focused leader-API implementations for the embedded Raft engine.

use std::sync::Arc;

use klights_cluster_core::{
    OutboxApplyError, OutboxOperation, OutboxStreamWatermark, PatchKind, ResourcePreconditions,
    StorageCommand, StorageCommandRejectionCode, StorageMutationError,
};
use klights_cluster_store::{AppliedMutation, StorageCommandResult};
use klights_leader_api::{
    LeaderResourceCommand, LeaderResourceQuery, OutboxDeliveryError, OutboxDeliveryOperation,
    ResourceCommandError, ResourceCommandFuture, ResourceCommandRequest, ResourceCommandResult,
    ResourceGetRequest, ResourceListRequest, ResourceQueryConsistency,
};
use klights_types::ResourceKey;

use crate::proposal::RaftProposal;

pub struct EmbeddedLeaderResourceCommand {
    proposal: Arc<dyn RaftProposal>,
    resource_query: Arc<dyn LeaderResourceQuery>,
    authority: Arc<dyn klights_leader_api::LeaderAuthority>,
}

impl EmbeddedLeaderResourceCommand {
    pub fn new(
        proposal: Arc<dyn RaftProposal>,
        resource_query: Arc<dyn LeaderResourceQuery>,
        authority: Arc<dyn klights_leader_api::LeaderAuthority>,
    ) -> Self {
        Self {
            proposal,
            resource_query,
            authority,
        }
    }

    fn local_permit(&self) -> Result<klights_leader_api::AuthorityPermit, ResourceCommandError> {
        let klights_leader_api::AuthorityRoute::Local(permit) = self.authority.route() else {
            return Err(ResourceCommandError::NotLeader);
        };
        self.authority
            .validate(&permit)
            .map_err(|_| ResourceCommandError::NotLeader)?;
        Ok(permit)
    }

    /// Preserve the narrow Pod hard-delete boundary while legacy datastore
    /// consumers move to the focused Pod lifecycle capabilities. Bound Pods
    /// are always translated to the actor command; only an observed
    /// unscheduled Pod retains the strict UID/RV delete command.
    async fn submit_actor_pod_delete(
        &self,
        command: StorageCommand,
    ) -> Result<i64, ResourceCommandError> {
        klights_leader_api::validate_scoped_authority()
            .map_err(|_| ResourceCommandError::NotLeader)?;
        klights_leader_api::validate_controller_lease_if_scoped().map_err(|error| {
            ResourceCommandError::retryable(format!(
                "controller authority rejected actor Pod deletion: {error}"
            ))
        })?;
        let StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace: Some(namespace),
            name,
            preconditions,
        } = command
        else {
            return Err(ResourceCommandError::UnsupportedCommand {
                command: command.variant_name(),
            });
        };
        if api_version != "v1" || kind != "Pod" {
            return Err(ResourceCommandError::UnsupportedCommand {
                command: "DeleteResource",
            });
        }
        let expected_uid = preconditions
            .uid
            .as_deref()
            .filter(|uid| !uid.is_empty())
            .ok_or_else(|| ResourceCommandError::invalid_request("pod.uid", "must not be empty"))?;
        let expected_rv = preconditions
            .resource_version
            .filter(|rv| *rv > 0)
            .ok_or_else(|| {
                ResourceCommandError::invalid_request("pod.resource_version", "must be positive")
            })?;
        let key = ResourceKey::new("v1", "Pod", Some(namespace.clone()), name.clone());
        let current = self
            .resource_query
            .get_resource(
                ResourceGetRequest::try_new(key, ResourceQueryConsistency::LeaderFresh).map_err(
                    |error| ResourceCommandError::invalid_request("pod", error.to_string()),
                )?,
            )
            .await
            .map_err(|error| ResourceCommandError::retryable(error.to_string()))?
            .ok_or_else(|| ResourceCommandError::NotFound {
                message: format!("Pod {namespace}/{name} not found"),
            })?;
        if current.uid != expected_uid || current.resource_version != expected_rv {
            return Err(ResourceCommandError::Conflict {
                message: format!("Pod {namespace}/{name} changed before actor deletion"),
            });
        }
        let routed = match current
            .data
            .pointer("/spec/nodeName")
            .and_then(serde_json::Value::as_str)
            .filter(|node_name| !node_name.is_empty())
        {
            Some(node_name) => StorageCommand::FinalizeBoundPod {
                namespace,
                name,
                pod_uid: current.uid,
                node_name: node_name.to_string(),
                observed_resource_version: current.resource_version,
            },
            None => StorageCommand::DeleteResource {
                api_version,
                kind,
                namespace: Some(namespace),
                name,
                preconditions,
            },
        };
        klights_leader_api::validate_scoped_authority()
            .map_err(|_| ResourceCommandError::NotLeader)?;
        let result = self
            .proposal
            .propose_command(routed)
            .await
            .map_err(resource_command_submission_error)?;
        match resource_command_result(result, false)? {
            ResourceCommandResult::Ack { resource_version } => Ok(resource_version),
            ResourceCommandResult::Resource(_) => Err(ResourceCommandError::corrupt_response(
                "actor Pod deletion returned a resource",
            )),
        }
    }
}

impl LeaderResourceCommand for EmbeddedLeaderResourceCommand {
    fn submit_resource_command(
        &self,
        request: ResourceCommandRequest,
    ) -> ResourceCommandFuture<'_, ResourceCommandResult> {
        Box::pin(async move {
            let permit = self.local_permit()?;
            klights_leader_api::scope_authority(self.authority.clone(), permit, async move {
                if matches!(
                    request.command(),
                    StorageCommand::DeleteResource {
                        api_version,
                        kind,
                        namespace: Some(_),
                        ..
                    } if api_version == "v1" && kind == "Pod"
                ) {
                    return self
                        .submit_actor_pod_delete(request.into_command())
                        .await
                        .map(|resource_version| ResourceCommandResult::Ack { resource_version });
                }
                klights_leader_api::validate_scoped_authority()
                    .map_err(|_| ResourceCommandError::NotLeader)?;
                klights_leader_api::validate_controller_lease_if_scoped().map_err(|error| {
                    ResourceCommandError::retryable(format!(
                        "controller authority rejected resource command: {error}"
                    ))
                })?;
                let command = request.into_command();
                let expects_resource = !matches!(
                    command,
                    StorageCommand::DeleteResource { .. }
                        | StorageCommand::DeleteNamespace { .. }
                        | StorageCommand::DeleteNamespaceContents { .. }
                        | StorageCommand::DeletePodCleanupIntentsForNode { .. }
                        | StorageCommand::ApplyResourceBatch { .. }
                );
                klights_leader_api::validate_scoped_authority()
                    .map_err(|_| ResourceCommandError::NotLeader)?;
                let result = self
                    .proposal
                    .propose_command(command.clone())
                    .await
                    .map_err(resource_command_submission_error)?;
                resource_command_result_or_current(
                    self.resource_query.as_ref(),
                    &command,
                    result,
                    expects_resource,
                )
                .await
            })
            .await
        })
    }
}

async fn resource_command_result_or_current(
    resource_query: &dyn LeaderResourceQuery,
    command: &StorageCommand,
    result: StorageCommandResult,
    expects_resource: bool,
) -> Result<ResourceCommandResult, ResourceCommandError> {
    let query_current = expects_resource
        && !result.public_resource_changed
        && result.error_message.is_none()
        && result.applied_mutation.is_none();
    if query_current {
        let key = resource_command_key(command).ok_or_else(|| {
            ResourceCommandError::corrupt_response(format!(
                "resource-returning {} command has no singular resource identity",
                command.variant_name()
            ))
        })?;
        let request = ResourceGetRequest::try_new(key, ResourceQueryConsistency::LeaderFresh)
            .map_err(|error| ResourceCommandError::corrupt_response(error.to_string()))?;
        return resource_query
            .get_resource(request)
            .await
            .map_err(|error| {
                ResourceCommandError::retryable(format!(
                    "read current resource after no-op resource commit: {error}"
                ))
            })?
            .map(ResourceCommandResult::Resource)
            .ok_or_else(|| {
                ResourceCommandError::corrupt_response(
                    "no-op resource commit target disappeared before current-resource delivery",
                )
            });
    }
    let query_ack_position =
        !expects_resource && result.applied_rv.is_none() && result.error_message.is_none();
    if query_ack_position {
        if matches!(command, StorageCommand::ApplyResourceBatch { .. }) {
            return Ok(ResourceCommandResult::Ack {
                resource_version: 0,
            });
        }
        let key = resource_command_key(command).ok_or_else(|| {
            ResourceCommandError::corrupt_response(format!(
                "resource-deleting {} command has no collection identity",
                command.variant_name()
            ))
        })?;
        let request = ResourceListRequest::try_new(
            key.api_version,
            key.kind,
            key.namespace,
            None,
            None,
            Some(0),
            None,
            ResourceQueryConsistency::LeaderFresh,
        )
        .map_err(|error| ResourceCommandError::corrupt_response(error.to_string()))?;
        let current = resource_query
            .list_resources(request)
            .await
            .map_err(|error| {
                ResourceCommandError::retryable(format!(
                    "read collection position after resource delete commit: {error}"
                ))
            })?;
        return Ok(ResourceCommandResult::Ack {
            resource_version: current.resource_version(),
        });
    }
    resource_command_result(result, expects_resource)
}

fn resource_command_key(command: &StorageCommand) -> Option<ResourceKey> {
    match command {
        StorageCommand::CreateResource {
            api_version,
            kind,
            namespace,
            name,
            ..
        }
        | StorageCommand::UpdateResource {
            api_version,
            kind,
            namespace,
            name,
            ..
        }
        | StorageCommand::PatchResource {
            api_version,
            kind,
            namespace,
            name,
            ..
        }
        | StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace,
            name,
            ..
        }
        | StorageCommand::DeleteResourceWithTombstone {
            api_version,
            kind,
            namespace,
            name,
            ..
        } => Some(ResourceKey::new(
            api_version.clone(),
            kind.clone(),
            namespace.clone(),
            name.clone(),
        )),
        StorageCommand::CreateNamespace { name, .. }
        | StorageCommand::UpdateNamespace { name, .. } => {
            Some(ResourceKey::new("v1", "Namespace", None, name.clone()))
        }
        StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace,
            name,
            ..
        } => Some(ResourceKey::new(
            api_version.clone(),
            kind.clone(),
            namespace.clone(),
            name.clone(),
        )),
        StorageCommand::DeleteNamespace { name }
        | StorageCommand::DeleteNamespaceContents { name } => {
            Some(ResourceKey::new("v1", "Namespace", None, name.clone()))
        }
        _ => None,
    }
}

fn resource_command_submission_error(error: anyhow::Error) -> ResourceCommandError {
    let message = error.to_string();
    match error
        .downcast_ref::<StorageMutationError>()
        .and_then(StorageMutationError::rejection_code)
    {
        Some(StorageCommandRejectionCode::AlreadyExists) => {
            ResourceCommandError::AlreadyExists { message }
        }
        Some(StorageCommandRejectionCode::Conflict) => ResourceCommandError::Conflict { message },
        Some(StorageCommandRejectionCode::NotFound) => ResourceCommandError::NotFound { message },
        Some(StorageCommandRejectionCode::InvalidCommit) | None => {
            ResourceCommandError::submission_failed(message)
        }
    }
}

fn resource_command_result(
    result: StorageCommandResult,
    expects_resource: bool,
) -> Result<ResourceCommandResult, ResourceCommandError> {
    if let Some(message) = result.error_message {
        return Err(
            match result
                .rejection_code
                .unwrap_or(StorageCommandRejectionCode::InvalidCommit)
            {
                StorageCommandRejectionCode::AlreadyExists => {
                    ResourceCommandError::AlreadyExists { message }
                }
                StorageCommandRejectionCode::Conflict => ResourceCommandError::Conflict { message },
                StorageCommandRejectionCode::NotFound => ResourceCommandError::NotFound { message },
                StorageCommandRejectionCode::InvalidCommit => {
                    ResourceCommandError::submission_failed(message)
                }
            },
        );
    }
    let applied_rv = result.applied_rv;
    let resource = result.applied_mutation.map(|mutation| match mutation {
        AppliedMutation::Resource(mut resource) => {
            if let Some(resource_version) = applied_rv {
                resource.resource_version = resource_version;
                if let Some(metadata) = std::sync::Arc::make_mut(&mut resource.data)
                    .get_mut("metadata")
                    .and_then(serde_json::Value::as_object_mut)
                {
                    metadata.insert(
                        "resourceVersion".to_string(),
                        serde_json::Value::String(resource_version.to_string()),
                    );
                }
            }
            resource
        }
    });
    if expects_resource {
        return resource
            .map(ResourceCommandResult::Resource)
            .ok_or_else(|| {
                ResourceCommandError::corrupt_response(
                    "committed resource command omitted its exact domain result",
                )
            });
    }
    applied_rv
        .map(|resource_version| ResourceCommandResult::Ack { resource_version })
        .ok_or_else(|| {
            ResourceCommandError::corrupt_response(
                "committed resource command acknowledgement omitted resourceVersion",
            )
        })
}

pub struct EmbeddedOutboxDelivery {
    proposal: Arc<dyn RaftProposal>,
    resource_query: Arc<dyn LeaderResourceQuery>,
    authority: Arc<dyn klights_leader_api::LeaderAuthority>,
}

impl EmbeddedOutboxDelivery {
    pub fn new(
        proposal: Arc<dyn RaftProposal>,
        resource_query: Arc<dyn LeaderResourceQuery>,
        authority: Arc<dyn klights_leader_api::LeaderAuthority>,
    ) -> Self {
        Self {
            proposal,
            resource_query,
            authority,
        }
    }

    pub async fn deliver_authenticated_outbox_command_effect(
        &self,
        authenticated_node: String,
        idempotency_key: String,
        operation: OutboxDeliveryOperation,
        decoded_command: Result<StorageCommand, OutboxDeliveryError>,
        watermark: Option<OutboxStreamWatermark>,
    ) -> Result<crate::proposal::RaftProposalEffect, OutboxDeliveryError> {
        let permit = match self.authority.route() {
            klights_leader_api::AuthorityRoute::Local(permit) => permit,
            klights_leader_api::AuthorityRoute::Forward { .. }
            | klights_leader_api::AuthorityRoute::Unavailable => {
                return Err(OutboxDeliveryError::NotLeader);
            }
        };
        self.authority
            .validate(&permit)
            .map_err(|_| OutboxDeliveryError::NotLeader)?;
        klights_leader_api::scope_authority(
            self.authority.clone(),
            permit,
            self.deliver_authenticated_outbox_command_effect_scoped(
                authenticated_node,
                idempotency_key,
                operation,
                decoded_command,
                watermark,
            ),
        )
        .await
    }

    async fn deliver_authenticated_outbox_command_effect_scoped(
        &self,
        authenticated_node: String,
        idempotency_key: String,
        operation: OutboxDeliveryOperation,
        decoded_command: Result<StorageCommand, OutboxDeliveryError>,
        watermark: Option<OutboxStreamWatermark>,
    ) -> Result<crate::proposal::RaftProposalEffect, OutboxDeliveryError> {
        klights_leader_api::validate_scoped_authority()
            .map_err(|_| OutboxDeliveryError::NotLeader)?;
        let mut command = match decoded_command {
            Ok(command) => command,
            Err(error) => {
                if !error.is_terminal() {
                    return Err(error);
                }
                self.consume_terminal_sequence(
                    &idempotency_key,
                    operation,
                    &authenticated_node,
                    watermark,
                )
                .await?;
                return Err(error);
            }
        };
        if let Err(error) = authorize_outbox_command(operation, &command, &authenticated_node) {
            self.consume_terminal_sequence(
                &idempotency_key,
                operation,
                &authenticated_node,
                watermark,
            )
            .await?;
            return Err(error);
        }
        if operation == OutboxDeliveryOperation::PodMetadata {
            match self
                .authorize_live_pod_metadata(command, &authenticated_node)
                .await
            {
                Ok(authorized) => command = authorized,
                Err(error) => {
                    if error.is_terminal() {
                        self.consume_terminal_sequence(
                            &idempotency_key,
                            operation,
                            &authenticated_node,
                            watermark,
                        )
                        .await?;
                    }
                    return Err(error);
                }
            }
        }
        if operation == OutboxDeliveryOperation::NodeStatus
            && let Err(error) = klights_leader_api::NodeSelfStatusRequest::validate_command(
                &command,
            )
            .map_err(|error| match error {
                klights_leader_api::NodeSelfStatusError::InvalidRequest { field, message } => {
                    OutboxDeliveryError::invalid(field, message)
                }
                other => OutboxDeliveryError::invalid("delivery.payload", other.to_string()),
            })
        {
            self.consume_terminal_sequence(
                &idempotency_key,
                operation,
                &authenticated_node,
                watermark,
            )
            .await?;
            return Err(error);
        }

        klights_leader_api::validate_scoped_authority()
            .map_err(|_| OutboxDeliveryError::NotLeader)?;
        self.proposal
            .propose_outbox_command_effect(
                &idempotency_key,
                OutboxOperation::from(operation).as_str(),
                command,
                &authenticated_node,
                watermark,
            )
            .await
            .map_err(OutboxDeliveryError::from)
    }

    async fn consume_terminal_sequence(
        &self,
        idempotency_key: &str,
        operation: OutboxDeliveryOperation,
        authenticated_node: &str,
        watermark: Option<OutboxStreamWatermark>,
    ) -> Result<(), OutboxDeliveryError> {
        let assigned_sequence = watermark.is_some();
        let result = self
            .proposal
            .propose_outbox_command(
                idempotency_key,
                OutboxOperation::from(operation).as_str(),
                terminal_decision_command(idempotency_key),
                authenticated_node,
                watermark,
            )
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(OutboxApplyError::NotFound(message))
                if assigned_sequence
                    && message == "Pod __klights-terminal-outbox__/decision not found" =>
            {
                Ok(())
            }
            Err(error)
                if matches!(
                    error,
                    OutboxApplyError::NotFound(_)
                        | OutboxApplyError::UidMismatch { .. }
                        | OutboxApplyError::ConflictTerminal(_)
                ) =>
            {
                Err(OutboxDeliveryError::unavailable(format!(
                    "terminal outbox ledger does not prove this stream position: {error}"
                )))
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn authorize_live_pod_metadata(
        &self,
        command: StorageCommand,
        authenticated_node: &str,
    ) -> Result<StorageCommand, OutboxDeliveryError> {
        if let StorageCommand::FinalizeBoundPod {
            namespace,
            name,
            pod_uid,
            node_name,
            observed_resource_version,
        } = &command
        {
            if namespace.is_empty()
                || name.is_empty()
                || pod_uid.is_empty()
                || *observed_resource_version <= 0
                || node_name != authenticated_node
            {
                return Err(OutboxDeliveryError::conflict(
                    "bound Pod finalization carries invalid actor observation",
                ));
            }
            let query = klights_leader_api::pod_get_request(
                namespace,
                name,
                ResourceQueryConsistency::LeaderFresh,
            )
            .map_err(|error| OutboxDeliveryError::unavailable(error.to_string()))?;
            let live = self
                .resource_query
                .get_resource(query)
                .await
                .map_err(|error| OutboxDeliveryError::unavailable(error.to_string()))?
                .ok_or_else(|| {
                    OutboxDeliveryError::not_found(format!("Pod {namespace}/{name} not found"))
                })?;
            if live.uid != *pod_uid {
                return Err(OutboxDeliveryError::uid_mismatch(pod_uid, live.uid));
            }
            let assigned_node = live
                .data
                .pointer("/spec/nodeName")
                .and_then(serde_json::Value::as_str)
                .filter(|assigned| !assigned.is_empty());
            if assigned_node != Some(authenticated_node) {
                return Err(OutboxDeliveryError::conflict(format!(
                    "PodMetadata delivery for {namespace}/{name} is restricted to its assigned node"
                )));
            }
            return Ok(StorageCommand::FinalizeBoundPod {
                namespace: namespace.clone(),
                name: name.clone(),
                pod_uid: pod_uid.clone(),
                node_name: node_name.clone(),
                observed_resource_version: live.resource_version,
            });
        }
        let Some((namespace, name, preconditions)) = pod_target(&command) else {
            return Err(OutboxDeliveryError::conflict(
                "PodMetadata delivery must target one namespaced v1/Pod",
            ));
        };
        let Some(expected_uid) = preconditions.uid.as_deref().filter(|uid| !uid.is_empty()) else {
            return Err(OutboxDeliveryError::conflict(
                "PodMetadata delivery must carry a Pod UID precondition",
            ));
        };
        let query = klights_leader_api::pod_get_request(
            namespace,
            name,
            ResourceQueryConsistency::LeaderFresh,
        )
        .map_err(|error| OutboxDeliveryError::unavailable(error.to_string()))?;
        let live = self
            .resource_query
            .get_resource(query)
            .await
            .map_err(|error| OutboxDeliveryError::unavailable(error.to_string()))?
            .ok_or_else(|| {
                OutboxDeliveryError::not_found(format!("Pod {namespace}/{name} not found"))
            })?;
        if live.uid != expected_uid {
            return Err(OutboxDeliveryError::uid_mismatch(expected_uid, live.uid));
        }
        let assigned_node = live
            .data
            .pointer("/spec/nodeName")
            .and_then(serde_json::Value::as_str)
            .filter(|node_name| !node_name.is_empty());
        if assigned_node != Some(authenticated_node) {
            return Err(OutboxDeliveryError::conflict(format!(
                "PodMetadata delivery for {namespace}/{name} is restricted to its assigned node"
            )));
        }
        Ok(command)
    }
}

fn terminal_decision_command(idempotency_key: &str) -> StorageCommand {
    StorageCommand::UpdateStatus {
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("__klights-terminal-outbox__".to_string()),
        name: "decision".to_string(),
        status: serde_json::json!({}),
        expected_rv: None,
        preconditions: ResourcePreconditions::uid(idempotency_key),
        observed_status_stamp: None,
    }
}

fn authorize_outbox_command(
    operation: OutboxDeliveryOperation,
    command: &StorageCommand,
    authenticated_node: &str,
) -> Result<(), OutboxDeliveryError> {
    if authenticated_node.is_empty() {
        return Err(OutboxDeliveryError::ConflictTerminal(
            "durable outbox delivery requires an authenticated node identity".to_string(),
        ));
    }
    let authorized = match operation {
        OutboxDeliveryOperation::PodStatus
        | OutboxDeliveryOperation::RuntimeReconcile
        | OutboxDeliveryOperation::ProbeReadiness
        | OutboxDeliveryOperation::DeadlineExceeded
        | OutboxDeliveryOperation::ContainerStatusSnapshot
        | OutboxDeliveryOperation::EphemeralContainerStatuses => is_uid_bound_pod_status(command),
        OutboxDeliveryOperation::PodMetadata => is_uid_bound_pod_metadata(command),
        OutboxDeliveryOperation::NodeRegistration => {
            is_authenticated_node_registration(command, authenticated_node)
        }
        OutboxDeliveryOperation::NodeStatus => {
            is_authenticated_node_status(command, authenticated_node)
        }
        OutboxDeliveryOperation::NodeDataplane => matches!(
            command,
            StorageCommand::UpdateNodeDataplane { node_name, .. }
                if node_name == authenticated_node
        ),
        OutboxDeliveryOperation::EventCreate => {
            is_authenticated_event_create(command, authenticated_node)
        }
    };
    if authorized {
        Ok(())
    } else {
        Err(OutboxDeliveryError::ConflictTerminal(format!(
            "outbox operation {} does not authorize {} for authenticated node {authenticated_node}",
            operation.as_wire_name(),
            command.variant_name(),
        )))
    }
}

fn is_uid_bound_pod_status(command: &StorageCommand) -> bool {
    matches!(
        command,
        StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace: Some(namespace),
            name,
            preconditions,
            ..
        } if api_version == "v1"
            && kind == "Pod"
            && !namespace.is_empty()
            && !name.is_empty()
            && preconditions.uid.as_deref().is_some_and(|uid| !uid.is_empty())
    )
}

fn is_uid_bound_pod_metadata(command: &StorageCommand) -> bool {
    match command {
        StorageCommand::FinalizeBoundPod {
            namespace,
            name,
            pod_uid,
            node_name,
            observed_resource_version,
        } => {
            !namespace.is_empty()
                && !name.is_empty()
                && !pod_uid.is_empty()
                && !node_name.is_empty()
                && *observed_resource_version > 0
        }
        StorageCommand::PatchResource {
            api_version,
            kind,
            namespace: Some(namespace),
            name,
            patch_kind: PatchKind::Merge,
            patch,
            preconditions,
            strict_resource_version,
        } => {
            api_version == "v1"
                && kind == "Pod"
                && !namespace.is_empty()
                && !name.is_empty()
                && preconditions
                    .uid
                    .as_deref()
                    .is_some_and(|uid| !uid.is_empty())
                && is_exact_pod_metadata_patch(
                    patch,
                    preconditions.resource_version,
                    *strict_resource_version,
                )
        }
        _ => false,
    }
}

fn is_exact_pod_metadata_patch(
    patch: &serde_json::Value,
    resource_version: Option<i64>,
    strict_resource_version: bool,
) -> bool {
    let Some(root) = patch.as_object().filter(|root| root.len() == 1) else {
        return false;
    };
    let Some(metadata) = root.get("metadata").and_then(serde_json::Value::as_object) else {
        return false;
    };
    if metadata.len() == 2 {
        return !strict_resource_version
            && resource_version.is_none()
            && metadata
                .get("deletionTimestamp")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|timestamp| !timestamp.is_empty())
            && metadata
                .get("deletionGracePeriodSeconds")
                .and_then(serde_json::Value::as_i64)
                .is_some_and(|seconds| seconds >= 0);
    }
    if metadata.len() != 1 || !strict_resource_version || resource_version.is_none_or(|rv| rv <= 0)
    {
        return false;
    }
    match metadata
        .iter()
        .next()
        .map(|(key, value)| (key.as_str(), value))
    {
        Some(("ownerReferences", owner_references)) => owner_references.is_array(),
        Some(("labels", labels)) => labels
            .as_object()
            .is_some_and(|labels| labels.values().all(|value| value.as_str().is_some())),
        Some(("annotations", annotations)) => annotations.as_object().is_some_and(|annotations| {
            annotations.len() == 1
                && annotations
                    .get("klights.dev/sandbox-id")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
        }),
        _ => false,
    }
}

fn is_authenticated_node_registration(command: &StorageCommand, authenticated_node: &str) -> bool {
    match command {
        StorageCommand::CreateResource {
            api_version,
            kind,
            namespace,
            name,
            data,
        } => {
            api_version == "v1"
                && kind == "Node"
                && namespace.is_none()
                && name == authenticated_node
                && resource_body_matches(data, api_version, kind, None, name, None)
        }
        StorageCommand::UpdateResource {
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
            ..
        } => {
            let Some(uid) = preconditions.uid.as_deref().filter(|uid| !uid.is_empty()) else {
                return false;
            };
            api_version == "v1"
                && kind == "Node"
                && namespace.is_none()
                && name == authenticated_node
                && resource_body_matches(data, api_version, kind, None, name, Some(uid))
        }
        _ => false,
    }
}

fn is_authenticated_node_status(command: &StorageCommand, authenticated_node: &str) -> bool {
    matches!(
        command,
        StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            ..
        } if api_version == "v1"
            && kind == "Node"
            && namespace.is_none()
            && name == authenticated_node
            && preconditions.uid.as_deref().is_some_and(|uid| !uid.is_empty())
    )
}

fn is_authenticated_event_create(command: &StorageCommand, authenticated_node: &str) -> bool {
    let StorageCommand::CreateResource {
        api_version,
        kind,
        namespace: Some(namespace),
        name,
        data,
    } = command
    else {
        return false;
    };
    if !matches!(api_version.as_str(), "v1" | "events.k8s.io/v1")
        || kind != "Event"
        || namespace.is_empty()
        || name.is_empty()
        || !resource_body_matches(data, api_version, kind, Some(namespace), name, None)
    {
        return false;
    }
    match api_version.as_str() {
        "v1" => {
            data.pointer("/source/host")
                .and_then(serde_json::Value::as_str)
                == Some(authenticated_node)
        }
        "events.k8s.io/v1" => {
            data.get("reportingInstance")
                .and_then(serde_json::Value::as_str)
                == Some(authenticated_node)
        }
        _ => false,
    }
}

fn resource_body_matches(
    data: &serde_json::Value,
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    uid: Option<&str>,
) -> bool {
    data.get("apiVersion").and_then(serde_json::Value::as_str) == Some(api_version)
        && data.get("kind").and_then(serde_json::Value::as_str) == Some(kind)
        && data
            .pointer("/metadata/name")
            .and_then(serde_json::Value::as_str)
            == Some(name)
        && match namespace {
            Some(namespace) => {
                data.pointer("/metadata/namespace")
                    .and_then(serde_json::Value::as_str)
                    == Some(namespace)
            }
            None => data.pointer("/metadata/namespace").is_none(),
        }
        && match uid {
            Some(uid) => {
                data.pointer("/metadata/uid")
                    .and_then(serde_json::Value::as_str)
                    == Some(uid)
            }
            None => true,
        }
}

fn pod_target(command: &StorageCommand) -> Option<(&str, &str, &ResourcePreconditions)> {
    match command {
        StorageCommand::UpdateStatus {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            ..
        }
        | StorageCommand::UpdateResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            ..
        }
        | StorageCommand::PatchResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
            ..
        }
        | StorageCommand::DeleteResource {
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        } if api_version == "v1" && kind == "Pod" => Some((
            namespace.as_deref().unwrap_or("default"),
            name,
            preconditions,
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn always_authority() -> Arc<dyn klights_leader_api::LeaderAuthority> {
        let (authority, _publisher) = crate::authority::WatchLeaderAuthority::channel(true, None);
        authority
    }

    fn follower_authority() -> Arc<dyn klights_leader_api::LeaderAuthority> {
        let (authority, _publisher) = crate::authority::WatchLeaderAuthority::channel(false, None);
        authority
    }
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn command_result(
        applied_rv: Option<i64>,
        error_message: Option<&str>,
        rejection_code: Option<StorageCommandRejectionCode>,
        resource: Option<klights_cluster_core::Resource>,
    ) -> StorageCommandResult {
        StorageCommandResult::new(
            applied_rv,
            error_message.map(str::to_string),
            rejection_code,
            false,
            resource.map(AppliedMutation::Resource),
            klights_cluster_core::PodEndpointEffect::NotApplicable,
        )
    }

    fn sample_resource() -> klights_cluster_core::Resource {
        klights_cluster_core::Resource::try_from_data(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "settings",
                "namespace": "default",
                "uid": "uid-settings",
                "resourceVersion": "42"
            },
            "data": {"mode": "strict"}
        })))
        .expect("sample resource")
    }

    struct FixedResourceQuery {
        resource: klights_cluster_core::Resource,
        gets: AtomicUsize,
    }

    impl LeaderResourceQuery for FixedResourceQuery {
        fn get_resource(
            &self,
            _request: klights_leader_api::ResourceGetRequest,
        ) -> klights_leader_api::ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>>
        {
            self.gets.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(Some(self.resource.clone())) })
        }

        fn list_resources(
            &self,
            _request: klights_leader_api::ResourceListRequest,
        ) -> klights_leader_api::ResourceQueryFuture<'_, klights_leader_api::ResourceListResult>
        {
            unreachable!("no-op status result lookup never lists resources")
        }
    }

    struct FixedProposal {
        result: StorageCommandResult,
    }

    struct RecordingProposal {
        result: StorageCommandResult,
        commands: std::sync::Mutex<Vec<StorageCommand>>,
    }

    struct RecordingOutboxProposal {
        calls: std::sync::Mutex<
            Vec<(
                String,
                String,
                StorageCommand,
                String,
                Option<OutboxStreamWatermark>,
            )>,
        >,
    }

    #[async_trait::async_trait]
    impl RaftProposal for RecordingOutboxProposal {
        async fn propose_command(
            &self,
            _command: StorageCommand,
        ) -> anyhow::Result<StorageCommandResult> {
            unreachable!("outbox routing regression must use the outbox proposal boundary")
        }

        async fn propose_outbox_command(
            &self,
            idempotency_key: &str,
            operation: &str,
            command: StorageCommand,
            authoring_node: &str,
            watermark: Option<OutboxStreamWatermark>,
        ) -> Result<klights_cluster_core::OutboxApplyOutcome, OutboxApplyError> {
            self.calls.lock().unwrap().push((
                idempotency_key.to_string(),
                operation.to_string(),
                command,
                authoring_node.to_string(),
                watermark,
            ));
            Ok(klights_cluster_core::OutboxApplyOutcome::Applied { applied_rv: 55 })
        }
    }

    #[async_trait::async_trait]
    impl RaftProposal for RecordingProposal {
        async fn propose_command(
            &self,
            command: StorageCommand,
        ) -> anyhow::Result<StorageCommandResult> {
            self.commands.lock().unwrap().push(command);
            Ok(self.result.clone())
        }

        async fn propose_outbox_command(
            &self,
            _idempotency_key: &str,
            _operation: &str,
            _command: StorageCommand,
            _authoring_node: &str,
            _watermark: Option<OutboxStreamWatermark>,
        ) -> Result<klights_cluster_core::OutboxApplyOutcome, OutboxApplyError> {
            unreachable!("actor Pod delete does not submit an outbox command")
        }
    }

    #[async_trait::async_trait]
    impl RaftProposal for FixedProposal {
        async fn propose_command(
            &self,
            _command: StorageCommand,
        ) -> anyhow::Result<StorageCommandResult> {
            Ok(self.result.clone())
        }

        async fn propose_outbox_command(
            &self,
            _idempotency_key: &str,
            _operation: &str,
            _command: StorageCommand,
            _authoring_node: &str,
            _watermark: Option<OutboxStreamWatermark>,
        ) -> Result<klights_cluster_core::OutboxApplyOutcome, OutboxApplyError> {
            unreachable!("resource-command tests do not submit outbox commands")
        }
    }

    #[tokio::test]
    async fn node_cleanup_bulk_delete_routes_exactly_once_through_raft_proposal() {
        let query = Arc::new(FixedResourceQuery {
            resource: sample_resource(),
            gets: AtomicUsize::new(0),
        });
        let proposal = Arc::new(RecordingProposal {
            result: command_result(Some(196), None, None, None),
            commands: std::sync::Mutex::new(Vec::new()),
        });
        let command = StorageCommand::DeletePodCleanupIntentsForNode {
            node_name: "e2e-fake-node".to_string(),
        };
        let client =
            EmbeddedLeaderResourceCommand::new(proposal.clone(), query.clone(), always_authority());

        let result = client
            .submit_resource_command(
                ResourceCommandRequest::try_new(command.clone())
                    .expect("node cleanup command must be admitted"),
            )
            .await
            .expect("node cleanup command must be proposed");

        assert_eq!(
            result,
            ResourceCommandResult::Ack {
                resource_version: 196
            }
        );
        assert_eq!(proposal.commands.lock().unwrap().as_slice(), &[command]);
        assert_eq!(query.gets.load(Ordering::Relaxed), 0);
    }

    fn exact_config_map_create_command() -> StorageCommand {
        StorageCommand::CreateResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "settings"},
                "data": {"mode": "strict"}
            }),
        }
    }

    fn exact_config_map_delete_command() -> StorageCommand {
        StorageCommand::DeleteResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            preconditions: ResourcePreconditions::uid_and_resource_version("uid-settings", 42),
        }
    }

    fn fixed_query() -> Arc<FixedResourceQuery> {
        Arc::new(FixedResourceQuery {
            resource: sample_resource(),
            gets: AtomicUsize::new(0),
        })
    }

    #[tokio::test]
    async fn single_node_create_resource_works() {
        let command = exact_config_map_create_command();
        let query = fixed_query();
        let proposal = Arc::new(RecordingProposal {
            result: command_result(Some(42), None, None, Some(sample_resource())),
            commands: Default::default(),
        });
        let service =
            EmbeddedLeaderResourceCommand::new(proposal.clone(), query.clone(), always_authority());

        let result = service
            .submit_resource_command(ResourceCommandRequest::try_new(command.clone()).unwrap())
            .await
            .expect("single-node leader create must return committed resource");

        assert_eq!(result, ResourceCommandResult::Resource(sample_resource()));
        assert_eq!(proposal.commands.lock().unwrap().as_slice(), &[command]);
        assert_eq!(query.gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn leader_allows_writes() {
        let command = exact_config_map_create_command();
        let query = fixed_query();
        let proposal = Arc::new(RecordingProposal {
            result: command_result(Some(42), None, None, Some(sample_resource())),
            commands: Default::default(),
        });
        let service =
            EmbeddedLeaderResourceCommand::new(proposal.clone(), query.clone(), always_authority());

        let result = service
            .submit_resource_command(ResourceCommandRequest::try_new(command.clone()).unwrap())
            .await
            .expect("current leader must accept the write");

        assert_eq!(result, ResourceCommandResult::Resource(sample_resource()));
        assert_eq!(proposal.commands.lock().unwrap().as_slice(), &[command]);
        assert_eq!(query.gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn leader_write_routes_through_proposer() {
        let command = exact_config_map_create_command();
        let query = fixed_query();
        let proposal = Arc::new(RecordingProposal {
            result: command_result(Some(42), None, None, Some(sample_resource())),
            commands: Default::default(),
        });
        let service =
            EmbeddedLeaderResourceCommand::new(proposal.clone(), query.clone(), always_authority());

        service
            .submit_resource_command(ResourceCommandRequest::try_new(command.clone()).unwrap())
            .await
            .expect("leader write must be proposed");

        assert_eq!(proposal.commands.lock().unwrap().as_slice(), &[command]);
        assert_eq!(query.gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn raft_mode_create_resource_routes_via_proposer() {
        let command = exact_config_map_create_command();
        let query = fixed_query();
        let proposal = Arc::new(RecordingProposal {
            result: command_result(Some(42), None, None, Some(sample_resource())),
            commands: Default::default(),
        });
        let service =
            EmbeddedLeaderResourceCommand::new(proposal.clone(), query.clone(), always_authority());

        let result = service
            .submit_resource_command(ResourceCommandRequest::try_new(command.clone()).unwrap())
            .await
            .expect("Raft create must return its committed resource");

        assert_eq!(result, ResourceCommandResult::Resource(sample_resource()));
        assert_eq!(proposal.commands.lock().unwrap().as_slice(), &[command]);
        assert_eq!(query.gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn raft_mode_delete_resource_routes_via_proposer() {
        let command = exact_config_map_delete_command();
        let query = fixed_query();
        let proposal = Arc::new(RecordingProposal {
            result: command_result(Some(43), None, None, None),
            commands: Default::default(),
        });
        let service =
            EmbeddedLeaderResourceCommand::new(proposal.clone(), query.clone(), always_authority());

        let result = service
            .submit_resource_command(ResourceCommandRequest::try_new(command.clone()).unwrap())
            .await
            .expect("Raft delete must return its committed position");

        assert_eq!(
            result,
            ResourceCommandResult::Ack {
                resource_version: 43
            }
        );
        assert_eq!(proposal.commands.lock().unwrap().as_slice(), &[command]);
        assert_eq!(query.gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn delete_resource_exposes_committed_rv_for_leader_log_apply() {
        let command = exact_config_map_delete_command();
        let query = fixed_query();
        let proposal = Arc::new(RecordingProposal {
            result: command_result(Some(43), None, None, None),
            commands: Default::default(),
        });
        let service =
            EmbeddedLeaderResourceCommand::new(proposal.clone(), query.clone(), always_authority());

        let result = service
            .submit_resource_command(ResourceCommandRequest::try_new(command.clone()).unwrap())
            .await
            .expect("delete acknowledgement must expose committed RV");

        assert_eq!(
            result,
            ResourceCommandResult::Ack {
                resource_version: 43
            }
        );
        assert_eq!(proposal.commands.lock().unwrap().as_slice(), &[command]);
        assert_eq!(query.gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn public_create_rejects_existing_name_with_different_uid() {
        let command = exact_config_map_create_command();
        let query = fixed_query();
        let proposal = Arc::new(RecordingProposal {
            result: command_result(
                None,
                Some("ConfigMap default/settings already exists with uid uid-settings"),
                Some(StorageCommandRejectionCode::AlreadyExists),
                None,
            ),
            commands: Default::default(),
        });
        let service =
            EmbeddedLeaderResourceCommand::new(proposal.clone(), query.clone(), always_authority());

        let error = service
            .submit_resource_command(ResourceCommandRequest::try_new(command.clone()).unwrap())
            .await
            .expect_err("same name with a different UID must remain AlreadyExists");

        assert!(matches!(error, ResourceCommandError::AlreadyExists { .. }));
        assert_eq!(proposal.commands.lock().unwrap().as_slice(), &[command]);
        assert_eq!(query.gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn raft_mode_follower_proposer_rejects_create_no_local_mutation() {
        let command = exact_config_map_create_command();
        let query = fixed_query();
        let proposal = Arc::new(RecordingProposal {
            result: command_result(Some(42), None, None, Some(sample_resource())),
            commands: Default::default(),
        });
        let service = EmbeddedLeaderResourceCommand::new(
            proposal.clone(),
            query.clone(),
            follower_authority(),
        );

        let error = service
            .submit_resource_command(ResourceCommandRequest::try_new(command).unwrap())
            .await
            .expect_err("follower create must be rejected before proposal");

        assert_eq!(error, ResourceCommandError::NotLeader);
        assert!(proposal.commands.lock().unwrap().is_empty());
        assert_eq!(query.gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn raft_mode_follower_proposer_rejects_delete_no_local_mutation() {
        let command = exact_config_map_delete_command();
        let query = fixed_query();
        let proposal = Arc::new(RecordingProposal {
            result: command_result(Some(43), None, None, None),
            commands: Default::default(),
        });
        let service = EmbeddedLeaderResourceCommand::new(
            proposal.clone(),
            query.clone(),
            follower_authority(),
        );

        let error = service
            .submit_resource_command(ResourceCommandRequest::try_new(command).unwrap())
            .await
            .expect_err("follower delete must be rejected before proposal");

        assert_eq!(error, ResourceCommandError::NotLeader);
        assert!(proposal.commands.lock().unwrap().is_empty());
        assert_eq!(query.gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn raft_mode_mark_for_delete_without_watch_reuses_mark_and_routes_through_raft() {
        let command = StorageCommand::DeleteResourceWithTombstone {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            preconditions: ResourcePreconditions::uid_and_resource_version("uid-settings", 42),
            grace_seconds: 30,
        };
        let marked = klights_cluster_core::Resource::try_from_data(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "settings",
                "namespace": "default",
                "uid": "uid-settings",
                "resourceVersion": "43",
                "deletionTimestamp": "2026-08-12T00:00:00Z",
                "deletionGracePeriodSeconds": 30
            }
        })))
        .unwrap();
        let query = fixed_query();
        let proposal = Arc::new(RecordingProposal {
            result: command_result(Some(43), None, None, Some(marked.clone())),
            commands: Default::default(),
        });
        let service =
            EmbeddedLeaderResourceCommand::new(proposal.clone(), query.clone(), always_authority());

        let result = service
            .submit_resource_command(ResourceCommandRequest::try_new(command.clone()).unwrap())
            .await
            .expect("existing deletion mark must be returned from committed Raft apply");

        assert_eq!(result, ResourceCommandResult::Resource(marked));
        assert_eq!(proposal.commands.lock().unwrap().as_slice(), &[command]);
        assert_eq!(query.gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn replicated_apply_preserves_preconditions_through_codec() {
        let command = StorageCommand::UpdateResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            data: (*sample_resource().data).clone(),
            expected_rv: 42,
            preconditions: ResourcePreconditions::uid_and_resource_version("uid-settings", 42),
            preserve_status: false,
        };
        let query = fixed_query();
        let proposal = Arc::new(RecordingProposal {
            result: command_result(Some(43), None, None, Some(sample_resource())),
            commands: Default::default(),
        });
        let service =
            EmbeddedLeaderResourceCommand::new(proposal.clone(), query.clone(), always_authority());

        service
            .submit_resource_command(ResourceCommandRequest::try_new(command.clone()).unwrap())
            .await
            .expect("replicated update must preserve strict preconditions");

        assert!(matches!(
            proposal.commands.lock().unwrap().as_slice(),
            [StorageCommand::UpdateResource {
                expected_rv: 42,
                preconditions,
                preserve_status: false,
                ..
            }] if preconditions.uid.as_deref() == Some("uid-settings")
                && preconditions.resource_version == Some(42)
        ));
        assert_eq!(query.gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn raft_mode_apply_outbox_transactionally_routes_via_proposer() {
        let command = pod_status_command(Some("pod-uid"));
        let query = fixed_query();
        let proposal = Arc::new(RecordingOutboxProposal {
            calls: Default::default(),
        });
        let service =
            EmbeddedOutboxDelivery::new(proposal.clone(), query.clone(), always_authority());

        let effect = service
            .deliver_authenticated_outbox_command_effect(
                "worker-a".to_string(),
                "outbox-17".to_string(),
                OutboxDeliveryOperation::PodStatus,
                Ok(command.clone()),
                None,
            )
            .await
            .expect("outbox apply must route through the authoritative proposer");

        assert!(matches!(
            effect.into_parts().0,
            klights_cluster_core::OutboxApplyOutcome::Applied { applied_rv: 55 }
        ));
        assert!(matches!(
            proposal.calls.lock().unwrap().as_slice(),
            [(idempotency_key, operation, recorded, authoring_node, None)]
                if idempotency_key == "outbox-17"
                    && operation == "PodStatus"
                    && recorded == &command
                    && authoring_node == "worker-a"
        ));
        assert_eq!(query.gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn raft_mode_follower_proposer_rejects_outbox_apply_no_local_mutation() {
        let query = fixed_query();
        let proposal = Arc::new(RecordingOutboxProposal {
            calls: Default::default(),
        });
        let service =
            EmbeddedOutboxDelivery::new(proposal.clone(), query.clone(), follower_authority());

        let error = match service
            .deliver_authenticated_outbox_command_effect(
                "worker-a".to_string(),
                "outbox-18".to_string(),
                OutboxDeliveryOperation::PodStatus,
                Ok(pod_status_command(Some("pod-uid"))),
                None,
            )
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("follower outbox apply must be rejected before proposal"),
        };

        assert!(matches!(error, OutboxDeliveryError::NotLeader));
        assert!(proposal.calls.lock().unwrap().is_empty());
        assert_eq!(query.gets.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn actor_pod_delete_routes_bound_identity_through_finalize_command() {
        let pod = klights_cluster_core::Resource::try_from_data(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "web",
                "namespace": "default",
                "uid": "pod-uid",
                "resourceVersion": "17",
                "deletionTimestamp": "2026-08-10T00:00:00Z"
            },
            "spec": {"nodeName": "worker-1"}
        })))
        .expect("bound Pod resource");
        let query = Arc::new(FixedResourceQuery {
            resource: pod.clone(),
            gets: AtomicUsize::new(0),
        });
        let proposal = Arc::new(RecordingProposal {
            result: command_result(Some(18), None, None, None),
            commands: Default::default(),
        });
        let service =
            EmbeddedLeaderResourceCommand::new(proposal.clone(), query, always_authority());

        let result = service
            .submit_resource_command(
                ResourceCommandRequest::try_new(StorageCommand::DeleteResource {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "web".to_string(),
                    preconditions: ResourcePreconditions::from_resource(&pod),
                })
                .expect("valid delete request"),
            )
            .await
            .expect("actor delete");

        assert_eq!(
            result,
            ResourceCommandResult::Ack {
                resource_version: 18
            }
        );
        assert!(matches!(
            proposal.commands.lock().unwrap().as_slice(),
            [StorageCommand::FinalizeBoundPod {
                namespace,
                name,
                pod_uid,
                node_name,
                observed_resource_version: 17,
            }] if namespace == "default"
                && name == "web"
                && pod_uid == "pod-uid"
                && node_name == "worker-1"
        ));
    }

    #[tokio::test]
    async fn noop_update_status_returns_current_resource_from_focused_query() {
        let current = sample_resource();
        let query = Arc::new(FixedResourceQuery {
            resource: current.clone(),
            gets: AtomicUsize::new(0),
        });
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            status: serde_json::json!({}),
            expected_rv: Some(current.resource_version),
            preconditions: ResourcePreconditions::from_resource(&current),
            observed_status_stamp: None,
        };
        let service = EmbeddedLeaderResourceCommand::new(
            Arc::new(FixedProposal {
                result: command_result(Some(current.resource_version), None, None, None),
            }),
            query.clone(),
            always_authority(),
        );
        let result = service
            .submit_resource_command(
                ResourceCommandRequest::try_new(command).expect("valid no-op status command"),
            )
            .await;
        assert_eq!(result, Ok(ResourceCommandResult::Resource(current)));
        assert_eq!(query.gets.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn noop_status_patch_returns_current_resource_from_focused_query() {
        let current = sample_resource();
        let query = Arc::new(FixedResourceQuery {
            resource: current.clone(),
            gets: AtomicUsize::new(0),
        });
        let command = StorageCommand::PatchResource {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            patch_kind: PatchKind::Merge,
            patch: serde_json::json!({"status": {}}),
            preconditions: ResourcePreconditions::from_resource(&current),
            strict_resource_version: false,
        };
        let service = EmbeddedLeaderResourceCommand::new(
            Arc::new(FixedProposal {
                result: command_result(Some(current.resource_version), None, None, None),
            }),
            query.clone(),
            always_authority(),
        );
        let result = service
            .submit_resource_command(
                ResourceCommandRequest::try_new(command).expect("valid no-op status patch"),
            )
            .await;
        assert_eq!(result, Ok(ResourceCommandResult::Resource(current)));
        assert_eq!(query.gets.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn noop_graceful_delete_returns_current_resource_from_focused_query() {
        let current = sample_resource();
        let query = Arc::new(FixedResourceQuery {
            resource: current.clone(),
            gets: AtomicUsize::new(0),
        });
        let command = StorageCommand::DeleteResourceWithTombstone {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            namespace: Some("default".to_string()),
            name: "settings".to_string(),
            preconditions: ResourcePreconditions::from_resource(&current),
            grace_seconds: 30,
        };
        let service = EmbeddedLeaderResourceCommand::new(
            Arc::new(FixedProposal {
                result: command_result(Some(current.resource_version), None, None, None),
            }),
            query.clone(),
            always_authority(),
        );
        let result = service
            .submit_resource_command(
                ResourceCommandRequest::try_new(command)
                    .expect("valid no-op graceful delete command"),
            )
            .await;
        assert_eq!(result, Ok(ResourceCommandResult::Resource(current)));
        assert_eq!(query.gets.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn exact_domain_result_preserves_resource_and_ack_variants() {
        let resource = sample_resource();
        assert_eq!(
            resource_command_result(
                command_result(
                    Some(resource.resource_version),
                    None,
                    None,
                    Some(resource.clone()),
                ),
                true,
            ),
            Ok(ResourceCommandResult::Resource(resource))
        );
        assert_eq!(
            resource_command_result(command_result(Some(43), None, None, None), false),
            Ok(ResourceCommandResult::Ack {
                resource_version: 43
            })
        );
    }

    #[test]
    fn exact_domain_result_rejects_missing_or_mismatched_shapes() {
        assert!(matches!(
            resource_command_result(command_result(Some(42), None, None, None), true),
            Err(ResourceCommandError::CorruptResponse { .. })
        ));
        assert!(matches!(
            resource_command_result(command_result(None, None, None, None), false),
            Err(ResourceCommandError::CorruptResponse { .. })
        ));
    }

    #[test]
    fn exact_domain_result_preserves_typed_rejections() {
        let cases = [
            (StorageCommandRejectionCode::AlreadyExists, "AlreadyExists"),
            (StorageCommandRejectionCode::Conflict, "Conflict"),
            (StorageCommandRejectionCode::NotFound, "NotFound"),
            (
                StorageCommandRejectionCode::InvalidCommit,
                "SubmissionFailed",
            ),
        ];
        for (code, expected) in cases {
            let error = resource_command_result(
                command_result(None, Some("rejected"), Some(code), None),
                true,
            )
            .expect_err("typed rejection");
            let actual = match error {
                ResourceCommandError::AlreadyExists { .. } => "AlreadyExists",
                ResourceCommandError::Conflict { .. } => "Conflict",
                ResourceCommandError::NotFound { .. } => "NotFound",
                ResourceCommandError::SubmissionFailed { .. } => "SubmissionFailed",
                other => panic!("unexpected rejection: {other:?}"),
            };
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn proposal_materialization_rejections_preserve_kubernetes_error_kind() {
        let cases = [
            StorageCommandRejectionCode::AlreadyExists,
            StorageCommandRejectionCode::Conflict,
            StorageCommandRejectionCode::NotFound,
        ];
        for code in cases {
            let error = resource_command_submission_error(anyhow::Error::new(
                StorageMutationError::rejected(code, format!("{code:?}")),
            ));
            assert!(
                matches!(
                    (&code, error),
                    (
                        StorageCommandRejectionCode::AlreadyExists,
                        ResourceCommandError::AlreadyExists { .. }
                    ) | (
                        StorageCommandRejectionCode::Conflict,
                        ResourceCommandError::Conflict { .. }
                    ) | (
                        StorageCommandRejectionCode::NotFound,
                        ResourceCommandError::NotFound { .. }
                    )
                ),
                "proposal rejection {code:?} lost its Kubernetes error kind"
            );
        }
    }

    #[test]
    fn worker_authorization_is_default_deny() {
        let command = StorageCommand::CreateNamespace {
            name: "forbidden".to_string(),
            data: serde_json::json!({}),
        };
        assert!(matches!(
            authorize_outbox_command(OutboxDeliveryOperation::PodMetadata, &command, "worker-a"),
            Err(OutboxDeliveryError::ConflictTerminal(_))
        ));
    }

    fn pod_status_command(uid: Option<&str>) -> StorageCommand {
        StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            status: serde_json::json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: uid.map(str::to_owned),
                resource_version: None,
            },
            observed_status_stamp: Some(41),
        }
    }

    #[test]
    fn outbox_authorization_admits_operation_specific_worker_command() {
        for operation in [
            OutboxDeliveryOperation::PodStatus,
            OutboxDeliveryOperation::RuntimeReconcile,
            OutboxDeliveryOperation::ProbeReadiness,
            OutboxDeliveryOperation::DeadlineExceeded,
            OutboxDeliveryOperation::ContainerStatusSnapshot,
            OutboxDeliveryOperation::EphemeralContainerStatuses,
        ] {
            authorize_outbox_command(operation, &pod_status_command(Some("pod-uid")), "worker-a")
                .expect("UID-bound Pod status is authorized");
        }

        let pod_patch = StorageCommand::PatchResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            patch_kind: PatchKind::Merge,
            patch: serde_json::json!({
                "metadata": {
                    "deletionTimestamp": "2026-07-18T00:00:00Z",
                    "deletionGracePeriodSeconds": 0
                }
            }),
            preconditions: ResourcePreconditions::uid("pod-uid"),
            strict_resource_version: false,
        };
        authorize_outbox_command(OutboxDeliveryOperation::PodMetadata, &pod_patch, "worker-a")
            .expect("UID-bound actor delete-mark patch is authorized");

        let node_status = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: "worker-a".to_string(),
            status: serde_json::json!({"conditions": []}),
            expected_rv: None,
            preconditions: ResourcePreconditions::uid("node-uid"),
            observed_status_stamp: None,
        };
        authorize_outbox_command(
            OutboxDeliveryOperation::NodeStatus,
            &node_status,
            "worker-a",
        )
        .expect("a node may update its own UID-bound status");

        let node_registration = StorageCommand::CreateResource {
            api_version: "v1".to_string(),
            kind: "Node".to_string(),
            namespace: None,
            name: "worker-a".to_string(),
            data: serde_json::json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "worker-a"}
            }),
        };
        authorize_outbox_command(
            OutboxDeliveryOperation::NodeRegistration,
            &node_registration,
            "worker-a",
        )
        .expect("a node may register only its own identity");

        let event = StorageCommand::CreateResource {
            api_version: "events.k8s.io/v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "started.123".to_string(),
            data: serde_json::json!({
                "apiVersion": "events.k8s.io/v1",
                "kind": "Event",
                "metadata": {"namespace": "default", "name": "started.123"},
                "reportingInstance": "worker-a"
            }),
        };
        authorize_outbox_command(OutboxDeliveryOperation::EventCreate, &event, "worker-a")
            .expect("a node may create only an Event attributed to itself");
    }

    #[test]
    fn pod_metadata_authorization_rejects_unfocused_commands() {
        let rejected = [
            StorageCommand::UpdateResource {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "web".to_string(),
                data: serde_json::json!({"apiVersion": "v1", "kind": "Pod"}),
                expected_rv: 7,
                preconditions: ResourcePreconditions::uid_and_resource_version("pod-uid", 7),
                preserve_status: false,
            },
            StorageCommand::PatchResource {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "web".to_string(),
                patch_kind: PatchKind::Merge,
                patch: serde_json::json!({"spec": {"nodeName": "worker-b"}}),
                preconditions: ResourcePreconditions::uid_and_resource_version("pod-uid", 7),
                strict_resource_version: true,
            },
            StorageCommand::DeleteResource {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "web".to_string(),
                preconditions: ResourcePreconditions::uid_and_resource_version("pod-uid", 7),
            },
        ];
        for command in rejected {
            assert!(
                matches!(
                    authorize_outbox_command(
                        OutboxDeliveryOperation::PodMetadata,
                        &command,
                        "worker-a"
                    ),
                    Err(OutboxDeliveryError::ConflictTerminal(_))
                ),
                "unfocused Pod mutation must be terminally rejected"
            );
        }
    }

    #[tokio::test]
    async fn pod_metadata_authorization_accepts_structural_finalize() {
        let pod = klights_cluster_core::Resource::try_from_data(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "web",
                "namespace": "default",
                "uid": "pod-uid",
                "resourceVersion": "17"
            },
            "spec": {"nodeName": "worker-a"}
        })))
        .expect("bound Pod resource");
        let query = Arc::new(FixedResourceQuery {
            resource: pod,
            gets: AtomicUsize::new(0),
        });
        let service = EmbeddedOutboxDelivery::new(
            Arc::new(FixedProposal {
                result: command_result(Some(18), None, None, None),
            }),
            query,
            always_authority(),
        );
        let command = StorageCommand::FinalizeBoundPod {
            namespace: "default".to_string(),
            name: "web".to_string(),
            pod_uid: "pod-uid".to_string(),
            node_name: "worker-a".to_string(),
            observed_resource_version: 7,
        };
        let authorized = service
            .authorize_live_pod_metadata(command, "worker-a")
            .await
            .expect("structurally valid finalize is authorized");
        assert!(matches!(
            authorized,
            StorageCommand::FinalizeBoundPod {
                observed_resource_version: 17,
                ..
            }
        ));
    }

    #[test]
    fn finalize_bound_pod_subject_is_uid_scoped() {
        let command = StorageCommand::FinalizeBoundPod {
            namespace: "team-a".to_string(),
            name: "web-0".to_string(),
            pod_uid: "uid-web-0".to_string(),
            node_name: "worker-a".to_string(),
            observed_resource_version: 41,
        };
        assert_eq!(
            klights_cluster_core::subject_key_for_command(&command),
            "v1/Pod/team-a/web-0/uid-web-0"
        );
    }
}
