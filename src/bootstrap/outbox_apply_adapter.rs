//! Test-only consensus fixture for callers that do not need a live OpenRaft node.
//!
//! This fixture deliberately owns no command decoding, CAS, dedupe, apply, or
//! side-effect policy. It implements the replication-owned proposal port by
//! delegating materialization and committed apply to the datastore's existing
//! production boundaries.

use std::sync::Arc;

use anyhow::Context as _;
use async_trait::async_trait;
use klights_cluster_core::{
    OutboxApplyError, OutboxApplyOutcome, OutboxStreamWatermark, StorageCommand,
};
use klights_replication::proposal::{RaftProposal, RaftProposalEffect};

use crate::datastore::{DatastoreBackend, ResourceListQuery};

pub(crate) struct BackendProposalFixture {
    backend: Arc<dyn DatastoreBackend>,
}

pub(crate) struct BackendResourceQueryFixture {
    backend: Arc<dyn DatastoreBackend>,
    is_leader_rx: tokio::sync::watch::Receiver<bool>,
}

impl BackendResourceQueryFixture {
    pub(crate) fn new(
        backend: Arc<dyn DatastoreBackend>,
        is_leader_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        Self {
            backend,
            is_leader_rx,
        }
    }
}

impl klights_leader_api::LeaderResourceQuery for BackendResourceQueryFixture {
    fn get_resource(
        &self,
        request: klights_leader_api::ResourceGetRequest,
    ) -> klights_leader_api::ResourceQueryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async move {
            if request.consistency() == klights_leader_api::ResourceQueryConsistency::LeaderFresh
                && !*self.is_leader_rx.borrow()
            {
                return Err(klights_leader_api::ResourceQueryError::retryable(
                    "test resource query target is not leader",
                ));
            }
            let key = request.into_key();
            self.backend
                .get_resource(
                    &key.api_version,
                    &key.kind,
                    key.namespace.as_deref(),
                    &key.name,
                )
                .await
                .map_err(|error| {
                    klights_leader_api::ResourceQueryError::query_failed(error.to_string())
                })
        })
    }

    fn list_resources(
        &self,
        request: klights_leader_api::ResourceListRequest,
    ) -> klights_leader_api::ResourceQueryFuture<'_, klights_leader_api::ResourceListResult> {
        Box::pin(async move {
            if request.consistency() == klights_leader_api::ResourceQueryConsistency::LeaderFresh
                && !*self.is_leader_rx.borrow()
            {
                return Err(klights_leader_api::ResourceQueryError::retryable(
                    "test resource query target is not leader",
                ));
            }
            let list = self
                .backend
                .list_resources(
                    request.api_version(),
                    request.kind(),
                    request.namespace(),
                    ResourceListQuery::new(
                        request.label_selector(),
                        request.field_selector(),
                        request.limit(),
                        request.continue_token(),
                    ),
                )
                .await
                .map_err(|error| {
                    klights_leader_api::ResourceQueryError::query_failed(error.to_string())
                })?;
            crate::control_plane::client::query_list_result(list)
        })
    }
}

impl BackendProposalFixture {
    pub(crate) fn new(backend: Arc<dyn DatastoreBackend>) -> Self {
        Self { backend }
    }
}

pub(crate) async fn propose_command_on_backend(
    backend: &dyn DatastoreBackend,
    command: StorageCommand,
) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
    let return_key = command_return_key(&command);
    let operation = klights_replication::proposal::derive_operation_label(&command);
    let commit = backend
        .build_log_apply_commit_for_command(command, operation.as_str(), "test-proposer")
        .await
        .map_err(
            crate::bootstrap::composition_adapters::cluster_store_replication_adapter::map_storage_mutation_error_for_test,
        )?;
    let receipt = backend
        .apply_raft_log_apply_commit_receipt(commit)
        .await
        .context("test committed apply")?;
    let mut result =
        klights_replication::committed_apply::storage_command_result_from_committed_outcome(
            &receipt,
        );
    if result.error_message.is_none()
        && result.applied_mutation.is_none()
        && let Some((api_version, kind, namespace, name)) = return_key
        && let Some(resource) = backend
            .get_resource(&api_version, &kind, namespace.as_deref(), &name)
            .await?
    {
        result.applied_mutation = Some(klights_cluster_store::AppliedMutation::Resource(resource));
    }
    Ok(result)
}

pub(crate) async fn propose_outbox_command_on_backend(
    backend: &dyn DatastoreBackend,
    idempotency_key: &str,
    operation: klights_cluster_core::OutboxOperation,
    command: StorageCommand,
    authoring_node: &str,
    watermark: Option<OutboxStreamWatermark>,
) -> Result<RaftProposalEffect, OutboxApplyError> {
    let return_key = command_return_key(&command);
    let effect = backend
        .apply_outbox_transactionally_with_watermark_effect(
            idempotency_key,
            operation.as_str(),
            command,
            authoring_node,
            watermark,
        )
        .await?;
    let (result, resource_effect, pod_endpoint_effect, mut committed_resource) =
        effect.into_parts();
    if committed_resource.is_none()
        && resource_effect == klights_cluster_core::ResourceMutationEffect::Changed
        && let Some((api_version, kind, namespace, name)) = return_key
    {
        committed_resource = backend
            .get_resource(&api_version, &kind, namespace.as_deref(), &name)
            .await
            .map_err(|error| OutboxApplyError::Retryable(error.to_string()))?;
    }
    Ok(
        RaftProposalEffect::new(result, resource_effect, pod_endpoint_effect)
            .with_committed_resource(committed_resource),
    )
}

fn command_return_key(
    command: &StorageCommand,
) -> Option<(String, String, Option<String>, String)> {
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
        } => Some((
            api_version.clone(),
            kind.clone(),
            namespace.clone(),
            name.clone(),
        )),
        StorageCommand::CreateNamespace { name, .. }
        | StorageCommand::UpdateNamespace { name, .. } => Some((
            "v1".to_string(),
            "Namespace".to_string(),
            None,
            name.clone(),
        )),
        _ => None,
    }
}

#[async_trait]
impl RaftProposal for BackendProposalFixture {
    async fn propose_command(
        &self,
        command: StorageCommand,
    ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
        propose_command_on_backend(self.backend.as_ref(), command).await
    }

    async fn propose_outbox_command(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<OutboxStreamWatermark>,
    ) -> Result<OutboxApplyOutcome, OutboxApplyError> {
        self.propose_outbox_command_effect(
            idempotency_key,
            operation,
            command,
            authoring_node,
            watermark,
        )
        .await
        .map(|effect| effect.into_parts().0)
    }

    async fn propose_outbox_command_effect(
        &self,
        idempotency_key: &str,
        operation: &str,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<OutboxStreamWatermark>,
    ) -> Result<RaftProposalEffect, OutboxApplyError> {
        let operation = klights_cluster_core::OutboxOperation::try_from(operation)
            .map_err(|error| OutboxApplyError::Retryable(error.to_string()))?;
        propose_outbox_command_on_backend(
            self.backend.as_ref(),
            idempotency_key,
            operation,
            command,
            authoring_node,
            watermark,
        )
        .await
    }
}

#[cfg(test)]
mod review_regressions {
    use klights_cluster_core::{
        OutboxApplyError, OutboxOperation, OutboxStreamWatermark, ResourcePreconditions,
        StorageCommand,
    };
    use serde_json::json;

    #[tokio::test]
    async fn watermarked_stale_uid_bound_pod_row_advances_stream_without_side_effect_command() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Namespace",
            None,
            "legacy-rv-seed",
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "legacy-rv-seed"}
            }),
        )
        .await
        .unwrap();
        let rv_before = db.get_current_resource_version().await.unwrap();
        let watch_before = db.current_watch_replay_position().await.unwrap();
        let watermark = OutboxStreamWatermark {
            client_id: "worker-client".to_string(),
            stream_id: 11,
            stream_seq: 1,
        };

        let result = super::propose_outbox_command_on_backend(
            &db,
            "missing-pod-status",
            OutboxOperation::PodStatus,
            StorageCommand::UpdateStatus {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                namespace: Some("default".to_string()),
                name: "already-gone".to_string(),
                status: json!({"phase": "Running"}),
                expected_rv: None,
                preconditions: ResourcePreconditions {
                    uid: Some("gone-uid".to_string()),
                    resource_version: None,
                },
                observed_status_stamp: Some(42),
            },
            "worker-a",
            Some(watermark.clone()),
        )
        .await;
        let Err(error) = result else {
            panic!("missing UID-bound Pod status must return its typed durable decision")
        };
        assert!(
            matches!(&error, OutboxApplyError::NotFound(_)),
            "unexpected stale UID decision: {error:?}"
        );
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap(),
            vec![watermark]
        );
        assert_eq!(db.get_current_resource_version().await.unwrap(), rv_before);
        assert_eq!(
            db.current_watch_replay_position().await.unwrap(),
            watch_before,
            "ledger/watermark-only terminal decisions must not append public watch history"
        );

        super::propose_outbox_command_on_backend(
            &db,
            "next-stream-entry",
            OutboxOperation::PodMetadata,
            StorageCommand::CreateNamespace {
                name: "after-stale-gap".to_string(),
                data: json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": "after-stale-gap"}
                }),
            },
            "worker-a",
            Some(OutboxStreamWatermark {
                client_id: "worker-client".to_string(),
                stream_id: 11,
                stream_seq: 2,
            }),
        )
        .await
        .expect("the next ordered entry must not wedge behind a stale Pod decision");

        assert!(db.get_namespace("after-stale-gap").await.unwrap().is_some());
        assert_eq!(
            db.list_outbox_stream_watermarks().await.unwrap()[0].stream_seq,
            2
        );
    }
}
