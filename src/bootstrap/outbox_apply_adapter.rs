//! Test-only consensus fixture for callers that do not need a live OpenRaft node.
//!
//! This fixture deliberately owns no command decoding, CAS, dedupe, apply, or
//! side-effect policy. It implements the replication-owned proposal port by
//! delegating materialization and committed apply to the datastore's existing
//! production boundaries.

use std::sync::Arc;

use async_trait::async_trait;
use klights_cluster_core::{
    BuildOutboxOutcome, OutboxApplyError, OutboxApplyOutcome, OutboxStreamWatermark, StorageCommand,
};
use klights_cluster_store::{AppliedOutboxLedger, ClusterResourceRead, ResourceGetRequest};
use klights_replication::proposal::{RaftProposal, RaftProposalEffect};

const TEST_RESOURCE_COMMAND_OPERATION: &str = "test-resource-command";

pub(crate) struct BackendProposalFixture {
    outbox_ledger: Arc<dyn AppliedOutboxLedger>,
    /// Test composition keeps the canonical concrete store solely for the
    /// existing post-commit publication boundary.  The focused privileged
    /// apply port deliberately returns only its receipt, while this fixture
    /// also has to drive the root-owned watch sink after durable commit.
    canonical: Arc<klights_cluster_datastore::sqlite::embedded::Datastore>,
    resource_reads: Arc<dyn ClusterResourceRead>,
}

impl BackendProposalFixture {
    pub(crate) fn new(
        outbox_ledger: Arc<dyn AppliedOutboxLedger>,
        canonical: Arc<klights_cluster_datastore::sqlite::embedded::Datastore>,
        resource_reads: Arc<dyn ClusterResourceRead>,
    ) -> Self {
        Self {
            outbox_ledger,
            canonical,
            resource_reads,
        }
    }

    async fn apply_command(
        &self,
        command: StorageCommand,
    ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
        let return_key = command_return_key(&command);
        let commit = self
            .outbox_ledger
            .build_log_apply_commit_for_command(
                command,
                TEST_RESOURCE_COMMAND_OPERATION,
                "test-proposer",
            )
            .await
            .map_err(|error| crate::bootstrap::composition_adapters::cluster_store_replication_adapter::map_storage_mutation_error_for_test(anyhow::Error::new(error)))?;
        let receipt = self
            .canonical
            .apply_raft_log_apply_commit_receipt(commit)
            .await
            .map_err(|error| anyhow::anyhow!("test committed apply: {error:#}"))?;
        let mut result =
            klights_replication::committed_apply::storage_command_result_from_committed_outcome(
                &receipt,
            );
        if result.error_message.is_none()
            && result.applied_mutation.is_none()
            && let Some((api_version, kind, namespace, name)) = return_key
            && let Some(resource) = self
                .resource_reads
                .get_resource(ResourceGetRequest::new(api_version, kind, namespace, name))
                .await?
        {
            result.applied_mutation =
                Some(klights_cluster_store::AppliedMutation::Resource(resource));
        }
        Ok(result)
    }

    async fn apply_outbox_effect(
        &self,
        idempotency_key: &str,
        operation: klights_cluster_core::OutboxOperation,
        command: StorageCommand,
        authoring_node: &str,
        watermark: Option<OutboxStreamWatermark>,
    ) -> Result<RaftProposalEffect, OutboxApplyError> {
        let return_key = command_return_key(&command);
        let effect = self
            .outbox_ledger
            .build_log_apply_commit_for_outbox_with_watermark(
                idempotency_key,
                operation.as_str(),
                command,
                authoring_node,
                watermark,
            )
            .await?;
        match effect {
            BuildOutboxOutcome::NeedsPropose {
                commit,
                applied_rv,
                terminal_error,
            } => {
                let receipt = self
                    .canonical
                    .apply_raft_log_apply_commit_receipt(commit)
                    .await
                    .map_err(|error| OutboxApplyError::Retryable(error.to_string()))?;
                if let Some(error) = terminal_error {
                    return Err(error);
                }
                if let Some(message) = receipt.terminal_rejection() {
                    return Err(OutboxApplyError::ConflictTerminal(message.to_string()));
                }
                let mut committed_resource = receipt.applied_resource().cloned();
                let resource_effect = if matches!(
                    receipt.outcome(),
                    klights_cluster_core::CommittedApplyOutcome::Visible { .. }
                ) {
                    klights_cluster_core::ResourceMutationEffect::Changed
                } else {
                    klights_cluster_core::ResourceMutationEffect::Unchanged
                };
                if committed_resource.is_none()
                    && resource_effect == klights_cluster_core::ResourceMutationEffect::Changed
                    && let Some((api_version, kind, namespace, name)) = return_key
                {
                    committed_resource = self
                        .resource_reads
                        .get_resource(ResourceGetRequest::new(api_version, kind, namespace, name))
                        .await
                        .map_err(|error| OutboxApplyError::Retryable(error.to_string()))?;
                }
                Ok(RaftProposalEffect::new(
                    OutboxApplyOutcome::Applied {
                        applied_rv: receipt.applied_resource_version().unwrap_or(applied_rv),
                    },
                    resource_effect,
                    receipt.pod_endpoint_effect(),
                )
                .with_committed_resource(committed_resource))
            }
            BuildOutboxOutcome::AlreadyApplied {
                applied_rv,
                committed_resource,
            } => Ok(RaftProposalEffect::new(
                OutboxApplyOutcome::AlreadyApplied { applied_rv },
                klights_cluster_core::ResourceMutationEffect::Unchanged,
                klights_cluster_core::PodEndpointEffect::Unchanged,
            )
            .with_committed_resource(committed_resource)),
            BuildOutboxOutcome::LeaseRenewShortcircuit => Ok(RaftProposalEffect::new(
                OutboxApplyOutcome::Applied { applied_rv: 0 },
                klights_cluster_core::ResourceMutationEffect::Unchanged,
                klights_cluster_core::PodEndpointEffect::NotApplicable,
            )),
        }
    }
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
        self.apply_command(command).await
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
        self.apply_outbox_effect(
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
    use std::sync::Arc;

    use klights_cluster_core::{
        OutboxApplyError, OutboxOperation, OutboxStreamWatermark, ResourcePreconditions,
        StorageCommand,
    };
    use klights_replication::proposal::RaftProposal;
    use serde_json::json;

    #[tokio::test]
    async fn watermarked_stale_uid_bound_pod_row_advances_stream_without_side_effect_command() {
        let db = crate::bootstrap::cluster_store::selector::canonical_sqlite_fixture()
            .await
            .unwrap();
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

        let canonical = db.clone();
        let fixture = super::BackendProposalFixture::new(
            Arc::new(canonical.clone()),
            Arc::new(canonical.clone()),
            canonical.focused_read_store(),
        );
        let result = fixture
            .propose_outbox_command_effect(
                "missing-pod-status",
                OutboxOperation::PodStatus.as_str(),
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

        fixture
            .propose_outbox_command_effect(
                "next-stream-entry",
                OutboxOperation::PodMetadata.as_str(),
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
