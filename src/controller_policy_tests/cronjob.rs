//! Base-owned integration coverage for the private root sequenced-datastore adapter.
//!
//! Pure CronJob policy coverage lives with `klights-controllers::cronjob`.
//! This single test remains here because it composes the root-private
//! `SequencedDatastore` with the replication proposal path; neither API is a
//! feature-crate dependency or a public integration constructor.

use crate::datastore::DatastoreBackend;
use async_trait::async_trait;
use klights_controllers::cronjob::reconcile_cronjob_one_at;
use serde_json::json;
use std::sync::Arc;

async fn make_raft_cronjob_datastore() -> crate::bootstrap::sequenced_datastore::SequencedDatastore
{
    use crate::datastore::backend::DatastoreHandle;
    use klights_cluster_core::StorageCommand;
    use klights_kubelet::node_outbox::payload::OutboxOperation;

    struct InlineProposer {
        inner: DatastoreHandle,
    }

    #[async_trait]
    impl klights_replication::proposal::RaftProposal for InlineProposer {
        async fn propose_command(
            &self,
            command: StorageCommand,
        ) -> anyhow::Result<klights_cluster_store::StorageCommandResult> {
            crate::bootstrap::outbox_apply_adapter::propose_command_on_backend(
                self.inner.as_ref(),
                command,
            )
            .await
            .map_err(|error| anyhow::anyhow!("inline cronjob propose: {error}"))
        }

        async fn propose_outbox_command(
            &self,
            idempotency_key: &str,
            operation: &str,
            command: StorageCommand,
            authoring_node: &str,
            _watermark: Option<klights_cluster_core::OutboxStreamWatermark>,
        ) -> Result<klights_cluster_core::OutboxApplyOutcome, klights_cluster_core::OutboxApplyError>
        {
            let outcome =
                crate::bootstrap::outbox_apply_adapter::propose_outbox_command_on_backend(
                    self.inner.as_ref(),
                    idempotency_key,
                    OutboxOperation::try_from(operation).map_err(|error| {
                        klights_cluster_core::OutboxApplyError::Retryable(error.to_string())
                    })?,
                    command,
                    authoring_node,
                    None,
                )
                .await?;
            Ok(outcome.into_parts().0)
        }
    }

    let inner = crate::datastore::test_support::in_memory().await;
    let handle: DatastoreHandle = Arc::new(inner);
    crate::bootstrap::sequenced_datastore::SequencedDatastore::new(
        handle.clone(),
        Arc::new(InlineProposer { inner: handle }),
    )
}

#[tokio::test]
async fn test_cronjob_reconcile_persists_last_schedule_time_through_raft_status_path() {
    let db = make_raft_cronjob_datastore().await;
    let now = chrono::Utc::now();
    let cronjob = json!({
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {
            "name": "test-cj-raft-status",
            "namespace": "default",
            "uid": "cj-uid-raft-status",
            "creationTimestamp": klights_cluster_core::k8s_time::format_time(
                now - chrono::Duration::minutes(2)
            )
        },
        "spec": {
            "schedule": "* * * * *",
            "concurrencyPolicy": "Allow",
            "jobTemplate": {"spec": {"template": {"spec": {
                "containers": [{"name": "c", "image": "nginx"}],
                "restartPolicy": "Never"
            }}}}
        },
        "status": {}
    });
    let created = db
        .create_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "test-cj-raft-status",
            cronjob.clone(),
        )
        .await
        .unwrap();

    let store: &dyn DatastoreBackend = &db;
    reconcile_cronjob_one_at(store, None, &cronjob, created.resource_version, now)
        .await
        .unwrap();

    let updated = db
        .get_resource(
            "batch/v1",
            "CronJob",
            Some("default"),
            "test-cj-raft-status",
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        updated
            .data
            .pointer("/status/lastScheduleTime")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.is_empty()),
        "raft-routed CronJob reconcile must persist status.lastScheduleTime: {:?}",
        updated.data
    );
}
