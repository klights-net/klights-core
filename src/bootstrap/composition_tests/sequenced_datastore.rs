use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use axum::response::IntoResponse;
use klights_cluster_core::{
    OutboxApplyError, OutboxApplyOutcome, OutboxStreamWatermark, StorageCommand,
    StorageCommandRejectionCode, StorageMutationError,
};
use klights_cluster_store::StorageCommandResult;
use klights_replication::proposal::RaftProposal;

use crate::bootstrap::sequenced_datastore::SequencedDatastore;
use crate::datastore::DatastoreBackend;

struct RejectingProposal(StorageCommandRejectionCode);

#[async_trait]
impl RaftProposal for RejectingProposal {
    async fn propose_command(&self, _command: StorageCommand) -> Result<StorageCommandResult> {
        let message = match self.0 {
            StorageCommandRejectionCode::AlreadyExists => "Resource already exists (409 Conflict)",
            StorageCommandRejectionCode::Conflict => {
                "resourceVersion precondition failed: expected 7 got 8 (409 Conflict)"
            }
            StorageCommandRejectionCode::NotFound => "resource not found",
            StorageCommandRejectionCode::InvalidCommit => "invalid commit",
        };
        Err(StorageMutationError::rejected(self.0, message).into())
    }

    async fn propose_outbox_command(
        &self,
        _idempotency_key: &str,
        _operation: &str,
        _command: StorageCommand,
        _authoring_node: &str,
        _watermark: Option<OutboxStreamWatermark>,
    ) -> std::result::Result<OutboxApplyOutcome, OutboxApplyError> {
        unreachable!("resource command regression does not submit outbox work")
    }
}

async fn rejecting_datastore(code: StorageCommandRejectionCode) -> SequencedDatastore {
    let passive: Arc<dyn DatastoreBackend> = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .expect("in-memory passive datastore"),
    );
    SequencedDatastore::new_with_clock(
        passive,
        Arc::new(RejectingProposal(code)),
        Arc::new(klights_supervisor::SystemWallClock),
        crate::control_plane::client::local::always_leader_watch(),
    )
}

#[tokio::test]
async fn sequenced_backend_preserves_typed_resource_command_rejections() {
    for (code, expected) in [
        (StorageCommandRejectionCode::AlreadyExists, "AlreadyExists"),
        (StorageCommandRejectionCode::Conflict, "Conflict"),
    ] {
        let datastore = rejecting_datastore(code).await;
        let error = DatastoreBackend::create_resource(
            &datastore,
            "v1",
            "ConfigMap",
            Some("default"),
            "settings",
            serde_json::json!({"metadata": {"name": "settings"}}),
        )
        .await
        .expect_err("proposal rejection must reach the compatibility caller");
        let datastore_error = error
            .downcast_ref::<klights_cluster_datastore::errors::DatastoreError>()
            .unwrap_or_else(|| {
                panic!("{expected} rejection lost its typed datastore identity: {error:#}")
            });
        assert!(
            matches!(
                (expected, datastore_error),
                (
                    "AlreadyExists",
                    klights_cluster_datastore::errors::DatastoreError::AlreadyExists { .. }
                ) | (
                    "Conflict",
                    klights_cluster_datastore::errors::DatastoreError::Conflict { .. }
                )
            ),
            "unexpected typed rejection for {expected}: {datastore_error:?}"
        );
        assert_eq!(
            k8s_native_service::AppError::from(error)
                .into_response()
                .status(),
            axum::http::StatusCode::CONFLICT,
            "{expected} must remain Kubernetes HTTP 409 rather than becoming retryable 503"
        );
    }
}

#[tokio::test]
async fn sequenced_backend_delegates_durable_positioned_watch_contract() {
    let passive: Arc<dyn DatastoreBackend> = Arc::new(
        crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .expect("in-memory passive datastore"),
    );
    let datastore = SequencedDatastore::new_with_clock(
        passive,
        Arc::new(RejectingProposal(StorageCommandRejectionCode::Conflict)),
        Arc::new(klights_supervisor::SystemWallClock),
        crate::control_plane::client::local::always_leader_watch(),
    );

    let position = DatastoreBackend::current_watch_replay_position(&datastore)
        .await
        .expect("sequenced compatibility surface must expose the passive durable anchor");
    let targets = [
        crate::datastore::types::WatchTarget::namespaced_in_namespace(
            "stable.example.com/v1",
            "SelectableFieldCRD",
            "default",
        ),
    ];
    DatastoreBackend::snapshot_resources_at_position(&datastore, &targets, None, None, position)
        .await
        .expect("sequenced compatibility surface must expose positioned snapshots");
    let limit = std::num::NonZeroUsize::new(16).unwrap();
    DatastoreBackend::list_watch_events_after_position_checked_bounded(
        &datastore, &targets, position, limit,
    )
    .await
    .expect("sequenced compatibility surface must expose decoded positioned replay");
    DatastoreBackend::list_raw_watch_events_after_position_checked_bounded(
        &datastore, &targets, position, limit,
    )
    .await
    .expect("sequenced compatibility surface must expose raw positioned replay");
}
