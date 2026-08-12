#![cfg(feature = "test-support")]

use std::sync::Arc;

use klights_pod_api::test_support::{
    PodApiMutationPorts, PodFixturePersistence, PodFixturePersistenceFuture,
    PodFixturePersistencePorts, PodPersistencePorts, PodQueryPorts, PodUpdatePorts,
    owner_references_from_values,
};
use klights_pod_api::{
    PodApiCreateRequest, PodApiCreateResult, PodApiDeleteCollectionRequest, PodApiDeleteOutcome,
    PodApiDeleteRequest, PodApiMutation, PodApiPatchRequest, PodApiUpdateRequest,
    PodApiWriteOutcome, PodBindingRequest, PodGetRequest, PodListRequest, PodListResult,
    PodMetadataPatchRequest, PodOwnerListRequest, PodPersistence, PodPersistenceCreateRequest,
    PodPersistenceReplaceRequest, PodQuery, PodRepositoryError, PodRepositoryFuture,
    PodSnapshotListOutcome, PodSnapshotListRequest, PodSnapshotQuery, PodUpdate, PodUpdateRequest,
};
use serde_json::json;

struct RejectingPodPersistence;

impl PodQuery for RejectingPodPersistence {
    fn get_pod(
        &self,
        _request: PodGetRequest,
    ) -> PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async { Err(PodRepositoryError::unavailable("query rejected")) })
    }

    fn list_pods(&self, _request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
        Box::pin(async { Err(PodRepositoryError::unavailable("list rejected")) })
    }

    fn list_pods_by_owner_uid(
        &self,
        _request: PodOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
        Box::pin(async { Err(PodRepositoryError::unavailable("owner list rejected")) })
    }
}

impl PodSnapshotQuery for RejectingPodPersistence {
    fn snapshot_pods(
        &self,
        _request: PodSnapshotListRequest,
    ) -> PodRepositoryFuture<'_, PodSnapshotListOutcome> {
        Box::pin(async { Err(PodRepositoryError::unavailable("snapshot rejected")) })
    }
}

impl PodUpdate for RejectingPodPersistence {
    fn update_pod(
        &self,
        _request: PodUpdateRequest,
    ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async { Err(PodRepositoryError::unavailable("update rejected")) })
    }
}

impl PodPersistence for RejectingPodPersistence {
    fn create_pod(
        &self,
        _request: PodPersistenceCreateRequest,
    ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async { Err(PodRepositoryError::conflict("create conflict")) })
    }

    fn replace_pod(
        &self,
        _request: PodPersistenceReplaceRequest,
    ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async { Err(PodRepositoryError::conflict("replace conflict")) })
    }

    fn replace_pod_including_status(
        &self,
        _request: PodPersistenceReplaceRequest,
    ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async { Err(PodRepositoryError::conflict("replace status conflict")) })
    }

    fn patch_pod_metadata(
        &self,
        _request: PodMetadataPatchRequest,
    ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async { Err(PodRepositoryError::conflict("metadata conflict")) })
    }
}

impl PodApiMutation for RejectingPodPersistence {
    fn create_pod(
        &self,
        _request: PodApiCreateRequest,
    ) -> PodRepositoryFuture<'_, PodApiCreateResult> {
        Box::pin(async { Err(PodRepositoryError::unavailable("api create rejected")) })
    }

    fn update_pod(
        &self,
        _request: PodApiUpdateRequest,
    ) -> PodRepositoryFuture<'_, PodApiWriteOutcome> {
        Box::pin(async { Err(PodRepositoryError::unavailable("api update rejected")) })
    }

    fn patch_pod(
        &self,
        _request: PodApiPatchRequest,
    ) -> PodRepositoryFuture<'_, PodApiWriteOutcome> {
        Box::pin(async { Err(PodRepositoryError::unavailable("api patch rejected")) })
    }

    fn delete_pod(
        &self,
        _request: PodApiDeleteRequest,
    ) -> PodRepositoryFuture<'_, PodApiDeleteOutcome> {
        Box::pin(async { Err(PodRepositoryError::unavailable("api delete rejected")) })
    }

    fn delete_collection_pods(
        &self,
        _request: PodApiDeleteCollectionRequest,
    ) -> PodRepositoryFuture<'_, ()> {
        Box::pin(async { Err(PodRepositoryError::unavailable("api collection rejected")) })
    }

    fn bind_pod(&self, _request: PodBindingRequest) -> PodRepositoryFuture<'_, ()> {
        Box::pin(async { Err(PodRepositoryError::unavailable("api bind rejected")) })
    }
}

impl PodFixturePersistence for RejectingPodPersistence {
    fn seed_pod(
        &self,
        _namespace: String,
        _name: String,
        _body: serde_json::Value,
    ) -> PodFixturePersistenceFuture<'_> {
        Box::pin(async { Err(PodRepositoryError::unavailable("seed rejected")) })
    }

    fn replace_pod(
        &self,
        _namespace: String,
        _name: String,
        _body: serde_json::Value,
        _expected_resource_version: i64,
    ) -> PodFixturePersistenceFuture<'_> {
        Box::pin(async { Err(PodRepositoryError::conflict("replace rejected")) })
    }
}

#[test]
fn owner_reference_values_preserve_validated_identity_and_flags() {
    let references = owner_references_from_values(vec![json!({
        "apiVersion": "apps/v1",
        "kind": "ReplicaSet",
        "name": "owner",
        "uid": "uid-1",
        "controller": true,
        "blockOwnerDeletion": false
    })])
    .expect("valid owner reference");

    assert_eq!(references.len(), 1);
    assert_eq!(references[0].api_version(), "apps/v1");
    assert_eq!(references[0].kind(), "ReplicaSet");
    assert_eq!(references[0].name(), "owner");
    assert_eq!(references[0].uid(), "uid-1");
    assert_eq!(references[0].controller(), Some(true));
    assert_eq!(references[0].block_owner_deletion(), Some(false));
}

#[test]
fn owner_reference_values_reject_missing_required_identity() {
    let error = owner_references_from_values(vec![json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "name": "owner"
    })])
    .expect_err("missing uid must fail closed");

    assert!(error.to_string().contains("missing uid"));
}

#[tokio::test]
async fn focused_query_and_update_ports_preserve_owner_errors() {
    let persistence = Arc::new(RejectingPodPersistence);
    let query = PodQueryPorts::new(persistence.clone(), persistence.clone());
    let update = PodUpdatePorts::new(persistence);

    let query_error = query
        .get_pod(PodGetRequest::try_by_name("default", "pod").unwrap())
        .await
        .expect_err("query error must propagate");
    assert!(query_error.to_string().contains("query rejected"));

    let update_error = update
        .update_pod(PodUpdateRequest::merge_labels(
            klights_pod_api::PodMutationTarget::try_by_identity(klights_types::PodIdentity::new(
                "default", "pod", "uid",
            ))
            .unwrap(),
            vec![],
        ))
        .await
        .expect_err("update error must propagate");
    assert!(update_error.to_string().contains("update rejected"));
}

#[tokio::test]
async fn focused_persistence_ports_preserve_cas_conflicts() {
    let ports = PodPersistencePorts::new(Arc::new(RejectingPodPersistence));
    let error = ports
        .replace_pod(PodPersistenceReplaceRequest {
            namespace: "default".into(),
            name: "demo".into(),
            body: serde_json::json!({}),
            expected_resource_version: 41,
        })
        .await
        .expect_err("owner conflict must remain visible");
    assert!(matches!(error, PodRepositoryError::Conflict { .. }));
}

#[tokio::test]
async fn focused_api_mutation_ports_preserve_owner_errors() {
    let ports = PodApiMutationPorts::new(Arc::new(RejectingPodPersistence));
    let error = ports
        .create(PodApiCreateRequest {
            namespace: "default".into(),
            body: serde_json::json!({}),
            dry_run: false,
        })
        .await
        .expect_err("owner error must remain visible");
    assert!(error.to_string().contains("api create rejected"));
}

#[tokio::test]
async fn focused_fixture_persistence_hides_store_and_preserves_errors() {
    let ports = PodFixturePersistencePorts::new(Arc::new(RejectingPodPersistence));
    let error = ports
        .seed_pod("default", "demo", serde_json::json!({}))
        .await
        .expect_err("owner error must remain visible");
    assert!(error.to_string().contains("seed rejected"));
}
