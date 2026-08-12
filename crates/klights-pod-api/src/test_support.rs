//! Canonical pure Pod-domain builders shared by integration tests.

use std::sync::Arc;
use std::{future::Future, pin::Pin};

use serde_json::Value;

use klights_cluster_core::Resource;

use klights_types::PodIdentity;

use crate::{
    PodApiCreateRequest, PodApiCreateResult, PodApiMutation, PodApiPatchRequest,
    PodApiUpdateRequest, PodApiWriteOutcome, PodGetRequest, PodLabel, PodListRequest,
    PodListResult, PodMetadataPatchRequest, PodMutationTarget, PodOwnerListRequest,
    PodOwnerReference, PodPersistence, PodPersistenceCreateRequest, PodPersistenceReplaceRequest,
    PodQuery, PodRepositoryError, PodSnapshotListOutcome, PodSnapshotListRequest, PodSnapshotQuery,
    PodUpdate, PodUpdateRequest,
};

#[derive(Clone)]
pub struct PodQueryPorts {
    query: Arc<dyn PodQuery>,
    snapshot: Option<Arc<dyn PodSnapshotQuery>>,
}

impl PodQueryPorts {
    pub fn new(query: Arc<dyn PodQuery>, snapshot: Arc<dyn PodSnapshotQuery>) -> Self {
        Self {
            query,
            snapshot: Some(snapshot),
        }
    }

    pub fn query_only(query: Arc<dyn PodQuery>) -> Self {
        Self {
            query,
            snapshot: None,
        }
    }

    pub fn as_query(&self) -> &dyn PodQuery {
        self.query.as_ref()
    }

    pub fn query_port(&self) -> Arc<dyn PodQuery> {
        self.query.clone()
    }

    pub async fn get_pod(
        &self,
        request: PodGetRequest,
    ) -> Result<Option<Resource>, PodRepositoryError> {
        self.query.get_pod(request).await
    }

    pub async fn get_pod_by_name(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>, PodRepositoryError> {
        self.get_pod(PodGetRequest::try_by_name(namespace, name)?)
            .await
    }

    pub async fn get_pod_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> Result<Option<Resource>, PodRepositoryError> {
        self.get_pod(PodGetRequest::try_by_identity(
            klights_types::PodIdentity::new(namespace, name, uid),
        )?)
        .await
    }

    pub async fn list_pods(
        &self,
        request: PodListRequest,
    ) -> Result<PodListResult, PodRepositoryError> {
        self.query.list_pods(request).await
    }

    pub async fn list_pods_filtered(
        &self,
        namespace: Option<&str>,
        label_selector: Option<&str>,
    ) -> Result<PodListResult, PodRepositoryError> {
        self.list_pods(PodListRequest::try_new(
            namespace.map(str::to_string),
            label_selector.map(str::to_string),
            None,
            None,
            None,
        )?)
        .await
    }

    pub async fn list_pods_exact(
        &self,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> Result<PodListResult, PodRepositoryError> {
        self.list_pods(PodListRequest::try_new(
            namespace.map(str::to_string),
            label_selector.map(str::to_string),
            field_selector.map(str::to_string),
            limit,
            continue_token.map(str::to_string),
        )?)
        .await
    }

    pub async fn list_pods_by_owner_uid(
        &self,
        request: PodOwnerListRequest,
    ) -> Result<Vec<Resource>, PodRepositoryError> {
        self.query.list_pods_by_owner_uid(request).await
    }

    pub async fn list_pods_by_owner(
        &self,
        namespace: &str,
        owner_uid: &str,
    ) -> Result<Vec<Resource>, PodRepositoryError> {
        self.list_pods_by_owner_uid(PodOwnerListRequest::try_new(namespace, owner_uid)?)
            .await
    }

    pub async fn snapshot_pods(
        &self,
        request: PodSnapshotListRequest,
    ) -> Result<PodSnapshotListOutcome, PodRepositoryError> {
        match &self.snapshot {
            Some(snapshot) => snapshot.snapshot_pods(request).await,
            None => Err(PodRepositoryError::unavailable(
                "snapshot query is not part of this focused fixture",
            )),
        }
    }
}

#[derive(Clone)]
pub struct PodUpdatePorts {
    update: Arc<dyn PodUpdate>,
}

pub type PodFixturePersistenceFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Resource, PodRepositoryError>> + Send + 'a>>;

/// Test-only Pod seed capability. Implementations may own a concrete store,
/// but consumers receive no generic datastore or delete surface.
pub trait PodFixturePersistence: Send + Sync {
    fn seed_pod(
        &self,
        namespace: String,
        name: String,
        body: Value,
    ) -> PodFixturePersistenceFuture<'_>;

    fn replace_pod(
        &self,
        namespace: String,
        name: String,
        body: Value,
        expected_resource_version: i64,
    ) -> PodFixturePersistenceFuture<'_>;
}

#[derive(Clone)]
pub struct PodFixturePersistencePorts {
    persistence: Arc<dyn PodFixturePersistence>,
}

impl PodFixturePersistencePorts {
    pub fn new(persistence: Arc<dyn PodFixturePersistence>) -> Self {
        Self { persistence }
    }

    pub async fn seed_pod(
        &self,
        namespace: &str,
        name: &str,
        body: Value,
    ) -> Result<Resource, PodRepositoryError> {
        self.persistence
            .seed_pod(namespace.to_string(), name.to_string(), body)
            .await
    }

    pub async fn replace_pod(
        &self,
        namespace: &str,
        name: &str,
        body: Value,
        expected_resource_version: i64,
    ) -> Result<Resource, PodRepositoryError> {
        self.persistence
            .replace_pod(
                namespace.to_string(),
                name.to_string(),
                body,
                expected_resource_version,
            )
            .await
    }
}

impl PodUpdatePorts {
    pub fn new(update: Arc<dyn PodUpdate>) -> Self {
        Self { update }
    }

    pub async fn update_pod(
        &self,
        request: PodUpdateRequest,
    ) -> Result<Resource, PodRepositoryError> {
        self.update.update_pod(request).await
    }

    /// Replaces owner references for the named Pod without a UID check.
    pub async fn replace_owner_references_by_name(
        &self,
        namespace: &str,
        name: &str,
        owner_references: Vec<Value>,
    ) -> Result<Resource, PodRepositoryError> {
        self.update_pod(PodUpdateRequest::replace_owner_references(
            PodMutationTarget::try_by_name(namespace, name)?,
            owner_references_from_values(owner_references)?,
        ))
        .await
    }

    /// Replaces owner references for a UID-qualified Pod.
    pub async fn replace_owner_references_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        owner_references: Vec<Value>,
    ) -> Result<Resource, PodRepositoryError> {
        self.update_pod(PodUpdateRequest::replace_owner_references(
            PodMutationTarget::try_by_identity(PodIdentity::new(namespace, name, uid))?,
            owner_references_from_values(owner_references)?,
        ))
        .await
    }

    /// Merges labels for the named Pod without a UID check.
    pub async fn merge_labels_by_name(
        &self,
        namespace: &str,
        name: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource, PodRepositoryError> {
        self.update_pod(PodUpdateRequest::merge_labels(
            PodMutationTarget::try_by_name(namespace, name)?,
            labels_from_pairs(labels)?,
        ))
        .await
    }

    /// Merges labels for a UID-qualified Pod.
    pub async fn merge_labels_for_uid(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
        labels: Vec<(String, String)>,
    ) -> Result<Resource, PodRepositoryError> {
        self.update_pod(PodUpdateRequest::merge_labels(
            PodMutationTarget::try_by_identity(PodIdentity::new(namespace, name, uid))?,
            labels_from_pairs(labels)?,
        ))
        .await
    }
}

fn labels_from_pairs(labels: Vec<(String, String)>) -> Result<Vec<PodLabel>, PodRepositoryError> {
    labels
        .into_iter()
        .map(|(key, value)| PodLabel::try_new(key, value))
        .collect()
}

/// Focused Pod persistence capability for tests. The concrete datastore and
/// generic resource operations remain unreachable behind the owning port.
#[derive(Clone)]
pub struct PodPersistencePorts {
    persistence: Arc<dyn PodPersistence>,
}

impl PodPersistencePorts {
    pub fn new(persistence: Arc<dyn PodPersistence>) -> Self {
        Self { persistence }
    }

    pub async fn create_pod(
        &self,
        request: PodPersistenceCreateRequest,
    ) -> Result<Resource, PodRepositoryError> {
        self.persistence.create_pod(request).await
    }

    pub async fn replace_pod(
        &self,
        request: PodPersistenceReplaceRequest,
    ) -> Result<Resource, PodRepositoryError> {
        self.persistence.replace_pod(request).await
    }

    pub async fn replace_pod_including_status(
        &self,
        request: PodPersistenceReplaceRequest,
    ) -> Result<Resource, PodRepositoryError> {
        self.persistence.replace_pod_including_status(request).await
    }

    pub async fn patch_pod_metadata(
        &self,
        request: PodMetadataPatchRequest,
    ) -> Result<Resource, PodRepositoryError> {
        self.persistence.patch_pod_metadata(request).await
    }
}

/// API-facing Pod mutation methods owned by the Pod API contract. Actor
/// deletion and status/network methods intentionally remain separate.
#[derive(Clone)]
pub struct PodApiMutationPorts {
    mutations: Arc<dyn PodApiMutation>,
}

impl PodApiMutationPorts {
    pub fn new(mutations: Arc<dyn PodApiMutation>) -> Self {
        Self { mutations }
    }

    pub async fn create(
        &self,
        request: PodApiCreateRequest,
    ) -> Result<PodApiCreateResult, PodRepositoryError> {
        self.mutations.create_pod(request).await
    }

    /// Creates a Pod and requires the API to have actually persisted it
    /// (rather than dry-run) — the shape every controller-owned Pod fixture
    /// needs, with an error message that names the target Pod.
    pub async fn create_or_require_resource(
        &self,
        namespace: &str,
        name: &str,
        body: Value,
    ) -> Result<Resource, PodRepositoryError> {
        self.create(PodApiCreateRequest {
            namespace: namespace.to_string(),
            body,
            dry_run: false,
        })
        .await?
        .resource
        .ok_or_else(|| {
            PodRepositoryError::invalid_request(
                "pod",
                format!("controller Pod {namespace}/{name} unexpectedly dry-ran"),
            )
        })
    }

    pub async fn update(
        &self,
        request: PodApiUpdateRequest,
    ) -> Result<PodApiWriteOutcome, PodRepositoryError> {
        self.mutations.update_pod(request).await
    }

    pub async fn update_pod(
        &self,
        namespace: &str,
        name: &str,
        body: serde_json::Value,
        current: Resource,
        dry_run: bool,
    ) -> Result<PodApiWriteOutcome, PodRepositoryError> {
        self.update(PodApiUpdateRequest {
            namespace: namespace.to_string(),
            name: name.to_string(),
            body,
            current,
            dry_run,
        })
        .await
    }

    pub async fn patch(
        &self,
        request: PodApiPatchRequest,
    ) -> Result<PodApiWriteOutcome, PodRepositoryError> {
        self.mutations.patch_pod(request).await
    }

    pub async fn patch_pod(
        &self,
        namespace: &str,
        name: &str,
        patch: serde_json::Value,
        patch_kind: crate::PodStatusPatchKind,
        dry_run: bool,
    ) -> Result<PodApiWriteOutcome, PodRepositoryError> {
        self.patch(PodApiPatchRequest {
            namespace: namespace.to_string(),
            name: name.to_string(),
            patch,
            patch_kind,
            dry_run,
        })
        .await
    }
}

pub fn owner_references_from_values(
    values: Vec<Value>,
) -> Result<Vec<PodOwnerReference>, PodRepositoryError> {
    values
        .into_iter()
        .map(|value| {
            let required = |field: &'static str| {
                value.get(field).and_then(Value::as_str).ok_or_else(|| {
                    PodRepositoryError::invalid_request(
                        "owner_reference",
                        format!("missing {field}"),
                    )
                })
            };
            PodOwnerReference::try_new(
                required("apiVersion")?,
                required("kind")?,
                required("name")?,
                required("uid")?,
                value.get("controller").and_then(Value::as_bool),
                value.get("blockOwnerDeletion").and_then(Value::as_bool),
            )
        })
        .collect()
}
