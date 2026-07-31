//! Kubelet-owned ordinary Pod repository service.
//!
//! Query, metadata update, graceful termination marking, and lifecycle wakeup
//! routing are deliberately expressed over focused ports. Neither this module
//! nor its public surface can remove a Pod row: bound-Pod actor finalization
//! and the leader-only unscheduled-Pod CAS remain separate capabilities.

use std::sync::Arc;

use klights_cluster_core::Resource;
use klights_pod_api::{
    PodGetRequest, PodLifecycleFuture, PodLifecycleWakeup, PodLifecycleWakeupRequest,
    PodListRequest, PodListResult, PodMarkTerminating, PodMarkTerminatingRequest,
    PodMutationTarget, PodOwnerListRequest, PodQuery, PodRepositoryFuture, PodUpdate,
    PodUpdateOperation, PodUpdateRequest,
};
use serde_json::{Map, Value};

/// Raw repository LIST result before the public Pod API contract validates
/// snapshot and pagination metadata.
#[derive(Clone, Debug)]
pub struct PodRepositoryList {
    items: Vec<Resource>,
    resource_version: i64,
    continue_token: Option<String>,
    remaining_item_count: Option<i64>,
}

impl PodRepositoryList {
    pub fn new(
        items: Vec<Resource>,
        resource_version: i64,
        continue_token: Option<String>,
        remaining_item_count: Option<i64>,
    ) -> Self {
        Self {
            items,
            resource_version,
            continue_token,
            remaining_item_count,
        }
    }

    fn into_parts(self) -> (Vec<Resource>, i64, Option<String>, Option<i64>) {
        (
            self.items,
            self.resource_version,
            self.continue_token,
            self.remaining_item_count,
        )
    }
}

/// Focused lower query port needed by [`PodRepositoryService`].
pub trait PodQueryPort: Send + Sync {
    fn read_pod<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> PodRepositoryFuture<'a, Option<Resource>>;

    fn read_pod_for_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        uid: &'a str,
    ) -> PodRepositoryFuture<'a, Option<Resource>>;

    fn list_pod_page<'a>(
        &'a self,
        request: &'a PodListRequest,
    ) -> PodRepositoryFuture<'a, PodRepositoryList>;

    fn list_pods_by_owner_uid<'a>(
        &'a self,
        namespace: &'a str,
        owner_uid: &'a str,
    ) -> PodRepositoryFuture<'a, Vec<Resource>>;
}

/// Focused lower metadata-update port. UID-qualified operations are separate
/// methods so a same-name replacement cannot be reached through a name-only
/// default implementation.
pub trait PodUpdatePort: Send + Sync {
    fn merge_pod_labels<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        labels: Vec<(String, String)>,
    ) -> PodRepositoryFuture<'a, Resource>;

    fn merge_pod_labels_for_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        uid: &'a str,
        labels: Vec<(String, String)>,
    ) -> PodRepositoryFuture<'a, Resource>;

    fn replace_pod_owner_references<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        owner_references: Vec<Value>,
    ) -> PodRepositoryFuture<'a, Resource>;

    fn replace_pod_owner_references_for_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        uid: &'a str,
        owner_references: Vec<Value>,
    ) -> PodRepositoryFuture<'a, Resource>;

    fn record_pod_sandbox_id<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        sandbox_id: String,
    ) -> PodRepositoryFuture<'a, Resource>;

    fn record_pod_sandbox_id_for_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        uid: &'a str,
        sandbox_id: String,
    ) -> PodRepositoryFuture<'a, Resource>;
}

/// Focused graceful-termination marker. This intent can update a Pod and
/// enqueue work, but it can never finalize or remove a Pod row.
pub trait PodTerminationPort: Send + Sync {
    fn mark_terminating(&self, target: PodMutationTarget) -> PodRepositoryFuture<'_, Resource>;
}

/// Ordinary Pod service composed from independently focused lower ports.
pub struct PodRepositoryService {
    query: Arc<dyn PodQueryPort>,
    update: Arc<dyn PodUpdatePort>,
    termination: Arc<dyn PodTerminationPort>,
}

impl PodRepositoryService {
    pub fn new(
        query: Arc<dyn PodQueryPort>,
        update: Arc<dyn PodUpdatePort>,
        termination: Arc<dyn PodTerminationPort>,
    ) -> Self {
        Self {
            query,
            update,
            termination,
        }
    }

    pub fn get_pod_from<'a, Query>(
        query: &'a Query,
        request: PodGetRequest,
    ) -> PodRepositoryFuture<'a, Option<Resource>>
    where
        Query: PodQueryPort + ?Sized,
    {
        Box::pin(async move {
            match request.uid() {
                Some(uid) => {
                    query
                        .read_pod_for_uid(request.namespace(), request.name(), uid)
                        .await
                }
                None => query.read_pod(request.namespace(), request.name()).await,
            }
        })
    }

    pub fn list_pods_from<'a, Query>(
        query: &'a Query,
        request: PodListRequest,
    ) -> PodRepositoryFuture<'a, PodListResult>
    where
        Query: PodQueryPort + ?Sized,
    {
        Box::pin(async move {
            let (items, resource_version, continue_token, remaining_item_count) =
                query.list_pod_page(&request).await?.into_parts();
            PodListResult::try_new(
                items,
                resource_version,
                continue_token,
                remaining_item_count,
            )
        })
    }

    pub fn list_pods_by_owner_uid_from<'a, Query>(
        query: &'a Query,
        request: PodOwnerListRequest,
    ) -> PodRepositoryFuture<'a, Vec<Resource>>
    where
        Query: PodQueryPort + ?Sized,
    {
        Box::pin(async move {
            query
                .list_pods_by_owner_uid(request.namespace(), request.owner_uid())
                .await
        })
    }

    pub fn update_pod_from<'a, Update>(
        update: &'a Update,
        request: PodUpdateRequest,
    ) -> PodRepositoryFuture<'a, Resource>
    where
        Update: PodUpdatePort + ?Sized,
    {
        Box::pin(async move {
            let (target, operation) = request.into_parts();
            match operation {
                PodUpdateOperation::MergeLabels(labels) => {
                    let labels = labels.into_iter().map(|label| label.into_parts()).collect();
                    match target.uid() {
                        Some(uid) => {
                            update
                                .merge_pod_labels_for_uid(
                                    target.namespace(),
                                    target.name(),
                                    uid,
                                    labels,
                                )
                                .await
                        }
                        None => {
                            update
                                .merge_pod_labels(target.namespace(), target.name(), labels)
                                .await
                        }
                    }
                }
                PodUpdateOperation::ReplaceOwnerReferences(owner_references) => {
                    let owner_references = owner_references
                        .into_iter()
                        .map(owner_reference_value)
                        .collect();
                    match target.uid() {
                        Some(uid) => {
                            update
                                .replace_pod_owner_references_for_uid(
                                    target.namespace(),
                                    target.name(),
                                    uid,
                                    owner_references,
                                )
                                .await
                        }
                        None => {
                            update
                                .replace_pod_owner_references(
                                    target.namespace(),
                                    target.name(),
                                    owner_references,
                                )
                                .await
                        }
                    }
                }
                PodUpdateOperation::RecordSandboxId(sandbox_id) => match target.uid() {
                    Some(uid) => {
                        update
                            .record_pod_sandbox_id_for_uid(
                                target.namespace(),
                                target.name(),
                                uid,
                                sandbox_id,
                            )
                            .await
                    }
                    None => {
                        update
                            .record_pod_sandbox_id(target.namespace(), target.name(), sandbox_id)
                            .await
                    }
                },
            }
        })
    }

    pub fn mark_pod_terminating_from<'a, Termination>(
        termination: &'a Termination,
        request: PodMarkTerminatingRequest,
    ) -> PodRepositoryFuture<'a, Resource>
    where
        Termination: PodTerminationPort + ?Sized,
    {
        termination.mark_terminating(request.into_target())
    }
}

impl PodQuery for PodRepositoryService {
    fn get_pod(&self, request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
        Self::get_pod_from(self.query.as_ref(), request)
    }

    fn list_pods(&self, request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
        Self::list_pods_from(self.query.as_ref(), request)
    }

    fn list_pods_by_owner_uid(
        &self,
        request: PodOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<Resource>> {
        Self::list_pods_by_owner_uid_from(self.query.as_ref(), request)
    }
}

impl PodUpdate for PodRepositoryService {
    fn update_pod(&self, request: PodUpdateRequest) -> PodRepositoryFuture<'_, Resource> {
        Self::update_pod_from(self.update.as_ref(), request)
    }
}

impl PodMarkTerminating for PodRepositoryService {
    fn mark_pod_terminating(
        &self,
        request: PodMarkTerminatingRequest,
    ) -> PodRepositoryFuture<'_, Resource> {
        Self::mark_pod_terminating_from(self.termination.as_ref(), request)
    }
}

fn owner_reference_value(owner: klights_pod_api::PodOwnerReference) -> Value {
    let (api_version, kind, name, uid, controller, block_owner_deletion) = owner.into_parts();
    let mut value = Map::new();
    value.insert("apiVersion".to_string(), Value::String(api_version));
    value.insert("kind".to_string(), Value::String(kind));
    value.insert("name".to_string(), Value::String(name));
    value.insert("uid".to_string(), Value::String(uid));
    if let Some(controller) = controller {
        value.insert("controller".to_string(), Value::Bool(controller));
    }
    if let Some(block_owner_deletion) = block_owner_deletion {
        value.insert(
            "blockOwnerDeletion".to_string(),
            Value::Bool(block_owner_deletion),
        );
    }
    Value::Object(value)
}

/// Transport-neutral UID-qualified route request delivered to the root actor
/// adapter. Root alone translates this value into private lifecycle messages.
#[derive(Clone, Debug)]
pub struct PodLifecycleRouteRequest {
    identity: klights_types::PodIdentity,
    resource_version: i64,
    pod: Resource,
}

impl PodLifecycleRouteRequest {
    pub fn identity(&self) -> &klights_types::PodIdentity {
        &self.identity
    }

    pub fn resource_version(&self) -> i64 {
        self.resource_version
    }

    pub fn pod(&self) -> &Resource {
        &self.pod
    }

    pub fn into_parts(self) -> (klights_types::PodIdentity, i64, Resource) {
        (self.identity, self.resource_version, self.pod)
    }
}

pub trait PodLifecycleRouteSink: Send + Sync {
    fn route_pod_lifecycle(&self, request: PodLifecycleRouteRequest) -> PodLifecycleFuture<'_>;
}

pub struct PodLifecycleWakeupService {
    route: Arc<dyn PodLifecycleRouteSink>,
}

impl PodLifecycleWakeupService {
    pub fn new(route: Arc<dyn PodLifecycleRouteSink>) -> Self {
        Self { route }
    }
}

impl PodLifecycleWakeup for PodLifecycleWakeupService {
    fn wake_pod_lifecycle(&self, request: PodLifecycleWakeupRequest) -> PodLifecycleFuture<'_> {
        let (identity, resource_version, pod) = request.into_parts();
        self.route.route_pod_lifecycle(PodLifecycleRouteRequest {
            identity,
            resource_version,
            pod,
        })
    }
}
