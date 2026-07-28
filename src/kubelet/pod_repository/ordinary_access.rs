//! Root adapters from focused Pod API contracts to repository and actor internals.

#[cfg(test)]
use klights_pod_api::{
    PodApiDeleteCollectionRequest, PodApiDeleteRequest, PodApiMutation, PodApiPatchRequest,
    PodApiUpdateRequest, PodApiWriteOutcome, PodEvictionDelete, PodEvictionDeleteOutcome,
    PodEvictionDeleteRequest, PodMarkTerminating, PodMarkTerminatingRequest,
};
use klights_pod_api::{
    PodGetRequest, PodLifecycleFuture, PodLifecycleWakeup, PodLifecycleWakeupRequest,
    PodListRequest, PodListResult, PodOwnerListRequest, PodQuery, PodRepositoryError,
    PodRepositoryFuture, PodRoutingError, PodSnapshotListRequest, PodSnapshotQuery, PodUpdate,
    PodUpdateOperation, PodUpdateRequest,
};
use serde_json::{Map, Value};

use crate::kubelet::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleKey};
use crate::kubelet::pod_lifecycle_router::PodLifecycleRouter;

use super::{PodObjectWriter, PodReader, PodRepository, store::PodStore};

macro_rules! impl_pod_query_via_reader {
    ($type:ty) => {
        impl PodQuery for $type {
            fn get_pod(
                &self,
                request: PodGetRequest,
            ) -> PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
                Box::pin(async move {
                    let pod = match request.uid() {
                        Some(uid) => {
                            PodReader::get_pod_for_uid(
                                self,
                                request.namespace(),
                                request.name(),
                                uid,
                            )
                            .await
                        }
                        None => PodReader::get_pod(self, request.namespace(), request.name()).await,
                    };
                    pod.map_err(|error| {
                        map_repository_error(error, request.namespace(), request.name())
                    })
                })
            }

            fn list_pods(&self, request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
                Box::pin(async move {
                    let list = PodReader::list_pods(
                        self,
                        request.namespace(),
                        request.label_selector(),
                        request.field_selector(),
                        request.limit(),
                        request.continue_token(),
                    )
                    .await
                    .map_err(|error| PodRepositoryError::unavailable(error.to_string()))?;
                    PodListResult::try_new(
                        list.items,
                        list.resource_version,
                        list.continue_token,
                        list.remaining_item_count,
                    )
                })
            }

            fn list_pods_by_owner_uid(
                &self,
                request: PodOwnerListRequest,
            ) -> PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
                Box::pin(async move {
                    PodReader::list_pods_by_owner_uid(
                        self,
                        request.namespace(),
                        request.owner_uid(),
                    )
                    .await
                    .map_err(|error| PodRepositoryError::unavailable(error.to_string()))
                })
            }
        }
    };
}

impl_pod_query_via_reader!(PodStore);
impl_pod_query_via_reader!(dyn PodReader + '_);

impl PodQuery for PodRepository {
    fn get_pod(
        &self,
        request: PodGetRequest,
    ) -> PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
        Box::pin(async move {
            let pod = match request.uid() {
                Some(uid) => {
                    PodReader::get_pod_for_uid(self, request.namespace(), request.name(), uid).await
                }
                None => PodReader::get_pod(self, request.namespace(), request.name()).await,
            };
            pod.map_err(|error| map_repository_error(error, request.namespace(), request.name()))
        })
    }

    fn list_pods(&self, request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
        Box::pin(async move {
            let list = PodReader::list_pods(
                self,
                request.namespace(),
                request.label_selector(),
                request.field_selector(),
                request.limit(),
                request.continue_token(),
            )
            .await
            .map_err(|error| PodRepositoryError::unavailable(error.to_string()))?;
            PodListResult::try_new(
                list.items,
                list.resource_version,
                list.continue_token,
                list.remaining_item_count,
            )
        })
    }

    fn list_pods_by_owner_uid(
        &self,
        request: PodOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
        Box::pin(async move {
            PodReader::list_pods_by_owner_uid(self, request.namespace(), request.owner_uid())
                .await
                .map_err(|error| PodRepositoryError::unavailable(error.to_string()))
        })
    }
}

impl PodUpdate for PodRepository {
    fn update_pod(
        &self,
        request: PodUpdateRequest,
    ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            let (target, operation) = request.into_parts();
            let result = match operation {
                PodUpdateOperation::MergeLabels(labels) => {
                    let labels = labels
                        .into_iter()
                        .map(klights_pod_api::PodLabel::into_parts)
                        .collect();
                    match target.uid() {
                        Some(uid) => {
                            PodObjectWriter::merge_pod_labels_for_uid(
                                self,
                                target.namespace(),
                                target.name(),
                                uid,
                                labels,
                            )
                            .await
                        }
                        None => {
                            PodObjectWriter::merge_pod_labels(
                                self,
                                target.namespace(),
                                target.name(),
                                labels,
                            )
                            .await
                        }
                    }
                }
                PodUpdateOperation::ReplaceOwnerReferences(owner_references) => {
                    let owner_references = owner_references
                        .into_iter()
                        .map(|owner| {
                            let (api_version, kind, name, uid, controller, block_owner_deletion) =
                                owner.into_parts();
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
                        })
                        .collect();
                    match target.uid() {
                        Some(uid) => {
                            PodObjectWriter::update_pod_owner_references_for_uid(
                                self,
                                target.namespace(),
                                target.name(),
                                uid,
                                owner_references,
                            )
                            .await
                        }
                        None => {
                            PodObjectWriter::update_pod_owner_references(
                                self,
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
                        super::PodMetadataWriter::record_sandbox_id_for_uid(
                            self,
                            target.namespace(),
                            target.name(),
                            uid,
                            &sandbox_id,
                        )
                        .await
                    }
                    None => {
                        super::PodMetadataWriter::record_sandbox_id(
                            self,
                            target.namespace(),
                            target.name(),
                            &sandbox_id,
                        )
                        .await
                    }
                },
            };
            result.map_err(|error| map_repository_error(error, target.namespace(), target.name()))
        })
    }
}

#[cfg(test)]
impl PodMarkTerminating for PodRepository {
    fn mark_pod_terminating(
        &self,
        request: PodMarkTerminatingRequest,
    ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            let target = request.into_target();
            let resource = super::PodApiPort::mark_terminating(
                self.test_api
                    .as_deref()
                    .expect("test termination requires the root Pod API adapter"),
                &target,
            )
            .await?;

            let _ = self
                .mutation_reconcile
                .reconcile_pod_mutation(
                    klights_reconcile_api::PodMutationReconcileRequest::RunHooks {
                        pod: resource.clone(),
                        named_hook: None,
                        context: "pod_object_mark_terminating",
                    },
                )
                .await;
            Ok(resource)
        })
    }
}

#[cfg(test)]
impl PodEvictionDelete for PodRepository {
    fn delete_for_eviction(
        &self,
        request: PodEvictionDeleteRequest,
    ) -> PodRepositoryFuture<'_, PodEvictionDeleteOutcome> {
        Box::pin(async move {
            let (namespace, name, options, dry_run) = request.into_parts();
            let outcome = super::PodApiPort::delete(
                self.test_api
                    .as_deref()
                    .expect("test eviction requires the root Pod API adapter"),
                &namespace,
                &name,
                options,
                dry_run,
            )
            .await?;
            match outcome {
                super::PodApiDeleteOutcome::DryRun(_) => Ok(PodEvictionDeleteOutcome::DryRun),
                super::PodApiDeleteOutcome::GracefulSet(resource) => {
                    let _ = self
                        .mutation_reconcile
                        .reconcile_pod_mutation(
                            klights_reconcile_api::PodMutationReconcileRequest::RunHooks {
                                pod: resource.clone(),
                                named_hook: None,
                                context: "pod_eviction_mark_terminating",
                            },
                        )
                        .await;
                    Ok(PodEvictionDeleteOutcome::Persisted(resource))
                }
            }
        })
    }
}

#[cfg(test)]
impl PodApiMutation for PodRepository {
    fn create_pod(
        &self,
        request: klights_pod_api::PodApiCreateRequest,
    ) -> PodRepositoryFuture<'_, klights_pod_api::PodApiCreateResult> {
        Box::pin(async move {
            let result = super::PodApiPort::create(
                self.test_api
                    .as_deref()
                    .expect("test create requires the root Pod API adapter"),
                super::PodApiCreateRequest {
                    namespace: request.namespace,
                    name: String::new(),
                    body: request.body,
                    dry_run: request.dry_run,
                    run_admission: true,
                },
            )
            .await?;
            Ok(klights_pod_api::PodApiCreateResult {
                resource: result.resource,
                body: result.body,
            })
        })
    }

    fn update_pod(
        &self,
        request: PodApiUpdateRequest,
    ) -> PodRepositoryFuture<'_, PodApiWriteOutcome> {
        Box::pin(async move {
            match super::PodApiPort::update(
                self.test_api
                    .as_deref()
                    .expect("test update requires the root Pod API adapter"),
                &request.namespace,
                &request.name,
                request.body,
                request.current,
                request.dry_run,
            )
            .await?
            {
                super::PodApiUpdateOutcome::Persisted(resource) => {
                    Ok(PodApiWriteOutcome::Persisted(resource))
                }
                super::PodApiUpdateOutcome::DryRun(value) => Ok(PodApiWriteOutcome::DryRun(value)),
            }
        })
    }

    fn patch_pod(
        &self,
        request: PodApiPatchRequest,
    ) -> PodRepositoryFuture<'_, PodApiWriteOutcome> {
        Box::pin(async move {
            let patch_type = match request.patch_kind {
                klights_pod_api::PodStatusPatchKind::JsonPatch => {
                    super::PodStatusPatchType::JsonPatch
                }
                klights_pod_api::PodStatusPatchKind::MergePatch => {
                    super::PodStatusPatchType::MergePatch
                }
                klights_pod_api::PodStatusPatchKind::StrategicMerge => {
                    super::PodStatusPatchType::StrategicMerge
                }
                klights_pod_api::PodStatusPatchKind::ApplyPatch => {
                    super::PodStatusPatchType::ApplyPatch
                }
            };
            match super::PodApiPort::patch(
                self.test_api
                    .as_deref()
                    .expect("test patch requires the root Pod API adapter"),
                &request.namespace,
                &request.name,
                request.patch,
                patch_type,
                request.dry_run,
            )
            .await?
            {
                super::PodApiUpdateOutcome::Persisted(resource) => {
                    Ok(PodApiWriteOutcome::Persisted(resource))
                }
                super::PodApiUpdateOutcome::DryRun(value) => Ok(PodApiWriteOutcome::DryRun(value)),
            }
        })
    }

    fn delete_pod(
        &self,
        request: PodApiDeleteRequest,
    ) -> PodRepositoryFuture<'_, klights_pod_api::PodApiDeleteOutcome> {
        Box::pin(async move {
            match super::PodApiPort::delete(
                self.test_api
                    .as_deref()
                    .expect("test delete requires the root Pod API adapter"),
                &request.namespace,
                &request.name,
                request.options,
                request.dry_run,
            )
            .await?
            {
                super::PodApiDeleteOutcome::GracefulSet(resource) => {
                    Ok(klights_pod_api::PodApiDeleteOutcome::GracefulSet(resource))
                }
                super::PodApiDeleteOutcome::DryRun(value) => {
                    Ok(klights_pod_api::PodApiDeleteOutcome::DryRun(value))
                }
            }
        })
    }

    fn delete_collection_pods(
        &self,
        request: PodApiDeleteCollectionRequest,
    ) -> PodRepositoryFuture<'_, ()> {
        Box::pin(async move {
            super::PodApiPort::delete_collection(
                self.test_api
                    .as_deref()
                    .expect("test collection delete requires the root Pod API adapter"),
                &request.namespace,
                request.label_selector.as_deref(),
                request.field_selector.as_deref(),
                request.dry_run,
            )
            .await
        })
    }

    fn bind_pod(&self, request: klights_pod_api::PodBindingRequest) -> PodRepositoryFuture<'_, ()> {
        Box::pin(async move {
            super::PodApiPort::bind(
                self.test_api
                    .as_deref()
                    .expect("test bind requires the root Pod API adapter"),
                &request.namespace,
                &request.name,
                request.binding,
                request.dry_run,
            )
            .await
        })
    }
}

impl PodSnapshotQuery for PodRepository {
    fn snapshot_pods(
        &self,
        request: PodSnapshotListRequest,
    ) -> PodRepositoryFuture<'_, klights_pod_api::PodSnapshotListOutcome> {
        Box::pin(async move {
            self.store
                .snapshot_list(request)
                .await
                .map_err(|error| PodRepositoryError::unavailable(error.to_string()))
        })
    }
}

impl klights_reconcile_api::NamespaceTerminationQueueSink for PodRepository {
    fn enqueue_namespace_termination(
        &self,
        namespace: String,
        uid: String,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async move {
            self.workqueue
                .enqueue_namespace_termination(namespace, uid)
                .await
                .map_err(|error| {
                    klights_reconcile_api::ReconcileSinkError::unavailable(error.to_string())
                })
        })
    }
}

impl PodLifecycleWakeup for PodLifecycleRouter {
    fn wake_pod_lifecycle(&self, request: PodLifecycleWakeupRequest) -> PodLifecycleFuture<'_> {
        Box::pin(async move {
            let (identity, resource_version, pod) = request.into_parts();
            self.route(LifecycleMessage::WatchModified {
                key: PodLifecycleKey::new(&identity.namespace, &identity.name, &identity.uid),
                resource_version: Some(resource_version),
                pod: std::sync::Arc::unwrap_or_clone(pod.data),
            })
            .await
            .map_err(|error| PodRoutingError::unavailable(error.to_string()))
        })
    }
}

pub(super) fn map_repository_error(
    error: anyhow::Error,
    _namespace: &str,
    _name: &str,
) -> PodRepositoryError {
    if let Some(mismatch) = error.downcast_ref::<super::PodUidMismatch>() {
        return PodRepositoryError::uid_mismatch(&mismatch.expected, &mismatch.actual);
    }
    if let Some(repository_error) = error.downcast_ref::<PodRepositoryError>() {
        return repository_error.clone();
    }
    PodRepositoryError::unavailable(error.to_string())
}

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;

    use crate::api::AppError;
    use crate::pod_api_service::map_api_error_to_pod_repository;

    #[tokio::test]
    async fn pod_repository_error_round_trips_kubernetes_http_categories() {
        let cases = [
            (AppError::BadRequest("bad".into()), 400),
            (AppError::Forbidden("denied".into()), 403),
            (AppError::NotFound("missing".into()), 404),
            (AppError::Conflict("stale".into()), 409),
            (AppError::UnprocessableEntity("invalid".into()), 422),
            (AppError::InternalError("queue".into()), 500),
            (AppError::ServiceUnavailable("leader".into()), 503),
        ];
        for (source, expected) in cases {
            let leaf = map_api_error_to_pod_repository(source, "default", "web");
            let response = AppError::from(leaf).into_response();
            assert_eq!(response.status().as_u16(), expected);
        }

        for code in [
            axum::http::StatusCode::BAD_REQUEST,
            axum::http::StatusCode::FORBIDDEN,
            axum::http::StatusCode::NOT_FOUND,
            axum::http::StatusCode::CONFLICT,
            axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let leaf = map_api_error_to_pod_repository(
                AppError::Status {
                    code,
                    reason: "TestReason",
                    message: format!("status {}", code.as_u16()),
                    details: serde_json::Value::Null,
                },
                "default",
                "web",
            );
            assert_eq!(AppError::from(leaf).into_response().status(), code);
        }
    }
}
