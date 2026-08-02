//! Root adapters from focused Pod API contracts to repository and actor internals.

#[cfg(test)]
use klights_pod_api::{
    PodApiDeleteCollectionRequest, PodApiDeleteRequest, PodApiMutation, PodApiPatchRequest,
    PodApiUpdateRequest, PodApiWriteOutcome, PodEvictionDelete, PodEvictionDeleteOutcome,
    PodEvictionDeleteRequest, PodMarkTerminating, PodMarkTerminatingRequest,
};
use klights_pod_api::{
    PodGetRequest, PodListRequest, PodListResult, PodOwnerListRequest, PodQuery,
    PodRepositoryError, PodRepositoryFuture, PodSnapshotListRequest, PodSnapshotQuery, PodUpdate,
    PodUpdateRequest,
};

use super::{PodObjectWriter, PodReader, PodRepository, store::PodStore};

macro_rules! impl_pod_query_via_reader {
    ($type:ty) => {
        impl klights_kubelet::pod_repository::PodQueryPort for $type {
            fn read_pod<'a>(
                &'a self,
                namespace: &'a str,
                name: &'a str,
            ) -> PodRepositoryFuture<'a, Option<klights_cluster_core::Resource>> {
                Box::pin(async move {
                    PodReader::get_pod(self, namespace, name)
                        .await
                        .map_err(|error| map_repository_error(error, namespace, name))
                })
            }

            fn read_pod_for_uid<'a>(
                &'a self,
                namespace: &'a str,
                name: &'a str,
                uid: &'a str,
            ) -> PodRepositoryFuture<'a, Option<klights_cluster_core::Resource>> {
                Box::pin(async move {
                    PodReader::get_pod_for_uid(self, namespace, name, uid)
                        .await
                        .map_err(|error| map_repository_error(error, namespace, name))
                })
            }

            fn list_pod_page<'a>(
                &'a self,
                request: &'a PodListRequest,
            ) -> PodRepositoryFuture<'a, klights_kubelet::pod_repository::PodRepositoryList> {
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
                    Ok(klights_kubelet::pod_repository::PodRepositoryList::new(
                        list.items,
                        list.resource_version,
                        list.continue_token,
                        list.remaining_item_count,
                    ))
                })
            }

            fn list_pods_by_owner_uid<'a>(
                &'a self,
                namespace: &'a str,
                owner_uid: &'a str,
            ) -> PodRepositoryFuture<'a, Vec<klights_cluster_core::Resource>> {
                Box::pin(async move {
                    PodReader::list_pods_by_owner_uid(self, namespace, owner_uid)
                        .await
                        .map_err(|error| PodRepositoryError::unavailable(error.to_string()))
                })
            }
        }

        impl PodQuery for $type {
            fn get_pod(
                &self,
                request: PodGetRequest,
            ) -> PodRepositoryFuture<'_, Option<klights_cluster_core::Resource>> {
                klights_kubelet::pod_repository::PodRepositoryService::get_pod_from(self, request)
            }

            fn list_pods(&self, request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
                klights_kubelet::pod_repository::PodRepositoryService::list_pods_from(self, request)
            }

            fn list_pods_by_owner_uid(
                &self,
                request: PodOwnerListRequest,
            ) -> PodRepositoryFuture<'_, Vec<klights_cluster_core::Resource>> {
                klights_kubelet::pod_repository::PodRepositoryService::list_pods_by_owner_uid_from(
                    self, request,
                )
            }
        }
    };
}

impl_pod_query_via_reader!(PodStore);
impl_pod_query_via_reader!(dyn PodReader + '_);
impl_pod_query_via_reader!(PodRepository);

impl klights_kubelet::pod_repository::PodUpdatePort for PodRepository {
    fn merge_pod_labels<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        labels: Vec<(String, String)>,
    ) -> PodRepositoryFuture<'a, klights_cluster_core::Resource> {
        Box::pin(async move {
            PodObjectWriter::merge_pod_labels(self, namespace, name, labels)
                .await
                .map_err(|error| map_repository_error(error, namespace, name))
        })
    }

    fn merge_pod_labels_for_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        uid: &'a str,
        labels: Vec<(String, String)>,
    ) -> PodRepositoryFuture<'a, klights_cluster_core::Resource> {
        Box::pin(async move {
            PodObjectWriter::merge_pod_labels_for_uid(self, namespace, name, uid, labels)
                .await
                .map_err(|error| map_repository_error(error, namespace, name))
        })
    }

    fn replace_pod_owner_references<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        owner_references: Vec<serde_json::Value>,
    ) -> PodRepositoryFuture<'a, klights_cluster_core::Resource> {
        Box::pin(async move {
            PodObjectWriter::update_pod_owner_references(self, namespace, name, owner_references)
                .await
                .map_err(|error| map_repository_error(error, namespace, name))
        })
    }

    fn replace_pod_owner_references_for_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        uid: &'a str,
        owner_references: Vec<serde_json::Value>,
    ) -> PodRepositoryFuture<'a, klights_cluster_core::Resource> {
        Box::pin(async move {
            PodObjectWriter::update_pod_owner_references_for_uid(
                self,
                namespace,
                name,
                uid,
                owner_references,
            )
            .await
            .map_err(|error| map_repository_error(error, namespace, name))
        })
    }

    fn record_pod_sandbox_id<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        sandbox_id: String,
    ) -> PodRepositoryFuture<'a, klights_cluster_core::Resource> {
        Box::pin(async move {
            super::PodMetadataWriter::record_sandbox_id(self, namespace, name, &sandbox_id)
                .await
                .map_err(|error| map_repository_error(error, namespace, name))
        })
    }

    fn record_pod_sandbox_id_for_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        uid: &'a str,
        sandbox_id: String,
    ) -> PodRepositoryFuture<'a, klights_cluster_core::Resource> {
        Box::pin(async move {
            super::PodMetadataWriter::record_sandbox_id_for_uid(
                self,
                namespace,
                name,
                uid,
                &sandbox_id,
            )
            .await
            .map_err(|error| map_repository_error(error, namespace, name))
        })
    }
}

impl PodUpdate for PodRepository {
    fn update_pod(
        &self,
        request: PodUpdateRequest,
    ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        klights_kubelet::pod_repository::PodRepositoryService::update_pod_from(self, request)
    }
}

#[cfg(test)]
impl klights_kubelet::pod_repository::PodTerminationPort for PodRepository {
    fn mark_terminating(
        &self,
        target: klights_pod_api::PodMutationTarget,
    ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        Box::pin(async move {
            let resource = self
                .test_mark_terminating
                .as_deref()
                .expect("test termination requires the neutral Pod termination port")
                .mark_pod_terminating(PodMarkTerminatingRequest::new(target))
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
impl PodMarkTerminating for PodRepository {
    fn mark_pod_terminating(
        &self,
        request: PodMarkTerminatingRequest,
    ) -> PodRepositoryFuture<'_, klights_cluster_core::Resource> {
        klights_kubelet::pod_repository::PodRepositoryService::mark_pod_terminating_from(
            self, request,
        )
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
            let outcome = self
                .test_api
                .as_deref()
                .expect("test eviction requires the neutral Pod API port")
                .delete_pod(PodApiDeleteRequest {
                    namespace: namespace.clone(),
                    name: name.clone(),
                    options,
                    dry_run,
                })
                .await?;
            match outcome {
                klights_pod_api::PodApiDeleteOutcome::DryRun(_) => {
                    Ok(PodEvictionDeleteOutcome::DryRun)
                }
                klights_pod_api::PodApiDeleteOutcome::GracefulSet(resource) => {
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
        self.test_api
            .as_deref()
            .expect("test create requires the neutral Pod API port")
            .create_pod(request)
    }

    fn update_pod(
        &self,
        request: PodApiUpdateRequest,
    ) -> PodRepositoryFuture<'_, PodApiWriteOutcome> {
        self.test_api
            .as_deref()
            .expect("test update requires the neutral Pod API port")
            .update_pod(request)
    }

    fn patch_pod(
        &self,
        request: PodApiPatchRequest,
    ) -> PodRepositoryFuture<'_, PodApiWriteOutcome> {
        self.test_api
            .as_deref()
            .expect("test patch requires the neutral Pod API port")
            .patch_pod(request)
    }

    fn delete_pod(
        &self,
        request: PodApiDeleteRequest,
    ) -> PodRepositoryFuture<'_, klights_pod_api::PodApiDeleteOutcome> {
        self.test_api
            .as_deref()
            .expect("test delete requires the neutral Pod API port")
            .delete_pod(request)
    }

    fn delete_collection_pods(
        &self,
        request: PodApiDeleteCollectionRequest,
    ) -> PodRepositoryFuture<'_, ()> {
        self.test_api
            .as_deref()
            .expect("test collection delete requires the neutral Pod API port")
            .delete_collection_pods(request)
    }

    fn bind_pod(&self, request: klights_pod_api::PodBindingRequest) -> PodRepositoryFuture<'_, ()> {
        self.test_api
            .as_deref()
            .expect("test bind requires the neutral Pod API port")
            .bind_pod(request)
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

    use crate::pod_native_orchestration::map_api_error_to_pod_repository;
    use k8s_native_service::AppError;

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
