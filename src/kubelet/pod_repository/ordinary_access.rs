//! Root adapters from focused Pod API contracts to repository and actor internals.

use klights_pod_api::{
    PodGetRequest, PodLifecycleFuture, PodLifecycleWakeup, PodLifecycleWakeupRequest,
    PodListRequest, PodListResult, PodMarkTerminating, PodMarkTerminatingRequest,
    PodOwnerListRequest, PodQuery, PodRepositoryError, PodRepositoryFuture, PodRoutingError,
    PodUpdate, PodUpdateOperation, PodUpdateRequest,
};
use serde_json::{Map, Value};

use crate::api::{AppError, DeleteOptions};
use crate::kubelet::pod_lifecycle_core::message::{LifecycleMessage, PodLifecycleKey};
use crate::kubelet::pod_lifecycle_router::PodLifecycleRouter;

use super::{PodApiDeleteOutcome, PodObjectWriter, PodReader, PodRepository};

impl PodQuery for PodRepository {
    fn get_pod(
        &self,
        request: PodGetRequest,
    ) -> PodRepositoryFuture<'_, Option<crate::datastore::Resource>> {
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
    ) -> PodRepositoryFuture<'_, Vec<crate::datastore::Resource>> {
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
    ) -> PodRepositoryFuture<'_, crate::datastore::Resource> {
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

impl PodMarkTerminating for PodRepository {
    fn mark_pod_terminating(
        &self,
        request: PodMarkTerminatingRequest,
    ) -> PodRepositoryFuture<'_, crate::datastore::Resource> {
        Box::pin(async move {
            let target = request.into_target();
            let options = target
                .uid()
                .map(DeleteOptions::with_uid_precondition)
                .unwrap_or_default();
            let outcome = self
                .api
                .api_delete_pod(target.namespace(), target.name(), options, false)
                .await
                .map_err(|error| map_api_error(error, target.namespace(), target.name()))?;
            let PodApiDeleteOutcome::GracefulSet(resource) = outcome else {
                return Err(PodRepositoryError::corrupt_response(
                    "persisted graceful mark unexpectedly returned a dry-run body",
                ));
            };

            crate::side_effects::run_hooks_logged(
                &self.side_effects,
                &resource.data,
                self.store.db().as_ref(),
                &self.metrics,
                "pod_object_mark_terminating",
            )
            .await;
            Ok(resource)
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

impl From<PodRepositoryError> for AppError {
    fn from(error: PodRepositoryError) -> Self {
        match error {
            PodRepositoryError::InvalidRequest { message, .. } => Self::BadRequest(message),
            PodRepositoryError::NotFound { namespace, name } => {
                Self::NotFound(format!("Pod {namespace}/{name} not found"))
            }
            PodRepositoryError::UidMismatch { expected, actual } => Self::Conflict(format!(
                "Pod UID mismatch: expected {expected}, found {actual}"
            )),
            PodRepositoryError::Conflict { message } => Self::Conflict(message),
            PodRepositoryError::Forbidden { message } => Self::Forbidden(message),
            PodRepositoryError::Unprocessable { message } => Self::UnprocessableEntity(message),
            PodRepositoryError::Internal { message } => Self::InternalError(message),
            PodRepositoryError::Unavailable { message } => Self::ServiceUnavailable(message),
            PodRepositoryError::CorruptResponse { message } => Self::InternalError(message),
            PodRepositoryError::Timeout => {
                Self::ServiceUnavailable("Pod repository request timed out".to_string())
            }
            PodRepositoryError::Cancelled => {
                Self::ServiceUnavailable("Pod repository request cancelled".to_string())
            }
        }
    }
}

fn map_repository_error(error: anyhow::Error, namespace: &str, name: &str) -> PodRepositoryError {
    if let Some(mismatch) = error.downcast_ref::<super::PodUidMismatch>() {
        return PodRepositoryError::uid_mismatch(&mismatch.expected, &mismatch.actual);
    }
    if let Some(datastore_error) = error.downcast_ref::<crate::datastore::errors::DatastoreError>()
    {
        return match datastore_error {
            crate::datastore::errors::DatastoreError::Conflict { message } => {
                PodRepositoryError::conflict(message)
            }
            crate::datastore::errors::DatastoreError::NotFound { .. } => {
                PodRepositoryError::not_found(namespace, name)
            }
        };
    }
    PodRepositoryError::unavailable(error.to_string())
}

fn map_api_error(error: AppError, namespace: &str, name: &str) -> PodRepositoryError {
    match error {
        AppError::NotFound(_) => PodRepositoryError::not_found(namespace, name),
        AppError::BadRequest(message) => PodRepositoryError::invalid_request("pod", message),
        AppError::UnprocessableEntity(message) => PodRepositoryError::unprocessable(message),
        AppError::AlreadyExists(message) | AppError::Conflict(message) => {
            PodRepositoryError::conflict(message)
        }
        AppError::Forbidden(message) => PodRepositoryError::forbidden(message),
        AppError::ServiceUnavailable(message) => PodRepositoryError::unavailable(message),
        AppError::InternalError(message) | AppError::Internal(message) => {
            PodRepositoryError::internal(message)
        }
        AppError::Status {
            reason: "NotFound", ..
        } => PodRepositoryError::not_found(namespace, name),
        AppError::Status {
            reason: "Conflict",
            message,
            ..
        }
        | AppError::Status {
            reason: "AlreadyExists",
            message,
            ..
        } => PodRepositoryError::conflict(message),
        AppError::Status {
            reason: "Forbidden",
            message,
            ..
        } => PodRepositoryError::forbidden(message),
        AppError::Status { code, message, .. } if code == axum::http::StatusCode::BAD_REQUEST => {
            PodRepositoryError::invalid_request("pod", message)
        }
        AppError::Status { code, .. } if code == axum::http::StatusCode::NOT_FOUND => {
            PodRepositoryError::not_found(namespace, name)
        }
        AppError::Status { code, message, .. } if code == axum::http::StatusCode::FORBIDDEN => {
            PodRepositoryError::forbidden(message)
        }
        AppError::Status { code, message, .. } if code == axum::http::StatusCode::CONFLICT => {
            PodRepositoryError::conflict(message)
        }
        AppError::Status { code, message, .. }
            if code == axum::http::StatusCode::UNPROCESSABLE_ENTITY =>
        {
            PodRepositoryError::unprocessable(message)
        }
        AppError::Status { code, message, .. }
            if code == axum::http::StatusCode::INTERNAL_SERVER_ERROR =>
        {
            PodRepositoryError::internal(message)
        }
        AppError::Status { code, message, .. }
            if code == axum::http::StatusCode::SERVICE_UNAVAILABLE =>
        {
            PodRepositoryError::unavailable(message)
        }
        other => PodRepositoryError::unavailable(format!("{other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;

    use super::*;

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
            let leaf = map_api_error(source, "default", "web");
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
            let leaf = map_api_error(
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
