use std::sync::Arc;

use klights_cluster_core::Resource;
use serde_json::Value;

use crate::api::AppError;
use crate::api::generated_handler_ports::{
    BuiltinAdmissionDefaultsPort, GeneratedHandlerFuture, GeneratedLifecyclePort,
    GeneratedResourceMutationPort, GeneratedWatchPort, GeneratedWatchRequest,
};
use crate::datastore::DatastoreHandle;

pub(crate) struct GeneratedHandlerAdapter {
    db: DatastoreHandle,
    watch_source: Arc<crate::watch_stream_adapter::DatastoreWatchStreamAdapter>,
    file_process: klights_supervisor::FileProcessExecutor,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ca_cert_path: std::path::PathBuf,
}

impl GeneratedHandlerAdapter {
    pub(crate) fn new(
        db: DatastoreHandle,
        watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
        file_process: klights_supervisor::FileProcessExecutor,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
        ca_cert_path: std::path::PathBuf,
    ) -> Arc<Self> {
        Arc::new(Self {
            watch_source: Arc::new(
                crate::watch_stream_adapter::DatastoreWatchStreamAdapter::new(
                    db.clone(),
                    watch_signals,
                ),
            ),
            db,
            file_process,
            task_supervisor,
            ca_cert_path,
        })
    }
}

impl BuiltinAdmissionDefaultsPort for GeneratedHandlerAdapter {
    fn ensure_namespace_active(&self, namespace: String) -> GeneratedHandlerFuture<'_, ()> {
        Box::pin(async move {
            let resource =
                crate::datastore::DatastoreBackend::get_namespace(self.db.as_ref(), &namespace)
                    .await?;
            match crate::namespace_admission::classify_namespace(
                &namespace,
                resource.as_ref().map(|resource| resource.data.as_ref()),
            ) {
                crate::namespace_admission::NamespaceCreateEligibility::Allowed => Ok(()),
                crate::namespace_admission::NamespaceCreateEligibility::Missing => Err(
                    AppError::Forbidden(format!("namespace {namespace} not found")),
                ),
                crate::namespace_admission::NamespaceCreateEligibility::Terminating => Err(
                    AppError::Forbidden(format!("namespace {namespace} is being terminated")),
                ),
            }
        })
    }

    fn validate_pod_volume_paths(&self, pod: &Value) -> Result<(), AppError> {
        crate::kubelet::volumes::validate_volume_subpaths(pod)
            .and_then(|()| crate::kubelet::volumes::validate_volume_projection_paths(pod))
            .map_err(AppError::UnprocessableEntity)
    }

    fn prepare_pod_create(
        &self,
        namespace: String,
        mut pod: Value,
    ) -> GeneratedHandlerFuture<'_, Value> {
        Box::pin(async move {
            crate::api::helpers::apply_pod_runtimeclass_admission(self.db.as_ref(), &mut pod)
                .await?;
            crate::api::helpers::apply_limitrange_defaults_to_pod(
                self.db.as_ref(),
                &namespace,
                &mut pod,
            )
            .await?;
            crate::api::helpers::enforce_limitrange_constraints_for_pod(
                self.db.as_ref(),
                &namespace,
                &pod,
            )
            .await?;
            Ok(pod)
        })
    }

    fn prepare_pvc_create(
        &self,
        namespace: String,
        mut claim: Value,
    ) -> GeneratedHandlerFuture<'_, Value> {
        Box::pin(async move {
            crate::api::helpers::apply_default_storage_class_admission(
                self.db.as_ref(),
                &mut claim,
            )
            .await?;
            crate::api::helpers::enforce_limitrange_constraints_for_pvc(
                self.db.as_ref(),
                &namespace,
                &claim,
            )
            .await?;
            Ok(claim)
        })
    }
}

impl GeneratedLifecyclePort for GeneratedHandlerAdapter {
    fn rotate_bootstrap_token_secret(
        &self,
        resource: Resource,
    ) -> GeneratedHandlerFuture<'_, Resource> {
        Box::pin(async move {
            crate::bootstrap::bootstrap_token::rotate_bootstrap_token_secret_for_get(
                self.db.as_ref(),
                &resource,
            )
            .await
            .map_err(AppError::from)
        })
    }

    fn reconcile_cluster_role_aggregation(&self) -> GeneratedHandlerFuture<'_, ()> {
        Box::pin(async move {
            crate::controllers::rbac_reconcile::reconcile_cluster_role_aggregation(self.db.as_ref())
                .await
                .map_err(|error| AppError::Internal(error.to_string()))
        })
    }

    fn create_default_service_account(&self, namespace: String) -> GeneratedHandlerFuture<'_, ()> {
        Box::pin(async move {
            crate::controllers::namespace::create_default_service_account(
                self.db.as_ref(),
                &namespace,
            )
            .await
            .map_err(|error| AppError::Internal(error.to_string()))
        })
    }

    fn create_root_ca_config_map(&self, namespace: String) -> GeneratedHandlerFuture<'_, ()> {
        Box::pin(async move {
            let ca_cert_pem =
                crate::utils::read_utf8_file_async(&self.file_process, &self.ca_cert_path)
                    .await
                    .map_err(|error| AppError::Internal(error.to_string()))?;
            crate::controllers::namespace::create_kube_root_ca_configmap(
                self.db.as_ref(),
                &namespace,
                &ca_cert_pem,
            )
            .await
            .map_err(|error| AppError::Internal(error.to_string()))
        })
    }

    fn reconcile_root_ca_data(&self, namespace: String) -> GeneratedHandlerFuture<'_, ()> {
        Box::pin(async move {
            crate::controllers::namespace::reconcile_kube_root_ca_data_with_path(
                &self.file_process,
                self.db.as_ref(),
                &namespace,
                &self.ca_cert_path,
            )
            .await
            .map_err(|error| AppError::Internal(error.to_string()))
        })
    }

    fn reconcile_root_ca(&self, namespace: String) -> GeneratedHandlerFuture<'_, ()> {
        Box::pin(async move {
            crate::controllers::namespace::reconcile_kube_root_ca_with_path(
                &self.file_process,
                self.db.as_ref(),
                &namespace,
                &self.ca_cert_path,
            )
            .await
            .map_err(|error| AppError::Internal(error.to_string()))
        })
    }

    fn delete_node_cleanup_intents(&self, node_name: String) -> GeneratedHandlerFuture<'_, ()> {
        Box::pin(async move {
            self.db
                .delete_pod_cleanup_intents_for_node(&node_name)
                .await
                .map_err(AppError::from)
        })
    }

    fn maybe_finalize_pod_after_finalizers_drained(
        &self,
        namespace: String,
        name: String,
        pod: Value,
    ) -> GeneratedHandlerFuture<'_, ()> {
        Box::pin(async move {
            let deletion_started = pod
                .pointer("/metadata/deletionTimestamp")
                .and_then(Value::as_str)
                .is_some();
            let finalizers_drained = pod
                .pointer("/metadata/finalizers")
                .and_then(Value::as_array)
                .is_none_or(Vec::is_empty);
            if deletion_started && finalizers_drained {
                tracing::debug!(
                    namespace = %namespace,
                    name = %name,
                    "pod finalizers drained; actor-owned UID cleanup will remove the row"
                );
            }
            Ok(())
        })
    }
}

impl GeneratedResourceMutationPort for GeneratedHandlerAdapter {
    fn update_main_resource(
        &self,
        api_version: String,
        kind: String,
        namespace: Option<String>,
        name: String,
        data: Value,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> GeneratedHandlerFuture<'_, Resource> {
        Box::pin(async move {
            self.db
                .update_main_resource_with_preconditions(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    data,
                    preconditions,
                )
                .await
                .map_err(AppError::from)
        })
    }
}

impl GeneratedWatchPort for GeneratedHandlerAdapter {
    fn build_watch_stream(
        &self,
        request: GeneratedWatchRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = axum::body::Body> + Send + '_>> {
        Box::pin(async move {
            crate::api::watch_stream::build_label_selector_watch_stream(
                crate::api::watch_stream::LabelSelectorWatchStreamRequest {
                    source: self.watch_source.clone(),
                    task_supervisor: self.task_supervisor.clone(),
                    api_version: &request.api_version,
                    kind: request.kind,
                    watch_namespace: request.namespace,
                    requested_rv: request.requested_resource_version,
                    send_initial_events: request.send_initial_events,
                    send_bookmarks: request.send_bookmarks,
                    label_selector: request.label_selector,
                    field_selector: request.field_selector,
                    table_format: request.table_format,
                    stream_format: request.stream_format,
                    timeout_seconds: request.timeout_seconds,
                    emit_initial_state_for_resource_version_zero: request
                        .emit_initial_state_for_resource_version_zero,
                },
            )
            .await
        })
    }
}
