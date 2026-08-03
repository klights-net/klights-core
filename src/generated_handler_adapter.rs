use std::sync::Arc;

use klights_cluster_core::Resource;
use serde_json::Value;

use crate::datastore::{DatastoreHandle, ResourceListQuery};
use k8s_native_service::AppError;
use k8s_native_service::generic_command::{
    BuiltinAdmissionDefaultsPort, GeneratedLifecyclePort, GeneratedResourceMutationPort,
    GenericCommandFuture,
};
use k8s_native_service::ports::{GeneratedWatchPort, GeneratedWatchRequest};

pub(crate) struct GeneratedHandlerAdapter {
    db: DatastoreHandle,
    watch_source: Arc<crate::watch_stream_adapter::DatastoreWatchStreamAdapter>,
    file_process: klights_supervisor::FileProcessExecutor,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ca_cert_path: std::path::PathBuf,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
}

impl GeneratedHandlerAdapter {
    pub(crate) fn new(
        db: DatastoreHandle,
        watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
        positioned_watch: klights_watch::PositionedWatchService,
        file_process: klights_supervisor::FileProcessExecutor,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
        ca_cert_path: std::path::PathBuf,
        identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
    ) -> Arc<Self> {
        Arc::new(Self {
            watch_source: Arc::new(
                crate::watch_stream_adapter::DatastoreWatchStreamAdapter::new(
                    db.clone(),
                    watch_signals,
                    positioned_watch,
                ),
            ),
            db,
            file_process,
            task_supervisor,
            ca_cert_path,
            identity,
        })
    }
}

impl BuiltinAdmissionDefaultsPort for GeneratedHandlerAdapter {
    fn ensure_namespace_active(&self, namespace: String) -> GenericCommandFuture<'_, ()> {
        Box::pin(async move {
            let resource =
                crate::datastore::DatastoreBackend::get_namespace(self.db.as_ref(), &namespace)
                    .await?;
            match k8s_native_service::classify_namespace(
                &namespace,
                resource.as_ref().map(|resource| resource.data.as_ref()),
            ) {
                k8s_native_service::NamespaceCreateEligibility::Allowed => Ok(()),
                k8s_native_service::NamespaceCreateEligibility::Missing => Err(
                    AppError::Forbidden(format!("namespace {namespace} not found")),
                ),
                k8s_native_service::NamespaceCreateEligibility::Terminating => Err(
                    AppError::Forbidden(format!("namespace {namespace} is being terminated")),
                ),
            }
        })
    }

    fn validate_pod_volume_paths(&self, pod: &Value) -> Result<(), AppError> {
        klights_kubelet::volumes::validate_volume_subpaths(pod)
            .and_then(|()| klights_kubelet::volumes::validate_volume_projection_paths(pod))
            .map_err(AppError::UnprocessableEntity)
    }

    fn prepare_pod_create(
        &self,
        namespace: String,
        mut pod: Value,
    ) -> GenericCommandFuture<'_, Value> {
        Box::pin(async move {
            k8s_native_service::apply_pod_runtimeclass_admission(self, &mut pod).await?;
            k8s_native_service::apply_limitrange_defaults_to_pod(self, &namespace, &mut pod)
                .await?;
            k8s_native_service::enforce_limitrange_constraints_for_pod(self, &namespace, &pod)
                .await?;
            Ok(pod)
        })
    }

    fn prepare_pvc_create(
        &self,
        namespace: String,
        mut claim: Value,
    ) -> GenericCommandFuture<'_, Value> {
        Box::pin(async move {
            k8s_native_service::apply_default_storage_class_admission(self, &mut claim).await?;
            k8s_native_service::enforce_limitrange_constraints_for_pvc(self, &namespace, &claim)
                .await?;
            Ok(claim)
        })
    }
}

#[async_trait::async_trait]
impl k8s_native_service::ports::AdmissionResourceStore for GeneratedHandlerAdapter {
    async fn get_admission_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>, klights_leader_api::ResourceQueryError> {
        self.db
            .get_resource(api_version, kind, namespace, name)
            .await
            .map_err(|error| {
                klights_leader_api::ResourceQueryError::retryable(format!(
                    "admission resource read failed: {error}"
                ))
            })
    }

    async fn list_admission_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>, klights_leader_api::ResourceQueryError> {
        self.db
            .list_resources(api_version, kind, namespace, ResourceListQuery::all())
            .await
            .map(|listing| listing.items)
            .map_err(|error| {
                klights_leader_api::ResourceQueryError::retryable(format!(
                    "admission resource list failed: {error}"
                ))
            })
    }
}

impl GeneratedLifecyclePort for GeneratedHandlerAdapter {
    fn rotate_bootstrap_token_secret(
        &self,
        resource: Resource,
    ) -> GenericCommandFuture<'_, Resource> {
        Box::pin(async move {
            crate::bootstrap::bootstrap_token::rotate_bootstrap_token_secret_for_get(
                self.db.as_ref(),
                &resource,
            )
            .await
            .map_err(AppError::from)
        })
    }

    fn reconcile_cluster_role_aggregation(&self) -> GenericCommandFuture<'_, ()> {
        Box::pin(async move {
            klights_controllers::rbac_reconcile::reconcile_cluster_role_aggregation(
                self.db.as_ref(),
            )
            .await
            .map_err(|error| AppError::Internal(error.to_string()))
        })
    }

    fn create_default_service_account(&self, namespace: String) -> GenericCommandFuture<'_, ()> {
        Box::pin(async move {
            klights_controllers::namespace::create_default_service_account_at(
                self.db.as_ref(),
                &namespace,
                chrono::Utc::now(),
                self.identity.as_ref(),
            )
            .await
            .map_err(|error| AppError::Internal(error.to_string()))
        })
    }

    fn create_root_ca_config_map(&self, namespace: String) -> GenericCommandFuture<'_, ()> {
        Box::pin(async move {
            let ca_cert_pem = klights_supervisor::runtime_fs::read_utf8_async(
                &self.file_process,
                &self.ca_cert_path,
            )
            .await
            .map_err(|error| AppError::Internal(error.to_string()))?;
            klights_controllers::namespace::create_kube_root_ca_configmap_at(
                self.db.as_ref(),
                &namespace,
                &ca_cert_pem,
                chrono::Utc::now(),
                self.identity.as_ref(),
            )
            .await
            .map_err(|error| AppError::Internal(error.to_string()))
        })
    }

    fn reconcile_root_ca_data(&self, namespace: String) -> GenericCommandFuture<'_, ()> {
        Box::pin(async move {
            klights_controllers::namespace::reconcile_kube_root_ca_data_with_path(
                &self.file_process,
                self.db.as_ref(),
                &namespace,
                &self.ca_cert_path,
                chrono::Utc::now(),
                self.identity.as_ref(),
            )
            .await
            .map_err(|error| AppError::Internal(error.to_string()))
        })
    }

    fn reconcile_root_ca(&self, namespace: String) -> GenericCommandFuture<'_, ()> {
        Box::pin(async move {
            klights_controllers::namespace::reconcile_kube_root_ca_with_path(
                &self.file_process,
                self.db.as_ref(),
                &namespace,
                &self.ca_cert_path,
                chrono::Utc::now(),
                self.identity.as_ref(),
            )
            .await
            .map_err(|error| AppError::Internal(error.to_string()))
        })
    }

    fn delete_node_cleanup_intents(&self, node_name: String) -> GenericCommandFuture<'_, ()> {
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
    ) -> GenericCommandFuture<'_, ()> {
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
    ) -> GenericCommandFuture<'_, Resource> {
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
            k8s_native_service::watch::build_label_selector_watch_stream(
                k8s_native_service::watch::LabelSelectorWatchStreamRequest {
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
                    table_renderer: k8s_native_service::response::watch_event_to_table_at,
                    stream_format: request.stream_format,
                    timeout_seconds: request.timeout_seconds,
                    emit_initial_state_for_resource_version_zero: request
                        .emit_initial_state_for_resource_version_zero,
                    wall_clock: request.wall_clock,
                },
            )
            .await
        })
    }
}
