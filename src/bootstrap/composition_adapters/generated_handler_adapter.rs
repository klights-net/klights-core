use klights_cluster_store::{
    ClusterResourceRead, ClusterTopologyRead, DurableAllocatorRead, ResourceCollectionScope,
    ResourceGetRequest, ResourceListQuery, ResourceListRead, ResourceListRequest,
};
use std::sync::Arc;

use klights_cluster_core::Resource;
use serde_json::Value;

use k8s_native_service::AppError;
use k8s_native_service::generic_command::{
    BuiltinAdmissionDefaultsPort, GeneratedLifecyclePort, GeneratedResourceMutationPort,
    GenericCommandFuture,
};
use k8s_native_service::ports::{GeneratedWatchPort, GeneratedWatchRequest};

pub(crate) async fn submit_node_cleanup_intents(
    commands: &dyn klights_leader_api::LeaderResourceCommand,
    node_name: &str,
) -> Result<(), AppError> {
    let request = klights_leader_api::ResourceCommandRequest::try_new(
        klights_cluster_core::StorageCommand::DeletePodCleanupIntentsForNode {
            node_name: node_name.to_string(),
        },
    )
    .map_err(AppError::from)?;
    commands
        .submit_resource_command(request)
        .await
        .map(|_| ())
        .map_err(AppError::from)
}

pub(crate) struct GeneratedHandlerAdapter {
    resource_reads: Arc<dyn ClusterResourceRead>,
    commands: Arc<dyn klights_leader_api::LeaderResourceCommand>,
    namespace_bootstrap: super::leader_bootstrap_store_adapter::LeaderBootstrapStore,
    watch_source: Arc<super::watch_stream_adapter::DatastoreWatchStreamAdapter>,
    file_process: klights_supervisor::FileProcessExecutor,
    task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ca_cert_path: std::path::PathBuf,
    identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
}

pub(crate) struct GeneratedHandlerStorage {
    resource_reads: Arc<dyn ClusterResourceRead>,
    topology_reads: Arc<dyn ClusterTopologyRead>,
    allocator_reads: Arc<dyn DurableAllocatorRead>,
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    commands: Arc<dyn klights_leader_api::LeaderResourceCommand>,
}

impl GeneratedHandlerStorage {
    pub(crate) fn new(
        resource_reads: Arc<dyn ClusterResourceRead>,
        topology_reads: Arc<dyn ClusterTopologyRead>,
        allocator_reads: Arc<dyn DurableAllocatorRead>,
        resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
        commands: Arc<dyn klights_leader_api::LeaderResourceCommand>,
    ) -> Self {
        Self {
            resource_reads,
            topology_reads,
            allocator_reads,
            resource_query,
            commands,
        }
    }
}

impl GeneratedHandlerAdapter {
    pub(crate) fn new(
        storage: GeneratedHandlerStorage,
        watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
        positioned_watch: klights_watch::PositionedWatchService,
        file_process: klights_supervisor::FileProcessExecutor,
        task_supervisor: Arc<klights_supervisor::TaskSupervisor>,
        ca_cert_path: std::path::PathBuf,
        identity: Arc<dyn klights_controllers::ControllerIdentityGenerator>,
    ) -> Arc<Self> {
        let GeneratedHandlerStorage {
            resource_reads,
            topology_reads,
            allocator_reads,
            resource_query,
            commands,
        } = storage;
        Arc::new(Self {
            watch_source: Arc::new(
                super::watch_stream_adapter::DatastoreWatchStreamAdapter::new(
                    resource_query,
                    allocator_reads,
                    watch_signals,
                    positioned_watch,
                ),
            ),
            namespace_bootstrap: super::leader_bootstrap_store_adapter::LeaderBootstrapStore::new(
                resource_reads.clone(),
                topology_reads,
                commands.clone(),
            ),
            resource_reads,
            commands,
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
            let resource = self
                .resource_reads
                .get_resource(ResourceGetRequest::new("v1", "Namespace", None, &namespace))
                .await
                .map_err(|error| AppError::Internal(error.to_string()))?;
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
        self.resource_reads
            .get_resource(ResourceGetRequest::new(
                api_version,
                kind,
                namespace.map(str::to_owned),
                name,
            ))
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
        self.resource_reads
            .list_resources(ResourceListRequest::new(
                api_version,
                kind,
                namespace
                    .map(|value| ResourceCollectionScope::Namespace(value.to_owned()))
                    .unwrap_or(ResourceCollectionScope::AllNamespaces),
                ResourceListQuery::all(),
            ))
            .await
            .and_then(|listing| match listing {
                ResourceListRead::Current(page) | ResourceListRead::Historical(page) => {
                    Ok(page.into_items())
                }
                ResourceListRead::Expired {
                    requested,
                    oldest_available,
                    ..
                } => Err(klights_cluster_store::ResourceReadError::Expired {
                    requested,
                    oldest_available,
                }),
            })
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
                &self.namespace_bootstrap,
                &resource,
            )
            .await
            .map_err(AppError::from)
        })
    }

    fn reconcile_cluster_role_aggregation(&self) -> GenericCommandFuture<'_, ()> {
        Box::pin(async move {
            klights_controllers::rbac_reconcile::reconcile_cluster_role_aggregation(
                &self.namespace_bootstrap,
            )
            .await
            .map_err(|error| AppError::Internal(error.to_string()))
        })
    }

    fn create_default_service_account(&self, namespace: String) -> GenericCommandFuture<'_, ()> {
        Box::pin(async move {
            klights_controllers::namespace::create_default_service_account_at(
                &self.namespace_bootstrap,
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
                &self.namespace_bootstrap,
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
                &self.namespace_bootstrap,
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
                &self.namespace_bootstrap,
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
        Box::pin(
            async move { submit_node_cleanup_intents(self.commands.as_ref(), &node_name).await },
        )
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
            let expected_rv = preconditions.resource_version.unwrap_or_default();
            let request = klights_leader_api::ResourceCommandRequest::try_new(
                klights_cluster_core::StorageCommand::UpdateResource {
                    api_version,
                    kind,
                    namespace,
                    name,
                    data,
                    expected_rv,
                    preconditions,
                    preserve_status: true,
                },
            )
            .map_err(AppError::from)?;
            match self
                .commands
                .submit_resource_command(request)
                .await
                .map_err(AppError::from)?
            {
                klights_leader_api::ResourceCommandResult::Resource(resource) => Ok(resource),
                klights_leader_api::ResourceCommandResult::Ack { .. } => {
                    Err(AppError::InternalError(
                        "generated resource update returned no resource".to_string(),
                    ))
                }
            }
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
                    scope: request.scope,
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
