use std::sync::Arc;

use klights_cluster_core::Resource;
use serde_json::Value;

use k8s_native_service::admission::{
    AdmissionDependencyError, AdmissionQuery, AdmissionResource, AdmissionWebhookClient,
    ReqwestAdmissionWebhookClient, ServiceWebhookTargetResolver, WebhookTargetResolver,
};
use klights_cluster_store::{
    ClusterResourceRead, ResourceCollectionScope, ResourceGetRequest, ResourceListQuery,
    ResourceListRead, ResourceListRequest,
};

pub(crate) struct DatastoreAdmissionQuery {
    resource_reads: Arc<dyn ClusterResourceRead>,
}

impl DatastoreAdmissionQuery {
    pub(crate) fn new_with_resource_reads(
        resource_reads: Arc<dyn ClusterResourceRead>,
    ) -> Arc<Self> {
        Arc::new(Self { resource_reads })
    }
}

fn admission_resource(resource: Resource) -> AdmissionResource {
    AdmissionResource {
        name: resource.name,
        data: resource.data,
    }
}

fn dependency_error(error: impl std::fmt::Display) -> AdmissionDependencyError {
    AdmissionDependencyError::new(error.to_string())
}

#[async_trait::async_trait]
impl AdmissionQuery for DatastoreAdmissionQuery {
    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> std::result::Result<Option<AdmissionResource>, AdmissionDependencyError> {
        self.resource_reads
            .get_resource(ResourceGetRequest::new(
                api_version,
                kind,
                namespace.map(ToOwned::to_owned),
                name,
            ))
            .await
            .map(|resource| resource.map(admission_resource))
            .map_err(dependency_error)
    }

    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
    ) -> std::result::Result<Vec<AdmissionResource>, AdmissionDependencyError> {
        match self
            .resource_reads
            .list_resources(ResourceListRequest::new(
                api_version,
                kind,
                namespace.map_or(ResourceCollectionScope::AllNamespaces, |namespace| {
                    ResourceCollectionScope::Namespace(namespace.to_string())
                }),
                ResourceListQuery::try_new_borrowed(
                    label_selector,
                    None,
                    None,
                    None,
                    klights_cluster_store::ResourceVersionMatch::Any,
                )
                .map_err(dependency_error)?,
            ))
            .await
            .map_err(dependency_error)?
        {
            ResourceListRead::Current(page) | ResourceListRead::Historical(page) => Ok(page
                .into_items()
                .into_iter()
                .map(admission_resource)
                .collect()),
            ResourceListRead::Expired {
                requested,
                oldest_available,
                ..
            } => Err(AdmissionDependencyError::new(format!(
                "LIST at resourceVersion {requested} expired before {oldest_available}"
            ))),
        }
    }
}

pub(crate) struct ResourceAdmissionAdapter {
    identity: Arc<dyn k8s_native_service::ApiIdentityGenerator>,
    query: Arc<dyn AdmissionQuery>,
    target_resolver: Arc<dyn WebhookTargetResolver>,
    webhook_client: Arc<dyn AdmissionWebhookClient>,
}

impl ResourceAdmissionAdapter {
    pub(crate) fn new_with_resource_reads(
        identity: Arc<dyn k8s_native_service::ApiIdentityGenerator>,
        resource_reads: Arc<dyn ClusterResourceRead>,
    ) -> Arc<Self> {
        let query: Arc<dyn AdmissionQuery> =
            DatastoreAdmissionQuery::new_with_resource_reads(resource_reads);
        let target_resolver: Arc<dyn WebhookTargetResolver> =
            ServiceWebhookTargetResolver::new(Arc::clone(&query));
        let webhook_client: Arc<dyn AdmissionWebhookClient> = ReqwestAdmissionWebhookClient::new();
        Arc::new(Self {
            identity,
            query,
            target_resolver,
            webhook_client,
        })
    }

    fn execute_admission<'a>(
        &'a self,
        context: k8s_native_service::admission::AdmissionRequestContext,
    ) -> k8s_native_service::generic_command::GenericCommandFuture<'a, Value> {
        Box::pin(k8s_native_service::admission::execute_admission_pipeline(
            self.identity.as_ref(),
            self.query.as_ref(),
            self.target_resolver.as_ref(),
            self.webhook_client.as_ref(),
            context,
        ))
    }
}

impl k8s_native_service::generic_command::ResourceAdmissionPort for ResourceAdmissionAdapter {
    fn admit(
        &self,
        request: k8s_native_service::generic_command::ResourceAdmissionRequest,
    ) -> k8s_native_service::generic_command::GenericCommandFuture<'_, Value> {
        let mut context = k8s_native_service::build_admission_context(
            k8s_native_service::AdmissionContextRequest {
                api_version: &request.api_version,
                kind: &request.kind,
                operation: &request.operation,
                namespace: request.namespace,
                name: request.name,
                object: request.object,
                old_object: request.old_object,
                dry_run: request.dry_run,
                subresource: request.subresource.as_deref(),
                options: request.options,
            },
        );
        if let Some(resource) = request.resource {
            context.resource = resource;
        }
        self.execute_admission(context)
    }
}
