use klights_cluster_store::ResourceListOptions;
use std::sync::Arc;

use klights_cluster_core::Resource;
use serde_json::Value;

use crate::datastore::DatastoreHandle;
use k8s_native_service::admission::{
    AdmissionDependencyError, AdmissionEngine, AdmissionQuery, AdmissionResource,
    AdmissionWebhookClient, ReqwestAdmissionWebhookClient, ServiceWebhookTargetResolver,
    WebhookTargetResolver,
};

pub(crate) struct DatastoreAdmissionQuery {
    db: DatastoreHandle,
}

impl DatastoreAdmissionQuery {
    pub(crate) fn new(db: DatastoreHandle) -> Arc<Self> {
        Arc::new(Self { db })
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
        crate::datastore::DatastoreBackend::get_resource(
            self.db.as_ref(),
            api_version,
            kind,
            namespace,
            name,
        )
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
        crate::datastore::DatastoreBackend::list_resources(
            self.db.as_ref(),
            api_version,
            kind,
            namespace,
            ResourceListOptions::new(label_selector, None, None, None),
        )
        .await
        .map(|page| page.items.into_iter().map(admission_resource).collect())
        .map_err(dependency_error)
    }
}

pub(crate) struct ResourceAdmissionAdapter {
    identity: Arc<dyn k8s_native_service::ApiIdentityGenerator>,
    query: Arc<dyn AdmissionQuery>,
    target_resolver: Arc<dyn WebhookTargetResolver>,
    webhook_client: Arc<dyn AdmissionWebhookClient>,
}

impl ResourceAdmissionAdapter {
    pub(crate) fn new(
        identity: Arc<dyn k8s_native_service::ApiIdentityGenerator>,
        db: DatastoreHandle,
    ) -> Arc<Self> {
        let query: Arc<dyn AdmissionQuery> = DatastoreAdmissionQuery::new(db);
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
        mut context: k8s_native_service::admission::AdmissionRequestContext,
    ) -> k8s_native_service::generic_command::GenericCommandFuture<'a, Value> {
        Box::pin(async move {
            let engine = AdmissionEngine::new(
                self.identity.as_ref(),
                self.query.as_ref(),
                self.target_resolver.as_ref(),
                self.webhook_client.as_ref(),
            );
            let admitted = engine
                .run_with_context(&context, true)
                .await
                .map_err(k8s_native_service::map_mutating_admission_error)?;
            context.object = admitted.clone();
            engine
                .run_with_context(&context, false)
                .await
                .map_err(k8s_native_service::map_validating_admission_error)?;
            Ok(admitted)
        })
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
