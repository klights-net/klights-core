#[async_trait::async_trait]
impl<T> crate::admission::AdmissionLookup for T
where
    T: crate::datastore::DatastoreBackend + ?Sized,
{
    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        crate::datastore::DatastoreBackend::get_resource(self, api_version, kind, namespace, name)
            .await
    }

    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
        Ok(crate::datastore::DatastoreBackend::list_resources(
            self,
            api_version,
            kind,
            namespace,
            crate::datastore::ResourceListQuery::new(label_selector, None, None, None),
        )
        .await?
        .items)
    }
}

struct DatastoreAdmissionLookup<'a> {
    inner: &'a dyn crate::datastore::DatastoreBackend,
}

#[async_trait::async_trait]
impl crate::admission::AdmissionLookup for DatastoreAdmissionLookup<'_> {
    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> anyhow::Result<Option<klights_cluster_core::Resource>> {
        crate::admission::AdmissionLookup::get_resource(
            self.inner,
            api_version,
            kind,
            namespace,
            name,
        )
        .await
    }

    async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
    ) -> anyhow::Result<Vec<klights_cluster_core::Resource>> {
        crate::admission::AdmissionLookup::list_resources(
            self.inner,
            api_version,
            kind,
            namespace,
            label_selector,
        )
        .await
    }
}

impl<T> crate::api::AdmissionExecution for T
where
    T: crate::datastore::DatastoreBackend,
{
    fn execute_admission<'a>(
        &'a self,
        context: crate::admission::AdmissionRequestContext,
    ) -> crate::api::AdmissionExecutionFuture<'a> {
        execute_admission(self, context)
    }
}

impl crate::api::AdmissionExecution for dyn crate::datastore::DatastoreBackend {
    fn execute_admission<'a>(
        &'a self,
        context: crate::admission::AdmissionRequestContext,
    ) -> crate::api::AdmissionExecutionFuture<'a> {
        execute_admission(self, context)
    }
}

fn execute_admission<'a>(
    db: &'a dyn crate::datastore::DatastoreBackend,
    mut context: crate::admission::AdmissionRequestContext,
) -> crate::api::AdmissionExecutionFuture<'a> {
    Box::pin(async move {
        let lookup = DatastoreAdmissionLookup { inner: db };
        let engine = crate::admission::AdmissionEngine::new(&lookup);
        let admitted = engine
            .run_with_context(&context, true)
            .await
            .map_err(crate::api::map_mutating_admission_error)?;
        context.object = admitted.clone();
        engine
            .run_with_context(&context, false)
            .await
            .map_err(crate::api::map_validating_admission_error)?;
        Ok(admitted)
    })
}

pub(crate) struct ResourceAdmissionAdapter {
    db: crate::datastore::DatastoreHandle,
}

impl ResourceAdmissionAdapter {
    pub(crate) fn new(db: crate::datastore::DatastoreHandle) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self { db })
    }
}

impl crate::api::admission_ports::ResourceAdmissionPort for ResourceAdmissionAdapter {
    fn admit(
        &self,
        request: crate::api::admission_ports::ResourceAdmissionRequest,
    ) -> crate::api::admission_ports::ResourceAdmissionFuture<'_> {
        Box::pin(async move {
            crate::api::run_admission(
                self.db.as_ref(),
                crate::api::AdmissionContextRequest {
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
            )
            .await
        })
    }
}
