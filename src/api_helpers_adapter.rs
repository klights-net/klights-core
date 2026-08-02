#[cfg(test)]
use crate::datastore::DatastoreBackend;

#[cfg(test)]
fn namespace_lifecycle_error(
    error: anyhow::Error,
) -> klights_reconcile_api::NamespaceLifecycleError {
    let message = error.to_string();
    if message.to_ascii_lowercase().contains("not found") {
        klights_reconcile_api::NamespaceLifecycleError::NotFound { message }
    } else if klights_cluster_datastore::errors::is_conflict_error(&error) {
        klights_reconcile_api::NamespaceLifecycleError::Conflict { message }
    } else {
        klights_reconcile_api::NamespaceLifecycleError::Internal { message }
    }
}

#[cfg(test)]
macro_rules! impl_namespace_lifecycle_store {
    ($store:ty) => {
        impl klights_reconcile_api::NamespaceLifecycleStore for $store {
            fn get_terminating_namespace(
                &self,
                namespace: String,
            ) -> klights_reconcile_api::NamespaceLifecycleFuture<
                '_,
                Option<klights_cluster_core::Resource>,
            > {
                Box::pin(async move {
                    self.get_namespace(&namespace)
                        .await
                        .map_err(namespace_lifecycle_error)
                })
            }

            fn list_namespace_pods(
                &self,
                namespace: String,
            ) -> klights_reconcile_api::NamespaceLifecycleFuture<
                '_,
                Vec<klights_cluster_core::Resource>,
            > {
                Box::pin(async move {
                    self.list_namespace_resources_of_kind(&namespace, "Pod")
                        .await
                        .map_err(namespace_lifecycle_error)
                })
            }

            fn mark_namespace_pod_terminating(
                &self,
                pod: klights_cluster_core::Resource,
                namespace: String,
                body: serde_json::Value,
            ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, ()> {
                Box::pin(async move {
                    self.update_resource_with_preconditions(
                        &pod.api_version,
                        &pod.kind,
                        Some(&namespace),
                        &pod.name,
                        body,
                        klights_cluster_core::ResourcePreconditions::from_resource(&pod),
                    )
                    .await
                    .map_err(namespace_lifecycle_error)?;
                    Ok(())
                })
            }

            fn update_terminating_namespace(
                &self,
                namespace: String,
                body: serde_json::Value,
                expected_resource_version: i64,
            ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, klights_cluster_core::Resource>
            {
                Box::pin(async move {
                    self.update_namespace(&namespace, body, expected_resource_version)
                        .await
                        .map_err(namespace_lifecycle_error)
                })
            }

            fn list_namespace_non_pod_resources(
                &self,
                namespace: String,
            ) -> klights_reconcile_api::NamespaceLifecycleFuture<
                '_,
                Vec<klights_cluster_core::Resource>,
            > {
                Box::pin(async move {
                    self.list_namespace_resources_excluding_kind(&namespace, "Pod")
                        .await
                        .map_err(namespace_lifecycle_error)
                })
            }

            fn delete_namespace_non_pod_resource(
                &self,
                resource: klights_cluster_core::Resource,
                namespace: String,
            ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, ()> {
                Box::pin(async move {
                    self.delete_resource(
                        &resource.api_version,
                        &resource.kind,
                        Some(&namespace),
                        &resource.name,
                    )
                    .await
                    .map_err(namespace_lifecycle_error)
                })
            }

            fn count_namespace_resources(
                &self,
                namespace: String,
            ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, i64> {
                Box::pin(async move {
                    DatastoreBackend::count_namespace_resources(self, &namespace)
                        .await
                        .map_err(namespace_lifecycle_error)
                })
            }

            fn delete_terminating_namespace(
                &self,
                namespace: String,
            ) -> klights_reconcile_api::NamespaceLifecycleFuture<'_, ()> {
                Box::pin(async move {
                    self.delete_namespace(&namespace)
                        .await
                        .map_err(namespace_lifecycle_error)
                })
            }
        }
    };
}

#[cfg(test)]
impl_namespace_lifecycle_store!(dyn DatastoreBackend + '_);
#[cfg(test)]
impl_namespace_lifecycle_store!(crate::datastore::sqlite::Datastore);
