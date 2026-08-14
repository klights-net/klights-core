use async_trait::async_trait;
use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_cluster_store::ResourceListOptions;
use klights_reconcile_api::ControllerStoreResult as Result;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::DatastoreBackend;
use klights_controllers::gc::GcResourceStore;

struct BorrowedGcResourceStore<'a> {
    db: &'a dyn DatastoreBackend,
}

pub(crate) fn borrowed_store(db: &dyn DatastoreBackend) -> impl GcResourceStore + '_ {
    BorrowedGcResourceStore { db }
}

#[async_trait]
impl GcResourceStore for BorrowedGcResourceStore<'_> {
    async fn list_custom_resource_definitions(&self) -> Result<Vec<Resource>> {
        GcResourceStore::list_custom_resource_definitions(self.db).await
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        GcResourceStore::get_resource(self.db, api_version, kind, namespace, name).await
    }

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        GcResourceStore::update_resource_with_preconditions(
            self.db,
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }

    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        GcResourceStore::update_main_resource_with_preconditions(
            self.db,
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
    }

    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        GcResourceStore::find_owned_resources(self.db, owner_uid, namespace).await
    }

    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        GcResourceStore::find_owned_by_name_kind_empty_uid(
            self.db,
            owner_api_version,
            owner_name,
            owner_kind,
            namespace,
        )
        .await
    }
}

#[async_trait]
impl GcResourceStore for dyn DatastoreBackend + '_ {
    async fn list_custom_resource_definitions(&self) -> Result<Vec<Resource>> {
        Ok(self
            .list_resources(
                "apiextensions.k8s.io/v1",
                "CustomResourceDefinition",
                None,
                ResourceListOptions::all(),
            )
            .await
            .map_err(map_controller_store_error)?
            .items)
    }

    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        DatastoreBackend::get_resource(self, api_version, kind, namespace, name)
            .await
            .map_err(map_controller_store_error)
    }

    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        DatastoreBackend::update_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
        .map_err(map_controller_store_error)
    }

    async fn update_main_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        DatastoreBackend::update_main_resource_with_preconditions(
            self,
            api_version,
            kind,
            namespace,
            name,
            data,
            preconditions,
        )
        .await
        .map_err(map_controller_store_error)
    }

    async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        DatastoreBackend::find_owned_resources(self, owner_uid, namespace)
            .await
            .map_err(map_controller_store_error)
    }

    async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        DatastoreBackend::find_owned_by_name_kind_empty_uid(
            self,
            owner_api_version,
            owner_name,
            owner_kind,
            namespace,
        )
        .await
        .map_err(map_controller_store_error)
    }
}
