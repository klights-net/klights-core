//! Compatibility facade for Redb Namespace operations.
//!
//! Canonical reads delegate to the already-extracted `RedbReadCore`; ordinary
//! Namespace mutations delegate to the Phase 10C.1 owner. The former inline
//! `db_call_with_post_commit` implementation and its `StagedPostCommit` result
//! now live in that owner.

use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;

use super::super::ordinary_mutations::RedbOrdinaryNamespaceStore;
use klights_cluster_core::Resource;
use klights_cluster_datastore::redb::RedbAccessor;
use klights_cluster_datastore::redb::read_core::RedbCollectionScope;
use klights_cluster_datastore::redb::read_core::RedbListQuery;
use klights_cluster_datastore::redb::read_core::RedbReadCore;

#[derive(Clone)]
pub struct RedbNamespaceStore {
    pub accessor: Arc<RedbAccessor>,
    ordinary: RedbOrdinaryNamespaceStore,
}

impl RedbNamespaceStore {
    pub fn new(accessor: Arc<RedbAccessor>) -> Self {
        Self {
            ordinary: RedbOrdinaryNamespaceStore::new(accessor.clone()),
            accessor,
        }
    }

    pub async fn get_namespace_impl(&self, name: &str) -> Result<Option<Resource>> {
        RedbReadCore::new(self.accessor.clone())
            .get_resource("v1", "Namespace", None, name)
            .await
    }

    pub async fn list_namespaces_impl(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Result<crate::datastore::ResourceList> {
        let page = RedbReadCore::new(self.accessor.clone())
            .list_resources(
                "v1",
                "Namespace",
                RedbCollectionScope::Cluster,
                RedbListQuery {
                    label_selector: label_selector.map(str::to_string),
                    field_selector: field_selector.map(str::to_string),
                    limit: None,
                    cursor: None,
                },
            )
            .await?;
        Ok(crate::datastore::ResourceList {
            resource_version: page.position.resource_version,
            watch_replay_position: Some(page.position),
            items: page.items,
            continue_token: None,
            remaining_item_count: None,
        })
    }

    pub async fn create_ns(
        &self,
        name: &str,
        data: Value,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
        self.ordinary.create_namespace(name, data).await
    }

    pub async fn update_ns_impl(
        &self,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        self.ordinary
            .update_namespace(name, data, expected_rv)
            .await
    }

    pub async fn delete_ns_impl(&self, name: &str) -> Result<()> {
        self.ordinary.delete_namespace(name).await
    }

    pub async fn list_namespace_resources_impl(&self, namespace: &str) -> Result<Vec<Resource>> {
        RedbReadCore::new(self.accessor.clone())
            .list_namespace_resources(namespace, None, false)
            .await
    }

    pub async fn list_cluster_resources_impl(&self) -> Result<Vec<Resource>> {
        RedbReadCore::new(self.accessor.clone())
            .list_cluster_resources()
            .await
    }

    pub async fn list_namespace_resources_of_kind_impl(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        RedbReadCore::new(self.accessor.clone())
            .list_namespace_resources(namespace, Some(kind), false)
            .await
    }

    pub async fn list_namespace_resources_excluding_kind_impl(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        RedbReadCore::new(self.accessor.clone())
            .list_namespace_resources(namespace, Some(kind), true)
            .await
    }

    pub async fn count_namespace_resources_impl(&self, namespace: &str) -> Result<i64> {
        RedbReadCore::new(self.accessor.clone())
            .count_namespace_resources(namespace)
            .await
    }

    pub async fn delete_namespace_contents_impl(&self, namespace: &str) -> Result<()> {
        self.ordinary.delete_namespace_contents(namespace).await
    }

    pub async fn list_resource_keys_for_scope_impl(
        &self,
        api_version: &str,
        kind: &str,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>> {
        Ok(RedbReadCore::new(self.accessor.clone())
            .list_resource_keys(api_version, kind, namespaced)
            .await?
            .into_iter()
            .map(|key| (key.namespace().map(str::to_string), key.name().to_string()))
            .collect())
    }
}
