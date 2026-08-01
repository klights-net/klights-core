//! Root compatibility facade for the Phase 10B namespace read owner.

use super::*;

impl Datastore {
    pub async fn get_namespace(&self, name: &str) -> Result<Option<Resource>> {
        self.focused_reads.get_namespace(name).await
    }

    pub async fn list_namespaces(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Result<ResourceList> {
        let list = self
            .focused_reads
            .list_namespaces(label_selector, field_selector)
            .await?;
        Ok(ResourceList {
            items: list.items,
            resource_version: list.resource_version,
            watch_replay_position: list.watch_replay_position,
            continue_token: list.continue_token,
            remaining_item_count: list.remaining_item_count,
        })
    }

    pub async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        let list = self
            .focused_reads
            .list_namespaces_page(
                label_selector,
                field_selector,
                page.limit(),
                page.continue_token(),
            )
            .await?;
        Ok(ResourceList {
            items: list.items,
            resource_version: list.resource_version,
            watch_replay_position: list.watch_replay_position,
            continue_token: list.continue_token,
            remaining_item_count: list.remaining_item_count,
        })
    }

    pub async fn list_namespace_resources(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.focused_reads.list_namespace_resources(namespace).await
    }

    pub async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        self.focused_reads
            .list_namespace_resources_of_kind(namespace, kind)
            .await
    }

    pub async fn list_namespace_resources_excluding_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        self.focused_reads
            .list_namespace_resources_excluding_kind(namespace, kind)
            .await
    }

    pub async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        self.focused_reads
            .count_namespace_resources(namespace)
            .await
    }
}
