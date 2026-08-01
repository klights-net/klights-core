use super::*;
#[cfg(test)]
use klights_cluster_datastore::sqlite::ListResourcesSnapshotPause;
use klights_cluster_datastore::sqlite::read_store::SqliteResourceListQuery;

impl Datastore {
    #[cfg(test)]
    pub(crate) fn install_list_resources_snapshot_pause_for_test(
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> std::sync::Arc<ListResourcesSnapshotPause> {
        SqliteReadStore::install_list_resources_snapshot_pause_for_test(
            api_version,
            kind,
            namespace,
            label_selector,
            field_selector,
            limit,
            continue_token,
        )
    }

    pub async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        self.focused_reads
            .get_resource(api_version, kind, namespace, name)
            .await
    }

    pub async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> Result<ResourceList> {
        let list = self
            .focused_reads
            .list_resources(
                api_version,
                kind,
                namespace,
                SqliteResourceListQuery::new(
                    query.label_selector,
                    query.field_selector,
                    query.limit,
                    query.continue_token,
                ),
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

    pub async fn list_resources_page(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        self.list_resources(
            api_version,
            kind,
            namespace,
            ResourceListQuery::new(
                label_selector,
                field_selector,
                page.limit(),
                page.continue_token(),
            ),
        )
        .await
    }

    pub async fn list_resources_for_watch_targets(
        &self,
        targets: &[WatchTarget],
        label_selector: Option<&str>,
    ) -> Result<ResourceList> {
        let snapshot = self
            .focused_reads
            .list_resources_for_watch_targets(&focused_watch_targets(targets), label_selector)
            .await?;
        let position = snapshot.snapshot().position();
        Ok(ResourceList {
            items: snapshot.into_items(),
            resource_version: position.resource_version,
            watch_replay_position: Some(position),
            continue_token: None,
            remaining_item_count: None,
        })
    }

    pub async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        self.focused_reads.list_cluster_resources().await
    }

    pub async fn list_resource_keys_for_scope(
        &self,
        api_version: String,
        kind: String,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>> {
        self.focused_reads
            .list_resource_keys_for_scope(api_version, kind, namespaced)
            .await
            .map(|keys| {
                keys.into_iter()
                    .map(|key| (key.namespace().map(str::to_string), key.name().to_string()))
                    .collect()
            })
    }

    pub async fn get_current_resource_version(&self) -> Result<i64> {
        self.focused_reads.get_current_resource_version().await
    }

    /// Allocate a logical list snapshot resourceVersion without emitting a watch event.
    ///
    /// Kubernetes can return an inconsistent continuation after an expired token. That
    /// continuation starts a new list snapshot and must use a resourceVersion distinct
    /// from the original snapshot, even if no object changed while the token aged out.
    pub async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        let rv = self
            .db_call("db_query", move |conn| {
                Ok(Self::advance_resource_version_after_in_conn(conn, min_rv)?)
            })
            .await
            .map_err(|e| anyhow!("Failed to advance resource version: {}", e))?;
        Ok(rv)
    }
}
