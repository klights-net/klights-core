//! Compatibility facade for Redb resource operations.
//!
//! Canonical reads delegate to the already-extracted `RedbReadCore`.
//! Ordinary mutations delegate to the Phase 10C.1 owner and status delegates
//! to the Phase 10C.2 owner. The former inline `db_call_with_post_commit`
//! implementation and its `StagedPostCommit` result now live in those owners.

use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;

use crate::redb::RedbAccessor;
use crate::redb::RedbOrdinaryResourceStore;
use crate::redb::live_committed_apply::RedbLiveCommittedApplyStore;
use crate::redb::read_core::RedbCollectionScope;
use crate::redb::read_core::RedbListQuery;
use crate::redb::read_core::RedbReadCore;
use klights_cluster_core::{Resource, ResourcePatchRequest, ResourcePreconditions};
use klights_cluster_store::{
    ListPageRequest, ResourceList, ResourceListOptions, WatchTarget, WatchTargetScope,
};

#[derive(Clone)]
pub struct RedbResourceStore {
    accessor: Arc<RedbAccessor>,
    ordinary: RedbOrdinaryResourceStore,
    live_committed_apply: RedbLiveCommittedApplyStore,
}

impl RedbResourceStore {
    pub fn new(
        accessor: Arc<RedbAccessor>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Self {
        Self {
            ordinary: RedbOrdinaryResourceStore::new(accessor.clone(), wall_clock),
            live_committed_apply: RedbLiveCommittedApplyStore::new(accessor.clone()),
            accessor,
        }
    }

    pub async fn create_res(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
        self.ordinary
            .create_resource(api_version, kind, namespace, name, data)
            .await
    }

    pub async fn get_res(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        RedbReadCore::new(self.accessor.clone())
            .get_resource(api_version, kind, namespace, name)
            .await
    }

    pub async fn update_res(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
        self.ordinary
            .update_resource(api_version, kind, namespace, name, data, expected_rv)
            .await
    }

    pub async fn update_res_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
        self.ordinary
            .update_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                data,
                preconditions,
            )
            .await
    }

    pub async fn update_main_res_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
        self.ordinary
            .update_main_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                data,
                preconditions,
            )
            .await
    }

    pub async fn delete_res(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<((), Option<klights_cluster_store::StagedPostCommit>)> {
        self.ordinary
            .delete_resource(api_version, kind, namespace, name)
            .await
    }

    pub async fn delete_res_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<((), Option<klights_cluster_store::StagedPostCommit>)> {
        self.ordinary
            .delete_resource_with_preconditions(api_version, kind, namespace, name, preconditions)
            .await
    }

    pub async fn delete_res_with_tombstone(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
        self.ordinary
            .delete_resource_with_tombstone(
                api_version,
                kind,
                namespace,
                name,
                preconditions,
                grace_seconds,
            )
            .await
    }

    pub async fn list_res(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListOptions<'_>,
    ) -> Result<ResourceList> {
        let page = RedbReadCore::new(self.accessor.clone())
            .list_resources(
                api_version,
                kind,
                namespace.map_or(RedbCollectionScope::LegacyAny, |namespace| {
                    RedbCollectionScope::Namespace(namespace.to_string())
                }),
                RedbListQuery {
                    label_selector: query.label_selector.map(str::to_string),
                    field_selector: query.field_selector.map(str::to_string),
                    limit: query.limit,
                    cursor: query.continue_token.map(|name| {
                        klights_cluster_store::ResourceCollectionKey::new(
                            namespace.map(str::to_string),
                            name.to_string(),
                        )
                    }),
                },
            )
            .await?;
        Ok(ResourceList {
            resource_version: page.position.resource_version,
            watch_replay_position: Some(page.position),
            items: page.items,
            continue_token: page
                .continuation
                .map(|continuation| continuation.name().to_string()),
            remaining_item_count: page.remaining_item_count,
        })
    }

    pub async fn list_res_page(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        self.list_res(
            api_version,
            kind,
            namespace,
            ResourceListOptions::new(
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
        let targets = targets
            .iter()
            .map(|target| match &target.scope {
                WatchTargetScope::Cluster => klights_cluster_store::DurableWatchTarget::cluster(
                    &target.api_version,
                    &target.kind,
                ),
                WatchTargetScope::Namespaced(None) => {
                    klights_cluster_store::DurableWatchTarget::namespaced(
                        &target.api_version,
                        &target.kind,
                    )
                }
                WatchTargetScope::Namespaced(Some(namespace)) => {
                    klights_cluster_store::DurableWatchTarget::namespaced_in_namespace(
                        &target.api_version,
                        &target.kind,
                        namespace,
                    )
                }
            })
            .collect::<Vec<_>>();
        let (items, position) = RedbReadCore::new(self.accessor.clone())
            .list_resources_for_watch_targets(&targets, label_selector)
            .await?;
        Ok(ResourceList {
            items,
            resource_version: position.resource_version,
            watch_replay_position: Some(position),
            continue_token: None,
            remaining_item_count: None,
        })
    }

    pub async fn update_status_only_impl(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
        self.live_committed_apply
            .update_status(api_version, kind, namespace, name, status, expected_rv)
            .await
    }

    pub async fn update_status_only_with_preconditions_impl(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
        self.live_committed_apply
            .update_status_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                status,
                preconditions,
            )
            .await
    }

    pub async fn patch(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        patch: Value,
    ) -> Result<(
        Option<Resource>,
        Option<klights_cluster_store::StagedPostCommit>,
    )> {
        self.ordinary
            .patch_resource(api_version, kind, namespace, name, patch)
            .await
    }

    pub async fn patch_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: ResourcePatchRequest,
    ) -> Result<(
        Option<Resource>,
        Option<klights_cluster_store::StagedPostCommit>,
    )> {
        self.ordinary
            .patch_resource_with_preconditions(api_version, kind, namespace, name, request)
            .await
    }

    pub async fn find_owned(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        RedbReadCore::new(self.accessor.clone())
            .find_owned(owner_uid, namespace)
            .await
    }
}
