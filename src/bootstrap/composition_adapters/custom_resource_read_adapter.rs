use std::sync::Arc;

use k8s_native_service::ports::{
    CustomResourceProjection, CustomResourceReadFuture, CustomResourceReadPort,
    CustomResourceWaitFuture, CustomResourceWatchTarget,
};

pub(crate) struct CustomResourceReadAdapter {
    resource_scopes: Arc<dyn klights_cluster_store::ClusterResourceScopeRead>,
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    positioned_watch: klights_watch::PositionedWatchService,
    watch_source: super::watch_stream_adapter::DatastoreWatchStreamAdapter,
    projected_baseline: Arc<dyn klights_watch::ProjectedWatchBaselineRead>,
}

impl CustomResourceReadAdapter {
    pub(crate) fn new(
        resource_scopes: Arc<dyn klights_cluster_store::ClusterResourceScopeRead>,
        resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
        allocator_reads: Arc<dyn klights_cluster_store::DurableAllocatorRead>,
        watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
        positioned_watch: klights_watch::PositionedWatchService,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Arc<Self> {
        let projected_baseline = Arc::new(CrdProjectedWatchBaseline {
            resource_scopes: resource_scopes.clone(),
        });
        Arc::new(Self {
            positioned_watch: positioned_watch.clone(),
            watch_source: super::watch_stream_adapter::DatastoreWatchStreamAdapter::new(
                resource_query.clone(),
                allocator_reads,
                watch_signals,
                positioned_watch,
            ),
            resource_scopes,
            resource_query,
            supervisor,
            projected_baseline,
        })
    }
}

fn leader_list_result(
    list: klights_cluster_store::ResourceScopeSnapshot,
) -> Result<klights_leader_api::ResourceListResult, k8s_native_service::AppError> {
    let snapshot = list.snapshot();
    klights_leader_api::ResourceListResult::try_new(
        list.into_items(),
        snapshot.resource_version(),
        Some(snapshot.position()),
        None,
        None,
    )
    .map_err(k8s_native_service::AppError::from)
}

impl CustomResourceReadPort for CustomResourceReadAdapter {
    fn list_resources_for_watch_targets(
        &self,
        targets: Vec<CustomResourceWatchTarget>,
        label_selector: Option<String>,
    ) -> CustomResourceReadFuture<'_, klights_leader_api::ResourceListResult> {
        Box::pin(async move {
            let targets = targets
                .iter()
                .map(custom_target_to_durable_target)
                .collect();
            self.resource_scopes
                .list_resources_for_watch_targets(
                    klights_cluster_store::ResourceWatchTargetsRequest::try_new(
                        targets,
                        label_selector,
                    )
                    .map_err(|error| k8s_native_service::AppError::Internal(error.to_string()))?,
                )
                .await
                .map_err(|error| k8s_native_service::AppError::Internal(error.to_string()))
                .and_then(leader_list_result)
        })
    }

    fn watch_projected_resources(
        &self,
        request: klights_leader_api::WatchRequest,
        targets: Vec<CustomResourceWatchTarget>,
        projection: Arc<dyn CustomResourceProjection>,
    ) -> klights_leader_api::LeaderWatchFuture<'_> {
        let topics = targets
            .iter()
            .map(|target| match target {
                CustomResourceWatchTarget::Cluster { api_version, kind }
                | CustomResourceWatchTarget::Namespaced {
                    api_version, kind, ..
                } => klights_watch::WatchTopic::new(api_version, kind),
            })
            .collect();
        let resource_scope = if targets
            .iter()
            .all(|target| matches!(target, CustomResourceWatchTarget::Cluster { .. }))
        {
            klights_watch::WatchResourceScope::Cluster
        } else {
            klights_watch::WatchResourceScope::Namespaced
        };
        let durable_targets = targets
            .iter()
            .map(custom_target_to_durable_target)
            .collect();
        let plan = match klights_watch::ProjectedWatchPlan::try_new(
            request,
            durable_targets,
            topics,
            resource_scope,
            self.projected_baseline.clone(),
            Arc::new(CustomResourceProjectionAdapter(projection)),
        ) {
            Ok(plan) => plan,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let positioned_watch = self.positioned_watch.clone();
        Box::pin(async move { positioned_watch.watch_projected_resources(plan).await })
    }

    fn wait_until_fresh(
        &self,
        target_rv: i64,
        api_version: String,
        kind: String,
    ) -> CustomResourceWaitFuture<'_> {
        Box::pin(async move {
            k8s_native_service::watch::wait_until_datastore_fresh(
                &self.watch_source,
                target_rv,
                &api_version,
                &kind,
                &self.supervisor,
            )
            .await;
        })
    }

    fn current_collection_resource_version(
        &self,
        api_version: String,
        kind: String,
        scope: klights_leader_api::ResourceListScope,
    ) -> CustomResourceReadFuture<'_, i64> {
        Box::pin(async move {
            self.resource_query
                .list_resources(
                    klights_leader_api::ResourceListRequest::try_new(
                        api_version,
                        kind,
                        scope,
                        None,
                        None,
                        Some(1),
                        None,
                        klights_leader_api::ResourceQueryConsistency::LeaderFresh,
                    )
                    .map_err(k8s_native_service::AppError::from)?,
                )
                .await
                .map(|list| list.resource_version())
                .map_err(k8s_native_service::AppError::from)
        })
    }
}

struct CustomResourceProjectionAdapter(Arc<dyn CustomResourceProjection>);

impl klights_watch::WatchResourceProjection for CustomResourceProjectionAdapter {
    fn project_resources(
        &self,
        resources: Vec<klights_cluster_core::Resource>,
    ) -> futures::future::BoxFuture<
        '_,
        Result<Vec<klights_cluster_core::Resource>, klights_leader_api::LeaderWatchError>,
    > {
        self.0.project_resources(resources)
    }
}

fn custom_target_to_durable_target(
    target: &CustomResourceWatchTarget,
) -> klights_cluster_store::DurableWatchTarget {
    match target {
        CustomResourceWatchTarget::Cluster { api_version, kind } => {
            klights_cluster_store::DurableWatchTarget::cluster(api_version, kind)
        }
        CustomResourceWatchTarget::Namespaced {
            api_version,
            kind,
            namespace: None,
        } => klights_cluster_store::DurableWatchTarget::namespaced(api_version, kind),
        CustomResourceWatchTarget::Namespaced {
            api_version,
            kind,
            namespace: Some(namespace),
        } => klights_cluster_store::DurableWatchTarget::namespaced_in_namespace(
            api_version,
            kind,
            namespace,
        ),
    }
}

struct CrdProjectedWatchBaseline {
    resource_scopes: Arc<dyn klights_cluster_store::ClusterResourceScopeRead>,
}

impl klights_watch::ProjectedWatchBaselineRead for CrdProjectedWatchBaseline {
    fn read_baseline(
        &self,
        request: klights_watch::ProjectedWatchBaselineRequest,
    ) -> futures::future::BoxFuture<
        '_,
        Result<klights_cluster_store::ResourceListRead, klights_leader_api::LeaderWatchError>,
    > {
        Box::pin(async move {
            match self
                .resource_scopes
                .snapshot_resources_at_position(
                    klights_cluster_store::ResourceSnapshotAtPositionRequest::try_new(
                        request.targets().to_vec(),
                        request.label_selector().map(str::to_owned),
                        None,
                        request.position(),
                    )
                    .map_err(|error| {
                        klights_leader_api::LeaderWatchError::unavailable(error.to_string())
                    })?,
                )
                .await
                .map_err(|error| {
                    klights_leader_api::LeaderWatchError::unavailable(format!("{error:?}"))
                })? {
                klights_cluster_store::ResourceSnapshotRead::Historical(list) => {
                    let snapshot = list.snapshot();
                    let page = klights_cluster_store::ResourceListPage::try_new(
                        list.into_items(),
                        snapshot,
                        None,
                        None,
                    )
                    .map_err(|error| {
                        klights_leader_api::LeaderWatchError::malformed_event(error.to_string())
                    })?;
                    Ok(klights_cluster_store::ResourceListRead::Historical(page))
                }
                klights_cluster_store::ResourceSnapshotRead::Expired => {
                    Ok(klights_cluster_store::ResourceListRead::Expired {
                        requested: request.position().resource_version,
                        oldest_available: request.position().resource_version.saturating_add(1),
                        replacement: None,
                    })
                }
                klights_cluster_store::ResourceSnapshotRead::Current => {
                    Err(klights_leader_api::LeaderWatchError::malformed_event(
                        "CRD positioned baseline returned an unpinned Current sentinel",
                    ))
                }
            }
        })
    }
}
