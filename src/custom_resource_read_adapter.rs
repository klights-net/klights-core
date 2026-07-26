use std::sync::Arc;

use crate::api::custom_resource_ports::{
    CustomResourceListSnapshot, CustomResourceReadFuture, CustomResourceReadPort,
    CustomResourceSnapshotRequest, CustomResourceWaitFuture, CustomResourceWatchTarget,
};

pub(crate) struct CustomResourceReadAdapter {
    db: crate::datastore::DatastoreHandle,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    positioned_watch: klights_watch::PositionedWatchService,
    projected_baseline: Arc<dyn klights_watch::ProjectedWatchBaselineRead>,
}

impl CustomResourceReadAdapter {
    pub(crate) fn new(
        db: crate::datastore::DatastoreHandle,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Arc<Self> {
        let projected_baseline = Arc::new(CrdProjectedWatchBaseline { db: db.clone() });
        Arc::new(Self {
            positioned_watch:
                crate::control_plane::client::local::datastore_positioned_watch_service(db.clone()),
            db,
            supervisor,
            projected_baseline,
        })
    }
}

fn datastore_target(target: &CustomResourceWatchTarget) -> crate::datastore::WatchTarget {
    match target {
        CustomResourceWatchTarget::Cluster { api_version, kind } => {
            crate::datastore::WatchTarget::cluster(api_version, kind)
        }
        CustomResourceWatchTarget::Namespaced {
            api_version,
            kind,
            namespace: None,
        } => crate::datastore::WatchTarget::namespaced(api_version, kind),
        CustomResourceWatchTarget::Namespaced {
            api_version,
            kind,
            namespace: Some(namespace),
        } => crate::datastore::WatchTarget::namespaced_in_namespace(api_version, kind, namespace),
    }
}

fn leader_list_result(
    list: crate::datastore::ResourceList,
) -> Result<klights_leader_api::ResourceListResult, crate::api::AppError> {
    klights_leader_api::ResourceListResult::try_new(
        list.items,
        list.resource_version,
        list.watch_replay_position,
        list.continue_token,
        list.remaining_item_count,
    )
    .map_err(crate::api::AppError::from)
}

impl CustomResourceReadPort for CustomResourceReadAdapter {
    fn snapshot_resources_at_rv(
        &self,
        request: CustomResourceSnapshotRequest,
    ) -> CustomResourceReadFuture<'_, CustomResourceListSnapshot> {
        Box::pin(async move {
            let query = crate::datastore::ResourceListQuery::new(
                request.label_selector.as_deref(),
                request.field_selector.as_deref(),
                request.limit,
                request.continue_token.as_deref(),
            );
            match self
                .db
                .snapshot_resources_at_rv(
                    &request.api_version,
                    &request.kind,
                    request.namespace.as_deref(),
                    query,
                    request.resource_version,
                )
                .await?
            {
                crate::datastore::SnapshotAtRv::Current => Ok(CustomResourceListSnapshot::Current),
                crate::datastore::SnapshotAtRv::Expired => Ok(CustomResourceListSnapshot::Expired),
                crate::datastore::SnapshotAtRv::List(list) => {
                    leader_list_result(list).map(CustomResourceListSnapshot::List)
                }
            }
        })
    }

    fn list_resources_for_watch_targets(
        &self,
        targets: Vec<CustomResourceWatchTarget>,
        label_selector: Option<String>,
    ) -> CustomResourceReadFuture<'_, klights_leader_api::ResourceListResult> {
        Box::pin(async move {
            let targets = targets.iter().map(datastore_target).collect::<Vec<_>>();
            self.db
                .list_resources_for_watch_targets(&targets, label_selector.as_deref())
                .await
                .map_err(crate::api::AppError::from)
                .and_then(leader_list_result)
        })
    }

    fn positioned_watch_service(&self) -> klights_watch::PositionedWatchService {
        self.positioned_watch.clone()
    }

    fn projected_watch_plan(
        &self,
        request: klights_leader_api::WatchRequest,
        targets: Vec<CustomResourceWatchTarget>,
        topics: Vec<klights_watch::WatchTopic>,
        resource_scope: klights_watch::WatchResourceScope,
        projection: Arc<dyn klights_watch::WatchResourceProjection>,
    ) -> Result<klights_watch::ProjectedWatchPlan, klights_leader_api::LeaderWatchError> {
        let targets = targets
            .iter()
            .map(custom_target_to_durable_target)
            .collect();
        klights_watch::ProjectedWatchPlan::try_new(
            request,
            targets,
            topics,
            resource_scope,
            self.projected_baseline.clone(),
            projection,
        )
    }

    fn wait_until_fresh(
        &self,
        target_rv: i64,
        topic: klights_watch::WatchTopic,
    ) -> CustomResourceWaitFuture<'_> {
        Box::pin(async move {
            crate::api::watch_stream::wait_until_datastore_fresh(
                &self.db,
                target_rv,
                topic,
                &self.supervisor,
            )
            .await;
        })
    }

    fn current_collection_resource_version(
        &self,
        api_version: String,
        kind: String,
        namespace: Option<String>,
    ) -> CustomResourceReadFuture<'_, i64> {
        Box::pin(async move {
            self.db
                .list_resources(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    crate::datastore::ResourceListQuery::new(None, None, Some(1), None),
                )
                .await
                .map(|list| list.resource_version)
                .map_err(crate::api::AppError::from)
        })
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
    db: crate::datastore::DatastoreHandle,
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
            let targets = request
                .targets()
                .iter()
                .map(durable_target_to_datastore_target)
                .collect::<Vec<_>>();
            match self
                .db
                .snapshot_resources_at_position(
                    &targets,
                    request.label_selector(),
                    None,
                    request.position(),
                )
                .await
                .map_err(|error| {
                    klights_leader_api::LeaderWatchError::unavailable(format!("{error:?}"))
                })? {
                crate::datastore::SnapshotAtRv::List(list) => {
                    let snapshot = klights_cluster_store::ResourceListSnapshot::try_new(
                        list.watch_replay_position.ok_or_else(|| {
                            klights_leader_api::LeaderWatchError::malformed_event(
                                "CRD positioned baseline omitted its replay position",
                            )
                        })?,
                    )
                    .map_err(|error| {
                        klights_leader_api::LeaderWatchError::malformed_event(error.to_string())
                    })?;
                    let page = klights_cluster_store::ResourceListPage::try_new(
                        list.items,
                        snapshot,
                        None,
                        list.remaining_item_count,
                    )
                    .map_err(|error| {
                        klights_leader_api::LeaderWatchError::malformed_event(error.to_string())
                    })?;
                    Ok(klights_cluster_store::ResourceListRead::Historical(page))
                }
                crate::datastore::SnapshotAtRv::Expired => {
                    Ok(klights_cluster_store::ResourceListRead::Expired {
                        requested: request.position().resource_version,
                        oldest_available: request.position().resource_version.saturating_add(1),
                    })
                }
                crate::datastore::SnapshotAtRv::Current => {
                    Err(klights_leader_api::LeaderWatchError::malformed_event(
                        "CRD positioned baseline returned an unpinned Current sentinel",
                    ))
                }
            }
        })
    }
}

fn durable_target_to_datastore_target(
    target: &klights_cluster_store::DurableWatchTarget,
) -> crate::datastore::WatchTarget {
    match target.scope() {
        klights_cluster_store::DurableWatchScope::Cluster => {
            crate::datastore::WatchTarget::cluster(target.api_version(), target.kind())
        }
        klights_cluster_store::DurableWatchScope::Namespaced(None) => {
            crate::datastore::WatchTarget::namespaced(target.api_version(), target.kind())
        }
        klights_cluster_store::DurableWatchScope::Namespaced(Some(namespace)) => {
            crate::datastore::WatchTarget::namespaced_in_namespace(
                target.api_version(),
                target.kind(),
                namespace,
            )
        }
    }
}
