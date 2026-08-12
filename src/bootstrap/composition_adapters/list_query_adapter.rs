use std::sync::Arc;

use k8s_native_service::generic_read::{
    ListResourceVersionFuture, ListResourceVersionPort, NamespaceListFuture, NamespaceListPage,
    NamespaceListPort, NamespaceListRequest, NamespaceListSnapshot,
};

pub(crate) struct DatastoreListResourceVersionPort {
    watch_maintenance: Arc<dyn klights_cluster_store::ClusterWatchMaintenance>,
}

impl DatastoreListResourceVersionPort {
    pub(crate) fn new(
        watch_maintenance: Arc<dyn klights_cluster_store::ClusterWatchMaintenance>,
    ) -> Arc<Self> {
        Arc::new(Self { watch_maintenance })
    }
}

impl ListResourceVersionPort for DatastoreListResourceVersionPort {
    fn advance_after(&self, minimum_resource_version: i64) -> ListResourceVersionFuture<'_> {
        Box::pin(async move {
            self.watch_maintenance
                .advance_resource_version_after(minimum_resource_version)
                .await
        })
    }
}

pub(crate) struct DatastoreNamespaceListPort {
    db: crate::datastore::DatastoreHandle,
}

impl DatastoreNamespaceListPort {
    pub(crate) fn new(db: crate::datastore::DatastoreHandle) -> Arc<Self> {
        Arc::new(Self { db })
    }

    fn page(list: crate::datastore::ResourceList) -> NamespaceListPage {
        NamespaceListPage {
            items: list.items,
            resource_version: list.resource_version,
            continue_token: list.continue_token,
            remaining_item_count: list.remaining_item_count,
        }
    }
}

impl NamespaceListPort for DatastoreNamespaceListPort {
    fn list_namespaces(
        &self,
        request: NamespaceListRequest,
    ) -> NamespaceListFuture<'_, NamespaceListPage> {
        Box::pin(async move {
            let page =
                crate::datastore::ListPageRequest::try_new(request.limit, request.continue_token)
                    .map_err(k8s_native_service::AppError::from)?;
            self.db
                .list_namespaces_page(
                    request.label_selector.as_deref(),
                    request.field_selector.as_deref(),
                    page,
                )
                .await
                .map(Self::page)
                .map_err(k8s_native_service::AppError::from)
        })
    }

    fn snapshot_namespaces(
        &self,
        request: NamespaceListRequest,
        snapshot_resource_version: i64,
    ) -> NamespaceListFuture<'_, NamespaceListSnapshot> {
        Box::pin(async move {
            let query = crate::datastore::ResourceListQuery::new(
                request.label_selector.as_deref(),
                request.field_selector.as_deref(),
                request.limit,
                request.continue_token.as_deref(),
            );
            self.db
                .snapshot_resources_at_rv("v1", "Namespace", None, query, snapshot_resource_version)
                .await
                .map(|snapshot| match snapshot {
                    crate::datastore::SnapshotAtRv::List(list) => {
                        NamespaceListSnapshot::List(Self::page(list))
                    }
                    crate::datastore::SnapshotAtRv::Current => NamespaceListSnapshot::Current,
                    crate::datastore::SnapshotAtRv::Expired => NamespaceListSnapshot::Expired,
                })
                .map_err(k8s_native_service::AppError::from)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RecordingWatchMaintenance {
        calls: AtomicUsize,
        returned_rv: i64,
    }

    #[async_trait]
    impl klights_cluster_store::ClusterWatchMaintenance for RecordingWatchMaintenance {
        async fn advance_resource_version_after(&self, _min_rv: i64) -> anyhow::Result<i64> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.returned_rv)
        }

        async fn watch_events_gc_prunable_count(
            &self,
            _max_rows: i64,
            _batch_cap: i64,
        ) -> anyhow::Result<usize> {
            Ok(0)
        }

        async fn gc_watch_events(&self, _max_rows: i64, _batch_cap: i64) -> anyhow::Result<usize> {
            Ok(0)
        }
    }

    #[tokio::test]
    async fn inconsistent_continue_rv_advance_uses_cluster_watch_maintenance() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let before = db.get_current_resource_version().await.unwrap();
        let maintenance = Arc::new(RecordingWatchMaintenance {
            calls: AtomicUsize::new(0),
            returned_rv: before + 11,
        });
        let port = DatastoreListResourceVersionPort::new(maintenance.clone());

        let advanced = port.advance_after(before + 10).await.unwrap();

        assert_eq!(advanced, before + 11);
        assert_eq!(maintenance.calls.load(Ordering::SeqCst), 1);
        assert_eq!(db.get_current_resource_version().await.unwrap(), before);
    }
}
