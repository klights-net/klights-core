use std::sync::Arc;

use crate::api::query::{
    ListPageMetadata, ListResourceVersionFuture, ListResourceVersionPort, ListSnapshotResolution,
    ListSnapshotResult, NamespaceListFuture, NamespaceListPage, NamespaceListPort,
    NamespaceListRequest, NamespaceListSnapshot,
};

pub(crate) struct DatastoreListResourceVersionPort {
    db: crate::datastore::DatastoreHandle,
}

impl DatastoreListResourceVersionPort {
    pub(crate) fn new(db: crate::datastore::DatastoreHandle) -> Arc<Self> {
        Arc::new(Self { db })
    }
}

impl ListResourceVersionPort for DatastoreListResourceVersionPort {
    fn advance_after(&self, minimum_resource_version: i64) -> ListResourceVersionFuture<'_> {
        Box::pin(async move {
            self.db
                .advance_resource_version_after(minimum_resource_version)
                .await
        })
    }
}

impl ListPageMetadata for crate::datastore::ResourceList {
    fn list_resource_version(&self) -> i64 {
        self.resource_version
    }
}

impl ListSnapshotResult<crate::datastore::ResourceList> for crate::datastore::SnapshotAtRv {
    fn into_list_snapshot_resolution(
        self,
    ) -> ListSnapshotResolution<crate::datastore::ResourceList> {
        match self {
            Self::List(list) => ListSnapshotResolution::List(list),
            Self::Current => ListSnapshotResolution::Current,
            Self::Expired => ListSnapshotResolution::Expired,
        }
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
                    .map_err(crate::api::AppError::from)?;
            self.db
                .list_namespaces_page(
                    request.label_selector.as_deref(),
                    request.field_selector.as_deref(),
                    page,
                )
                .await
                .map(Self::page)
                .map_err(crate::api::AppError::from)
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
                .map_err(crate::api::AppError::from)
        })
    }
}
