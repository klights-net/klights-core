use std::sync::Arc;

use async_trait::async_trait;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use crate::datastore::{DatastoreBackend, DatastoreHandle};
use klights_controllers::crd::{CrdRegistryReader, CrdRegistryRuntime, CrdRegistryWatchSession};
use klights_leader_api::{LeaderWatch, LeaderWatchError, WatchRequest};

struct LeaderCrdRegistryRuntime {
    db: DatastoreHandle,
    positioned_watch: klights_watch::PositionedWatchService,
}

#[async_trait]
impl CrdRegistryReader for dyn DatastoreBackend + '_ {
    async fn list_crd_values(
        &self,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<serde_json::Value>> {
        self.list_resources(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .map(|listing| {
            listing
                .items
                .into_iter()
                .map(|resource| Arc::unwrap_or_clone(resource.data))
                .collect()
        })
        .map_err(map_controller_store_error)
    }
}

#[async_trait]
impl CrdRegistryReader for LeaderCrdRegistryRuntime {
    async fn list_crd_values(
        &self,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<serde_json::Value>> {
        CrdRegistryReader::list_crd_values(self.db.as_ref()).await
    }
}

#[async_trait]
impl CrdRegistryRuntime for LeaderCrdRegistryRuntime {
    async fn open_crd_watch(
        &self,
    ) -> std::result::Result<CrdRegistryWatchSession, LeaderWatchError> {
        let listing = self
            .db
            .list_resources(
                "apiextensions.k8s.io/v1",
                "CustomResourceDefinition",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .map_err(|error| LeaderWatchError::unavailable(error.to_string()))?;
        let request = WatchRequest::try_new(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            None,
            None,
            Some(listing.resource_version),
            listing.watch_replay_position,
        )?;
        let events = self.positioned_watch.watch_resources(request).await?;
        Ok(CrdRegistryWatchSession {
            initial_values: listing
                .items
                .into_iter()
                .map(|resource| Arc::unwrap_or_clone(resource.data))
                .collect(),
            events,
        })
    }
}

pub(crate) fn new_runtime(
    db: DatastoreHandle,
    positioned_watch: klights_watch::PositionedWatchService,
) -> Arc<dyn CrdRegistryRuntime> {
    Arc::new(LeaderCrdRegistryRuntime {
        db,
        positioned_watch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_controllers::crd::{
        CrdRegistry, run_crd_registry_watch_with_components, sync_registry_from_datastore,
    };
    use serde_json::{Value, json};
    use tokio_util::sync::CancellationToken;

    fn crd(group: &str, kind: &str, plural: &str) -> Value {
        json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": format!("{plural}.{group}")},
            "spec": {
                "group": group,
                "scope": "Namespaced",
                "names": {"kind": kind, "plural": plural, "singular": plural.trim_end_matches('s')},
                "versions": [{"name": "v1", "served": true, "storage": true, "schema": {"openAPIV3Schema": {"type": "object"}}}]
            }
        })
    }

    #[tokio::test]
    async fn datastore_reader_is_registry_source_of_truth() {
        let db = crate::datastore::test_support::in_memory().await;
        let registry = CrdRegistry::new();
        db.create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "syncwidgets.sync.example.com",
            crd("sync.example.com", "SyncWidget", "syncwidgets"),
        )
        .await
        .unwrap();

        sync_registry_from_datastore(&db as &dyn DatastoreBackend, &registry)
            .await
            .unwrap();
        assert!(
            registry
                .get("sync.example.com", "v1", "syncwidgets")
                .await
                .is_some()
        );

        db.delete_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "syncwidgets.sync.example.com",
        )
        .await
        .unwrap();
        sync_registry_from_datastore(&db as &dyn DatastoreBackend, &registry)
            .await
            .unwrap();
        assert!(
            registry
                .get("sync.example.com", "v1", "syncwidgets")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn positioned_watch_runtime_syncs_datastore_applied_crds() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let handle: DatastoreHandle = Arc::new(db.clone());
        let passive_reads = crate::datastore::test_support::sqlite_passive_read_ports(&db);
        let registry = CrdRegistry::new();
        let cancel = CancellationToken::new();
        let watcher = tokio::spawn(run_crd_registry_watch_with_components(
            new_runtime(
                handle.clone(),
                crate::positioned_watch_adapter::for_test(&passive_reads, handle),
            ),
            registry.clone(),
            cancel.clone(),
        ));

        db.create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "watchwidgets.watch.example.com",
            crd("watch.example.com", "WatchWidget", "watchwidgets"),
        )
        .await
        .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if registry
                    .get("watch.example.com", "v1", "watchwidgets")
                    .await
                    .is_some()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("positioned watch must observe datastore-applied CRD");

        cancel.cancel();
        watcher.await.unwrap();
    }
}
