use std::sync::Arc;

use async_trait::async_trait;

use crate::bootstrap::controller_adapters::controller_store_error_adapter::map_controller_store_error;
use klights_controllers::crd::{CrdRegistryReader, CrdRegistryRuntime, CrdRegistryWatchSession};
use klights_leader_api::{LeaderWatch, LeaderWatchError, WatchRequest};

struct LeaderCrdRegistryRuntime {
    resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    positioned_watch: klights_watch::PositionedWatchService,
}

#[async_trait]
impl CrdRegistryReader for LeaderCrdRegistryRuntime {
    async fn list_crd_values(
        &self,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<serde_json::Value>> {
        list_crd_values(self.resource_reads.as_ref()).await
    }
}

#[async_trait]
impl CrdRegistryRuntime for LeaderCrdRegistryRuntime {
    async fn open_crd_watch(
        &self,
    ) -> std::result::Result<CrdRegistryWatchSession, LeaderWatchError> {
        let listing = self
            .resource_reads
            .list_resources(klights_cluster_store::ResourceListRequest::new(
                "apiextensions.k8s.io/v1",
                "CustomResourceDefinition",
                klights_cluster_store::ResourceCollectionScope::Cluster,
                klights_cluster_store::ResourceListQuery::all(),
            ))
            .await
            .map_err(|error| LeaderWatchError::unavailable(error.to_string()))?;
        let page = match listing {
            klights_cluster_store::ResourceListRead::Current(page)
            | klights_cluster_store::ResourceListRead::Historical(page) => page,
            klights_cluster_store::ResourceListRead::Expired {
                requested,
                oldest_available,
                ..
            } => {
                return Err(LeaderWatchError::unavailable(format!(
                    "CustomResourceDefinition LIST at resourceVersion {requested} expired before {oldest_available}"
                )));
            }
        };
        let request = WatchRequest::try_new(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            None,
            None,
            Some(page.snapshot().resource_version()),
            Some(page.snapshot().position()),
        )?;
        let events = self.positioned_watch.watch_resources(request).await?;
        Ok(CrdRegistryWatchSession {
            initial_values: page
                .into_items()
                .into_iter()
                .map(|resource| Arc::unwrap_or_clone(resource.data))
                .collect(),
            events,
        })
    }
}

pub(crate) fn new_runtime(
    resource_reads: Arc<dyn klights_cluster_store::ClusterResourceRead>,
    positioned_watch: klights_watch::PositionedWatchService,
) -> Arc<dyn CrdRegistryRuntime> {
    Arc::new(LeaderCrdRegistryRuntime {
        resource_reads,
        positioned_watch,
    })
}

async fn list_crd_values(
    resource_reads: &dyn klights_cluster_store::ClusterResourceRead,
) -> klights_reconcile_api::ControllerStoreResult<Vec<serde_json::Value>> {
    match resource_reads
        .list_resources(klights_cluster_store::ResourceListRequest::new(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            klights_cluster_store::ResourceCollectionScope::Cluster,
            klights_cluster_store::ResourceListQuery::all(),
        ))
        .await
        .map_err(|error| map_controller_store_error(error.into()))?
    {
        klights_cluster_store::ResourceListRead::Current(page)
        | klights_cluster_store::ResourceListRead::Historical(page) => Ok(page
            .into_items()
            .into_iter()
            .map(|resource| Arc::unwrap_or_clone(resource.data))
            .collect()),
        klights_cluster_store::ResourceListRead::Expired {
            requested,
            oldest_available,
            ..
        } => Err(klights_reconcile_api::ControllerStoreError::unavailable(
            format!(
                "CustomResourceDefinition LIST at resourceVersion {requested} expired before {oldest_available}"
            ),
        )),
    }
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
        let db = klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory()
            .await
            .unwrap();
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

        sync_registry_from_datastore(
            new_runtime(
                db.focused_read_store(),
                crate::bootstrap::composition_adapters::positioned_watch_adapter::for_test(
                    &crate::bootstrap::cluster_store::selector::sqlite_passive_read_ports(&db),
                    &db,
                ),
            )
            .as_ref(),
            &registry,
        )
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
        sync_registry_from_datastore(
            new_runtime(
                db.focused_read_store(),
                crate::bootstrap::composition_adapters::positioned_watch_adapter::for_test(
                    &crate::bootstrap::cluster_store::selector::sqlite_passive_read_ports(&db),
                    &db,
                ),
            )
            .as_ref(),
            &registry,
        )
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
        let db = klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory()
            .await
            .unwrap();
        let passive_reads =
            crate::bootstrap::cluster_store::selector::sqlite_passive_read_ports(&db);
        let registry = CrdRegistry::new();
        let cancel = CancellationToken::new();
        let watcher = tokio::spawn(run_crd_registry_watch_with_components(
            new_runtime(
                db.focused_read_store(),
                crate::bootstrap::composition_adapters::positioned_watch_adapter::for_test(
                    &passive_reads,
                    &db,
                ),
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
