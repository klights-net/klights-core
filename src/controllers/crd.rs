use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt as _;
pub use klights_leader_api::{CrdRegistry, CrdResourceInfo};
use klights_leader_api::{LeaderWatchError, ResourceEvent, WatchEventType, WatchStream};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait CrdRegistryReader: Send + Sync {
    async fn list_crd_values(&self) -> Result<Vec<serde_json::Value>>;
}

pub struct CrdRegistryWatchSession {
    pub initial_values: Vec<serde_json::Value>,
    pub events: WatchStream,
}

#[async_trait]
pub trait CrdRegistryRuntime: CrdRegistryReader {
    async fn open_crd_watch(
        &self,
    ) -> std::result::Result<CrdRegistryWatchSession, LeaderWatchError>;
}

pub async fn load_existing_crds<S: CrdRegistryReader + ?Sized>(
    source: &S,
    registry: &CrdRegistry,
) -> Result<()> {
    sync_registry_from_datastore(source, registry).await
}

pub async fn sync_registry_from_datastore<S: CrdRegistryReader + ?Sized>(
    source: &S,
    registry: &CrdRegistry,
) -> Result<()> {
    let mut infos = Vec::new();
    for crd_raw in source.list_crd_values().await? {
        infos.extend(crd_resource_infos_from_value(&crd_raw)?);
    }

    registry.replace_all(infos).await;
    Ok(())
}

pub async fn run_crd_registry_watch_with_components(
    runtime: Arc<dyn CrdRegistryRuntime>,
    registry: CrdRegistry,
    cancel: CancellationToken,
) {
    let mut session = match runtime.open_crd_watch().await {
        Ok(session) => session,
        Err(err) => {
            tracing::warn!("crd_registry: watch subscribe failed: {err:#}");
            return;
        }
    };
    if let Err(err) = replace_registry_from_values(&registry, &session.initial_values).await {
        tracing::warn!("crd_registry: initial snapshot failed: {err:#}");
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            result = session.events.next() => {
                match result {
                    Some(Ok(event)) => {
                        if !is_crd_event(&event) {
                            continue;
                        }
                        if let Err(err) = sync_registry_from_datastore(runtime.as_ref(), &registry).await {
                            tracing::warn!("crd_registry: sync after watch event failed: {err:#}");
                        }
                    }
                    None => {
                        tracing::warn!("crd_registry: watch signal channel closed");
                        break;
                    }
                    Some(Err(LeaderWatchError::ReplayExpired { .. })) => {
                        tracing::warn!("crd_registry: replay window expired; running full resync");
                        match runtime.open_crd_watch().await {
                            Ok(reopened) => {
                                if let Err(err) = replace_registry_from_values(
                                    &registry,
                                    &reopened.initial_values,
                                ).await {
                                    tracing::warn!("crd_registry: resync after expired replay failed: {err:#}");
                                }
                                session = reopened;
                            }
                            Err(err) => {
                                tracing::warn!("crd_registry: reopen after expired replay failed: {err:#}");
                                break;
                            }
                        }
                    }
                    Some(Err(err)) => {
                        tracing::warn!("crd_registry: watch replay failed: {err:#}");
                    }
                }
            }
        }
    }
}

async fn replace_registry_from_values(
    registry: &CrdRegistry,
    values: &[serde_json::Value],
) -> Result<()> {
    let mut infos = Vec::new();
    for value in values {
        infos.extend(crd_resource_infos_from_value(value)?);
    }
    registry.replace_all(infos).await;
    Ok(())
}

fn is_crd_event(event: &ResourceEvent) -> bool {
    matches!(
        event.event_type(),
        WatchEventType::Added | WatchEventType::Modified | WatchEventType::Deleted
    ) && event.resource().api_version == "apiextensions.k8s.io/v1"
        && event.resource().kind == "CustomResourceDefinition"
}

pub async fn register_crd_from_value(
    registry: &CrdRegistry,
    crd_value: &serde_json::Value,
) -> Result<()> {
    for info in crd_resource_infos_from_value(crd_value)? {
        registry.register(info).await;
    }

    Ok(())
}

fn crd_resource_infos_from_value(crd_value: &serde_json::Value) -> Result<Vec<CrdResourceInfo>> {
    klights_leader_api::resource_infos_from_value(crd_value).map_err(anyhow::Error::msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ========================
    // CrdRegistry tests
    // ========================

    #[tokio::test]
    async fn test_crd_registry_register_and_get() {
        let registry = CrdRegistry::new();
        let info = CrdResourceInfo {
            group: "cert-manager.io".to_string(),
            version: "v1".to_string(),
            kind: "Certificate".to_string(),
            plural: "certificates".to_string(),
            singular: "certificate".to_string(),
            namespaced: true,
            selectable_fields: Vec::new(),
        };
        registry.register(info).await;

        let result = registry.get("cert-manager.io", "v1", "certificates").await;
        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.kind, "Certificate");
        assert_eq!(info.singular, "certificate");
        assert!(info.namespaced);
    }

    #[tokio::test]
    async fn test_crd_registry_get_nonexistent_returns_none() {
        let registry = CrdRegistry::new();
        let result = registry.get("nonexistent.io", "v1", "widgets").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_crd_registry_list_group_versions_deduplicates() {
        let registry = CrdRegistry::new();

        // Register two resources in the same group/version
        registry
            .register(CrdResourceInfo {
                group: "argoproj.io".to_string(),
                version: "v1alpha1".to_string(),
                kind: "Application".to_string(),
                plural: "applications".to_string(),
                singular: "application".to_string(),
                namespaced: true,
                selectable_fields: Vec::new(),
            })
            .await;
        registry
            .register(CrdResourceInfo {
                group: "argoproj.io".to_string(),
                version: "v1alpha1".to_string(),
                kind: "AppProject".to_string(),
                plural: "appprojects".to_string(),
                singular: "appproject".to_string(),
                namespaced: true,
                selectable_fields: Vec::new(),
            })
            .await;

        let gvs = registry.list_group_versions().await;
        assert_eq!(gvs.len(), 1);
        assert_eq!(gvs[0], ("argoproj.io".to_string(), "v1alpha1".to_string()));
    }

    #[tokio::test]
    async fn test_crd_registry_list_resources_filters_by_group_version() {
        let registry = CrdRegistry::new();

        registry
            .register(CrdResourceInfo {
                group: "cert-manager.io".to_string(),
                version: "v1".to_string(),
                kind: "Certificate".to_string(),
                plural: "certificates".to_string(),
                singular: "certificate".to_string(),
                namespaced: true,
                selectable_fields: Vec::new(),
            })
            .await;
        registry
            .register(CrdResourceInfo {
                group: "cert-manager.io".to_string(),
                version: "v1".to_string(),
                kind: "Issuer".to_string(),
                plural: "issuers".to_string(),
                singular: "issuer".to_string(),
                namespaced: true,
                selectable_fields: Vec::new(),
            })
            .await;
        registry
            .register(CrdResourceInfo {
                group: "traefik.io".to_string(),
                version: "v1alpha1".to_string(),
                kind: "IngressRoute".to_string(),
                plural: "ingressroutes".to_string(),
                singular: "ingressroute".to_string(),
                namespaced: true,
                selectable_fields: Vec::new(),
            })
            .await;

        let cert_resources = registry.list_resources("cert-manager.io", "v1").await;
        assert_eq!(cert_resources.len(), 2);

        let traefik_resources = registry.list_resources("traefik.io", "v1alpha1").await;
        assert_eq!(traefik_resources.len(), 1);
        assert_eq!(traefik_resources[0].kind, "IngressRoute");

        let empty = registry.list_resources("nonexistent.io", "v1").await;
        assert_eq!(empty.len(), 0);
    }

    // ========================
    // register_crd_from_value tests
    // ========================

    fn make_crd_value(
        group: &str,
        kind: &str,
        plural: &str,
        scope: &str,
        versions: Vec<(&str, bool)>,
    ) -> serde_json::Value {
        let version_entries: Vec<serde_json::Value> = versions
            .iter()
            .map(|(name, served)| {
                json!({
                    "name": name,
                    "served": served,
                    "storage": true,
                    "schema": {
                        "openAPIV3Schema": {
                            "type": "object",
                            "x-kubernetes-preserve-unknown-fields": true
                        }
                    }
                })
            })
            .collect();

        json!({
            "apiVersion": "apiextensions.k8s.io/v1",
            "kind": "CustomResourceDefinition",
            "metadata": {
                "name": format!("{}.{}", plural, group)
            },
            "spec": {
                "group": group,
                "scope": scope,
                "names": {
                    "kind": kind,
                    "plural": plural,
                    "singular": kind.to_lowercase()
                },
                "versions": version_entries
            }
        })
    }

    #[tokio::test]
    async fn test_register_crd_from_value_namespaced_crd() {
        let registry = CrdRegistry::new();
        let crd = make_crd_value(
            "cert-manager.io",
            "Certificate",
            "certificates",
            "Namespaced",
            vec![("v1", true)],
        );

        register_crd_from_value(&registry, &crd).await.unwrap();

        let info = registry
            .get("cert-manager.io", "v1", "certificates")
            .await
            .unwrap();
        assert_eq!(info.kind, "Certificate");
        assert!(info.namespaced);
    }

    #[tokio::test]
    async fn test_register_crd_from_value_cluster_scoped_crd() {
        let registry = CrdRegistry::new();
        let crd = make_crd_value(
            "cert-manager.io",
            "ClusterIssuer",
            "clusterissuers",
            "Cluster",
            vec![("v1", true)],
        );

        register_crd_from_value(&registry, &crd).await.unwrap();

        let info = registry
            .get("cert-manager.io", "v1", "clusterissuers")
            .await
            .unwrap();
        assert_eq!(info.kind, "ClusterIssuer");
        assert!(!info.namespaced);
    }

    #[tokio::test]
    async fn test_register_crd_from_value_multiple_versions_registers_only_served() {
        let registry = CrdRegistry::new();
        let crd = make_crd_value(
            "argoproj.io",
            "Application",
            "applications",
            "Namespaced",
            vec![("v1alpha1", true), ("v1beta1", false), ("v1", true)],
        );

        register_crd_from_value(&registry, &crd).await.unwrap();

        // v1alpha1 served=true -> registered
        assert!(
            registry
                .get("argoproj.io", "v1alpha1", "applications")
                .await
                .is_some()
        );
        // v1beta1 served=false -> NOT registered
        assert!(
            registry
                .get("argoproj.io", "v1beta1", "applications")
                .await
                .is_none()
        );
        // v1 served=true -> registered
        assert!(
            registry
                .get("argoproj.io", "v1", "applications")
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn crd_registry_sync_uses_datastore_as_source_of_truth() {
        let db = crate::datastore::test_support::in_memory().await;
        let registry = CrdRegistry::new();
        let crd = make_crd_value(
            "sync.example.com",
            "SyncWidget",
            "syncwidgets",
            "Namespaced",
            vec![("v1", true)],
        );

        db.create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "syncwidgets.sync.example.com",
            crd,
        )
        .await
        .unwrap();

        sync_registry_from_datastore(&db, &registry).await.unwrap();

        assert!(
            registry
                .get("sync.example.com", "v1", "syncwidgets")
                .await
                .is_some(),
            "CRD registry must include CRDs that were applied through cluster.db"
        );

        db.delete_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "syncwidgets.sync.example.com",
        )
        .await
        .unwrap();

        sync_registry_from_datastore(&db, &registry).await.unwrap();

        assert!(
            registry
                .get("sync.example.com", "v1", "syncwidgets")
                .await
                .is_none(),
            "CRD registry must drop CRDs that no longer exist in cluster.db"
        );
    }

    #[tokio::test]
    async fn crd_registry_watch_syncs_datastore_applied_crds() {
        use std::sync::Arc;
        use std::time::Duration;
        use tokio_util::sync::CancellationToken;
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let db_handle: crate::datastore::DatastoreHandle = Arc::new(db.clone());
        let registry = CrdRegistry::new();
        let cancel = CancellationToken::new();

        let watcher = tokio::spawn(run_crd_registry_watch_with_components(
            crate::crd_registry_adapter::new_runtime(
                db_handle.clone(),
                crate::watch_commit_observation_adapter::test_signal_source(&db_handle),
            ),
            registry.clone(),
            cancel.clone(),
        ));

        let crd = make_crd_value(
            "watch.example.com",
            "WatchWidget",
            "watchwidgets",
            "Namespaced",
            vec![("v1", true)],
        );
        db.create_resource(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            "watchwidgets.watch.example.com",
            crd,
        )
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
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
        .expect("watch-driven CRD registry sync should observe datastore-applied CRD");

        cancel.cancel();
        watcher.await.unwrap();
    }
}
