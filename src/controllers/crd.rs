use crate::datastore::{DatastoreBackend, DatastoreHandle, WatchTarget};
use crate::watch::{
    EventType, SignalWatchCursor, WatchCursorError, WatchDeliveryScope, WatchEvent, WatchTopic,
    WindowPolicy,
};
use anyhow::Result;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct CrdResourceInfo {
    pub group: String,
    pub version: String,
    pub kind: String,
    pub plural: String,
    pub singular: String,
    pub namespaced: bool,
    pub selectable_fields: Vec<String>,
}

type CrdResourceMap = HashMap<(String, String, String), CrdResourceInfo>;

#[derive(Debug, Clone, Default)]
pub struct CrdRegistry {
    // (group, version, plural) -> CrdResourceInfo
    resources: Arc<RwLock<CrdResourceMap>>,
}

impl CrdRegistry {
    pub fn new() -> Self {
        Self {
            resources: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register(&self, info: CrdResourceInfo) {
        let key = (
            info.group.clone(),
            info.version.clone(),
            info.plural.clone(),
        );
        self.resources.write().await.insert(key, info);
    }

    pub async fn replace_all(&self, infos: Vec<CrdResourceInfo>) {
        let mut resources = self.resources.write().await;
        resources.clear();
        for info in infos {
            let key = (
                info.group.clone(),
                info.version.clone(),
                info.plural.clone(),
            );
            resources.insert(key, info);
        }
    }

    pub async fn get(&self, group: &str, version: &str, plural: &str) -> Option<CrdResourceInfo> {
        let key = (group.to_string(), version.to_string(), plural.to_string());
        self.resources.read().await.get(&key).cloned()
    }

    pub async fn list_group_versions(&self) -> Vec<(String, String)> {
        let resources = self.resources.read().await;
        let mut gvs: Vec<_> = resources
            .keys()
            .map(|(g, v, _)| (g.clone(), v.clone()))
            .collect();
        gvs.sort();
        gvs.dedup();
        gvs
    }

    pub async fn list_versions_by_group(&self) -> BTreeMap<String, Vec<String>> {
        let resources = self.resources.read().await;
        let mut grouped: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (group, version, _) in resources.keys() {
            grouped
                .entry(group.clone())
                .or_default()
                .insert(version.clone());
        }

        let mut result = BTreeMap::new();
        for (group, versions) in grouped {
            result.insert(group, versions.into_iter().collect());
        }
        result
    }

    pub async fn list_resources(&self, group: &str, version: &str) -> Vec<CrdResourceInfo> {
        let resources = self.resources.read().await;
        resources
            .values()
            .filter(|info| info.group == group && info.version == version)
            .cloned()
            .collect()
    }

    /// Remove a CRD from the registry by group, version, and plural name
    pub async fn remove(&self, group: &str, version: &str, plural: &str) {
        let key = (group.to_string(), version.to_string(), plural.to_string());
        self.resources.write().await.remove(&key);
    }
}

fn normalize_selectable_json_path(path: &str) -> Option<String> {
    let trimmed = path.trim();
    let stripped = trimmed
        .strip_prefix("$.")
        .or_else(|| trimmed.strip_prefix('.'))
        .or_else(|| trimmed.strip_prefix('$'))
        .unwrap_or(trimmed)
        .trim();
    if stripped.is_empty() {
        return None;
    }
    Some(stripped.to_string())
}

fn extract_selectable_fields_for_version(
    crd_value: &serde_json::Value,
    version_name: &str,
) -> Vec<String> {
    let mut fields = BTreeSet::new();
    let Some(versions) = crd_value
        .pointer("/spec/versions")
        .and_then(|v| v.as_array())
    else {
        return Vec::new();
    };

    for version in versions {
        if version.get("name").and_then(|v| v.as_str()) != Some(version_name) {
            continue;
        }
        if version.get("served").and_then(|v| v.as_bool()) != Some(true) {
            continue;
        }
        let Some(selectable) = version.get("selectableFields").and_then(|v| v.as_array()) else {
            continue;
        };
        for entry in selectable {
            if let Some(path) = entry.get("jsonPath").and_then(|v| v.as_str())
                && let Some(normalized) = normalize_selectable_json_path(path)
            {
                fields.insert(normalized);
            }
        }
    }

    fields.into_iter().collect()
}

pub async fn load_existing_crds(db: &dyn DatastoreBackend, registry: &CrdRegistry) -> Result<()> {
    sync_registry_from_datastore(db, registry).await
}

pub async fn sync_registry_from_datastore(
    db: &dyn DatastoreBackend,
    registry: &CrdRegistry,
) -> Result<()> {
    let crd_list = db
        .list_resources(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    let mut infos = Vec::new();
    for crd_resource in crd_list.items {
        let crd_raw: serde_json::Value = std::sync::Arc::unwrap_or_clone(crd_resource.data);
        infos.extend(crd_resource_infos_from_value(&crd_raw)?);
    }

    registry.replace_all(infos).await;
    Ok(())
}

pub async fn run_crd_registry_watch_with_components(
    db: DatastoreHandle,
    registry: CrdRegistry,
    _task_supervisor: Arc<crate::task_supervisor::TaskSupervisor>,
    cancel: CancellationToken,
) {
    let start_rv = db.get_current_resource_version().await.unwrap_or(0);
    if let Err(err) = sync_registry_from_datastore(db.as_ref(), &registry).await {
        tracing::warn!("crd_registry: initial sync failed: {err:#}");
    }

    let topic = WatchTopic::new("apiextensions.k8s.io/v1", "CustomResourceDefinition");
    let mut cursor = SignalWatchCursor::new(
        db.subscribe_watch_signals(topic.clone()),
        crate::datastore::sqlite::DatastoreWatchReplaySource::new(
            db.clone(),
            vec![WatchTarget::cluster(
                "apiextensions.k8s.io/v1",
                "CustomResourceDefinition",
            )],
        ),
        topic,
        WatchDeliveryScope::Cluster,
        start_rv,
        WindowPolicy::default_watch_delivery(),
    );
    if let Err(err) = cursor.prime_replay_or_expired().await {
        tracing::warn!(?err, "crd_registry: initial replay failed");
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            result = cursor.next_event() => {
                match result {
                    Ok(event) => {
                        if !is_crd_event(&event) {
                            continue;
                        }
                        if let Err(err) = sync_registry_from_datastore(db.as_ref(), &registry).await {
                            tracing::warn!("crd_registry: sync after watch event failed: {err:#}");
                        }
                    }
                    Err(WatchCursorError::Closed) => {
                        tracing::warn!("crd_registry: watch signal channel closed");
                        break;
                    }
                    Err(WatchCursorError::Expired) => {
                        tracing::warn!("crd_registry: replay window expired; running full resync");
                        if let Err(err) = sync_registry_from_datastore(db.as_ref(), &registry).await {
                            tracing::warn!("crd_registry: sync after expired replay failed: {err:#}");
                        }
                    }
                    Err(WatchCursorError::Replay(err)) => {
                        tracing::warn!("crd_registry: watch replay failed: {err:#}");
                    }
                }
            }
        }
    }
}

fn is_crd_event(event: &WatchEvent) -> bool {
    matches!(
        event.event_type,
        EventType::Added | EventType::Modified | EventType::Deleted
    ) && event.object.get("apiVersion").and_then(|v| v.as_str()) == Some("apiextensions.k8s.io/v1")
        && event.object.get("kind").and_then(|v| v.as_str()) == Some("CustomResourceDefinition")
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
    let crd: CustomResourceDefinition = serde_json::from_value(crd_value.clone())?;
    let spec = &crd.spec;
    let group = spec.group.clone();
    let scope_namespaced = spec.scope == "Namespaced";
    let mut infos = Vec::new();

    for version in &spec.versions {
        if !version.served {
            continue;
        }
        infos.push(CrdResourceInfo {
            group: group.clone(),
            version: version.name.clone(),
            kind: spec.names.kind.clone(),
            plural: spec.names.plural.clone(),
            singular: spec
                .names
                .singular
                .clone()
                .unwrap_or_else(|| spec.names.plural.to_lowercase()),
            namespaced: scope_namespaced,
            selectable_fields: extract_selectable_fields_for_version(crd_value, &version.name),
        });
        tracing::info!(
            "Registered CRD: {}/{} ({})",
            group,
            version.name,
            spec.names.plural
        );
    }

    Ok(infos)
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
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let cancel = CancellationToken::new();

        let watcher = tokio::spawn(run_crd_registry_watch_with_components(
            db_handle,
            registry.clone(),
            supervisor.clone(),
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
        supervisor.shutdown(Duration::from_secs(1)).await;
    }
}
