//! Side effect to enqueue HPAs after target or Pod mutations.

use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ReconcileKey;
use serde_json::Value;
use std::collections::HashSet;

#[async_trait]
pub(crate) trait HpaSideEffectStore: Send + Sync {
    async fn list_hpas(&self, api_version: &'static str, namespace: &str) -> Result<Vec<Resource>>;
}

pub(crate) async fn hpa_reconcile_keys_for_resource<Store: HpaSideEffectStore + ?Sized>(
    resource: &Value,
    store: &Store,
) -> Result<Vec<ReconcileKey>> {
    let namespace = resource
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if namespace.is_empty() {
        return Ok(Vec::new());
    }

    let api_version = resource
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let kind = resource.get("kind").and_then(|v| v.as_str()).unwrap_or("");

    if (api_version, kind) == ("v1", "Pod") {
        return hpa_reconcile_keys_for_namespace(store, namespace).await;
    }

    hpa_reconcile_keys_for_target(store, namespace, api_version, kind, resource).await
}

async fn hpa_reconcile_keys_for_namespace<Store: HpaSideEffectStore + ?Sized>(
    store: &Store,
    namespace: &str,
) -> Result<Vec<ReconcileKey>> {
    let mut keys = Vec::new();
    let mut seen = HashSet::new();
    append_hpa_keys_for_version(
        store,
        namespace,
        "autoscaling/v1",
        None,
        &mut seen,
        &mut keys,
    )
    .await?;
    append_hpa_keys_for_version(
        store,
        namespace,
        "autoscaling/v2",
        None,
        &mut seen,
        &mut keys,
    )
    .await?;
    Ok(keys)
}

async fn hpa_reconcile_keys_for_target<Store: HpaSideEffectStore + ?Sized>(
    store: &Store,
    namespace: &str,
    api_version: &str,
    kind: &str,
    resource: &Value,
) -> Result<Vec<ReconcileKey>> {
    let name = resource
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if name.is_empty() {
        return Ok(Vec::new());
    }

    let target = TargetRef {
        api_version,
        kind,
        name,
    };
    let mut keys = Vec::new();
    let mut seen = HashSet::new();
    append_hpa_keys_for_version(
        store,
        namespace,
        "autoscaling/v1",
        Some(target),
        &mut seen,
        &mut keys,
    )
    .await?;
    append_hpa_keys_for_version(
        store,
        namespace,
        "autoscaling/v2",
        Some(target),
        &mut seen,
        &mut keys,
    )
    .await?;
    Ok(keys)
}

#[derive(Clone, Copy)]
struct TargetRef<'a> {
    api_version: &'a str,
    kind: &'a str,
    name: &'a str,
}

async fn append_hpa_keys_for_version<Store: HpaSideEffectStore + ?Sized>(
    store: &Store,
    namespace: &str,
    hpa_api_version: &'static str,
    target: Option<TargetRef<'_>>,
    seen: &mut HashSet<(&'static str, String)>,
    keys: &mut Vec<ReconcileKey>,
) -> Result<()> {
    for hpa in store.list_hpas(hpa_api_version, namespace).await? {
        if let Some(target) = target
            && !hpa_targets_resource(&hpa.data, target)
        {
            continue;
        }
        if seen.insert((hpa_api_version, hpa.name.clone())) {
            keys.push(ReconcileKey::namespaced(
                hpa_api_version,
                "HorizontalPodAutoscaler",
                namespace,
                &hpa.name,
            ));
        }
    }
    Ok(())
}

fn hpa_targets_resource(hpa: &Value, target: TargetRef<'_>) -> bool {
    let target_ref = hpa.pointer("/spec/scaleTargetRef");
    let Some(target_ref) = target_ref else {
        return false;
    };
    let hpa_api_version = target_ref
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("apps/v1");
    hpa_api_version == target.api_version
        && target_ref.get("kind").and_then(|v| v.as_str()) == Some(target.kind)
        && target_ref.get("name").and_then(|v| v.as_str()) == Some(target.name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn hpa_target_mutation_enqueues_matching_hpa_versions_only() {
        let db = crate::datastore::test_support::in_memory().await;
        for (api_version, name, target_name) in [
            ("autoscaling/v1", "web-v1", "web"),
            ("autoscaling/v2", "web-v2", "web"),
            ("autoscaling/v2", "api-v2", "api"),
        ] {
            db.create_resource(
                api_version,
                "HorizontalPodAutoscaler",
                Some("default"),
                name,
                json!({
                    "apiVersion": api_version,
                    "kind": "HorizontalPodAutoscaler",
                    "metadata": {"name": name, "namespace": "default"},
                    "spec": {
                        "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": target_name},
                        "maxReplicas": 5
                    }
                }),
            )
            .await
            .unwrap();
        }

        let keys = hpa_reconcile_keys_for_resource(
            &json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "web", "namespace": "default"}
            }),
            &db,
        )
        .await
        .unwrap();

        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&ReconcileKey::namespaced(
            "autoscaling/v1",
            "HorizontalPodAutoscaler",
            "default",
            "web-v1"
        )));
        assert!(keys.contains(&ReconcileKey::namespaced(
            "autoscaling/v2",
            "HorizontalPodAutoscaler",
            "default",
            "web-v2"
        )));
    }

    #[tokio::test]
    async fn pod_mutation_enqueues_all_namespace_hpas() {
        let db = crate::datastore::test_support::in_memory().await;
        for (namespace, name) in [("default", "web"), ("other", "other")] {
            db.create_resource(
                "autoscaling/v2",
                "HorizontalPodAutoscaler",
                Some(namespace),
                name,
                json!({
                    "apiVersion": "autoscaling/v2",
                    "kind": "HorizontalPodAutoscaler",
                    "metadata": {"name": name, "namespace": namespace},
                    "spec": {
                        "scaleTargetRef": {"apiVersion": "apps/v1", "kind": "Deployment", "name": name},
                        "maxReplicas": 5
                    }
                }),
            )
            .await
            .unwrap();
        }

        let keys = hpa_reconcile_keys_for_resource(
            &json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "web-0", "namespace": "default"}
            }),
            &db,
        )
        .await
        .unwrap();

        assert_eq!(
            keys,
            vec![ReconcileKey::namespaced(
                "autoscaling/v2",
                "HorizontalPodAutoscaler",
                "default",
                "web"
            )]
        );
    }
}
