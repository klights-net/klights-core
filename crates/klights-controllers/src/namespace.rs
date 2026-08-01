use anyhow::Result;
use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Namespace, NamespaceSpec, NamespaceStatus};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use klights_cluster_core::Resource;
use klights_reconcile_api::ControllerStoreResult;

const DEFAULT_NAMESPACES: [&str; 4] = ["default", "kube-system", "kube-public", "kube-node-lease"];

#[async_trait]
pub trait NamespaceBootstrapStore: Send + Sync {
    async fn get_namespace(&self, name: &str) -> ControllerStoreResult<Option<Resource>>;
    async fn create_namespace(
        &self,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource>;
    async fn get_default_service_account(
        &self,
        namespace: &str,
    ) -> ControllerStoreResult<Option<Resource>>;
    async fn create_default_service_account(
        &self,
        namespace: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource>;
    async fn get_configmap(
        &self,
        namespace: &str,
        name: &str,
    ) -> ControllerStoreResult<Option<Resource>>;
    async fn create_configmap(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
    ) -> ControllerStoreResult<Resource>;
    async fn update_configmap(
        &self,
        namespace: &str,
        name: &str,
        value: serde_json::Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource>;
}

pub async fn init_default_namespaces_with_ca_path<S: NamespaceBootstrapStore + ?Sized>(
    file_process: &klights_supervisor::FileProcessExecutor,
    store: &S,
    ca_cert_path: &std::path::Path,
    now: chrono::DateTime<chrono::Utc>,
    identity: &dyn crate::ControllerIdentityGenerator,
) -> Result<()> {
    // Read CA cert once (will be used for all namespaces)
    let ca_cert_pem = klights_supervisor::runtime_fs::read_utf8_async(file_process, ca_cert_path)
        .await
        .ok();

    for ns_name in DEFAULT_NAMESPACES {
        // Check if namespace already exists (use new get_namespace method)
        let exists = store.get_namespace(ns_name).await?.is_some();

        if !exists {
            let namespace = Namespace {
                metadata: ObjectMeta {
                    name: Some(ns_name.to_string()),
                    creation_timestamp: Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                        now,
                    )),
                    uid: Some(identity.new_uid()),
                    ..Default::default()
                },
                spec: Some(NamespaceSpec {
                    finalizers: Some(vec!["kubernetes".to_string()]),
                }),
                status: Some(NamespaceStatus {
                    phase: Some("Active".to_string()),
                    ..Default::default()
                }),
            };

            let namespace_json = serde_json::to_value(&namespace)?;
            // Use new create_namespace method (handles PRIMARY KEY uniqueness)
            store.create_namespace(ns_name, namespace_json).await?;
            tracing::info!("Created default namespace: {}", ns_name);

            // Create default ServiceAccount in the namespace
            create_default_service_account_at(store, ns_name, now, identity).await?;
        }

        // Create kube-root-ca.crt ConfigMap in the namespace (whether new or existing)
        if let Some(ref ca_pem) = ca_cert_pem {
            // Check if ConfigMap already exists
            let cm_exists = store
                .get_configmap(ns_name, "kube-root-ca.crt")
                .await?
                .is_some();

            if !cm_exists
                && let Err(e) =
                    create_kube_root_ca_configmap_at(store, ns_name, ca_pem, now, identity).await
            {
                tracing::warn!(
                    "Failed to create kube-root-ca.crt ConfigMap in namespace {}: {:#}",
                    ns_name,
                    e
                );
            }

            // The aggregator auth ConfigMap is expected in kube-system for extension API servers.
            if ns_name == "kube-system"
                && let Err(e) = reconcile_extension_apiserver_authentication_configmap(
                    store, ca_pem, now, identity,
                )
                .await
            {
                tracing::warn!(
                    "Failed to reconcile extension-apiserver-authentication ConfigMap: {:#}",
                    e
                );
            }
        } else {
            tracing::warn!(
                "CA cert not found at {}, skipping kube-root-ca.crt ConfigMap creation",
                ca_cert_path.display()
            );
        }
    }

    Ok(())
}

pub async fn create_default_service_account_at<S: NamespaceBootstrapStore + ?Sized>(
    store: &S,
    namespace: &str,
    now: chrono::DateTime<chrono::Utc>,
    identity: &dyn crate::ControllerIdentityGenerator,
) -> Result<()> {
    let sa = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": "default",
            "namespace": namespace,
            "creationTimestamp": klights_cluster_core::k8s_time::format_time(now),
            "uid": identity.new_uid()
        },
        "secrets": []
    });

    store.create_default_service_account(namespace, sa).await?;

    tracing::info!("Created default ServiceAccount in namespace: {}", namespace);
    Ok(())
}

/// Reconcile the default ServiceAccount in a namespace.
///
/// This is event-driven maintenance for active namespaces only. It deliberately
/// skips missing or terminating namespaces so namespace finalization can delete
/// ServiceAccounts without racing a recreate.
pub async fn reconcile_default_service_account_at<S: NamespaceBootstrapStore + ?Sized>(
    store: &S,
    namespace: &str,
    now: chrono::DateTime<chrono::Utc>,
    identity: &dyn crate::ControllerIdentityGenerator,
) -> Result<()> {
    if namespace_absent_or_terminating(store, namespace).await? {
        return Ok(());
    }
    if store
        .get_default_service_account(namespace)
        .await?
        .is_some()
    {
        return Ok(());
    }
    create_default_service_account_at(store, namespace, now, identity).await
}

pub async fn create_kube_root_ca_configmap_at<S: NamespaceBootstrapStore + ?Sized>(
    store: &S,
    namespace: &str,
    ca_cert_pem: &str,
    now: chrono::DateTime<chrono::Utc>,
    identity: &dyn crate::ControllerIdentityGenerator,
) -> Result<()> {
    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "kube-root-ca.crt",
            "namespace": namespace,
            "creationTimestamp": klights_cluster_core::k8s_time::format_time(now),
            "uid": identity.new_uid()
        },
        "data": {
            "ca.crt": ca_cert_pem
        }
    });

    store
        .create_configmap(namespace, "kube-root-ca.crt", cm)
        .await?;

    tracing::info!(
        "Created kube-root-ca.crt ConfigMap in namespace: {}",
        namespace
    );
    Ok(())
}

/// Check if a namespace is absent or terminating.
async fn namespace_absent_or_terminating<S: NamespaceBootstrapStore + ?Sized>(
    store: &S,
    namespace: &str,
) -> Result<bool> {
    let Some(ns) = store.get_namespace(namespace).await? else {
        return Ok(true);
    };
    Ok(ns
        .data
        .pointer("/metadata/deletionTimestamp")
        .and_then(|v| v.as_str())
        .is_some_and(|s| !s.is_empty()))
}

/// Reconcile `kube-root-ca.crt` in a namespace: read the CA from the
/// bootstrap file and create the ConfigMap if it does not exist.
/// Skips if the namespace is terminating.
pub async fn reconcile_kube_root_ca_with_path<S: NamespaceBootstrapStore + ?Sized>(
    file_process: &klights_supervisor::FileProcessExecutor,
    store: &S,
    namespace: &str,
    ca_cert_path: &std::path::Path,
    now: chrono::DateTime<chrono::Utc>,
    identity: &dyn crate::ControllerIdentityGenerator,
) -> Result<()> {
    if namespace_absent_or_terminating(store, namespace).await? {
        return Ok(());
    }

    // Skip if it already exists
    if store
        .get_configmap(namespace, "kube-root-ca.crt")
        .await?
        .is_some()
    {
        return Ok(());
    }

    // Read the CA cert from the bootstrap file
    let ca_pem =
        match klights_supervisor::runtime_fs::read_utf8_async(file_process, ca_cert_path).await {
            Ok(pem) => pem,
            Err(e) => {
                tracing::warn!("Cannot read CA cert from {}: {e}", ca_cert_path.display());
                return Ok(());
            }
        };

    create_kube_root_ca_configmap_at(store, namespace, &ca_pem, now, identity).await
}

/// Reconcile `kube-root-ca.crt` data in a namespace: read the CA from
/// the bootstrap file and update the existing ConfigMap's `ca.crt` key.
/// Used when the data is cleared or modified by a user.
/// Skips if the namespace is terminating.
pub async fn reconcile_kube_root_ca_data_with_path<S: NamespaceBootstrapStore + ?Sized>(
    file_process: &klights_supervisor::FileProcessExecutor,
    store: &S,
    namespace: &str,
    ca_cert_path: &std::path::Path,
    now: chrono::DateTime<chrono::Utc>,
    identity: &dyn crate::ControllerIdentityGenerator,
) -> Result<()> {
    if namespace_absent_or_terminating(store, namespace).await? {
        return Ok(());
    }

    // Read the CA cert from the bootstrap file
    let ca_pem =
        match klights_supervisor::runtime_fs::read_utf8_async(file_process, ca_cert_path).await {
            Ok(pem) => pem,
            Err(e) => {
                tracing::warn!("Cannot read CA cert from {}: {e}", ca_cert_path.display());
                return Ok(());
            }
        };

    // Get current CM and update its data
    let Some(cm) = store.get_configmap(namespace, "kube-root-ca.crt").await? else {
        // CM doesn't exist, use the create path
        return create_kube_root_ca_configmap_at(store, namespace, &ca_pem, now, identity).await;
    };

    // Check if data already matches
    let current_ca = cm
        .data
        .pointer("/data/ca.crt")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if current_ca == ca_pem {
        return Ok(()); // Already correct
    }

    let mut updated: serde_json::Value = (*cm.data).clone();
    if let Some(data) = updated.pointer_mut("/data/ca.crt") {
        *data = serde_json::Value::String(ca_pem);
    }

    store
        .update_configmap(namespace, "kube-root-ca.crt", updated, cm.resource_version)
        .await?;

    tracing::info!(
        "Reconciled kube-root-ca.crt data in namespace: {}",
        namespace
    );
    Ok(())
}

pub async fn create_extension_apiserver_authentication_configmap_at<
    S: NamespaceBootstrapStore + ?Sized,
>(
    store: &S,
    ca_cert_pem: &str,
    now: chrono::DateTime<chrono::Utc>,
    identity: &dyn crate::ControllerIdentityGenerator,
) -> Result<()> {
    let cm = extension_apiserver_authentication_configmap(ca_cert_pem, now, identity);

    store
        .create_configmap("kube-system", "extension-apiserver-authentication", cm)
        .await?;

    tracing::info!("Created extension-apiserver-authentication ConfigMap in kube-system");
    Ok(())
}

async fn reconcile_extension_apiserver_authentication_configmap<
    S: NamespaceBootstrapStore + ?Sized,
>(
    store: &S,
    ca_cert_pem: &str,
    now: chrono::DateTime<chrono::Utc>,
    identity: &dyn crate::ControllerIdentityGenerator,
) -> Result<()> {
    let desired = extension_apiserver_authentication_configmap(ca_cert_pem, now, identity);
    let desired_data = desired["data"].clone();
    let Some(existing) = store
        .get_configmap("kube-system", "extension-apiserver-authentication")
        .await?
    else {
        store
            .create_configmap("kube-system", "extension-apiserver-authentication", desired)
            .await?;
        tracing::info!("Created extension-apiserver-authentication ConfigMap in kube-system");
        return Ok(());
    };

    if existing.data.get("data") == Some(&desired_data) {
        return Ok(());
    }

    let mut updated = (*existing.data).clone();
    if let Some(object) = updated.as_object_mut() {
        object.insert("data".to_string(), desired_data);
    }
    store
        .update_configmap(
            "kube-system",
            "extension-apiserver-authentication",
            updated,
            existing.resource_version,
        )
        .await?;
    tracing::info!("Updated extension-apiserver-authentication ConfigMap in kube-system");
    Ok(())
}

fn extension_apiserver_authentication_configmap(
    ca_cert_pem: &str,
    now: chrono::DateTime<chrono::Utc>,
    identity: &dyn crate::ControllerIdentityGenerator,
) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "extension-apiserver-authentication",
            "namespace": "kube-system",
            "creationTimestamp": klights_cluster_core::k8s_time::format_time(now),
            "uid": identity.new_uid()
        },
        "data": {
            "client-ca-file": ca_cert_pem,
            "requestheader-client-ca-file": ca_cert_pem,
            "requestheader-allowed-names": format!(
                "[\"{}\"]",
                klights_types::APISERVICE_PROXY_COMMON_NAME
            ),
            "requestheader-username-headers": "[\"X-Remote-User\"]",
            "requestheader-group-headers": "[\"X-Remote-Group\"]",
            "requestheader-extra-headers-prefix": "[\"X-Remote-Extra-\"]"
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct FixedIdentity;

    impl crate::ControllerIdentityGenerator for FixedIdentity {
        fn generate_name(&self, prefix: &str) -> String {
            format!("{prefix}fixed")
        }

        fn new_uid(&self) -> String {
            "00000000-0000-4000-8000-000000000001".to_string()
        }
    }

    struct MockNamespaceStore {
        namespace: Resource,
        service_accounts: Mutex<Vec<serde_json::Value>>,
    }

    impl MockNamespaceStore {
        fn new(terminating: bool) -> Self {
            let deletion = terminating.then_some("2026-01-01T00:00:00Z");
            Self {
                namespace: Resource::from_data_lossy(Arc::new(serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": "team-a", "deletionTimestamp": deletion}
                }))),
                service_accounts: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl NamespaceBootstrapStore for MockNamespaceStore {
        async fn get_namespace(&self, _name: &str) -> ControllerStoreResult<Option<Resource>> {
            Ok(Some(self.namespace.clone()))
        }

        async fn create_namespace(
            &self,
            _name: &str,
            _value: serde_json::Value,
        ) -> ControllerStoreResult<Resource> {
            unreachable!("namespace already exists")
        }

        async fn get_default_service_account(
            &self,
            _namespace: &str,
        ) -> ControllerStoreResult<Option<Resource>> {
            Ok(None)
        }

        async fn create_default_service_account(
            &self,
            _namespace: &str,
            value: serde_json::Value,
        ) -> ControllerStoreResult<Resource> {
            self.service_accounts.lock().unwrap().push(value.clone());
            Ok(Resource::from_data_lossy(Arc::new(value)))
        }

        async fn get_configmap(
            &self,
            _namespace: &str,
            _name: &str,
        ) -> ControllerStoreResult<Option<Resource>> {
            Ok(None)
        }

        async fn create_configmap(
            &self,
            _namespace: &str,
            _name: &str,
            value: serde_json::Value,
        ) -> ControllerStoreResult<Resource> {
            Ok(Resource::from_data_lossy(Arc::new(value)))
        }

        async fn update_configmap(
            &self,
            _namespace: &str,
            _name: &str,
            value: serde_json::Value,
            _expected_resource_version: i64,
        ) -> ControllerStoreResult<Resource> {
            Ok(Resource::from_data_lossy(Arc::new(value)))
        }
    }

    #[tokio::test]
    async fn default_service_account_reconcile_uses_injected_time() {
        let store = MockNamespaceStore::new(false);
        let now = "2026-01-02T03:04:05Z".parse().unwrap();
        reconcile_default_service_account_at(&store, "team-a", now, &FixedIdentity)
            .await
            .unwrap();

        let service_accounts = store.service_accounts.lock().unwrap();
        assert_eq!(service_accounts.len(), 1);
        assert_eq!(
            service_accounts[0].pointer("/metadata/creationTimestamp"),
            Some(&serde_json::json!("2026-01-02T03:04:05Z"))
        );
        assert_eq!(
            service_accounts[0].pointer("/metadata/uid"),
            Some(&serde_json::json!("00000000-0000-4000-8000-000000000001"))
        );
    }

    #[tokio::test]
    async fn terminating_namespace_does_not_recreate_service_account() {
        let store = MockNamespaceStore::new(true);
        reconcile_default_service_account_at(&store, "team-a", chrono::Utc::now(), &FixedIdentity)
            .await
            .unwrap();
        assert!(store.service_accounts.lock().unwrap().is_empty());
    }
}
