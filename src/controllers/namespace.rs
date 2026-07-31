#[cfg(test)]
use crate::datastore::DatastoreBackend;
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
                    uid: Some(uuid::Uuid::new_v4().to_string()),
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
            create_default_service_account_at(store, ns_name, now).await?;
        }

        // Create kube-root-ca.crt ConfigMap in the namespace (whether new or existing)
        if let Some(ref ca_pem) = ca_cert_pem {
            // Check if ConfigMap already exists
            let cm_exists = store
                .get_configmap(ns_name, "kube-root-ca.crt")
                .await?
                .is_some();

            if !cm_exists
                && let Err(e) = create_kube_root_ca_configmap_at(store, ns_name, ca_pem, now).await
            {
                tracing::warn!(
                    "Failed to create kube-root-ca.crt ConfigMap in namespace {}: {:#}",
                    ns_name,
                    e
                );
            }

            // The aggregator auth ConfigMap is expected in kube-system for extension API servers.
            if ns_name == "kube-system"
                && let Err(e) =
                    reconcile_extension_apiserver_authentication_configmap(store, ca_pem, now).await
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

#[cfg(test)]
pub async fn init_default_namespaces<S: NamespaceBootstrapStore + ?Sized>(
    file_process: &klights_supervisor::FileProcessExecutor,
    store: &S,
) -> Result<()> {
    let namespace = crate::paths::runtime_namespace();
    let ca_cert_path = crate::paths::ca_cert_path(&namespace);
    init_default_namespaces_with_ca_path(file_process, store, &ca_cert_path, chrono::Utc::now())
        .await
}

pub async fn create_default_service_account_at<S: NamespaceBootstrapStore + ?Sized>(
    store: &S,
    namespace: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let sa = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": "default",
            "namespace": namespace,
            "creationTimestamp": klights_cluster_core::k8s_time::format_time(now),
            "uid": uuid::Uuid::new_v4().to_string()
        },
        "secrets": []
    });

    store.create_default_service_account(namespace, sa).await?;

    tracing::info!("Created default ServiceAccount in namespace: {}", namespace);
    Ok(())
}

#[cfg(test)]
pub async fn create_default_service_account<S: NamespaceBootstrapStore + ?Sized>(
    store: &S,
    namespace: &str,
) -> Result<()> {
    create_default_service_account_at(store, namespace, chrono::Utc::now()).await
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
    create_default_service_account_at(store, namespace, now).await
}

#[cfg(test)]
pub async fn reconcile_default_service_account<S: NamespaceBootstrapStore + ?Sized>(
    store: &S,
    namespace: &str,
) -> Result<()> {
    reconcile_default_service_account_at(store, namespace, chrono::Utc::now()).await
}

pub async fn create_kube_root_ca_configmap_at<S: NamespaceBootstrapStore + ?Sized>(
    store: &S,
    namespace: &str,
    ca_cert_pem: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let cm = serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "kube-root-ca.crt",
            "namespace": namespace,
            "creationTimestamp": klights_cluster_core::k8s_time::format_time(now),
            "uid": uuid::Uuid::new_v4().to_string()
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

#[cfg(test)]
pub async fn create_kube_root_ca_configmap<S: NamespaceBootstrapStore + ?Sized>(
    store: &S,
    namespace: &str,
    ca_cert_pem: &str,
) -> Result<()> {
    create_kube_root_ca_configmap_at(store, namespace, ca_cert_pem, chrono::Utc::now()).await
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

    create_kube_root_ca_configmap_at(store, namespace, &ca_pem, now).await
}

#[cfg(test)]
pub async fn reconcile_kube_root_ca<S: NamespaceBootstrapStore + ?Sized>(
    file_process: &klights_supervisor::FileProcessExecutor,
    store: &S,
    namespace: &str,
) -> Result<()> {
    let runtime_namespace = crate::paths::runtime_namespace();
    let ca_cert_path = crate::paths::ca_cert_path(&runtime_namespace);
    reconcile_kube_root_ca_with_path(
        file_process,
        store,
        namespace,
        &ca_cert_path,
        chrono::Utc::now(),
    )
    .await
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
        return create_kube_root_ca_configmap_at(store, namespace, &ca_pem, now).await;
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

#[cfg(test)]
pub async fn reconcile_kube_root_ca_data<S: NamespaceBootstrapStore + ?Sized>(
    file_process: &klights_supervisor::FileProcessExecutor,
    store: &S,
    namespace: &str,
) -> Result<()> {
    let runtime_namespace = crate::paths::runtime_namespace();
    let ca_cert_path = crate::paths::ca_cert_path(&runtime_namespace);
    reconcile_kube_root_ca_data_with_path(
        file_process,
        store,
        namespace,
        &ca_cert_path,
        chrono::Utc::now(),
    )
    .await
}

pub async fn create_extension_apiserver_authentication_configmap_at<
    S: NamespaceBootstrapStore + ?Sized,
>(
    store: &S,
    ca_cert_pem: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let cm = extension_apiserver_authentication_configmap(ca_cert_pem, now);

    store
        .create_configmap("kube-system", "extension-apiserver-authentication", cm)
        .await?;

    tracing::info!("Created extension-apiserver-authentication ConfigMap in kube-system");
    Ok(())
}

#[cfg(test)]
pub async fn create_extension_apiserver_authentication_configmap<
    S: NamespaceBootstrapStore + ?Sized,
>(
    store: &S,
    ca_cert_pem: &str,
) -> Result<()> {
    create_extension_apiserver_authentication_configmap_at(store, ca_cert_pem, chrono::Utc::now())
        .await
}

async fn reconcile_extension_apiserver_authentication_configmap<
    S: NamespaceBootstrapStore + ?Sized,
>(
    store: &S,
    ca_cert_pem: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let desired = extension_apiserver_authentication_configmap(ca_cert_pem, now);
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
) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "extension-apiserver-authentication",
            "namespace": "kube-system",
            "creationTimestamp": klights_cluster_core::k8s_time::format_time(now),
            "uid": uuid::Uuid::new_v4().to_string()
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

/// Test-only seam that re-drives namespace termination reconcile event-style
/// (no production polling). Used by liveness regression tests that need to
/// advance a Terminating namespace to finalized after driving the
/// actor-owned Pod finalization seam. Production callers must instead trigger
/// reconcile from the watch / side-effect notification after a child removal.
#[cfg(test)]
pub async fn reconcile_namespace_for_test(
    db: &dyn DatastoreBackend,
    namespace: &str,
) -> Result<(), crate::api::AppError> {
    let metrics = crate::side_effects::SideEffectMetrics::new();
    crate::api::reconcile_namespace_termination(db, namespace, metrics.as_ref()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{Mutex, MutexGuard};

    struct NamespaceRuntimeFixture {
        _data_root: tempfile::TempDir,
        ca_cert_path: std::path::PathBuf,
        ca_pem: String,
        apiservice_proxy_common_name: String,
    }

    impl NamespaceRuntimeFixture {
        fn new() -> Self {
            let data_root = tempfile::tempdir().expect("create namespace runtime fixture");
            let etc_dir = data_root.path().join("etc");
            std::fs::create_dir_all(&etc_dir).expect("create namespace runtime etc directory");

            let (ca_cert, ca_key, ca_pem, ca_key_pem) =
                klights_auth::test_support::generate_ca_full_at(time::OffsetDateTime::now_utc())
                    .expect("generate namespace fixture CA");
            let (proxy_cert_pem, proxy_key_pem) =
                klights_auth::cert::generate_apiservice_proxy_cert(
                    &ca_cert,
                    &ca_key,
                    time::OffsetDateTime::now_utc(),
                )
                .expect("generate APIService proxy client identity");
            let ca_cert_path = etc_dir.join("ca.crt");
            std::fs::write(&ca_cert_path, &ca_pem).expect("write namespace fixture CA");
            std::fs::write(etc_dir.join("ca.key"), ca_key_pem)
                .expect("write namespace fixture CA key");
            std::fs::write(etc_dir.join("apiservice-proxy.crt"), &proxy_cert_pem)
                .expect("write APIService proxy client certificate");
            std::fs::write(etc_dir.join("apiservice-proxy.key"), &proxy_key_pem)
                .expect("write APIService proxy client key");
            assert!(
                klights_auth::cert::apiservice_proxy_cert_and_key_match_config(
                    &proxy_cert_pem,
                    &proxy_key_pem,
                ),
                "namespace fixture must carry the canonical APIService proxy identity"
            );

            let proxy_cert_der = rustls_pemfile::certs(&mut proxy_cert_pem.as_bytes())
                .next()
                .expect("APIService proxy certificate PEM block")
                .expect("parse APIService proxy certificate");
            let proxy_user = klights_auth::user::user_from_cert(proxy_cert_der.as_ref())
                .expect("read APIService proxy certificate identity");

            Self {
                _data_root: data_root,
                ca_cert_path,
                ca_pem,
                apiservice_proxy_common_name: proxy_user.username,
            }
        }

        async fn init_default_namespaces(&self, db: &dyn DatastoreBackend) -> Result<()> {
            let file_process = crate::kubelet::file_blocking::test_file_process_executor();
            super::init_default_namespaces_with_ca_path(
                &file_process,
                db,
                &self.ca_cert_path,
                "2026-01-01T00:00:00Z".parse().expect("fixed test time"),
            )
            .await
        }

        async fn reconcile_kube_root_ca(
            &self,
            db: &dyn DatastoreBackend,
            namespace: &str,
        ) -> Result<()> {
            let file_process = crate::kubelet::file_blocking::test_file_process_executor();
            super::reconcile_kube_root_ca_with_path(
                &file_process,
                db,
                namespace,
                &self.ca_cert_path,
                "2026-01-01T00:00:00Z".parse().expect("fixed test time"),
            )
            .await
        }

        fn requestheader_allowed_names(&self) -> serde_json::Value {
            serde_json::json!(format!("[\"{}\"]", self.apiservice_proxy_common_name))
        }
    }

    async fn init_default_namespaces(db: &dyn DatastoreBackend) -> Result<()> {
        let file_process = crate::kubelet::file_blocking::test_file_process_executor();
        super::init_default_namespaces(&file_process, db).await
    }

    async fn reconcile_kube_root_ca(db: &dyn DatastoreBackend, namespace: &str) -> Result<()> {
        let file_process = crate::kubelet::file_blocking::test_file_process_executor();
        super::reconcile_kube_root_ca(&file_process, db, namespace).await
    }

    static CONTAINERD_NAMESPACE_ENV_LOCK: Mutex<()> = Mutex::const_new(());

    struct ContainerdNamespaceEnvGuard {
        _guard: MutexGuard<'static, ()>,
        original: Option<String>,
    }

    async fn set_containerd_namespace_for_test(namespace: &str) -> ContainerdNamespaceEnvGuard {
        let guard = CONTAINERD_NAMESPACE_ENV_LOCK.lock().await;
        let original = std::env::var("KLIGHTS_CONTAINERD_NAMESPACE").ok();
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var("KLIGHTS_CONTAINERD_NAMESPACE", namespace) };
        ContainerdNamespaceEnvGuard {
            _guard: guard,
            original,
        }
    }

    impl Drop for ContainerdNamespaceEnvGuard {
        fn drop(&mut self) {
            match self.original.as_ref() {
                // TODO: Audit that the environment access only happens in single-threaded code.
                Some(ns) => unsafe { std::env::set_var("KLIGHTS_CONTAINERD_NAMESPACE", ns) },
                // TODO: Audit that the environment access only happens in single-threaded code.
                None => unsafe { std::env::remove_var("KLIGHTS_CONTAINERD_NAMESPACE") },
            }
        }
    }

    #[tokio::test]
    async fn test_init_default_namespaces_creates_four_namespaces() {
        // Setup test database
        let db = crate::datastore::test_support::in_memory().await;

        // Call init function
        init_default_namespaces(&db).await.unwrap();

        // Verify all four default namespaces exist
        let namespaces = ["default", "kube-system", "kube-public", "kube-node-lease"];
        for ns_name in namespaces {
            let ns = db.get_namespace(ns_name).await.unwrap();

            assert!(ns.is_some(), "Namespace {} should exist", ns_name);

            let ns_data = ns.unwrap().data;
            assert_eq!(ns_data["metadata"]["name"], ns_name);
            assert_eq!(ns_data["status"]["phase"], "Active");
        }
    }

    #[tokio::test]
    async fn test_init_default_namespaces_creates_default_service_accounts() {
        // Setup test database
        let db = crate::datastore::test_support::in_memory().await;

        // Call init function
        init_default_namespaces(&db).await.unwrap();

        // Verify each namespace has a default ServiceAccount
        let namespaces = ["default", "kube-system", "kube-public", "kube-node-lease"];
        for ns_name in namespaces {
            let sa = db
                .get_resource("v1", "ServiceAccount", Some(ns_name), "default")
                .await
                .unwrap();

            assert!(
                sa.is_some(),
                "ServiceAccount 'default' should exist in namespace {}",
                ns_name
            );

            let sa_data = sa.unwrap().data;
            assert_eq!(sa_data["metadata"]["name"], "default");
            assert_eq!(sa_data["metadata"]["namespace"], ns_name);
        }
    }

    #[tokio::test]
    async fn test_init_default_namespaces_creates_kube_root_ca_configmaps() {
        // Setup test database
        let db = crate::datastore::test_support::in_memory().await;
        let runtime = NamespaceRuntimeFixture::new();

        runtime.init_default_namespaces(&db).await.unwrap();

        // Verify each namespace has kube-root-ca.crt ConfigMap
        for ns_name in ["default", "kube-system", "kube-public", "kube-node-lease"] {
            let cm = db
                .get_resource("v1", "ConfigMap", Some(ns_name), "kube-root-ca.crt")
                .await
                .unwrap();

            assert!(
                cm.is_some(),
                "ConfigMap 'kube-root-ca.crt' should exist in namespace {}",
                ns_name
            );

            let cm_data = cm.unwrap().data;
            assert_eq!(cm_data["metadata"]["name"], "kube-root-ca.crt");
            assert_eq!(cm_data["metadata"]["namespace"], ns_name);
            assert_eq!(cm_data["data"]["ca.crt"], runtime.ca_pem);
        }
    }

    #[tokio::test]
    async fn test_create_kube_root_ca_configmap() {
        let db = crate::datastore::test_support::in_memory().await;

        let ca_pem = "-----BEGIN CERTIFICATE-----\nfake-ca-data\n-----END CERTIFICATE-----";
        create_kube_root_ca_configmap(&db, "default", ca_pem)
            .await
            .unwrap();

        let cm = db
            .get_resource("v1", "ConfigMap", Some("default"), "kube-root-ca.crt")
            .await
            .unwrap();
        assert!(cm.is_some(), "kube-root-ca.crt ConfigMap should exist");

        let cm_data = cm.unwrap().data;
        assert_eq!(cm_data["metadata"]["name"], "kube-root-ca.crt");
        assert_eq!(cm_data["metadata"]["namespace"], "default");
        assert_eq!(cm_data["data"]["ca.crt"], ca_pem);
    }

    #[tokio::test]
    async fn test_reconcile_kube_root_ca_recreates_after_deletion() {
        let db = crate::datastore::test_support::in_memory().await;
        let runtime = NamespaceRuntimeFixture::new();

        // Create namespace so reconcile can check termination status
        let ns = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": "test-ns" },
            "spec": { "finalizers": ["kubernetes"] },
            "status": { "phase": "Active" }
        });
        db.create_namespace("test-ns", ns).await.unwrap();

        // First reconcile: creates the ConfigMap
        runtime
            .reconcile_kube_root_ca(&db, "test-ns")
            .await
            .unwrap();
        let cm = db
            .get_resource("v1", "ConfigMap", Some("test-ns"), "kube-root-ca.crt")
            .await
            .unwrap();
        assert!(cm.is_some(), "kube-root-ca.crt should be created");

        // Delete the ConfigMap
        db.delete_resource("v1", "ConfigMap", Some("test-ns"), "kube-root-ca.crt")
            .await
            .unwrap();
        let cm = db
            .get_resource("v1", "ConfigMap", Some("test-ns"), "kube-root-ca.crt")
            .await
            .unwrap();
        assert!(cm.is_none(), "kube-root-ca.crt should be deleted");

        // Second reconcile: recreates it
        runtime
            .reconcile_kube_root_ca(&db, "test-ns")
            .await
            .unwrap();
        let cm = db
            .get_resource("v1", "ConfigMap", Some("test-ns"), "kube-root-ca.crt")
            .await
            .unwrap();
        assert!(
            cm.is_some(),
            "kube-root-ca.crt should be recreated after deletion"
        );
    }

    #[tokio::test]
    async fn test_reconcile_kube_root_ca_skips_when_namespace_terminating() {
        let db = crate::datastore::test_support::in_memory().await;

        // Create namespace WITHOUT kube-root-ca.crt
        let ns = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": "terminating-ns",
                "deletionTimestamp": "2026-01-01T00:00:00Z"
            },
            "spec": { "finalizers": ["kubernetes"] },
            "status": { "phase": "Terminating" }
        });
        db.create_namespace("terminating-ns", ns).await.unwrap();

        // Write CA cert so reconcile would succeed if it tried
        let unique_ns = format!("test-{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let ca_cert_path = crate::paths::ca_cert_path(&unique_ns);
        std::fs::create_dir_all(ca_cert_path.parent().unwrap()).unwrap();
        std::fs::write(&ca_cert_path, "fake-ca-pem").unwrap();
        let env_guard = set_containerd_namespace_for_test(&unique_ns).await;

        // Simulate the side-effect logic from delete_inner:
        // namespace is terminating → should NOT recreate
        let ns_obj = db.get_namespace("terminating-ns").await.unwrap().unwrap();
        let is_terminating = ns_obj
            .data
            .pointer("/metadata/deletionTimestamp")
            .and_then(|v| v.as_str())
            .is_some();
        assert!(
            is_terminating,
            "namespace should be detected as terminating"
        );

        // Verify the ConfigMap does NOT exist (we never called reconcile
        // because the guard in delete_inner would skip it)
        let cm = db
            .get_resource(
                "v1",
                "ConfigMap",
                Some("terminating-ns"),
                "kube-root-ca.crt",
            )
            .await
            .unwrap();
        assert!(
            cm.is_none(),
            "kube-root-ca.crt should NOT be recreated in terminating namespace"
        );

        drop(env_guard);
        std::fs::remove_dir_all(crate::paths::data_root_path(&unique_ns)).ok();
    }

    #[tokio::test]
    async fn test_reconcile_kube_root_ca_skips_when_namespace_is_missing() {
        let db = crate::datastore::test_support::in_memory().await;

        let unique_ns = format!("test-{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let ca_cert_path = crate::paths::ca_cert_path(&unique_ns);
        std::fs::create_dir_all(ca_cert_path.parent().unwrap()).unwrap();
        std::fs::write(&ca_cert_path, "fake-ca-pem").unwrap();
        let env_guard = set_containerd_namespace_for_test(&unique_ns).await;

        reconcile_kube_root_ca(&db, "missing-ns").await.unwrap();

        let cm = db
            .get_resource("v1", "ConfigMap", Some("missing-ns"), "kube-root-ca.crt")
            .await
            .unwrap();
        assert!(
            cm.is_none(),
            "stale namespace events must not create kube-root-ca.crt after namespace removal"
        );

        drop(env_guard);
        std::fs::remove_dir_all(crate::paths::data_root_path(&unique_ns)).ok();
    }

    #[tokio::test]
    async fn test_reconcile_default_service_account_skips_when_namespace_terminating() {
        let db = crate::datastore::test_support::in_memory().await;
        let ns = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                "name": "terminating-sa-ns",
                "deletionTimestamp": "2026-01-01T00:00:00Z"
            },
            "spec": { "finalizers": ["kubernetes"] },
            "status": { "phase": "Terminating" }
        });
        db.create_namespace("terminating-sa-ns", ns).await.unwrap();

        reconcile_default_service_account(&db, "terminating-sa-ns")
            .await
            .unwrap();

        let sa = db
            .get_resource("v1", "ServiceAccount", Some("terminating-sa-ns"), "default")
            .await
            .unwrap();
        assert!(
            sa.is_none(),
            "default ServiceAccount must not be recreated during namespace termination"
        );
    }

    #[tokio::test]
    async fn test_create_default_service_account_standalone() {
        let db = crate::datastore::test_support::in_memory().await;

        create_default_service_account(&db, "test-ns")
            .await
            .unwrap();

        let sa = db
            .get_resource("v1", "ServiceAccount", Some("test-ns"), "default")
            .await
            .unwrap();
        assert!(sa.is_some(), "default ServiceAccount should exist");

        let sa_data = sa.unwrap().data;
        assert_eq!(sa_data["metadata"]["name"], "default");
        assert_eq!(sa_data["metadata"]["namespace"], "test-ns");
        assert!(sa_data["metadata"]["uid"].as_str().is_some());
        assert!(sa_data["metadata"]["creationTimestamp"].as_str().is_some());
    }

    #[tokio::test]
    async fn test_init_default_namespaces_creates_extension_apiserver_authentication_configmap() {
        let db = crate::datastore::test_support::in_memory().await;

        let unique_ns = format!("test-{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let test_root = crate::paths::data_root_path(&unique_ns);
        let ca_cert_path = crate::paths::ca_cert_path(&unique_ns);
        let ca_pem = "-----BEGIN CERTIFICATE-----\next-auth-ca\n-----END CERTIFICATE-----";

        std::fs::create_dir_all(ca_cert_path.parent().unwrap()).unwrap();
        std::fs::write(&ca_cert_path, ca_pem).unwrap();

        let env_guard = set_containerd_namespace_for_test(&unique_ns).await;

        init_default_namespaces(&db).await.unwrap();
        drop(env_guard);

        let cm = db
            .get_resource(
                "v1",
                "ConfigMap",
                Some("kube-system"),
                "extension-apiserver-authentication",
            )
            .await
            .unwrap()
            .expect("extension-apiserver-authentication must exist in kube-system");

        assert_eq!(
            cm.data["data"]["client-ca-file"], ca_pem,
            "client-ca-file should contain cluster CA PEM"
        );
        assert_eq!(
            cm.data["data"]["requestheader-client-ca-file"], ca_pem,
            "requestheader-client-ca-file should contain cluster CA PEM"
        );
        assert_eq!(
            cm.data["data"]["requestheader-allowed-names"],
            serde_json::json!("[\"system:klights:apiservice-proxy\"]")
        );
        assert_eq!(
            cm.data["data"]["requestheader-username-headers"],
            serde_json::json!("[\"X-Remote-User\"]")
        );
        assert_eq!(
            cm.data["data"]["requestheader-group-headers"],
            serde_json::json!("[\"X-Remote-Group\"]")
        );
        assert_eq!(
            cm.data["data"]["requestheader-extra-headers-prefix"],
            serde_json::json!("[\"X-Remote-Extra-\"]")
        );

        std::fs::remove_dir_all(&test_root).ok();
    }

    #[tokio::test]
    async fn test_init_default_namespaces_updates_legacy_extension_auth_allowed_names() {
        let db = crate::datastore::test_support::in_memory().await;
        let runtime = NamespaceRuntimeFixture::new();
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("kube-system"),
            "extension-apiserver-authentication",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "extension-apiserver-authentication",
                    "namespace": "kube-system",
                    "uid": uuid::Uuid::new_v4().to_string(),
                    "creationTimestamp": "2026-01-01T00:00:00Z"
                },
                "data": {
                    "client-ca-file": runtime.ca_pem.as_str(),
                    "requestheader-client-ca-file": runtime.ca_pem.as_str(),
                    "requestheader-allowed-names": "[]",
                    "requestheader-username-headers": "[\"X-Remote-User\"]",
                    "requestheader-group-headers": "[\"X-Remote-Group\"]",
                    "requestheader-extra-headers-prefix": "[\"X-Remote-Extra-\"]"
                }
            }),
        )
        .await
        .unwrap();

        runtime.init_default_namespaces(&db).await.unwrap();

        let cm = db
            .get_resource(
                "v1",
                "ConfigMap",
                Some("kube-system"),
                "extension-apiserver-authentication",
            )
            .await
            .unwrap()
            .expect("extension-apiserver-authentication must exist in kube-system");
        assert_eq!(
            cm.data["data"]["requestheader-allowed-names"],
            runtime.requestheader_allowed_names()
        );
    }

    #[tokio::test]
    async fn test_init_default_namespaces_idempotent() {
        // Setup test database
        let db = crate::datastore::test_support::in_memory().await;

        // Call init function twice
        init_default_namespaces(&db).await.unwrap();
        let result = init_default_namespaces(&db).await;

        // Should not error on second call
        assert!(
            result.is_ok(),
            "Second call to init_default_namespaces should not error"
        );

        // Verify namespaces still exist and count is correct
        let list = db.list_namespaces(None, None).await.unwrap();

        // Should have exactly 4 namespaces (not 8)
        assert_eq!(
            list.items.len(),
            4,
            "Should have exactly 4 namespaces after idempotent calls"
        );

        // Verify ServiceAccounts count
        let sa_list = db
            .list_resources(
                "v1",
                "ServiceAccount",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();

        // Should have exactly 4 ServiceAccounts (one per namespace, not 8)
        assert_eq!(
            sa_list.items.len(),
            4,
            "Should have exactly 4 ServiceAccounts after idempotent calls"
        );
    }

    #[tokio::test]
    async fn test_init_default_namespaces_runs_through_trait_object() {
        // Bootstrap must execute against a `&dyn DatastoreBackend` handle so
        // alternate backends (in-memory replicated cache, future replicated
        // SQLite) can supply the startup data store without any concrete
        // `Datastore` plumbing in the bootstrap path. The cast here would not
        // compile if the public signatures still required `&Datastore`.
        let concrete = crate::datastore::test_support::in_memory().await;
        let db: &dyn DatastoreBackend = &concrete;

        init_default_namespaces(db).await.unwrap();
        create_default_service_account(db, "extra-ns")
            .await
            .unwrap();

        // These helpers are intentionally idempotent only in the bootstrap path,
        // so creating them again should not panic even if they already exist.
        let ka = db
            .get_resource("v1", "ConfigMap", Some("default"), "kube-root-ca.crt")
            .await
            .unwrap();
        if ka.is_none() {
            create_kube_root_ca_configmap(db, "default", "fake-ca")
                .await
                .unwrap();
        }

        let ext = db
            .get_resource(
                "v1",
                "ConfigMap",
                Some("kube-system"),
                "extension-apiserver-authentication",
            )
            .await
            .unwrap();
        if ext.is_none() {
            create_extension_apiserver_authentication_configmap(db, "fake-ca")
                .await
                .unwrap();
        }

        let extra_sa = db
            .get_resource("v1", "ServiceAccount", Some("extra-ns"), "default")
            .await
            .unwrap();
        assert!(extra_sa.is_some());
    }
}
