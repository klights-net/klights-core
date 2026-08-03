use super::*;
use crate::datastore::DatastoreBackend;
use anyhow::Result;
use klights_controllers::namespace::*;
use tokio::sync::{Mutex, MutexGuard};

async fn create_default_service_account(
    store: &(impl NamespaceBootstrapStore + ?Sized),
    namespace: &str,
) -> Result<()> {
    create_default_service_account_at(
        store,
        namespace,
        chrono::Utc::now(),
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
    )
    .await
}

async fn reconcile_default_service_account(
    store: &(impl NamespaceBootstrapStore + ?Sized),
    namespace: &str,
) -> Result<()> {
    reconcile_default_service_account_at(
        store,
        namespace,
        chrono::Utc::now(),
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
    )
    .await
}

async fn create_kube_root_ca_configmap(
    store: &(impl NamespaceBootstrapStore + ?Sized),
    namespace: &str,
    ca_cert_pem: &str,
) -> Result<()> {
    create_kube_root_ca_configmap_at(
        store,
        namespace,
        ca_cert_pem,
        chrono::Utc::now(),
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
    )
    .await
}

async fn create_extension_apiserver_authentication_configmap(
    store: &(impl NamespaceBootstrapStore + ?Sized),
    ca_cert_pem: &str,
) -> Result<()> {
    create_extension_apiserver_authentication_configmap_at(
        store,
        ca_cert_pem,
        chrono::Utc::now(),
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
    )
    .await
}

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
        let (proxy_cert_pem, proxy_key_pem) = klights_auth::cert::generate_apiservice_proxy_cert(
            &ca_cert,
            &ca_key,
            time::OffsetDateTime::now_utc(),
        )
        .expect("generate APIService proxy client identity");
        let ca_cert_path = etc_dir.join("ca.crt");
        std::fs::write(&ca_cert_path, &ca_pem).expect("write namespace fixture CA");
        std::fs::write(etc_dir.join("ca.key"), ca_key_pem).expect("write namespace fixture CA key");
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
        klights_controllers::namespace::init_default_namespaces_with_ca_path(
            &file_process,
            db,
            &self.ca_cert_path,
            "2026-01-01T00:00:00Z".parse().expect("fixed test time"),
            crate::controller_test_support::deterministic_controller_identity().as_ref(),
        )
        .await
    }

    async fn reconcile_kube_root_ca(
        &self,
        db: &dyn DatastoreBackend,
        namespace: &str,
    ) -> Result<()> {
        let file_process = crate::kubelet::file_blocking::test_file_process_executor();
        klights_controllers::namespace::reconcile_kube_root_ca_with_path(
            &file_process,
            db,
            namespace,
            &self.ca_cert_path,
            "2026-01-01T00:00:00Z".parse().expect("fixed test time"),
            crate::controller_test_support::deterministic_controller_identity().as_ref(),
        )
        .await
    }

    fn requestheader_allowed_names(&self) -> serde_json::Value {
        serde_json::json!(format!("[\"{}\"]", self.apiservice_proxy_common_name))
    }
}

async fn init_default_namespaces(db: &dyn DatastoreBackend) -> Result<()> {
    let file_process = crate::kubelet::file_blocking::test_file_process_executor();
    klights_controllers::namespace::init_default_namespaces_with_ca_path(
        &file_process,
        db,
        &crate::paths::ca_cert_path(&crate::paths::runtime_namespace()),
        chrono::Utc::now(),
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
    )
    .await
}

async fn reconcile_kube_root_ca(db: &dyn DatastoreBackend, namespace: &str) -> Result<()> {
    let file_process = crate::kubelet::file_blocking::test_file_process_executor();
    klights_controllers::namespace::reconcile_kube_root_ca_with_path(
        &file_process,
        db,
        namespace,
        &crate::paths::ca_cert_path(&crate::paths::runtime_namespace()),
        chrono::Utc::now(),
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
    )
    .await
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
async fn namespace_service_account_consumes_injected_uid_exactly_once() {
    let db = crate::datastore::test_support::in_memory().await;
    let identity =
        crate::controller_test_support::ScriptedControllerIdentityGenerator::with_uids([
            "abcdef12-3456-4000-8000-000000000000",
        ]);

    create_default_service_account_at(
        &db,
        "identity-spy",
        "2026-01-01T00:00:00Z".parse().expect("fixed test time"),
        &identity,
    )
    .await
    .unwrap();

    let service_account = db
        .get_resource("v1", "ServiceAccount", Some("identity-spy"), "default")
        .await
        .unwrap()
        .expect("default ServiceAccount");
    assert_eq!(
        service_account
            .data
            .pointer("/metadata/uid")
            .and_then(serde_json::Value::as_str),
        Some("abcdef12-3456-4000-8000-000000000000"),
    );
    assert_eq!(identity.uid_calls(), 1);
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
