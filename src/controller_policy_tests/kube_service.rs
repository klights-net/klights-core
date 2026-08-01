use klights_controllers::kube_service::*;
use std::sync::Mutex;

async fn init_default_namespaces(db: &dyn crate::datastore::DatastoreBackend) {
    klights_controllers::namespace::init_default_namespaces_with_ca_path(
        &crate::kubelet::file_blocking::test_file_process_executor(),
        db,
        &crate::paths::ca_cert_path(&crate::paths::runtime_namespace()),
        chrono::DateTime::UNIX_EPOCH,
        crate::controllers::test_utils::deterministic_controller_identity().as_ref(),
    )
    .await
    .unwrap();
}

async fn bootstrap_kubernetes_service_root(
    db: &dyn crate::datastore::DatastoreBackend,
    service_cidr: &str,
    tls_port: u16,
    datapath: &dyn klights_network_api::Datapath,
) -> anyhow::Result<()> {
    klights_controllers::kube_service::bootstrap_kubernetes_service(
        db,
        service_cidr,
        tls_port,
        datapath,
    )
    .await
}

async fn bootstrap_default_service_cidr_root(
    db: &dyn crate::datastore::DatastoreBackend,
    service_cidr: &str,
) -> anyhow::Result<()> {
    klights_controllers::kube_service::bootstrap_default_service_cidr(db, service_cidr).await
}

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvGuard {
    name: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvGuard {
    fn set(name: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(name);
        // TODO: Audit that the environment access only happens in single-threaded code.
        unsafe { std::env::set_var(name, value) };
        Self { name, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            // TODO: Audit that the environment access only happens in single-threaded code.
            Some(value) => unsafe { std::env::set_var(self.name, value) },
            // TODO: Audit that the environment access only happens in single-threaded code.
            None => unsafe { std::env::remove_var(self.name) },
        }
    }
}

#[test]
fn test_derive_kubernetes_service_ip_default() {
    assert_eq!(
        derive_kubernetes_service_ip("10.43.128.0/17"),
        "10.43.128.1"
    );
}

#[test]
fn test_derive_kubernetes_service_ip_custom() {
    assert_eq!(
        derive_kubernetes_service_ip("10.50.128.0/17"),
        "10.50.128.1"
    );
    assert_eq!(
        derive_kubernetes_service_ip("10.44.128.0/17"),
        "10.44.128.1"
    );
    assert_eq!(
        derive_kubernetes_service_ip("192.168.0.0/24"),
        "192.168.0.1"
    );
}

#[tokio::test]
async fn test_bootstrap_kubernetes_service_creates_service_and_endpoints() {
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;

    bootstrap_kubernetes_service_root(
        &db,
        "10.50.128.0/17",
        7444,
        &crate::networking::test_support::MockNetworkProvider::new(),
    )
    .await
    .unwrap();

    // Verify Service uses derived ClusterIP from service CIDR
    let svc = db
        .get_resource("v1", "Service", Some("default"), "kubernetes")
        .await
        .unwrap();
    assert!(svc.is_some(), "kubernetes Service should exist");
    let svc_data = svc.unwrap().data;
    assert_eq!(svc_data["spec"]["clusterIP"], "10.50.128.1");
    assert_eq!(svc_data["spec"]["ports"][0]["port"], 443);
    assert_eq!(svc_data["spec"]["ports"][0]["targetPort"], 7444);

    // Verify Endpoints exist with host IP and correct port
    let ep = db
        .get_resource("v1", "Endpoints", Some("default"), "kubernetes")
        .await
        .unwrap();
    assert!(ep.is_some(), "kubernetes Endpoints should exist");
    let ep_data = ep.unwrap().data;
    let subsets = ep_data["subsets"].as_array().unwrap();
    // Host IP varies by machine — just verify it's a non-empty IP
    let ip = subsets[0]["addresses"][0]["ip"].as_str().unwrap();
    assert!(!ip.is_empty(), "Endpoint IP should not be empty");
    assert_ne!(ip, "0.0.0.0", "Endpoint IP should not be 0.0.0.0");
    assert_eq!(subsets[0]["ports"][0]["port"], 7444);

    // P0-E2E-20260424b-08: verify EndpointSlice also exists
    let eps = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "kubernetes",
        )
        .await
        .unwrap();
    assert!(eps.is_some(), "kubernetes EndpointSlice should exist");
    let eps_data = eps.unwrap().data;
    assert_eq!(
        eps_data["metadata"]["labels"]["kubernetes.io/service-name"],
        "kubernetes"
    );
    assert_eq!(eps_data["addressType"], "IPv4");
    assert_eq!(eps_data["ports"][0]["port"], 7444);
    let eps_ip = eps_data["endpoints"][0]["addresses"][0]
        .as_str()
        .unwrap_or("");
    assert!(
        !eps_ip.is_empty(),
        "EndpointSlice address should not be empty"
    );
}

#[allow(clippy::await_holding_lock)] // ENV_LOCK serializes env-var-mutating tests.
#[tokio::test]
async fn test_bootstrap_kubernetes_service_uses_pod_gateway_not_underlay_host_ip() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _endpoint = EnvGuard::set("KLIGHTS_EXTERNAL_ENDPOINT", "198.51.100.74");
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;
    let network = crate::networking::test_support::MockNetworkProvider::new();
    network.set_host_ip(std::net::Ipv4Addr::new(10, 206, 0, 10));
    network.set_pod_gateway_ip(std::net::Ipv4Addr::new(10, 43, 0, 1));

    bootstrap_kubernetes_service_root(&db, "10.43.128.0/17", 7679, &network)
        .await
        .unwrap();

    let endpoints = db
        .get_resource("v1", "Endpoints", Some("default"), "kubernetes")
        .await
        .unwrap()
        .expect("kubernetes Endpoints should exist")
        .data;
    assert_eq!(endpoints["subsets"][0]["addresses"][0]["ip"], "10.43.0.1");

    let endpoint_slice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "kubernetes",
        )
        .await
        .unwrap()
        .expect("kubernetes EndpointSlice should exist")
        .data;
    assert_eq!(endpoint_slice["endpoints"][0]["addresses"][0], "10.43.0.1");
}

#[tokio::test]
async fn test_bootstrap_kubernetes_service_idempotent() {
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;

    bootstrap_kubernetes_service_root(
        &db,
        "10.43.128.0/17",
        7443,
        &crate::networking::test_support::MockNetworkProvider::new(),
    )
    .await
    .unwrap();
    let result = bootstrap_kubernetes_service_root(
        &db,
        "10.43.128.0/17",
        7443,
        &crate::networking::test_support::MockNetworkProvider::new(),
    )
    .await;
    assert!(result.is_ok(), "Second bootstrap call should not error");

    let svcs = db
        .list_resources(
            "v1",
            "Service",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let kubernetes_svcs: Vec<_> = svcs
        .items
        .iter()
        .filter(|r| r.name == "kubernetes")
        .collect();
    assert_eq!(
        kubernetes_svcs.len(),
        1,
        "Should have exactly 1 kubernetes Service"
    );
}

#[tokio::test]
async fn test_bootstrap_kubernetes_service_reconciles_existing_tls_port() {
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;
    let network = crate::networking::test_support::MockNetworkProvider::new();

    bootstrap_kubernetes_service_root(&db, "10.43.128.0/17", 7443, &network)
        .await
        .unwrap();
    bootstrap_kubernetes_service_root(&db, "10.43.128.0/17", 7679, &network)
        .await
        .unwrap();

    let service = db
        .get_resource("v1", "Service", Some("default"), "kubernetes")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        service.data["spec"]["ports"][0]["targetPort"].as_i64(),
        Some(7679)
    );

    let endpoints = db
        .get_resource("v1", "Endpoints", Some("default"), "kubernetes")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        endpoints.data["subsets"][0]["ports"][0]["port"].as_i64(),
        Some(7679)
    );

    let endpoint_slice = db
        .get_resource(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            "kubernetes",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint_slice.data["ports"][0]["port"].as_i64(), Some(7679));
}

#[tokio::test]
async fn test_bootstrap_default_service_cidr_creates_resource() {
    let db = crate::datastore::test_support::in_memory().await;

    bootstrap_default_service_cidr_root(&db, "10.43.128.0/17")
        .await
        .unwrap();

    let sc = db
        .get_resource("networking.k8s.io/v1", "ServiceCIDR", None, "kubernetes")
        .await
        .unwrap();
    assert!(sc.is_some(), "default ServiceCIDR should exist");
    let sc_data = sc.unwrap().data;
    assert_eq!(sc_data["spec"]["cidrs"][0], "10.43.128.0/17");
}

#[tokio::test]
async fn test_bootstrap_default_service_cidr_idempotent() {
    let db = crate::datastore::test_support::in_memory().await;

    bootstrap_default_service_cidr_root(&db, "10.43.128.0/17")
        .await
        .unwrap();
    bootstrap_default_service_cidr_root(&db, "10.43.128.0/17")
        .await
        .unwrap();

    let list = db
        .list_resources(
            "networking.k8s.io/v1",
            "ServiceCIDR",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let defaults: Vec<_> = list
        .items
        .iter()
        .filter(|r| r.name == "kubernetes")
        .collect();
    assert_eq!(
        defaults.len(),
        1,
        "must only create one default ServiceCIDR"
    );
}
