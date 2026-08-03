use super::*;
use crate::bootstrap::controller_adapters::coredns_bootstrap_adapter::bootstrap_coredns as bootstrap_coredns_root;
use klights_controllers::coredns::*;
use klights_reconcile_api::ControllerStoreResult;
use serde_json::json;
use std::sync::Mutex;

async fn init_default_namespaces(db: &dyn crate::datastore::DatastoreBackend) {
    klights_controllers::namespace::init_default_namespaces_with_ca_path(
        &crate::kubelet::file_blocking::test_file_process_executor(),
        db,
        &crate::paths::ca_cert_path(&crate::paths::runtime_namespace()),
        chrono::DateTime::UNIX_EPOCH,
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
    )
    .await
    .unwrap();
}

async fn bootstrap_coredns(
    db: &dyn crate::datastore::DatastoreBackend,
    pod_repository: std::sync::Arc<crate::kubelet::pod_repository::PodRepository>,
    tls_port: u16,
    service_cidr: &str,
    containerd_namespace: &str,
    node_name: &str,
) -> anyhow::Result<()> {
    let controller_pods = std::sync::Arc::new(
        crate::bootstrap::controller_adapters::controller_runtime_adapter::RootControllerPodPort::new_for_test(
            pod_repository.clone(),
        ),
    );
    bootstrap_coredns_root(
        db,
        pod_repository.clone(),
        controller_pods,
        pod_repository,
        crate::controller_test_support::non_pod_finalization_port_for_test(),
        &klights_controllers::ControllerCoordination::new(),
        crate::controller_test_support::deterministic_controller_identity().as_ref(),
        crate::bootstrap::controller_adapters::coredns_bootstrap_adapter::CoreDnsBootstrapConfig {
            tls_port,
            service_cidr,
            containerd_namespace,
            node_name,
        },
    )
    .await
}

#[derive(Default)]
struct FakeCoreDnsStore {
    created: Mutex<Vec<CoreDnsResourceKind>>,
    reconciled_deployments: Mutex<usize>,
}

#[async_trait]
impl CoreDnsBootstrapStore for FakeCoreDnsStore {
    async fn get_coredns_resource(
        &self,
        _kind: CoreDnsResourceKind,
    ) -> ControllerStoreResult<Option<Resource>> {
        Ok(None)
    }

    async fn create_coredns_resource(
        &self,
        kind: CoreDnsResourceKind,
        value: Value,
    ) -> ControllerStoreResult<Resource> {
        self.created.lock().unwrap().push(kind);
        Resource::try_from_data(std::sync::Arc::new(value)).map_err(|error| {
            klights_reconcile_api::ControllerStoreError::internal(error.to_string())
        })
    }

    async fn update_coredns_resource(
        &self,
        _kind: CoreDnsResourceKind,
        _value: Value,
        _expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource> {
        unreachable!("empty fake store never updates")
    }

    async fn reconcile_coredns_deployment(
        &self,
        _deployment: Resource,
        _node_name: &str,
    ) -> ControllerStoreResult<()> {
        *self.reconciled_deployments.lock().unwrap() += 1;
        Ok(())
    }
}

#[tokio::test]
async fn focused_store_bootstrap_creates_exact_resource_family_once() {
    let store = FakeCoreDnsStore::default();
    bootstrap_coredns_with_store(&store, 6443, "10.43.128.0/17", "klights", "node-a")
        .await
        .unwrap();
    assert_eq!(
        *store.created.lock().unwrap(),
        vec![
            CoreDnsResourceKind::ServiceAccount,
            CoreDnsResourceKind::ClusterRole,
            CoreDnsResourceKind::ClusterRoleBinding,
            CoreDnsResourceKind::ConfigMap,
            CoreDnsResourceKind::Deployment,
            CoreDnsResourceKind::Service,
        ]
    );
    assert_eq!(*store.reconciled_deployments.lock().unwrap(), 1);
}

#[tokio::test]
async fn test_bootstrap_coredns_creates_all_resources() {
    let db = crate::datastore::test_support::in_memory().await;

    // Bootstrap needs kube-system namespace
    init_default_namespaces(&db).await;

    bootstrap_coredns(
        &db,
        crate::controller_test_support::pod_repository_for_test(&db),
        7443,
        "10.43.128.0/17",
        "klights",
        "test-node",
    )
    .await
    .unwrap();

    // Verify ConfigMap
    let cm = db
        .get_resource("v1", "ConfigMap", Some("kube-system"), "coredns")
        .await
        .unwrap();
    assert!(cm.is_some(), "CoreDNS ConfigMap should exist");
    let cm_data = cm.unwrap().data;
    let corefile = cm_data["data"]["Corefile"].as_str().unwrap();
    assert!(
        corefile.contains("kubernetes cluster.local"),
        "Corefile should contain kubernetes plugin"
    );
    assert!(
        !corefile.contains("kubeconfig "),
        "CoreDNS must use in-cluster service account config, not a node-local kubeconfig"
    );

    // Verify Deployment
    let deploy = db
        .get_resource("apps/v1", "Deployment", Some("kube-system"), "coredns")
        .await
        .unwrap();
    assert!(deploy.is_some(), "CoreDNS Deployment should exist");
    let deploy_data = deploy.unwrap().data;
    assert_eq!(deploy_data["spec"]["replicas"], 1);
    assert_eq!(
        deploy_data["spec"]["template"]["spec"]["containers"][0]["image"],
        "coredns/coredns:1.11.1"
    );

    // Verify Service
    let svc = db
        .get_resource("v1", "Service", Some("kube-system"), "kube-dns")
        .await
        .unwrap();
    assert!(svc.is_some(), "kube-dns Service should exist");
    let svc_data = svc.unwrap().data;
    assert_eq!(svc_data["spec"]["clusterIP"], "10.43.128.10");
}

#[tokio::test]
async fn test_bootstrap_coredns_creates_serviceaccount_and_rbac() {
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;

    bootstrap_coredns(
        &db,
        crate::controller_test_support::pod_repository_for_test(&db),
        7679,
        "10.43.128.0/17",
        "klights",
        "test-node",
    )
    .await
    .unwrap();

    let service_account = db
        .get_resource("v1", "ServiceAccount", Some("kube-system"), "coredns")
        .await
        .unwrap();
    assert!(
        service_account.is_some(),
        "CoreDNS projected tokens must be bound to an existing kube-system/coredns ServiceAccount"
    );

    let cluster_role = db
        .get_resource(
            "rbac.authorization.k8s.io/v1",
            "ClusterRole",
            None,
            "system:coredns",
        )
        .await
        .unwrap()
        .expect("CoreDNS ClusterRole must exist");
    let rules = cluster_role
        .data
        .pointer("/rules")
        .and_then(|rules| rules.as_array())
        .expect("CoreDNS ClusterRole must have rules");
    assert!(
        rules.iter().any(|rule| {
            rule["apiGroups"]
                .as_array()
                .is_some_and(|groups| groups.iter().any(|group| group.as_str() == Some("")))
                && rule["resources"].as_array().is_some_and(|resources| {
                    ["endpoints", "namespaces", "pods", "services"]
                        .iter()
                        .all(|expected| {
                            resources
                                .iter()
                                .any(|resource| resource.as_str() == Some(*expected))
                        })
                })
                && rule["verbs"].as_array().is_some_and(|verbs| {
                    ["list", "watch"]
                        .iter()
                        .all(|expected| verbs.iter().any(|verb| verb.as_str() == Some(*expected)))
                })
        }),
        "CoreDNS ClusterRole must allow list/watch for core service discovery resources"
    );
    assert!(
        rules.iter().any(|rule| {
            rule["apiGroups"].as_array().is_some_and(|groups| {
                groups
                    .iter()
                    .any(|group| group.as_str() == Some("discovery.k8s.io"))
            }) && rule["resources"].as_array().is_some_and(|resources| {
                resources
                    .iter()
                    .any(|resource| resource.as_str() == Some("endpointslices"))
            }) && rule["verbs"].as_array().is_some_and(|verbs| {
                ["list", "watch"]
                    .iter()
                    .all(|expected| verbs.iter().any(|verb| verb.as_str() == Some(*expected)))
            })
        }),
        "CoreDNS ClusterRole must allow list/watch for EndpointSlices"
    );

    let binding = db
        .get_resource(
            "rbac.authorization.k8s.io/v1",
            "ClusterRoleBinding",
            None,
            "system:coredns",
        )
        .await
        .unwrap()
        .expect("CoreDNS ClusterRoleBinding must exist");
    assert_eq!(
        binding
            .data
            .pointer("/roleRef/name")
            .and_then(|v| v.as_str()),
        Some("system:coredns")
    );
    assert!(
        binding
            .data
            .pointer("/subjects")
            .and_then(|subjects| subjects.as_array())
            .is_some_and(|subjects| {
                subjects.iter().any(|subject| {
                    subject["kind"].as_str() == Some("ServiceAccount")
                        && subject["name"].as_str() == Some("coredns")
                        && subject["namespace"].as_str() == Some("kube-system")
                })
            }),
        "CoreDNS ClusterRoleBinding must bind kube-system/coredns"
    );
}

#[tokio::test]
async fn test_bootstrap_coredns_idempotent() {
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;

    bootstrap_coredns(
        &db,
        crate::controller_test_support::pod_repository_for_test(&db),
        7443,
        "10.43.128.0/17",
        "klights",
        "test-node",
    )
    .await
    .unwrap();
    let result = bootstrap_coredns(
        &db,
        crate::controller_test_support::pod_repository_for_test(&db),
        7443,
        "10.43.128.0/17",
        "klights",
        "test-node",
    )
    .await;
    assert!(result.is_ok(), "Second bootstrap call should not error");

    // Should still have exactly 1 of each
    let cms = db
        .list_resources(
            "v1",
            "ConfigMap",
            Some("kube-system"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    let coredns_cms: Vec<_> = cms.items.iter().filter(|r| r.name == "coredns").collect();
    assert_eq!(
        coredns_cms.len(),
        1,
        "Should have exactly 1 CoreDNS ConfigMap"
    );
}

#[tokio::test]
async fn test_bootstrap_coredns_repairs_legacy_node_local_kubeconfig_resources() {
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;

    db.create_resource(
            "v1",
            "ConfigMap",
            Some("kube-system"),
            "coredns",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "coredns", "namespace": "kube-system"},
                "data": {
                    "Corefile": ".:53 {\n  kubernetes cluster.local in-addr.arpa ip6.arpa {\n    kubeconfig /etc/coredns/kubeconfig.yaml klights-mn-controlplane1\n  }\n}\n"
                }
            }),
        )
        .await
        .unwrap();
    db.create_resource(
            "apps/v1",
            "Deployment",
            Some("kube-system"),
            "coredns",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "coredns", "namespace": "kube-system"},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"k8s-app": "kube-dns"}},
                    "template": {
                        "metadata": {
                            "labels": {"k8s-app": "kube-dns"},
                            "annotations": {
                                "klights.dev/kubeconfig-port": "7679",
                                "klights.dev/kubeconfig-path": "/old/kubeconfig.yaml"
                            }
                        },
                        "spec": {
                            "nodeName": "mn-controlplane1",
                            "containers": [{
                                "name": "coredns",
                                "image": "coredns/coredns:1.11.1",
                                "args": ["-conf", "/etc/coredns/Corefile"],
                                "volumeMounts": [
                                    {"name": "config-volume", "mountPath": "/etc/coredns/Corefile", "subPath": "Corefile", "readOnly": true},
                                    {"name": "kubeconfig", "mountPath": "/etc/coredns/kubeconfig.yaml", "subPath": "kubeconfig.yaml", "readOnly": true}
                                ]
                            }],
                            "volumes": [
                                {"name": "config-volume", "configMap": {"name": "coredns"}},
                                {"name": "kubeconfig", "hostPath": {"path": "/old/kubeconfig.yaml", "type": "File"}}
                            ],
                            "dnsPolicy": "Default"
                        }
                    }
                }
            }),
        )
        .await
        .unwrap();

    bootstrap_coredns(
        &db,
        crate::controller_test_support::pod_repository_for_test(&db),
        7679,
        "10.43.128.0/17",
        "klights",
        "test-node",
    )
    .await
    .unwrap();

    let updated = db
        .get_resource("apps/v1", "Deployment", Some("kube-system"), "coredns")
        .await
        .unwrap()
        .unwrap();
    let cm = db
        .get_resource("v1", "ConfigMap", Some("kube-system"), "coredns")
        .await
        .unwrap()
        .unwrap();
    let corefile = cm.data["data"]["Corefile"].as_str().unwrap();
    assert!(
        !corefile.contains("kubeconfig "),
        "bootstrap must repair stale node-local CoreDNS kubeconfig directives"
    );
    assert!(
        updated
            .data
            .pointer("/spec/template/spec/nodeName")
            .is_none(),
        "CoreDNS must not stay pinned to the bootstrap node"
    );
    let volume_mounts = updated.data["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
        .as_array()
        .unwrap();
    assert!(
        volume_mounts
            .iter()
            .all(|vm| vm["name"].as_str() != Some("kubeconfig")),
        "CoreDNS must not mount a stale node-local kubeconfig"
    );
    let volumes = updated.data["spec"]["template"]["spec"]["volumes"]
        .as_array()
        .unwrap();
    assert!(
        volumes
            .iter()
            .all(|volume| volume["name"].as_str() != Some("kubeconfig")),
        "CoreDNS must not keep the stale kubeconfig hostPath volume"
    );
}

#[test]
fn test_derive_dns_service_ip_from_service_cidr() {
    let test_cases = vec![
        ("10.43.128.0/17", "10.43.128.10"),
        ("10.50.128.0/17", "10.50.128.10"),
        ("192.168.0.0/24", "192.168.0.10"),
        ("172.16.0.0/16", "172.16.0.10"),
    ];

    for (cidr, expected_ip) in test_cases {
        let result = derive_dns_service_ip(cidr);
        assert_eq!(
            result, expected_ip,
            "CIDR {} should yield DNS IP {}",
            cidr, expected_ip
        );
    }
}

#[tokio::test]
async fn test_coredns_service_uses_derived_ip_from_custom_cidr() {
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;

    let custom_service_cidr = "10.50.128.0/17";
    let expected_dns_ip = "10.50.128.10";

    bootstrap_coredns(
        &db,
        crate::controller_test_support::pod_repository_for_test(&db),
        7443,
        custom_service_cidr,
        "klights",
        "test-node",
    )
    .await
    .unwrap();

    let svc = db
        .get_resource("v1", "Service", Some("kube-system"), "kube-dns")
        .await
        .unwrap();
    assert!(svc.is_some(), "kube-dns Service should exist");
    let svc_data = svc.unwrap().data;
    assert_eq!(
        svc_data["spec"]["clusterIP"], expected_dns_ip,
        "Service ClusterIP should match derived DNS IP"
    );
}

#[tokio::test]
async fn test_coredns_deployment_has_dns_policy_default() {
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;

    bootstrap_coredns(
        &db,
        crate::controller_test_support::pod_repository_for_test(&db),
        7443,
        "10.43.128.0/17",
        "klights",
        "test-node",
    )
    .await
    .unwrap();

    let deploy = db
        .get_resource("apps/v1", "Deployment", Some("kube-system"), "coredns")
        .await
        .unwrap()
        .unwrap();

    let dns_policy = deploy
        .data
        .pointer("/spec/template/spec/dnsPolicy")
        .and_then(|v| v.as_str());
    assert_eq!(
        dns_policy,
        Some("Default"),
        "CoreDNS must use dnsPolicy: Default to avoid DNS loop"
    );
}

#[tokio::test]
async fn test_coredns_deployment_template_is_not_pinned_to_bootstrap_node() {
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;

    bootstrap_coredns(
        &db,
        crate::controller_test_support::pod_repository_for_test(&db),
        7443,
        "10.43.128.0/17",
        "klights",
        "bootstrap-node",
    )
    .await
    .unwrap();

    let deploy = db
        .get_resource("apps/v1", "Deployment", Some("kube-system"), "coredns")
        .await
        .unwrap()
        .unwrap();

    assert!(
        deploy
            .data
            .pointer("/spec/template/spec/nodeName")
            .is_none(),
        "CoreDNS Deployment must remain scheduler-bindable after bootstrap node loss"
    );
}

#[tokio::test]
async fn test_coredns_deployment_volume_mounts() {
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;

    bootstrap_coredns(
        &db,
        crate::controller_test_support::pod_repository_for_test(&db),
        7443,
        "10.43.128.0/17",
        "klights",
        "test-node",
    )
    .await
    .unwrap();

    let deploy = db
        .get_resource("apps/v1", "Deployment", Some("kube-system"), "coredns")
        .await
        .unwrap()
        .unwrap();

    let container = &deploy.data["spec"]["template"]["spec"]["containers"][0];
    let volume_mounts = container["volumeMounts"].as_array().unwrap();

    // Verify Corefile mount
    let corefile_mount = volume_mounts
        .iter()
        .find(|vm| vm["mountPath"].as_str() == Some("/etc/coredns/Corefile"));
    assert!(
        corefile_mount.is_some(),
        "Must mount Corefile at /etc/coredns/Corefile"
    );
    assert_eq!(
        corefile_mount.unwrap()["subPath"].as_str(),
        Some("Corefile"),
        "Corefile mount must use subPath"
    );

    // Verify there is no node-local kubeconfig mount. CoreDNS must use its
    // projected ServiceAccount token and the kubernetes Service instead.
    let kubeconfig_mount = volume_mounts
        .iter()
        .find(|vm| vm["mountPath"].as_str() == Some("/etc/coredns/kubeconfig.yaml"));
    assert!(
        kubeconfig_mount.is_none(),
        "Must not mount a node-local kubeconfig at /etc/coredns/kubeconfig.yaml"
    );
}

#[tokio::test]
async fn test_coredns_deployment_labels() {
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;

    bootstrap_coredns(
        &db,
        crate::controller_test_support::pod_repository_for_test(&db),
        7443,
        "10.43.128.0/17",
        "klights",
        "test-node",
    )
    .await
    .unwrap();

    let deploy = db
        .get_resource("apps/v1", "Deployment", Some("kube-system"), "coredns")
        .await
        .unwrap()
        .unwrap();

    // Deployment metadata labels
    assert_eq!(
        deploy.data["metadata"]["labels"]["k8s-app"].as_str(),
        Some("kube-dns"),
        "Deployment must have k8s-app=kube-dns label"
    );

    // Pod template labels must match selector
    let selector_labels = &deploy.data["spec"]["selector"]["matchLabels"];
    let template_labels = &deploy.data["spec"]["template"]["metadata"]["labels"];
    assert_eq!(
        selector_labels, template_labels,
        "Selector matchLabels must match template labels"
    );
}

#[tokio::test]
async fn test_coredns_service_cluster_ips_array() {
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;

    bootstrap_coredns(
        &db,
        crate::controller_test_support::pod_repository_for_test(&db),
        7443,
        "10.43.128.0/17",
        "klights",
        "test-node",
    )
    .await
    .unwrap();

    let svc = db
        .get_resource("v1", "Service", Some("kube-system"), "kube-dns")
        .await
        .unwrap()
        .unwrap();

    let cluster_ip = svc.data["spec"]["clusterIP"].as_str().unwrap();
    let cluster_ips = svc.data["spec"]["clusterIPs"].as_array().unwrap();

    assert_eq!(cluster_ips.len(), 1);
    assert_eq!(
        cluster_ips[0].as_str().unwrap(),
        cluster_ip,
        "clusterIPs[0] must match clusterIP"
    );
}

#[tokio::test]
async fn test_coredns_service_ports() {
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;

    bootstrap_coredns(
        &db,
        crate::controller_test_support::pod_repository_for_test(&db),
        7443,
        "10.43.128.0/17",
        "klights",
        "test-node",
    )
    .await
    .unwrap();

    let svc = db
        .get_resource("v1", "Service", Some("kube-system"), "kube-dns")
        .await
        .unwrap()
        .unwrap();

    let ports = svc.data["spec"]["ports"].as_array().unwrap();
    assert_eq!(ports.len(), 2, "kube-dns must expose UDP and TCP port 53");

    let udp_port = ports.iter().find(|p| p["protocol"].as_str() == Some("UDP"));
    assert!(udp_port.is_some(), "Must have UDP port");
    assert_eq!(udp_port.unwrap()["port"].as_i64(), Some(53));

    let tcp_port = ports.iter().find(|p| p["protocol"].as_str() == Some("TCP"));
    assert!(tcp_port.is_some(), "Must have TCP port");
    assert_eq!(tcp_port.unwrap()["port"].as_i64(), Some(53));
}

#[tokio::test]
async fn test_coredns_configmap_namespace_in_corefile() {
    let db = crate::datastore::test_support::in_memory().await;
    init_default_namespaces(&db).await;

    // Use a custom containerd namespace
    bootstrap_coredns(
        &db,
        crate::controller_test_support::pod_repository_for_test(&db),
        7443,
        "10.43.128.0/17",
        "klights-architect",
        "test-node",
    )
    .await
    .unwrap();

    let cm = db
        .get_resource("v1", "ConfigMap", Some("kube-system"), "coredns")
        .await
        .unwrap()
        .unwrap();

    let corefile = cm.data["data"]["Corefile"].as_str().unwrap();
    assert!(
        !corefile.contains("kubeconfig "),
        "Corefile must not reference a containerd namespace as kubeconfig context"
    );
}
