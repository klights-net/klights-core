//! Base-owned Service reconcile coverage crossing controller and datastore adapters.

use crate::bootstrap::composition_tests::native_api::support::klights_cluster_datastore::test_support::ResourceTestStore;
use klights_cluster_core::Resource;
use klights_cluster_store::ResourceListOptions;
use klights_controllers::service::*;
use klights_pod_api::{
    PodGetRequest, PodListRequest, PodListResult, PodOwnerListRequest, PodQuery,
    PodRepositoryError, PodRepositoryFuture,
};
use serde_json::json;

#[derive(Clone)]
struct DatastorePodQuery {
    db: klights_cluster_datastore::test_support::ResourceTestStore,
}

/// Private P12.2b storage adapter for this file's retained P12.2c Service
/// algorithms. It exposes only their exact controller traits.
#[derive(Clone)]
struct ServiceResourceFixtureStore(ResourceTestStore);

fn service_store_error(error: anyhow::Error) -> klights_reconcile_api::ControllerStoreError {
    if klights_cluster_datastore::errors::is_conflict_error(&error) {
        klights_reconcile_api::ControllerStoreError::conflict(error.to_string())
    } else {
        klights_reconcile_api::ControllerStoreError::internal(error.to_string())
    }
}

#[async_trait::async_trait]
impl klights_controllers::service::ServiceReconcileStore for ServiceResourceFixtureStore {
    async fn list_services(&self) -> klights_reconcile_api::ControllerStoreResult<Vec<Resource>> {
        Ok(self
            .0
            .list_resources("v1", "Service", None, ResourceListOptions::all())
            .await
            .map_err(service_store_error)?
            .items)
    }
    async fn get_service(
        &self,
        namespace: &str,
        name: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Option<Resource>> {
        self.0
            .get_resource("v1", "Service", Some(namespace), name)
            .await
            .map_err(service_store_error)
    }
    async fn update_service(
        &self,
        namespace: &str,
        name: &str,
        data: serde_json::Value,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        self.0
            .update_resource_with_preconditions(
                "v1",
                "Service",
                Some(namespace),
                name,
                data,
                preconditions,
            )
            .await
            .map_err(service_store_error)
    }
}

#[async_trait::async_trait]
impl klights_controllers::endpoints::EndpointReconcileStore for ServiceResourceFixtureStore {
    async fn endpoint_namespace_is_terminating(
        &self,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<bool> {
        Ok(self
            .0
            .get_namespace(namespace)
            .await
            .map_err(service_store_error)?
            .is_some_and(|resource| {
                resource
                    .data
                    .pointer("/metadata/deletionTimestamp")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
            }))
    }
    async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Option<Resource>> {
        self.0
            .get_resource(api_version, kind, namespace, name)
            .await
            .map_err(service_store_error)
    }
    async fn list_service_endpoint_slices(
        &self,
        namespace: &str,
        service_name: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<Vec<Resource>> {
        Ok(self
            .0
            .list_resources(
                "discovery.k8s.io/v1",
                "EndpointSlice",
                Some(namespace),
                ResourceListOptions::new(
                    Some(&format!("kubernetes.io/service-name={service_name}")),
                    None,
                    None,
                    None,
                ),
            )
            .await
            .map_err(service_store_error)?
            .items)
    }
    async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        self.0
            .create_resource(api_version, kind, namespace, name, data)
            .await
            .map_err(service_store_error)
    }
    async fn update_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        data: serde_json::Value,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> klights_reconcile_api::ControllerStoreResult<Resource> {
        self.0
            .update_resource_with_preconditions(
                api_version,
                kind,
                namespace,
                name,
                data,
                preconditions,
            )
            .await
            .map_err(service_store_error)
    }
    async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> klights_reconcile_api::ControllerStoreResult<()> {
        self.0
            .delete_resource_with_preconditions(api_version, kind, namespace, name, preconditions)
            .await
            .map_err(service_store_error)
    }
    async fn apply_resource_batch(
        &self,
        operations: Vec<klights_cluster_core::ResourceBatchOperation>,
    ) -> klights_reconcile_api::ControllerStoreResult<()> {
        self.0
            .apply_resource_batch(operations)
            .await
            .map_err(service_store_error)
    }
}

fn pod_store_error(error: anyhow::Error) -> PodRepositoryError {
    PodRepositoryError::unavailable(error.to_string())
}

impl PodQuery for DatastorePodQuery {
    fn get_pod(&self, request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
        Box::pin(async move {
            let resource = self
                .db
                .get_resource("v1", "Pod", Some(request.namespace()), request.name())
                .await
                .map_err(pod_store_error)?;
            Ok(resource.filter(|resource| {
                request
                    .uid()
                    .is_none_or(|expected_uid| resource.uid == expected_uid)
            }))
        })
    }

    fn list_pods(&self, request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
        Box::pin(async move {
            let listing = self
                .db
                .list_resources(
                    "v1",
                    "Pod",
                    request.namespace(),
                    ResourceListOptions::new(
                        request.label_selector(),
                        request.field_selector(),
                        request.limit(),
                        request.continue_token(),
                    ),
                )
                .await
                .map_err(pod_store_error)?;
            PodListResult::try_new(
                listing.items,
                listing.resource_version,
                listing.continue_token,
                listing.remaining_item_count,
            )
        })
    }

    fn list_pods_by_owner_uid(
        &self,
        request: PodOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<Resource>> {
        Box::pin(async move {
            self.db
                .list_resources_by_owner_uid(
                    "v1",
                    "Pod",
                    Some(request.namespace()),
                    request.owner_uid(),
                )
                .await
                .map_err(pod_store_error)
        })
    }
}

fn inject_resource_version(
    data: std::sync::Arc<serde_json::Value>,
    resource_version: i64,
) -> serde_json::Value {
    let mut data = std::sync::Arc::unwrap_or_clone(data);
    data["metadata"]["resourceVersion"] = json!(resource_version.to_string());
    data
}

async fn reconcile_service(
    db: &(impl ServiceControllerStore + ?Sized),
    pod_reader: &(impl klights_pod_api::PodQuery + ?Sized),
    service: &serde_json::Value,
    service_ipam: &ServiceIpam,
) -> anyhow::Result<serde_json::Value> {
    reconcile_service_with_nodeport_at(
        db,
        pod_reader,
        service,
        service_ipam,
        &NodePortAllocator::new(),
        chrono::Utc::now(),
        crate::bootstrap::composition_tests::native_api::support::deterministic_controller_identity()
            .as_ref(),
    )
    .await
}

#[tokio::test]
async fn test_service_stale_snapshot_after_delete_does_not_recreate_endpoints() {
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let store = ServiceResourceFixtureStore(db.clone());
    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    db.create_resource(
        "v1",
        "Namespace",
        None,
        "default",
        json!({"metadata": {"name": "default"}}),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "svc-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "svc-pod",
                "namespace": "default",
                "uid": "svc-pod-uid",
                "labels": {"app": "stale-svc"}
            },
            "spec": {"containers": [{"name": "app", "image": "nginx", "ports": [{"containerPort": 8080}]}]},
            "status": {
                "phase": "Running",
                "podIP": "10.43.0.20",
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        }),
    )
    .await
    .unwrap();

    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "stale-svc", "namespace": "default", "uid": "stale-svc-uid"},
        "spec": {
            "type": "ClusterIP",
            "clusterIP": "10.43.128.20",
            "clusterIPs": ["10.43.128.20"],
            "selector": {"app": "stale-svc"},
            "ports": [{"name": "http", "port": 80, "targetPort": 8080, "protocol": "TCP"}]
        }
    });
    let created = db
        .create_resource("v1", "Service", Some("default"), "stale-svc", service)
        .await
        .unwrap();
    let stale_snapshot = inject_resource_version(created.data, created.resource_version);

    db.delete_resource("v1", "Service", Some("default"), "stale-svc")
        .await
        .unwrap();

    reconcile_service(
        &store,
        &DatastorePodQuery { db: db.clone() },
        &stale_snapshot,
        &service_ipam,
    )
    .await
    .unwrap();

    let endpoints = db
        .get_resource("v1", "Endpoints", Some("default"), "stale-svc")
        .await
        .unwrap();
    assert!(
        endpoints.is_none(),
        "stale deleted Service reconcile must not recreate Endpoints"
    );
    let slices = db
        .list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            ResourceListOptions::all(),
        )
        .await
        .unwrap();
    assert!(
        slices.items.is_empty(),
        "stale deleted Service reconcile must not recreate EndpointSlices"
    );
}

#[tokio::test]
async fn service_reconcile_commits_endpointslice_and_legacy_endpoints_in_one_batch() {
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let store = ServiceResourceFixtureStore(db.clone());
    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    db.create_resource(
        "v1",
        "Pod",
        Some("default"),
        "latency-pod",
        json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "latency-pod",
                "namespace": "default",
                "uid": "latency-pod-uid",
                "labels": {"app": "latency"}
            },
            "spec": {"containers": [{"name": "app", "image": "nginx", "ports": [{"containerPort": 8080}]}]},
            "status": {
                "phase": "Running",
                "podIP": "10.43.0.21",
                "conditions": [{"type": "Ready", "status": "True"}]
            }
        }),
    )
    .await
    .unwrap();

    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "latency-svc", "namespace": "default", "uid": "latency-svc-uid"},
        "spec": {
            "type": "ClusterIP",
            "clusterIP": "10.43.128.21",
            "clusterIPs": ["10.43.128.21"],
            "sessionAffinity": "None",
            "selector": {"app": "latency"},
            "ports": [{"name": "http", "port": 80, "targetPort": 8080, "protocol": "TCP"}]
        }
    });
    let created = db
        .create_resource("v1", "Service", Some("default"), "latency-svc", service)
        .await
        .unwrap();
    let service_snapshot = inject_resource_version(created.data, created.resource_version);

    reconcile_service(
        &store,
        &DatastorePodQuery { db: db.clone() },
        &service_snapshot,
        &service_ipam,
    )
    .await
    .unwrap();

    let endpoints = db
        .get_resource("v1", "Endpoints", Some("default"), "latency-svc")
        .await
        .unwrap()
        .expect("legacy Endpoints should be reconciled");
    let slices = db
        .list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            ResourceListOptions::new(
                Some("kubernetes.io/service-name=latency-svc"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();
    let slice = slices
        .items
        .first()
        .expect("EndpointSlice should be reconciled");

    assert_eq!(
        slice.resource_version, endpoints.resource_version,
        "EndpointSlice and legacy Endpoints must be committed in the same raft entry"
    );
}

// Integration test (requires root for nftables/netlink)
#[tokio::test]
#[ignore] // Ignored by default, run manually with --ignored flag as root
async fn test_reconcile_service_preserves_headless_cluster_ip_none() {
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let store = ServiceResourceFixtureStore(db.clone());
    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    // Create a headless service (clusterIP: None)
    let mut service = json!({
        "metadata": {
            "name": "headless-svc",
            "namespace": "default",
            "uid": "test-uid-1"
        },
        "spec": {
            "clusterIP": "None",
            "selector": {"app": "test"},
            "ports": [{"port": 80, "targetPort": 8080}]
        }
    });

    // Insert service into DB first
    let name = service
        .get("metadata")
        .unwrap()
        .get("name")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let created = db
        .create_resource("v1", "Service", Some("default"), &name, service.clone())
        .await
        .unwrap();

    // Inject resource version for reconciliation
    if let Some(metadata) = service.as_object_mut().and_then(|o| o.get_mut("metadata"))
        && let Some(meta_obj) = metadata.as_object_mut()
    {
        meta_obj.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    let result = reconcile_service(
        &store,
        &DatastorePodQuery { db: db.clone() },
        &service,
        &service_ipam,
    )
    .await
    .unwrap();

    // Verify clusterIP is still "None" (not allocated)
    let cluster_ip = result
        .get("spec")
        .and_then(|s| s.get("clusterIP"))
        .and_then(|ip| ip.as_str());

    assert_eq!(
        cluster_ip,
        Some("None"),
        "Headless service must preserve clusterIP: None"
    );

    // Verify no clusterIPs array was added
    assert!(
        result
            .get("spec")
            .and_then(|s| s.get("clusterIPs"))
            .is_none(),
        "Headless service must not have clusterIPs array"
    );
}

// Integration test (requires root for nftables/netlink)
#[tokio::test]
#[ignore] // Ignored by default, run manually with --ignored flag as root
async fn test_reconcile_service_allocates_cluster_ip_when_not_set() {
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let store = ServiceResourceFixtureStore(db.clone());
    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    // Create a normal service without clusterIP
    let mut service = json!({
        "metadata": {
            "name": "normal-svc",
            "namespace": "default",
            "uid": "test-uid-2"
        },
        "spec": {
            "selector": {"app": "test"},
            "ports": [{"port": 80, "targetPort": 8080}]
        }
    });

    // Insert service into DB first
    let name = service
        .get("metadata")
        .unwrap()
        .get("name")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let created = db
        .create_resource("v1", "Service", Some("default"), &name, service.clone())
        .await
        .unwrap();

    // Inject resource version for reconciliation
    if let Some(metadata) = service.as_object_mut().and_then(|o| o.get_mut("metadata"))
        && let Some(meta_obj) = metadata.as_object_mut()
    {
        meta_obj.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    let result = reconcile_service(
        &store,
        &DatastorePodQuery { db: db.clone() },
        &service,
        &service_ipam,
    )
    .await
    .unwrap();

    // Verify clusterIP was allocated (should be 10.43.128.2, since .1 is reserved)
    let cluster_ip = result
        .get("spec")
        .and_then(|s| s.get("clusterIP"))
        .and_then(|ip| ip.as_str());

    assert_eq!(
        cluster_ip,
        Some("10.43.128.2"),
        "Normal service must allocate a clusterIP"
    );

    // Verify clusterIPs array was added
    let cluster_ips = result
        .get("spec")
        .and_then(|s| s.get("clusterIPs"))
        .and_then(|ips| ips.as_array());

    assert!(cluster_ips.is_some(), "Must have clusterIPs array");
    assert_eq!(
        cluster_ips.unwrap().len(),
        1,
        "clusterIPs array must have one entry"
    );
}

#[tokio::test]
#[ignore] // Requires root for nftables/netlink
async fn test_service_external_name_no_cluster_ip() {
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let store = ServiceResourceFixtureStore(db.clone());
    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    let mut service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "external-db",
            "namespace": "default"
        },
        "spec": {
            "type": "ExternalName",
            "externalName": "my-database.example.com"
        }
    });

    // Insert service into DB
    let name = "external-db".to_string();
    let created = db
        .create_resource("v1", "Service", Some("default"), &name, service.clone())
        .await
        .unwrap();

    // Inject resource version
    if let Some(metadata) = service.as_object_mut().and_then(|o| o.get_mut("metadata"))
        && let Some(meta_obj) = metadata.as_object_mut()
    {
        meta_obj.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    let result = reconcile_service(
        &store,
        &DatastorePodQuery { db: db.clone() },
        &service,
        &service_ipam,
    )
    .await
    .unwrap();

    // ExternalName services MUST NOT have clusterIP
    let cluster_ip = result.get("spec").and_then(|s| s.get("clusterIP"));

    assert!(
        cluster_ip.is_none(),
        "ExternalName service must not allocate clusterIP"
    );

    // Verify externalName field is preserved
    let external_name = result
        .get("spec")
        .and_then(|s| s.get("externalName"))
        .and_then(|en| en.as_str());

    assert_eq!(
        external_name,
        Some("my-database.example.com"),
        "ExternalName field must be preserved"
    );
}

#[tokio::test]
#[ignore] // Requires root for nftables/netlink
async fn test_service_external_name_no_endpoints() {
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let store = ServiceResourceFixtureStore(db.clone());
    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    let mut service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "external-api",
            "namespace": "default"
        },
        "spec": {
            "type": "ExternalName",
            "externalName": "api.external.com"
        }
    });

    // Insert service into DB
    let name = "external-api".to_string();
    let created = db
        .create_resource("v1", "Service", Some("default"), &name, service.clone())
        .await
        .unwrap();

    // Inject resource version
    if let Some(metadata) = service.as_object_mut().and_then(|o| o.get_mut("metadata"))
        && let Some(meta_obj) = metadata.as_object_mut()
    {
        meta_obj.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    let _result = reconcile_service(
        &store,
        &DatastorePodQuery { db: db.clone() },
        &service,
        &service_ipam,
    )
    .await
    .unwrap();

    // ExternalName services MUST NOT create Endpoints
    let endpoints = db
        .get_resource("v1", "Endpoints", Some("default"), "external-api")
        .await;

    assert!(
        matches!(endpoints, Ok(None)),
        "ExternalName service must not create Endpoints"
    );
}

#[tokio::test]
#[ignore] // Requires root for nftables/netlink
async fn test_service_external_name_no_endpoint_slice() {
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let store = ServiceResourceFixtureStore(db.clone());
    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    let mut service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "external-svc",
            "namespace": "default"
        },
        "spec": {
            "type": "ExternalName",
            "externalName": "external.example.org"
        }
    });

    // Insert service into DB
    let name = "external-svc".to_string();
    let created = db
        .create_resource("v1", "Service", Some("default"), &name, service.clone())
        .await
        .unwrap();

    // Inject resource version
    if let Some(metadata) = service.as_object_mut().and_then(|o| o.get_mut("metadata"))
        && let Some(meta_obj) = metadata.as_object_mut()
    {
        meta_obj.insert(
            "resourceVersion".to_string(),
            json!(created.resource_version.to_string()),
        );
    }

    let _result = reconcile_service(
        &store,
        &DatastorePodQuery { db: db.clone() },
        &service,
        &service_ipam,
    )
    .await
    .unwrap();

    // ExternalName services MUST NOT create EndpointSlice
    let endpoint_slice_list = db
        .list_resources(
            "discovery.k8s.io/v1",
            "EndpointSlice",
            Some("default"),
            ResourceListOptions::new(
                Some("kubernetes.io/service-name=external-svc"),
                None,
                None,
                None,
            ),
        )
        .await
        .unwrap();

    assert_eq!(
        endpoint_slice_list.items.len(),
        0,
        "ExternalName service must not create EndpointSlice"
    );
}

#[tokio::test]
async fn test_reconcile_service_defaults_single_stack_ip_family_fields() {
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let store = ServiceResourceFixtureStore(db.clone());
    let service_ipam = ServiceIpam::new("10.43.128.0/17");

    let service = json!({
        "metadata": {
            "name": "family-defaults",
            "namespace": "default",
            "uid": "family-defaults-uid"
        },
        "spec": {
            "selector": {"app": "family-defaults"},
            "ports": [{"port": 80, "targetPort": 8080}]
        }
    });
    let created = db
        .create_resource("v1", "Service", Some("default"), "family-defaults", service)
        .await
        .unwrap();
    let service = inject_resource_version(created.data, created.resource_version);

    let result = reconcile_service(
        &store,
        &DatastorePodQuery { db: db.clone() },
        &service,
        &service_ipam,
    )
    .await
    .unwrap();

    assert_eq!(
        result.pointer("/spec/ipFamilyPolicy"),
        Some(&json!("SingleStack"))
    );
    assert_eq!(result.pointer("/spec/ipFamilies"), Some(&json!(["IPv4"])));
}

// F6-02: Tests for leader-safe NodePort allocator with readiness state

#[tokio::test]
async fn test_nodeport_allocator_rebuild_scans_existing_services() {
    use std::sync::Arc;

    // Create in-memory DB with existing services having NodePorts
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let store = ServiceResourceFixtureStore(db.clone());
    let ns = "default";

    // Create a service with an existing NodePort
    db.create_resource(
        "v1",
        "Service",
        Some(ns),
        "existing-svc",
        serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "existing-svc",
                "namespace": ns,
                "uid": "svc-123"
            },
            "spec": {
                "type": "NodePort",
                "ports": [
                    {
                        "port": 80,
                        "targetPort": 8080,
                        "nodePort": 30000,
                        "protocol": "TCP"
                    }
                ],
                "selector": {"app": "test"}
            }
        }),
    )
    .await
    .unwrap();

    // Create a fresh allocator and rebuild from DB
    let alloc = Arc::new(NodePortAllocator::new());
    rebuild_nodeport_allocator_from_services(&store, &alloc)
        .await
        .unwrap();

    // After rebuild, allocator should have marked 30000 as used
    assert!(alloc.is_ready(), "Allocator should be ready after rebuild");

    // Allocating should skip the already-used port 30000
    let new_port = alloc.allocate().unwrap();
    assert_ne!(new_port, 30000, "Should not allocate existing port 30000");
}

#[tokio::test]
async fn service_reconcile_recovers_cluster_ip_after_generic_service_delete() {
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let store = ServiceResourceFixtureStore(db.clone());
    let ipam = ServiceIpam::new("10.0.0.0/30");
    let alloc = NodePortAllocator::new();
    alloc.set_ready();
    let pod_reader = DatastorePodQuery { db: db.clone() };

    let mut first = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "first", "namespace": "default"},
        "spec": {
            "selector": {"app": "first"},
            "ports": [{"port": 80}]
        }
    });
    let created_first = db
        .create_resource("v1", "Service", Some("default"), "first", first.clone())
        .await
        .unwrap();
    first["metadata"]["resourceVersion"] =
        serde_json::json!(created_first.resource_version.to_string());
    first["metadata"]["uid"] = serde_json::json!(created_first.uid);

    let first_result = reconcile_service_with_nodeport_at(
        &store,
        &pod_reader,
        &first,
        &ipam,
        &alloc,
        chrono::Utc::now(),
        crate::bootstrap::composition_tests::native_api::support::deterministic_controller_identity()
            .as_ref(),
    )
    .await
    .unwrap();
    assert_eq!(first_result["spec"]["clusterIP"], "10.0.0.2");

    // Namespace termination, delete-collection, and GC delete Service rows
    // through generic datastore paths. ClusterIP allocation must recover from
    // those paths even though they cannot call ServiceIpam::release directly.
    db.delete_resource("v1", "Service", Some("default"), "first")
        .await
        .unwrap();

    let mut second = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "second", "namespace": "default"},
        "spec": {
            "selector": {"app": "second"},
            "ports": [{"port": 8080}]
        }
    });
    let created_second = db
        .create_resource("v1", "Service", Some("default"), "second", second.clone())
        .await
        .unwrap();
    second["metadata"]["resourceVersion"] =
        serde_json::json!(created_second.resource_version.to_string());
    second["metadata"]["uid"] = serde_json::json!(created_second.uid);

    let second_result = reconcile_service_with_nodeport_at(
        &store,
        &pod_reader,
        &second,
        &ipam,
        &alloc,
        chrono::Utc::now(),
        crate::bootstrap::composition_tests::native_api::support::deterministic_controller_identity()
            .as_ref(),
    )
    .await
    .unwrap();

    assert_eq!(second_result["spec"]["clusterIP"], "10.0.0.2");
}

/// Reconciling an already-normalized Service with no endpoint-relevant
/// changes must not bump the persisted resourceVersion.
#[tokio::test]
async fn reconcile_idempotent_does_not_churn_resource_version() {
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let store = ServiceResourceFixtureStore(db.clone());
    let ipam = ServiceIpam::new("10.43.128.0/17");
    let alloc = NodePortAllocator::new();
    alloc.set_ready();

    let mut svc = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "my-svc", "namespace": "default"},
        "spec": {
            "type": "ClusterIP",
            "selector": {"app": "my"},
            "ports": [{"port": 80, "protocol": "TCP"}]
        }
    });
    let created = db
        .create_resource("v1", "Service", Some("default"), "my-svc", svc.clone())
        .await
        .unwrap();
    if let Some(meta) = svc.as_object_mut().and_then(|o| o.get_mut("metadata"))
        && let Some(meta_obj) = meta.as_object_mut()
    {
        meta_obj.insert(
            "resourceVersion".to_string(),
            serde_json::json!(created.resource_version.to_string()),
        );
        meta_obj.insert("uid".to_string(), serde_json::json!(created.uid));
    }

    let result = reconcile_service_with_nodeport_at(
        &store,
        &DatastorePodQuery { db: db.clone() },
        &svc,
        &ipam,
        &alloc,
        chrono::Utc::now(),
        crate::bootstrap::composition_tests::native_api::support::deterministic_controller_identity()
            .as_ref(),
    )
    .await
    .unwrap();
    let rv1 = result["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .unwrap();
    assert!(rv1 > created.resource_version);

    // Second reconcile — no changes, must not bump resourceVersion.
    let result2 = reconcile_service_with_nodeport_at(
        &store,
        &DatastorePodQuery { db: db.clone() },
        &result,
        &ipam,
        &alloc,
        chrono::Utc::now(),
        crate::bootstrap::composition_tests::native_api::support::deterministic_controller_identity()
            .as_ref(),
    )
    .await
    .unwrap();
    let rv2 = result2["metadata"]["resourceVersion"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .unwrap();
    assert_eq!(rv2, rv1);
}

// Task 10 regression tests: create-allocation behavior

#[tokio::test]
async fn create_service_returns_error_when_allocation_fails() {
    // /30 CIDR has exactly one usable ClusterIP (network+2 = .2, broadcast-1 = .2)
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let store = ServiceResourceFixtureStore(db.clone());
    let service_ipam = ServiceIpam::new("10.0.0.0/30");
    let nodeport_alloc = NodePortAllocator::new();
    nodeport_alloc.set_ready();

    // Seed the DB with a Service that owns the only available IP so
    // rebuild_service_ipam_from_services cannot reclaim it.
    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "occupying-svc",
        json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"name": "occupying-svc", "namespace": "default"},
            "spec": {
                "type": "ClusterIP",
                "clusterIP": "10.0.0.2",
                "clusterIPs": ["10.0.0.2"],
                "ports": [{"port": 80, "protocol": "TCP"}]
            }
        }),
    )
    .await
    .unwrap();

    // Exhaust the in-memory IPAM so the first allocate() call fails and
    // forces a rebuild, which will re-mark 10.0.0.2 from the DB Service.
    let _ = service_ipam.allocate();

    let mut new_svc = json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": "new-svc", "namespace": "default"},
        "spec": {"type": "ClusterIP", "ports": [{"port": 80, "protocol": "TCP"}]}
    });
    let result =
        prepare_service_for_create(&store, &mut new_svc, &service_ipam, &nodeport_alloc).await;
    assert!(
        result.is_err(),
        "prepare_service_for_create must return Err when ClusterIP pool is exhausted"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("ClusterIP") || msg.contains("exhausted"),
        "error must mention ClusterIP exhaustion, got: {msg}"
    );
}

// NOTE: The "does not enqueue after allocation failure" and "enqueues exactly
// once on success" invariants are end-to-end properties of the create handler
// (`create_inner` -> `prepare_service_for_create` -> ... ->
// `enqueue_generated_controller_after_mutation`). `prepare_service_for_create`
// performs allocation only and never enqueues, so it cannot observe the enqueue
// here. Those two tests therefore live in
// `tests/native_api/cases/handlers/core_v1_tests.rs` where the HTTP create path is driven and
// the enqueue is observed via `MockServiceRouter::sync_count`:
//   - create_service_does_not_enqueue_reconcile_after_allocation_failure
//   - create_service_success_response_contains_allocated_fields_and_enqueues_once
// The tests below cover the allocation primitive in isolation.

#[tokio::test]
async fn prepare_service_for_create_populates_allocated_fields() {
    // A normal Service create allocates a ClusterIP and populates the spec.
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let store = ServiceResourceFixtureStore(db.clone());
    let service_ipam = ServiceIpam::new("10.43.128.0/17");
    let nodeport_alloc = NodePortAllocator::new();
    nodeport_alloc.set_ready();

    let mut svc = json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": "my-svc", "namespace": "default"},
        "spec": {"type": "ClusterIP", "ports": [{"port": 80, "protocol": "TCP"}]}
    });
    let pending = prepare_service_for_create(&store, &mut svc, &service_ipam, &nodeport_alloc)
        .await
        .expect("prepare_service_for_create must succeed when IPs are available");

    let cluster_ip = svc
        .pointer("/spec/clusterIP")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        !cluster_ip.is_empty() && cluster_ip != "None",
        "response must carry an allocated clusterIP, got: {cluster_ip:?}"
    );
    assert!(
        svc.pointer("/spec/clusterIPs")
            .and_then(|v| v.as_array())
            .is_some_and(|arr| !arr.is_empty()),
        "response must carry non-empty clusterIPs"
    );
    // Pending allocations must release the exact ClusterIP on create rollback.
    pending.release(&service_ipam, &nodeport_alloc);
    assert_eq!(
        service_ipam.allocate().unwrap(),
        cluster_ip,
        "PendingServiceAllocations must track the allocated ClusterIP for rollback"
    );
}

#[test]
fn create_service_allocation_conflict_maps_to_kubernetes_409() {
    use axum::http::StatusCode;
    use axum::response::IntoResponse as _;
    use k8s_native_service::AppError;
    use klights_cluster_datastore::errors::DatastoreError;

    // When create_resource fails with DatastoreError::Conflict (duplicate name),
    // AppError::from(e) must produce a 409 CONFLICT response — not 500.
    let ds_err = DatastoreError::Conflict {
        message: "services \"my-svc\" already exists".to_string(),
    };
    let app_err = AppError::from(anyhow::Error::new(ds_err));
    let response = app_err.into_response();
    assert_eq!(
        response.status(),
        StatusCode::CONFLICT,
        "DatastoreError::Conflict must map to HTTP 409 for Service creates"
    );
}
