//! Base-owned Service/Pod coverage crossing controller and datastore adapters.

use std::sync::Arc;

use klights_cluster_core::Resource;
use klights_cluster_datastore::test_support::ResourceTestStore;
use klights_cluster_store::ResourceListOptions;
use klights_controllers::side_effects::{ControllerDispatcherSlot, service_pod};
use klights_pod_api::{
    PodGetRequest, PodListRequest, PodListResult, PodOwnerListRequest, PodQuery,
    PodRepositoryError, PodRepositoryFuture,
};
use klights_reconcile_api::ServiceReconcileKey;
use klights_types::PodIdentity;
use serde_json::json;

#[derive(Clone)]
struct DatastorePodQuery {
    db: klights_cluster_datastore::test_support::ResourceTestStore,
}

/// Private P12.2b persistence projection for this file's retained service
/// side-effect algorithms; it deliberately is not reusable controller support.
#[derive(Clone)]
struct ServicePodResourceFixtureStore(ResourceTestStore);

#[async_trait::async_trait]
impl service_pod::ServicePodStore for ServicePodResourceFixtureStore {
    async fn load_service_endpoint_state(
        &self,
        namespace: &str,
    ) -> anyhow::Result<service_pod::ServiceEndpointState> {
        Ok(service_pod::ServiceEndpointState {
            services: self
                .0
                .list_resources("v1", "Service", Some(namespace), ResourceListOptions::all())
                .await?
                .items,
            endpoints: self
                .0
                .list_resources(
                    "v1",
                    "Endpoints",
                    Some(namespace),
                    ResourceListOptions::all(),
                )
                .await?
                .items,
            endpoint_slices: self
                .0
                .list_resources(
                    "discovery.k8s.io/v1",
                    "EndpointSlice",
                    Some(namespace),
                    ResourceListOptions::all(),
                )
                .await?
                .items,
        })
    }
}

fn service_pod_store_error(error: anyhow::Error) -> klights_reconcile_api::ControllerStoreError {
    if klights_cluster_datastore::errors::is_conflict_error(&error) {
        klights_reconcile_api::ControllerStoreError::conflict(error.to_string())
    } else {
        klights_reconcile_api::ControllerStoreError::internal(error.to_string())
    }
}

#[async_trait::async_trait]
impl klights_controllers::endpoints::EndpointReconcileStore for ServicePodResourceFixtureStore {
    async fn endpoint_namespace_is_terminating(
        &self,
        namespace: &str,
    ) -> klights_reconcile_api::ControllerStoreResult<bool> {
        Ok(self
            .0
            .get_namespace(namespace)
            .await
            .map_err(service_pod_store_error)?
            .is_some_and(|r| {
                r.data
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
            .map_err(service_pod_store_error)
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
            .map_err(service_pod_store_error)?
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
            .map_err(service_pod_store_error)
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
            .map_err(service_pod_store_error)
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
            .map_err(service_pod_store_error)
    }
    async fn apply_resource_batch(
        &self,
        operations: Vec<klights_cluster_core::ResourceBatchOperation>,
    ) -> klights_reconcile_api::ControllerStoreResult<()> {
        self.0
            .apply_resource_batch(operations)
            .await
            .map_err(service_pod_store_error)
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

#[tokio::test]
async fn datastore_pod_query_preserves_read_contract_without_mutation() {
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let query = DatastorePodQuery { db: db.clone() };
    for (namespace, name, uid, app, node, owner_uid) in [
        ("default", "web-a", "pod-a", "web", "node-a", "owner-a"),
        ("default", "web-b", "pod-b", "web", "node-a", "owner-a"),
        ("default", "api", "pod-api", "api", "node-b", "owner-b"),
        ("other", "web-c", "pod-c", "web", "node-a", "owner-a"),
    ] {
        db.create_resource(
            "v1",
            "Pod",
            Some(namespace),
            name,
            json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": name,
                    "namespace": namespace,
                    "uid": uid,
                    "labels": {"app": app},
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "owner",
                        "uid": owner_uid
                    }]
                },
                "spec": {"nodeName": node, "containers": [{"name": "main", "image": "test"}]}
            }),
        )
        .await
        .unwrap();
    }
    let resource_version_before_reads = db.get_current_resource_version().await.unwrap();

    struct GetCase {
        description: &'static str,
        request: PodGetRequest,
        expected_uid: Option<&'static str>,
    }
    let get_cases = [
        GetCase {
            description: "name lookup returns the matching Pod",
            request: PodGetRequest::try_by_name("default", "web-a").unwrap(),
            expected_uid: Some("pod-a"),
        },
        GetCase {
            description: "UID-qualified lookup accepts the matching identity",
            request: PodGetRequest::try_by_identity(PodIdentity::new("default", "web-a", "pod-a"))
                .unwrap(),
            expected_uid: Some("pod-a"),
        },
        GetCase {
            description: "UID-qualified lookup rejects a same-name replacement",
            request: PodGetRequest::try_by_identity(PodIdentity::new(
                "default",
                "web-a",
                "wrong-uid",
            ))
            .unwrap(),
            expected_uid: None,
        },
        GetCase {
            description: "namespace remains part of Pod identity",
            request: PodGetRequest::try_by_name("other", "web-a").unwrap(),
            expected_uid: None,
        },
    ];
    for case in get_cases {
        assert_eq!(
            query
                .get_pod(case.request)
                .await
                .unwrap()
                .as_ref()
                .map(|pod| pod.uid.as_str()),
            case.expected_uid,
            "{}",
            case.description
        );
    }

    struct ListCase {
        description: &'static str,
        namespace: Option<&'static str>,
        label_selector: Option<&'static str>,
        field_selector: Option<&'static str>,
        limit: Option<i64>,
    }
    let list_cases = [
        ListCase {
            description: "namespace, selectors, limit, and pagination metadata are preserved",
            namespace: Some("default"),
            label_selector: Some("app=web"),
            field_selector: Some("spec.nodeName=node-a"),
            limit: Some(1),
        },
        ListCase {
            description: "all-namespace list remains cluster-wide",
            namespace: None,
            label_selector: Some("app=web"),
            field_selector: Some("spec.nodeName=node-a"),
            limit: None,
        },
    ];
    for case in list_cases {
        let options =
            ResourceListOptions::new(case.label_selector, case.field_selector, case.limit, None);
        let expected = db
            .list_resources("v1", "Pod", case.namespace, options)
            .await
            .unwrap();
        let actual = query
            .list_pods(
                PodListRequest::try_new(
                    case.namespace.map(str::to_string),
                    case.label_selector.map(str::to_string),
                    case.field_selector.map(str::to_string),
                    case.limit,
                    None,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            actual
                .items()
                .iter()
                .map(|pod| &pod.uid)
                .collect::<Vec<_>>(),
            expected
                .items
                .iter()
                .map(|pod| &pod.uid)
                .collect::<Vec<_>>(),
            "{}",
            case.description
        );
        assert_eq!(actual.resource_version(), expected.resource_version);
        assert_eq!(actual.continue_token(), expected.continue_token.as_deref());
        assert_eq!(actual.remaining_item_count(), expected.remaining_item_count);

        if let Some(token) = expected.continue_token {
            let expected_next = db
                .list_resources(
                    "v1",
                    "Pod",
                    case.namespace,
                    ResourceListOptions::new(
                        case.label_selector,
                        case.field_selector,
                        case.limit,
                        Some(&token),
                    ),
                )
                .await
                .unwrap();
            let actual_next = query
                .list_pods(
                    PodListRequest::try_new(
                        case.namespace.map(str::to_string),
                        case.label_selector.map(str::to_string),
                        case.field_selector.map(str::to_string),
                        case.limit,
                        Some(token),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                actual_next
                    .items()
                    .iter()
                    .map(|pod| &pod.uid)
                    .collect::<Vec<_>>(),
                expected_next
                    .items
                    .iter()
                    .map(|pod| &pod.uid)
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                actual_next.resource_version(),
                expected_next.resource_version
            );
            assert_eq!(
                actual_next.continue_token(),
                expected_next.continue_token.as_deref()
            );
            assert_eq!(
                actual_next.remaining_item_count(),
                expected_next.remaining_item_count
            );
        }
    }

    for (namespace, owner_uid, expected_uids) in [
        ("default", "owner-a", vec!["pod-a", "pod-b"]),
        ("default", "owner-b", vec!["pod-api"]),
        ("default", "owner-missing", vec![]),
        ("other", "owner-a", vec!["pod-c"]),
    ] {
        let mut actual = query
            .list_pods_by_owner_uid(PodOwnerListRequest::try_new(namespace, owner_uid).unwrap())
            .await
            .unwrap()
            .into_iter()
            .map(|pod| pod.uid)
            .collect::<Vec<_>>();
        actual.sort();
        assert_eq!(actual, expected_uids, "{namespace}/{owner_uid}");
    }

    match pod_store_error(anyhow::anyhow!("service-pod-adapter-sentinel")) {
        PodRepositoryError::Unavailable { message } => {
            assert_eq!(message, "service-pod-adapter-sentinel");
        }
        error => panic!("datastore errors must remain unavailable: {error:?}"),
    }

    assert_eq!(
        db.get_current_resource_version().await.unwrap(),
        resource_version_before_reads,
        "DatastorePodQuery must not mutate cluster state"
    );
}

#[tokio::test]
async fn service_sink_gates_irrelevant_updates_and_stale_targetref_self_extinguishes() {
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let db_handle = db.clone();
    let store = ServicePodResourceFixtureStore(db.clone());
    let sink = Arc::new(klights_controllers::test_support::RecordingReconcileSink::default());
    let slot = ControllerDispatcherSlot::with_service_reconcile_sink(sink.clone());

    let service = db
        .create_resource(
            "v1",
            "Service",
            Some("default"),
            "stale",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "stale", "namespace": "default"},
                "spec": {"selector": {"app": "different"}, "ports": [{"port": 80}]}
            }),
        )
        .await
        .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "stale",
        json!({
            "apiVersion": "v1",
            "kind": "Endpoints",
            "metadata": {"name": "stale", "namespace": "default"},
            "subsets": [{"addresses": [{
                "ip": "10.42.0.8",
                "targetRef": {
                    "kind": "Pod", "namespace": "default", "name": "old", "uid": "uid-old"
                }
            }]}]
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "discovery.k8s.io/v1",
        "EndpointSlice",
        Some("default"),
        "stale-klights",
        json!({
            "apiVersion": "discovery.k8s.io/v1",
            "kind": "EndpointSlice",
            "metadata": {
                "name": "stale-klights",
                "namespace": "default",
                "labels": {
                    "kubernetes.io/service-name": "stale",
                    "endpointslice.kubernetes.io/managed-by": "endpointslice-controller.k8s.io"
                }
            },
            "addressType": "IPv4",
            "endpoints": [{
                "addresses": ["10.42.0.8"],
                "targetRef": {
                    "kind": "Pod", "namespace": "default", "name": "old", "uid": "uid-old"
                }
            }],
            "ports": []
        }),
    )
    .await
    .unwrap();

    let pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "old",
            "namespace": "default",
            "uid": "uid-old",
            "labels": {"app": "old"},
            "deletionTimestamp": "2026-07-20T00:00:00Z"
        },
        "status": {"phase": "Running", "podIP": "10.42.0.8"}
    });
    let mut annotation_only = pod.clone();
    annotation_only["metadata"]["annotations"] = json!({"example": "changed"});

    service_pod::enqueue_services_after_pod_update(&pod, &annotation_only, &store, &slot)
        .await
        .unwrap();
    assert!(sink.pending_keys().await.is_empty());

    service_pod::enqueue_services_after_pod_delete(&pod, &store, &slot)
        .await
        .unwrap();
    assert_eq!(
        sink.pending_keys().await,
        vec![ServiceReconcileKey::new("default", "stale").into_reconcile_key()]
    );

    let pod_query = DatastorePodQuery {
        db: db_handle.clone(),
    };
    klights_controllers::endpoints::reconcile_service_endpoints_batch(
        &store,
        &pod_query,
        klights_controllers::endpoints::ServiceEndpointBatchReconcileRequest {
            service_name: "stale",
            service_uid: &service.uid,
            namespace: "default",
            selector: service.data.pointer("/spec/selector"),
            service_ports: service.data.pointer("/spec/ports"),
            publish_not_ready: false,
        },
    )
    .await
    .unwrap();

    service_pod::enqueue_services_after_pod_delete(&pod, &store, &slot)
        .await
        .unwrap();
    assert_eq!(
        sink.pending_keys().await.len(),
        1,
        "after stale targetRef cleanup the same Pod fact must produce no further Service work"
    );
}

#[tokio::test]
async fn selectorless_manual_endpoints_and_slices_are_never_pod_cleanup_targets() {
    let db = crate::bootstrap::composition_tests::native_api::support::in_memory().await;
    let store = ServicePodResourceFixtureStore(db.clone());
    db.create_resource(
        "v1",
        "Service",
        Some("default"),
        "manual",
        json!({
            "metadata": {"name": "manual", "namespace": "default"},
            "spec": {"ports": [{"port": 80}]}
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "v1",
        "Endpoints",
        Some("default"),
        "manual",
        json!({
            "metadata": {"name": "manual", "namespace": "default"},
            "subsets": [{"addresses": [{"ip": "10.42.0.8", "targetRef": {
                "kind": "Pod", "namespace": "default", "name": "old", "uid": "uid-old"
            }}]}]
        }),
    )
    .await
    .unwrap();
    db.create_resource(
        "discovery.k8s.io/v1",
        "EndpointSlice",
        Some("default"),
        "manual-user",
        json!({
            "metadata": {"name": "manual-user", "namespace": "default", "labels": {
                "kubernetes.io/service-name": "manual",
                "endpointslice.kubernetes.io/managed-by": "example.test/manual"
            }},
            "addressType": "IPv4",
            "endpoints": [{"addresses": ["10.42.0.8"], "targetRef": {
                "kind": "Pod", "namespace": "default", "name": "old", "uid": "uid-old"
            }}]
        }),
    )
    .await
    .unwrap();
    let pod = json!({"metadata": {
        "name": "old", "namespace": "default", "uid": "uid-old", "labels": {"app": "web"}
    }});

    assert!(
        service_pod::service_reconcile_keys_for_pod(&pod, &store, "default")
            .await
            .unwrap()
            .is_empty(),
        "selectorless Service state is user-managed even when targetRefs are stale"
    );
}
