use std::sync::Arc;

use klights_controllers::side_effects::{ControllerDispatcherSlot, service_pod};
use klights_reconcile_api::{ReconcileSinkFuture, ServiceReconcileKey, ServiceReconcileSink};
use serde_json::json;

#[derive(Default)]
struct RecordingServiceSink {
    keys: tokio::sync::Mutex<Vec<ServiceReconcileKey>>,
}

impl ServiceReconcileSink for RecordingServiceSink {
    fn enqueue_service_reconcile_batch(
        &self,
        keys: Vec<ServiceReconcileKey>,
    ) -> ReconcileSinkFuture<'_> {
        Box::pin(async move {
            self.keys.lock().await.extend(keys);
            Ok(())
        })
    }
}

#[test]
fn unchanged_endpoint_classification_is_allocation_free() {
    let previous = json!({
        "metadata": {"labels": {"app": "web"}},
        "status": {
            "phase": "Running",
            "podIP": "10.42.0.8",
            "podIPs": [{"ip": "10.42.0.8"}],
            "conditions": [{"type": "Ready", "status": "True"}]
        }
    });
    let updated = previous.clone();
    let allocated =
        tikv_jemalloc_ctl::thread::allocatedp::read().expect("read thread allocation counter");
    let before = allocated.get();
    for _ in 0..4096 {
        std::hint::black_box(service_pod::pod_endpoint_state_changed(&previous, &updated));
    }
    assert_eq!(
        allocated.get() - before,
        0,
        "borrowed unchanged-Pod classification must stay allocation-free"
    );
}

#[tokio::test]
async fn service_sink_gates_irrelevant_updates_and_stale_targetref_self_extinguishes() {
    let (db, db_handle) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
    let sink = Arc::new(RecordingServiceSink::default());
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

    service_pod::enqueue_services_after_pod_update(
        &pod,
        &annotation_only,
        db_handle.as_ref(),
        &slot,
    )
    .await
    .unwrap();
    assert!(sink.keys.lock().await.is_empty());

    service_pod::enqueue_services_after_pod_delete(&pod, db_handle.as_ref(), &slot)
        .await
        .unwrap();
    assert_eq!(
        *sink.keys.lock().await,
        vec![ServiceReconcileKey::new("default", "stale")]
    );

    let pod_store = crate::kubelet::pod_repository::store::PodStore::new(db_handle.clone());
    klights_controllers::endpoints::reconcile_service_endpoints_batch(
        db_handle.as_ref(),
        &pod_store,
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

    service_pod::enqueue_services_after_pod_delete(&pod, db_handle.as_ref(), &slot)
        .await
        .unwrap();
    assert_eq!(
        sink.keys.lock().await.len(),
        1,
        "after stale targetRef cleanup the same Pod fact must produce no further Service work"
    );
}

#[tokio::test]
async fn selectorless_manual_endpoints_and_slices_are_never_pod_cleanup_targets() {
    let (db, db_handle) = crate::datastore::sqlite::Datastore::new_in_memory_with_handle().await;
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
        service_pod::service_reconcile_keys_for_pod(&pod, db_handle.as_ref(), "default")
            .await
            .unwrap()
            .is_empty(),
        "selectorless Service state is user-managed even when targetRefs are stale"
    );
}
