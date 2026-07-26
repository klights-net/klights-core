//! Service-namespace helpers for reconciling Services after endpoint-affecting
//! Pod mutations.

use super::ControllerDispatcherSlot;
use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ServiceReconcileKey;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(crate) struct ServiceEndpointState {
    pub(crate) services: Vec<Resource>,
    pub(crate) endpoints: Vec<Resource>,
    pub(crate) endpoint_slices: Vec<Resource>,
}

#[async_trait]
pub(crate) trait ServicePodStore: Send + Sync {
    async fn load_service_endpoint_state(&self, namespace: &str) -> Result<ServiceEndpointState>;
}

pub(crate) async fn service_reconcile_keys_for_pod<Store: ServicePodStore + ?Sized>(
    pod: &Value,
    store: &Store,
    namespace: &str,
) -> Result<Vec<ServiceReconcileKey>> {
    service_reconcile_keys_for_pods(&[pod], store, namespace).await
}

async fn service_reconcile_keys_for_pods<Store: ServicePodStore + ?Sized>(
    pods: &[&Value],
    store: &Store,
    namespace: &str,
) -> Result<Vec<ServiceReconcileKey>> {
    let state = store.load_service_endpoint_state(namespace).await?;
    let endpoints_by_service: HashMap<String, Arc<Value>> = state
        .endpoints
        .into_iter()
        .map(|resource| (resource.name, resource.data))
        .collect();
    let mut slices_by_service: HashMap<String, Vec<Arc<Value>>> = HashMap::new();
    for slice in state.endpoint_slices {
        if slice
            .data
            .pointer("/metadata/labels/endpointslice.kubernetes.io~1managed-by")
            .and_then(Value::as_str)
            != Some("endpointslice-controller.k8s.io")
        {
            continue;
        }
        let Some(service_name) = slice
            .data
            .pointer("/metadata/labels/kubernetes.io~1service-name")
            .and_then(Value::as_str)
        else {
            continue;
        };
        slices_by_service
            .entry(service_name.to_string())
            .or_default()
            .push(slice.data);
    }
    let mut keys = Vec::new();
    let mut seen = HashSet::new();

    for service in state.services {
        if service.data.pointer("/spec/type").and_then(|v| v.as_str()) == Some("ExternalName") {
            continue;
        }
        let Some(selector) = crate::controllers::endpoints::endpoints_selector(
            service.data.pointer("/spec/selector"),
        ) else {
            continue;
        };
        let selected = pods.iter().any(|pod| selector.matches_resource(pod));
        let stale_endpoints = endpoints_by_service
            .get(&service.name)
            .is_some_and(|endpoints| {
                pods.iter().any(|pod| {
                    resource_target_refs_reference_pod(
                        endpoints.pointer("/subsets"),
                        &["addresses", "notReadyAddresses"],
                        namespace,
                        pod,
                    )
                })
            });
        let stale_slices = slices_by_service.get(&service.name).is_some_and(|slices| {
            slices.iter().any(|slice| {
                pods.iter().any(|pod| {
                    endpoint_slice_entries_reference_pod(
                        slice.pointer("/endpoints"),
                        namespace,
                        pod,
                    )
                })
            })
        });
        if !selected && !stale_endpoints && !stale_slices {
            continue;
        }
        let key = ServiceReconcileKey::new(namespace, &service.name);
        if seen.insert(key.clone()) {
            keys.push(key);
        }
    }

    Ok(keys)
}

pub(crate) async fn enqueue_services_after_pod_create<Store: ServicePodStore + ?Sized>(
    pod: &Value,
    store: &Store,
    controller_dispatcher: &ControllerDispatcherSlot,
) -> Result<()> {
    let Some(dispatcher) = controller_dispatcher.service() else {
        return Ok(());
    };
    let namespace = pod
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    dispatcher
        .enqueue_service_reconcile_batch(
            service_reconcile_keys_for_pod(pod, store, namespace).await?,
        )
        .await?;
    Ok(())
}

pub(crate) async fn enqueue_services_after_pod_update<Store: ServicePodStore + ?Sized>(
    previous: &Value,
    updated: &Value,
    store: &Store,
    controller_dispatcher: &ControllerDispatcherSlot,
) -> Result<()> {
    if !pod_endpoint_state_changed(previous, updated) {
        return Ok(());
    }
    let Some(dispatcher) = controller_dispatcher.service() else {
        return Ok(());
    };
    let namespace = updated
        .pointer("/metadata/namespace")
        .or_else(|| previous.pointer("/metadata/namespace"))
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    dispatcher
        .enqueue_service_reconcile_batch(
            service_reconcile_keys_for_pods(&[previous, updated], store, namespace).await?,
        )
        .await?;
    Ok(())
}

pub(crate) async fn enqueue_services_after_pod_delete<Store: ServicePodStore + ?Sized>(
    deleted: &Value,
    store: &Store,
    controller_dispatcher: &ControllerDispatcherSlot,
) -> Result<()> {
    let Some(dispatcher) = controller_dispatcher.service() else {
        return Ok(());
    };
    let namespace = deleted
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    dispatcher
        .enqueue_service_reconcile_batch(
            service_reconcile_keys_for_pod(deleted, store, namespace).await?,
        )
        .await?;
    Ok(())
}

pub fn pod_endpoint_state_changed(previous: &Value, updated: &Value) -> bool {
    crate::pod_endpoint_state::pod_endpoint_state(previous)
        .differs_from(&crate::pod_endpoint_state::pod_endpoint_state(updated))
}

fn pod_identity(pod: &Value) -> (&str, Option<&str>) {
    let pod_name = pod
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let pod_uid = pod.pointer("/metadata/uid").and_then(|v| v.as_str());
    (pod_name, pod_uid)
}

#[cfg(test)]
fn endpoint_addresses_reference_pod(
    subsets: Option<&Value>,
    namespace: &str,
    pod_name: &str,
    pod_uid: Option<&str>,
) -> bool {
    resource_target_refs_reference_identity(
        subsets,
        &["addresses", "notReadyAddresses"],
        namespace,
        pod_name,
        pod_uid,
    )
}

fn resource_target_refs_reference_pod(
    groups: Option<&Value>,
    fields: &[&str],
    namespace: &str,
    pod: &Value,
) -> bool {
    let (pod_name, pod_uid) = pod_identity(pod);
    resource_target_refs_reference_identity(groups, fields, namespace, pod_name, pod_uid)
}

fn resource_target_refs_reference_identity(
    groups: Option<&Value>,
    fields: &[&str],
    namespace: &str,
    pod_name: &str,
    pod_uid: Option<&str>,
) -> bool {
    let Some(groups) = groups.and_then(|v| v.as_array()) else {
        return false;
    };
    groups.iter().any(|group| {
        fields.iter().any(|field| {
            group
                .get(*field)
                .and_then(|v| v.as_array())
                .is_some_and(|addresses| {
                    addresses.iter().any(|address| {
                        let Some(target_ref) = address.get("targetRef") else {
                            return false;
                        };
                        if target_ref.get("kind").and_then(|v| v.as_str()) != Some("Pod") {
                            return false;
                        }
                        let target_namespace = target_ref
                            .get("namespace")
                            .and_then(|v| v.as_str())
                            .unwrap_or(namespace);
                        if target_namespace != namespace {
                            return false;
                        }
                        let target_name = target_ref.get("name").and_then(|v| v.as_str());
                        let target_uid = target_ref.get("uid").and_then(|v| v.as_str());
                        match (target_uid, pod_uid) {
                            // When both identities are present, UID is authoritative.
                            // A same-name replacement must not inherit stale work for
                            // the Pod that previously occupied that name.
                            (Some(target_uid), Some(pod_uid)) => target_uid == pod_uid,
                            // Legacy Endpoints may omit targetRef.uid. Preserve their
                            // Kubernetes-compatible name fallback only in that case.
                            _ => target_name == Some(pod_name),
                        }
                    })
                })
        })
    })
}

fn endpoint_slice_entries_reference_pod(
    endpoints: Option<&Value>,
    namespace: &str,
    pod: &Value,
) -> bool {
    let (pod_name, pod_uid) = pod_identity(pod);
    let Some(endpoints) = endpoints.and_then(Value::as_array) else {
        return false;
    };
    endpoints.iter().any(|endpoint| {
        let Some(target_ref) = endpoint.get("targetRef") else {
            return false;
        };
        target_ref.get("kind").and_then(Value::as_str) == Some("Pod")
            && target_ref
                .get("namespace")
                .and_then(Value::as_str)
                .unwrap_or(namespace)
                == namespace
            && match (target_ref.get("uid").and_then(Value::as_str), pod_uid) {
                (Some(target_uid), Some(pod_uid)) => target_uid == pod_uid,
                _ => target_ref.get("name").and_then(Value::as_str) == Some(pod_name),
            }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

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
    fn endpoint_targetref_matching_is_uid_safe_for_ready_and_not_ready_addresses() {
        struct Case {
            description: &'static str,
            field: &'static str,
            target_name: &'static str,
            target_uid: Option<&'static str>,
            pod_name: &'static str,
            pod_uid: Option<&'static str>,
            expected: bool,
        }

        let cases = [
            Case {
                description: "ready address exact identity",
                field: "addresses",
                target_name: "old",
                target_uid: Some("uid-old"),
                pod_name: "old",
                pod_uid: Some("uid-old"),
                expected: true,
            },
            Case {
                description: "same name replacement has a different UID",
                field: "addresses",
                target_name: "web",
                target_uid: Some("uid-old"),
                pod_name: "web",
                pod_uid: Some("uid-new"),
                expected: false,
            },
            Case {
                description: "UID identifies renamed target",
                field: "addresses",
                target_name: "old-name",
                target_uid: Some("uid-same"),
                pod_name: "new-name",
                pod_uid: Some("uid-same"),
                expected: true,
            },
            Case {
                description: "legacy target without UID falls back to name",
                field: "addresses",
                target_name: "web",
                target_uid: None,
                pod_name: "web",
                pod_uid: Some("uid-web"),
                expected: true,
            },
            Case {
                description: "not-ready address uses the same identity policy",
                field: "notReadyAddresses",
                target_name: "pending",
                target_uid: Some("uid-pending"),
                pod_name: "pending",
                pod_uid: Some("uid-pending"),
                expected: true,
            },
        ];

        for case in cases {
            let target_ref = match case.target_uid {
                Some(uid) => json!({
                    "kind": "Pod",
                    "namespace": "default",
                    "name": case.target_name,
                    "uid": uid
                }),
                None => json!({
                    "kind": "Pod",
                    "namespace": "default",
                    "name": case.target_name
                }),
            };
            let subsets = json!([{(case.field): [{
                "ip": "10.42.0.8",
                "targetRef": target_ref
            }]}]);

            assert_eq!(
                endpoint_addresses_reference_pod(
                    Some(&subsets),
                    "default",
                    case.pod_name,
                    case.pod_uid,
                ),
                case.expected,
                "{}",
                case.description
            );
        }
    }

    #[test]
    fn endpoint_change_classification_is_limited_to_endpoint_relevant_fields() {
        let base = json!({
            "metadata": {"labels": {"app": "web"}},
            "status": {"phase": "Running"}
        });
        let cases = [
            (
                "annotations",
                json!({"metadata": {
                "labels": {"app": "web"}, "annotations": {"x": "y"}
            }, "status": {"phase": "Running"}}),
                false,
            ),
            (
                "labels",
                json!({"metadata": {"labels": {"app": "api"}},
                "status": {"phase": "Running"}}),
                true,
            ),
            (
                "podIP",
                json!({"metadata": {"labels": {"app": "web"}},
                "status": {"phase": "Running", "podIP": "10.42.0.8"}}),
                true,
            ),
            (
                "podIPs",
                json!({"metadata": {"labels": {"app": "web"}},
                "status": {"phase": "Running", "podIPs": [{"ip": "10.42.0.8"}]}}),
                true,
            ),
            (
                "Ready",
                json!({"metadata": {"labels": {"app": "web"}},
                "status": {"phase": "Running", "conditions": [
                    {"type": "Ready", "status": "True"}
                ]}}),
                true,
            ),
            (
                "container readiness fallback is not a Ready condition",
                json!({"metadata": {"labels": {"app": "web"}},
                "status": {"phase": "Running", "containerStatuses": [
                    {"name": "web", "ready": true}
                ]}}),
                false,
            ),
            (
                "deletionTimestamp",
                json!({"metadata": {
                "labels": {"app": "web"}, "deletionTimestamp": "2026-07-20T00:00:00Z"
            }, "status": {"phase": "Running"}}),
                true,
            ),
            (
                "terminal phase",
                json!({"metadata": {"labels": {"app": "web"}},
                "status": {"phase": "Succeeded"}}),
                true,
            ),
        ];

        for (description, updated, expected) in cases {
            assert_eq!(
                pod_endpoint_state_changed(&base, &updated),
                expected,
                "{description}"
            );
        }

        let effectively_not_ready = [
            json!({"status": {"conditions": [{"type": "Ready", "status": "False"}]}}),
            json!({"status": {"conditions": [{"type": "Ready", "status": "Unknown"}]}}),
            json!({"status": {"conditions": []}}),
            json!({"status": {}}),
        ];
        for previous in &effectively_not_ready {
            for updated in &effectively_not_ready {
                assert!(
                    !pod_endpoint_state_changed(previous, updated),
                    "False, Unknown, and missing Ready are one effective endpoint state"
                );
            }
        }
        assert!(!pod_endpoint_state_changed(
            &json!({"status": {"phase": "Succeeded"}}),
            &json!({"status": {"phase": "Failed"}}),
        ));
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
            std::hint::black_box(pod_endpoint_state_changed(&previous, &updated));
        }
        let allocated_bytes = allocated.get() - before;
        assert_eq!(
            allocated_bytes, 0,
            "borrowed unchanged-Pod classification must stay allocation-free"
        );
    }

    #[tokio::test]
    async fn service_sink_gates_irrelevant_updates_and_stale_targetref_self_extinguishes() {
        let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        let sink = Arc::new(RecordingServiceSink::default());
        let slot = ControllerDispatcherSlot::with_service_reconcile_sink_for_test(sink.clone());

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
                    "spec": {"selector": {"app": "different"}}
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
                        "kind": "Pod",
                        "namespace": "default",
                        "name": "old",
                        "uid": "uid-old"
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
                    "targetRef": {"kind": "Pod", "namespace": "default", "name": "old", "uid": "uid-old"}
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

        enqueue_services_after_pod_update(&pod, &annotation_only, db_handle.as_ref(), &slot)
            .await
            .unwrap();
        assert!(sink.keys.lock().await.is_empty());

        enqueue_services_after_pod_delete(&pod, db_handle.as_ref(), &slot)
            .await
            .unwrap();
        assert_eq!(
            *sink.keys.lock().await,
            vec![ServiceReconcileKey::new("default", "stale")]
        );

        let pod_store = crate::kubelet::pod_repository::store::PodStore::new(db_handle.clone());
        crate::controllers::endpoints::reconcile_service_endpoints_batch(
            db_handle.as_ref(),
            &pod_store,
            crate::controllers::endpoints::ServiceEndpointBatchReconcileRequest {
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

        enqueue_services_after_pod_delete(&pod, db_handle.as_ref(), &slot)
            .await
            .unwrap();
        assert_eq!(
            sink.keys.lock().await.len(),
            1,
            "after stale targetRef cleanup, the same Pod fact must produce no further Service work"
        );
    }

    #[tokio::test]
    async fn selectorless_manual_endpoints_and_slices_are_never_pod_cleanup_targets() {
        let (db, db_handle) = crate::datastore::test_support::in_memory_with_handle().await;
        db.create_resource(
            "v1",
            "Service",
            Some("default"),
            "manual",
            json!({
                "apiVersion": "v1",
                "kind": "Service",
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
            service_reconcile_keys_for_pod(&pod, db_handle.as_ref(), "default")
                .await
                .unwrap()
                .is_empty(),
            "selectorless Service state is user-managed even when targetRefs are stale"
        );
    }
}
