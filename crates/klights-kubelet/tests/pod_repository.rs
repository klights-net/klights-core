use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use klights_cluster_core::Resource;
use klights_kubelet::pod_repository::{
    PodLifecycleRouteRequest, PodLifecycleRouteSink, PodLifecycleWakeupService, PodQueryPort,
    PodRepositoryList, PodRepositoryService, PodTerminationPort, PodUpdatePort,
};
use klights_pod_api::{
    PodGetRequest, PodLabel, PodLifecycleWakeup, PodLifecycleWakeupRequest, PodListRequest,
    PodMarkTerminating, PodMarkTerminatingRequest, PodMutationTarget, PodOwnerListRequest,
    PodOwnerReference, PodQuery, PodRepositoryError, PodRepositoryFuture, PodRoutingError,
    PodUpdate, PodUpdateRequest,
};
use klights_types::PodIdentity;
use serde_json::{Value, json};

fn pod(namespace: &str, name: &str, uid: &str, resource_version: i64) -> Resource {
    Resource::try_from_data(Arc::new(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "namespace": namespace,
            "name": name,
            "uid": uid,
            "resourceVersion": resource_version.to_string()
        }
    })))
    .unwrap()
}

fn resolve<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("fake-port future unexpectedly pending"),
    }
}

#[derive(Clone, Debug, PartialEq)]
enum QueryCall {
    ByName {
        namespace: String,
        name: String,
    },
    ByUid {
        namespace: String,
        name: String,
        uid: String,
    },
    List {
        namespace: Option<String>,
        label_selector: Option<String>,
        field_selector: Option<String>,
        limit: Option<i64>,
        continue_token: Option<String>,
    },
    ByOwner {
        namespace: String,
        owner_uid: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
enum UpdateCall {
    MergeLabels {
        identity: PodIdentity,
        labels: Vec<(String, String)>,
    },
    ReplaceOwnerReferences {
        identity: PodIdentity,
        owner_references: Vec<Value>,
    },
    RecordSandboxId {
        identity: PodIdentity,
        sandbox_id: String,
    },
}

#[derive(Default)]
struct FakePodPorts {
    query_calls: Mutex<Vec<QueryCall>>,
    update_calls: Mutex<Vec<UpdateCall>>,
    marked: Mutex<Vec<PodMutationTarget>>,
}

fn identity(namespace: &str, name: &str, uid: Option<&str>) -> PodIdentity {
    PodIdentity::new(namespace, name, uid.unwrap_or_default())
}

impl PodQueryPort for FakePodPorts {
    fn read_pod<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
    ) -> PodRepositoryFuture<'a, Option<Resource>> {
        self.query_calls.lock().unwrap().push(QueryCall::ByName {
            namespace: namespace.to_string(),
            name: name.to_string(),
        });
        Box::pin(async { Ok(Some(pod(namespace, name, "uid-live", 11))) })
    }

    fn read_pod_for_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        uid: &'a str,
    ) -> PodRepositoryFuture<'a, Option<Resource>> {
        self.query_calls.lock().unwrap().push(QueryCall::ByUid {
            namespace: namespace.to_string(),
            name: name.to_string(),
            uid: uid.to_string(),
        });
        Box::pin(async { Ok(Some(pod(namespace, name, uid, 12))) })
    }

    fn list_pod_page(
        &self,
        request: &PodListRequest,
    ) -> PodRepositoryFuture<'_, PodRepositoryList> {
        self.query_calls.lock().unwrap().push(QueryCall::List {
            namespace: request.namespace().map(str::to_string),
            label_selector: request.label_selector().map(str::to_string),
            field_selector: request.field_selector().map(str::to_string),
            limit: request.limit(),
            continue_token: request.continue_token().map(str::to_string),
        });
        Box::pin(async {
            Ok(PodRepositoryList::new(
                vec![pod("default", "web", "uid-list", 20)],
                20,
                Some("next".to_string()),
                Some(4),
            ))
        })
    }

    fn list_pods_by_owner_uid<'a>(
        &'a self,
        namespace: &'a str,
        owner_uid: &'a str,
    ) -> PodRepositoryFuture<'a, Vec<Resource>> {
        self.query_calls.lock().unwrap().push(QueryCall::ByOwner {
            namespace: namespace.to_string(),
            owner_uid: owner_uid.to_string(),
        });
        Box::pin(async { Ok(vec![pod(namespace, "owned", "uid-owned", 21)]) })
    }
}

impl PodUpdatePort for FakePodPorts {
    fn merge_pod_labels<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        labels: Vec<(String, String)>,
    ) -> PodRepositoryFuture<'a, Resource> {
        self.update_calls
            .lock()
            .unwrap()
            .push(UpdateCall::MergeLabels {
                identity: identity(namespace, name, None),
                labels,
            });
        Box::pin(async { Ok(pod(namespace, name, "uid-updated", 30)) })
    }

    fn merge_pod_labels_for_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        uid: &'a str,
        labels: Vec<(String, String)>,
    ) -> PodRepositoryFuture<'a, Resource> {
        self.update_calls
            .lock()
            .unwrap()
            .push(UpdateCall::MergeLabels {
                identity: identity(namespace, name, Some(uid)),
                labels,
            });
        Box::pin(async { Ok(pod(namespace, name, uid, 30)) })
    }

    fn replace_pod_owner_references<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        owner_references: Vec<Value>,
    ) -> PodRepositoryFuture<'a, Resource> {
        self.update_calls
            .lock()
            .unwrap()
            .push(UpdateCall::ReplaceOwnerReferences {
                identity: identity(namespace, name, None),
                owner_references,
            });
        Box::pin(async { Ok(pod(namespace, name, "uid-updated", 31)) })
    }

    fn replace_pod_owner_references_for_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        uid: &'a str,
        owner_references: Vec<Value>,
    ) -> PodRepositoryFuture<'a, Resource> {
        self.update_calls
            .lock()
            .unwrap()
            .push(UpdateCall::ReplaceOwnerReferences {
                identity: identity(namespace, name, Some(uid)),
                owner_references,
            });
        Box::pin(async { Ok(pod(namespace, name, uid, 31)) })
    }

    fn record_pod_sandbox_id<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        sandbox_id: String,
    ) -> PodRepositoryFuture<'a, Resource> {
        self.update_calls
            .lock()
            .unwrap()
            .push(UpdateCall::RecordSandboxId {
                identity: identity(namespace, name, None),
                sandbox_id,
            });
        Box::pin(async { Ok(pod(namespace, name, "uid-updated", 32)) })
    }

    fn record_pod_sandbox_id_for_uid<'a>(
        &'a self,
        namespace: &'a str,
        name: &'a str,
        uid: &'a str,
        sandbox_id: String,
    ) -> PodRepositoryFuture<'a, Resource> {
        self.update_calls
            .lock()
            .unwrap()
            .push(UpdateCall::RecordSandboxId {
                identity: identity(namespace, name, Some(uid)),
                sandbox_id,
            });
        Box::pin(async { Ok(pod(namespace, name, uid, 32)) })
    }
}

impl PodTerminationPort for FakePodPorts {
    fn mark_terminating(&self, target: PodMutationTarget) -> PodRepositoryFuture<'_, Resource> {
        self.marked.lock().unwrap().push(target);
        Box::pin(async { Ok(pod("default", "web", "uid-marked", 40)) })
    }
}

#[test]
fn query_service_routes_name_uid_page_and_owner_requests_exactly() {
    let ports = Arc::new(FakePodPorts::default());
    let service = PodRepositoryService::new(ports.clone(), ports.clone(), ports.clone());

    let by_name = resolve(PodQuery::get_pod(
        &service,
        PodGetRequest::try_by_name("default", "web").unwrap(),
    ))
    .unwrap()
    .unwrap();
    assert_eq!(by_name.uid, "uid-live");

    let by_uid = resolve(PodQuery::get_pod(
        &service,
        PodGetRequest::try_by_identity(PodIdentity::new("default", "web", "uid-exact")).unwrap(),
    ))
    .unwrap()
    .unwrap();
    assert_eq!(by_uid.uid, "uid-exact");

    let listed = resolve(PodQuery::list_pods(
        &service,
        PodListRequest::try_new(
            Some("default".to_string()),
            Some("app=web".to_string()),
            Some("status.phase=Running".to_string()),
            Some(25),
            Some("cursor".to_string()),
        )
        .unwrap(),
    ))
    .unwrap();
    assert_eq!(listed.resource_version(), 20);
    assert_eq!(listed.continue_token(), Some("next"));
    assert_eq!(listed.remaining_item_count(), Some(4));

    let owned = resolve(PodQuery::list_pods_by_owner_uid(
        &service,
        PodOwnerListRequest::try_new("default", "owner-uid").unwrap(),
    ))
    .unwrap();
    assert_eq!(owned[0].uid, "uid-owned");

    assert_eq!(
        *ports.query_calls.lock().unwrap(),
        vec![
            QueryCall::ByName {
                namespace: "default".to_string(),
                name: "web".to_string(),
            },
            QueryCall::ByUid {
                namespace: "default".to_string(),
                name: "web".to_string(),
                uid: "uid-exact".to_string(),
            },
            QueryCall::List {
                namespace: Some("default".to_string()),
                label_selector: Some("app=web".to_string()),
                field_selector: Some("status.phase=Running".to_string()),
                limit: Some(25),
                continue_token: Some("cursor".to_string()),
            },
            QueryCall::ByOwner {
                namespace: "default".to_string(),
                owner_uid: "owner-uid".to_string(),
            },
        ]
    );
}

#[test]
fn update_service_preserves_every_variant_and_uid_qualification() {
    let ports = Arc::new(FakePodPorts::default());
    let service = PodRepositoryService::new(ports.clone(), ports.clone(), ports.clone());

    for uid in [None, Some("uid-exact")] {
        let target = match uid {
            Some(uid) => {
                PodMutationTarget::try_by_identity(PodIdentity::new("default", "web", uid)).unwrap()
            }
            None => PodMutationTarget::try_by_name("default", "web").unwrap(),
        };
        resolve(PodUpdate::update_pod(
            &service,
            PodUpdateRequest::merge_labels(
                target.clone(),
                vec![PodLabel::try_new("app", "web").unwrap()],
            ),
        ))
        .unwrap();
        resolve(PodUpdate::update_pod(
            &service,
            PodUpdateRequest::replace_owner_references(
                target.clone(),
                vec![
                    PodOwnerReference::try_new(
                        "apps/v1",
                        "ReplicaSet",
                        "web-rs",
                        "owner-uid",
                        Some(true),
                        Some(false),
                    )
                    .unwrap(),
                ],
            ),
        ))
        .unwrap();
        resolve(PodUpdate::update_pod(
            &service,
            PodUpdateRequest::try_record_sandbox_id(target, "sandbox-a").unwrap(),
        ))
        .unwrap();
    }

    let calls = ports.update_calls.lock().unwrap();
    assert_eq!(calls.len(), 6);
    for (index, expected_uid) in [None, Some("uid-exact")].into_iter().enumerate() {
        fn call_identity(call: &UpdateCall) -> &PodIdentity {
            match call {
                UpdateCall::MergeLabels { identity, .. }
                | UpdateCall::ReplaceOwnerReferences { identity, .. }
                | UpdateCall::RecordSandboxId { identity, .. } => identity,
            }
        }
        for call in &calls[index * 3..index * 3 + 3] {
            assert_eq!(
                if call_identity(call).uid.is_empty() {
                    None
                } else {
                    Some(call_identity(call).uid.as_str())
                },
                expected_uid
            );
        }
    }
    assert_eq!(
        calls[0],
        UpdateCall::MergeLabels {
            identity: PodIdentity::new("default", "web", ""),
            labels: vec![("app".to_string(), "web".to_string())],
        }
    );
    assert_eq!(
        calls[1],
        UpdateCall::ReplaceOwnerReferences {
            identity: PodIdentity::new("default", "web", ""),
            owner_references: vec![json!({
                "apiVersion": "apps/v1",
                "kind": "ReplicaSet",
                "name": "web-rs",
                "uid": "owner-uid",
                "controller": true,
                "blockOwnerDeletion": false
            })],
        }
    );
    assert_eq!(
        calls[2],
        UpdateCall::RecordSandboxId {
            identity: PodIdentity::new("default", "web", ""),
            sandbox_id: "sandbox-a".to_string(),
        }
    );
}

#[test]
fn mark_service_delegates_only_the_validated_target_and_preserves_errors() {
    let ports = Arc::new(FakePodPorts::default());
    let service = PodRepositoryService::new(ports.clone(), ports.clone(), ports.clone());
    let target =
        PodMutationTarget::try_by_identity(PodIdentity::new("default", "web", "uid-exact"))
            .unwrap();

    let marked = resolve(PodMarkTerminating::mark_pod_terminating(
        &service,
        PodMarkTerminatingRequest::new(target.clone()),
    ))
    .unwrap();

    assert_eq!(marked.uid, "uid-marked");
    let actual = ports.marked.lock().unwrap();
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].namespace(), target.namespace());
    assert_eq!(actual[0].name(), target.name());
    assert_eq!(actual[0].uid(), target.uid());
    drop(actual);

    struct FailingMarker;
    impl PodTerminationPort for FailingMarker {
        fn mark_terminating(
            &self,
            _target: PodMutationTarget,
        ) -> PodRepositoryFuture<'_, Resource> {
            Box::pin(async { Err(PodRepositoryError::conflict("stale UID")) })
        }
    }
    let failing = PodRepositoryService::new(
        ports.clone(),
        ports,
        Arc::new(FailingMarker) as Arc<dyn PodTerminationPort>,
    );
    let error = resolve(PodMarkTerminating::mark_pod_terminating(
        &failing,
        PodMarkTerminatingRequest::new(target),
    ))
    .unwrap_err();
    assert!(matches!(
        error,
        PodRepositoryError::Conflict { message } if message == "stale UID"
    ));
}

#[derive(Default)]
struct RecordingRouteSink {
    routed: Mutex<Vec<PodLifecycleRouteRequest>>,
}

impl PodLifecycleRouteSink for RecordingRouteSink {
    fn route_pod_lifecycle(
        &self,
        request: PodLifecycleRouteRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), PodRoutingError>> + Send + '_>> {
        self.routed.lock().unwrap().push(request);
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn lifecycle_wakeup_service_routes_uid_resource_version_and_pod_without_router_types() {
    let sink = Arc::new(RecordingRouteSink::default());
    let service = PodLifecycleWakeupService::new(sink.clone());
    let resource = pod("default", "web", "uid-exact", 55);

    resolve(PodLifecycleWakeup::wake_pod_lifecycle(
        &service,
        PodLifecycleWakeupRequest::try_from_pod(
            PodIdentity::new("default", "web", "uid-exact"),
            resource.clone(),
        )
        .unwrap(),
    ))
    .unwrap();

    let mut routed = sink.routed.lock().unwrap();
    let route = routed.pop().unwrap();
    assert_eq!(route.identity().namespace, "default");
    assert_eq!(route.identity().name, "web");
    assert_eq!(route.identity().uid, "uid-exact");
    assert_eq!(route.resource_version(), 55);
    assert_eq!(route.pod().uid, resource.uid);
    assert_eq!(
        route.pod().data.pointer("/metadata/uid"),
        Some(&json!("uid-exact"))
    );
}
