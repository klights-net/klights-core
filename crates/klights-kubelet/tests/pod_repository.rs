use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use klights_cluster_core::Resource;
use klights_kubelet::pod_repository::{
    PodLifecycleRouteRequest, PodLifecycleRouteSink, PodLifecycleWakeupService,
    PodMetadataDependencies, PodRepositoryService, PodTerminationPort,
};
use klights_pod_api::{
    PodGetRequest, PodLifecycleWakeup, PodLifecycleWakeupRequest, PodListRequest, PodListResult,
    PodMarkTerminating, PodMarkTerminatingRequest, PodMutationTarget, PodOwnerListRequest,
    PodPersistence, PodPersistenceCreateRequest, PodPersistenceReplaceRequest, PodQuery,
    PodRepositoryError, PodRepositoryFuture, PodRoutingError,
};
use klights_types::PodIdentity;
use serde_json::json;

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
    Get {
        namespace: String,
        name: String,
        uid: Option<String>,
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

#[derive(Default)]
struct RecordingQuery {
    calls: Mutex<Vec<QueryCall>>,
}

impl PodQuery for RecordingQuery {
    fn get_pod(&self, request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
        let uid = request.uid().map(str::to_string);
        self.calls.lock().unwrap().push(QueryCall::Get {
            namespace: request.namespace().to_string(),
            name: request.name().to_string(),
            uid: uid.clone(),
        });
        let resource = pod(
            request.namespace(),
            request.name(),
            uid.as_deref().unwrap_or("uid-live"),
            11,
        );
        Box::pin(async move { Ok(Some(resource)) })
    }

    fn list_pods(&self, request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
        self.calls.lock().unwrap().push(QueryCall::List {
            namespace: request.namespace().map(str::to_string),
            label_selector: request.label_selector().map(str::to_string),
            field_selector: request.field_selector().map(str::to_string),
            limit: request.limit(),
            continue_token: request.continue_token().map(str::to_string),
        });
        Box::pin(async {
            PodListResult::try_new(
                vec![pod("default", "web", "uid-list", 20)],
                20,
                Some("next".to_string()),
                Some(4),
            )
        })
    }

    fn list_pods_by_owner_uid(
        &self,
        request: PodOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<Resource>> {
        self.calls.lock().unwrap().push(QueryCall::ByOwner {
            namespace: request.namespace().to_string(),
            owner_uid: request.owner_uid().to_string(),
        });
        let resource = pod(request.namespace(), "owned", "uid-owned", 21);
        Box::pin(async move { Ok(vec![resource]) })
    }
}

struct UnexpectedPersistence;

impl PodPersistence for UnexpectedPersistence {
    fn create_pod(
        &self,
        _request: PodPersistenceCreateRequest,
    ) -> PodRepositoryFuture<'_, Resource> {
        Box::pin(async { Err(PodRepositoryError::internal("unexpected create")) })
    }

    fn replace_pod(
        &self,
        _request: PodPersistenceReplaceRequest,
    ) -> PodRepositoryFuture<'_, Resource> {
        Box::pin(async { Err(PodRepositoryError::internal("unexpected replace")) })
    }

    fn replace_pod_including_status(
        &self,
        _request: PodPersistenceReplaceRequest,
    ) -> PodRepositoryFuture<'_, Resource> {
        Box::pin(async { Err(PodRepositoryError::internal("unexpected scheduler replace")) })
    }

    fn patch_pod_metadata(
        &self,
        _request: klights_pod_api::PodMetadataPatchRequest,
    ) -> PodRepositoryFuture<'_, Resource> {
        Box::pin(async { Err(PodRepositoryError::internal("unexpected metadata patch")) })
    }
}

struct NoopEffects;

impl klights_reconcile_api::PodMutationReconcileSink for NoopEffects {
    fn reconcile_pod_mutation(
        &self,
        _request: klights_reconcile_api::PodMutationReconcileRequest,
    ) -> klights_reconcile_api::ReconcileSinkFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

struct FixedClock;

impl klights_kubelet::runtime_clock::RuntimeClock for FixedClock {
    fn now_ms(&self) -> i64 {
        1
    }
}

#[derive(Default)]
struct RecordingTermination {
    targets: Mutex<Vec<PodMutationTarget>>,
    fail: bool,
}

impl PodTerminationPort for RecordingTermination {
    fn mark_terminating(&self, target: PodMutationTarget) -> PodRepositoryFuture<'_, Resource> {
        self.targets.lock().unwrap().push(target.clone());
        let fail = self.fail;
        let namespace = target.namespace().to_string();
        let name = target.name().to_string();
        let uid = target.uid().unwrap_or("uid-marked").to_string();
        Box::pin(async move {
            if fail {
                Err(PodRepositoryError::conflict("stale UID"))
            } else {
                Ok(pod(&namespace, &name, &uid, 40))
            }
        })
    }
}

#[test]
fn canonical_query_service_preserves_identity_selectors_and_page_metadata() {
    let query = Arc::new(RecordingQuery::default());
    let service = PodRepositoryService::new(
        query.clone(),
        PodMetadataDependencies {
            persistence: Arc::new(UnexpectedPersistence),
            outbox: None,
            remote_delivery_required: false,
            mutation_reconcile: Arc::new(NoopEffects),
            wall_clock: Arc::new(FixedClock),
        },
        Arc::new(RecordingTermination::default()),
    );

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
        *query.calls.lock().unwrap(),
        vec![
            QueryCall::Get {
                namespace: "default".to_string(),
                name: "web".to_string(),
                uid: None,
            },
            QueryCall::Get {
                namespace: "default".to_string(),
                name: "web".to_string(),
                uid: Some("uid-exact".to_string()),
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
fn canonical_mark_service_preserves_the_validated_target_and_structural_error() {
    fn service(
        query: Arc<RecordingQuery>,
        termination: Arc<RecordingTermination>,
    ) -> PodRepositoryService {
        PodRepositoryService::new(
            query,
            PodMetadataDependencies {
                persistence: Arc::new(UnexpectedPersistence),
                outbox: None,
                remote_delivery_required: false,
                mutation_reconcile: Arc::new(NoopEffects),
                wall_clock: Arc::new(FixedClock),
            },
            termination,
        )
    }

    let target =
        PodMutationTarget::try_by_identity(PodIdentity::new("default", "web", "uid-exact"))
            .unwrap();
    let termination = Arc::new(RecordingTermination::default());
    let marked = resolve(PodMarkTerminating::mark_pod_terminating(
        &service(Arc::new(RecordingQuery::default()), termination.clone()),
        PodMarkTerminatingRequest::new(target.clone()),
    ))
    .unwrap();
    assert_eq!(marked.uid, "uid-exact");
    let actual = termination.targets.lock().unwrap();
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].namespace(), target.namespace());
    assert_eq!(actual[0].name(), target.name());
    assert_eq!(actual[0].uid(), target.uid());
    drop(actual);

    let failing = Arc::new(RecordingTermination {
        targets: Mutex::new(Vec::new()),
        fail: true,
    });
    let error = resolve(PodMarkTerminating::mark_pod_terminating(
        &service(Arc::new(RecordingQuery::default()), failing.clone()),
        PodMarkTerminatingRequest::new(target),
    ))
    .unwrap_err();
    assert!(matches!(
        error,
        PodRepositoryError::Conflict { message } if message == "stale UID"
    ));
    assert_eq!(failing.targets.lock().unwrap().len(), 1);
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
