use std::sync::Arc;

use klights_cluster_core::Resource;
use klights_pod_api::{
    PodDeleteOptions, PodDeletePreconditions, PodGetRequest, PodLabel, PodLifecycleFuture,
    PodLifecycleWakeup, PodLifecycleWakeupRequest, PodListRequest, PodListResult,
    PodMarkTerminating, PodMarkTerminatingRequest, PodMutationTarget, PodOwnerListRequest,
    PodOwnerReference, PodQuery, PodRepositoryError, PodRepositoryFuture, PodRoutingError,
    PodUpdate, PodUpdateRequest,
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
            "resourceVersion": resource_version.to_string(),
        },
        "spec": {"nodeName": "worker-a"},
    })))
    .expect("canonical Pod")
}

struct FakeOrdinaryPodAccess;

impl PodQuery for FakeOrdinaryPodAccess {
    fn get_pod(&self, request: PodGetRequest) -> PodRepositoryFuture<'_, Option<Resource>> {
        Box::pin(async move {
            Ok(Some(pod(
                request.namespace(),
                request.name(),
                request.uid().unwrap_or("uid-a"),
                17,
            )))
        })
    }

    fn list_pods(&self, _request: PodListRequest) -> PodRepositoryFuture<'_, PodListResult> {
        Box::pin(async {
            PodListResult::try_new(vec![pod("default", "web", "uid-a", 17)], 17, None, None)
        })
    }

    fn list_pods_by_owner_uid(
        &self,
        _request: PodOwnerListRequest,
    ) -> PodRepositoryFuture<'_, Vec<Resource>> {
        Box::pin(async { Ok(vec![pod("default", "web", "uid-a", 17)]) })
    }
}

impl PodUpdate for FakeOrdinaryPodAccess {
    fn update_pod(&self, request: PodUpdateRequest) -> PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            Ok(pod(
                request.target().namespace(),
                request.target().name(),
                request.target().uid().unwrap_or("uid-a"),
                18,
            ))
        })
    }
}

impl PodMarkTerminating for FakeOrdinaryPodAccess {
    fn mark_pod_terminating(
        &self,
        request: PodMarkTerminatingRequest,
    ) -> PodRepositoryFuture<'_, Resource> {
        Box::pin(async move {
            Ok(pod(
                request.target().namespace(),
                request.target().name(),
                request.target().uid().unwrap_or("uid-a"),
                18,
            ))
        })
    }
}

impl PodLifecycleWakeup for FakeOrdinaryPodAccess {
    fn wake_pod_lifecycle(&self, _request: PodLifecycleWakeupRequest) -> PodLifecycleFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

fn assert_query_object_safe(_: &dyn PodQuery) {}
fn assert_update_object_safe(_: &dyn PodUpdate) {}
fn assert_mark_object_safe(_: &dyn PodMarkTerminating) {}
fn assert_wakeup_object_safe(_: &dyn PodLifecycleWakeup) {}

#[test]
fn ordinary_ports_are_object_safe_and_contract_values_are_send_sync() {
    let fake = FakeOrdinaryPodAccess;
    assert_query_object_safe(&fake);
    assert_update_object_safe(&fake);
    assert_mark_object_safe(&fake);
    assert_wakeup_object_safe(&fake);

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PodGetRequest>();
    assert_send_sync::<PodListRequest>();
    assert_send_sync::<PodListResult>();
    assert_send_sync::<PodOwnerListRequest>();
    assert_send_sync::<PodUpdateRequest>();
    assert_send_sync::<PodMarkTerminatingRequest>();
    assert_send_sync::<PodLifecycleWakeupRequest>();
    assert_send_sync::<PodRepositoryError>();
    assert_send_sync::<PodRoutingError>();
}

#[test]
fn query_requests_preserve_name_uid_selectors_and_page_metadata() {
    let by_name = PodGetRequest::try_by_name("default", "web").expect("name query");
    assert_eq!(by_name.namespace(), "default");
    assert_eq!(by_name.name(), "web");
    assert_eq!(by_name.uid(), None);

    let by_uid = PodGetRequest::try_by_identity(PodIdentity::new("default", "web", "uid-a"))
        .expect("UID query");
    assert_eq!(by_uid.uid(), Some("uid-a"));

    let list = PodListRequest::try_new(
        Some("default".to_string()),
        Some("app=web".to_string()),
        Some("spec.nodeName=worker-a".to_string()),
        Some(20),
        Some("next".to_string()),
    )
    .expect("list request");
    assert_eq!(list.namespace(), Some("default"));
    assert_eq!(list.label_selector(), Some("app=web"));
    assert_eq!(list.field_selector(), Some("spec.nodeName=worker-a"));
    assert_eq!(list.limit(), Some(20));
    assert_eq!(list.continue_token(), Some("next"));
    let owned = PodOwnerListRequest::try_new("default", "owner-a").expect("owner query");
    assert_eq!(owned.namespace(), "default");
    assert_eq!(owned.owner_uid(), "owner-a");

    let result = PodListResult::try_new(
        vec![pod("default", "web", "uid-a", 17)],
        17,
        Some("next".to_string()),
        Some(4),
    )
    .expect("list result");
    assert_eq!(result.items().len(), 1);
    assert_eq!(result.resource_version(), 17);
    assert_eq!(result.continue_token(), Some("next"));
    assert_eq!(result.remaining_item_count(), Some(4));
}

#[test]
fn ordinary_update_is_an_exact_non_deleting_operation_set() {
    let target = PodMutationTarget::try_by_identity(PodIdentity::new("default", "web", "uid-a"))
        .expect("exact target");

    let labels = PodUpdateRequest::merge_labels(
        target.clone(),
        vec![
            PodLabel::try_new("app", "web").expect("label"),
            PodLabel::try_new("track", "stable").expect("label"),
        ],
    );
    assert_eq!(labels.target().uid(), Some("uid-a"));
    assert_eq!(labels.labels().expect("label update").len(), 2);
    assert!(labels.owner_references().is_none());
    assert_eq!(labels.sandbox_id(), None);

    let sandbox = PodUpdateRequest::try_record_sandbox_id(target, "sandbox-a")
        .expect("sandbox annotation update");
    assert_eq!(sandbox.sandbox_id(), Some("sandbox-a"));

    let owners = PodUpdateRequest::replace_owner_references(
        PodMutationTarget::try_by_name("default", "web").expect("target"),
        vec![
            PodOwnerReference::try_new(
                "apps/v1",
                "ReplicaSet",
                "web-rs",
                "rs-uid",
                Some(true),
                Some(false),
            )
            .expect("owner reference"),
        ],
    );
    let owner = &owners.owner_references().expect("owner update")[0];
    assert_eq!(owner.api_version(), "apps/v1");
    assert_eq!(owner.kind(), "ReplicaSet");
    assert_eq!(owner.name(), "web-rs");
    assert_eq!(owner.uid(), "rs-uid");
    assert_eq!(owner.controller(), Some(true));
    assert_eq!(owner.block_owner_deletion(), Some(false));
}

#[test]
fn mark_and_wakeup_are_explicit_intents_without_actor_authority() {
    let target = PodMutationTarget::try_by_name("default", "web").expect("named target");
    let mark = PodMarkTerminatingRequest::new(target);
    assert_eq!(mark.target().namespace(), "default");
    assert_eq!(mark.target().name(), "web");
    assert_eq!(mark.target().uid(), None);

    let resource = pod("default", "web", "uid-a", 17);
    let identity = PodIdentity::new("default", "web", "uid-a");
    let wake = PodLifecycleWakeupRequest::try_from_pod(identity.clone(), resource.clone())
        .expect("wake intent");
    assert_eq!(wake.identity(), &identity);
    assert_eq!(wake.resource_version(), 17);
    assert_eq!(wake.pod().uid, resource.uid);
}

#[test]
fn delete_policy_preserves_kubernetes_options_without_http_types() {
    let options = PodDeleteOptions::new(
        Some("Foreground".to_string()),
        Some(false),
        Some(30),
        PodDeletePreconditions::new(Some("uid-a".to_string()), Some("17".to_string())),
    );
    assert_eq!(options.propagation_policy(), Some("Foreground"));
    assert_eq!(options.orphan_dependents(), Some(false));
    assert_eq!(options.grace_period_seconds(), Some(30));
    assert_eq!(options.preconditions().uid(), Some("uid-a"));
    assert_eq!(options.preconditions().resource_version(), Some("17"));

    let uid_only = PodDeleteOptions::with_uid_precondition("uid-b");
    assert_eq!(uid_only.preconditions().uid(), Some("uid-b"));
    assert_eq!(uid_only.preconditions().resource_version(), None);
}

#[test]
fn invalid_contract_values_and_narrow_errors_fail_closed() {
    for request in [
        PodGetRequest::try_by_name("", "web"),
        PodGetRequest::try_by_name("default", ""),
        PodGetRequest::try_by_identity(PodIdentity::new("default", "web", "")),
    ] {
        assert!(matches!(
            request,
            Err(PodRepositoryError::InvalidRequest { .. })
        ));
    }
    assert!(matches!(
        PodListRequest::try_new(None, None, None, Some(-1), None),
        Err(PodRepositoryError::InvalidRequest {
            field: "list.limit",
            ..
        })
    ));
    assert!(matches!(
        PodLifecycleWakeupRequest::try_from_pod(
            PodIdentity::new("default", "web", "uid-a"),
            Resource::try_from_data(Arc::new(json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"namespace": "default", "name": "web", "uid": "uid-a"}
            })))
            .expect("non-Pod resource")
        ),
        Err(PodRoutingError::InvalidRequest { .. })
    ));
    assert!(matches!(
        PodLifecycleWakeupRequest::try_from_pod(
            PodIdentity::new("default", "web", "stale-uid"),
            pod("default", "web", "replacement-uid", 18),
        ),
        Err(PodRoutingError::InvalidRequest {
            field: "pod.identity",
            ..
        })
    ));

    for error in [
        PodRepositoryError::invalid_request("pod", "bad request"),
        PodRepositoryError::not_found("default", "web"),
        PodRepositoryError::uid_mismatch("uid-a", "uid-b"),
        PodRepositoryError::conflict("resourceVersion conflict"),
        PodRepositoryError::forbidden("admission denied"),
        PodRepositoryError::unprocessable("admission validation failed"),
        PodRepositoryError::internal("deferred queue failed"),
        PodRepositoryError::unavailable("leader unavailable"),
    ] {
        assert!(!error.to_string().is_empty());
    }
    assert!(
        !PodRoutingError::unavailable("actor inbox closed")
            .to_string()
            .is_empty()
    );
}
