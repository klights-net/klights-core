use axum::http::{HeaderMap, Method};
use k8s_native_service::{
    admission::{AdmissionEngine, AdmissionRequestContext},
    audit::{AuditEvent, AuditSink},
    auth_http::{authenticate_request, authenticate_token_for_review, authorize_request},
    priority_fairness::ApiPriorityFairness,
    request_info::{ResolvedAuthz, resolve_request_info},
    response::{K8sResponse, prefers_protobuf},
};

#[test]
fn phase17d_policy_pipeline_public_surface_compiles_from_the_native_owner() {
    let _ = std::any::TypeId::of::<ApiPriorityFairness>();
    let _ = std::any::TypeId::of::<AdmissionRequestContext>();
    let _ = std::any::TypeId::of::<K8sResponse>();
    let _ = std::any::TypeId::of::<AuditEvent>();
    let _ = std::any::TypeId::of::<&dyn AuditSink>();
    let _ = AdmissionEngine::new;
    let _ = authenticate_request;
    let _ = authenticate_token_for_review;
    let _ = authorize_request;
    assert!(!prefers_protobuf(&HeaderMap::new()));
    assert!(matches!(
        resolve_request_info(&Method::GET, "/api/v1/pods", None),
        ResolvedAuthz::Authorize(_)
    ));
}
