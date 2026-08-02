use crate::api::*;
use axum::{
    Json, Router,
    extract::{Query, State},
};
use serde_json::Value;
use std::sync::Arc;

pub fn networking_v1_routes() -> Router<Arc<ApiState>> {
    Router::new()
        .route("/ingresses", get(list_all_ingresses))
        .route(
            "/namespaces/{namespace}/ingresses",
            get(list_ingresses)
                .post(create_ingress)
                .delete(delete_collection_ingresses),
        )
        .route(
            "/namespaces/{namespace}/ingresses/{name}",
            get(get_ingress)
                .put(update_ingress)
                .patch(patch_ingress)
                .delete(delete_ingress),
        )
        .route(
            "/namespaces/{namespace}/ingresses/{name}/status",
            get(get_ingress)
                .put(update_ingress_status)
                .patch(patch_ingress_status),
        )
        .route(
            "/ingressclasses",
            get(list_ingressclasses)
                .post(create_ingressclass)
                .delete(delete_collection_ingressclasses),
        )
        .route(
            "/ingressclasses/{name}",
            get(get_ingressclass)
                .put(update_ingressclass)
                .patch(patch_ingressclass)
                .delete(delete_ingressclass),
        )
        .route(
            "/servicecidrs",
            get(list_servicecidrs).post(create_servicecidr),
        )
        .route(
            "/servicecidrs/{name}",
            get(get_servicecidr)
                .put(update_servicecidr)
                .patch(patch_servicecidr)
                .delete(delete_servicecidr),
        )
        .route(
            "/servicecidrs/{name}/status",
            get(get_servicecidr)
                .put(update_servicecidr)
                .patch(patch_servicecidr),
        )
        .route(
            "/ipaddresses",
            get(list_ipaddresses)
                .post(create_ipaddress)
                .delete(delete_collection_ipaddresses),
        )
        .route(
            "/ipaddresses/{name}",
            get(get_ipaddress)
                .put(update_ipaddress)
                .patch(patch_ipaddress)
                .delete(delete_ipaddress),
        )
        // F1-01: NetworkPolicy CRUD + watch routes mirroring the Ingress
        // namespaced-resource pattern.
        .route("/networkpolicies", get(list_all_networkpolicies))
        .route(
            "/namespaces/{namespace}/networkpolicies",
            get(list_networkpolicies)
                .post(create_networkpolicy)
                .delete(delete_collection_networkpolicies),
        )
        .route(
            "/namespaces/{namespace}/networkpolicies/{name}",
            get(get_networkpolicy)
                .put(update_networkpolicy)
                .patch(patch_networkpolicy)
                .delete(delete_networkpolicy),
        )
}

async fn delete_collection_ingressclasses(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    k8s_native_service::generic_command::delete_non_pod_collection(
        state.resource_mutation().resource_query.as_ref(),
        state.resource_mutation().resource_command.as_ref(),
        "networking.k8s.io/v1",
        "IngressClass",
        None,
        query.label_selector.as_deref(),
    )
    .await?;
    Ok(Json(
        k8s_native_service::generic_command::delete_collection_success_status(),
    ))
}
