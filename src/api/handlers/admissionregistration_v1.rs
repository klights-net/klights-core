use crate::api::*;
use axum::{
    Json, Router,
    extract::{Query, State},
};
use serde_json::Value;
use std::sync::Arc;

pub fn admissionregistration_v1_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/mutatingwebhookconfigurations",
            get(list_mutatingwebhookconfigurations)
                .post(create_mutatingwebhookconfiguration)
                .delete(delete_collection_mutatingwebhookconfigurations),
        )
        .route(
            "/mutatingwebhookconfigurations/{name}",
            get(get_mutatingwebhookconfiguration)
                .put(update_mutatingwebhookconfiguration)
                .patch(patch_mutatingwebhookconfiguration)
                .delete(delete_mutatingwebhookconfiguration),
        )
        .route(
            "/mutatingwebhookconfigurations/{name}/status",
            get(get_mutatingwebhookconfiguration_status)
                .put(update_mutatingwebhookconfiguration_status)
                .patch(patch_mutatingwebhookconfiguration_status),
        )
        .route(
            "/validatingwebhookconfigurations",
            get(list_validatingwebhookconfigurations)
                .post(create_validatingwebhookconfiguration)
                .delete(delete_collection_validatingwebhookconfigurations),
        )
        .route(
            "/validatingwebhookconfigurations/{name}",
            get(get_validatingwebhookconfiguration)
                .put(update_validatingwebhookconfiguration)
                .patch(patch_validatingwebhookconfiguration)
                .delete(delete_validatingwebhookconfiguration),
        )
        .route(
            "/validatingwebhookconfigurations/{name}/status",
            get(get_validatingwebhookconfiguration_status)
                .put(update_validatingwebhookconfiguration_status)
                .patch(patch_validatingwebhookconfiguration_status),
        )
        .route(
            "/validatingadmissionpolicies",
            get(list_validatingadmissionpolicies)
                .post(create_validatingadmissionpolicy)
                .delete(delete_collection_validatingadmissionpolicies),
        )
        .route(
            "/validatingadmissionpolicies/{name}",
            get(get_validatingadmissionpolicy)
                .put(update_validatingadmissionpolicy)
                .patch(patch_validatingadmissionpolicy)
                .delete(delete_validatingadmissionpolicy),
        )
        .route(
            "/validatingadmissionpolicies/{name}/status",
            get(get_validatingadmissionpolicy_status)
                .put(update_validatingadmissionpolicy_status)
                .patch(patch_validatingadmissionpolicy_status),
        )
        .route(
            "/validatingadmissionpolicybindings",
            get(list_validatingadmissionpolicybindings)
                .post(create_validatingadmissionpolicybinding)
                .delete(delete_collection_validatingadmissionpolicybindings),
        )
        .route(
            "/validatingadmissionpolicybindings/{name}",
            get(get_validatingadmissionpolicybinding)
                .put(update_validatingadmissionpolicybinding)
                .patch(patch_validatingadmissionpolicybinding)
                .delete(delete_validatingadmissionpolicybinding),
        )
        .route(
            "/validatingadmissionpolicybindings/{name}/status",
            get(get_validatingadmissionpolicybinding_status)
                .put(update_validatingadmissionpolicybinding_status)
                .patch(patch_validatingadmissionpolicybinding_status),
        )
}

async fn delete_collection_mutatingwebhookconfigurations(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    let list = state
        .db
        .list_resources(
            "admissionregistration.k8s.io/v1",
            "MutatingWebhookConfiguration",
            None,
            crate::datastore::ResourceListQuery::new(
                query.label_selector.as_deref(),
                None,
                None,
                None,
            ),
        )
        .await?;
    for resource in list.items {
        let _ = state
            .db
            .delete_resource(
                "admissionregistration.k8s.io/v1",
                "MutatingWebhookConfiguration",
                None,
                &resource.name.clone(),
            )
            .await;
    }
    Ok(Json(
        serde_json::json!({"apiVersion":"v1","kind":"Status","status":"Success","code":200}),
    ))
}

async fn delete_collection_validatingwebhookconfigurations(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    let list = state
        .db
        .list_resources(
            "admissionregistration.k8s.io/v1",
            "ValidatingWebhookConfiguration",
            None,
            crate::datastore::ResourceListQuery::new(
                query.label_selector.as_deref(),
                None,
                None,
                None,
            ),
        )
        .await?;
    for resource in list.items {
        let _ = state
            .db
            .delete_resource(
                "admissionregistration.k8s.io/v1",
                "ValidatingWebhookConfiguration",
                None,
                &resource.name.clone(),
            )
            .await;
    }
    Ok(Json(
        serde_json::json!({"apiVersion":"v1","kind":"Status","status":"Success","code":200}),
    ))
}

async fn delete_collection_validatingadmissionpolicies(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    let list = state
        .db
        .list_resources(
            "admissionregistration.k8s.io/v1",
            "ValidatingAdmissionPolicy",
            None,
            crate::datastore::ResourceListQuery::new(
                query.label_selector.as_deref(),
                None,
                None,
                None,
            ),
        )
        .await?;
    for resource in list.items {
        let _ = state
            .db
            .delete_resource(
                "admissionregistration.k8s.io/v1",
                "ValidatingAdmissionPolicy",
                None,
                &resource.name.clone(),
            )
            .await;
    }
    Ok(Json(
        serde_json::json!({"apiVersion":"v1","kind":"Status","status":"Success","code":200}),
    ))
}

async fn delete_collection_validatingadmissionpolicybindings(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    let list = state
        .db
        .list_resources(
            "admissionregistration.k8s.io/v1",
            "ValidatingAdmissionPolicyBinding",
            None,
            crate::datastore::ResourceListQuery::new(
                query.label_selector.as_deref(),
                None,
                None,
                None,
            ),
        )
        .await?;
    for resource in list.items {
        let _ = state
            .db
            .delete_resource(
                "admissionregistration.k8s.io/v1",
                "ValidatingAdmissionPolicyBinding",
                None,
                &resource.name.clone(),
            )
            .await;
    }
    Ok(Json(
        serde_json::json!({"apiVersion":"v1","kind":"Status","status":"Success","code":200}),
    ))
}
