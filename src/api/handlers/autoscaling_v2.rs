use crate::api::*;

pub fn autoscaling_v2_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/horizontalpodautoscalers", get(list_all_hpas_v2))
        .route(
            "/namespaces/{namespace}/horizontalpodautoscalers",
            get(list_hpas_v2)
                .post(create_hpa_v2)
                .delete(delete_collection_hpas_v2),
        )
        .route(
            "/namespaces/{namespace}/horizontalpodautoscalers/{name}",
            get(get_hpa_v2)
                .put(update_hpa_v2)
                .patch(patch_hpa_v2)
                .delete(delete_hpa_v2),
        )
        .route(
            "/namespaces/{namespace}/horizontalpodautoscalers/{name}/status",
            get(get_hpa_v2)
                .put(update_hpa_v2_status)
                .patch(patch_hpa_v2_status),
        )
}
