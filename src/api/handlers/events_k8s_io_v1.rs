use crate::api::*;

pub fn events_k8s_io_v1_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/events", get(list_all_events_k8s_io))
        .route(
            "/namespaces/{namespace}/events",
            get(list_events_k8s_io)
                .post(create_event_k8s_io)
                .delete(delete_collection_events_k8s_io),
        )
        .route(
            "/namespaces/{namespace}/events/{name}",
            get(get_event_k8s_io)
                .put(update_event_k8s_io)
                .patch(patch_event_k8s_io)
                .delete(delete_event_k8s_io),
        )
}
