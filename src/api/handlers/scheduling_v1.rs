use crate::api::*;

pub fn scheduling_v1_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/priorityclasses",
            get(list_priorityclasses)
                .post(create_priorityclass)
                .delete(delete_collection_priorityclasses),
        )
        .route(
            "/priorityclasses/{name}",
            get(get_priorityclass)
                .put(update_priorityclass)
                .patch(patch_priorityclass)
                .delete(delete_priorityclass),
        )
}
