use crate::api::*;

pub fn discovery_v1_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/endpointslices", get(list_all_endpointslices))
        .route(
            "/namespaces/{namespace}/endpointslices",
            get(list_endpointslices)
                .post(create_endpointslice)
                .delete(delete_collection_endpointslices),
        )
        .route(
            "/namespaces/{namespace}/endpointslices/{name}",
            get(get_endpointslice)
                .put(update_endpointslice)
                .patch(patch_endpointslice)
                .delete(delete_endpointslice),
        )
}
