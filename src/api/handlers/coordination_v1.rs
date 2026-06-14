use crate::api::*;

pub fn coordination_v1_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/leases", get(list_all_leases_coordination))
        .route(
            "/namespaces/{namespace}/leases",
            get(list_leases_coordination)
                .post(create_lease_coordination)
                .delete(delete_collection_leases_coordination),
        )
        .route(
            "/namespaces/{namespace}/leases/{name}",
            get(get_lease_coordination)
                .put(update_lease_coordination)
                .patch(patch_lease_coordination)
                .delete(delete_lease_coordination),
        )
}
