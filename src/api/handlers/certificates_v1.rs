use crate::api::*;

pub fn certificates_v1_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/certificatesigningrequests",
            get(list_certificatesigningrequests)
                .post(create_certificatesigningrequest)
                .delete(delete_collection_certificatesigningrequests),
        )
        .route(
            "/certificatesigningrequests/{name}",
            get(get_certificatesigningrequest)
                .put(update_certificatesigningrequest)
                .patch(patch_certificatesigningrequest)
                .delete(delete_certificatesigningrequest),
        )
        .route(
            "/certificatesigningrequests/{name}/status",
            get(get_csr_status)
                .put(update_csr_status)
                .patch(patch_csr_status),
        )
        .route(
            "/certificatesigningrequests/{name}/approval",
            get(get_csr_approval)
                .put(update_csr_approval)
                .patch(patch_csr_approval),
        )
}
