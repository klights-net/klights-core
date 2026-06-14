use crate::api::*;

pub fn batch_v1_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/jobs", get(list_all_jobs))
        .route(
            "/namespaces/{namespace}/jobs",
            get(list_jobs)
                .post(create_job)
                .delete(delete_collection_jobs),
        )
        .route(
            "/namespaces/{namespace}/jobs/{name}",
            get(get_job)
                .put(update_job)
                .patch(patch_job)
                .delete(delete_job),
        )
        .route(
            "/namespaces/{namespace}/jobs/{name}/status",
            get(get_job).put(update_job_status).patch(patch_job_status),
        )
        .route("/cronjobs", get(list_all_cronjobs))
        .route(
            "/namespaces/{namespace}/cronjobs",
            get(list_cronjobs)
                .post(create_cronjob)
                .delete(delete_collection_cronjobs),
        )
        .route(
            "/namespaces/{namespace}/cronjobs/{name}",
            get(get_cronjob)
                .put(update_cronjob)
                .patch(patch_cronjob)
                .delete(delete_cronjob),
        )
        .route(
            "/namespaces/{namespace}/cronjobs/{name}/status",
            get(get_cronjob)
                .put(update_cronjob_status)
                .patch(patch_cronjob_status),
        )
}
