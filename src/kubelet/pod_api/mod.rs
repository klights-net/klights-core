//! `PodApiFacade` — API-facing Pod facade above repository persistence and
//! below HTTP handlers. Wraps the existing `PodApiService` during migration
//! and will hold separate create / update / delete service fields once
//! Tasks 6.2–6.4 split them.

use std::sync::Arc;

use crate::kubelet::pod_repository::PodRepository;
use crate::kubelet::pod_repository::api::{PodApiService, PodApiServiceDependencies};

mod create;
mod delete;
mod scheduling;
mod update;
pub use create::PodApiCreateService;
pub use delete::PodApiDeleteService;
pub use scheduling::PodSchedulingService;
pub use update::PodApiUpdateService;

/// API-facing Pod facade. During migration this wraps the existing
/// `PodApiService`; after Tasks 6.2–6.4 the individual create / update /
/// delete service fields replace it.
pub struct PodApiFacade {
    pub repository: Arc<PodRepository>,
    _api: Arc<PodApiService>,
    pub create_service: PodApiCreateService,
    pub update_service: PodApiUpdateService,
    pub delete_service: PodApiDeleteService,
    pub scheduling_service: PodSchedulingService,
}

impl PodApiFacade {
    pub fn new(
        repository: Arc<PodRepository>,
        api_dependencies: PodApiServiceDependencies,
    ) -> Self {
        let api = Arc::new(PodApiService::new(api_dependencies));
        Self {
            repository,
            create_service: PodApiCreateService::new(api.clone()),
            update_service: PodApiUpdateService::new(api.clone()),
            delete_service: PodApiDeleteService::new(api.clone()),
            scheduling_service: PodSchedulingService::new(api.clone()),
            _api: api,
        }
    }
}

#[cfg(test)]
mod tests;
