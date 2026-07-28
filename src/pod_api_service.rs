//! API-facing Pod service assembled exclusively from focused neutral ports.

use std::sync::Arc;

use klights_pod_api::{
    PodApiCreateRequest, PodApiCreateResult, PodApiDeleteCollectionRequest, PodApiDeleteOutcome,
    PodApiDeleteRequest, PodApiMutation, PodApiPatchRequest, PodApiUpdateRequest,
    PodApiWriteOutcome, PodBindingRequest, PodRepositoryFuture,
};
use klights_reconcile_api::{GcPodDeleteFuture, GcPodDeleteRequest, GcPodDeleteSink};

pub struct PodApiService {
    mutation: Arc<dyn PodApiMutation>,
    gc_delete: Arc<dyn GcPodDeleteSink>,
}

pub struct PodApiServiceDependencies {
    pub mutation: Arc<dyn PodApiMutation>,
    pub gc_delete: Arc<dyn GcPodDeleteSink>,
}

impl PodApiService {
    pub fn new(dependencies: PodApiServiceDependencies) -> Self {
        Self {
            mutation: dependencies.mutation,
            gc_delete: dependencies.gc_delete,
        }
    }
}

impl PodApiMutation for PodApiService {
    fn create_pod(
        &self,
        request: PodApiCreateRequest,
    ) -> PodRepositoryFuture<'_, PodApiCreateResult> {
        self.mutation.create_pod(request)
    }

    fn update_pod(
        &self,
        request: PodApiUpdateRequest,
    ) -> PodRepositoryFuture<'_, PodApiWriteOutcome> {
        self.mutation.update_pod(request)
    }

    fn patch_pod(
        &self,
        request: PodApiPatchRequest,
    ) -> PodRepositoryFuture<'_, PodApiWriteOutcome> {
        self.mutation.patch_pod(request)
    }

    fn delete_pod(
        &self,
        request: PodApiDeleteRequest,
    ) -> PodRepositoryFuture<'_, PodApiDeleteOutcome> {
        self.mutation.delete_pod(request)
    }

    fn delete_collection_pods(
        &self,
        request: PodApiDeleteCollectionRequest,
    ) -> PodRepositoryFuture<'_, ()> {
        self.mutation.delete_collection_pods(request)
    }

    fn bind_pod(&self, request: PodBindingRequest) -> PodRepositoryFuture<'_, ()> {
        self.mutation.bind_pod(request)
    }
}

impl GcPodDeleteSink for PodApiService {
    fn request_gc_pod_delete(&self, request: GcPodDeleteRequest) -> GcPodDeleteFuture<'_> {
        self.gc_delete.request_gc_pod_delete(request)
    }
}
