/// Root-composed implementation types stored by the API state contexts.
///
/// The API owner declares capability-family slots; the composition root binds
/// them to concrete implementations without exposing lower-owner module paths
/// from the context definition.
pub(crate) trait ApiStateCompositionTypes {
    type ResourceStore;
    type PodRepository;
    type PodLifecycleRouter;
    type FailureMetrics;
    type NodeLeaseObservations;
}

pub(crate) enum ApiStateComposition {}

pub(crate) type ApiResourceStore = <ApiStateComposition as ApiStateCompositionTypes>::ResourceStore;
pub(crate) type ApiPodRepository = <ApiStateComposition as ApiStateCompositionTypes>::PodRepository;
pub(crate) type ApiPodLifecycleRouter =
    <ApiStateComposition as ApiStateCompositionTypes>::PodLifecycleRouter;
pub(crate) type ApiFailureMetrics =
    <ApiStateComposition as ApiStateCompositionTypes>::FailureMetrics;
pub(crate) type ApiNodeLeaseObservations =
    <ApiStateComposition as ApiStateCompositionTypes>::NodeLeaseObservations;
