//! PodRepository facade types -- build parts and the isolated service
//! traits extracted from the monolithic repository.

use super::background::PodRepositoryBackground;

/// Returned by `PodRepository::build_parts`. Separates the repository from
/// services that require explicit startup so construction is side-effect-free.
pub struct PodRepositoryParts {
    pub repository: super::PodRepository,
    pub background: PodRepositoryBackground,
}
