//! Read-only namespace-content inventory.

use klights_cluster_core::Resource;

use crate::{ResourceReadError, ResourceReadFuture};

pub type NamespaceContentFuture<'a, T> = ResourceReadFuture<'a, T>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceRequest {
    namespace: String,
}

impl NamespaceRequest {
    pub fn try_new(namespace: impl Into<String>) -> Result<Self, ResourceReadError> {
        let namespace = namespace.into();
        crate::read_validation::validate_namespace(&namespace)
            .map_err(crate::read_validation::map_invalid_request)?;
        Ok(Self { namespace })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceKindRequest {
    namespace: String,
    kind: String,
}

impl NamespaceKindRequest {
    pub fn try_new(
        namespace: impl Into<String>,
        kind: impl Into<String>,
    ) -> Result<Self, ResourceReadError> {
        let namespace = namespace.into();
        let kind = kind.into();
        crate::read_validation::validate_namespace(&namespace)
            .map_err(crate::read_validation::map_invalid_request)?;
        crate::read_validation::validate_resource_identity("v1", &kind)
            .map_err(crate::read_validation::map_invalid_request)?;
        Ok(Self { namespace, kind })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }
}

pub trait NamespaceContentRead: Send + Sync {
    fn list_namespace_resources(
        &self,
        request: NamespaceRequest,
    ) -> NamespaceContentFuture<'_, Vec<Resource>>;

    fn list_namespace_resources_of_kind(
        &self,
        request: NamespaceKindRequest,
    ) -> NamespaceContentFuture<'_, Vec<Resource>>;

    fn list_namespace_resources_excluding_kind(
        &self,
        request: NamespaceKindRequest,
    ) -> NamespaceContentFuture<'_, Vec<Resource>>;

    fn count_namespace_resources(
        &self,
        request: NamespaceRequest,
    ) -> NamespaceContentFuture<'_, i64>;
}
