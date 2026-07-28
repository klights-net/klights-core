//! Indexed Kubernetes owner-reference queries.

use klights_cluster_core::Resource;

use crate::{ResourceReadError, ResourceReadFuture};

pub type OwnershipReadFuture<'a, T> = ResourceReadFuture<'a, T>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerUidRequest {
    owner_uid: String,
    namespace: Option<String>,
}

impl OwnerUidRequest {
    pub fn try_new(
        owner_uid: impl Into<String>,
        namespace: Option<String>,
    ) -> Result<Self, ResourceReadError> {
        let owner_uid = owner_uid.into();
        crate::read_validation::validate_nonempty(&owner_uid, "owner UID")
            .map_err(crate::read_validation::map_invalid_request)?;
        crate::read_validation::validate_optional_namespace(namespace.as_deref())
            .map_err(crate::read_validation::map_invalid_request)?;
        Ok(Self {
            owner_uid,
            namespace,
        })
    }

    pub fn owner_uid(&self) -> &str {
        &self.owner_uid
    }

    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedKindRequest {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    owner_uid: String,
}

impl OwnedKindRequest {
    pub fn try_new(
        api_version: impl Into<String>,
        kind: impl Into<String>,
        namespace: Option<String>,
        owner_uid: impl Into<String>,
    ) -> Result<Self, ResourceReadError> {
        let api_version = api_version.into();
        let kind = kind.into();
        let owner_uid = owner_uid.into();
        crate::read_validation::validate_resource_identity(&api_version, &kind)
            .map_err(crate::read_validation::map_invalid_request)?;
        crate::read_validation::validate_optional_namespace(namespace.as_deref())
            .map_err(crate::read_validation::map_invalid_request)?;
        crate::read_validation::validate_nonempty(&owner_uid, "owner UID")
            .map_err(crate::read_validation::map_invalid_request)?;
        Ok(Self {
            api_version,
            kind,
            namespace,
            owner_uid,
        })
    }

    pub fn api_version(&self) -> &str {
        &self.api_version
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub fn owner_uid(&self) -> &str {
        &self.owner_uid
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerNameKindRequest {
    owner_api_version: String,
    owner_name: String,
    owner_kind: String,
    namespace: Option<String>,
}

impl OwnerNameKindRequest {
    pub fn try_new(
        owner_api_version: impl Into<String>,
        owner_name: impl Into<String>,
        owner_kind: impl Into<String>,
        namespace: Option<String>,
    ) -> Result<Self, ResourceReadError> {
        let owner_api_version = owner_api_version.into();
        let owner_name = owner_name.into();
        let owner_kind = owner_kind.into();
        crate::read_validation::validate_resource_identity(&owner_api_version, &owner_kind)
            .map_err(crate::read_validation::map_invalid_request)?;
        crate::read_validation::validate_nonempty(&owner_name, "owner name")
            .map_err(crate::read_validation::map_invalid_request)?;
        crate::read_validation::validate_optional_namespace(namespace.as_deref())
            .map_err(crate::read_validation::map_invalid_request)?;
        Ok(Self {
            owner_api_version,
            owner_name,
            owner_kind,
            namespace,
        })
    }

    pub fn owner_api_version(&self) -> &str {
        &self.owner_api_version
    }

    pub fn owner_name(&self) -> &str {
        &self.owner_name
    }

    pub fn owner_kind(&self) -> &str {
        &self.owner_kind
    }

    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }
}

pub trait ClusterOwnershipRead: Send + Sync {
    fn find_owned_resources(
        &self,
        request: OwnerUidRequest,
    ) -> OwnershipReadFuture<'_, Vec<Resource>>;

    fn list_resources_by_owner_uid(
        &self,
        request: OwnedKindRequest,
    ) -> OwnershipReadFuture<'_, Vec<Resource>>;

    fn find_owned_by_name_kind_empty_uid(
        &self,
        request: OwnerNameKindRequest,
    ) -> OwnershipReadFuture<'_, Vec<Resource>>;
}
