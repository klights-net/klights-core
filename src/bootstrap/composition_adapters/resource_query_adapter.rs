//! Composition-owned passive resource query.
//!
//! The selected cluster store is adapted to the transport-neutral query port
//! at the bootstrap boundary.  Local leader effects may consume that port,
//! but the local client does not own the store adapter or its read surface.

use std::sync::Arc;

use klights_leader_api::{
    LeaderResourceQuery, ResourceGetRequest, ResourceListRequest, ResourceListResult,
    ResourceQueryConsistency, ResourceQueryError, ResourceQueryFuture,
};

use crate::bootstrap::authority::AuthorityHandle;
use crate::datastore::{DatastoreHandle, Resource};

pub(crate) struct DatastoreResourceQueryAdapter {
    db: DatastoreHandle,
    authority: AuthorityHandle,
}

impl DatastoreResourceQueryAdapter {
    pub(crate) fn new<A: Into<AuthorityHandle>>(db: DatastoreHandle, authority: A) -> Arc<Self> {
        Arc::new(Self {
            db,
            authority: authority.into(),
        })
    }

    fn sample_leader_fresh(
        &self,
        consistency: ResourceQueryConsistency,
    ) -> Result<Option<klights_leader_api::AuthorityPermit>, ResourceQueryError> {
        if consistency != ResourceQueryConsistency::LeaderFresh {
            return Ok(None);
        }
        self.authority.local_permit().map(Some).map_err(|_| {
            ResourceQueryError::retryable(
                "leader-fresh resource query reached a non-authoritative local store",
            )
        })
    }

    fn query_error(error: impl std::fmt::Display) -> ResourceQueryError {
        ResourceQueryError::query_failed(error.to_string())
    }
}

impl LeaderResourceQuery for DatastoreResourceQueryAdapter {
    fn get_resource(
        &self,
        request: ResourceGetRequest,
    ) -> ResourceQueryFuture<'_, Option<Resource>> {
        Box::pin(async move {
            let leadership = self.sample_leader_fresh(request.consistency())?;
            let key = request.key();
            let resource = self
                .db
                .get_resource(
                    &key.api_version,
                    &key.kind,
                    key.namespace.as_deref(),
                    &key.name,
                )
                .await
                .map_err(Self::query_error)?;
            if leadership
                .as_ref()
                .is_some_and(|permit| self.authority.validate(permit).is_err())
            {
                return Err(ResourceQueryError::retryable(
                    "leader authority changed during local leader-fresh resource query",
                ));
            }
            Ok(resource)
        })
    }

    fn list_resources(
        &self,
        request: ResourceListRequest,
    ) -> ResourceQueryFuture<'_, ResourceListResult> {
        Box::pin(async move {
            let leadership = self.sample_leader_fresh(request.consistency())?;
            let list = self
                .db
                .list_resources(
                    request.api_version(),
                    request.kind(),
                    request.namespace(),
                    crate::datastore::ResourceListQuery::new(
                        request.label_selector(),
                        request.field_selector(),
                        request.limit(),
                        request.continue_token(),
                    ),
                )
                .await
                .map_err(Self::query_error)?;
            if leadership
                .as_ref()
                .is_some_and(|permit| self.authority.validate(permit).is_err())
            {
                return Err(ResourceQueryError::retryable(
                    "leader authority changed during local leader-fresh resource query",
                ));
            }
            ResourceListResult::try_new(
                list.items,
                list.resource_version,
                list.watch_replay_position,
                list.continue_token,
                list.remaining_item_count,
            )
        })
    }
}
