pub use crate::generic_command::{CreateUpdateQuery, DeleteCollectionQuery};
pub use crate::generic_read::{
    ListQuery, ListResourceVersionPort, ListSnapshotResolution, ListSnapshotResult,
    NamespaceListPort, NamespaceListRequest, ResolvedListPage, encode_response_continue_token_at,
    process_continue_token_at, resolve_list_page,
};

impl ListSnapshotResult<klights_leader_api::ResourceListResult>
    for crate::current::custom_resource_ports::CustomResourceListSnapshot
{
    fn into_list_snapshot_resolution(
        self,
    ) -> ListSnapshotResolution<klights_leader_api::ResourceListResult> {
        match self {
            Self::List(list) => ListSnapshotResolution::List(list),
            Self::Current => ListSnapshotResolution::Current,
            Self::Expired => ListSnapshotResolution::Expired,
        }
    }
}

#[cfg(test)]
#[path = "query_tests.rs"]
mod resolve_list_page_tests;
