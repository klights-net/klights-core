//! Generic Kubernetes create/update/patch/delete orchestration.

mod delete;
mod event;
mod finalizer;
mod generated;
mod options;
mod ports;
mod response;
mod store;
mod write;

pub use delete::{
    DeleteResult, DeleteStrategy, FinalizerAwareDeleteStrategy, delete_collection_items,
    delete_loaded_with_strategy, delete_with_strategy, ensure_delete_preconditions_match,
};
pub use event::{MutationEvent, dispatch_mutation_event};
pub use finalizer::{
    DeleteCompletion, NonForegroundDeleteRequest, ResourceDeleteTarget,
    complete_non_foreground_delete_with_live_recheck, ensure_deletion_timestamp_at,
    mark_foreground_deletion_with_retry, preserve_deletion_timestamp_on_update,
    ready_to_finalize_after_update, set_deletion_timestamp_at,
};
pub use generated::{
    GeneratedDeleteInnerRequest, GeneratedNamedResource, GeneratedPatchInnerRequest,
    GeneratedUpdateInnerRequest, create_inner, delete_collection_inner,
    delete_collection_shared_inner, delete_inner, finalize_after_update_if_ready, patch_inner,
    update_inner,
};
pub use options::{
    CreateUpdateQuery, DeleteCollectionQuery, DeleteIntent, DeleteOptions, DeletePreconditions,
    DryRunMode, PropagationPolicy, parse_delete_options_body, parse_delete_options_protobuf,
};
pub use ports::{
    BuiltinAdmissionDefaultsPort, GeneratedLifecyclePort, GeneratedResourceMutationPort,
    GenericCommandAdmission, GenericCommandAuthorization, GenericCommandFuture,
    GenericCommandLifecycle, GenericCommandPolicy, GenericCommandReconcile, GenericCommandRuntime,
    GenericCommandState, GenericCommandStore, PreparedCreate, ResourceAdmissionPort,
    ResourceAdmissionRequest,
};
pub use response::{
    accepted_delete_status, accepted_object, delete_collection_success_status,
    delete_success_status, persisted_object,
};
pub use store::{
    create_namespace, create_non_pod_resource, delete_namespace, delete_non_pod_collection,
    delete_non_pod_resource, patch_non_pod_resource, update_namespace, update_non_pod_resource,
    update_resource_status,
};
pub use write::{
    CreateStrategy, PatchStrategy, UpdateStrategy, WriteResult, create_with_strategy,
    patch_with_strategy, prepare_builtin_generation_for_update, prepare_create_metadata,
    prepare_custom_generation_for_update, update_with_strategy,
};
