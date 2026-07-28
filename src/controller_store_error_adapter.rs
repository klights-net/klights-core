use klights_reconcile_api::ControllerStoreError;

pub(crate) fn map_controller_store_error(error: anyhow::Error) -> ControllerStoreError {
    if let Some(datastore_error) =
        error.downcast_ref::<klights_cluster_datastore::errors::DatastoreError>()
    {
        return match datastore_error {
            klights_cluster_datastore::errors::DatastoreError::AlreadyExists { message } => {
                ControllerStoreError::already_exists(message.clone())
            }
            klights_cluster_datastore::errors::DatastoreError::Conflict { message } => {
                ControllerStoreError::conflict(message.clone())
            }
            klights_cluster_datastore::errors::DatastoreError::NotFound { message } => {
                ControllerStoreError::not_found(message.clone())
            }
        };
    }

    ControllerStoreError::unavailable(format!("{error:#}"))
}
