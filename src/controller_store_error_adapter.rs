use klights_reconcile_api::ControllerStoreError;

pub(crate) fn map_controller_store_error(error: anyhow::Error) -> ControllerStoreError {
    if let Some(pod_error) = error.downcast_ref::<klights_pod_api::PodRepositoryError>() {
        use klights_pod_api::PodRepositoryError;

        let message = pod_error.to_string();
        return match pod_error {
            PodRepositoryError::NotFound { .. } => ControllerStoreError::not_found(message),
            PodRepositoryError::AlreadyExists { .. } => {
                ControllerStoreError::already_exists(message)
            }
            PodRepositoryError::UidMismatch { .. } | PodRepositoryError::Conflict { .. } => {
                ControllerStoreError::conflict(message)
            }
            PodRepositoryError::Unavailable { .. }
            | PodRepositoryError::Timeout
            | PodRepositoryError::Cancelled => ControllerStoreError::unavailable(message),
            PodRepositoryError::InvalidRequest { .. }
            | PodRepositoryError::Forbidden { .. }
            | PodRepositoryError::Unprocessable { .. }
            | PodRepositoryError::Internal { .. }
            | PodRepositoryError::CorruptResponse { .. } => ControllerStoreError::internal(message),
        };
    }

    if let Some(storage_error) = error.downcast_ref::<klights_cluster_core::StorageMutationError>()
    {
        use klights_cluster_core::StorageCommandRejectionCode;

        let message = storage_error.message().to_string();
        return match storage_error.rejection_code() {
            Some(StorageCommandRejectionCode::AlreadyExists) => {
                ControllerStoreError::already_exists(message)
            }
            Some(StorageCommandRejectionCode::NotFound) => ControllerStoreError::not_found(message),
            Some(StorageCommandRejectionCode::Conflict) => ControllerStoreError::conflict(message),
            Some(StorageCommandRejectionCode::InvalidCommit) => {
                ControllerStoreError::internal(message)
            }
            None => ControllerStoreError::unavailable(message),
        };
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use klights_cluster_core::{StorageCommandRejectionCode, StorageMutationError};

    #[test]
    fn typed_storage_rejections_preserve_controller_error_semantics() {
        let cases = [
            (
                StorageCommandRejectionCode::AlreadyExists,
                ControllerStoreError::already_exists("rejected"),
            ),
            (
                StorageCommandRejectionCode::NotFound,
                ControllerStoreError::not_found("rejected"),
            ),
            (
                StorageCommandRejectionCode::Conflict,
                ControllerStoreError::conflict("rejected"),
            ),
            (
                StorageCommandRejectionCode::InvalidCommit,
                ControllerStoreError::internal("rejected"),
            ),
        ];

        for (code, expected) in cases {
            let actual =
                map_controller_store_error(StorageMutationError::rejected(code, "rejected").into());
            assert_eq!(actual, expected, "wrong mapping for {code:?}");
        }
    }

    #[test]
    fn typed_storage_persistence_failure_is_unavailable() {
        let actual = map_controller_store_error(
            StorageMutationError::persistence("database unavailable").into(),
        );
        assert_eq!(
            actual,
            ControllerStoreError::unavailable("database unavailable")
        );
    }

    #[test]
    fn typed_pod_already_exists_preserves_controller_error_semantics() {
        let actual = map_controller_store_error(anyhow::Error::new(
            klights_pod_api::PodRepositoryError::already_exists("Pod default/taken exists"),
        ));
        assert_eq!(
            actual,
            ControllerStoreError::already_exists("Pod already exists: Pod default/taken exists")
        );
    }
}
