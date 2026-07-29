//! Resource delete — hard-delete with precondition validation and watch
//! event emission carrying the deleted object body.

use super::super::ordinary;
use super::*;

use super::super::create_staged_post_commit;

impl Datastore {
    /// Apply a patch against the current state of a resource without a
    /// compare-and-swap resourceVersion check.
    ///
    /// Returns `Ok(None)` when the row is missing.
    pub async fn delete_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()> {
        self.delete_resource_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            ResourcePreconditions::default(),
        )
        .await
    }

    pub async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<()> {
        self.delete_resource_with_preconditions_observed_rv(
            api_version,
            kind,
            namespace,
            name,
            preconditions,
        )
        .await
        .map(|_| ())
    }

    pub async fn delete_resource_with_preconditions_observed_rv(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<i64> {
        // tokio-rusqlite::call closures must be `'static`.
        let av = api_version.to_string();
        let k = kind.to_string();
        let n = name.to_string();

        // Route to correct table based on resource scope.
        // Hard-delete: read the row's data first (so the watch_events DELETED event
        // carries the object body), DELETE the row, then INSERT the watch_event.
        let namespace_for_db = namespace.map(str::to_string);
        let preconditions_for_delete = preconditions.clone();
        let result = self
            .db_call("db_query", move |conn| {
                ordinary::delete_resource_in_conn(
                    conn,
                    ordinary::DeleteResourceInput {
                        api_version: av,
                        kind: k,
                        namespace: namespace_for_db,
                        name: n,
                        preconditions: preconditions_for_delete,
                    },
                )
            })
            .await;

        match result {
            Ok(ordinary::DeleteResourceAttempt::Deleted(rv, data_bytes)) => {
                if let Ok(data) = serde_json::from_slice::<Value>(&data_bytes) {
                    let _pending = create_staged_post_commit(
                        api_version,
                        kind,
                        namespace,
                        name,
                        rv,
                        "DELETED",
                        data,
                    );
                    #[cfg(test)]
                    self.publish_watch_event(_pending);
                }
                Ok(rv)
            }
            Ok(ordinary::DeleteResourceAttempt::NotFound) => Err(anyhow!("Resource not found")),
            Ok(ordinary::DeleteResourceAttempt::PreconditionFailed { message, live_uid }) => {
                if let Some(expected_uid) = preconditions.uid.as_deref()
                    && live_uid.as_deref() != Some(expected_uid)
                {
                    warn_uid_precondition_mismatch(
                        "delete_resource",
                        api_version,
                        kind,
                        namespace,
                        name,
                        expected_uid,
                        live_uid.as_deref(),
                    );
                }
                Err(klights_cluster_datastore::errors::DatastoreError::conflict(message).into())
            }
            Err(e) => Err(anyhow!("Failed to delete resource: {}", e)),
        }
    }
}
