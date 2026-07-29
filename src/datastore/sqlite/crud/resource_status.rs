//! Root compatibility facade for 10C.2 status-only mutation.

use anyhow::{Context, Result, anyhow};
use serde_json::Value;

use super::*;
use crate::datastore::sqlite::create_staged_post_commit;

impl Datastore {
    pub async fn update_status_only(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        self.update_status_only_with_preconditions(
            api_version,
            kind,
            namespace,
            name,
            status,
            ResourcePreconditions {
                uid: None,
                resource_version: expected_rv,
            },
        )
        .await
    }

    pub async fn update_status_only_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        let expected_uid_for_log = preconditions.uid.clone();
        let av = api_version.to_string();
        let k = kind.to_string();
        let ns = use_namespaced_table(api_version, kind, &namespace)
            .then(|| namespace.unwrap_or("default").to_string());
        let n = name.to_string();
        let result = self
            .db_call("db_query", move |conn| {
                super::super::live_apply::status::update_status_in_conn(
                    conn,
                    &av,
                    &k,
                    ns.as_deref(),
                    &n,
                    status,
                    preconditions,
                )
            })
            .await;

        match result {
            Ok(outcome) => {
                let data: Value = serde_json::from_slice(&outcome.data)
                    .context("deserialize merged status payload")?;
                if outcome.changed {
                    let _pending = create_staged_post_commit(
                        api_version,
                        kind,
                        namespace,
                        name,
                        outcome.resource_version,
                        "MODIFIED",
                        data.clone(),
                    );
                    #[cfg(test)]
                    self.publish_watch_event(_pending);
                }
                Ok(Resource {
                    id: outcome.id,
                    api_version: api_version.to_string(),
                    kind: kind.to_string(),
                    namespace: namespace.map(str::to_string),
                    name: name.to_string(),
                    uid: Resource::uid_from_data(&data),
                    resource_version: outcome.resource_version,
                    data: std::sync::Arc::new(data),
                })
            }
            Err(tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows)) => {
                if let Some(expected_uid) = expected_uid_for_log.as_deref() {
                    self.warn_uid_precondition_mismatch_if_live(
                        "update_status_only",
                        api_version,
                        kind,
                        namespace,
                        name,
                        expected_uid,
                    )
                    .await;
                }
                Err(klights_cluster_datastore::errors::DatastoreError::conflict(
                    "Resource not found or version conflict",
                )
                .into())
            }
            Err(error) => Err(anyhow!("Failed to update status: {error}")),
        }
    }
}
