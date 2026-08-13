use std::sync::Arc;

use klights_leader_api::LeaderWatchError;

use crate::datastore::selector::PassiveReadPorts;
use crate::datastore::{DatastoreBackend, DatastoreHandle};

pub(crate) fn datastore_positioned_watch_service(
    passive_reads: &PassiveReadPorts,
    db: DatastoreHandle,
    watch_signals: Arc<dyn klights_watch::WatchSignalSubscribe>,
) -> klights_watch::PositionedWatchService {
    klights_watch::PositionedWatchService::new(
        passive_reads.resource_reads(),
        passive_reads.history_reads(),
        passive_reads.allocator_reads(),
        watch_signals,
        Arc::new(DatastoreWatchScopes { db }),
    )
}

#[cfg(test)]
pub(crate) fn for_test(
    passive_reads: &PassiveReadPorts,
    db: DatastoreHandle,
) -> klights_watch::PositionedWatchService {
    datastore_positioned_watch_service(
        passive_reads,
        db.clone(),
        crate::bootstrap::watch_commit_wiring::test_signal_source(&db),
    )
}

struct DatastoreWatchScopes {
    db: DatastoreHandle,
}

pub(crate) async fn datastore_watch_resource_scope(
    db: &dyn DatastoreBackend,
    api_version: &str,
    kind: &str,
) -> Result<klights_watch::WatchResourceScope, LeaderWatchError> {
    if klights_cluster_datastore::sqlite::scope::is_builtin_api_version(api_version) {
        return Ok(
            if klights_cluster_datastore::sqlite::scope::is_namespaced(kind) {
                klights_watch::WatchResourceScope::Namespaced
            } else {
                klights_watch::WatchResourceScope::Cluster
            },
        );
    }

    let Some((group, version)) = api_version.rsplit_once('/') else {
        return Err(LeaderWatchError::invalid_request(
            "apiVersion",
            format!("custom resource apiVersion {api_version:?} must contain group/version"),
        ));
    };
    let crds = db
        .list_resources(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .map_err(|error| {
            LeaderWatchError::unavailable(format!(
                "failed to resolve custom resource watch scope: {error:#}"
            ))
        })?;
    for crd in crds.items {
        let spec = crd.data.get("spec").unwrap_or(&serde_json::Value::Null);
        if spec.get("group").and_then(serde_json::Value::as_str) != Some(group)
            || spec
                .pointer("/names/kind")
                .and_then(serde_json::Value::as_str)
                != Some(kind)
        {
            continue;
        }
        let serves_version = spec
            .get("versions")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|versions| {
                versions.iter().any(|candidate| {
                    candidate.get("name").and_then(serde_json::Value::as_str) == Some(version)
                        && candidate
                            .get("served")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(true)
                })
            });
        if !serves_version {
            continue;
        }
        return match spec.get("scope").and_then(serde_json::Value::as_str) {
            Some("Namespaced") => Ok(klights_watch::WatchResourceScope::Namespaced),
            Some("Cluster") => Ok(klights_watch::WatchResourceScope::Cluster),
            scope => Err(LeaderWatchError::invalid_request(
                "kind",
                format!("CRD for {api_version} {kind} has invalid scope {scope:?}"),
            )),
        };
    }
    Err(LeaderWatchError::invalid_request(
        "kind",
        format!("no served CRD defines {api_version} {kind}"),
    ))
}

impl klights_watch::WatchScopeResolver for DatastoreWatchScopes {
    fn resource_scope<'a>(
        &'a self,
        api_version: &'a str,
        kind: &'a str,
    ) -> futures::future::BoxFuture<'a, Result<klights_watch::WatchResourceScope, LeaderWatchError>>
    {
        Box::pin(datastore_watch_resource_scope(
            self.db.as_ref(),
            api_version,
            kind,
        ))
    }
}
