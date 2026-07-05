use async_trait::async_trait;
use axum::http::StatusCode;
use serde_json::Value;

use crate::api::{AdmissionContextRequest, AppError};
use crate::datastore::DatastoreBackend;

pub fn prepare_create_metadata(ns: Option<&str>, body: &mut Value, resource_name: &str) {
    crate::api::inject_create_metadata(ns, body, resource_name);
}

pub fn prepare_builtin_generation_for_update(kind: &str, current: &Value, body: &mut Value) {
    crate::api::increment_generation_if_spec_changed(kind, current, body);
}

pub fn prepare_custom_generation_for_update(current: &Value, body: &mut Value) {
    crate::api::increment_generation_for_spec_change(current, body);
}

pub async fn run_admission(
    db: &dyn DatastoreBackend,
    request: AdmissionContextRequest<'_>,
) -> Result<Value, AppError> {
    crate::api::run_admission_for_request(db, crate::api::build_admission_context(request)).await
}

pub enum WriteResult {
    DryRun(Value),
    Persisted(crate::datastore::Resource),
    PersistedValue(Value),
    Response { status: StatusCode, body: Value },
}

impl WriteResult {
    pub fn into_response_parts(self, default_status: StatusCode) -> (StatusCode, Value) {
        match self {
            Self::DryRun(value) | Self::PersistedValue(value) => (default_status, value),
            Self::Persisted(resource) => (
                default_status,
                crate::api::mutation::response::persisted_object(
                    resource.data,
                    resource.resource_version,
                ),
            ),
            Self::Response { status, body } => (status, body),
        }
    }

    pub fn into_response_value(self) -> Value {
        self.into_response_parts(StatusCode::OK).1
    }

    pub fn persisted_resource(&self) -> Option<&crate::datastore::Resource> {
        match self {
            Self::Persisted(resource) => Some(resource),
            Self::DryRun(_) | Self::PersistedValue(_) | Self::Response { .. } => None,
        }
    }
}

#[async_trait]
pub trait CreateStrategy: Send + Sync {
    async fn before_admission(&self, body: Value) -> Result<Value, AppError>;

    async fn admit(
        &self,
        body: Value,
        dry_run: crate::api::mutation::DryRunMode,
    ) -> Result<Value, AppError>;

    async fn persist_create(
        &self,
        body: Value,
        dry_run: crate::api::mutation::DryRunMode,
    ) -> Result<WriteResult, AppError>;
}

#[async_trait]
pub trait UpdateStrategy: Send + Sync {
    async fn load_current(&self) -> Result<crate::datastore::Resource, AppError>;

    async fn prepare_update(
        &self,
        current: &crate::datastore::Resource,
        body: Value,
        dry_run: crate::api::mutation::DryRunMode,
    ) -> Result<Value, AppError>;

    async fn persist_update(
        &self,
        current: crate::datastore::Resource,
        body: Value,
        dry_run: crate::api::mutation::DryRunMode,
    ) -> Result<WriteResult, AppError>;
}

#[async_trait]
pub trait PatchStrategy: Send + Sync {
    async fn apply_patch(
        &self,
        patch: Value,
        dry_run: crate::api::mutation::DryRunMode,
    ) -> Result<WriteResult, AppError>;
}

pub async fn create_with_strategy<S>(
    strategy: &S,
    body: Value,
    dry_run: crate::api::mutation::DryRunMode,
) -> Result<WriteResult, AppError>
where
    S: CreateStrategy,
{
    let body = strategy.before_admission(body).await?;
    let body = strategy.admit(body, dry_run).await?;
    strategy.persist_create(body, dry_run).await
}

pub async fn update_with_strategy<S>(
    strategy: &S,
    body: Value,
    dry_run: crate::api::mutation::DryRunMode,
) -> Result<WriteResult, AppError>
where
    S: UpdateStrategy,
{
    let current = strategy.load_current().await?;
    let body = strategy.prepare_update(&current, body, dry_run).await?;
    strategy.persist_update(current, body, dry_run).await
}

pub async fn patch_with_strategy<S>(
    strategy: &S,
    patch: Value,
    dry_run: crate::api::mutation::DryRunMode,
) -> Result<WriteResult, AppError>
where
    S: PatchStrategy,
{
    strategy.apply_patch(patch, dry_run).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    #[test]
    fn prepare_create_metadata_stamps_identity_and_generation() {
        let mut body = serde_json::json!({"metadata": {}});
        prepare_create_metadata(Some("default"), &mut body, "cm1");

        assert_eq!(body["metadata"]["namespace"], "default");
        assert_eq!(body["metadata"]["name"], "cm1");
        assert_eq!(body["metadata"]["generation"], 1);
        assert!(
            body["metadata"]["uid"]
                .as_str()
                .is_some_and(|uid| !uid.is_empty())
        );
    }

    #[test]
    fn prepare_builtin_generation_for_update_uses_kind_policy() {
        let current = serde_json::json!({
            "metadata": {"generation": 3},
            "spec": {"replicas": 1}
        });
        let mut body = serde_json::json!({
            "metadata": {"generation": 3},
            "spec": {"replicas": 2}
        });

        prepare_builtin_generation_for_update("Deployment", &current, &mut body);

        assert_eq!(body["metadata"]["generation"], 4);
    }

    #[test]
    fn prepare_custom_generation_for_update_bumps_on_spec_change() {
        let current = serde_json::json!({
            "metadata": {"generation": 8},
            "spec": {"value": "old"}
        });
        let mut body = serde_json::json!({
            "metadata": {"generation": 8},
            "spec": {"value": "new"}
        });

        prepare_custom_generation_for_update(&current, &mut body);

        assert_eq!(body["metadata"]["generation"], 9);
    }

    #[derive(Clone)]
    struct RecordingCreateStrategy {
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl CreateStrategy for RecordingCreateStrategy {
        async fn before_admission(&self, body: Value) -> Result<Value, AppError> {
            self.calls.lock().unwrap().push("before_admission");
            Ok(body)
        }

        async fn admit(
            &self,
            body: Value,
            _dry_run: crate::api::mutation::DryRunMode,
        ) -> Result<Value, AppError> {
            self.calls.lock().unwrap().push("admit");
            Ok(body)
        }

        async fn persist_create(
            &self,
            body: Value,
            dry_run: crate::api::mutation::DryRunMode,
        ) -> Result<WriteResult, AppError> {
            self.calls.lock().unwrap().push("persist_create");
            if dry_run.is_all() {
                Ok(WriteResult::DryRun(body))
            } else {
                Ok(WriteResult::PersistedValue(body))
            }
        }
    }

    #[derive(Clone)]
    struct RecordingUpdateStrategy {
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl UpdateStrategy for RecordingUpdateStrategy {
        async fn load_current(&self) -> Result<crate::datastore::Resource, AppError> {
            self.calls.lock().unwrap().push("load_current");
            Ok(crate::datastore::Resource {
                id: 1,
                api_version: "v1".to_string(),
                kind: "ConfigMap".to_string(),
                namespace: Some("default".to_string()),
                name: "cm".to_string(),
                uid: "uid-1".to_string(),
                resource_version: 7,
                data: Arc::new(serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": "cm", "namespace": "default", "uid": "uid-1"}
                })),
            })
        }

        async fn prepare_update(
            &self,
            _current: &crate::datastore::Resource,
            body: Value,
            _dry_run: crate::api::mutation::DryRunMode,
        ) -> Result<Value, AppError> {
            self.calls.lock().unwrap().push("prepare_update");
            Ok(body)
        }

        async fn persist_update(
            &self,
            _current: crate::datastore::Resource,
            body: Value,
            dry_run: crate::api::mutation::DryRunMode,
        ) -> Result<WriteResult, AppError> {
            self.calls.lock().unwrap().push("persist_update");
            if dry_run.is_all() {
                Ok(WriteResult::DryRun(body))
            } else {
                Ok(WriteResult::PersistedValue(body))
            }
        }
    }

    #[derive(Clone)]
    struct RecordingPatchStrategy {
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl PatchStrategy for RecordingPatchStrategy {
        async fn apply_patch(
            &self,
            patch: Value,
            dry_run: crate::api::mutation::DryRunMode,
        ) -> Result<WriteResult, AppError> {
            self.calls.lock().unwrap().push("apply_patch");
            if dry_run.is_all() {
                Ok(WriteResult::DryRun(patch))
            } else {
                Ok(WriteResult::PersistedValue(patch))
            }
        }
    }

    #[tokio::test]
    async fn create_with_strategy_runs_before_admission_then_admission_then_persist() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let strategy = RecordingCreateStrategy {
            calls: calls.clone(),
        };
        let result = create_with_strategy(
            &strategy,
            serde_json::json!({"metadata": {"name": "cm"}}),
            crate::api::mutation::DryRunMode::Live,
        )
        .await
        .unwrap();
        assert!(matches!(result, WriteResult::PersistedValue(_)));
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            ["before_admission", "admit", "persist_create"]
        );
    }

    #[tokio::test]
    async fn update_with_strategy_loads_prepares_then_persists() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let strategy = RecordingUpdateStrategy {
            calls: calls.clone(),
        };
        let result = update_with_strategy(
            &strategy,
            serde_json::json!({"metadata": {"name": "cm"}}),
            crate::api::mutation::DryRunMode::Live,
        )
        .await
        .unwrap();
        assert!(matches!(result, WriteResult::PersistedValue(_)));
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            ["load_current", "prepare_update", "persist_update"]
        );
    }

    #[tokio::test]
    async fn patch_with_strategy_delegates_complete_patch_flow_to_strategy() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let strategy = RecordingPatchStrategy {
            calls: calls.clone(),
        };
        let result = patch_with_strategy(
            &strategy,
            serde_json::json!({"metadata": {"name": "cm"}}),
            crate::api::mutation::DryRunMode::Live,
        )
        .await
        .unwrap();
        assert!(matches!(result, WriteResult::PersistedValue(_)));
        assert_eq!(calls.lock().unwrap().as_slice(), ["apply_patch"]);
    }
}
