//! Generic create/update/patch strategy orchestration.

use async_trait::async_trait;
use axum::http::StatusCode;
use serde_json::Value;

use crate::AppError;

use super::{DryRunMode, response};

const SPEC_BEARING_KINDS: &[&str] = &[
    "APIService",
    "CertificateSigningRequest",
    "CSIDriver",
    "CSINode",
    "DaemonSet",
    "Deployment",
    "FlowSchema",
    "HorizontalPodAutoscaler",
    "Ingress",
    "CronJob",
    "Job",
    "LimitRange",
    "MutatingWebhookConfiguration",
    "NetworkPolicy",
    "PersistentVolume",
    "PersistentVolumeClaim",
    "Pod",
    "PodDisruptionBudget",
    "PriorityLevelConfiguration",
    "ReplicaSet",
    "ReplicationController",
    "ResourceQuota",
    "Service",
    "StatefulSet",
    "ValidatingAdmissionPolicy",
    "ValidatingAdmissionPolicyBinding",
    "ValidatingWebhookConfiguration",
    "VolumeAttachment",
];

pub fn prepare_create_metadata(
    namespace: Option<&str>,
    body: &mut Value,
    resource_name: &str,
    now: chrono::DateTime<chrono::Utc>,
) {
    let Some(metadata) = body
        .as_object_mut()
        .and_then(|object| object.get_mut("metadata"))
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    if let Some(namespace) = namespace {
        metadata.insert(
            "namespace".to_string(),
            Value::String(namespace.to_string()),
        );
    }
    metadata.insert("name".to_string(), Value::String(resource_name.to_string()));
    let uid_missing_or_empty = metadata.get("uid").is_none_or(|value| {
        value.is_null() || value.as_str().is_some_and(|value| value.trim().is_empty())
    });
    if uid_missing_or_empty {
        metadata.insert(
            "uid".to_string(),
            Value::String(uuid::Uuid::new_v4().to_string()),
        );
    }
    if metadata.get("creationTimestamp").is_none_or(Value::is_null) {
        metadata.insert(
            "creationTimestamp".to_string(),
            Value::String(klights_cluster_core::k8s_time::format_time(now)),
        );
    }
    if metadata
        .get("generation")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        == 0
    {
        metadata.insert("generation".to_string(), serde_json::json!(1));
    }
}

pub fn prepare_custom_generation_for_update(current: &Value, body: &mut Value) {
    if body.pointer("/spec") == current.pointer("/spec") {
        return;
    }
    let Some(metadata) = body.pointer_mut("/metadata").and_then(Value::as_object_mut) else {
        return;
    };
    let current_generation = current
        .pointer("/metadata/generation")
        .and_then(Value::as_i64)
        .unwrap_or(1);
    metadata.insert(
        "generation".to_string(),
        serde_json::json!(current_generation + 1),
    );
}

pub fn prepare_builtin_generation_for_update(kind: &str, current: &Value, body: &mut Value) {
    if SPEC_BEARING_KINDS.contains(&kind) {
        prepare_custom_generation_for_update(current, body);
    }
}

pub enum WriteResult {
    DryRun(Value),
    Persisted(klights_cluster_core::Resource),
    PersistedValue(Value),
    Response { status: StatusCode, body: Value },
}

impl WriteResult {
    pub fn into_response_parts(self, default_status: StatusCode) -> (StatusCode, Value) {
        match self {
            Self::DryRun(value) | Self::PersistedValue(value) => (default_status, value),
            Self::Persisted(resource) => (
                default_status,
                response::persisted_object(resource.data, resource.resource_version),
            ),
            Self::Response { status, body } => (status, body),
        }
    }

    pub fn into_response_value(self) -> Value {
        self.into_response_parts(StatusCode::OK).1
    }

    pub fn persisted_resource(&self) -> Option<&klights_cluster_core::Resource> {
        match self {
            Self::Persisted(resource) => Some(resource),
            Self::DryRun(_) | Self::PersistedValue(_) | Self::Response { .. } => None,
        }
    }
}

#[async_trait]
pub trait CreateStrategy: Send + Sync {
    async fn before_admission(&self, body: Value) -> Result<Value, AppError>;
    async fn admit(&self, body: Value, dry_run: DryRunMode) -> Result<Value, AppError>;
    async fn persist_create(
        &self,
        body: Value,
        dry_run: DryRunMode,
    ) -> Result<WriteResult, AppError>;
}

#[async_trait]
pub trait UpdateStrategy: Send + Sync {
    async fn load_current(&self) -> Result<klights_cluster_core::Resource, AppError>;
    async fn prepare_update(
        &self,
        current: &klights_cluster_core::Resource,
        body: Value,
        dry_run: DryRunMode,
    ) -> Result<Value, AppError>;
    async fn persist_update(
        &self,
        current: klights_cluster_core::Resource,
        body: Value,
        dry_run: DryRunMode,
    ) -> Result<WriteResult, AppError>;
}

#[async_trait]
pub trait PatchStrategy: Send + Sync {
    async fn apply_patch(&self, patch: Value, dry_run: DryRunMode)
    -> Result<WriteResult, AppError>;
}

pub async fn create_with_strategy<S>(
    strategy: &S,
    body: Value,
    dry_run: DryRunMode,
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
    dry_run: DryRunMode,
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
    dry_run: DryRunMode,
) -> Result<WriteResult, AppError>
where
    S: PatchStrategy,
{
    strategy.apply_patch(patch, dry_run).await
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn prepare_create_metadata_stamps_identity_and_generation() {
        let mut body = serde_json::json!({"metadata": {}});
        prepare_create_metadata(
            Some("default"),
            &mut body,
            "cm1",
            chrono::DateTime::UNIX_EPOCH,
        );
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

    struct RecordingCreateStrategy(Arc<Mutex<Vec<&'static str>>>);

    struct RecordingUpdateStrategy(Arc<Mutex<Vec<&'static str>>>);

    struct RecordingPatchStrategy(Arc<Mutex<Vec<&'static str>>>);

    #[async_trait]
    impl CreateStrategy for RecordingCreateStrategy {
        async fn before_admission(&self, body: Value) -> Result<Value, AppError> {
            self.0.lock().unwrap().push("before_admission");
            Ok(body)
        }

        async fn admit(&self, body: Value, _: DryRunMode) -> Result<Value, AppError> {
            self.0.lock().unwrap().push("admit");
            Ok(body)
        }

        async fn persist_create(
            &self,
            body: Value,
            _: DryRunMode,
        ) -> Result<WriteResult, AppError> {
            self.0.lock().unwrap().push("persist_create");
            Ok(WriteResult::PersistedValue(body))
        }
    }

    #[async_trait]
    impl UpdateStrategy for RecordingUpdateStrategy {
        async fn load_current(&self) -> Result<klights_cluster_core::Resource, AppError> {
            self.0.lock().unwrap().push("load_current");
            Ok(klights_cluster_core::Resource {
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
            _current: &klights_cluster_core::Resource,
            body: Value,
            _dry_run: DryRunMode,
        ) -> Result<Value, AppError> {
            self.0.lock().unwrap().push("prepare_update");
            Ok(body)
        }

        async fn persist_update(
            &self,
            _current: klights_cluster_core::Resource,
            body: Value,
            _dry_run: DryRunMode,
        ) -> Result<WriteResult, AppError> {
            self.0.lock().unwrap().push("persist_update");
            Ok(WriteResult::PersistedValue(body))
        }
    }

    #[async_trait]
    impl PatchStrategy for RecordingPatchStrategy {
        async fn apply_patch(
            &self,
            patch: Value,
            _dry_run: DryRunMode,
        ) -> Result<WriteResult, AppError> {
            self.0.lock().unwrap().push("apply_patch");
            Ok(WriteResult::PersistedValue(patch))
        }
    }

    #[tokio::test]
    async fn create_strategy_preserves_stage_order() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        create_with_strategy(
            &RecordingCreateStrategy(calls.clone()),
            serde_json::json!({}),
            DryRunMode::Live,
        )
        .await
        .unwrap();
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            ["before_admission", "admit", "persist_create"]
        );
    }

    #[tokio::test]
    async fn update_strategy_preserves_stage_order() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        update_with_strategy(
            &RecordingUpdateStrategy(calls.clone()),
            serde_json::json!({}),
            DryRunMode::Live,
        )
        .await
        .unwrap();
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            ["load_current", "prepare_update", "persist_update"]
        );
    }

    #[tokio::test]
    async fn patch_strategy_preserves_stage_order() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        patch_with_strategy(
            &RecordingPatchStrategy(calls.clone()),
            serde_json::json!({}),
            DryRunMode::Live,
        )
        .await
        .unwrap();
        assert_eq!(calls.lock().unwrap().as_slice(), ["apply_patch"]);
    }

    #[test]
    fn generation_helpers_preserve_spec_change_semantics() {
        let current = serde_json::json!({
            "metadata": {"generation": 3},
            "spec": {"replicas": 1}
        });
        let mut updated = serde_json::json!({
            "metadata": {"generation": 3},
            "spec": {"replicas": 2}
        });
        prepare_builtin_generation_for_update("Deployment", &current, &mut updated);
        assert_eq!(updated["metadata"]["generation"], 4);
    }
}
