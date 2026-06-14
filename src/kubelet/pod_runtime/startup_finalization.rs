use crate::kubelet::pod_runtime::service::PodRuntimeKey;
use crate::kubelet::pod_runtime::store::PodRuntimeStore;

fn sandbox_id_annotation(pod: &serde_json::Value) -> Option<String> {
    pod.pointer("/metadata/annotations/klights.dev~1sandbox-id")
        .and_then(|v| v.as_str())
        .filter(|sandbox_id| !sandbox_id.trim().is_empty())
        .map(str::to_string)
}

pub async fn resolve_startup_sandbox_id(
    store: &dyn PodRuntimeStore,
    key: &PodRuntimeKey,
    sandbox_id_hint: Option<&str>,
    pod: &serde_json::Value,
) -> Option<String> {
    let stored = match store.get_sandbox_id(key).await {
        Ok(Some(id)) if !id.trim().is_empty() => Some(id),
        Ok(Some(_)) | Ok(None) => None,
        Err(e) => {
            tracing::warn!(
                namespace = key.namespace,
                name = key.name,
                uid = key.uid,
                "failed to read sandbox id from runtime store during startup finalization: {e:#}"
            );
            None
        }
    };

    stored
        .or_else(|| {
            sandbox_id_hint
                .filter(|id| !id.trim().is_empty())
                .map(str::to_string)
        })
        .or_else(|| sandbox_id_annotation(pod))
}
