//! Side effect to mirror Endpoints to EndpointSlices.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use super::SideEffect;

#[async_trait]
pub trait EndpointMirrorStore: Send + Sync {
    async fn mirror_endpoints(&self, resource: &Value) -> Result<()>;
    async fn delete_mirrored_endpointslice(&self, resource: &Value) -> Result<()>;
}

struct EndpointMirrorEffect {
    store: Arc<dyn EndpointMirrorStore>,
}

#[async_trait]
impl SideEffect for EndpointMirrorEffect {
    fn name(&self) -> &'static str {
        "endpoint_mirror"
    }

    async fn apply(&self, resource: &Value) -> Result<()> {
        self.store.mirror_endpoints(resource).await
    }

    async fn apply_delete(&self, resource: &Value) -> Result<()> {
        self.store.delete_mirrored_endpointslice(resource).await
    }
}

pub fn effect(store: Arc<dyn EndpointMirrorStore>) -> Arc<dyn SideEffect> {
    Arc::new(EndpointMirrorEffect { store })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct RecordingStore {
        operations: Mutex<Vec<&'static str>>,
    }

    #[async_trait]
    impl EndpointMirrorStore for RecordingStore {
        async fn mirror_endpoints(&self, _resource: &Value) -> Result<()> {
            self.operations.lock().unwrap().push("apply");
            Ok(())
        }

        async fn delete_mirrored_endpointslice(&self, _resource: &Value) -> Result<()> {
            self.operations.lock().unwrap().push("delete");
            Ok(())
        }
    }

    #[tokio::test]
    async fn endpoint_mirror_delegates_apply_and_delete() {
        let store = Arc::new(RecordingStore {
            operations: Mutex::new(Vec::new()),
        });
        let effect = effect(store.clone());
        let resource = serde_json::json!({"apiVersion": "v1", "kind": "Endpoints"});

        effect.apply(&resource).await.unwrap();
        effect.apply_delete(&resource).await.unwrap();

        assert_eq!(effect.name(), "endpoint_mirror");
        assert_eq!(*store.operations.lock().unwrap(), vec!["apply", "delete"]);
    }
}
