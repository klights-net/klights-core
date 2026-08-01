use super::*;
use crate::kubelet::pod_repository::PodObjectWriter;
use klights_cluster_core::Resource;
use klights_pod_api::PodRepositoryError;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

struct GeneratedNameCollisionWriter {
    collisions_before_success: usize,
    attempts: AtomicUsize,
    creates: Mutex<Vec<(String, String)>>,
}

impl GeneratedNameCollisionWriter {
    fn new(collisions_before_success: usize) -> Self {
        Self {
            collisions_before_success,
            attempts: AtomicUsize::new(0),
            creates: Mutex::new(Vec::new()),
        }
    }

    fn create_names(&self) -> Vec<(String, String)> {
        self.creates.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl PodObjectWriter for GeneratedNameCollisionWriter {
    async fn create_controller_pod(
        &self,
        namespace: &str,
        name: &str,
        _node_name: &str,
        pod: serde_json::Value,
    ) -> anyhow::Result<Resource> {
        let body_name = pod
            .pointer("/metadata/name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        self.creates
            .lock()
            .unwrap()
            .push((name.to_string(), body_name));
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt < self.collisions_before_success {
            return Err(anyhow::Error::new(PodRepositoryError::already_exists(
                format!("Pod {namespace}/{name} already exists"),
            )));
        }

        Ok(Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(namespace.to_string()),
            name: name.to_string(),
            uid: format!("uid-{name}"),
            resource_version: 1,
            data: std::sync::Arc::new(pod),
        })
    }

    async fn delete_pod(&self, _namespace: &str, _name: &str) -> anyhow::Result<()> {
        panic!("generated-name collision retries must not delete a Pod")
    }

    async fn update_pod_owner_references(
        &self,
        _namespace: &str,
        _name: &str,
        _owner_refs: Vec<serde_json::Value>,
    ) -> anyhow::Result<Resource> {
        panic!("generated-name collision retries must not mutate an existing Pod")
    }

    async fn merge_pod_labels(
        &self,
        _namespace: &str,
        _name: &str,
        _labels: Vec<(String, String)>,
    ) -> anyhow::Result<Resource> {
        panic!("generated-name collision retries must not mutate an existing Pod")
    }
}

fn pod_template() -> serde_json::Value {
    json!({
        "metadata": {"labels": {"app": "collision-test"}},
        "spec": {"containers": [{"name": "main", "image": "example.invalid/test:1"}]}
    })
}

#[tokio::test]
async fn generated_pod_create_retries_typed_already_exists_then_succeeds() {
    let writer = GeneratedNameCollisionWriter::new(2);
    let mut generated = ["rs-first", "rs-second", "rs-third"].into_iter();

    super::create_pod_with_name_generator(
        &writer,
        "rs",
        "rs-uid",
        "default",
        "test-node",
        &pod_template(),
        || {
            generated
                .next()
                .expect("retry budget exceeded test names")
                .to_string()
        },
    )
    .await
    .expect("a fresh generated name should succeed after collisions");

    assert_eq!(
        writer.create_names(),
        vec![
            ("rs-first".to_string(), "rs-first".to_string()),
            ("rs-second".to_string(), "rs-second".to_string()),
            ("rs-third".to_string(), "rs-third".to_string()),
        ]
    );
}

#[tokio::test]
async fn generated_pod_create_exhausts_exactly_eight_name_collisions() {
    let writer = GeneratedNameCollisionWriter::new(usize::MAX);
    let mut generated = (0..8).map(|attempt| format!("rs-attempt-{attempt}"));

    let error = super::create_pod_with_name_generator(
        &writer,
        "rs",
        "rs-uid",
        "default",
        "test-node",
        &pod_template(),
        || generated.next().expect("more than eight names requested"),
    )
    .await
    .expect_err("eight generated-name collisions must return AlreadyExists");

    let controller_error = error
        .downcast_ref::<klights_reconcile_api::ControllerStoreError>()
        .expect("typed controller error must survive retry exhaustion");
    assert!(controller_error.is_already_exists());
    assert_eq!(writer.create_names().len(), 8);
}

#[tokio::test]
async fn generated_pod_retry_uses_a_fresh_name_without_mutating_the_conflict() {
    let writer = GeneratedNameCollisionWriter::new(1);
    let mut generated = ["rs-occupied", "rs-replacement"].into_iter();

    super::create_pod_with_name_generator(
        &writer,
        "rs",
        "rs-uid",
        "default",
        "test-node",
        &pod_template(),
        || {
            generated
                .next()
                .expect("retry must use exactly two names")
                .to_string()
        },
    )
    .await
    .expect("replacement generated name should be created");

    let creates = writer.create_names();
    assert_eq!(creates.len(), 2);
    assert_eq!(creates[0].0, "rs-occupied");
    assert_eq!(creates[1].0, "rs-replacement");
    assert_ne!(creates[0].0, creates[1].0);
    assert!(creates.iter().all(|(argument, body)| argument == body));
}
