#![cfg(feature = "test-support")]

use std::sync::Arc;

use klights_node_datastore::test_support::RuntimeWorkTestStore;
use klights_node_store::test_support::PodWorkqueueTestPorts;
use klights_node_store::{
    PodWorkIdentity, PodWorkqueueEnqueue, PodWorkqueueLeaseToken, PodWorkqueueMutationOutcome,
};
use klights_supervisor::{TaskCategoryConfig, TaskSupervisor};
use klights_types::PodIdentity;

async fn fixture() -> (RuntimeWorkTestStore, PodWorkqueueTestPorts) {
    let store = RuntimeWorkTestStore::open(
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
        "sqlite:p12-1c-runtime-work-support",
    )
    .await
    .unwrap();
    let ports = PodWorkqueueTestPorts::new(store.pod_workqueue());
    (store, ports)
}

async fn enqueue(store: &RuntimeWorkTestStore, uid: &str) {
    store
        .pod_workqueue()
        .enqueue_work(
            PodWorkqueueEnqueue::try_new(
                PodWorkIdentity::try_pod(PodIdentity::new("default", "pod-a", uid)).unwrap(),
                br#"{"target_node":"node-a"}"#.to_vec(),
                0,
                0,
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn wrong_uid_token_cannot_ack_actor_work() {
    let (store, ports) = fixture().await;
    enqueue(&store, "uid-live").await;
    let claim = ports
        .claim_uid_bound_pod_work(i64::MAX / 4, 100)
        .await
        .unwrap()
        .unwrap();
    let token = claim.lease_token();
    let wrong_uid = PodWorkqueueLeaseToken::try_new(
        token.id().get(),
        PodWorkIdentity::try_pod(PodIdentity::new("default", "pod-a", "uid-replacement")).unwrap(),
        token.leased_next_due_ms().get(),
    )
    .unwrap();
    assert_eq!(
        ports.acknowledge_token(wrong_uid).await.unwrap(),
        PodWorkqueueMutationOutcome::Stale
    );
    assert_eq!(
        ports.acknowledge_claim(claim).await.unwrap(),
        PodWorkqueueMutationOutcome::Applied
    );
}

#[tokio::test]
async fn expired_lease_cannot_ack_reclaimed_actor_work() {
    let (store, ports) = fixture().await;
    enqueue(&store, "uid-live").await;
    let first = ports
        .claim_uid_bound_pod_work(i64::MAX / 4, 10)
        .await
        .unwrap()
        .unwrap();
    let second = ports
        .claim_uid_bound_pod_work(i64::MAX / 4 + 10, 10)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ports.acknowledge_claim(first).await.unwrap(),
        PodWorkqueueMutationOutcome::Stale
    );
    assert_eq!(
        ports.acknowledge_claim(second).await.unwrap(),
        PodWorkqueueMutationOutcome::Applied
    );
}
