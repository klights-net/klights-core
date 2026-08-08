use std::sync::Arc;
use std::time::{Duration, SystemTime};

use klights_node_datastore::{SqliteRuntimeWorkStore, open};
use klights_node_store::{
    PodWorkIdentity, PodWorkqueueClaimRequest, PodWorkqueueEnqueue, PodWorkqueueLeaseToken,
    PodWorkqueueMutationOutcome, PodWorkqueueRequeue, PodWorkqueueStore, RuntimeWorkError,
};
use klights_supervisor::{TaskCategoryConfig, TaskSupervisor, WallClock};
use klights_types::PodIdentity;

struct FixedClock(i64);

impl WallClock for FixedClock {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(self.0 as u64)
    }
}

async fn fresh(now_ms: i64) -> SqliteRuntimeWorkStore {
    let executor = open::open_with_opts(
        open::in_memory_opts(),
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
        "sqlite:workqueue-lease-persistence-test",
    )
    .await
    .unwrap();
    SqliteRuntimeWorkStore::new(executor, Arc::new(FixedClock(now_ms)))
}

async fn disk(path: &std::path::Path, now_ms: i64, key: &str) -> SqliteRuntimeWorkStore {
    let executor = open::open_with_opts(
        open::disk_opts(path),
        Arc::new(TaskSupervisor::new(TaskCategoryConfig::default())),
        key,
    )
    .await
    .unwrap();
    SqliteRuntimeWorkStore::new(executor, Arc::new(FixedClock(now_ms)))
}

fn enqueue(uid: &str, delay_ms: i64) -> PodWorkqueueEnqueue {
    enqueue_named("web", uid, delay_ms)
}

fn enqueue_named(name: &str, uid: &str, delay_ms: i64) -> PodWorkqueueEnqueue {
    PodWorkqueueEnqueue::try_new(
        PodWorkIdentity::try_pod(PodIdentity::new("default", name, uid)).unwrap(),
        vec![0, 255, 1],
        0,
        delay_ms,
        None,
    )
    .unwrap()
}

fn ensure_named(name: &str, uid: &str, payload: &[u8], delay_ms: i64) -> PodWorkqueueEnqueue {
    PodWorkqueueEnqueue::try_new(
        PodWorkIdentity::try_pod(PodIdentity::new("default", name, uid)).unwrap(),
        payload.to_vec(),
        0,
        delay_ms,
        None,
    )
    .unwrap()
}

#[tokio::test]
async fn ensure_absent_inserts_once_and_preserves_pending_row_exactly() {
    let store = fresh(100).await;
    assert!(
        store
            .ensure_work_if_absent(ensure_named("web", "uid-web", b"node-a", 10))
            .await
            .unwrap()
    );
    assert!(
        !store
            .ensure_work_if_absent(ensure_named("web", "uid-web", b"node-b", 900))
            .await
            .unwrap()
    );
    assert!(
        !store
            .ensure_work_if_absent(ensure_named("web", "uid-web", b"node-c", i64::MAX))
            .await
            .unwrap(),
        "an existing exact reminder must return unchanged before evaluating a replacement due time"
    );
    let claimed = store
        .claim_due_work_with_lease(PodWorkqueueClaimRequest::try_new(110, 50).unwrap())
        .await
        .unwrap()
        .expect("original due row");
    assert_eq!(claimed.entry().payload(), b"node-a");
    assert_eq!(claimed.entry().identity().as_pod().unwrap().uid, "uid-web");
}

#[tokio::test]
async fn ensure_absent_does_not_mutate_an_existing_lease() {
    let store = fresh(100).await;
    store.enqueue_work(enqueue("uid-web", 0)).await.unwrap();
    let claimed = store
        .claim_due_work_with_lease(PodWorkqueueClaimRequest::try_new(100, 50).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !store
            .ensure_work_if_absent(ensure_named("web", "uid-web", b"replacement", 0))
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .acknowledge_work(claimed.token().clone())
            .await
            .unwrap(),
        PodWorkqueueMutationOutcome::Applied
    );
}

#[tokio::test]
async fn concurrent_ensure_inserts_one_exact_uid_row_and_keeps_other_uids_distinct() {
    let store = Arc::new(fresh(100).await);
    let left = store.clone();
    let right = store.clone();
    let (left_inserted, right_inserted) = tokio::join!(
        async move {
            left.ensure_work_if_absent(ensure_named("web", "uid-web", b"node-a", 0))
                .await
                .unwrap()
        },
        async move {
            right
                .ensure_work_if_absent(ensure_named("web", "uid-web", b"node-a", 0))
                .await
                .unwrap()
        }
    );
    assert_eq!(usize::from(left_inserted) + usize::from(right_inserted), 1);
    assert!(
        store
            .ensure_work_if_absent(ensure_named("web", "uid-replacement", b"node-b", 0))
            .await
            .unwrap(),
        "same-name replacement UID must remain a distinct reminder identity"
    );
}

#[tokio::test]
async fn crash_reopen_preserves_claim_until_lease_expiry() {
    let directory = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    std::fs::set_permissions(
        directory.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .unwrap();
    let path = directory.path().join("node.db");
    let first_store = disk(&path, 100, "sqlite:workqueue-crash-before-ack").await;
    first_store
        .enqueue_work(enqueue("uid-web", 0))
        .await
        .unwrap();
    let first = first_store
        .claim_due_work_with_lease(PodWorkqueueClaimRequest::try_new(100, 50).unwrap())
        .await
        .unwrap()
        .unwrap();
    let claimed_id = first.entry().id();
    drop(first);
    drop(first_store);

    let reopened = disk(&path, 150, "sqlite:workqueue-after-reopen").await;
    assert!(
        reopened
            .claim_due_work_with_lease(PodWorkqueueClaimRequest::try_new(149, 50).unwrap())
            .await
            .unwrap()
            .is_none()
    );
    let reclaimed = reopened
        .claim_due_work_with_lease(PodWorkqueueClaimRequest::try_new(150, 50).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reclaimed.entry().id(), claimed_id);
}

#[tokio::test]
async fn concurrent_claimers_cannot_own_the_same_unexpired_row() {
    let store = Arc::new(fresh(100).await);
    store.enqueue_work(enqueue("uid-web", 0)).await.unwrap();
    let request = PodWorkqueueClaimRequest::try_new(100, 50).unwrap();
    let left_store = store.clone();
    let right_store = store.clone();
    let (left, right) = tokio::join!(
        async move { left_store.claim_due_work_with_lease(request).await.unwrap() },
        async move {
            right_store
                .claim_due_work_with_lease(request)
                .await
                .unwrap()
        },
    );
    assert_eq!(
        usize::from(left.is_some()) + usize::from(right.is_some()),
        1
    );
}

#[tokio::test]
async fn claim_retains_row_until_token_ack_and_expired_lease_is_reclaimable() {
    let store = fresh(100).await;
    store.enqueue_work(enqueue("uid-web", 0)).await.unwrap();

    let first = store
        .claim_due_work_with_lease(PodWorkqueueClaimRequest::try_new(100, 50).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.token().leased_next_due_ms().get(), 150);
    assert_eq!(store.peek_next_due_ms().await.unwrap(), Some(150));
    assert!(
        store
            .claim_due_work_with_lease(PodWorkqueueClaimRequest::try_new(149, 50).unwrap())
            .await
            .unwrap()
            .is_none()
    );

    let reclaimed = store
        .claim_due_work_with_lease(PodWorkqueueClaimRequest::try_new(150, 50).unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reclaimed.entry().id(), first.entry().id());
    assert_eq!(
        store.acknowledge_work(first.token().clone()).await.unwrap(),
        PodWorkqueueMutationOutcome::Stale
    );
    assert_eq!(
        store
            .acknowledge_work(reclaimed.token().clone())
            .await
            .unwrap(),
        PodWorkqueueMutationOutcome::Applied
    );
    assert_eq!(store.peek_next_due_ms().await.unwrap(), None);
}

#[tokio::test]
async fn enqueue_invalidates_old_lease_ownership_before_ack() {
    let store = fresh(100).await;
    store.enqueue_work(enqueue("uid-web", 0)).await.unwrap();
    let claimed = store
        .claim_due_work_with_lease(PodWorkqueueClaimRequest::try_new(100, 50).unwrap())
        .await
        .unwrap()
        .unwrap();

    store.enqueue_work(enqueue("uid-web", 10)).await.unwrap();
    assert_eq!(
        store
            .acknowledge_work(claimed.token().clone())
            .await
            .unwrap(),
        PodWorkqueueMutationOutcome::Stale
    );
    assert!(store.peek_next_due_ms().await.unwrap().is_some());
}

#[tokio::test]
async fn stale_requeue_and_wrong_identity_tokens_cannot_mutate_claimed_row() {
    let store = fresh(100).await;
    store.enqueue_work(enqueue("uid-web", 0)).await.unwrap();
    let claimed = store
        .claim_due_work_with_lease(PodWorkqueueClaimRequest::try_new(100, 50).unwrap())
        .await
        .unwrap()
        .unwrap();
    let wrong_identity = PodWorkqueueLeaseToken::try_new(
        claimed.entry().id().get(),
        PodWorkIdentity::try_pod(PodIdentity::new("default", "web", "other-uid")).unwrap(),
        claimed.token().leased_next_due_ms().get(),
    )
    .unwrap();
    assert_eq!(
        store
            .acknowledge_work(wrong_identity.clone())
            .await
            .unwrap(),
        PodWorkqueueMutationOutcome::Stale
    );
    assert_eq!(
        store
            .requeue_work(
                PodWorkqueueRequeue::try_new(wrong_identity, vec![1], 1, 0, None).unwrap(),
            )
            .await
            .unwrap(),
        PodWorkqueueMutationOutcome::Stale
    );

    let original_token = claimed.token().clone();
    assert_eq!(
        store
            .requeue_work(
                PodWorkqueueRequeue::try_new(
                    original_token.clone(),
                    vec![2],
                    1,
                    0,
                    Some("retry".to_string()),
                )
                .unwrap(),
            )
            .await
            .unwrap(),
        PodWorkqueueMutationOutcome::Applied
    );
    assert_eq!(
        store
            .acknowledge_work(original_token.clone())
            .await
            .unwrap(),
        PodWorkqueueMutationOutcome::Stale
    );
    assert_eq!(
        store
            .requeue_work(
                PodWorkqueueRequeue::try_new(original_token, vec![3], 2, 0, None).unwrap(),
            )
            .await
            .unwrap(),
        PodWorkqueueMutationOutcome::Stale
    );
}

#[tokio::test]
async fn enqueue_tail_order_is_strict_and_overflow_is_rejected() {
    let store = fresh(i64::MAX - 1).await;
    store
        .enqueue_work(enqueue_named("first", "uid-first", 0))
        .await
        .unwrap();
    assert_eq!(store.peek_next_due_ms().await.unwrap(), Some(i64::MAX - 1));
    store
        .enqueue_work(enqueue_named("second", "uid-second", 0))
        .await
        .unwrap();
    let error = store
        .enqueue_work(enqueue_named("third", "uid-third", 0))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        RuntimeWorkError::InvalidInput {
            field: "next_due_ms",
            ..
        }
    ));
}
