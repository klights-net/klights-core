//! Phase 10C.1 move-only characterization for shared SQLite/Redb mutations.
//!
//! The SQLite committed-apply adapter suites remain the authoritative coverage
//! for `CommittedApplyV1` receipts, durable idempotency, terminal rejection,
//! and transactional rollback. Pod repository/workqueue suites likewise
//! remain authoritative for the unscheduled-Pod UID/RV CAS exception. These
//! cases fill only the missing cross-backend behavior freeze needed before the
//! datastore mutation slice moves.

use std::num::NonZeroUsize;

use serde_json::{Value, json};

use crate::datastore::backend::DatastoreBackend;
use crate::datastore::redb::RedbDatastore;
use crate::datastore::sqlite::Datastore as SqliteDatastore;
use crate::datastore::types::{PositionedWatchReplayRead, WatchTarget};
use klights_cluster_core::{
    PatchKind, ResourcePatchRequest, ResourcePreconditions, WatchReplayPosition,
};

const API_VERSION: &str = "v1";
const KIND: &str = "ConfigMap";
const NAMESPACE: &str = "phase10c1";
const NAME: &str = "mutation-freeze";
const UID: &str = "phase10c1-mutation-uid";

async fn sqlite_db() -> SqliteDatastore {
    SqliteDatastore::new_in_memory().await.unwrap()
}

async fn redb_db() -> RedbDatastore {
    RedbDatastore::new_in_memory().await.unwrap()
}

macro_rules! characterize_backends {
    ($name:ident, $case:ident) => {
        mod $name {
            #[tokio::test]
            async fn sqlite() {
                let db = super::sqlite_db().await;
                super::$case(&db).await;
            }

            #[tokio::test]
            async fn redb() {
                let db = super::redb_db().await;
                super::$case(&db).await;
            }
        }
    };
}

fn config_map(value: &str) -> Value {
    json!({
        "apiVersion": API_VERSION,
        "kind": KIND,
        "metadata": {
            "name": NAME,
            "namespace": NAMESPACE,
            "uid": UID
        },
        "data": {"value": value}
    })
}

fn assert_one_visible_mutation(
    before: WatchReplayPosition,
    after: WatchReplayPosition,
    assigned_resource_version: i64,
    operation: &str,
) {
    assert_eq!(
        assigned_resource_version,
        before.resource_version + 1,
        "{operation} must allocate exactly one public resourceVersion"
    );
    assert_eq!(
        after.resource_version, assigned_resource_version,
        "{operation} must expose its assigned resourceVersion at the durable position"
    );
    assert_eq!(
        after.event_id,
        before.event_id + 1,
        "{operation} must insert exactly one durable watch event"
    );
    assert_eq!(
        after.resource_version_filter_through_event_id, 0,
        "{operation} must leave an exact durable position"
    );
}

#[derive(Clone, Copy)]
enum RejectedResourceMutation {
    DuplicateCreate,
    StaleUpdate,
    WrongUidPatch,
    StaleDelete,
}

impl RejectedResourceMutation {
    fn name(self) -> &'static str {
        match self {
            Self::DuplicateCreate => "duplicate create",
            Self::StaleUpdate => "stale update",
            Self::WrongUidPatch => "wrong-UID patch",
            Self::StaleDelete => "stale delete",
        }
    }
}

async fn normal_mutation_cas_rv_watch_and_rollback(db: &dyn DatastoreBackend) {
    let anchor = db.current_watch_replay_position().await.unwrap();
    let created = db
        .create_resource(
            API_VERSION,
            KIND,
            Some(NAMESPACE),
            NAME,
            config_map("created"),
        )
        .await
        .unwrap();
    let after_create = db.current_watch_replay_position().await.unwrap();
    assert_eq!(created.uid, UID);
    assert_one_visible_mutation(anchor, after_create, created.resource_version, "create");

    let rejected_cases = [
        RejectedResourceMutation::DuplicateCreate,
        RejectedResourceMutation::StaleUpdate,
        RejectedResourceMutation::WrongUidPatch,
        RejectedResourceMutation::StaleDelete,
    ];
    for case in rejected_cases {
        let result = match case {
            RejectedResourceMutation::DuplicateCreate => db
                .create_resource(
                    API_VERSION,
                    KIND,
                    Some(NAMESPACE),
                    NAME,
                    config_map("duplicate"),
                )
                .await
                .map(|_| ()),
            RejectedResourceMutation::StaleUpdate => db
                .update_resource_with_preconditions(
                    API_VERSION,
                    KIND,
                    Some(NAMESPACE),
                    NAME,
                    config_map("stale-update"),
                    ResourcePreconditions::uid_and_resource_version(
                        UID,
                        created.resource_version + 100,
                    ),
                )
                .await
                .map(|_| ()),
            RejectedResourceMutation::WrongUidPatch => db
                .patch_resource_latest_with_preconditions(
                    API_VERSION,
                    KIND,
                    Some(NAMESPACE),
                    NAME,
                    ResourcePatchRequest::new(
                        PatchKind::Merge,
                        json!({"data": {"value": "wrong-uid-patch"}}),
                        ResourcePreconditions::uid_and_resource_version(
                            "phase10c1-wrong-uid",
                            created.resource_version,
                        ),
                    )
                    .with_strict_resource_version(),
                )
                .await
                .map(|_| ()),
            RejectedResourceMutation::StaleDelete => {
                db.delete_resource_with_preconditions(
                    API_VERSION,
                    KIND,
                    Some(NAMESPACE),
                    NAME,
                    ResourcePreconditions::uid_and_resource_version(
                        UID,
                        created.resource_version + 100,
                    ),
                )
                .await
            }
        };
        assert!(result.is_err(), "{} must be rejected", case.name());
        assert_eq!(
            db.current_watch_replay_position().await.unwrap(),
            after_create,
            "{} must not allocate public RV or watch history",
            case.name()
        );
        let stored = db
            .get_resource(API_VERSION, KIND, Some(NAMESPACE), NAME)
            .await
            .unwrap()
            .expect("rejected mutation must preserve the live row");
        assert_eq!(
            stored.resource_version,
            created.resource_version,
            "{} must preserve the live resourceVersion",
            case.name()
        );
        assert_eq!(
            stored.data.pointer("/data/value").and_then(Value::as_str),
            Some("created"),
            "{} must roll back its object change",
            case.name()
        );
    }

    let updated = db
        .update_resource_with_preconditions(
            API_VERSION,
            KIND,
            Some(NAMESPACE),
            NAME,
            config_map("updated"),
            ResourcePreconditions::uid_and_resource_version(UID, created.resource_version),
        )
        .await
        .unwrap();
    let after_update = db.current_watch_replay_position().await.unwrap();
    assert_one_visible_mutation(
        after_create,
        after_update,
        updated.resource_version,
        "strict CAS update",
    );
    assert_eq!(
        updated.data.pointer("/data/value").and_then(Value::as_str),
        Some("updated")
    );

    db.delete_resource_with_preconditions(
        API_VERSION,
        KIND,
        Some(NAMESPACE),
        NAME,
        ResourcePreconditions::uid_and_resource_version(UID, updated.resource_version),
    )
    .await
    .unwrap();
    let after_delete = db.current_watch_replay_position().await.unwrap();
    assert_one_visible_mutation(
        after_update,
        after_delete,
        after_delete.resource_version,
        "strict CAS delete",
    );
    assert!(
        db.get_resource(API_VERSION, KIND, Some(NAMESPACE), NAME)
            .await
            .unwrap()
            .is_none()
    );

    let replay = db
        .list_watch_events_after_position_checked_bounded(
            &[WatchTarget::namespaced_in_namespace(
                API_VERSION,
                KIND,
                NAMESPACE,
            )],
            anchor,
            NonZeroUsize::new(8).unwrap(),
        )
        .await
        .unwrap();
    let PositionedWatchReplayRead::Events(replay) = replay else {
        panic!("fresh mutation anchor must remain replayable");
    };
    assert_eq!(replay.next_position, after_delete);
    let expected = [
        ("ADDED", created.resource_version),
        ("MODIFIED", updated.resource_version),
        ("DELETED", after_delete.resource_version),
    ];
    assert_eq!(replay.events.len(), expected.len());
    for (index, (event, (event_type, resource_version))) in
        replay.events.iter().zip(expected).enumerate()
    {
        assert_eq!(
            event.event.event_type.as_ref(),
            event_type,
            "watch event {index} type"
        );
        assert_eq!(event.event.resource.name, NAME, "watch event {index} name");
        assert_eq!(event.event.resource.uid, UID, "watch event {index} UID");
        assert_eq!(
            event.event.resource.resource_version, resource_version,
            "watch event {index} resourceVersion"
        );
        assert_eq!(
            event.position,
            WatchReplayPosition {
                resource_version,
                event_id: anchor.event_id + index as i64 + 1,
                resource_version_filter_through_event_id: 0,
            },
            "watch event {index} durable position"
        );
    }
}

async fn namespace_create_update_rv_and_watch(db: &dyn DatastoreBackend) {
    const NS_NAME: &str = "phase10c1-namespace";
    const NS_UID: &str = "phase10c1-namespace-uid";

    let anchor = db.current_watch_replay_position().await.unwrap();
    let created = db
        .create_namespace(
            NS_NAME,
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": NS_NAME, "uid": NS_UID}
            }),
        )
        .await
        .unwrap();
    let after_create = db.current_watch_replay_position().await.unwrap();
    assert_eq!(created.uid, NS_UID);
    assert_one_visible_mutation(
        anchor,
        after_create,
        created.resource_version,
        "namespace create",
    );

    let updated = db
        .update_namespace(
            NS_NAME,
            json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": NS_NAME,
                    "uid": NS_UID,
                    "labels": {"phase": "10c1"}
                }
            }),
            created.resource_version,
        )
        .await
        .unwrap();
    let after_update = db.current_watch_replay_position().await.unwrap();
    assert_one_visible_mutation(
        after_create,
        after_update,
        updated.resource_version,
        "namespace update",
    );
    let stored = db
        .get_namespace(NS_NAME)
        .await
        .unwrap()
        .expect("updated Namespace must remain stored");
    assert_eq!(stored.uid, NS_UID);
    assert_eq!(
        stored
            .data
            .pointer("/metadata/labels/phase")
            .and_then(Value::as_str),
        Some("10c1")
    );

    let replay = db
        .list_watch_events_after_position_checked_bounded(
            &[WatchTarget::cluster("v1", "Namespace")],
            anchor,
            NonZeroUsize::new(4).unwrap(),
        )
        .await
        .unwrap();
    let PositionedWatchReplayRead::Events(replay) = replay else {
        panic!("fresh Namespace mutation anchor must remain replayable");
    };
    assert_eq!(replay.next_position, after_update);
    let expected = [
        ("ADDED", created.resource_version),
        ("MODIFIED", updated.resource_version),
    ];
    assert_eq!(replay.events.len(), expected.len());
    for (index, (event, (event_type, resource_version))) in
        replay.events.iter().zip(expected).enumerate()
    {
        assert_eq!(event.event.event_type.as_ref(), event_type);
        assert_eq!(event.event.resource.name, NS_NAME);
        assert_eq!(event.event.resource.uid, NS_UID);
        assert_eq!(event.event.resource.resource_version, resource_version);
        assert_eq!(
            event.position,
            WatchReplayPosition {
                resource_version,
                event_id: anchor.event_id + index as i64 + 1,
                resource_version_filter_through_event_id: 0,
            }
        );
    }
}

characterize_backends!(
    normal_mutation_cas_rv_watch_and_rollback,
    normal_mutation_cas_rv_watch_and_rollback
);
characterize_backends!(
    namespace_create_update_rv_and_watch,
    namespace_create_update_rv_and_watch
);
