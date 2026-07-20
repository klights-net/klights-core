use std::collections::HashSet;

use klights_reconcile_api::{MutationFacts, MutationOperation, ReconcileKey, ResourceChange};

#[test]
fn persisted_live_mutations_have_one_neutral_change_classification() {
    let cases = [
        (MutationOperation::Create, ResourceChange::Created),
        (MutationOperation::Update, ResourceChange::Updated),
        (MutationOperation::Patch, ResourceChange::Updated),
        (MutationOperation::DeleteMark, ResourceChange::Updated),
        (MutationOperation::HardDelete, ResourceChange::Deleted),
    ];

    for (operation, expected) in cases {
        let facts = MutationFacts::new(operation, true, false);
        assert_eq!(facts.operation(), operation);
        assert!(facts.persisted());
        assert!(!facts.dry_run());
        assert_eq!(facts.change(), Some(expected));
    }
}

#[test]
fn non_persisted_and_dry_run_mutations_have_no_reconcile_change() {
    for operation in [
        MutationOperation::Create,
        MutationOperation::Update,
        MutationOperation::Patch,
        MutationOperation::DeleteMark,
        MutationOperation::HardDelete,
    ] {
        for (persisted, dry_run) in [(false, false), (false, true), (true, true)] {
            let facts = MutationFacts::new(operation, persisted, dry_run);
            assert_eq!(
                facts.change(),
                None,
                "operation={operation:?}, persisted={persisted}, dry_run={dry_run}"
            );
        }
    }
}

#[test]
fn reconcile_keys_preserve_scope_and_deduplicate_by_full_identity() {
    let namespaced = ReconcileKey::namespaced("apps/v1", "Deployment", "default", "web");
    let duplicate = ReconcileKey::namespaced("apps/v1", "Deployment", "default", "web");
    let other_namespace = ReconcileKey::namespaced("apps/v1", "Deployment", "other", "web");
    let cluster = ReconcileKey::cluster("v1", "Namespace", "default");

    assert_eq!(namespaced.api_version(), "apps/v1");
    assert_eq!(namespaced.kind(), "Deployment");
    assert_eq!(namespaced.namespace(), Some("default"));
    assert_eq!(namespaced.name(), "web");
    assert_eq!(cluster.namespace(), None);
    assert_eq!(cluster.to_string(), "v1/Namespace default");

    assert_eq!(
        ReconcileKey::namespaced("v1", "Pod", "default", "web").into_parts(),
        ("v1", "Pod", Some("default".to_string()), "web".to_string())
    );

    let keys = HashSet::from([namespaced, duplicate, other_namespace, cluster]);
    assert_eq!(keys.len(), 3);
}
