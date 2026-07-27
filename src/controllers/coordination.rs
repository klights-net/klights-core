//! Instance-owned coordination shared by one controller composition graph.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};

use klights_reconcile_api::GcForegroundDeleteCoordination;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum CoordinatedControllerKind {
    Job,
    ReplicaSet,
    ReplicationController,
}

/// Root-owned ambient inputs shared by one controller reconcile invocation.
///
/// Keeping these values together makes the execution identity explicit without
/// teaching controller logic about the composition root that owns them.
#[derive(Clone, Copy)]
pub(crate) struct ControllerReconcileContext<'a> {
    pub(crate) coordination: &'a ControllerCoordination,
    pub(crate) node_name: &'a str,
}

impl<'a> ControllerReconcileContext<'a> {
    pub(crate) fn new(coordination: &'a ControllerCoordination, node_name: &'a str) -> Self {
        Self {
            coordination,
            node_name,
        }
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct ControllerReconcileKey {
    kind: CoordinatedControllerKind,
    namespace: Box<str>,
    name: Box<str>,
}

impl ControllerReconcileKey {
    fn new(kind: CoordinatedControllerKind, namespace: &str, name: &str) -> Self {
        Self {
            kind,
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct ForegroundPodDeleteKey {
    owner_uid: Box<str>,
    child_uid: Box<str>,
}

impl ForegroundPodDeleteKey {
    fn new(owner_uid: &str, child_uid: &str) -> Self {
        Self {
            owner_uid: owner_uid.into(),
            child_uid: child_uid.into(),
        }
    }
}

/// Coordination state owned by one root-constructed controller graph.
///
/// Reconcile locks are held weakly so observing new Kubernetes names does not
/// retain one lock allocation per name for the life of the process.
#[derive(Default)]
pub(crate) struct ControllerCoordination {
    reconcile_locks: Mutex<HashMap<ControllerReconcileKey, Weak<tokio::sync::Mutex<()>>>>,
    foreground_pod_deletes: Mutex<HashSet<ForegroundPodDeleteKey>>,
}

impl ControllerCoordination {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn reconcile_lock(
        &self,
        kind: CoordinatedControllerKind,
        namespace: &str,
        name: &str,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self
            .reconcile_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locks.retain(|_, lock| lock.strong_count() != 0);
        let key = ControllerReconcileKey::new(kind, namespace, name);
        if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(key, Arc::downgrade(&lock));
        lock
    }

    #[cfg(test)]
    fn reconcile_lock_count(&self) -> usize {
        self.reconcile_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

impl GcForegroundDeleteCoordination for ControllerCoordination {
    fn reserve(&self, owner_uid: &str, child_uid: &str) -> bool {
        if owner_uid.is_empty() || child_uid.is_empty() {
            return false;
        }
        self.foreground_pod_deletes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(ForegroundPodDeleteKey::new(owner_uid, child_uid))
    }

    fn release(&self, owner_uid: &str, child_uid: &str) {
        if owner_uid.is_empty() || child_uid.is_empty() {
            return;
        }
        self.foreground_pod_deletes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&ForegroundPodDeleteKey::new(owner_uid, child_uid));
    }

    fn retain_owner_children(&self, owner_uid: &str, seen_child_uids: &HashSet<String>) {
        self.foreground_pod_deletes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|key| {
                key.owner_uid.as_ref() != owner_uid
                    || seen_child_uids.contains(key.child_uid.as_ref())
            });
    }

    fn clear_owner(&self, owner_uid: &str) {
        self.foreground_pod_deletes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|key| key.owner_uid.as_ref() != owner_uid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn same_instance_and_key_share_one_lock() {
        let coordination = ControllerCoordination::new();
        let first =
            coordination.reconcile_lock(CoordinatedControllerKind::ReplicaSet, "default", "web");
        let second =
            coordination.reconcile_lock(CoordinatedControllerKind::ReplicaSet, "default", "web");
        let guard = first.lock().await;
        assert!(second.try_lock().is_err());
        drop(guard);
        assert!(second.try_lock().is_ok());
    }

    #[test]
    fn different_keys_and_graphs_are_isolated() {
        let first_graph = ControllerCoordination::new();
        let second_graph = ControllerCoordination::new();
        let first = first_graph.reconcile_lock(CoordinatedControllerKind::Job, "default", "first");
        let different_key =
            first_graph.reconcile_lock(CoordinatedControllerKind::Job, "default", "second");
        let different_graph =
            second_graph.reconcile_lock(CoordinatedControllerKind::Job, "default", "first");

        let _guard = first.try_lock().expect("first key");
        assert!(different_key.try_lock().is_ok());
        assert!(different_graph.try_lock().is_ok());
    }

    #[test]
    fn dead_reconcile_locks_are_pruned() {
        let coordination = ControllerCoordination::new();
        let lock = coordination.reconcile_lock(
            CoordinatedControllerKind::ReplicationController,
            "default",
            "expired",
        );
        assert_eq!(coordination.reconcile_lock_count(), 1);
        drop(lock);

        let _replacement = coordination.reconcile_lock(
            CoordinatedControllerKind::ReplicationController,
            "default",
            "live",
        );
        assert_eq!(coordination.reconcile_lock_count(), 1);
    }

    #[test]
    fn foreground_reservations_are_graph_local_and_prunable() {
        let first_graph = ControllerCoordination::new();
        let second_graph = ControllerCoordination::new();
        assert!(first_graph.reserve("owner", "first"));
        assert!(!first_graph.reserve("owner", "first"));
        assert!(second_graph.reserve("owner", "first"));
        assert!(first_graph.reserve("owner", "second"));

        first_graph.retain_owner_children("owner", &HashSet::from(["second".to_string()]));
        assert!(first_graph.reserve("owner", "first"));
        assert!(!first_graph.reserve("owner", "second"));

        first_graph.release("owner", "second");
        assert!(first_graph.reserve("owner", "second"));
        first_graph.clear_owner("owner");
        assert!(first_graph.reserve("owner", "second"));
    }
}
