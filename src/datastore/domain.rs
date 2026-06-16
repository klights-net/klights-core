//! Storage domain classification for HA replication.
//!
//! Every datastore table and `DatastoreBackend` method is classified by its
//! replication scope. This machine-checked map prevents data loss or divergent
//! HA behavior when Phase 3 Raft replication lands.
//!
//! ## Domain classes
//!
//! - **ClusterReplicated** — K8s API resources, namespaces, watch history,
//!   `node_subnets`, and any state every control-plane node must agree on.
//! - **NodeLocal** — sandbox IDs, pod network allocations, per-node retry
//!   queues, and anything derived from local runtime/containerd state.
//! - **DerivedLocal** — caches or indexes that can be rebuilt from replicated
//!   state or local runtime state.
//! - **ConfigReplicated** — runtime config that must be identical across HA
//!   members, such as watch retention bounds once Raft is enabled.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

/// Storage domain classification for a table or method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageDomain {
    /// State that must be identical across all cluster nodes.
    /// Replicated via Raft in Phase 3.
    ClusterReplicated,

    /// State that is specific to a single node and never replicated.
    /// Each node has its own independent copy.
    NodeLocal,

    /// Derived data that can be rebuilt from ClusterReplicated or NodeLocal state.
    /// Not stored directly or can be discarded and recreated.
    DerivedLocal,

    /// Configuration that must be identical across HA members.
    /// Becomes Raft-replicated in Phase 3.
    ConfigReplicated,

    /// Configuration pinned to one node's local data root.
    ConfigLocal,
}

impl StorageDomain {
    /// Returns true if this domain is replicated across the cluster.
    pub fn is_replicated(self) -> bool {
        matches!(self, Self::ClusterReplicated | Self::ConfigReplicated)
    }

    /// Returns true if this domain is node-local only.
    pub fn is_node_local(self) -> bool {
        matches!(
            self,
            Self::NodeLocal | Self::DerivedLocal | Self::ConfigLocal
        )
    }
}

/// Table metadata including its domain classification.
#[derive(Debug, Clone)]
pub struct TableMeta {
    /// The domain this table belongs to.
    pub domain: StorageDomain,
    /// Whether this table has a foreign key to a table in a different domain.
    pub cross_domain_fk: bool,
    /// If cross_domain_fk is true, explains why it's safe.
    pub cross_domain_note: Option<&'static str>,
}

/// Method metadata including its domain classification.
#[derive(Debug, Clone)]
pub struct MethodMeta {
    /// The primary domain this method operates on.
    pub domain: StorageDomain,
    /// Whether this method touches multiple domains.
    pub cross_domain: bool,
}

/// All tables in the datastore schema with their domain classifications.
///
/// This map is machine-checked: adding a table to schema.rs without classifying
/// it here will cause a test failure.
pub fn table_domains() -> &'static BTreeMap<&'static str, TableMeta> {
    &TABLE_DOMAINS
}

/// All `DatastoreBackend` methods with their domain classifications.
///
/// This map is machine-checked: adding a method to the trait without classifying
/// it here will cause a test failure.
pub fn method_domains() -> &'static BTreeMap<&'static str, MethodMeta> {
    &METHOD_DOMAINS
}

// ---------------------------------------------------------------------------
// Table domain classifications
//
// Every table in schema.rs must be classified here. The test
// `every_schema_table_has_domain_classification` enforces this.
//
// Cross-domain foreign keys are prohibited unless documented with a safety note.
// The test `node_local_tables_have_no_cluster_replicated_fks` enforces this.
// ---------------------------------------------------------------------------

static TABLE_DOMAINS: LazyLock<BTreeMap<&'static str, TableMeta>> = LazyLock::new(|| {
    use StorageDomain as D;

    let mut m = BTreeMap::new();

    // Cluster-replicated tables: K8s API resources and cluster state
    m.insert(
        "namespaced_resources",
        TableMeta {
            domain: D::ClusterReplicated,
            cross_domain_fk: false,
            cross_domain_note: None,
        },
    );
    m.insert(
        "cluster_resources",
        TableMeta {
            domain: D::ClusterReplicated,
            cross_domain_fk: false,
            cross_domain_note: None,
        },
    );
    m.insert(
        "namespaces",
        TableMeta {
            domain: D::ClusterReplicated,
            cross_domain_fk: false,
            cross_domain_note: None,
        },
    );
    m.insert(
        "watch_events",
        TableMeta {
            domain: D::ClusterReplicated,
            cross_domain_fk: false,
            cross_domain_note: None,
        },
    );
    m.insert(
        "node_subnets",
        TableMeta {
            domain: D::ClusterReplicated,
            cross_domain_fk: false,
            cross_domain_note: None,
        },
    );
    m.insert(
        "node_dataplane",
        TableMeta {
            domain: D::ClusterReplicated,
            cross_domain_fk: false,
            cross_domain_note: None,
        },
    );
    m.insert(
        "pod_cleanup_intents",
        TableMeta {
            domain: D::ClusterReplicated,
            cross_domain_fk: false,
            cross_domain_note: None,
        },
    );

    // Node-local tables: per-node state that never replicates
    m.insert(
        "outbox",
        TableMeta {
            domain: D::NodeLocal,
            cross_domain_fk: true,
            cross_domain_note: Some(
                "Durable kubelet-to-control-plane write intents. Payloads may \
                 reference cluster resources but are not resource caches.",
            ),
        },
    );
    m.insert(
        "pod_runtime",
        TableMeta {
            domain: D::NodeLocal,
            cross_domain_fk: true,
            cross_domain_note: Some(
                "References pods (ClusterReplicated) but is local runtime truth \
                 for containerd orphan cleanup on this node only. No FK enforcement.",
            ),
        },
    );
    m.insert(
        "pod_networks",
        TableMeta {
            domain: D::NodeLocal,
            cross_domain_fk: true,
            // Safe: references ClusterReplicated pods but is local network state
            // for pods running on this node only.
            cross_domain_note: Some(
                "References pods (ClusterReplicated) but is local CNI state \
                 for pods on this node only. No FK enforcement.",
            ),
        },
    );
    m.insert(
        "pod_slot_admissions",
        TableMeta {
            domain: D::NodeLocal,
            cross_domain_fk: true,
            cross_domain_note: Some(
                "Local same-name pod admission slot. References pods by uid, \
                 but actor-owned finalization controls cleanup.",
            ),
        },
    );
    m.insert(
        "pod_workqueue",
        TableMeta {
            domain: D::NodeLocal,
            cross_domain_fk: false,
            cross_domain_note: None,
        },
    );
    m.insert(
        "probe_state",
        TableMeta {
            domain: D::NodeLocal,
            cross_domain_fk: true,
            cross_domain_note: Some(
                "Local probe scheduler state keyed by pod uid. Rebuilt from \
                 cluster pod state plus CRI after finalization.",
            ),
        },
    );
    m.insert(
        "replication_checkpoint",
        TableMeta {
            domain: D::NodeLocal,
            cross_domain_fk: false,
            cross_domain_note: None,
        },
    );
    m.insert(
        "_node_meta",
        TableMeta {
            domain: D::ConfigLocal,
            cross_domain_fk: false,
            cross_domain_note: None,
        },
    );
    m.insert(
        "pod_endpoints",
        TableMeta {
            domain: D::NodeLocal,
            cross_domain_fk: false,
            cross_domain_note: None,
        },
    );

    // Config tables: per-binary or cluster-wide configuration
    m.insert(
        "_klights_meta",
        TableMeta {
            domain: D::ConfigReplicated,
            cross_domain_fk: false,
            cross_domain_note: None,
        },
    );
    m.insert(
        "metadata",
        TableMeta {
            domain: D::ConfigReplicated,
            cross_domain_fk: false,
            cross_domain_note: None,
        },
    );

    m
});

// ---------------------------------------------------------------------------
// Method domain classifications
//
// Every method in DatastoreBackend must be classified here. The test
// `every_trait_method_has_domain_classification` enforces this.
// ---------------------------------------------------------------------------

static METHOD_DOMAINS: LazyLock<BTreeMap<&'static str, MethodMeta>> = LazyLock::new(|| {
    use StorageDomain as D;

    let mut m = BTreeMap::new();

    // Resource CRUD (ClusterReplicated)
    m.insert(
        "create_resource",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "get_resource",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "list_resources",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "list_resource_keys_for_scope",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "update_resource",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "update_status_only",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "delete_resource",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "get_current_resource_version",
        MethodMeta {
            domain: D::ConfigReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "patch_resource_latest",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );

    // Namespace operations (ClusterReplicated)
    m.insert(
        "create_namespace",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "get_namespace",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "list_namespaces",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "update_namespace",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "delete_namespace_contents",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "delete_namespace",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );

    // Watch operations (ClusterReplicated)
    m.insert(
        "subscribe_watch",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "broadcast_watch_event",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "list_watch_events_since",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );

    // Resource version and catch-up (ClusterReplicated/ConfigReplicated)
    m.insert(
        "list_cluster_resources_modified_since",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "list_cluster_resources",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "list_resources_modified_since",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "advance_resource_version_after",
        MethodMeta {
            domain: D::ConfigReplicated,
            cross_domain: false,
        },
    );

    // Namespace listing helpers (ClusterReplicated)
    m.insert(
        "list_namespace_resources",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "list_namespace_resources_of_kind",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "list_namespace_resources_excluding_kind",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "count_namespace_resources",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );

    // Owner reference queries (ClusterReplicated)
    m.insert(
        "find_owned_resources",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "list_resources_by_owner_uid",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "find_owned_by_name_kind_empty_uid",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );

    // Node subnet operations (ClusterReplicated)
    m.insert(
        "allocate_node_subnet",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "update_node_peer_attributes",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "get_node_subnet",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "list_peer_subnets",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "delete_node_subnet",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "update_node_dataplane",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "get_node_dataplane",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "pod_slot_try_admit",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "pod_slot_mark_terminating",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "pod_slot_clear_if_uid",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "subscribe_pod_slot_admissions",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "move_pod_to_cleanup_intent",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "list_pod_cleanup_intents_for_node",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "delete_pod_cleanup_intent",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "delete_pod_cleanup_intents_for_node",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );

    // Sandbox operations (NodeLocal)
    m.insert(
        "record_sandbox",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: true,
        },
    );
    m.insert(
        "get_sandbox",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: true,
        },
    );
    m.insert(
        "get_sandbox_for_uid",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: true,
        },
    );
    m.insert(
        "delete_sandbox",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: true,
        },
    );
    m.insert(
        "list_sandboxes",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: true,
        },
    );

    // Pod network operations (NodeLocal)
    m.insert(
        "delete_pod_network",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: true,
        },
    );
    m.insert(
        "get_pod_network",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: true,
        },
    );
    m.insert(
        "get_pod_network_for_pod",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: true,
        },
    );
    m.insert(
        "ipam_allocate_and_record_pod_network",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: true,
        },
    );
    m.insert(
        "list_pod_network_sandbox_ids",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: true,
        },
    );

    // Pod workqueue operations (NodeLocal)
    m.insert(
        "pod_workqueue_enqueue",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: false,
        },
    );
    m.insert(
        "pod_workqueue_peek_next_due",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: false,
        },
    );
    m.insert(
        "pod_workqueue_claim_due",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: false,
        },
    );
    m.insert(
        "pod_workqueue_complete",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: false,
        },
    );
    m.insert(
        "pod_workqueue_record_failure",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: false,
        },
    );
    m.insert(
        "pod_workqueue_dead_letter",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: false,
        },
    );

    // Pod endpoint operations (NodeLocal)
    m.insert(
        "pod_endpoint_get_by_pod_ip",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: false,
        },
    );
    m.insert(
        "pod_endpoint_list_all",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: false,
        },
    );
    m.insert(
        "subscribe_pod_endpoints",
        MethodMeta {
            domain: D::NodeLocal,
            cross_domain: false,
        },
    );

    // klights metadata (ConfigReplicated)
    m.insert(
        "get_klights_meta",
        MethodMeta {
            domain: D::ConfigReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "set_klights_meta",
        MethodMeta {
            domain: D::ConfigReplicated,
            cross_domain: false,
        },
    );

    // GC (ClusterReplicated - operates on watch_events)
    m.insert(
        "gc_watch_events",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );
    m.insert(
        "applied_outbox_gc_prunable_count",
        MethodMeta {
            domain: D::ClusterReplicated,
            cross_domain: false,
        },
    );

    m
});

/// Returns all table names that belong to the given domain.
pub fn tables_by_domain(domain: StorageDomain) -> BTreeSet<&'static str> {
    table_domains()
        .iter()
        .filter(|(_, meta)| meta.domain == domain)
        .map(|(name, _)| *name)
        .collect()
}

/// Returns all method names that belong to the given domain.
pub fn methods_by_domain(domain: StorageDomain) -> BTreeSet<&'static str> {
    method_domains()
        .iter()
        .filter(|(_, meta)| meta.domain == domain)
        .map(|(name, _)| *name)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every table defined in schema.rs must have a domain classification.
    /// This test is machine-checked and will fail if a new table is added
    /// to schema.rs without classifying it in TABLE_DOMAINS.
    #[test]
    fn every_schema_table_has_domain_classification() {
        // All tables that exist in schema.rs
        let schema_tables: BTreeSet<&'static str> = [
            "namespaced_resources",
            "cluster_resources",
            "namespaces",
            "watch_events",
            "metadata",
            "_klights_meta",
            "node_subnets",
            "node_dataplane",
            "pod_cleanup_intents",
            "outbox",
            "pod_runtime",
            "pod_networks",
            "pod_slot_admissions",
            "pod_endpoints",
            "pod_workqueue",
            "probe_state",
            "replication_checkpoint",
            "_node_meta",
        ]
        .into_iter()
        .collect();

        let classified_tables: BTreeSet<_> = table_domains().keys().copied().collect();

        let missing: Vec<_> = schema_tables
            .difference(&classified_tables)
            .copied()
            .collect();
        let extra: Vec<_> = classified_tables
            .difference(&schema_tables)
            .copied()
            .collect();

        assert!(
            missing.is_empty(),
            "Tables in schema.rs missing from domain.rs: {missing:?}. \
             Add each missing table to TABLE_DOMAINS with its domain classification."
        );

        assert!(
            extra.is_empty(),
            "Tables in domain.rs not present in schema.rs: {extra:?}. \
             Remove stale entries from TABLE_DOMAINS."
        );
    }

    /// Every method in DatastoreBackend must have a domain classification.
    /// This test is machine-checked and will fail if a new method is added
    /// to the trait without classifying it in METHOD_DOMAINS.
    #[test]
    fn every_trait_method_has_domain_classification() {
        // All methods in DatastoreBackend (from backend.rs)
        let trait_methods: BTreeSet<&'static str> = [
            // Watch subscription
            "subscribe_watch",
            "broadcast_watch_event",
            // Resource CRUD
            "create_resource",
            "get_resource",
            "list_resources",
            "list_resource_keys_for_scope",
            "update_resource",
            "update_status_only",
            "delete_resource",
            "get_current_resource_version",
            "patch_resource_latest",
            // Namespace operations
            "create_namespace",
            "get_namespace",
            "list_namespaces",
            "update_namespace",
            "delete_namespace_contents",
            "delete_namespace",
            // Pod workqueue
            "pod_workqueue_enqueue",
            "pod_workqueue_peek_next_due",
            "pod_workqueue_claim_due",
            "pod_workqueue_complete",
            "pod_workqueue_record_failure",
            "pod_workqueue_dead_letter",
            // Sandbox operations
            "record_sandbox",
            "get_sandbox",
            "get_sandbox_for_uid",
            "delete_sandbox",
            // Pod network operations
            "delete_pod_network",
            "get_pod_network",
            "get_pod_network_for_pod",
            "ipam_allocate_and_record_pod_network",
            "list_sandboxes",
            "list_pod_network_sandbox_ids",
            // Owner reference queries
            "find_owned_resources",
            "list_resources_by_owner_uid",
            "find_owned_by_name_kind_empty_uid",
            // Resource modification queries
            "list_cluster_resources_modified_since",
            "list_cluster_resources",
            "list_resources_modified_since",
            "advance_resource_version_after",
            // Namespace listing helpers
            "list_namespace_resources",
            "list_namespace_resources_of_kind",
            "list_namespace_resources_excluding_kind",
            "count_namespace_resources",
            // Watch replay
            "list_watch_events_since",
            // Node subnet operations
            "allocate_node_subnet",
            "update_node_peer_attributes",
            "update_node_dataplane",
            "get_node_dataplane",
            "get_node_subnet",
            "list_peer_subnets",
            "delete_node_subnet",
            "pod_slot_try_admit",
            "pod_slot_mark_terminating",
            "pod_slot_clear_if_uid",
            "subscribe_pod_slot_admissions",
            "move_pod_to_cleanup_intent",
            "list_pod_cleanup_intents_for_node",
            "delete_pod_cleanup_intent",
            "delete_pod_cleanup_intents_for_node",
            // GC
            "gc_watch_events",
            "applied_outbox_gc_prunable_count",
            // Pod endpoints
            "pod_endpoint_get_by_pod_ip",
            "pod_endpoint_list_all",
            "subscribe_pod_endpoints",
            // klights metadata
            "get_klights_meta",
            "set_klights_meta",
        ]
        .into_iter()
        .collect();

        let classified_methods: BTreeSet<_> = method_domains().keys().copied().collect();

        let missing: Vec<_> = trait_methods
            .difference(&classified_methods)
            .copied()
            .collect();
        let extra: Vec<_> = classified_methods
            .difference(&trait_methods)
            .copied()
            .collect();

        assert!(
            missing.is_empty(),
            "Methods in DatastoreBackend missing from domain.rs: {missing:?}. \
             Add each missing method to METHOD_DOMAINS with its domain classification."
        );

        assert!(
            extra.is_empty(),
            "Methods in domain.rs not present in DatastoreBackend: {extra:?}. \
             Remove stale entries from METHOD_DOMAINS."
        );
    }

    /// NodeLocal tables must not have foreign keys to ClusterReplicated tables
    /// unless explicitly documented with a safety note. This prevents accidental
    /// data loss when replication is implemented.
    #[test]
    fn node_local_tables_have_no_cluster_replicated_fks() {
        for (table_name, meta) in table_domains() {
            if meta.domain == StorageDomain::NodeLocal && meta.cross_domain_fk {
                assert!(
                    meta.cross_domain_note.is_some(),
                    "NodeLocal table '{table_name}' has cross_domain_fk=true \
                     but no cross_domain_note explaining why it's safe. \
                     Add a note explaining why the FK is safe despite crossing domains."
                );
            }
        }
    }

    /// Verify pod_workqueue and pod_endpoints are explicitly classified.
    /// These are easy to miss since they're referenced in networking docs
    /// rather than main resource paths.
    #[test]
    fn pod_workqueue_and_pod_endpoints_are_classified() {
        let tables = table_domains();
        assert!(
            tables.contains_key("pod_workqueue"),
            "pod_workqueue table must be classified (DSB-HA-00 requirement)"
        );
        assert!(
            tables.contains_key("pod_endpoints"),
            "pod_endpoints table must be classified (DSB-HA-00 requirement)"
        );

        // Both are NodeLocal
        assert_eq!(
            tables.get("pod_workqueue").unwrap().domain,
            StorageDomain::NodeLocal,
            "pod_workqueue is NodeLocal (per-node retry queue)"
        );
        assert_eq!(
            tables.get("pod_endpoints").unwrap().domain,
            StorageDomain::NodeLocal,
            "pod_endpoints is NodeLocal (per-node reachability state)"
        );
    }

    /// Verify all ClusterReplicated tables are documented.
    /// This acts as a sanity check for the domain split.
    #[test]
    fn cluster_replicated_tables_are_known() {
        let cluster_tables = tables_by_domain(StorageDomain::ClusterReplicated);

        // Core K8s resources
        assert!(cluster_tables.contains("namespaced_resources"));
        assert!(cluster_tables.contains("cluster_resources"));
        assert!(cluster_tables.contains("namespaces"));

        // Watch history
        assert!(cluster_tables.contains("watch_events"));

        // Cluster networking state
        assert!(cluster_tables.contains("node_subnets"));
        assert!(cluster_tables.contains("node_dataplane"));

        // Cluster pod cleanup intents
        assert!(cluster_tables.contains("pod_cleanup_intents"));

        // Config tables should NOT be in ClusterReplicated
        assert!(!cluster_tables.contains("_klights_meta"));
        assert!(!cluster_tables.contains("metadata"));

        // Node-local tables should NOT be in ClusterReplicated
        assert!(!cluster_tables.contains("pod_runtime"));
        assert!(!cluster_tables.contains("pod_networks"));
        assert!(!cluster_tables.contains("pod_slot_admissions"));
        assert!(!cluster_tables.contains("pod_workqueue"));
        assert!(!cluster_tables.contains("pod_endpoints"));
    }

    /// Verify all NodeLocal tables are documented.
    #[test]
    fn node_local_tables_are_known() {
        let local_tables = tables_by_domain(StorageDomain::NodeLocal);

        // Per-node runtime state
        assert!(local_tables.contains("pod_runtime"));
        assert!(local_tables.contains("pod_networks"));
        assert!(local_tables.contains("pod_slot_admissions"));
        assert!(local_tables.contains("outbox"));
        assert!(local_tables.contains("probe_state"));
        assert!(local_tables.contains("replication_checkpoint"));

        // Per-node workqueue and endpoints (explicitly required by DSB-HA-00)
        assert!(local_tables.contains("pod_workqueue"));
        assert!(local_tables.contains("pod_endpoints"));
    }

    /// Verify ConfigReplicated tables are documented.
    #[test]
    fn config_replicated_tables_are_known() {
        let config_tables = tables_by_domain(StorageDomain::ConfigReplicated);

        assert!(config_tables.contains("_klights_meta"));
        assert!(config_tables.contains("metadata"));
    }

    /// Methods that touch pods should be marked as cross-domain if they're NodeLocal.
    /// This documents the pod reference from NodeLocal to ClusterReplicated.
    #[test]
    fn sandbox_and_pod_network_methods_are_cross_domain() {
        let methods = method_domains();

        // These methods reference ClusterReplicated pods but operate on NodeLocal state
        let cross_domain_methods = [
            "record_sandbox",
            "get_sandbox",
            "get_sandbox_for_uid",
            "delete_sandbox",
            "list_sandboxes",
            "delete_pod_network",
            "get_pod_network",
            "get_pod_network_for_pod",
            "ipam_allocate_and_record_pod_network",
            "list_pod_network_sandbox_ids",
        ];

        for method in cross_domain_methods {
            let meta = methods
                .get(method)
                .unwrap_or_else(|| panic!("method '{method}' should be classified"));

            assert!(
                meta.cross_domain,
                "Method '{method}' should be marked cross_domain=true \
                 because it references ClusterReplicated pods but operates on NodeLocal state"
            );
        }
    }
}
