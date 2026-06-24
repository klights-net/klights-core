//! Differential nft route plan.
//!
//! Diffs the previously-applied set of [`ServiceSpec`]s against the current
//! inventory snapshot. The plan lists per-service add / update / remove
//! operations so the coalescer can apply only what changed instead of
//! rebuilding the entire `services` chain on every watch event.
//!
//! A full chain rebuild is still the recovery path for watch compaction or
//! inventory corruption — the planner just keeps the steady-state cost
//! proportional to actual changes.

use std::collections::HashMap;

use super::service_rules::ServiceSpec;

/// One concrete plan to bring nft state from `prev` to `next`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RoutePlan {
    pub added: Vec<ServiceSpec>,
    pub updated: Vec<ServiceSpec>,
    pub removed: Vec<ServiceSpec>,
}

impl RoutePlan {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.updated.is_empty() && self.removed.is_empty()
    }

    /// Compute the diff between two ServiceSpec slices. Specs are matched by
    /// ClusterIP since that is the routable identity in nft DNAT terms.
    pub fn diff(prev: &[ServiceSpec], next: &[ServiceSpec]) -> RoutePlan {
        let prev_by_ip: HashMap<_, _> = prev.iter().map(|s| (s.cluster_ip, s)).collect();
        let next_by_ip: HashMap<_, _> = next.iter().map(|s| (s.cluster_ip, s)).collect();

        let mut added = Vec::new();
        let mut updated = Vec::new();
        let mut removed = Vec::new();

        for (ip, next_spec) in &next_by_ip {
            match prev_by_ip.get(ip) {
                None => added.push((*next_spec).clone()),
                Some(prev_spec) if *prev_spec != *next_spec => updated.push((*next_spec).clone()),
                Some(_) => {}
            }
        }
        for (ip, prev_spec) in &prev_by_ip {
            if !next_by_ip.contains_key(ip) {
                removed.push((*prev_spec).clone());
            }
        }

        added.sort_by_key(|s| s.cluster_ip);
        updated.sort_by_key(|s| s.cluster_ip);
        removed.sort_by_key(|s| s.cluster_ip);

        RoutePlan {
            added,
            updated,
            removed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::networking::service_routing::service_rules::{PortSpec, Protocol};
    use crate::networking::service_routing::session_affinity::SessionAffinity;
    use std::net::Ipv4Addr;

    fn spec(cluster_ip: &str, endpoint: &str, port: u16) -> ServiceSpec {
        ServiceSpec {
            cluster_ip: cluster_ip.parse().unwrap(),
            ports: vec![PortSpec {
                protocol: Protocol::Tcp,
                service_port: port,
                target_port: port,
                node_port: None,
                endpoints: vec![endpoint.parse::<Ipv4Addr>().unwrap()],
            }],
            session_affinity: SessionAffinity::None,
        }
    }

    #[test]
    fn route_planner_noops_identical_inventory() {
        let a = vec![spec("10.43.0.1", "10.50.0.10", 80)];
        let b = vec![spec("10.43.0.1", "10.50.0.10", 80)];
        let plan = RoutePlan::diff(&a, &b);
        assert!(
            plan.is_empty(),
            "identical inventory must produce empty plan: {plan:?}"
        );
    }

    #[test]
    fn route_planner_removes_stale_endpoint() {
        let prev = vec![
            spec("10.43.0.1", "10.50.0.10", 80),
            spec("10.43.0.2", "10.50.0.20", 80),
        ];
        let next = vec![spec("10.43.0.1", "10.50.0.10", 80)];
        let plan = RoutePlan::diff(&prev, &next);
        assert!(plan.added.is_empty());
        assert!(plan.updated.is_empty());
        assert_eq!(plan.removed.len(), 1);
        assert_eq!(
            plan.removed[0].cluster_ip,
            "10.43.0.2".parse::<Ipv4Addr>().unwrap()
        );
    }

    #[test]
    fn route_planner_adds_new_endpoint_without_full_resync() {
        let prev = vec![spec("10.43.0.1", "10.50.0.10", 80)];
        let next = vec![
            spec("10.43.0.1", "10.50.0.10", 80),
            spec("10.43.0.2", "10.50.0.20", 80),
        ];
        let plan = RoutePlan::diff(&prev, &next);
        assert_eq!(
            plan.added.len(),
            1,
            "only the new service should be added: {plan:?}"
        );
        assert!(plan.removed.is_empty());
        assert!(plan.updated.is_empty());
        assert_eq!(
            plan.added[0].cluster_ip,
            "10.43.0.2".parse::<Ipv4Addr>().unwrap()
        );
    }

    #[test]
    fn route_planner_marks_changed_endpoint_set_as_updated() {
        let prev = vec![spec("10.43.0.1", "10.50.0.10", 80)];
        let next = vec![spec("10.43.0.1", "10.50.0.99", 80)];
        let plan = RoutePlan::diff(&prev, &next);
        assert!(plan.added.is_empty());
        assert!(plan.removed.is_empty());
        assert_eq!(plan.updated.len(), 1);
    }
}
