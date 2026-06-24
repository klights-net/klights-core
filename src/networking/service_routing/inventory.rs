//! Local desired-state inventory for Service routing.
//!
//! Holds the raw `v1.Service`, `v1.Endpoints`, and
//! `discovery.k8s.io/v1.EndpointSlice` objects keyed by `(namespace, name)`
//! so the route sync can build `ServiceSpec`s from cached state instead of
//! re-listing the entire cluster API on every event.
//!
//! The inventory is updated from watch events:
//!   `apply_service_event` / `apply_endpoints_event` / `apply_endpoint_slice_event`
//!
//! Stale events are rejected by `resource_version` comparison so out-of-order
//! delivery never regresses to an older state.

use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

use super::service_rules::ServiceSpec;

/// Per-Service inventory entry. `endpoint_slices` is keyed by EndpointSlice
/// name so updates and deletes are O(1).
#[derive(Clone, Debug, Default)]
pub struct ServiceInventoryEntry {
    pub service: Option<Value>,
    pub service_rv: i64,
    pub endpoints: Option<Value>,
    pub endpoints_rv: i64,
    pub endpoint_slices: BTreeMap<String, EndpointSliceEntry>,
}

#[derive(Clone, Debug)]
pub struct EndpointSliceEntry {
    pub data: Value,
    pub resource_version: i64,
}

/// Result of applying one watch event to the inventory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InventoryApply {
    /// Event observed and applied — the inventory changed.
    Applied,
    /// Event was a strict no-op (same RV, equal content) or stale
    /// (RV not newer than the cached one).
    NoChange,
    /// Event removed an entry from the inventory.
    Removed,
}

#[derive(Clone, Debug, Default)]
pub struct ServiceRouteInventory {
    entries: HashMap<(String, String), ServiceInventoryEntry>,
}

impl ServiceRouteInventory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, namespace: &str, name: &str) -> Option<&ServiceInventoryEntry> {
        self.entries.get(&(namespace.to_string(), name.to_string()))
    }

    fn entry_mut(&mut self, namespace: &str, name: &str) -> &mut ServiceInventoryEntry {
        self.entries
            .entry((namespace.to_string(), name.to_string()))
            .or_default()
    }

    /// Replace the inventory wholesale from a bulk snapshot (initial sync).
    pub fn replace_from_snapshot(
        &mut self,
        services: impl IntoIterator<Item = (String, String, i64, Value)>,
        endpoints: impl IntoIterator<Item = (String, String, i64, Value)>,
        endpoint_slices: impl IntoIterator<Item = (String, String, String, i64, Value)>,
    ) {
        self.entries.clear();
        for (ns, name, rv, data) in services {
            let entry = self.entry_mut(&ns, &name);
            entry.service = Some(data);
            entry.service_rv = rv;
        }
        for (ns, name, rv, data) in endpoints {
            let entry = self.entry_mut(&ns, &name);
            entry.endpoints = Some(data);
            entry.endpoints_rv = rv;
        }
        for (ns, service_name, slice_name, rv, data) in endpoint_slices {
            let entry = self.entry_mut(&ns, &service_name);
            entry.endpoint_slices.insert(
                slice_name,
                EndpointSliceEntry {
                    data,
                    resource_version: rv,
                },
            );
        }
    }

    pub fn apply_service_event(
        &mut self,
        namespace: &str,
        name: &str,
        resource_version: i64,
        deleted: bool,
        data: Option<Value>,
    ) -> InventoryApply {
        if deleted {
            return match self
                .entries
                .remove(&(namespace.to_string(), name.to_string()))
            {
                Some(_) => InventoryApply::Removed,
                None => InventoryApply::NoChange,
            };
        }
        let entry = self.entry_mut(namespace, name);
        if resource_version <= entry.service_rv {
            return InventoryApply::NoChange;
        }
        entry.service = data;
        entry.service_rv = resource_version;
        InventoryApply::Applied
    }

    pub fn apply_endpoints_event(
        &mut self,
        namespace: &str,
        name: &str,
        resource_version: i64,
        deleted: bool,
        data: Option<Value>,
    ) -> InventoryApply {
        let key = (namespace.to_string(), name.to_string());
        if deleted {
            return match self.entries.get_mut(&key) {
                Some(entry) => {
                    let had = entry.endpoints.is_some();
                    entry.endpoints = None;
                    entry.endpoints_rv = 0;
                    if had {
                        InventoryApply::Removed
                    } else {
                        InventoryApply::NoChange
                    }
                }
                None => InventoryApply::NoChange,
            };
        }
        let entry = self.entry_mut(namespace, name);
        if resource_version <= entry.endpoints_rv {
            return InventoryApply::NoChange;
        }
        entry.endpoints = data;
        entry.endpoints_rv = resource_version;
        InventoryApply::Applied
    }

    pub fn apply_endpoint_slice_event(
        &mut self,
        namespace: &str,
        service_name: &str,
        slice_name: &str,
        resource_version: i64,
        deleted: bool,
        data: Option<Value>,
    ) -> InventoryApply {
        let key = (namespace.to_string(), service_name.to_string());
        if deleted {
            return match self.entries.get_mut(&key) {
                Some(entry) => match entry.endpoint_slices.remove(slice_name) {
                    Some(_) => InventoryApply::Removed,
                    None => InventoryApply::NoChange,
                },
                None => InventoryApply::NoChange,
            };
        }
        let entry = self.entry_mut(namespace, service_name);
        if let Some(existing) = entry.endpoint_slices.get(slice_name)
            && resource_version <= existing.resource_version
        {
            return InventoryApply::NoChange;
        }
        let data = match data {
            Some(d) => d,
            None => return InventoryApply::NoChange,
        };
        entry.endpoint_slices.insert(
            slice_name.to_string(),
            EndpointSliceEntry {
                data,
                resource_version,
            },
        );
        InventoryApply::Applied
    }

    /// Build the routable `ServiceSpec` list from the cached inventory. The
    /// order is deterministic (namespace, name) so the planner can diff.
    pub fn to_specs(&self) -> Vec<ServiceSpec> {
        let mut keys: Vec<&(String, String)> = self.entries.keys().collect();
        keys.sort();
        let mut specs = Vec::with_capacity(keys.len());
        for key in keys {
            let entry = &self.entries[key];
            let Some(service) = entry.service.as_ref() else {
                continue;
            };
            let slice_refs: Vec<&Value> = entry.endpoint_slices.values().map(|s| &s.data).collect();
            let spec = if !slice_refs.is_empty() {
                ServiceSpec::from_service_and_endpointslices(service, &slice_refs).or_else(|| {
                    entry
                        .endpoints
                        .as_ref()
                        .and_then(|eps| ServiceSpec::from_service_and_endpoints(service, Some(eps)))
                })
            } else {
                entry
                    .endpoints
                    .as_ref()
                    .and_then(|eps| ServiceSpec::from_service_and_endpoints(service, Some(eps)))
            };
            if let Some(spec) = spec {
                specs.push(spec);
            }
        }
        specs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dns_service() -> Value {
        json!({
            "apiVersion": "v1", "kind": "Service",
            "metadata": {"namespace": "kube-system", "name": "kube-dns"},
            "spec": {
                "clusterIP": "10.43.0.10",
                "ports": [{"name": "dns", "port": 53, "protocol": "UDP", "targetPort": 53}]
            }
        })
    }

    fn dns_endpoints(ip: &str) -> Value {
        json!({
            "apiVersion": "v1", "kind": "Endpoints",
            "metadata": {"namespace": "kube-system", "name": "kube-dns"},
            "subsets": [{
                "addresses": [{"ip": ip}],
                "ports": [{"name": "dns", "port": 53, "protocol": "UDP"}]
            }]
        })
    }

    fn dns_slice(slice_name: &str, ip: &str) -> Value {
        json!({
            "apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice",
            "metadata": {
                "namespace": "kube-system",
                "name": slice_name,
                "labels": {"kubernetes.io/service-name": "kube-dns"}
            },
            "addressType": "IPv4",
            "ports": [{"name": "dns", "port": 53, "protocol": "UDP"}],
            "endpoints": [{"addresses": [ip], "conditions": {"ready": true}}]
        })
    }

    #[test]
    fn route_inventory_updates_from_service_endpoint_and_slice_events() {
        let mut inv = ServiceRouteInventory::new();

        // 1. Service add — no endpoints yet → no spec routable.
        assert_eq!(
            inv.apply_service_event("kube-system", "kube-dns", 1, false, Some(dns_service())),
            InventoryApply::Applied
        );
        assert!(inv.to_specs().is_empty());

        // 2. Endpoints add → service becomes routable.
        assert_eq!(
            inv.apply_endpoints_event(
                "kube-system",
                "kube-dns",
                2,
                false,
                Some(dns_endpoints("10.50.0.20"))
            ),
            InventoryApply::Applied
        );
        let specs = inv.to_specs();
        assert_eq!(specs.len(), 1);
        assert_eq!(
            specs[0].ports[0].endpoints,
            vec!["10.50.0.20".parse::<std::net::Ipv4Addr>().unwrap()]
        );

        // 3. EndpointSlice add — takes precedence over legacy Endpoints.
        assert_eq!(
            inv.apply_endpoint_slice_event(
                "kube-system",
                "kube-dns",
                "kube-dns-1",
                3,
                false,
                Some(dns_slice("kube-dns-1", "10.50.0.30"))
            ),
            InventoryApply::Applied
        );
        let specs = inv.to_specs();
        assert_eq!(
            specs[0].ports[0].endpoints,
            vec!["10.50.0.30".parse::<std::net::Ipv4Addr>().unwrap()]
        );

        // 4. Stale endpoint-slice event (same RV) → NoChange.
        assert_eq!(
            inv.apply_endpoint_slice_event(
                "kube-system",
                "kube-dns",
                "kube-dns-1",
                3,
                false,
                Some(dns_slice("kube-dns-1", "10.50.0.99"))
            ),
            InventoryApply::NoChange
        );

        // 5. Delete the slice → falls back to legacy endpoints.
        assert_eq!(
            inv.apply_endpoint_slice_event("kube-system", "kube-dns", "kube-dns-1", 4, true, None),
            InventoryApply::Removed
        );
        let specs = inv.to_specs();
        assert_eq!(
            specs[0].ports[0].endpoints,
            vec!["10.50.0.20".parse::<std::net::Ipv4Addr>().unwrap()]
        );

        // 6. Delete the service → entry gone.
        assert_eq!(
            inv.apply_service_event("kube-system", "kube-dns", 5, true, None),
            InventoryApply::Removed
        );
        assert!(inv.to_specs().is_empty());
    }
}
