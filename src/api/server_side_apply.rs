//! Server-Side Apply (SSA) — K8s `application/apply-patch+yaml` / `+json`.
//!
//! Implements the observable SSA contract that kubectl (`--server-side`) and
//! ArgoCD depend on:
//!
//! * `metadata.managedFields` entries are produced, stored and returned, one
//!   per (manager, operation, apiVersion, subresource) tuple, each carrying a
//!   `fieldsV1` ownership set.
//! * An apply that *drops* a field it previously owned removes that field from
//!   the live object (the central thing plain strategic-merge gets wrong).
//! * Fields owned by another manager that the apply would change are reported
//!   as conflicts (HTTP 409) unless `?force=true`, in which case ownership is
//!   transferred.
//!
//! The design is object-oriented around three types:
//!
//! * [`PathElem`]/[`FieldPath`] — one addressable location in the object.
//! * [`FieldSet`] — the set of paths a manager owns; knows how to extract
//!   itself from an object, (de)serialize to `fieldsV1`, diff, and resolve
//!   values. All path-set arithmetic lives here so callers never duplicate it.
//! * [`ManagedFields`] — the `metadata.managedFields` list as an addressable
//!   collection of per-manager [`FieldSet`]s.
//!
//! Because klights has no per-kind OpenAPI type system, list element
//! associativity is resolved through the same merge-key table the
//! strategic-merge path uses ([`crate::api::helpers::strategic_merge_key`]);
//! lists without a merge key are atomic (owned as a unit) and pure-scalar lists
//! are sets. This matches upstream behaviour for the kinds klights serves.

use crate::api::helpers::strategic_merge_key;
use serde_json::{Map, Value};
use std::collections::HashSet;

const DEFAULT_FIELD_MANAGER: &str = "kubectl";

/// One element of a managed-field path.
#[derive(Clone, Debug, PartialEq)]
enum PathElem {
    /// A struct/map field, serialized `f:<name>`.
    Field(String),
    /// An associative-list element selected by its merge key(s), `k:<json>`.
    Key(Value),
    /// A set element selected by its scalar value, `v:<json>`.
    Val(Value),
    /// The membership marker `.` — "the node at this path is owned".
    SelfMarker,
}

impl PathElem {
    /// `fieldsV1` key encoding for this element.
    fn encode(&self) -> String {
        match self {
            PathElem::Field(s) => format!("f:{s}"),
            PathElem::Key(v) => format!("k:{}", serde_json::to_string(v).unwrap_or_default()),
            PathElem::Val(v) => format!("v:{}", serde_json::to_string(v).unwrap_or_default()),
            PathElem::SelfMarker => ".".to_string(),
        }
    }

    /// Parse a `fieldsV1` key. Returns `None` for forms klights does not track
    /// (`i:<index>` atomic-by-index and anything unrecognised).
    fn decode(key: &str) -> Option<PathElem> {
        if key == "." {
            Some(PathElem::SelfMarker)
        } else if let Some(rest) = key.strip_prefix("f:") {
            Some(PathElem::Field(rest.to_string()))
        } else if let Some(rest) = key.strip_prefix("k:") {
            serde_json::from_str(rest).ok().map(PathElem::Key)
        } else if let Some(rest) = key.strip_prefix("v:") {
            serde_json::from_str(rest).ok().map(PathElem::Val)
        } else {
            None
        }
    }
}

/// An ordered sequence of [`PathElem`]s addressing one location in an object.
#[derive(Clone, Debug, PartialEq)]
struct FieldPath(Vec<PathElem>);

impl FieldPath {
    fn root() -> Self {
        FieldPath(Vec::new())
    }

    fn child(&self, elem: PathElem) -> Self {
        let mut v = self.0.clone();
        v.push(elem);
        FieldPath(v)
    }

    /// Canonical comparable key (used for set membership / equality).
    fn key(&self) -> String {
        self.0
            .iter()
            .map(PathElem::encode)
            .collect::<Vec<_>>()
            .join("\u{1f}")
    }

    /// True when the path addresses a concrete scalar value (a `Field` leaf) —
    /// the only paths value-conflict detection runs on.
    fn is_value_leaf(&self) -> bool {
        matches!(self.0.last(), Some(PathElem::Field(_)))
    }

    /// Navigate `obj` by this path and return the referenced value.
    fn resolve<'a>(&self, obj: &'a Value) -> Option<&'a Value> {
        let mut cur = obj;
        for elem in &self.0 {
            match elem {
                PathElem::Field(f) => cur = cur.get(f)?,
                PathElem::Key(km) => cur = cur.as_array()?.iter().find(|e| key_matches(e, km))?,
                PathElem::Val(v) => cur = cur.as_array()?.iter().find(|e| *e == v)?,
                PathElem::SelfMarker => return Some(cur),
            }
        }
        Some(cur)
    }

    fn resolve_mut<'a>(&self, obj: &'a mut Value) -> Option<&'a mut Value> {
        let mut cur = obj;
        for elem in &self.0 {
            match elem {
                PathElem::Field(f) => cur = cur.as_object_mut()?.get_mut(f)?,
                PathElem::Key(km) => {
                    cur = cur
                        .as_array_mut()?
                        .iter_mut()
                        .find(|e| key_matches(e, km))?
                }
                PathElem::Val(v) => cur = cur.as_array_mut()?.iter_mut().find(|e| &**e == v)?,
                PathElem::SelfMarker => return Some(cur),
            }
        }
        Some(cur)
    }

    /// Remove the value this path addresses from `obj`, if present.
    fn remove_from(&self, obj: &mut Value) {
        let Some(last) = self.0.last() else {
            return;
        };
        let parent = FieldPath(self.0[..self.0.len() - 1].to_vec());
        match last {
            PathElem::Field(f) => {
                if let Some(p) = parent.resolve_mut(obj)
                    && let Some(map) = p.as_object_mut()
                {
                    map.remove(f);
                }
            }
            PathElem::Val(v) => {
                if let Some(p) = parent.resolve_mut(obj)
                    && let Some(arr) = p.as_array_mut()
                {
                    arr.retain(|e| e != v);
                }
            }
            PathElem::SelfMarker => {
                // Element/membership removal. Only keyed-list elements are
                // deleted (the whole element); granular-map memberships are left
                // in place (an emptied map is harmless and avoids dropping a
                // field a user legitimately set to `{}`).
                if self.0.len() >= 2
                    && let PathElem::Key(km) = &self.0[self.0.len() - 2]
                {
                    let grandparent = FieldPath(self.0[..self.0.len() - 2].to_vec());
                    if let Some(p) = grandparent.resolve_mut(obj)
                        && let Some(arr) = p.as_array_mut()
                    {
                        arr.retain(|e| !key_matches(e, km));
                    }
                }
            }
            PathElem::Key(_) => {}
        }
    }

    /// Dotted human-readable rendering for conflict messages, e.g.
    /// `.spec.containers[name="nginx"].image`.
    fn display(&self) -> String {
        let mut s = String::new();
        for elem in &self.0 {
            match elem {
                PathElem::Field(f) => {
                    s.push('.');
                    s.push_str(f);
                }
                PathElem::Key(km) => {
                    if let Some(o) = km.as_object() {
                        let inner: Vec<String> =
                            o.iter().map(|(k, v)| format!("{k}={v}")).collect();
                        s.push_str(&format!("[{}]", inner.join(",")));
                    }
                }
                PathElem::Val(v) => s.push_str(&format!("[{v}]")),
                PathElem::SelfMarker => {}
            }
        }
        s
    }
}

/// The set of object paths a single manager owns.
#[derive(Clone, Debug, Default)]
struct FieldSet {
    paths: Vec<FieldPath>,
}

impl FieldSet {
    /// Extract the ownership set implied by an applied configuration object.
    fn from_object(obj: &Value) -> Self {
        let mut set = FieldSet::default();
        set.walk(obj, &FieldPath::root(), "");
        set
    }

    /// Parse a `fieldsV1` object into an ownership set.
    fn from_v1(v1: &Value) -> Self {
        fn walk(node: &Value, prefix: &FieldPath, out: &mut Vec<FieldPath>) {
            let Some(m) = node.as_object() else { return };
            if m.is_empty() {
                out.push(prefix.clone());
                return;
            }
            for (k, child) in m {
                let Some(elem) = PathElem::decode(k) else {
                    continue;
                };
                let p = prefix.child(elem);
                if child.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                    out.push(p);
                } else {
                    walk(child, &p, out);
                }
            }
        }
        let mut paths = Vec::new();
        walk(v1, &FieldPath::root(), &mut paths);
        FieldSet { paths }
    }

    /// Serialize this set into the K8s `fieldsV1` nested object.
    fn to_v1(&self) -> Value {
        let mut root = Map::new();
        for path in &self.paths {
            let mut node = &mut root;
            for elem in &path.0 {
                let entry = node
                    .entry(elem.encode())
                    .or_insert_with(|| Value::Object(Map::new()));
                if !entry.is_object() {
                    *entry = Value::Object(Map::new());
                }
                node = entry.as_object_mut().unwrap();
            }
        }
        Value::Object(root)
    }

    fn keys(&self) -> HashSet<String> {
        self.paths.iter().map(FieldPath::key).collect()
    }

    fn contains_key(&self, key: &str) -> bool {
        self.paths.iter().any(|p| p.key() == key)
    }

    /// Paths in `self` not present in `other` — the fields given up between two
    /// applies by the same manager.
    fn difference(&self, other_keys: &HashSet<String>) -> Vec<FieldPath> {
        self.paths
            .iter()
            .filter(|p| !other_keys.contains(&p.key()))
            .cloned()
            .collect()
    }

    /// Drop every path whose key is in `keys`, returning a new set.
    fn without_keys(&self, keys: &HashSet<String>) -> FieldSet {
        FieldSet {
            paths: self
                .paths
                .iter()
                .filter(|p| !keys.contains(&p.key()))
                .cloned()
                .collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    fn push_self(&mut self, prefix: &FieldPath) {
        self.paths.push(prefix.child(PathElem::SelfMarker));
    }

    /// Recursive structural walk shared by [`from_object`].
    fn walk(&mut self, value: &Value, prefix: &FieldPath, field_path: &str) {
        match value {
            Value::Object(m) => self.walk_object(m, prefix, field_path),
            Value::Array(a) => self.walk_array(a, prefix, field_path),
            _ => self.paths.push(prefix.clone()),
        }
    }

    fn walk_object(&mut self, m: &Map<String, Value>, prefix: &FieldPath, field_path: &str) {
        if m.is_empty() {
            self.push_self(prefix);
            return;
        }
        if is_granular_map(m) && !field_path.is_empty() {
            // labels/annotations/data-style: membership + per-key leaves.
            self.push_self(prefix);
            for k in m.keys() {
                self.paths.push(prefix.child(PathElem::Field(k.clone())));
            }
            return;
        }
        for (k, v) in m {
            if is_ignored_field(field_path, k) {
                continue;
            }
            let child = prefix.child(PathElem::Field(k.clone()));
            let fp = if field_path.is_empty() {
                k.clone()
            } else {
                format!("{field_path}.{k}")
            };
            self.walk(v, &child, &fp);
        }
    }

    fn walk_array(&mut self, a: &[Value], prefix: &FieldPath, field_path: &str) {
        if let Some(key) = strategic_merge_key(field_path) {
            for elem in a {
                let Some(km) = key_map(elem, key) else {
                    // Element missing its merge key ⇒ treat the list as atomic.
                    self.paths.push(prefix.clone());
                    return;
                };
                let child = prefix.child(PathElem::Key(km));
                self.push_self(&child);
                self.walk(elem, &child, field_path);
            }
        } else if a.iter().all(is_scalar) {
            for elem in a {
                self.paths.push(prefix.child(PathElem::Val(elem.clone())));
            }
        } else {
            self.paths.push(prefix.clone());
        }
    }
}

/// One conflict: the dotted field path and the manager that currently owns it.
#[derive(Clone, Debug, PartialEq)]
pub struct Conflict {
    pub field: String,
    pub manager: String,
}

/// Conflicts detected during a non-forced apply.
#[derive(Clone, Debug)]
pub struct SsaConflicts {
    pub conflicts: Vec<Conflict>,
}

impl SsaConflicts {
    /// Upstream-shaped conflict message used for the 409 Status.
    pub fn message(&self) -> String {
        let n = self.conflicts.len();
        let mut grouped: std::collections::BTreeMap<String, Vec<String>> = Default::default();
        for c in &self.conflicts {
            grouped
                .entry(c.manager.clone())
                .or_default()
                .push(c.field.clone());
        }
        let parts: Vec<String> = grouped
            .iter()
            .map(|(manager, fields)| {
                let mut lines: Vec<String> = fields.iter().map(|f| format!("- {f}")).collect();
                lines.sort();
                format!("conflicts with \"{manager}\":\n{}", lines.join("\n"))
            })
            .collect();
        format!(
            "Apply failed with {n} conflict{}: {}",
            if n == 1 { "" } else { "s" },
            parts.join("\n")
        )
    }
}

/// One manager's ownership entry as read from `metadata.managedFields`.
struct ManagerEntry {
    manager: String,
    set: FieldSet,
}

/// The `metadata.managedFields` list of a live object, scoped to one
/// subresource, parsed into addressable per-manager [`FieldSet`]s.
struct ManagedFields {
    entries: Vec<ManagerEntry>,
}

impl ManagedFields {
    fn read(live: &Value, subresource: Option<&str>) -> Self {
        let entries = live
            .get("metadata")
            .and_then(|m| m.get("managedFields"))
            .and_then(|f| f.as_array())
            .map(|arr| {
                arr.iter()
                    .filter(|e| e.get("subresource").and_then(|s| s.as_str()) == subresource)
                    .filter_map(|e| {
                        Some(ManagerEntry {
                            manager: e.get("manager").and_then(|m| m.as_str())?.to_string(),
                            set: FieldSet::from_v1(e.get("fieldsV1")?),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        ManagedFields { entries }
    }

    fn owner_of(&self, manager: &str) -> Option<&FieldSet> {
        self.entries
            .iter()
            .find(|e| e.manager == manager)
            .map(|e| &e.set)
    }

    /// Union of keys owned by every manager other than `manager`.
    fn keys_owned_by_others(&self, manager: &str) -> HashSet<String> {
        self.entries
            .iter()
            .filter(|e| e.manager != manager)
            .flat_map(|e| e.set.keys())
            .collect()
    }
}

/// Resolve the manager name for an apply request, defaulting lenient clients.
pub fn resolve_field_manager(field_manager: Option<&str>) -> String {
    match field_manager {
        Some(m) if !m.trim().is_empty() => m.trim().to_string(),
        _ => DEFAULT_FIELD_MANAGER.to_string(),
    }
}

fn is_scalar(v: &Value) -> bool {
    matches!(
        v,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
    )
}

/// A "granular" map (labels/annotations/data-style) — every value is a scalar.
fn is_granular_map(m: &Map<String, Value>) -> bool {
    !m.is_empty() && m.values().all(is_scalar)
}

/// Fields never tracked in managedFields (server identity / type info).
fn is_ignored_field(field_path: &str, key: &str) -> bool {
    match field_path {
        "" => matches!(key, "apiVersion" | "kind"),
        "metadata" => matches!(
            key,
            "name"
                | "namespace"
                | "generateName"
                | "uid"
                | "resourceVersion"
                | "generation"
                | "creationTimestamp"
                | "deletionTimestamp"
                | "deletionGracePeriodSeconds"
                | "selfLink"
                | "managedFields"
        ),
        _ => false,
    }
}

/// Build the merge-key map for `elem` (e.g. `{"name":"nginx"}`), or None when
/// the element is missing its key (the list is then treated as atomic).
fn key_map(elem: &Value, key: &str) -> Option<Value> {
    let v = elem.get(key)?;
    let mut m = Map::new();
    m.insert(key.to_string(), v.clone());
    Some(Value::Object(m))
}

fn key_matches(elem: &Value, km: &Value) -> bool {
    match (elem.as_object(), km.as_object()) {
        (Some(eo), Some(ko)) => ko.iter().all(|(k, v)| eo.get(k) == Some(v)),
        _ => false,
    }
}

/// Perform a server-side apply.
///
/// * `live` — current stored object, or `None` for apply-create.
/// * `applied` — the apply configuration the client sent.
/// * `manager` — resolved field-manager name.
/// * `api_version` — apiVersion of the request (recorded in the entry).
/// * `now` — RFC3339 timestamp for the entry's `time`.
/// * `force` — take ownership of conflicting fields instead of erroring.
///
/// Returns the merged object with an updated `metadata.managedFields`, or the
/// set of conflicts when `force` is false.
pub fn server_side_apply(
    live: Option<&Value>,
    applied: &Value,
    manager: &str,
    api_version: &str,
    now: &str,
    force: bool,
) -> Result<Value, SsaConflicts> {
    let subresource: Option<&str> = None;
    let empty = Value::Object(Map::new());
    let base = live.unwrap_or(&empty);

    let new_set = FieldSet::from_object(applied);
    let owners = ManagedFields::read(base, subresource);

    // 1. Conflict detection: scalar fields this apply would change that another
    //    manager owns. Equal values ⇒ co-ownership, never a conflict.
    let mut conflicts: Vec<Conflict> = Vec::new();
    let mut forced_away: HashSet<String> = HashSet::new();
    for path in new_set.paths.iter().filter(|p| p.is_value_leaf()) {
        if path.resolve(applied) == path.resolve(base) {
            continue;
        }
        let pk = path.key();
        for owner in &owners.entries {
            if owner.manager == manager || !owner.set.contains_key(&pk) {
                continue;
            }
            if force {
                forced_away.insert(pk.clone());
            } else {
                conflicts.push(Conflict {
                    field: path.display(),
                    manager: owner.manager.clone(),
                });
            }
        }
    }
    if !conflicts.is_empty() {
        conflicts.sort_by(|a, b| {
            (a.manager.as_str(), a.field.as_str()).cmp(&(b.manager.as_str(), b.field.as_str()))
        });
        conflicts.dedup();
        return Err(SsaConflicts { conflicts });
    }

    // 2. Merge applied over live (keeps server-managed fields the config omits).
    let mut merged = crate::api::helpers::strategic_merge(base.clone(), applied, "");

    // 3. Remove fields this manager previously owned but no longer applies,
    //    provided no other manager still owns them. Longest paths first.
    let new_keys = new_set.keys();
    let other_keys = owners.keys_owned_by_others(manager);
    if let Some(old_set) = owners.owner_of(manager) {
        let mut to_remove = old_set.difference(&new_keys);
        to_remove.retain(|p| !other_keys.contains(&p.key()));
        to_remove.sort_by_key(|p| std::cmp::Reverse(p.0.len()));
        for path in &to_remove {
            path.remove_from(&mut merged);
        }
    }

    // 4. Rebuild managedFields.
    let entries = rebuild_managed_fields(
        base,
        subresource,
        manager,
        api_version,
        now,
        &new_set,
        &forced_away,
    );
    if let Some(meta) = merged.as_object_mut().and_then(|o| {
        o.entry("metadata")
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
    }) {
        meta.insert("managedFields".to_string(), Value::Array(entries));
    }

    Ok(merged)
}

/// Compose the new `metadata.managedFields` array: our fresh Apply entry, other
/// managers with forced-away fields stripped (empty entries dropped), and any
/// entries from other subresources preserved verbatim.
fn rebuild_managed_fields(
    base: &Value,
    subresource: Option<&str>,
    manager: &str,
    api_version: &str,
    now: &str,
    new_set: &FieldSet,
    forced_away: &HashSet<String>,
) -> Vec<Value> {
    let mut entries: Vec<Value> = Vec::new();
    if let Some(existing) = base
        .get("metadata")
        .and_then(|m| m.get("managedFields"))
        .and_then(|f| f.as_array())
    {
        for e in existing {
            let e_sub = e.get("subresource").and_then(|s| s.as_str());
            if e_sub != subresource {
                entries.push(e.clone());
                continue;
            }
            let is_ours = e.get("manager").and_then(|m| m.as_str()) == Some(manager)
                && e.get("operation").and_then(|o| o.as_str()) == Some("Apply");
            if is_ours {
                continue; // replaced by our fresh entry below
            }
            if forced_away.is_empty() {
                entries.push(e.clone());
                continue;
            }
            // Strip forced-away fields from this manager's set.
            let Some(v1) = e.get("fieldsV1") else {
                entries.push(e.clone());
                continue;
            };
            let remaining = FieldSet::from_v1(v1).without_keys(forced_away);
            if remaining.is_empty() {
                continue;
            }
            let mut e2 = e.clone();
            if let Some(o) = e2.as_object_mut() {
                o.insert("fieldsV1".to_string(), remaining.to_v1());
            }
            entries.push(e2);
        }
    }

    let mut our_entry = serde_json::json!({
        "manager": manager,
        "operation": "Apply",
        "apiVersion": api_version,
        "time": now,
        "fieldsType": "FieldsV1",
        "fieldsV1": new_set.to_v1(),
    });
    if let Some(sub) = subresource
        && let Some(o) = our_entry.as_object_mut()
    {
        o.insert("subresource".to_string(), Value::String(sub.to_string()));
    }
    entries.push(our_entry);

    entries.sort_by_key(managed_sort_key);
    entries
}

fn managed_sort_key(e: &Value) -> (String, String) {
    (
        e.get("manager")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        e.get("operation")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn managed(obj: &Value) -> &Vec<Value> {
        obj["metadata"]["managedFields"].as_array().unwrap()
    }

    #[test]
    fn fieldsv1_roundtrip_scalar_map_and_keyed_list() {
        let applied = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "cm", "labels": {"app": "x"}},
            "data": {"a": "1", "b": "2"},
        });
        let set = FieldSet::from_object(&applied);
        let v1 = set.to_v1();
        // Round-trips back to the same path set.
        let mut a: Vec<String> = set.keys().into_iter().collect();
        let mut b: Vec<String> = FieldSet::from_v1(&v1).keys().into_iter().collect();
        a.sort();
        b.sort();
        assert_eq!(a, b);
        // apiVersion/kind/metadata.name are not tracked.
        assert!(v1.get("f:apiVersion").is_none());
        assert!(v1.get("f:kind").is_none());
        assert!(v1["f:metadata"].get("f:name").is_none());
        // granular maps carry a membership marker plus per-key entries.
        assert!(v1["f:data"].get(".").is_some());
        assert!(v1["f:data"].get("f:a").is_some());
        assert!(v1["f:metadata"]["f:labels"].get(".").is_some());
    }

    #[test]
    fn apply_create_sets_managed_fields_apply_entry() {
        let applied = json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "cm"},
            "data": {"a": "1"},
        });
        let merged = server_side_apply(
            None,
            &applied,
            "kubectl",
            "v1",
            "2026-01-01T00:00:00Z",
            false,
        )
        .unwrap();
        let mf = managed(&merged);
        assert_eq!(mf.len(), 1);
        assert_eq!(mf[0]["manager"], "kubectl");
        assert_eq!(mf[0]["operation"], "Apply");
        assert_eq!(mf[0]["apiVersion"], "v1");
        assert_eq!(mf[0]["fieldsType"], "FieldsV1");
        assert_eq!(merged["data"]["a"], "1");
        // SSA must not write the client-side last-applied-configuration marker.
        assert!(
            merged["metadata"]["annotations"]
                .get("kubectl.kubernetes.io/last-applied-configuration")
                .is_none()
        );
    }

    #[test]
    fn apply_removes_field_no_longer_in_config() {
        let first = json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "cm"},
            "data": {"a": "1", "b": "2"},
        });
        let live = server_side_apply(None, &first, "mgr", "v1", "t0", false).unwrap();
        let second = json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "cm"},
            "data": {"a": "1"},
        });
        let merged = server_side_apply(Some(&live), &second, "mgr", "v1", "t1", false).unwrap();
        assert_eq!(merged["data"]["a"], "1");
        assert!(
            merged["data"].get("b").is_none(),
            "field dropped from apply config must be removed: {}",
            merged["data"]
        );
    }

    #[test]
    fn two_managers_conflict_without_force() {
        let a = json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "cm"},
            "data": {"key": "from-a"},
        });
        let live = server_side_apply(None, &a, "mgr-a", "v1", "t0", false).unwrap();
        let b = json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "cm"},
            "data": {"key": "from-b"},
        });
        let err = server_side_apply(Some(&live), &b, "mgr-b", "v1", "t1", false).unwrap_err();
        assert_eq!(err.conflicts.len(), 1);
        assert_eq!(err.conflicts[0].manager, "mgr-a");
        assert!(err.conflicts[0].field.contains("data"));
        assert!(err.message().contains("conflicts with \"mgr-a\""));
    }

    #[test]
    fn force_resolves_conflict_and_transfers_ownership() {
        let a = json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "cm"},
            "data": {"key": "from-a"},
        });
        let live = server_side_apply(None, &a, "mgr-a", "v1", "t0", false).unwrap();
        let b = json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "cm"},
            "data": {"key": "from-b"},
        });
        let merged = server_side_apply(Some(&live), &b, "mgr-b", "v1", "t1", true).unwrap();
        assert_eq!(merged["data"]["key"], "from-b");
        // mgr-a must have lost ownership of data.key.
        let mf = managed(&merged);
        let a_entry = mf.iter().find(|e| e["manager"] == "mgr-a");
        let owns_key = a_entry
            .and_then(|e| e["fieldsV1"]["f:data"].get("f:key"))
            .is_some();
        assert!(!owns_key, "mgr-a should no longer own data.key after force");
        let b_entry = mf.iter().find(|e| e["manager"] == "mgr-b").unwrap();
        assert!(b_entry["fieldsV1"]["f:data"].get("f:key").is_some());
    }

    #[test]
    fn reapply_same_value_is_idempotent_no_conflict() {
        let a = json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "cm"},
            "data": {"key": "v"},
        });
        let live = server_side_apply(None, &a, "mgr-a", "v1", "t0", false).unwrap();
        let same = a.clone();
        let merged = server_side_apply(Some(&live), &same, "mgr-b", "v1", "t1", false).unwrap();
        assert_eq!(merged["data"]["key"], "v");
    }

    #[test]
    fn keyed_list_element_merge_and_drop() {
        let first = json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p"},
            "spec": {"containers": [
                {"name": "main", "image": "a"},
                {"name": "side", "image": "b"},
            ]},
        });
        let live = server_side_apply(None, &first, "mgr", "v1", "t0", false).unwrap();
        let second = json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "p"},
            "spec": {"containers": [
                {"name": "main", "image": "a2"},
            ]},
        });
        let merged = server_side_apply(Some(&live), &second, "mgr", "v1", "t1", false).unwrap();
        let containers = merged["spec"]["containers"].as_array().unwrap();
        assert_eq!(containers.len(), 1, "side container should be dropped");
        assert_eq!(containers[0]["name"], "main");
        assert_eq!(containers[0]["image"], "a2");
    }

    #[test]
    fn apply_preserves_server_managed_status() {
        let live = json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "cm", "uid": "abc", "resourceVersion": "5"},
            "data": {"a": "1"},
        });
        let applied = json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": "cm"},
            "data": {"a": "2"},
        });
        let merged = server_side_apply(Some(&live), &applied, "mgr", "v1", "t1", false).unwrap();
        assert_eq!(merged["metadata"]["uid"], "abc");
        assert_eq!(merged["data"]["a"], "2");
    }
}
