//! Pure RBAC PolicyRule evaluator.
//!
//! Matches authorization requests against PolicyRule structs without any
//! datastore, network, filesystem, supervisor, or clock dependency.

/// A single Kubernetes PolicyRule (from Role or ClusterRole).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyRule {
    pub verbs: Vec<String>,
    pub api_groups: Vec<String>,
    pub resources: Vec<String>,
    pub resource_names: Vec<String>,
    pub non_resource_urls: Vec<String>,
}

/// A subject that can be bound to a role.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subject {
    pub kind: SubjectKind,
    pub name: String,
    pub namespace: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubjectKind {
    User,
    Group,
    ServiceAccount,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuleMatchRequest<'a> {
    pub verb: &'a str,
    pub api_group: Option<&'a str>,
    pub resource: Option<&'a str>,
    pub subresource: Option<&'a str>,
    pub resource_name: Option<&'a str>,
    pub non_resource_url: Option<&'a str>,
    pub field_selector: Option<&'a str>,
}

/// Check if a PolicyRule matches the given request attributes.
///
/// This is the core Kubernetes RBAC matching algorithm. Returns true if
/// the rule grants the requested access.
///
/// `field_selector` is required for `list`/`watch` requests that use
/// `resourceNames`: Kubernetes requires a `metadata.name=<name>` field
/// selector that matches one of the allowed names.
pub fn rule_matches(rule: &PolicyRule, request: RuleMatchRequest<'_>) -> bool {
    let RuleMatchRequest {
        verb,
        api_group,
        resource,
        subresource,
        resource_name,
        non_resource_url,
        field_selector,
    } = request;

    // Malformed rules should never grant access.
    // Kubernetes expects a rule to target either resources or non-resource
    // URLs, never both.
    let has_resources = !rule.resources.is_empty();
    let has_non_resource_urls = !rule.non_resource_urls.is_empty();
    if has_resources == has_non_resource_urls {
        return false;
    }

    // Non-resource URL matching
    if let Some(url) = non_resource_url {
        if !has_non_resource_urls {
            return false;
        }
        if !matches_verb(&rule.verbs, verb) {
            return false;
        }
        return matches_non_resource_url(&rule.non_resource_urls, url);
    }

    // Resource matching
    if resource.is_none() {
        return false;
    }
    if !has_resources {
        return false;
    }
    if !matches_verb(&rule.verbs, verb) {
        return false;
    }
    if !matches_api_group(&rule.api_groups, api_group.unwrap_or("")) {
        return false;
    }

    let full_resource = match subresource {
        Some(sr) => format!("{}/{}", resource.unwrap(), sr),
        None => resource.unwrap().to_string(),
    };

    if !matches_resource(&rule.resources, &full_resource) {
        return false;
    }

    // resourceNames: if the rule specifies names, the request must match one.
    // - create and deletecollection are NOT authorized by resourceNames.
    // - list/watch require a fieldSelector containing `metadata.name=<name>`
    //   for one of the allowed names.
    if !rule.resource_names.is_empty() {
        if verb == "create" || verb == "deletecollection" {
            return false;
        }

        // list/watch with resourceNames: must have fieldSelector=metadata.name=<name>
        if verb == "list" || verb == "watch" {
            return field_selector_matches_resource_names(
                field_selector.unwrap_or(""),
                &rule.resource_names,
            );
        }

        let Some(req_name) = resource_name else {
            return false;
        };
        if !rule
            .resource_names
            .iter()
            .any(|n| n == "*" || n == req_name)
        {
            return false;
        }
    }

    true
}

/// Check if a subject matches an authenticated identity.
pub fn subject_matches(subject: &Subject, username: &str, groups: &[String]) -> bool {
    match subject.kind {
        // Kubernetes RBAC does not support wildcards in subjects: a User/Group
        // subject named "*" matches only the literal name "*", never every user
        // or every group. Treating it as a wildcard would let anyone allowed to
        // bind a role silently grant it to all authenticated users.
        SubjectKind::User => subject.name == username,
        SubjectKind::Group => groups.iter().any(|g| g == &subject.name),
        SubjectKind::ServiceAccount => {
            let expected = match &subject.namespace {
                Some(ns) => format!("system:serviceaccount:{ns}:{}", subject.name),
                None => return false,
            };
            expected == username
        }
    }
}

/// A single-dimension policy rule produced by [`breakdown_rule`]. Kubernetes
/// RBAC escalation ("Covers") semantics operate one tuple at a time.
struct AtomicRule {
    verb: String,
    api_group: Option<String>,
    resource: Option<String>,
    /// `None` means "all names" (the requested rule had no `resourceNames`).
    resource_name: Option<String>,
    non_resource_url: Option<String>,
}

/// Returns true when `holder` grants at least everything `requested` grants.
///
/// This implements the Kubernetes RBAC privilege-escalation check
/// (`rbacregistryvalidation.Covers`): the `requested` rule is decomposed into
/// atomic (one verb / group / resource / resourceName, or one verb / URL)
/// units, and every unit must be covered by some rule the holder already has.
/// Used to prevent a user from authoring a Role/ClusterRole (or binding to a
/// role) that grants more than the user themselves hold.
pub fn rules_cover(holder: &[PolicyRule], requested: &PolicyRule) -> bool {
    breakdown_rule(requested)
        .iter()
        .all(|atom| holder.iter().any(|owner| atomic_rule_covered(owner, atom)))
}

/// Returns true when `holder` covers every rule in `requested`.
pub fn rules_cover_all(holder: &[PolicyRule], requested: &[PolicyRule]) -> bool {
    requested.iter().all(|rule| rules_cover(holder, rule))
}

fn breakdown_rule(rule: &PolicyRule) -> Vec<AtomicRule> {
    let mut atoms = Vec::new();
    if !rule.resources.is_empty() {
        for verb in &rule.verbs {
            for group in &rule.api_groups {
                for resource in &rule.resources {
                    if rule.resource_names.is_empty() {
                        atoms.push(AtomicRule {
                            verb: verb.clone(),
                            api_group: Some(group.clone()),
                            resource: Some(resource.clone()),
                            resource_name: None,
                            non_resource_url: None,
                        });
                    } else {
                        for name in &rule.resource_names {
                            atoms.push(AtomicRule {
                                verb: verb.clone(),
                                api_group: Some(group.clone()),
                                resource: Some(resource.clone()),
                                resource_name: Some(name.clone()),
                                non_resource_url: None,
                            });
                        }
                    }
                }
            }
        }
    }
    if !rule.non_resource_urls.is_empty() {
        for verb in &rule.verbs {
            for url in &rule.non_resource_urls {
                atoms.push(AtomicRule {
                    verb: verb.clone(),
                    api_group: None,
                    resource: None,
                    resource_name: None,
                    non_resource_url: Some(url.clone()),
                });
            }
        }
    }
    atoms
}

fn atomic_rule_covered(owner: &PolicyRule, atom: &AtomicRule) -> bool {
    if let Some(url) = &atom.non_resource_url {
        return !owner.non_resource_urls.is_empty()
            && matches_verb(&owner.verbs, &atom.verb)
            && matches_non_resource_url(&owner.non_resource_urls, url);
    }
    if owner.resources.is_empty() {
        return false;
    }
    matches_verb(&owner.verbs, &atom.verb)
        && matches_api_group(&owner.api_groups, atom.api_group.as_deref().unwrap_or(""))
        && matches_resource(&owner.resources, atom.resource.as_deref().unwrap_or(""))
        && owner_covers_resource_name(owner, atom.resource_name.as_deref())
}

fn owner_covers_resource_name(owner: &PolicyRule, requested_name: Option<&str>) -> bool {
    if owner.resource_names.is_empty() {
        // No resourceNames restriction on the owner rule → it grants all names.
        return true;
    }
    match requested_name {
        // The requested rule grants ALL names but the owner is restricted to a
        // subset → not covered.
        None => false,
        Some(name) => owner.resource_names.iter().any(|n| n == "*" || n == name),
    }
}

fn matches_verb(verbs: &[String], verb: &str) -> bool {
    verbs.iter().any(|v| v == "*" || v == verb)
}

fn matches_api_group(groups: &[String], group: &str) -> bool {
    groups.iter().any(|g| g == "*" || g == group)
}

fn matches_resource(resources: &[String], resource: &str) -> bool {
    resources.iter().any(|r| {
        if r == "*" {
            return true;
        }
        if let Some(prefix) = r.strip_suffix('*') {
            return resource.starts_with(prefix);
        }
        r == resource
    })
}

/// Check if a field selector constrains to one of the allowed resource names.
///
/// Kubernetes requires that list/watch requests with `resourceNames` carry a
/// field selector like `metadata.name=<name>` matching one of the allowed names.
fn field_selector_matches_resource_names(field_selector: &str, resource_names: &[String]) -> bool {
    if field_selector.is_empty() {
        return false;
    }
    for part in field_selector.split(',') {
        let trimmed = part.trim();
        if let Some(name) = trimmed.strip_prefix("metadata.name=") {
            let name = name.trim();
            if resource_names.iter().any(|n| n == "*" || n == name) {
                return true;
            }
        }
    }
    false
}

fn matches_non_resource_url(urls: &[String], url: &str) -> bool {
    urls.iter().any(|u| {
        if u == "*" {
            return true;
        }
        if let Some(prefix) = u.strip_suffix('*') {
            return url.starts_with(prefix);
        }
        u == url
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_rule() -> PolicyRuleBuilder {
        PolicyRuleBuilder::default()
    }

    struct PolicyRuleBuilder {
        verbs: Vec<String>,
        api_groups: Vec<String>,
        resources: Vec<String>,
        resource_names: Vec<String>,
        non_resource_urls: Vec<String>,
    }

    impl Default for PolicyRuleBuilder {
        fn default() -> Self {
            Self {
                verbs: vec!["*".to_string()],
                api_groups: vec!["*".to_string()],
                resources: vec!["*".to_string()],
                resource_names: vec![],
                non_resource_urls: vec![],
            }
        }
    }

    impl PolicyRuleBuilder {
        fn verbs(mut self, v: &[&str]) -> Self {
            self.verbs = v.iter().map(|s| s.to_string()).collect();
            self
        }
        fn api_groups(mut self, g: &[&str]) -> Self {
            self.api_groups = g.iter().map(|s| s.to_string()).collect();
            self
        }
        fn resources(mut self, r: &[&str]) -> Self {
            self.resources = r.iter().map(|s| s.to_string()).collect();
            self
        }
        fn resource_names(mut self, n: &[&str]) -> Self {
            self.resource_names = n.iter().map(|s| s.to_string()).collect();
            self
        }
        fn non_resource_urls(mut self, u: &[&str]) -> Self {
            self.non_resource_urls = u.iter().map(|s| s.to_string()).collect();
            self
        }
        fn build(self) -> PolicyRule {
            PolicyRule {
                verbs: self.verbs,
                api_groups: self.api_groups,
                resources: self.resources,
                resource_names: self.resource_names,
                non_resource_urls: self.non_resource_urls,
            }
        }
    }

    struct RequestCase<'a> {
        verb: &'a str,
        api_group: Option<&'a str>,
        resource: Option<&'a str>,
        subresource: Option<&'a str>,
        resource_name: Option<&'a str>,
        non_resource_url: Option<&'a str>,
        field_selector: Option<&'a str>,
    }

    struct RuleMatchCase<'a> {
        name: &'a str,
        rule: PolicyRule,
        request: RequestCase<'a>,
        expect_allow: bool,
    }

    #[test]
    fn rule_matches_matrix() {
        let cases = [
            RuleMatchCase {
                name: "wildcard verb matches any verb",
                rule: make_rule().verbs(&["*"]).build(),
                request: RequestCase {
                    verb: "get",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: true,
            },
            RuleMatchCase {
                name: "specific verb list matches list",
                rule: make_rule().verbs(&["get", "list"]).build(),
                request: RequestCase {
                    verb: "list",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: true,
            },
            RuleMatchCase {
                name: "specific verb mismatch is denied",
                rule: make_rule().verbs(&["get", "list"]).build(),
                request: RequestCase {
                    verb: "create",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: false,
            },
            RuleMatchCase {
                name: "core API group matches empty",
                rule: make_rule().api_groups(&[""]).build(),
                request: RequestCase {
                    verb: "get",
                    api_group: Some(""),
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: true,
            },
            RuleMatchCase {
                name: "resource wildcard matches any resource",
                rule: make_rule().resources(&["*"]).build(),
                request: RequestCase {
                    verb: "get",
                    api_group: None,
                    resource: Some("secrets"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: true,
            },
            RuleMatchCase {
                name: "resource wildcard prefix does not match wrong resource",
                rule: make_rule().resources(&["pods/*"]).build(),
                request: RequestCase {
                    verb: "get",
                    api_group: None,
                    resource: Some("deployments"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: false,
            },
            RuleMatchCase {
                name: "subresource split matching",
                rule: make_rule().resources(&["pods/status"]).build(),
                request: RequestCase {
                    verb: "get",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: Some("status"),
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: true,
            },
            RuleMatchCase {
                name: "resource mismatch for exact subresource rule denied",
                rule: make_rule().resources(&["pods/status"]).build(),
                request: RequestCase {
                    verb: "get",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: false,
            },
            RuleMatchCase {
                name: "resourceName exact match",
                rule: make_rule()
                    .resources(&["pods"])
                    .resource_names(&["my-pod"])
                    .build(),
                request: RequestCase {
                    verb: "get",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: Some("my-pod"),
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: true,
            },
            RuleMatchCase {
                name: "resourceName mismatch denied",
                rule: make_rule()
                    .resources(&["pods"])
                    .resource_names(&["my-pod"])
                    .build(),
                request: RequestCase {
                    verb: "get",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: Some("other-pod"),
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: false,
            },
            RuleMatchCase {
                name: "resourceName rule does not authorize create",
                rule: make_rule()
                    .resources(&["pods"])
                    .resource_names(&["my-pod"])
                    .build(),
                request: RequestCase {
                    verb: "create",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: Some("my-pod"),
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: false,
            },
            RuleMatchCase {
                name: "resourceName rule does not authorize deletecollection",
                rule: make_rule()
                    .resources(&["pods"])
                    .resource_names(&["my-pod"])
                    .build(),
                request: RequestCase {
                    verb: "deletecollection",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: false,
            },
            RuleMatchCase {
                name: "list with matching field selector",
                rule: make_rule()
                    .resources(&["pods"])
                    .resource_names(&["my-pod"])
                    .build(),
                request: RequestCase {
                    verb: "list",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: Some("metadata.name=my-pod"),
                },
                expect_allow: true,
            },
            RuleMatchCase {
                name: "list without field selector denied",
                rule: make_rule()
                    .resources(&["pods"])
                    .resource_names(&["my-pod"])
                    .build(),
                request: RequestCase {
                    verb: "list",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: false,
            },
            RuleMatchCase {
                name: "list with non-matching field selector denied",
                rule: make_rule()
                    .resources(&["pods"])
                    .resource_names(&["my-pod"])
                    .build(),
                request: RequestCase {
                    verb: "list",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: Some("metadata.name=other-pod"),
                },
                expect_allow: false,
            },
            RuleMatchCase {
                name: "watch with matching field selector",
                rule: make_rule()
                    .resources(&["pods"])
                    .resource_names(&["my-pod"])
                    .build(),
                request: RequestCase {
                    verb: "watch",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: Some("metadata.name=my-pod"),
                },
                expect_allow: true,
            },
            RuleMatchCase {
                name: "watch without field selector denied",
                rule: make_rule()
                    .resources(&["pods"])
                    .resource_names(&["my-pod"])
                    .build(),
                request: RequestCase {
                    verb: "watch",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: false,
            },
            RuleMatchCase {
                name: "nonResource exact URL match",
                rule: make_rule()
                    .verbs(&["get"])
                    .resources(&[])
                    .non_resource_urls(&["/api/v1"])
                    .build(),
                request: RequestCase {
                    verb: "get",
                    api_group: None,
                    resource: None,
                    subresource: None,
                    resource_name: None,
                    non_resource_url: Some("/api/v1"),
                    field_selector: None,
                },
                expect_allow: true,
            },
            RuleMatchCase {
                name: "nonResource wildcard matches suffix",
                rule: make_rule()
                    .verbs(&["get"])
                    .resources(&[])
                    .non_resource_urls(&["/apis/*"])
                    .build(),
                request: RequestCase {
                    verb: "get",
                    api_group: None,
                    resource: None,
                    subresource: None,
                    resource_name: None,
                    non_resource_url: Some("/apis/rbac.authorization.k8s.io/v1"),
                    field_selector: None,
                },
                expect_allow: true,
            },
            RuleMatchCase {
                name: "nonResource wildcard requires trailing path",
                rule: make_rule()
                    .verbs(&["get"])
                    .resources(&[])
                    .non_resource_urls(&["/apis/*"])
                    .build(),
                request: RequestCase {
                    verb: "get",
                    api_group: None,
                    resource: None,
                    subresource: None,
                    resource_name: None,
                    non_resource_url: Some("/apis"),
                    field_selector: None,
                },
                expect_allow: false,
            },
            RuleMatchCase {
                name: "malformed rule with both resources and nonResourceURLs denied",
                rule: make_rule()
                    .verbs(&["get"])
                    .resources(&["pods"])
                    .non_resource_urls(&["/api"])
                    .build(),
                request: RequestCase {
                    verb: "get",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: false,
            },
            RuleMatchCase {
                name: "malformed rule with neither resources nor nonResourceURLs denied",
                rule: make_rule()
                    .verbs(&["get"])
                    .resources(&[])
                    .non_resource_urls(&[])
                    .build(),
                request: RequestCase {
                    verb: "get",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: false,
            },
            RuleMatchCase {
                name: "empty rule denied",
                rule: PolicyRule {
                    verbs: vec![],
                    api_groups: vec![],
                    resources: vec![],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                request: RequestCase {
                    verb: "get",
                    api_group: None,
                    resource: Some("pods"),
                    subresource: None,
                    resource_name: None,
                    non_resource_url: None,
                    field_selector: None,
                },
                expect_allow: false,
            },
        ];

        for case in cases {
            let actual = rule_matches(
                &case.rule,
                RuleMatchRequest {
                    verb: case.request.verb,
                    api_group: case.request.api_group,
                    resource: case.request.resource,
                    subresource: case.request.subresource,
                    resource_name: case.request.resource_name,
                    non_resource_url: case.request.non_resource_url,
                    field_selector: case.request.field_selector,
                },
            );
            assert_eq!(actual, case.expect_allow, "rule case failed: {}", case.name);
        }
    }

    #[test]
    fn rules_cover_matrix() {
        // holder, requested, expect_covered
        struct CoverCase<'a> {
            name: &'a str,
            holder: Vec<PolicyRule>,
            requested: PolicyRule,
            expect: bool,
        }
        let cases = [
            CoverCase {
                name: "wildcard holder covers anything",
                holder: vec![make_rule().build()],
                requested: make_rule()
                    .verbs(&["get", "create"])
                    .api_groups(&["apps"])
                    .resources(&["deployments"])
                    .build(),
                expect: true,
            },
            CoverCase {
                name: "exact holder covers exact request",
                holder: vec![
                    make_rule()
                        .verbs(&["get", "list"])
                        .api_groups(&[""])
                        .resources(&["pods"])
                        .build(),
                ],
                requested: make_rule()
                    .verbs(&["get"])
                    .api_groups(&[""])
                    .resources(&["pods"])
                    .build(),
                expect: true,
            },
            CoverCase {
                name: "missing verb is not covered (escalation)",
                holder: vec![
                    make_rule()
                        .verbs(&["get", "list"])
                        .api_groups(&[""])
                        .resources(&["pods"])
                        .build(),
                ],
                requested: make_rule()
                    .verbs(&["get", "delete"])
                    .api_groups(&[""])
                    .resources(&["pods"])
                    .build(),
                expect: false,
            },
            CoverCase {
                name: "missing resource is not covered (escalation to secrets)",
                holder: vec![
                    make_rule()
                        .verbs(&["*"])
                        .api_groups(&[""])
                        .resources(&["pods"])
                        .build(),
                ],
                requested: make_rule()
                    .verbs(&["get"])
                    .api_groups(&[""])
                    .resources(&["secrets"])
                    .build(),
                expect: false,
            },
            CoverCase {
                name: "wrong api group is not covered",
                holder: vec![
                    make_rule()
                        .verbs(&["*"])
                        .api_groups(&[""])
                        .resources(&["*"])
                        .build(),
                ],
                requested: make_rule()
                    .verbs(&["get"])
                    .api_groups(&["apps"])
                    .resources(&["deployments"])
                    .build(),
                expect: false,
            },
            CoverCase {
                name: "named-request covered by unrestricted holder",
                holder: vec![
                    make_rule()
                        .verbs(&["get"])
                        .api_groups(&[""])
                        .resources(&["secrets"])
                        .build(),
                ],
                requested: make_rule()
                    .verbs(&["get"])
                    .api_groups(&[""])
                    .resources(&["secrets"])
                    .resource_names(&["my-secret"])
                    .build(),
                expect: true,
            },
            CoverCase {
                name: "all-names request not covered by name-restricted holder",
                holder: vec![
                    make_rule()
                        .verbs(&["get"])
                        .api_groups(&[""])
                        .resources(&["secrets"])
                        .resource_names(&["my-secret"])
                        .build(),
                ],
                requested: make_rule()
                    .verbs(&["get"])
                    .api_groups(&[""])
                    .resources(&["secrets"])
                    .build(),
                expect: false,
            },
            CoverCase {
                name: "covered across multiple holder rules",
                holder: vec![
                    make_rule()
                        .verbs(&["get"])
                        .api_groups(&[""])
                        .resources(&["pods"])
                        .build(),
                    make_rule()
                        .verbs(&["delete"])
                        .api_groups(&[""])
                        .resources(&["pods"])
                        .build(),
                ],
                requested: make_rule()
                    .verbs(&["get", "delete"])
                    .api_groups(&[""])
                    .resources(&["pods"])
                    .build(),
                expect: true,
            },
            CoverCase {
                name: "non-resource url covered",
                holder: vec![
                    make_rule()
                        .verbs(&["get"])
                        .resources(&[])
                        .non_resource_urls(&["/healthz", "/metrics"])
                        .build(),
                ],
                requested: make_rule()
                    .verbs(&["get"])
                    .resources(&[])
                    .non_resource_urls(&["/healthz"])
                    .build(),
                expect: true,
            },
            CoverCase {
                name: "empty holder covers nothing",
                holder: vec![],
                requested: make_rule()
                    .verbs(&["get"])
                    .api_groups(&[""])
                    .resources(&["pods"])
                    .build(),
                expect: false,
            },
        ];
        for case in cases {
            assert_eq!(
                rules_cover(&case.holder, &case.requested),
                case.expect,
                "rules_cover case failed: {}",
                case.name
            );
        }
    }

    #[test]
    fn subject_matching_matrix() {
        struct SubjectCase {
            name: &'static str,
            subject: Subject,
            username: &'static str,
            groups: Vec<String>,
            expect_allow: bool,
        }

        let cases = [
            SubjectCase {
                name: "user matches exact username",
                subject: Subject {
                    kind: SubjectKind::User,
                    name: "alice".to_string(),
                    namespace: None,
                },
                username: "alice",
                groups: vec![],
                expect_allow: true,
            },
            SubjectCase {
                name: "user mismatch denied",
                subject: Subject {
                    kind: SubjectKind::User,
                    name: "alice".to_string(),
                    namespace: None,
                },
                username: "bob",
                groups: vec![],
                expect_allow: false,
            },
            SubjectCase {
                name: "group matches membership",
                subject: Subject {
                    kind: SubjectKind::Group,
                    name: "system:bootstrappers".to_string(),
                    namespace: None,
                },
                username: "any-user",
                groups: vec!["system:bootstrappers".to_string()],
                expect_allow: true,
            },
            SubjectCase {
                name: "group missing membership denied",
                subject: Subject {
                    kind: SubjectKind::Group,
                    name: "system:bootstrappers".to_string(),
                    namespace: None,
                },
                username: "any-user",
                groups: vec!["system:nodes".to_string()],
                expect_allow: false,
            },
            SubjectCase {
                name: "service account exact match",
                subject: Subject {
                    kind: SubjectKind::ServiceAccount,
                    name: "default".to_string(),
                    namespace: Some("kube-system".to_string()),
                },
                username: "system:serviceaccount:kube-system:default",
                groups: vec![],
                expect_allow: true,
            },
            SubjectCase {
                name: "service account namespace mismatch denied",
                subject: Subject {
                    kind: SubjectKind::ServiceAccount,
                    name: "default".to_string(),
                    namespace: Some("kube-system".to_string()),
                },
                username: "system:serviceaccount:default:default",
                groups: vec![],
                expect_allow: false,
            },
            // Kubernetes RBAC does NOT support wildcards in subjects. A subject
            // named "*" is a literal name, never "all users"/"all groups".
            SubjectCase {
                name: "user named * does not match an arbitrary username",
                subject: Subject {
                    kind: SubjectKind::User,
                    name: "*".to_string(),
                    namespace: None,
                },
                username: "anyone",
                groups: vec![],
                expect_allow: false,
            },
            SubjectCase {
                name: "user named * matches only the literal username *",
                subject: Subject {
                    kind: SubjectKind::User,
                    name: "*".to_string(),
                    namespace: None,
                },
                username: "*",
                groups: vec![],
                expect_allow: true,
            },
            SubjectCase {
                name: "group named * does not match an arbitrary group membership",
                subject: Subject {
                    kind: SubjectKind::Group,
                    name: "*".to_string(),
                    namespace: None,
                },
                username: "anyone",
                groups: vec!["system:authenticated".to_string()],
                expect_allow: false,
            },
            SubjectCase {
                name: "group named * matches only the literal group *",
                subject: Subject {
                    kind: SubjectKind::Group,
                    name: "*".to_string(),
                    namespace: None,
                },
                username: "anyone",
                groups: vec!["*".to_string()],
                expect_allow: true,
            },
        ];

        for case in cases {
            assert_eq!(
                subject_matches(&case.subject, case.username, &case.groups),
                case.expect_allow,
                "subject case failed: {}",
                case.name
            );
        }
    }
}
