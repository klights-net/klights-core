use crate::api::*;
use axum::http::HeaderMap;
use serde_json::Value;

pub fn validate_crd_field_selector(
    api_version: &str,
    plural: &str,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
    namespaced: bool,
    selectable_fields: &[String],
) -> Result<(), AppError> {
    let Some(selector) = field_selector else {
        return Ok(());
    };
    let selector = selector.trim();
    if selector.is_empty() {
        return Ok(());
    }

    let mut supported_fields = std::collections::HashSet::new();
    supported_fields.insert("metadata.name".to_string());
    if namespaced {
        supported_fields.insert("metadata.namespace".to_string());
    }
    for field in selectable_fields {
        supported_fields.insert(field.clone());
    }

    for part in selector.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let key = if let Some((key, _)) = part.split_once("!=") {
            key.trim()
        } else if let Some((key, _)) = part.split_once('=') {
            key.trim()
        } else {
            continue;
        };

        if !supported_fields.contains(key) {
            return Err(AppError::BadRequest(format!(
                "Unable to find \"{}, Resource={}\" that match label selector \"{}\", field selector \"{}\": field label not supported: {}",
                api_version,
                plural,
                label_selector.unwrap_or_default(),
                selector,
                key
            )));
        }
    }

    Ok(())
}

/// Standard K8s top-level fields accepted by all resource types.
static KNOWN_TOP_LEVEL_KEYS: &[&str] = &[
    "apiVersion",
    "kind",
    "metadata",
    "spec",
    "status",
    "data",
    "stringData",
    "binaryData",
    "type",
    "subsets",
    "endpoints",
    "rules",
    "roleRef",
    "subjects",
    "secrets",
    "imagePullSecrets",
    "automountServiceAccountToken",
    "involvedObject",
    "reason",
    "message",
    "source",
    "firstTimestamp",
    "lastTimestamp",
    "count",
    "reportingComponent",
    "reportingInstance",
    "action",
    "related",
    "series",
    "eventTime",
    "deprecatedFirstTimestamp",
    "deprecatedLastTimestamp",
    "deprecatedCount",
    "deprecatedSource",
    "note",
    "regarding",
    "regarding",
    "type",
    "immutable",
    "provisioner",
    "parameters",
    "reclaimPolicy",
    "allowVolumeExpansion",
    "volumeBindingMode",
    "mountOptions",
    "claimRef",
    "accessModes",
    "capacity",
    "volumeMode",
    "handler",
    "scheduling",
    "webhooks",
    "conversion",
    "versions",
    "scope",
    "names",
    "group",
    "preserved",
    "validation",
    "additionalPrinterColumns",
    "revisionHistoryLimit",
    "template",
    "selector",
    "replicas",
    "updateStrategy",
    "podManagementPolicy",
    "serviceName",
    "volumeClaimTemplates",
    "minReadySeconds",
    "jobTemplate",
    "schedule",
    "concurrencyPolicy",
    "successfulJobsHistoryLimit",
    "failedJobsHistoryLimit",
    "startingDeadlineSeconds",
    "suspend",
    "timeZone",
    "completions",
    "parallelism",
    "backoffLimit",
    "activeDeadlineSeconds",
    "ttlSecondsAfterFinished",
    "completionMode",
    "successPolicy",
    "automountServiceAccountToken",
    "nodeName",
    "nodeSelector",
    "tolerations",
    "affinity",
    "priorityClassName",
    "priority",
    "runtimeClassName",
    "schedulerName",
    "hostname",
    "subdomain",
    "hostNetwork",
    "hostPID",
    "hostIPC",
    "shareProcessNamespace",
    "restartPolicy",
    "terminationGracePeriodSeconds",
    "initContainers",
    "containers",
    "ephemeralContainers",
    "volumes",
    "dnsPolicy",
    "dnsConfig",
    "hostAliases",
    "readinessGates",
    "topologySpreadConstraints",
    "overhead",
    "setHostnameAsFQDN",
    "enableServiceLinks",
    "preemptionPolicy",
    "os",
    "clusterName",
    "generateName",
    "finalizers",
    "ownerReferences",
    "managedFields",
    "labels",
    "annotations",
    "namespace",
    "name",
    "resourceVersion",
    "uid",
    "selfLink",
    "creationTimestamp",
    "deletionTimestamp",
    "deletionGracePeriodSeconds",
    "generation",
];

/// Known valid metadata fields for all K8s resources.
static KNOWN_METADATA_KEYS: &[&str] = &[
    "name",
    "namespace",
    "generateName",
    "selfLink",
    "uid",
    "resourceVersion",
    "generation",
    "creationTimestamp",
    "deletionTimestamp",
    "deletionGracePeriodSeconds",
    "labels",
    "annotations",
    "ownerReferences",
    "finalizers",
    "managedFields",
    "clusterName",
];

/// When fieldValidation=Strict, reject requests with unknown top-level or metadata fields.
pub fn check_field_validation_strict(
    query: &CreateUpdateQuery,
    body: &Value,
) -> Result<(), AppError> {
    if query.field_validation.as_deref() != Some("Strict") {
        return Ok(());
    }
    if let Some(obj) = body.as_object() {
        for key in obj.keys() {
            if !KNOWN_TOP_LEVEL_KEYS.contains(&key.as_str()) {
                return Err(AppError::BadRequest(format!(
                    "strict decoding error: unknown field \"{}\"",
                    key
                )));
            }
        }
        // Also validate metadata fields
        if let Some(metadata) = obj.get("metadata").and_then(|m| m.as_object()) {
            for key in metadata.keys() {
                if !KNOWN_METADATA_KEYS.contains(&key.as_str()) {
                    return Err(AppError::BadRequest(format!(
                        "strict decoding error: unknown field \"metadata.{}\"",
                        key
                    )));
                }
            }
        }
    }
    Ok(())
}

fn pod_container_resources_by_name(
    pod: &Value,
    pointer: &str,
) -> std::collections::BTreeMap<String, Value> {
    let mut by_name = std::collections::BTreeMap::new();
    let Some(containers) = pod.pointer(pointer).and_then(|v| v.as_array()) else {
        return by_name;
    };
    for container in containers {
        let Some(name) = container.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let resources = container
            .get("resources")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        by_name.insert(name.to_string(), resources);
    }
    by_name
}

pub fn validate_pod_resource_requirements_immutable(
    old_pod: &Value,
    new_pod: &Value,
) -> Result<(), AppError> {
    let old_containers = pod_container_resources_by_name(old_pod, "/spec/containers");
    let new_containers = pod_container_resources_by_name(new_pod, "/spec/containers");
    if old_containers != new_containers {
        return Err(AppError::Forbidden(
            "Pod updates may not change container resource requirements".to_string(),
        ));
    }

    let old_init = pod_container_resources_by_name(old_pod, "/spec/initContainers");
    let new_init = pod_container_resources_by_name(new_pod, "/spec/initContainers");
    if old_init != new_init {
        return Err(AppError::Forbidden(
            "Pod updates may not change initContainer resource requirements".to_string(),
        ));
    }

    Ok(())
}

pub fn validate_priorityclass_update_immutable(
    current: &Value,
    updated: &Value,
) -> Result<(), AppError> {
    let name = current
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if current.get("value") != updated.get("value") {
        return Err(AppError::UnprocessableEntity(format!(
            "PriorityClass \"{}\" is invalid: value: Forbidden: may not be changed in an update.",
            name
        )));
    }

    if current.get("preemptionPolicy") != updated.get("preemptionPolicy") {
        return Err(AppError::UnprocessableEntity(format!(
            "PriorityClass \"{}\" is invalid: preemptionPolicy: Invalid value: field is immutable",
            name
        )));
    }

    Ok(())
}

pub fn validate_pod_sysctls(pod: &Value) -> Result<(), AppError> {
    let Some(sysctls) = pod
        .pointer("/spec/securityContext/sysctls")
        .and_then(|v| v.as_array())
    else {
        return Ok(());
    };

    let host_network = pod
        .pointer("/spec/hostNetwork")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let host_ipc = pod
        .pointer("/spec/hostIPC")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let mut seen = std::collections::HashSet::new();
    let mut errors = Vec::new();

    for (idx, entry) in sysctls.iter().enumerate() {
        let Some(name) = entry.get("name").and_then(|v| v.as_str()) else {
            errors.push(format!(
                "spec.securityContext.sysctls[{idx}].name: Required value"
            ));
            continue;
        };

        if !is_valid_sysctl_name(name) {
            errors.push(format!(
                "spec.securityContext.sysctls[{idx}].name: Invalid value: \"{name}\": must have at most 253 characters and match sysctl naming rules"
            ));
            continue;
        }

        if !seen.insert(name.to_string()) {
            errors.push(format!(
                "spec.securityContext.sysctls[{idx}].name: Duplicate value: \"{name}\""
            ));
            continue;
        }

        let normalized = name.replace('/', ".");
        if host_network && normalized.starts_with("net.") {
            errors.push(format!(
                "spec.securityContext.sysctls[{idx}].name: Invalid value: \"{name}\": may not be specified when 'hostNetwork' is true"
            ));
        }
        if host_ipc
            && (normalized.starts_with("kernel.shm") || normalized.starts_with("kernel.sem"))
        {
            errors.push(format!(
                "spec.securityContext.sysctls[{idx}].name: Invalid value: \"{name}\": may not be specified when 'hostIPC' is true"
            ));
        }
    }

    if !errors.is_empty() {
        return Err(AppError::UnprocessableEntity(errors.join("; ")));
    }

    Ok(())
}

fn is_valid_sysctl_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 253 {
        return false;
    }

    let mut saw_separator = false;
    for segment in name.split(['.', '/']) {
        if segment.is_empty() {
            return false;
        }
        saw_separator = true;

        let mut chars = segment.chars();
        let Some(first) = chars.next() else {
            return false;
        };
        if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
            return false;
        }

        let Some(last) = segment.chars().last() else {
            return false;
        };
        if !last.is_ascii_lowercase() && !last.is_ascii_digit() {
            return false;
        }

        if !segment
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
        {
            return false;
        }
    }

    saw_separator
}

struct JsonDupPathParser<'a> {
    input: &'a [u8],
    pos: usize,
    duplicates: Vec<String>,
}

impl<'a> JsonDupPathParser<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            pos: 0,
            duplicates: Vec::new(),
        }
    }

    fn parse(mut self) -> Result<Vec<String>, AppError> {
        self.skip_ws();
        self.parse_value(&[])?;
        self.skip_ws();
        if self.pos != self.input.len() {
            return Err(AppError::BadRequest(
                "Invalid JSON: trailing characters".to_string(),
            ));
        }
        Ok(self.duplicates)
    }

    fn parse_value(&mut self, path: &[String]) -> Result<(), AppError> {
        self.skip_ws();
        match self.peek() {
            Some(b'{') => self.parse_object(path),
            Some(b'[') => self.parse_array(path),
            Some(b'"') => {
                let _ = self.parse_string()?;
                Ok(())
            }
            Some(b'-') | Some(b'0'..=b'9') => self.parse_number(),
            Some(b't') => self.expect_bytes(b"true"),
            Some(b'f') => self.expect_bytes(b"false"),
            Some(b'n') => self.expect_bytes(b"null"),
            _ => Err(AppError::BadRequest(
                "Invalid JSON: unexpected token".to_string(),
            )),
        }
    }

    fn parse_object(&mut self, path: &[String]) -> Result<(), AppError> {
        self.expect_byte(b'{')?;
        self.skip_ws();
        if self.consume_if(b'}') {
            return Ok(());
        }

        let mut seen = std::collections::HashSet::new();
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            self.expect_byte(b':')?;

            let mut child_path = path.to_vec();
            child_path.push(key.clone());
            if !seen.insert(key) {
                self.duplicates.push(child_path.join("."));
            }

            self.parse_value(&child_path)?;
            self.skip_ws();
            if self.consume_if(b',') {
                continue;
            }
            self.expect_byte(b'}')?;
            break;
        }
        Ok(())
    }

    fn parse_array(&mut self, path: &[String]) -> Result<(), AppError> {
        self.expect_byte(b'[')?;
        self.skip_ws();
        if self.consume_if(b']') {
            return Ok(());
        }
        loop {
            self.parse_value(path)?;
            self.skip_ws();
            if self.consume_if(b',') {
                continue;
            }
            self.expect_byte(b']')?;
            break;
        }
        Ok(())
    }

    fn parse_number(&mut self) -> Result<(), AppError> {
        let start = self.pos;
        if self.consume_if(b'-') && !self.peek().is_some_and(|b| b.is_ascii_digit()) {
            return Err(AppError::BadRequest(
                "Invalid JSON: invalid number".to_string(),
            ));
        }
        if self.consume_if(b'0') {
            // leading zero handled
        } else {
            self.consume_digits();
        }
        if self.consume_if(b'.') {
            if !self.peek().is_some_and(|b| b.is_ascii_digit()) {
                return Err(AppError::BadRequest(
                    "Invalid JSON: invalid number".to_string(),
                ));
            }
            self.consume_digits();
        }
        if self.consume_if(b'e') || self.consume_if(b'E') {
            let _ = self.consume_if(b'+') || self.consume_if(b'-');
            if !self.peek().is_some_and(|b| b.is_ascii_digit()) {
                return Err(AppError::BadRequest(
                    "Invalid JSON: invalid number".to_string(),
                ));
            }
            self.consume_digits();
        }
        if self.pos == start {
            return Err(AppError::BadRequest(
                "Invalid JSON: invalid number".to_string(),
            ));
        }
        Ok(())
    }

    fn consume_digits(&mut self) {
        while self.peek().is_some_and(|b| b.is_ascii_digit()) {
            self.pos += 1;
        }
    }

    fn parse_string(&mut self) -> Result<String, AppError> {
        self.expect_byte(b'"')?;
        let mut out = String::new();
        loop {
            let b = self.next_byte().ok_or_else(|| {
                AppError::BadRequest("Invalid JSON: unterminated string".to_string())
            })?;
            match b {
                b'"' => break,
                b'\\' => {
                    let esc = self.next_byte().ok_or_else(|| {
                        AppError::BadRequest("Invalid JSON: invalid escape".to_string())
                    })?;
                    match esc {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{0008}'),
                        b'f' => out.push('\u{000C}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let cp = self.parse_hex4()?;
                            let ch = char::from_u32(cp as u32).ok_or_else(|| {
                                AppError::BadRequest(
                                    "Invalid JSON: invalid unicode escape".to_string(),
                                )
                            })?;
                            out.push(ch);
                        }
                        _ => {
                            return Err(AppError::BadRequest(
                                "Invalid JSON: invalid escape".to_string(),
                            ));
                        }
                    }
                }
                b if b < 0x20 => {
                    return Err(AppError::BadRequest(
                        "Invalid JSON: control character in string".to_string(),
                    ));
                }
                _ => out.push(b as char),
            }
        }
        Ok(out)
    }

    fn parse_hex4(&mut self) -> Result<u16, AppError> {
        let mut val: u16 = 0;
        for _ in 0..4 {
            let b = self.next_byte().ok_or_else(|| {
                AppError::BadRequest("Invalid JSON: invalid unicode escape".to_string())
            })?;
            val = (val << 4)
                | match b {
                    b'0'..=b'9' => (b - b'0') as u16,
                    b'a'..=b'f' => (b - b'a' + 10) as u16,
                    b'A'..=b'F' => (b - b'A' + 10) as u16,
                    _ => {
                        return Err(AppError::BadRequest(
                            "Invalid JSON: invalid unicode escape".to_string(),
                        ));
                    }
                };
        }
        Ok(val)
    }

    fn skip_ws(&mut self) {
        while self
            .peek()
            .is_some_and(|b| b == b' ' || b == b'\n' || b == b'\r' || b == b'\t')
        {
            self.pos += 1;
        }
    }

    fn expect_bytes(&mut self, expected: &[u8]) -> Result<(), AppError> {
        for b in expected {
            if self.next_byte() != Some(*b) {
                return Err(AppError::BadRequest(
                    "Invalid JSON: unexpected token".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn expect_byte(&mut self, expected: u8) -> Result<(), AppError> {
        if self.next_byte() == Some(expected) {
            Ok(())
        } else {
            Err(AppError::BadRequest(
                "Invalid JSON: unexpected token".to_string(),
            ))
        }
    }

    fn consume_if(&mut self, b: u8) -> bool {
        if self.peek() == Some(b) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn next_byte(&mut self) -> Option<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Some(b)
    }
}

fn parse_json_duplicate_field_paths(body: &[u8]) -> Result<Vec<String>, AppError> {
    JsonDupPathParser::new(body).parse()
}

fn normalize_path(path: &str) -> String {
    path.trim_start_matches('.')
        .replace(".?.", ".")
        .replace(".?", ".")
}

/// Strictly decode `bytes` into the k8s-openapi typed struct for
/// `(api_version, kind)`, pushing every unknown (ignored) field path into
/// `unknown`. Returns:
///   * `Some(Ok(()))`  — a typed schema exists and decoding ran (check `unknown`),
///   * `Some(Err(..))` — a typed schema exists but the JSON is malformed,
///   * `None`          — no typed schema for this kind (caller falls back).
///
/// This is the single source of truth for "does field X exist on kind Y";
/// both the raw-bytes create path and the parsed-Value create/update/patch
/// paths route through it so nested unknown fields (`spec.bogus`) are caught
/// for every built-in kind, not just at the top level.
fn typed_strict_decode(
    api_version: &str,
    kind: &str,
    bytes: &[u8],
    unknown: &mut Vec<String>,
) -> Option<Result<(), AppError>> {
    use k8s_openapi::api;

    macro_rules! decode {
        ($ty:ty) => {{
            let mut de = serde_json::Deserializer::from_slice(bytes);
            let r: Result<$ty, _> = serde_ignored::deserialize(&mut de, |path| {
                unknown.push(normalize_path(&path.to_string()))
            });
            match r {
                Ok(_) => Some(Ok(())),
                Err(err) => {
                    let msg = err.to_string();
                    // Duplicate-field errors are reported separately by the
                    // caller (from the raw bytes); ignore here.
                    if msg.contains("duplicate field") {
                        Some(Ok(()))
                    } else {
                        Some(Err(AppError::BadRequest(format!("Invalid JSON: {}", err))))
                    }
                }
            }
        }};
    }

    match (api_version, kind) {
        ("v1", "Pod") => decode!(api::core::v1::Pod),
        ("v1", "Service") => decode!(api::core::v1::Service),
        ("v1", "ConfigMap") => decode!(api::core::v1::ConfigMap),
        ("v1", "Secret") => decode!(api::core::v1::Secret),
        ("v1", "Namespace") => decode!(api::core::v1::Namespace),
        ("v1", "Node") => decode!(api::core::v1::Node),
        ("v1", "PersistentVolumeClaim") => decode!(api::core::v1::PersistentVolumeClaim),
        ("v1", "PersistentVolume") => decode!(api::core::v1::PersistentVolume),
        ("v1", "ServiceAccount") => decode!(api::core::v1::ServiceAccount),
        ("v1", "Endpoints") => decode!(api::core::v1::Endpoints),
        ("v1", "ReplicationController") => decode!(api::core::v1::ReplicationController),
        ("v1", "ResourceQuota") => decode!(api::core::v1::ResourceQuota),
        ("v1", "LimitRange") => decode!(api::core::v1::LimitRange),
        ("v1", "Event") => decode!(api::core::v1::Event),
        ("apps/v1", "Deployment") => decode!(api::apps::v1::Deployment),
        ("apps/v1", "ReplicaSet") => decode!(api::apps::v1::ReplicaSet),
        ("apps/v1", "StatefulSet") => decode!(api::apps::v1::StatefulSet),
        ("apps/v1", "DaemonSet") => decode!(api::apps::v1::DaemonSet),
        ("apps/v1", "ControllerRevision") => decode!(api::apps::v1::ControllerRevision),
        ("batch/v1", "Job") => decode!(api::batch::v1::Job),
        ("batch/v1", "CronJob") => decode!(api::batch::v1::CronJob),
        ("networking.k8s.io/v1", "Ingress") => decode!(api::networking::v1::Ingress),
        ("networking.k8s.io/v1", "IngressClass") => decode!(api::networking::v1::IngressClass),
        ("networking.k8s.io/v1", "NetworkPolicy") => decode!(api::networking::v1::NetworkPolicy),
        ("rbac.authorization.k8s.io/v1", "Role") => decode!(api::rbac::v1::Role),
        ("rbac.authorization.k8s.io/v1", "ClusterRole") => decode!(api::rbac::v1::ClusterRole),
        ("rbac.authorization.k8s.io/v1", "RoleBinding") => decode!(api::rbac::v1::RoleBinding),
        ("rbac.authorization.k8s.io/v1", "ClusterRoleBinding") => {
            decode!(api::rbac::v1::ClusterRoleBinding)
        }
        ("storage.k8s.io/v1", "StorageClass") => decode!(api::storage::v1::StorageClass),
        ("policy/v1", "PodDisruptionBudget") => decode!(api::policy::v1::PodDisruptionBudget),
        ("scheduling.k8s.io/v1", "PriorityClass") => decode!(api::scheduling::v1::PriorityClass),
        ("coordination.k8s.io/v1", "Lease") => decode!(api::coordination::v1::Lease),
        ("discovery.k8s.io/v1", "EndpointSlice") => decode!(api::discovery::v1::EndpointSlice),
        ("node.k8s.io/v1", "RuntimeClass") => decode!(api::node::v1::RuntimeClass),
        ("events.k8s.io/v1", "Event") => decode!(api::events::v1::Event),
        ("autoscaling/v1", "HorizontalPodAutoscaler") => {
            decode!(api::autoscaling::v1::HorizontalPodAutoscaler)
        }
        ("certificates.k8s.io/v1", "CertificateSigningRequest") => {
            decode!(api::certificates::v1::CertificateSigningRequest)
        }
        _ => None,
    }
}

/// Build the `strict decoding error: ...` message (and 400) from the collected
/// unknown and duplicate field paths, or `Ok(())` when there are none. Upstream
/// lists every offending field, deduplicated and sorted.
fn strict_decoding_result(
    mut unknown: Vec<String>,
    mut duplicates: Vec<String>,
) -> Result<(), AppError> {
    unknown.retain(|p| !p.is_empty());
    unknown.sort();
    unknown.dedup();
    duplicates.sort();
    duplicates.dedup();
    if unknown.is_empty() && duplicates.is_empty() {
        return Ok(());
    }
    let mut parts: Vec<String> = Vec::new();
    parts.extend(
        unknown
            .into_iter()
            .map(|p| format!("unknown field \"{p}\"")),
    );
    parts.extend(
        duplicates
            .into_iter()
            .map(|p| format!("duplicate field \"{p}\"")),
    );
    Err(AppError::BadRequest(format!(
        "strict decoding error: {}",
        parts.join(", ")
    )))
}

pub fn check_deployment_strict_decode_from_raw_json(
    query: &CreateUpdateQuery,
    body: &[u8],
) -> Result<(), AppError> {
    if query.field_validation.as_deref() != Some("Strict") {
        return Ok(());
    }
    let duplicate_paths = parse_json_duplicate_field_paths(body)?;
    let mut unknown_paths = Vec::new();
    if let Some(res) = typed_strict_decode("apps/v1", "Deployment", body, &mut unknown_paths) {
        res?;
    }
    strict_decoding_result(unknown_paths, duplicate_paths)
}

/// Strict/Warn field validation for a parsed resource Value. For built-in kinds
/// with a typed schema this performs a deep decode that catches nested unknown
/// fields (`spec.bogus`); other kinds fall back to the shallow top-level +
/// metadata check. Strict rejects with 400 BadRequest. (Duplicate-key detection
/// requires the raw bytes and is handled on the create paths that have them.)
pub fn check_field_validation_strict_typed(
    api_version: &str,
    kind: &str,
    query: &CreateUpdateQuery,
    body: &Value,
) -> Result<(), AppError> {
    if query.field_validation.as_deref() != Some("Strict") {
        return Ok(());
    }
    let bytes = serde_json::to_vec(body).unwrap_or_default();
    let mut unknown_paths = Vec::new();
    match typed_strict_decode(api_version, kind, &bytes, &mut unknown_paths) {
        Some(res) => {
            res?;
            strict_decoding_result(unknown_paths, Vec::new())
        }
        // No typed schema — fall back to the shallow top-level/metadata check.
        None => check_field_validation_strict(query, body),
    }
}

/// Check immutable ConfigMap/Secret: reject changes to data, binaryData, or immutable field.
/// Metadata changes (labels, annotations) are still allowed.
pub fn check_immutable_fields(
    current: &Value,
    updated: &Value,
    kind: &str,
    namespace: &str,
    name: &str,
) -> Result<(), AppError> {
    // Check if immutable field itself changed (can't flip true -> false/null)
    let new_immutable = updated
        .get("immutable")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !new_immutable {
        return Err(AppError::UnprocessableEntity(format!(
            "{} {}/{} is immutable, the field `immutable` is immutable",
            kind, namespace, name
        )));
    }

    // Check if data changed
    if current.get("data") != updated.get("data") {
        return Err(AppError::UnprocessableEntity(format!(
            "{} {}/{} is immutable, the field `data` is immutable",
            kind, namespace, name
        )));
    }

    // Check if binaryData changed
    if current.get("binaryData") != updated.get("binaryData") {
        return Err(AppError::UnprocessableEntity(format!(
            "{} {}/{} is immutable, the field `binaryData` is immutable",
            kind, namespace, name
        )));
    }

    Ok(())
}

/// Validate a MutatingWebhookConfiguration or ValidatingWebhookConfiguration body.
/// Rejects configs with invalid sideEffects, empty admissionReviewVersions, etc.
pub fn prepare_admissionregistration_resource(
    kind: &str,
    body: &mut Value,
) -> Result<(), AppError> {
    validate_admissionregistration_resource(kind, body)?;
    if kind == "ValidatingAdmissionPolicy" {
        crate::admission::apply_validating_admission_policy_typechecking_status(body);
    }
    Ok(())
}

pub fn validate_admissionregistration_resource(kind: &str, body: &Value) -> Result<(), AppError> {
    match kind {
        "MutatingWebhookConfiguration" | "ValidatingWebhookConfiguration" => {
            validate_webhook_configuration(body)
        }
        "ValidatingAdmissionPolicy" => crate::admission::validate_validating_admission_policy(body)
            .map_err(AppError::UnprocessableEntity),
        "ValidatingAdmissionPolicyBinding" => {
            crate::admission::validate_validating_admission_policy_binding(body)
                .map_err(AppError::UnprocessableEntity)
        }
        _ => Ok(()),
    }
}

pub fn validate_webhook_configuration(body: &Value) -> Result<(), AppError> {
    let valid_side_effects = ["None", "NoneOnDryRun", "Some", "Unknown"];

    let webhooks = match body.pointer("/webhooks").and_then(|w| w.as_array()) {
        Some(wh) => wh,
        None => return Ok(()), // No webhooks is valid (empty config)
    };

    for (i, webhook) in webhooks.iter().enumerate() {
        // Validate sideEffects
        let side_effects = webhook.pointer("/sideEffects").and_then(|s| s.as_str());
        if let Some(se) = side_effects
            && !valid_side_effects.contains(&se)
        {
            return Err(AppError::UnprocessableEntity(format!(
                "Unsupported value: webhooks[{}].sideEffects: Supported values: {:?}",
                i, valid_side_effects
            )));
        }

        // admissionReviewVersions must be non-empty
        let arv = webhook
            .pointer("/admissionReviewVersions")
            .and_then(|a| a.as_array());
        if let Some(arv) = arv
            && arv.is_empty()
        {
            return Err(AppError::UnprocessableEntity(format!(
                "Required value: webhooks[{}].admissionReviewVersions must not be empty",
                i
            )));
        }

        // clientConfig must have exactly one of url or service
        let has_url = webhook.pointer("/clientConfig/url").is_some();
        let has_service = webhook.pointer("/clientConfig/service").is_some();
        if has_url && has_service {
            return Err(AppError::UnprocessableEntity(format!(
                "Invalid value: webhooks[{}].clientConfig: must specify exactly one of url or service",
                i
            )));
        }

        // matchConditions: each entry must have a non-empty name and a syntactically valid
        // CEL expression (parsed via rust-cel-parser).
        if let Some(conditions) = webhook
            .pointer("/matchConditions")
            .and_then(|c| c.as_array())
        {
            for (j, condition) in conditions.iter().enumerate() {
                let name = condition
                    .pointer("/name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("");
                if name.is_empty() {
                    return Err(AppError::UnprocessableEntity(format!(
                        "Required value: webhooks[{}].matchConditions[{}].name must not be empty",
                        i, j
                    )));
                }

                let expr = condition
                    .pointer("/expression")
                    .and_then(|e| e.as_str())
                    .unwrap_or("");
                if expr.is_empty() {
                    return Err(AppError::UnprocessableEntity(format!(
                        "Required value: webhooks[{}].matchConditions[{}].expression must not be empty",
                        i, j
                    )));
                }

                if let Err(e) = rust_cel_parser::parse_cel_program(expr) {
                    let msg = e.to_string().replace('\n', " ");
                    return Err(AppError::UnprocessableEntity(format!(
                        "Invalid value: webhooks[{}].matchConditions[{}].expression: compilation failed: {}",
                        i, j, msg
                    )));
                }
            }
        }
    }

    Ok(())
}

/// Validate a custom resource body against the CRD's OpenAPI schema when fieldValidation=Strict.
/// Rejects extra properties not defined in the schema at any level.
pub async fn check_cr_field_validation_strict(
    db: &dyn crate::datastore::DatastoreBackend,
    group: &str,
    version: &str,
    kind: &str,
    body: &Value,
) -> Result<(), AppError> {
    let schema = match load_crd_openapi_schema(db, group, version, kind).await? {
        Some(s) => s,
        None => return Ok(()), // No schema found, allow
    };

    validate_against_schema(body, &schema, "")
}

/// Apply CRD schema defaults to a custom resource body.
/// Walks the schema tree and sets default values for missing fields.
pub async fn apply_crd_defaults(
    db: &dyn crate::datastore::DatastoreBackend,
    group: &str,
    version: &str,
    kind: &str,
    body: &mut Value,
) {
    if let Ok(Some(schema)) = load_crd_openapi_schema(db, group, version, kind).await {
        apply_schema_defaults(body, &schema);
    }
}

/// Apply CRD schema pruning to remove unknown fields not allowed by schema.
pub async fn apply_crd_pruning(
    db: &dyn crate::datastore::DatastoreBackend,
    group: &str,
    version: &str,
    kind: &str,
    body: &mut Value,
) {
    if let Ok(Some(schema)) = load_crd_openapi_schema(db, group, version, kind).await {
        prune_against_schema(body, &schema, true);
    }
}

async fn load_crd_openapi_schema(
    db: &dyn crate::datastore::DatastoreBackend,
    group: &str,
    version: &str,
    kind: &str,
) -> Result<Option<Value>, AppError> {
    let crds = db
        .list_resources(
            "apiextensions.k8s.io/v1",
            "CustomResourceDefinition",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .map_err(|e| AppError::InternalError(format!("Failed to list CRDs: {}", e)))?;

    for crd in &crds.items {
        let crd_group = crd.data.pointer("/spec/group").and_then(|g| g.as_str());
        let crd_kind = crd
            .data
            .pointer("/spec/names/kind")
            .and_then(|k| k.as_str());
        if crd_group != Some(group) || crd_kind != Some(kind) {
            continue;
        }
        if let Some(versions) = crd
            .data
            .pointer("/spec/versions")
            .and_then(|v| v.as_array())
        {
            for ver in versions {
                if ver.get("name").and_then(|n| n.as_str()) == Some(version) {
                    return Ok(ver.pointer("/schema/openAPIV3Schema").cloned());
                }
            }
        }
        break;
    }

    Ok(None)
}

/// Recursively apply default values from an OpenAPI v3 schema to a JSON value.
/// Public alias for testing.
#[cfg(test)]
pub fn apply_schema_defaults_pub(value: &mut Value, schema: &Value) {
    apply_schema_defaults(value, schema);
}

fn apply_schema_defaults(value: &mut Value, schema: &Value) {
    let props = match schema.get("properties").and_then(|p| p.as_object()) {
        Some(p) => p,
        None => return,
    };

    let obj = match value.as_object_mut() {
        Some(o) => o,
        None => return,
    };

    for (key, prop_schema) in props {
        // Skip standard K8s top-level fields
        if matches!(key.as_str(), "apiVersion" | "kind" | "metadata") {
            continue;
        }

        if let Some(existing) = obj.get_mut(key) {
            // Field exists — recurse into nested objects
            if existing.is_object() {
                apply_schema_defaults(existing, prop_schema);
            }
        } else if let Some(default_val) = prop_schema.get("default") {
            // Field missing — apply default
            obj.insert(key.clone(), default_val.clone());
        }
    }
}

fn prune_against_schema(value: &mut Value, schema: &Value, is_root: bool) {
    if let (Some(items), Some(item_schema)) = (value.as_array_mut(), schema.get("items")) {
        for item in items {
            prune_against_schema(item, item_schema, false);
        }
        return;
    }

    let Some(obj) = value.as_object_mut() else {
        return;
    };

    let preserve_unknown = schema
        .get("x-kubernetes-preserve-unknown-fields")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let is_embedded_resource = schema
        .get("x-kubernetes-embedded-resource")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let schema_properties = schema.get("properties").and_then(|p| p.as_object());
    let additional_properties = schema.get("additionalProperties");

    let keys: Vec<String> = obj.keys().cloned().collect();
    for key in keys {
        if is_root && matches!(key.as_str(), "apiVersion" | "kind" | "metadata" | "status") {
            continue;
        }
        if is_embedded_resource
            && matches!(key.as_str(), "apiVersion" | "kind" | "metadata" | "status")
        {
            continue;
        }

        let mut child_schema = schema_properties.and_then(|props| props.get(&key));
        if child_schema.is_none() {
            match additional_properties {
                Some(Value::Bool(true)) => continue,
                Some(Value::Bool(false)) => {
                    obj.remove(&key);
                    continue;
                }
                Some(schema_obj) if schema_obj.is_object() => {
                    child_schema = Some(schema_obj);
                }
                _ if preserve_unknown => continue,
                _ => {
                    obj.remove(&key);
                    continue;
                }
            }
        }

        if let Some(schema_for_key) = child_schema
            && let Some(child) = obj.get_mut(&key)
        {
            prune_against_schema(child, schema_for_key, false);
        }
    }
}

/// Recursively validate a JSON value against an OpenAPI v3 schema.
/// Returns error on unknown properties when the schema has explicit `properties`.
pub fn validate_against_schema(value: &Value, schema: &Value, path: &str) -> Result<(), AppError> {
    if let Some(enum_values) = schema.get("enum").and_then(|v| v.as_array()) {
        let valid = enum_values.iter().any(|candidate| candidate == value);
        if !valid {
            let field = if path.is_empty() { "<root>" } else { path };
            let value_rendered =
                serde_json::to_string(value).unwrap_or_else(|_| format!("{:?}", value));
            return Err(AppError::UnprocessableEntity(format!(
                "Unsupported value: {} for {}: supported values: {:?}",
                value_rendered, field, enum_values
            )));
        }
    }

    if let (Some(items), Some(item_schema)) = (value.as_array(), schema.get("items")) {
        for (idx, item) in items.iter().enumerate() {
            let item_path = if path.is_empty() {
                format!("[{}]", idx)
            } else {
                format!("{}[{}]", path, idx)
            };
            validate_against_schema(item, item_schema, &item_path)?;
        }
        return Ok(());
    }

    let obj = match value.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };

    if let Some(required_fields) = schema.get("required").and_then(|v| v.as_array()) {
        for required in required_fields.iter().filter_map(|v| v.as_str()) {
            if !obj.contains_key(required) {
                let field_path = if path.is_empty() {
                    required.to_string()
                } else {
                    format!("{}.{}", path, required)
                };
                return Err(AppError::UnprocessableEntity(format!(
                    "{}: Required value",
                    field_path
                )));
            }
        }
    }

    // If the schema allows unknown fields, skip unknown field validation at this level
    let preserve_unknown = schema
        .get("x-kubernetes-preserve-unknown-fields")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let is_embedded_resource = schema
        .get("x-kubernetes-embedded-resource")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let validate_object_metadata = path.is_empty() || is_embedded_resource;

    let schema_properties = schema.get("properties").and_then(|p| p.as_object());

    if validate_object_metadata && let Some(meta) = obj.get("metadata").and_then(|m| m.as_object())
    {
        validate_metadata_fields_at_path(meta, path)?;
    }

    if let Some(props) = schema_properties {
        for key in obj.keys() {
            // Skip standard K8s top-level fields always allowed on CRs
            if path.is_empty() && matches!(key.as_str(), "apiVersion" | "kind" | "status") {
                continue;
            }

            // Embedded resources always allow the standard object envelope.
            if is_embedded_resource && matches!(key.as_str(), "apiVersion" | "kind") {
                continue;
            }

            // Validate metadata fields against known ObjectMeta fields.
            if validate_object_metadata && key == "metadata" {
                continue;
            }

            if !props.contains_key(key) {
                // Allow unknown fields if schema has x-kubernetes-preserve-unknown-fields
                if preserve_unknown {
                    continue;
                }
                let field_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", path, key)
                };
                return Err(AppError::UnprocessableEntity(schema_unknown_field_error(
                    &field_path,
                )));
            }

            // Recurse into nested objects
            if let Some(prop_schema) = props.get(key) {
                let nested_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", path, key)
                };
                validate_against_schema(&obj[key], prop_schema, &nested_path)?;
            }
        }
    } else if preserve_unknown {
        // Schema has x-kubernetes-preserve-unknown-fields but no properties — all fields allowed
        // Metadata validation already ran above for root and embedded resources.
    }

    Ok(())
}

/// Known ObjectMeta fields from the K8s API spec.
const KNOWN_METADATA_FIELDS: &[&str] = &[
    "name",
    "generateName",
    "namespace",
    "selfLink",
    "uid",
    "resourceVersion",
    "generation",
    "creationTimestamp",
    "deletionTimestamp",
    "deletionGracePeriodSeconds",
    "labels",
    "annotations",
    "ownerReferences",
    "finalizers",
    "managedFields",
    "clusterName",
];

/// Validate metadata fields against known ObjectMeta fields.
#[cfg(test)]
pub fn validate_metadata_fields(meta: &serde_json::Map<String, Value>) -> Result<(), AppError> {
    validate_metadata_fields_at_path(meta, "")
}

fn validate_metadata_fields_at_path(
    meta: &serde_json::Map<String, Value>,
    path: &str,
) -> Result<(), AppError> {
    for key in meta.keys() {
        if !KNOWN_METADATA_FIELDS.contains(&key.as_str()) {
            let field_path = if path.is_empty() {
                format!("metadata.{}", key)
            } else {
                format!("{}.metadata.{}", path, key)
            };
            return Err(AppError::UnprocessableEntity(schema_unknown_field_error(
                &field_path,
            )));
        }
    }
    Ok(())
}

fn schema_unknown_field_error(field_path: &str) -> String {
    let dotted_path = if field_path.starts_with('.') {
        field_path.to_string()
    } else {
        format!(".{field_path}")
    };
    format!(
        "strict decoding error: unknown field \"{}\" (field not declared in schema); {}: field not declared in schema",
        field_path, dotted_path
    )
}

#[derive(Deserialize, serde::Serialize, Default)]
pub struct DeleteOptions {
    #[serde(rename = "propagationPolicy")]
    pub propagation_policy: Option<String>,
    #[serde(rename = "orphanDependents")]
    pub orphan_dependents: Option<bool>,
    #[serde(rename = "gracePeriodSeconds")]
    pub _grace_period_seconds: Option<i64>,
    pub preconditions: Option<DeletePreconditions>,
}

#[derive(Clone, Deserialize, serde::Serialize, Default)]
pub struct DeletePreconditions {
    pub uid: Option<String>,
    #[serde(rename = "resourceVersion")]
    pub resource_version: Option<String>,
}

impl DeleteOptions {
    pub fn with_uid_precondition(uid: impl Into<String>) -> Self {
        Self {
            preconditions: Some(DeletePreconditions {
                uid: Some(uid.into()),
                resource_version: None,
            }),
            ..Default::default()
        }
    }

    pub fn resource_preconditions(
        &self,
    ) -> Result<crate::datastore::ResourcePreconditions, String> {
        let Some(preconditions) = &self.preconditions else {
            return Ok(crate::datastore::ResourcePreconditions::default());
        };
        let resource_version = preconditions
            .resource_version
            .as_deref()
            .map(|rv| {
                rv.parse::<i64>().map_err(|_| {
                    format!("invalid DeleteOptions preconditions.resourceVersion: {rv}")
                })
            })
            .transpose()?;
        Ok(crate::datastore::ResourcePreconditions {
            uid: preconditions.uid.clone(),
            resource_version,
        })
    }
}

pub fn parse_delete_options_body(body: &[u8]) -> DeleteOptions {
    if body.is_empty() {
        return DeleteOptions::default();
    }

    if let Ok(opts) = serde_json::from_slice::<DeleteOptions>(body) {
        return opts;
    }

    if let Some(opts) = parse_delete_options_protobuf(body) {
        return opts;
    }

    DeleteOptions::default()
}

pub fn parse_delete_options_protobuf(body: &[u8]) -> Option<DeleteOptions> {
    use prost::Message;

    fn map_pb_delete_options(
        pb: k8s_pb::apimachinery::pkg::apis::meta::v1::DeleteOptions,
    ) -> DeleteOptions {
        DeleteOptions {
            propagation_policy: pb.propagation_policy,
            orphan_dependents: pb.orphan_dependents,
            _grace_period_seconds: pb.grace_period_seconds,
            preconditions: pb.preconditions.map(|p| DeletePreconditions {
                uid: p.uid,
                resource_version: p.resource_version,
            }),
        }
    }

    fn parse_unknown_payload(payload: &[u8]) -> Option<DeleteOptions> {
        use prost::Message;

        let unknown = crate::protobuf::Unknown::decode(payload).ok()?;

        if let Ok(opts) = serde_json::from_slice::<DeleteOptions>(&unknown.raw) {
            return Some(opts);
        }

        k8s_pb::apimachinery::pkg::apis::meta::v1::DeleteOptions::decode(unknown.raw.as_slice())
            .ok()
            .map(map_pb_delete_options)
    }

    const K8S_MAGIC_PREFIX: [u8; 4] = [0x6b, 0x38, 0x73, 0x00];

    if body.len() >= 4
        && body[0..4] == K8S_MAGIC_PREFIX
        && let Some(opts) = parse_unknown_payload(&body[4..])
    {
        return Some(opts);
    }

    if let Some(opts) = parse_unknown_payload(body) {
        return Some(opts);
    }

    let pb_bytes = if body.len() >= 4 && body[0..4] == K8S_MAGIC_PREFIX {
        &body[4..]
    } else {
        body
    };
    k8s_pb::apimachinery::pkg::apis::meta::v1::DeleteOptions::decode(pb_bytes)
        .ok()
        .map(map_pb_delete_options)
}

pub struct AdmissionContextRequest<'a> {
    pub api_version: &'a str,
    pub kind: &'a str,
    pub operation: &'a str,
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub object: Value,
    pub old_object: Option<Value>,
    pub dry_run: bool,
    pub subresource: Option<&'a str>,
    pub options: Option<Value>,
}

pub fn build_admission_context(
    request: AdmissionContextRequest<'_>,
) -> crate::admission::AdmissionRequestContext {
    let AdmissionContextRequest {
        api_version,
        kind,
        operation,
        namespace,
        name,
        object,
        old_object,
        dry_run,
        subresource,
        options,
    } = request;

    let mut ctx = crate::admission::AdmissionRequestContext::from_legacy(
        &object,
        api_version,
        kind,
        operation,
    );
    if object.is_null() {
        let (group, version) = if let Some((group, version)) = api_version.split_once('/') {
            (group.to_string(), version.to_string())
        } else {
            ("".to_string(), api_version.to_string())
        };
        ctx.api_version = api_version.to_string();
        ctx.api_group = group;
        ctx.version = version;
        ctx.kind = kind.to_string();
        ctx.resource = kind.to_ascii_lowercase() + "s";
        ctx.object = Value::Null;
    }
    ctx.operation = operation.to_string();
    ctx.namespace = namespace;
    ctx.name = name;
    ctx.dry_run = Some(dry_run);
    ctx.old_object = old_object;
    ctx.subresource = subresource.map(ToString::to_string);
    ctx.options = options;
    ctx
}

pub async fn run_admission_for_request(
    db: &dyn DatastoreBackend,
    mut ctx: crate::admission::AdmissionRequestContext,
) -> Result<Value, AppError> {
    let engine = crate::admission::AdmissionEngine::new(db);
    let admitted = engine
        .run_with_context(&ctx, true)
        .await
        .map_err(map_mutating_admission_error)?;
    ctx.object = admitted.clone();
    engine
        .run_with_context(&ctx, false)
        .await
        .map_err(map_validating_admission_error)?;
    Ok(admitted)
}

// Helper function to check content negotiation
pub fn check_content_type(_headers: &HeaderMap) -> Result<(), AppError> {
    // No-op: klights always serves JSON regardless of Accept/Content-Type headers.
    // All K8s clients can parse JSON. See content negotiation comment at top of file.
    Ok(())
}

pub fn parse_apply_yaml(body: &[u8]) -> Result<Value, AppError> {
    struct StrictYamlValue(Value);

    impl<'de> serde::Deserialize<'de> for StrictYamlValue {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct StrictYamlVisitor;

            impl<'de> serde::de::Visitor<'de> for StrictYamlVisitor {
                type Value = Value;

                fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    formatter.write_str("a YAML value")
                }

                fn visit_unit<E>(self) -> Result<Self::Value, E> {
                    Ok(Value::Null)
                }

                fn visit_none<E>(self) -> Result<Self::Value, E> {
                    Ok(Value::Null)
                }

                fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                    Ok(Value::Bool(value))
                }

                fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                    Ok(Value::Number(value.into()))
                }

                fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                    Ok(Value::Number(value.into()))
                }

                fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    serde_json::Number::from_f64(value)
                        .map(Value::Number)
                        .ok_or_else(|| E::custom(format!("invalid floating-point value: {value}")))
                }

                fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                    Ok(Value::String(value.to_string()))
                }

                fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                    Ok(Value::String(value))
                }

                fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where
                    A: serde::de::SeqAccess<'de>,
                {
                    let mut items = Vec::new();
                    while let Some(item) = seq.next_element::<StrictYamlValue>()? {
                        items.push(item.0);
                    }
                    Ok(Value::Array(items))
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: serde::de::MapAccess<'de>,
                {
                    let mut obj = serde_json::Map::new();
                    while let Some((key, value)) = map.next_entry::<String, StrictYamlValue>()? {
                        if obj.contains_key(&key) {
                            return Err(serde::de::Error::custom(format!(
                                "key \"{key}\" already set in map"
                            )));
                        }
                        obj.insert(key, value.0);
                    }
                    Ok(Value::Object(obj))
                }
            }

            deserializer
                .deserialize_any(StrictYamlVisitor)
                .map(StrictYamlValue)
        }
    }

    serde_yaml::from_slice::<StrictYamlValue>(body)
        .map(|value| value.0)
        .map_err(format_apply_yaml_error)
}

fn format_apply_yaml_error(err: serde_yaml::Error) -> AppError {
    let raw = err.to_string();
    if raw.contains("already set in map") {
        let duplicate_key_message = extract_yaml_duplicate_key_message(&raw)
            .unwrap_or_else(|| "duplicate field in map".to_string());
        if let Some(line) = err.location().map(|loc| loc.line()) {
            let normalized_line = line.saturating_add(2);
            return AppError::BadRequest(format!(
                "Invalid YAML: line {normalized_line}: {duplicate_key_message}"
            ));
        }
        return AppError::BadRequest(format!("Invalid YAML: {duplicate_key_message}"));
    }
    AppError::BadRequest(format!("Invalid YAML: {raw}"))
}

fn extract_yaml_duplicate_key_message(raw: &str) -> Option<String> {
    let key_start = raw.find("key \"")?;
    let key_end = raw
        .find("already set in map")
        .map(|idx| idx + "already set in map".len())?;
    if key_end <= key_start {
        return None;
    }
    Some(raw[key_start..key_end].trim().to_string())
}

// Helper function to inject resourceVersion into metadata.
// Accepts `Arc<Value>` so callers can pass `resource.data` (the datastore
// `Resource` body) directly without an upstream deep clone — we only pay
// the clone here if the Arc has other live readers.
pub fn inject_resource_version(
    data: impl Into<std::sync::Arc<Value>>,
    resource_version: i64,
) -> Value {
    let mut data = std::sync::Arc::unwrap_or_clone(data.into());
    if let Some(obj) = data.as_object_mut()
        && let Some(metadata) = obj.get_mut("metadata")
        && let Some(meta_obj) = metadata.as_object_mut()
    {
        meta_obj.insert(
            "resourceVersion".to_string(),
            serde_json::json!(resource_version.to_string()),
        );

        // Add uid if missing/null/empty
        let uid_missing_or_empty = meta_obj
            .get("uid")
            .is_none_or(|v| v.is_null() || v.as_str().is_some_and(|s| s.trim().is_empty()));
        if uid_missing_or_empty {
            meta_obj.insert(
                "uid".to_string(),
                serde_json::Value::String(uuid::Uuid::new_v4().to_string()),
            );
        }

        // Add creationTimestamp if not present
        if meta_obj
            .get("creationTimestamp")
            .is_none_or(|v| v.is_null())
        {
            meta_obj.insert(
                "creationTimestamp".to_string(),
                serde_json::Value::String(crate::utils::k8s_timestamp()),
            );
        }
    }
    data
}

/// Validate that `name` is a valid DNS-style K8s metadata.name.
///
/// The per-label 63-char DNS limit is NOT enforced — upstream K8s only
/// applies the 253-char total length limit for these object names.
///
/// RBAC resources are deliberately not handled here; they use path-segment
/// validation and are routed through `validate_metadata_name_for_kind`.
pub fn validate_dns_subdomain(name: &str, context: &str) -> Result<(), AppError> {
    if name.is_empty() {
        return Err(AppError::BadRequest(format!(
            "Invalid {context}: must be non-empty"
        )));
    }
    if name.len() > 253 {
        return Err(AppError::BadRequest(format!(
            "Invalid {context} '{name}': must be no more than 253 characters"
        )));
    }

    // Check each character is valid: lowercase alphanumeric, hyphen, dot.
    for (i, ch) in name.char_indices() {
        let valid = ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' || ch == '.';
        if !valid {
            return Err(AppError::BadRequest(format!(
                "Invalid {context} '{name}': must be a valid DNS subdomain (lowercase alphanumeric, hyphens, dots; max 253 chars; cannot start/end with hyphen or dot) at position {i}: '{ch}'"
            )));
        }
    }

    // Check start/end constraints
    if name.starts_with('-') || name.starts_with('.') {
        return Err(AppError::BadRequest(format!(
            "Invalid {context} '{name}': must not start with hyphen or dot"
        )));
    }
    if name.ends_with('-') || name.ends_with('.') {
        return Err(AppError::BadRequest(format!(
            "Invalid {context} '{name}': must not end with hyphen or dot"
        )));
    }

    // Note: upstream K8s does NOT enforce a per-label 63-char limit on
    // metadata.name; only the total 253-char limit applies.  The 63-char
    // per-label rule is a DNS RFC constraint but K8s resource names are
    // identifiers, not DNS labels.  Conformance tests create names with
    // single labels up to ~70 chars (e.g. projected-configmap-test-volume-...).

    Ok(())
}

pub fn validate_path_segment_name(name: &str, context: &str) -> Result<(), AppError> {
    if name.is_empty() {
        return Err(AppError::BadRequest(format!(
            "Invalid {context}: must be non-empty"
        )));
    }
    if name == "." || name == ".." {
        return Err(AppError::BadRequest(format!(
            "Invalid {context} '{name}': may not be '.' or '..'"
        )));
    }
    if name.contains('/') || name.contains('%') {
        return Err(AppError::BadRequest(format!(
            "Invalid {context} '{name}': may not contain '/' or '%'"
        )));
    }
    Ok(())
}

pub fn validate_metadata_name_for_kind(
    api_version: &str,
    kind: &str,
    name: &str,
    context: &str,
) -> Result<(), AppError> {
    if kind == "IPAddress"
        || crate::api::metadata_name_uses_path_segment_validation(api_version, kind)
    {
        return validate_path_segment_name(name, context);
    }
    validate_dns_subdomain(name, context)
}
