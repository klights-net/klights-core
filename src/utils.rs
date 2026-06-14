//! Shared utility functions for klights

/// Acquire a `std::sync::Mutex` guard, recovering silently from poison.
///
/// Used for short, panic-free critical sections (counter bookkeeping,
/// HashMap insert/remove, Vec push) where the protected data stays
/// consistent regardless of which line a panicking caller died on. A
/// long-running daemon that turns one buggy panic into a permanent
/// poison cascade dies for every subsequent caller — using this helper
/// limits blast radius to the single panic that caused it.
///
/// Do NOT use for sections whose protected data could be left in an
/// inconsistent state mid-mutation; for those, the right fix is to
/// audit the section to make it panic-free, not to recover blindly.
#[must_use = "the returned MutexGuard must be bound or the lock is released immediately"]
pub fn lock_recover<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Structural equality for two top-level K8s resource bodies that
/// ignores a single field inside the top-level `metadata` object
/// (typically `resourceVersion`). Walks both trees once without
/// allocating, so callers can detect no-op patches without the deep
/// `Value::clone()` + strip-rv-from-clone pattern.
pub fn resource_bodies_equal_ignoring_metadata_field(
    a: &serde_json::Value,
    b: &serde_json::Value,
    metadata_field: &str,
) -> bool {
    use serde_json::Value;
    let (Value::Object(ao), Value::Object(bo)) = (a, b) else {
        return a == b;
    };
    if ao.len() != bo.len() {
        return false;
    }
    for (key, av) in ao {
        let bv = match bo.get(key) {
            Some(v) => v,
            None => return false,
        };
        if key == "metadata" {
            if !objects_equal_ignoring_key(av, bv, metadata_field) {
                return false;
            }
        } else if av != bv {
            return false;
        }
    }
    true
}

/// Compare two `Object` Values for equality, skipping one named key on
/// both sides. Falls back to `==` if either side isn't an object so the
/// outer helper handles unusual shapes safely.
fn objects_equal_ignoring_key(a: &serde_json::Value, b: &serde_json::Value, key: &str) -> bool {
    use serde_json::Value;
    let (Value::Object(ao), Value::Object(bo)) = (a, b) else {
        return a == b;
    };
    let a_count = ao.iter().filter(|(k, _)| *k != key).count();
    let b_count = bo.iter().filter(|(k, _)| *k != key).count();
    if a_count != b_count {
        return false;
    }
    for (k, av) in ao {
        if k == key {
            continue;
        }
        match bo.get(k) {
            Some(bv) if av == bv => {}
            _ => return false,
        }
    }
    true
}

#[cfg(test)]
mod resource_bodies_equal_tests {
    use super::resource_bodies_equal_ignoring_metadata_field;
    use serde_json::json;

    #[test]
    fn identical_bodies_are_equal() {
        let a = json!({"metadata": {"name": "n"}, "spec": {"x": 1}});
        let b = json!({"metadata": {"name": "n"}, "spec": {"x": 1}});
        assert!(resource_bodies_equal_ignoring_metadata_field(
            &a,
            &b,
            "resourceVersion"
        ));
    }

    #[test]
    fn ignores_only_the_named_metadata_field() {
        let a = json!({"metadata": {"name": "n", "resourceVersion": "10"}, "spec": {"x": 1}});
        let b = json!({"metadata": {"name": "n", "resourceVersion": "11"}, "spec": {"x": 1}});
        assert!(resource_bodies_equal_ignoring_metadata_field(
            &a,
            &b,
            "resourceVersion"
        ));
    }

    #[test]
    fn detects_difference_in_other_metadata_field() {
        let a = json!({"metadata": {"name": "n", "uid": "u1"}, "spec": {"x": 1}});
        let b = json!({"metadata": {"name": "n", "uid": "u2"}, "spec": {"x": 1}});
        assert!(!resource_bodies_equal_ignoring_metadata_field(
            &a,
            &b,
            "resourceVersion"
        ));
    }

    #[test]
    fn detects_difference_outside_metadata() {
        let a = json!({"metadata": {"name": "n"}, "spec": {"x": 1}});
        let b = json!({"metadata": {"name": "n"}, "spec": {"x": 2}});
        assert!(!resource_bodies_equal_ignoring_metadata_field(
            &a,
            &b,
            "resourceVersion"
        ));
    }

    #[test]
    fn detects_added_top_level_field() {
        let a = json!({"metadata": {"name": "n"}, "spec": {"x": 1}});
        let b = json!({"metadata": {"name": "n"}, "spec": {"x": 1}, "status": {"ready": true}});
        assert!(!resource_bodies_equal_ignoring_metadata_field(
            &a,
            &b,
            "resourceVersion"
        ));
    }

    #[test]
    fn metadata_field_present_on_one_side_only_is_still_equal() {
        // RV present on one side, missing on the other — still equal because
        // the helper ignores it.
        let a = json!({"metadata": {"name": "n", "resourceVersion": "10"}, "spec": {}});
        let b = json!({"metadata": {"name": "n"}, "spec": {}});
        assert!(resource_bodies_equal_ignoring_metadata_field(
            &a,
            &b,
            "resourceVersion"
        ));
    }
}

#[cfg(test)]
mod lock_recover_tests {
    use super::lock_recover;
    use std::sync::Mutex;

    #[test]
    fn lock_recover_returns_inner_value_after_poison() {
        let m = Mutex::new(42u32);
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = m.lock().expect("first lock should succeed");
            panic!("intentional poison for test");
        }));
        assert!(unwind.is_err(), "panic must have unwound");

        // Standard .lock() must now report poisoning…
        assert!(m.lock().is_err(), "mutex should be poisoned after panic");

        // …but lock_recover hands back the (consistent) inner value.
        let guard = lock_recover(&m);
        assert_eq!(*guard, 42, "recovery should expose the protected value");
    }
}

/// Format a `chrono::DateTime<Utc>` as a K8s-canonical `metav1.Time`:
/// `YYYY-MM-DDTHH:MM:SSZ` (no fractional seconds, `Z` suffix). K8s upstream
/// uses `time.Format("2006-01-02T15:04:05Z07:00")` which yields this exact
/// shape for UTC. Use for `creationTimestamp`, condition `lastTransitionTime`,
/// node heartbeats — anything typed `metav1.Time`.
pub fn k8s_time_format(dt: chrono::DateTime<chrono::Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// `k8s_time_format(now)`.
pub fn k8s_time_now() -> String {
    k8s_time_format(chrono::Utc::now())
}

/// Format a `chrono::DateTime<Utc>` as a K8s-canonical `metav1.MicroTime`:
/// `YYYY-MM-DDTHH:MM:SS.ffffffZ` (exactly 6 microsecond digits, `Z` suffix).
/// K8s upstream uses `time.Format("2006-01-02T15:04:05.000000Z07:00")`.
/// Use for `MicroTime` fields: Lease `renewTime`/`acquireTime`, Event
/// micro-timestamps. Do NOT use `chrono::DateTime::to_rfc3339()` — it emits
/// `+00:00` instead of `Z` and trips strict K8s parsers (P0-E2E-20260423-12).
pub fn k8s_microtime_format(dt: chrono::DateTime<chrono::Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S.%6fZ").to_string()
}

/// `k8s_microtime_format(now)`.
pub fn k8s_microtime_now() -> String {
    k8s_microtime_format(chrono::Utc::now())
}

/// Canonicalize `events.k8s.io/v1` `metav1.MicroTime` fields in-place.
pub fn normalize_event_microtime_fields(value: &mut serde_json::Value) {
    fn normalize_path(value: &mut serde_json::Value, path: &[&str]) {
        if path.is_empty() {
            return;
        }
        if path.len() == 1 {
            if let Some(raw) = value.get(path[0]).and_then(|v| v.as_str())
                && let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(raw)
            {
                let canonical = k8s_microtime_format(parsed.with_timezone(&chrono::Utc));
                if let Some(obj) = value.as_object_mut() {
                    obj.insert(path[0].to_string(), serde_json::Value::String(canonical));
                }
            }
            return;
        }
        if let Some(next) = value.get_mut(path[0]) {
            normalize_path(next, &path[1..]);
        }
    }

    normalize_path(value, &["eventTime"]);
    normalize_path(value, &["series", "lastObservedTime"]);
}

/// Legacy alias retained for callers that historically called
/// `crate::utils::k8s_timestamp()`. New code should pick the precision
/// explicitly with [`k8s_time_now`] (Time fields) or [`k8s_microtime_now`]
/// (MicroTime fields).
pub fn k8s_timestamp() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S.%fZ")
        .to_string()
}

pub fn read_utf8_file(path: impl AsRef<std::path::Path>) -> std::io::Result<String> {
    let bytes = std::fs::read(path)?;
    String::from_utf8(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

pub fn create_dir_all(path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    std::fs::create_dir_all(path)
}

pub fn write_file(
    path: impl AsRef<std::path::Path>,
    contents: impl AsRef<[u8]>,
) -> std::io::Result<()> {
    std::fs::write(path, contents)
}

pub fn open_append_file(path: impl AsRef<std::path::Path>) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}

/// Set Unix permission bits on a file (e.g. `0o600` for private keys). Lives in
/// the filesystem-allowlisted utils module so callers outside the allowlist do
/// not use raw `std::fs` directly.
#[cfg(unix)]
pub fn set_unix_mode(path: impl AsRef<std::path::Path>, mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

pub async fn read_utf8_file_async(path: impl AsRef<std::path::Path>) -> std::io::Result<String> {
    let path_buf = path.as_ref().to_path_buf();
    let key = path_buf.to_string_lossy().into_owned();
    crate::kubelet::file_blocking::run_blocking_file_keyed("utils_read_utf8_file", key, move || {
        read_utf8_file(path_buf).map_err(anyhow::Error::from)
    })
    .await
    .map_err(|e| {
        if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
            std::io::Error::new(io_err.kind(), io_err.to_string())
        } else {
            std::io::Error::other(e.to_string())
        }
    })
}

pub async fn create_dir_all_async(path: impl AsRef<std::path::Path>) -> anyhow::Result<()> {
    let path_buf = path.as_ref().to_path_buf();
    let key = path_buf.to_string_lossy().into_owned();
    crate::kubelet::file_blocking::run_blocking_file_keyed("utils_create_dir_all", key, move || {
        std::fs::create_dir_all(path_buf).map_err(anyhow::Error::from)
    })
    .await
}

pub async fn write_file_async(
    path: impl AsRef<std::path::Path>,
    contents: impl AsRef<[u8]>,
) -> anyhow::Result<()> {
    let path_buf = path.as_ref().to_path_buf();
    let bytes = contents.as_ref().to_vec();
    let key = path_buf.to_string_lossy().into_owned();
    crate::kubelet::file_blocking::run_blocking_file_keyed("utils_write_file", key, move || {
        std::fs::write(path_buf, bytes).map_err(anyhow::Error::from)
    })
    .await
}

pub async fn path_exists_async(path: impl AsRef<std::path::Path>) -> anyhow::Result<bool> {
    let path_buf = path.as_ref().to_path_buf();
    let key = path_buf.to_string_lossy().into_owned();
    crate::kubelet::file_blocking::run_blocking_file_keyed("utils_path_exists", key, move || {
        match std::fs::metadata(path_buf) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(anyhow::Error::from(e)),
        }
    })
    .await
}

pub async fn canonicalize_async(
    path: impl AsRef<std::path::Path>,
) -> anyhow::Result<std::path::PathBuf> {
    let path_buf = path.as_ref().to_path_buf();
    let key = path_buf.to_string_lossy().into_owned();
    crate::kubelet::file_blocking::run_blocking_file_keyed("utils_canonicalize", key, move || {
        std::fs::canonicalize(path_buf).map_err(anyhow::Error::from)
    })
    .await
}

pub async fn remove_file_if_exists_async(
    path: impl AsRef<std::path::Path>,
) -> anyhow::Result<bool> {
    let path_buf = path.as_ref().to_path_buf();
    let key = path_buf.to_string_lossy().into_owned();
    crate::kubelet::file_blocking::run_blocking_file_keyed(
        "utils_remove_file_if_exists",
        key,
        move || match std::fs::remove_file(path_buf) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(anyhow::Error::from(e)),
        },
    )
    .await
}

pub async fn remove_dir_if_exists_async(path: impl AsRef<std::path::Path>) -> anyhow::Result<bool> {
    let path_buf = path.as_ref().to_path_buf();
    let key = path_buf.to_string_lossy().into_owned();
    crate::kubelet::file_blocking::run_blocking_file_keyed(
        "utils_remove_dir_if_exists",
        key,
        move || match std::fs::remove_dir(path_buf) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(anyhow::Error::from(e)),
        },
    )
    .await
}

pub async fn remove_dir_all_if_exists_async(
    path: impl AsRef<std::path::Path>,
) -> anyhow::Result<bool> {
    let path_buf = path.as_ref().to_path_buf();
    let key = path_buf.to_string_lossy().into_owned();
    crate::kubelet::file_blocking::run_blocking_file_keyed(
        "utils_remove_dir_all_if_exists",
        key,
        move || match std::fs::remove_dir_all(path_buf) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(anyhow::Error::from(e)),
        },
    )
    .await
}
/// Extract resourceVersion from a K8s `metadata` sub-object as i64.
///
/// The argument is the *metadata block*, e.g. `pod.get("metadata")`. Returns 0
/// if missing or unparseable. For a full resource object, use
/// [`extract_resource_version_from_object`].
pub fn extract_resource_version(metadata: &serde_json::Value) -> i64 {
    metadata
        .get("resourceVersion")
        .and_then(|rv| rv.as_str())
        .and_then(|rv| rv.parse::<i64>().ok())
        .unwrap_or(0)
}

/// Extract resourceVersion from a full K8s resource object as i64. Navigates
/// `/metadata/resourceVersion` so callers don't have to remember to pass the
/// metadata sub-object. Returns 0 if missing or unparseable.
pub fn extract_resource_version_from_object(object: &serde_json::Value) -> i64 {
    object
        .pointer("/metadata/resourceVersion")
        .and_then(|rv| rv.as_str())
        .and_then(|rv| rv.parse::<i64>().ok())
        .unwrap_or(0)
}

/// Derive the first usable IP from a CIDR (network address + 1).
/// Example: "10.43.0.0/17" -> "10.43.0.1", "10.43.128.0/17" -> "10.43.128.1"
///
/// Returns "0.0.0.0" on an unparseable CIDR (the previous behavior of
/// `derive_first_ip`-style callers, used for legacy auth/kube-service init).
pub fn derive_first_ip(cidr: &str) -> String {
    use crate::networking::ClusterCidr;
    match ClusterCidr::parse(cidr) {
        Ok(c) => ip_u32_to_string(c.network() + 1),
        Err(_) => "0.0.0.0".to_string(),
    }
}

/// Convert a u32 IP address to dotted-quad string (e.g., "10.43.0.2").
pub fn ip_u32_to_string(ip: u32) -> String {
    format!(
        "{}.{}.{}.{}",
        (ip >> 24) & 0xFF,
        (ip >> 16) & 0xFF,
        (ip >> 8) & 0xFF,
        ip & 0xFF
    )
}

/// Generate a K8s resource name by appending a random 5-character lowercase alphanumeric suffix.
/// Example: "sonobuoy-" -> "sonobuoy-a7k2x"
pub fn generate_name(prefix: &str) -> String {
    use rand::distr::{Distribution, Uniform};
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    let range = Uniform::new(0, CHARSET.len()).expect("valid range");
    let suffix: String = (0..5)
        .map(|_| {
            let idx = range.sample(&mut rng);
            CHARSET[idx] as char
        })
        .collect();
    format!("{}{}", prefix, suffix)
}

/// One-shot reconnect backoff shared by every stream-reconnect loop — leader
/// watch streams (gRPC server-streaming `watch_resources`: worker store mirror,
/// remote informer, service-routing watch set) and the local CRI event stream.
/// Exponential from 500ms, doubling per consecutive failed reconnect, capped at
/// 60s: 500ms, 1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s…
///
/// Centralised so no reconnect loop can regress to a fixed short interval, which
/// under a sustained failure turns N stream scopes × M nodes into a reconnect
/// storm that worsens the very contention the loop is waiting out. `attempt` is
/// the count of consecutive failures; callers reset it to 0 once the stream
/// makes progress (an event is received), so a healthy stream that blips
/// reconnects in 500ms.
///
/// Deterministic (no jitter) to match the codebase's other backoff helpers and
/// keep tests stable.
pub fn watch_reconnect_delay(attempt: u32) -> std::time::Duration {
    const BASE_MS: u64 = 500;
    const MAX_MS: u64 = 60_000;
    // 500 << 7 = 64000 (> cap); clamp the shift so the multiply never overflows.
    let shift = attempt.min(7);
    std::time::Duration::from_millis((BASE_MS << shift).min(MAX_MS))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watch_reconnect_delay_is_exponential_from_500ms_capped_at_60s() {
        use std::time::Duration;
        assert_eq!(watch_reconnect_delay(0), Duration::from_millis(500));
        assert_eq!(watch_reconnect_delay(1), Duration::from_secs(1));
        assert_eq!(watch_reconnect_delay(2), Duration::from_secs(2));
        assert_eq!(watch_reconnect_delay(3), Duration::from_secs(4));
        assert_eq!(watch_reconnect_delay(4), Duration::from_secs(8));
        assert_eq!(watch_reconnect_delay(5), Duration::from_secs(16));
        assert_eq!(watch_reconnect_delay(6), Duration::from_secs(32));
        // Caps at 60s (500<<7 = 64s would exceed it) and stays there.
        assert_eq!(watch_reconnect_delay(7), Duration::from_secs(60));
        assert_eq!(watch_reconnect_delay(8), Duration::from_secs(60));
        assert_eq!(watch_reconnect_delay(1000), Duration::from_secs(60));
    }

    #[test]
    fn test_k8s_timestamp_format() {
        // Legacy helper still emits 9-digit fractional + Z (kept for callers
        // that pre-date the time/microtime split).
        let ts = k8s_timestamp();
        assert!(ts.ends_with("Z"));
        assert!(!ts.contains("+"));
        assert!(ts.contains("."));
        assert_eq!(ts.len(), 30);
    }

    #[test]
    fn test_k8s_time_format_is_second_precision_z_suffixed() {
        let dt = chrono::DateTime::parse_from_rfc3339("2026-04-23T12:34:56.789012345+00:00")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(k8s_time_format(dt), "2026-04-23T12:34:56Z");
    }

    #[test]
    fn test_k8s_microtime_format_is_microsecond_precision_z_suffixed() {
        // Exactly 6 fractional digits, Z suffix — matches K8s upstream
        // metav1.MicroTime serialization. Trailing zeros are preserved.
        let dt = chrono::DateTime::parse_from_rfc3339("2026-04-23T12:34:56.123456789+00:00")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(k8s_microtime_format(dt), "2026-04-23T12:34:56.123456Z");

        // Sub-microsecond (just 0.001s) still emits 6 padded digits.
        let dt2 = chrono::DateTime::parse_from_rfc3339("2026-04-23T12:34:56.001+00:00")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(k8s_microtime_format(dt2), "2026-04-23T12:34:56.001000Z");

        // Zero fractional still emits the trailing .000000Z (not a bare Z).
        let dt3 = chrono::DateTime::parse_from_rfc3339("2026-04-23T12:34:56+00:00")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(k8s_microtime_format(dt3), "2026-04-23T12:34:56.000000Z");
    }

    #[test]
    fn test_k8s_microtime_now_shape_matches_canonical_microtime() {
        let ts = k8s_microtime_now();
        assert!(ts.ends_with("Z"), "MicroTime must end with Z, got: {ts}");
        assert!(
            !ts.contains("+"),
            "MicroTime must not have offset, got: {ts}"
        );
        // YYYY-MM-DDTHH:MM:SS.ffffffZ = 27 chars exactly.
        assert_eq!(ts.len(), 27, "MicroTime must be 27 chars, got: {ts:?}");
    }

    #[test]
    fn test_normalize_event_microtime_fields_accepts_rfc3339_variants() {
        let mut event = serde_json::json!({
            "eventTime": "2017-09-19T13:49:16+00:00",
            "series": {"lastObservedTime": "2017-09-19T13:49:16.123456789+00:00"}
        });

        normalize_event_microtime_fields(&mut event);

        assert_eq!(event["eventTime"], "2017-09-19T13:49:16.000000Z");
        assert_eq!(
            event["series"]["lastObservedTime"],
            "2017-09-19T13:49:16.123456Z"
        );
    }

    #[test]
    fn test_extract_resource_version_valid() {
        let metadata = serde_json::json!({"resourceVersion": "42"});
        assert_eq!(extract_resource_version(&metadata), 42);
    }

    #[test]
    fn test_extract_resource_version_missing() {
        let metadata = serde_json::json!({"name": "test"});
        assert_eq!(extract_resource_version(&metadata), 0);
    }

    #[test]
    fn test_extract_resource_version_from_object_navigates_metadata() {
        // Full K8s resource shape — resourceVersion lives under metadata.
        let obj = serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": "mn-worker",
                "namespace": "kube-node-lease",
                "resourceVersion": "58",
                "uid": "abcd",
            },
            "spec": {"holderIdentity": "mn-worker"},
        });
        assert_eq!(extract_resource_version_from_object(&obj), 58);
    }

    #[test]
    fn test_extract_resource_version_from_object_missing_returns_zero() {
        let obj = serde_json::json!({"metadata": {"name": "noversion"}});
        assert_eq!(extract_resource_version_from_object(&obj), 0);
    }

    #[test]
    fn test_extract_resource_version_on_full_object_does_not_silently_zero() {
        // Regression guard: passing a full resource object to the metadata-only
        // helper used to silently return 0, which broke worker lease renewal
        // (preconditions targeted rv=0). Callers must use
        // `extract_resource_version_from_object` for full resources.
        let obj = serde_json::json!({
            "metadata": {"resourceVersion": "99"},
        });
        assert_eq!(extract_resource_version(&obj), 0);
        assert_eq!(extract_resource_version_from_object(&obj), 99);
    }

    #[test]
    fn test_ip_u32_to_string() {
        assert_eq!(ip_u32_to_string((10 << 24) | (43 << 16) | 2), "10.43.0.2");
        assert_eq!(
            ip_u32_to_string((192 << 24) | (168 << 16) | (1 << 8) | 1),
            "192.168.1.1"
        );
    }

    #[test]
    fn test_derive_first_ip() {
        assert_eq!(derive_first_ip("10.43.0.0/17"), "10.43.0.1");
        assert_eq!(derive_first_ip("10.43.128.0/17"), "10.43.128.1");
        assert_eq!(derive_first_ip("10.50.0.0/17"), "10.50.0.1");
        assert_eq!(derive_first_ip("10.50.128.0/17"), "10.50.128.1");
        assert_eq!(derive_first_ip("192.168.0.0/24"), "192.168.0.1");
        assert_eq!(derive_first_ip("172.16.0.0/16"), "172.16.0.1");
        assert_eq!(derive_first_ip("10.44.128.0/17"), "10.44.128.1");
    }

    #[test]
    fn test_generate_name_produces_valid_suffix() {
        let name = generate_name("sonobuoy-");
        assert!(
            name.starts_with("sonobuoy-"),
            "Generated name should start with prefix"
        );
        assert_eq!(
            name.len(),
            "sonobuoy-".len() + 5,
            "Generated name should be prefix + 5 chars"
        );
        let suffix = &name["sonobuoy-".len()..];
        assert!(
            suffix
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
            "Suffix should be lowercase alphanumeric only, got: {}",
            suffix
        );
    }

    #[test]
    fn test_generate_name_uniqueness() {
        let name1 = generate_name("test-");
        let name2 = generate_name("test-");
        assert_ne!(
            name1, name2,
            "Calling generate_name twice should produce different names"
        );
    }

    #[test]
    fn test_generate_name_empty_prefix() {
        let name = generate_name("");
        assert_eq!(name.len(), 5, "Empty prefix should produce 5-char name");
        assert!(
            name.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
            "Should be all lowercase alphanumeric, got: {}",
            name
        );
    }

    #[test]
    fn test_generate_name_long_prefix() {
        let long_prefix = "a".repeat(200);
        let name = generate_name(&long_prefix);
        assert_eq!(name.len(), 205, "Long prefix + 5-char suffix");
        assert!(name.starts_with(&long_prefix));
        let suffix = &name[200..];
        assert!(
            suffix
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
            "Suffix must be lowercase alphanumeric even with long prefix, got: {}",
            suffix
        );
    }

    #[test]
    fn test_generate_name_special_chars_prefix() {
        // generateName prefix may contain hyphens, dots, etc.
        // The suffix must always be lowercase alphanumeric regardless
        let name = generate_name("my-app.v2-");
        assert!(name.starts_with("my-app.v2-"));
        let suffix = &name["my-app.v2-".len()..];
        assert_eq!(suffix.len(), 5);
        assert!(
            suffix
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
            "Suffix must be alphanumeric regardless of prefix chars, got: {}",
            suffix
        );
    }
}
