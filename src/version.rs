//! Build-time version information for klights
//!
//! Version format: v{k8s major.minor.patch}+klights{klights version}
//! Example: v1.34.6+klights1.0.2
//!
//! Version is derived from git tags (e.g., v1.0.0 → 1.0.0)

/// k8s API version we're compatible with
pub const K8S_VERSION: &str = "1.34.6";

/// Full K8s-compatible git version string.
pub const GIT_VERSION: &str = concat!("v1.34.6+klights", env!("KLIGHTS_GIT_VERSION"));

/// Short git commit hash (first 8 chars of HEAD) baked in at build time.
pub const GIT_COMMIT_SHORT: &str = env!("KLIGHTS_GIT_COMMIT_SHORT");

/// `klights --version` string: full K8s version plus the short commit hash so
/// operators can correlate a running binary back to a specific build. clap
/// requires a `&'static str`, hence the `concat!` const.
pub const GIT_VERSION_WITH_COMMIT: &str = concat!(
    "v1.34.6+klights",
    env!("KLIGHTS_GIT_VERSION"),
    " ",
    env!("KLIGHTS_GIT_COMMIT_SHORT"),
);

/// Get klights version from git tag (e.g., "v1.0.0" → "1.0.0", "1.0.0" → "1.0.0")
///
/// Validation is done in build.rs at compile time
#[cfg(test)]
fn klights_version_from_git() -> &'static str {
    env!("KLIGHTS_GIT_VERSION")
}

fn k8s_major_minor_version() -> (&'static str, &'static str) {
    let mut parts = K8S_VERSION.split('.');
    (parts.next().unwrap_or("1"), parts.next().unwrap_or("34"))
}

/// Full git version string
///
/// Format: v{k8s_major_minor_patch}+klights{klights_version}
/// Example: v1.34.6+klights1.0.2
pub fn git_version() -> String {
    GIT_VERSION.to_string()
}

/// kubeletVersion for Node status.
pub fn kubelet_version_for_mode(node_mode: &crate::bootstrap::NodeMode) -> String {
    let mut version = git_version();
    if matches!(node_mode, crate::bootstrap::NodeMode::Rootless { .. }) {
        version.push_str(" (rootless)");
    }
    version
}

/// Git commit SHA (from vergen, empty if not built from git)
pub fn git_commit_hash() -> &'static str {
    option_env!("VERGEN_GIT_SHA").unwrap_or("unknown")
}

/// Build timestamp (from vergen, empty if not available)
pub fn build_date() -> &'static str {
    option_env!("VERGEN_BUILD_TIMESTAMP").unwrap_or("")
}

/// Rustc version used to compile
pub fn rustc_version() -> &'static str {
    option_env!("VERGEN_RUSTC_SEMVER").unwrap_or("unknown")
}

/// Target triple
pub fn target_triple() -> &'static str {
    option_env!("VERGEN_CARGO_TARGET_TRIPLE").unwrap_or("unknown")
}

/// Git tree state (clean or dirty)
pub fn git_tree_state() -> &'static str {
    match option_env!("VERGEN_GIT_DIRTY") {
        Some("true") => "dirty",
        Some("false") => "clean",
        _ => "unknown",
    }
}

/// K8s-style version info for /version endpoint
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionInfo {
    pub major: String,
    pub minor: String,
    pub git_version: String,
    pub git_commit: String,
    pub git_tree_state: String,
    pub build_date: String,
    pub go_version: String,
    pub compiler: String,
    pub platform: String,
}

impl Default for VersionInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionInfo {
    pub fn new() -> Self {
        let (major, minor) = k8s_major_minor_version();

        VersionInfo {
            major: major.to_string(),
            minor: minor.to_string(),
            git_version: git_version(),
            git_commit: git_commit_hash().to_string(),
            git_tree_state: git_tree_state().to_string(),
            build_date: build_date().to_string(),
            go_version: "go1.22.5".to_string(), // For K8s API compatibility
            compiler: format!("rustc {}", rustc_version()),
            platform: target_triple().to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_git_version_format() {
        let v = git_version();
        assert!(v.starts_with('v'), "version must start with 'v'");
        assert!(v.contains("+klights"), "version must contain '+klights'");
        assert!(
            !v.contains("+Klights"),
            "version must use lowercase '+klights'"
        );
        assert!(
            !v.ends_with("-dev"),
            "version must be tag-derived without a debug suffix"
        );
    }

    #[test]
    fn test_git_version_uses_k8s_minor_and_lowercase_klights_tag() {
        let v = git_version();
        assert_eq!(
            v,
            format!("v{}+klights{}", K8S_VERSION, klights_version_from_git())
        );
    }

    #[test]
    fn test_git_version_uses_full_semver_core_before_build_metadata() {
        let v = git_version();
        let core = v
            .strip_prefix('v')
            .and_then(|value| value.split_once('+').map(|(core, _)| core))
            .expect("gitVersion must be v<semver>+<metadata>");
        let parts: Vec<&str> = core.split('.').collect();
        assert_eq!(
            parts.len(),
            3,
            "kubectl rejects gitVersion values without major.minor.patch: {v}"
        );
    }

    #[test]
    fn test_version_info_serialization() {
        let info = VersionInfo::new();
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("gitVersion"));
    }

    #[test]
    fn test_version_info_k8s_compatible_fields() {
        let info = VersionInfo::new();
        // Ensure all K8s required fields exist
        assert!(!info.major.is_empty());
        assert!(!info.minor.is_empty());
        assert!(!info.git_version.is_empty());
        assert!(!info.compiler.is_empty());
        assert!(!info.platform.is_empty());
    }

    #[test]
    fn test_k8s_version_constant() {
        assert!(!K8S_VERSION.is_empty());
        assert_eq!(K8S_VERSION, "1.34.6");
    }

    #[test]
    fn test_git_tree_state_parsing() {
        // Test that the function handles all cases
        let state = git_tree_state();
        assert!(state == "clean" || state == "dirty" || state == "unknown");
    }

    #[test]
    fn test_klights_version_from_git_format() {
        // The version from git should be a valid semver string (x.y.z)
        let v = klights_version_from_git();
        let parts: Vec<&str> = v.split('.').collect();

        // Should have exactly 3 parts (major.minor.patch)
        assert_eq!(
            parts.len(),
            3,
            "version should have 3 parts (x.y.z format): {}",
            v
        );

        // Each part should be numeric
        for (i, part) in parts.iter().enumerate() {
            assert!(
                part.parse::<u32>().is_ok(),
                "version part {} should be numeric: '{}'",
                i,
                part
            );
        }

        // Should not start with 'v' (we strip it)
        assert!(
            !v.starts_with('v'),
            "version should not start with 'v': {}",
            v
        );
    }

    // The git tag rerun invariant is enforced by
    // `scripts/check_deploy_invariants.sh`, run as part of `./build.sh`.
}
