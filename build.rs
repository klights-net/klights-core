use std::path::PathBuf;
use std::process::Command;
use vergen::EmitBuilder;

fn main() {
    println!("cargo:rerun-if-changed=proto/replication.proto");
    println!("cargo:rerun-if-changed=.git/refs/tags");
    println!("cargo:rerun-if-changed=.git/packed-refs");
    let descriptor_path = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"))
        .join("klights_replication_descriptor.bin");
    tonic_prost_build::configure()
        .build_transport(false)
        .file_descriptor_set_path(&descriptor_path)
        .type_attribute(
            "klights.replication.LeaderMessage.payload",
            "#[allow(clippy::large_enum_variant)]",
        )
        .compile_protos(&["proto/replication.proto"], &["proto"])
        .expect("failed to compile replication gRPC protobuf");

    let version = latest_git_version_tag().unwrap_or_else(|err| panic!("{err}"));

    println!("cargo:rustc-env=KLIGHTS_GIT_VERSION={}", version);

    let commit_short = short_commit_hash();
    println!("cargo:rustc-env=KLIGHTS_GIT_COMMIT_SHORT={}", commit_short);
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");

    // Configure vergen to emit version info at compile time
    EmitBuilder::builder()
        .all_build()
        .all_cargo()
        .all_git()
        .all_rustc()
        .all_sysinfo()
        .emit()
        .expect("Unable to generate vergen build info");
}

fn short_commit_hash() -> String {
    Command::new("git")
        .args(["rev-parse", "--short=8", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn latest_git_version_tag() -> Result<String, String> {
    let tags = Command::new("git")
        .args(["tag", "--list"])
        .output()
        .map_err(|e| format!("Failed to run git tag --list: {e}"))?;

    if !tags.status.success() {
        return Err("Failed to list git tags: git tag --list failed".to_string());
    }

    let stdout = String::from_utf8_lossy(&tags.stdout);
    let mut version_tags: Vec<ParsedTag> = stdout.lines().filter_map(parse_version_tag).collect();

    version_tags.sort_by(|a, b| {
        b.version
            .cmp(&a.version)
            .then_with(|| b.has_v_prefix.cmp(&a.has_v_prefix))
    });

    let Some(latest) = version_tags.into_iter().next() else {
        return Err(
            "No git version tag found. Expected latest tag in format vX.Y.Z, e.g. v1.0.0."
                .to_string(),
        );
    };

    Ok(latest.version_string)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedTag {
    version: (u32, u32, u32),
    version_string: String,
    has_v_prefix: bool,
}

fn parse_version_tag(tag: &str) -> Option<ParsedTag> {
    let tag = tag.trim();
    let (has_v_prefix, version) = if let Some(stripped) = tag.strip_prefix('v') {
        (true, stripped)
    } else {
        // Backward-compatible fallback for existing bare semver tags. Literal
        // vX.Y.Z tags are still preferred when both forms exist.
        (false, tag)
    };

    let mut parts = version.split('.');
    let major = parts.next()?.parse::<u32>().ok()?;
    let minor = parts.next()?.parse::<u32>().ok()?;
    let patch = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }

    Some(ParsedTag {
        version: (major, minor, patch),
        version_string: format!("{major}.{minor}.{patch}"),
        has_v_prefix,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_version_tag;

    #[test]
    fn parses_v_semver_tag() {
        let parsed = parse_version_tag("v1.2.3").expect("v tag should parse");
        assert_eq!(parsed.version, (1, 2, 3));
        assert_eq!(parsed.version_string, "1.2.3");
        assert!(parsed.has_v_prefix);
    }

    #[test]
    fn rejects_non_semver_tag() {
        assert!(parse_version_tag("single_node_coh_pass").is_none());
        assert!(parse_version_tag("v1.2").is_none());
        assert!(parse_version_tag("v1.2.3-rc1").is_none());
    }
}
