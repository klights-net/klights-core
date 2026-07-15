use std::path::{Path, PathBuf};
use std::process::Command;
use vergen::EmitBuilder;

fn main() {
    println!("cargo:rerun-if-changed=proto/replication.proto");
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

    // Configure vergen to emit version info at compile time
    EmitBuilder::builder()
        .all_build()
        .all_cargo()
        .git_branch()
        .git_commit_author_email()
        .git_commit_author_name()
        .git_commit_count()
        .git_commit_date()
        .git_commit_message()
        .git_commit_timestamp()
        .git_describe(false, false, None)
        .git_sha(false)
        .git_cmd(None)
        .all_rustc()
        .all_sysinfo()
        .emit()
        .expect("Unable to generate vergen build info");
    emit_git_dirty();
    emit_git_rerun_inputs();
}

fn emit_git_dirty() {
    println!("cargo:rerun-if-env-changed=VERGEN_GIT_DIRTY");
    let dirty = match std::env::var("VERGEN_GIT_DIRTY") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => live_git_dirty().to_string(),
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("VERGEN_GIT_DIRTY must be valid Unicode")
        }
    };
    println!("cargo:rustc-env=VERGEN_GIT_DIRTY={}", dirty);
}

fn live_git_dirty() -> bool {
    let manifest_dir = PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set"),
    );
    let output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .env("GIT_OPTIONAL_LOCKS", "0")
        .current_dir(manifest_dir)
        .output()
        .expect("failed to query git dirty state");
    assert!(output.status.success(), "git status failed");
    !output.stdout.is_empty()
}

fn emit_git_rerun_inputs() {
    for input in ["HEAD", "index", "refs/tags", "packed-refs"] {
        emit_existing_git_input(input);
    }
    if let Some(reference) = symbolic_head() {
        emit_git_input_or_namespace(&reference, "refs/heads");
    }
}

fn emit_existing_git_input(input: &str) {
    let Some(path) = git_path(input) else {
        return;
    };
    if path.exists() {
        emit_rerun_path(path);
    }
}

fn emit_git_input_or_namespace(input: &str, namespace: &str) {
    let Some(path) = git_path(input) else {
        return;
    };
    if path.exists() {
        emit_rerun_path(path);
    } else {
        emit_existing_git_input(namespace);
    }
}

fn emit_rerun_path(path: PathBuf) {
    let path = path.canonicalize().unwrap_or(path);
    println!("cargo:rerun-if-changed={}", path.display());
}

fn git_path(path: &str) -> Option<PathBuf> {
    let manifest_dir = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR")?);
    let output = Command::new("git")
        .args(["rev-parse", "--git-path", path])
        .current_dir(&manifest_dir)
        .output()
        .ok()?
        .stdout;
    let output = String::from_utf8(output).ok()?;
    let path = Path::new(output.trim());
    if path.is_absolute() {
        Some(path.to_path_buf())
    } else {
        Some(manifest_dir.join(path))
    }
}

fn symbolic_head() -> Option<String> {
    let manifest_dir = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR")?);
    let output = Command::new("git")
        .args(["symbolic-ref", "-q", "HEAD"])
        .current_dir(manifest_dir)
        .output()
        .ok()?
        .stdout;
    String::from_utf8(output)
        .ok()
        .map(|reference| reference.trim().to_string())
        .filter(|reference| !reference.is_empty())
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
