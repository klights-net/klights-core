//! Kubernetes `/version` response owned by the permanent HTTP adapter.

/// K8s-compatible version payload.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct VersionInfo {
    pub(crate) major: String,
    pub(crate) minor: String,
    pub(crate) git_version: String,
    pub(crate) git_commit: String,
    pub(crate) git_tree_state: String,
    pub(crate) build_date: String,
    pub(crate) go_version: String,
    pub(crate) compiler: String,
    pub(crate) platform: String,
}

impl VersionInfo {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        major: impl Into<String>,
        minor: impl Into<String>,
        git_version: impl Into<String>,
        git_commit: impl Into<String>,
        git_tree_state: impl Into<String>,
        build_date: impl Into<String>,
        compiler: impl Into<String>,
        platform: impl Into<String>,
    ) -> Self {
        Self {
            major: major.into(),
            minor: minor.into(),
            git_version: git_version.into(),
            git_commit: git_commit.into(),
            git_tree_state: git_tree_state.into(),
            build_date: build_date.into(),
            go_version: "go1.22.5".to_string(),
            compiler: compiler.into(),
            platform: platform.into(),
        }
    }
}
