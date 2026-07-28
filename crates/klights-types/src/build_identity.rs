//! Immutable build identity passed from composition into feature code.

/// Version and source revision of the running binary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildIdentity {
    kubelet_version: String,
    git_commit: String,
}

impl BuildIdentity {
    pub fn new(kubelet_version: impl Into<String>, git_commit: impl Into<String>) -> Self {
        Self {
            kubelet_version: kubelet_version.into(),
            git_commit: git_commit.into(),
        }
    }

    pub fn kubelet_version(&self) -> &str {
        &self.kubelet_version
    }

    pub fn git_commit(&self) -> &str {
        &self.git_commit
    }
}

#[cfg(test)]
mod tests {
    use super::BuildIdentity;

    #[test]
    fn preserves_root_supplied_build_facts() {
        let identity = BuildIdentity::new("v1.34.6+klights1.0.0", "abcdef12");
        assert_eq!(identity.kubelet_version(), "v1.34.6+klights1.0.0");
        assert_eq!(identity.git_commit(), "abcdef12");
    }
}
