//! Unforgeable construction authorities for destructive cluster-store ports.

pub(crate) struct CommittedApplyAuthority {
    _private: (),
}

pub(crate) struct SnapshotInstallAuthority {
    _private: (),
}

pub(crate) const fn committed_apply() -> CommittedApplyAuthority {
    CommittedApplyAuthority { _private: () }
}

pub(crate) const fn snapshot_install() -> SnapshotInstallAuthority {
    SnapshotInstallAuthority { _private: () }
}
