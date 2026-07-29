//! Unforgeable construction authorities for destructive cluster-store ports.

pub(crate) struct SnapshotInstallAuthority {
    _private: (),
}

pub(crate) const fn snapshot_install() -> SnapshotInstallAuthority {
    SnapshotInstallAuthority { _private: () }
}
