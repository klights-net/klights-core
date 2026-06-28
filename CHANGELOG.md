# Changelog

All notable public release changes for `klights-core` are documented here.

This project uses GitHub Releases as the canonical public release page. The
release workflow extracts the matching version section from this file and
attaches distro packages to the GitHub Release.

## [0.9.10] - 2026-06-28

### Added

- Public tag-triggered GitHub Actions release workflow.
- Static binary packaging for Ubuntu 24.04 (`noble`) and Ubuntu 26.04 (`resolute`).
- Static binary packaging for RHEL 9 (`el9`) and RHEL 10 (`el10`).
- GitHub Pages publication for APT and RPM package repository metadata.
- Systemd service packaging with default `RUST_LOG=info`.
- Internal public release checklist in `public-release.md`.

### Notes

- GitHub Pages must be enabled with source set to GitHub Actions before the
  first public tag release.
- Optional repository signing uses GitHub encrypted secrets
  `PACKAGE_GPG_PRIVATE_KEY` and `PACKAGE_GPG_PASSPHRASE`.
