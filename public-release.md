# Public release checklist

Internal checklist for publishing `klights-core` to the public GitHub repository.

## Public repository

Repository:

```text
git@github.com:klights-net/klights-core.git
https://github.com/klights-net/klights-core
```

Package repository URLs after GitHub Pages deployment:

```text
https://klights-net.github.io/klights-core/apt
https://klights-net.github.io/klights-core/rpm
```

GitHub Releases URL:

```text
https://github.com/klights-net/klights-core/releases
```

## GitHub settings required before first tag

- Enable GitHub Actions for the repository.
- Enable GitHub Pages with source set to GitHub Actions.
- Confirm workflow permissions allow `Read and write permissions` if organization policy does not honor workflow-level `contents: write`.
- Confirm public repository visibility before pushing release tags.
- Confirm branch protection does not block tag-triggered release workflows.

## Optional signing secrets

The release workflow works without signing secrets. If package repository signing is required, configure these repository secrets before tagging:

```text
PACKAGE_GPG_PRIVATE_KEY
PACKAGE_GPG_PASSPHRASE
```

Do not commit private keys, passphrases, tokens, kubeconfigs, local paths, or machine-specific config into the public repository.

## Release trigger

The public release workflow runs on pushed tags matching:

```text
v*
```

Expected tag format:

```text
vMAJOR.MINOR.PATCH
```

The workflow strips the leading `v` and rejects tags that are not strict
`vMAJOR.MINOR.PATCH` versions.

Before tagging, add or update the matching section in `CHANGELOG.md`:

```text
## [MAJOR.MINOR.PATCH] - YYYY-MM-DD
```

GitHub Releases are the canonical public release notes. The workflow extracts
the matching changelog section into the GitHub Release body and attaches the
binary package assets.

Example:

```bash
git tag v0.1.0
git push public v0.1.0
```

Use the actual remote name configured in `klights-core` for `git@github.com:klights-net/klights-core.git`.

## Workflow outputs

For each release tag, verify GitHub Release assets include:

```text
klights_VERSION-1~noble_amd64.deb
klights_VERSION-1~resolute_amd64.deb
klights-VERSION-1.el9.x86_64.rpm
klights-VERSION-1.el10.x86_64.rpm
SHA256SUMS
```

Verify GitHub Pages contains these repository paths:

```text
apt/dists/noble/main/binary-amd64/Packages
apt/dists/noble/main/binary-amd64/Packages.gz
apt/dists/noble/Release
apt/dists/resolute/main/binary-amd64/Packages
apt/dists/resolute/main/binary-amd64/Packages.gz
apt/dists/resolute/Release
rpm/el9/x86_64/repodata/repomd.xml
rpm/el10/x86_64/repodata/repomd.xml
```

If signing secrets are configured, also verify:

```text
apt/dists/noble/InRelease
apt/dists/noble/Release.gpg
apt/dists/resolute/InRelease
apt/dists/resolute/Release.gpg
rpm/el9/x86_64/repodata/repomd.xml.asc
rpm/el10/x86_64/repodata/repomd.xml.asc
```

## Package contents to verify

Debian packages must contain:

```text
/usr/bin/klights
/lib/systemd/system/klights.service
/etc/default/klights
```

RPM packages must contain:

```text
/usr/bin/klights
/usr/lib/systemd/system/klights.service
/etc/sysconfig/klights
```

The systemd service must default to:

```text
RUST_LOG=info
```

The default must be overrideable through distro environment files.

## Pre-public checks

Before pushing the first public tag:

- Confirm `Cargo.toml` public metadata points to the public GitHub repository, not private or legacy remotes.
- Confirm README and docs do not mention private remotes, private infrastructure, local hostnames, kubeconfigs, tokens, or personal machine paths.
- Confirm no generated binaries or package artifacts are committed to git.
- Confirm `.github/workflows/release.yml` uses only GitHub encrypted secrets by reference, not literal secret values.
- Confirm `release.sh` works from the `klights-core` repository root.
- Confirm package scripts do not depend on files outside `klights-core`.
- Confirm package metadata license matches the project license.
- Confirm the public remote in `klights-core` points to `git@github.com:klights-net/klights-core.git`.

## Release verification commands

Run from `klights-core` after package artifacts are produced locally or downloaded from GitHub Releases.

Inspect Debian package contents:

```bash
dpkg-deb -c klights_VERSION-1~noble_amd64.deb
dpkg-deb -c klights_VERSION-1~resolute_amd64.deb
```

Inspect Debian package metadata:

```bash
dpkg-deb -I klights_VERSION-1~noble_amd64.deb
dpkg-deb -I klights_VERSION-1~resolute_amd64.deb
```

Inspect RPM package contents:

```bash
rpm -qpl klights-VERSION-1.el9.x86_64.rpm
rpm -qpl klights-VERSION-1.el10.x86_64.rpm
```

Inspect RPM package metadata:

```bash
rpm -qpi klights-VERSION-1.el9.x86_64.rpm
rpm -qpi klights-VERSION-1.el10.x86_64.rpm
```

Verify checksums:

```bash
sha256sum -c SHA256SUMS
```

Smoke-test APT metadata after Pages deploy:

```bash
curl -fsSL https://klights-net.github.io/klights-core/apt/dists/noble/Release
curl -fsSL https://klights-net.github.io/klights-core/apt/dists/resolute/Release
```

Smoke-test RPM metadata after Pages deploy:

```bash
curl -fsSL https://klights-net.github.io/klights-core/rpm/el9/x86_64/repodata/repomd.xml
curl -fsSL https://klights-net.github.io/klights-core/rpm/el10/x86_64/repodata/repomd.xml
```

## Public release sequence

1. Review the public repository diff in `klights-core`.
2. Run the normal project release/build gate required for the release branch.
3. Push the release commit to `git@github.com:klights-net/klights-core.git`.
4. Push the `vMAJOR.MINOR.PATCH` tag.
5. Watch the GitHub Actions `release` workflow.
6. Verify GitHub Release assets.
7. Verify GitHub Pages APT/RPM metadata URLs.
8. Install-test the packages on Ubuntu 24.04, Ubuntu 26.04, RHEL 9, and RHEL 10 test hosts before announcing public availability.
