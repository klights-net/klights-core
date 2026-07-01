# Klights package repositories

The `package-repo` branch hosts the static package indexes:
- Apt: `https://raw.githubusercontent.com/klights-net/klights-core/package-repo/apt`
- RPM: `https://raw.githubusercontent.com/klights-net/klights-core/package-repo/rpm`

Binary package assets are published on GitHub Releases.

Ubuntu 24.04 (`noble`) and Ubuntu 26.04 (`resolute`) examples:
```bash
sudo install -d -m 0755 /etc/apt/keyrings
sudo curl -fsSL \
  https://raw.githubusercontent.com/klights-net/klights-core/package-repo/klights-archive-keyring.asc \
  -o /etc/apt/keyrings/klights-archive-keyring.asc
sudo chmod 0644 /etc/apt/keyrings/klights-archive-keyring.asc
sudo tee /etc/apt/sources.list.d/klights.sources >/dev/null <<'EOF'
Types: deb
URIs: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/apt/
Suites: noble
Components: main
Signed-By: /etc/apt/keyrings/klights-archive-keyring.asc
EOF
sudo apt-get update && sudo apt-get install -y klights
```
For Ubuntu 26.04, use `Suites: resolute` in the source stanza instead of
`Suites: noble`.

RHEL 9 (`el9`) and RHEL 10 (`el10`) examples:
```bash
sudo install -d -m 0755 /etc/pki/rpm-gpg
sudo curl -fsSL \
  https://raw.githubusercontent.com/klights-net/klights-core/package-repo/klights-archive-keyring.asc \
  -o /etc/pki/rpm-gpg/klights-archive-keyring.asc
grep -q "BEGIN PGP PUBLIC KEY BLOCK" /etc/pki/rpm-gpg/klights-archive-keyring.asc
sudo rpm --import /etc/pki/rpm-gpg/klights-archive-keyring.asc
sudo tee /etc/yum.repos.d/klights.repo >/dev/null <<'EOF'
[klights]
name=Klights
baseurl=https://raw.githubusercontent.com/klights-net/klights-core/package-repo/rpm/el9/x86_64
enabled=1
repo_gpgcheck=1
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/klights-archive-keyring.asc
EOF
sudo dnf install -y klights
```
For RHEL 10, use `el10` in `baseurl`.

`/etc/default/klights` and `/etc/sysconfig/klights` are installed with Ubuntu and RHEL
packages and control `KLIGHTS_ARGS` (service arguments). Logging defaults to `RUST_LOG=info`
unless overridden in those files.
