# Klights package repositories

GitHub Pages hosts the static package indexes:
- Apt: `https://klights-net.github.io/klights-core/apt`
- RPM: `https://klights-net.github.io/klights-core/rpm`

Binary package assets are published on GitHub Releases.

Ubuntu 24.04 (`noble`) and Ubuntu 26.04 (`resolute`) examples:
```bash
sudo install -d /etc/apt/keyrings
# Unsigned fallback (no GPG key configured):
echo "deb [trusted=yes] https://klights-net.github.io/klights-core/apt noble main" | \
  sudo tee /etc/apt/sources.list.d/klights.list
# Signed repo (configure your keyring):
# echo "deb [signed-by=/etc/apt/keyrings/klights-archive-keyring.gpg] https://klights-net.github.io/klights-core/apt noble main" | \
#   sudo tee /etc/apt/sources.list.d/klights.list
sudo apt-get update && sudo apt-get install -y klights
```
For Ubuntu 26.04, use `resolute` in the repo stanza instead of `noble`.

RHEL 9 (`el9`) and RHEL 10 (`el10`) examples:
```bash
cat >/etc/yum.repos.d/klights.repo <<'EOF'
[klights]
name=Klights
baseurl=https://klights-net.github.io/klights-core/rpm/el9
gpgcheck=0
# Signed repo (set your keyring path):
# gpgcheck=1
# gpgkey=file:///etc/pki/rpm-gpg/klights-RPM-GPG-KEY
enabled=1
EOF
sudo dnf install -y klights
```
For RHEL 10, use `el10` in `baseurl`.

`/etc/default/klights` and `/etc/sysconfig/klights` are installed with Ubuntu and RHEL
packages and control `KLIGHTS_ARGS` (service arguments). Logging defaults to `RUST_LOG=info`
unless overridden in those files.
