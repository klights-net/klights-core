# Quickstart

Install klights from the public package repository, then start the packaged
systemd services. The packages include the klights service unit, default
`RUST_LOG=info`, default `KLIGHTS_ARGS=start`, and package-manager dependency
metadata for the runtime packages.

Packages are published for:

- Ubuntu 24.04 (`noble`) on `amd64` and `arm64`
- Ubuntu 26.04 (`resolute`) on `amd64` and `arm64`
- RHEL-compatible 9 on `x86_64` and `aarch64`
- RHEL-compatible 10 on `x86_64` and `aarch64`

## Prerequisites

- Linux host with root access for runtime work
- `containerd`
- `iproute2` on Ubuntu or `iproute` on RHEL, including the `ip` command used
  by CNI setup and cleanup paths
- `nftables`, including the `nft` command used for rootful service routing
- `kmod`, including `modprobe` for kernel module setup such as `br_netfilter`
- `kubectl` for interacting with the generated kubeconfig

When installing from the public APT or RPM repositories, the package metadata
declares the runtime dependencies so the package manager installs them with
`klights`.

For source builds, also install:

- Rust toolchain with `cargo` and `rustc`
- Native networking build dependencies such as `pkg-config`, `libnftnl`, and
  `libmnl`

## Ubuntu 24.04

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
sudo apt-get update
sudo apt-get install -y klights
sudo systemctl enable --now containerd
sudo systemctl enable --now klights
```

## Ubuntu 26.04

```bash
sudo install -d -m 0755 /etc/apt/keyrings
sudo curl -fsSL \
  https://raw.githubusercontent.com/klights-net/klights-core/package-repo/klights-archive-keyring.asc \
  -o /etc/apt/keyrings/klights-archive-keyring.asc
sudo chmod 0644 /etc/apt/keyrings/klights-archive-keyring.asc
sudo tee /etc/apt/sources.list.d/klights.sources >/dev/null <<'EOF'
Types: deb
URIs: https://raw.githubusercontent.com/klights-net/klights-core/package-repo/apt/
Suites: resolute
Components: main
Signed-By: /etc/apt/keyrings/klights-archive-keyring.asc
EOF
sudo apt-get update
sudo apt-get install -y klights
sudo systemctl enable --now containerd
sudo systemctl enable --now klights
```

## RHEL-Compatible 9

```bash
sudo install -d -m 0755 /etc/pki/rpm-gpg
sudo curl -fsSL \
  https://raw.githubusercontent.com/klights-net/klights-core/package-repo/klights-archive-keyring.asc \
  -o /etc/pki/rpm-gpg/klights-archive-keyring.asc
grep -q "BEGIN PGP PUBLIC KEY BLOCK" /etc/pki/rpm-gpg/klights-archive-keyring.asc
sudo rpm --import /etc/pki/rpm-gpg/klights-archive-keyring.asc
sudo tee /etc/yum.repos.d/klights.repo >/dev/null <<'EOF'
[klights]
name=klights
baseurl=https://raw.githubusercontent.com/klights-net/klights-core/package-repo/rpm/el9/$basearch
enabled=1
repo_gpgcheck=1
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/klights-archive-keyring.asc
EOF
sudo dnf install -y klights
sudo systemctl enable --now containerd
sudo systemctl enable --now klights
```

## RHEL-Compatible 10

```bash
sudo install -d -m 0755 /etc/pki/rpm-gpg
sudo curl -fsSL \
  https://raw.githubusercontent.com/klights-net/klights-core/package-repo/klights-archive-keyring.asc \
  -o /etc/pki/rpm-gpg/klights-archive-keyring.asc
grep -q "BEGIN PGP PUBLIC KEY BLOCK" /etc/pki/rpm-gpg/klights-archive-keyring.asc
sudo rpm --import /etc/pki/rpm-gpg/klights-archive-keyring.asc
sudo tee /etc/yum.repos.d/klights.repo >/dev/null <<'EOF'
[klights]
name=klights
baseurl=https://raw.githubusercontent.com/klights-net/klights-core/package-repo/rpm/el10/$basearch
enabled=1
repo_gpgcheck=1
gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/klights-archive-keyring.asc
EOF
sudo dnf install -y klights
sudo systemctl enable --now containerd
sudo systemctl enable --now klights
```

## Verify

Verify the node through the generated kubeconfig:

```bash
sudo kubectl --kubeconfig /var/lib/klights/etc/kubeconfig.yaml get nodes -o wide
```

The package quickstart starts a single-node leader. For build, run,
configuration, and operations details, see [doc/README.md](doc/README.md).
