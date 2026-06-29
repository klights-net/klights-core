#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<EOF_USAGE
Usage: $(basename "$0") --containerd-dir DIR --runc-bin PATH --containerd-version VERSION --runc-version VERSION --containerd-license PATH --containerd-notice PATH --runc-license PATH --runc-notice PATH --distro el9|el10 --out DIR

Create RHEL-compatible RPMs for the open-source container runtime dependencies
published by the klights package repository.
EOF_USAGE
  exit 1
}

CONTAINERD_DIR=""
RUNC_BIN=""
CONTAINERD_VERSION=""
RUNC_VERSION=""
CONTAINERD_LICENSE=""
CONTAINERD_NOTICE=""
RUNC_LICENSE=""
RUNC_NOTICE=""
DISTRO=""
OUT_DIR=""

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --containerd-dir)
      CONTAINERD_DIR=${2:-}
      shift 2
      ;;
    --runc-bin)
      RUNC_BIN=${2:-}
      shift 2
      ;;
    --containerd-version)
      CONTAINERD_VERSION=${2:-}
      shift 2
      ;;
    --runc-version)
      RUNC_VERSION=${2:-}
      shift 2
      ;;
    --containerd-license)
      CONTAINERD_LICENSE=${2:-}
      shift 2
      ;;
    --containerd-notice)
      CONTAINERD_NOTICE=${2:-}
      shift 2
      ;;
    --runc-license)
      RUNC_LICENSE=${2:-}
      shift 2
      ;;
    --runc-notice)
      RUNC_NOTICE=${2:-}
      shift 2
      ;;
    --distro)
      DISTRO=${2:-}
      shift 2
      ;;
    --out)
      OUT_DIR=${2:-}
      shift 2
      ;;
    --help|-h)
      usage
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      ;;
  esac
done

if [[ -z "$CONTAINERD_DIR" || -z "$RUNC_BIN" || -z "$CONTAINERD_VERSION" || -z "$RUNC_VERSION" || -z "$CONTAINERD_LICENSE" || -z "$CONTAINERD_NOTICE" || -z "$RUNC_LICENSE" || -z "$RUNC_NOTICE" || -z "$DISTRO" || -z "$OUT_DIR" ]]; then
  echo "Missing required argument." >&2
  usage
fi

if [[ "$DISTRO" != "el9" && "$DISTRO" != "el10" ]]; then
  echo "--distro must be el9 or el10" >&2
  exit 1
fi

if ! command -v rpmbuild >/dev/null 2>&1; then
  echo "rpmbuild not found. Install rpm-build on this host." >&2
  exit 1
fi

if [[ ! -d "$CONTAINERD_DIR/bin" ]]; then
  echo "containerd bin directory not found: $CONTAINERD_DIR/bin" >&2
  exit 1
fi

if [[ ! -f "$RUNC_BIN" ]]; then
  echo "runc binary not found: $RUNC_BIN" >&2
  exit 1
fi

if [[ ! -f "$CONTAINERD_LICENSE" ]]; then
  echo "containerd license file not found: $CONTAINERD_LICENSE" >&2
  exit 1
fi

if [[ ! -f "$RUNC_LICENSE" ]]; then
  echo "runc license file not found: $RUNC_LICENSE" >&2
  exit 1
fi

if [[ ! -f "$CONTAINERD_NOTICE" ]]; then
  echo "containerd notice file not found: $CONTAINERD_NOTICE" >&2
  exit 1
fi

if [[ ! -f "$RUNC_NOTICE" ]]; then
  echo "runc notice file not found: $RUNC_NOTICE" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
TMPROOT="$(mktemp -d -t klights-runtime-rpm-XXXXXXXX)"
cleanup() {
  rm -rf "$TMPROOT"
}
trap cleanup EXIT

TOPDIR="$TMPROOT/topdir"
mkdir -p "$TOPDIR/SPECS" "$TOPDIR/SOURCES" "$TOPDIR/BUILD" "$TOPDIR/RPMS" "$TOPDIR/SRPMS"

CONTAINERD_SOURCE="$TMPROOT/containerd-${CONTAINERD_VERSION}"
mkdir -p "$CONTAINERD_SOURCE/usr/bin" "$CONTAINERD_SOURCE/usr/lib/systemd/system" "$CONTAINERD_SOURCE/usr/share/licenses/containerd"
install -m 0755 "$CONTAINERD_DIR"/bin/* "$CONTAINERD_SOURCE/usr/bin/"
install -m 0644 "$CONTAINERD_LICENSE" "$CONTAINERD_SOURCE/usr/share/licenses/containerd/LICENSE"
install -m 0644 "$CONTAINERD_NOTICE" "$CONTAINERD_SOURCE/usr/share/licenses/containerd/NOTICE"
cat > "$CONTAINERD_SOURCE/usr/lib/systemd/system/containerd.service" <<'EOF_SERVICE'
[Unit]
Description=containerd container runtime
Documentation=https://containerd.io
After=network.target local-fs.target

[Service]
ExecStartPre=-/sbin/modprobe overlay
ExecStart=/usr/bin/containerd
Type=notify
Delegate=yes
KillMode=process
Restart=always
RestartSec=5
LimitNPROC=infinity
LimitCORE=infinity
LimitNOFILE=infinity

[Install]
WantedBy=multi-user.target
EOF_SERVICE
tar -C "$TMPROOT" -czf "$TOPDIR/SOURCES/containerd-${CONTAINERD_VERSION}.tar.gz" "containerd-${CONTAINERD_VERSION}"

cat > "$TOPDIR/SPECS/containerd.spec" <<EOF_SPEC
Name:           containerd
Version:        ${CONTAINERD_VERSION}
Release:        1.${DISTRO}
Summary:        Open container runtime
License:        Apache-2.0
BuildArch:      x86_64
Source0:        containerd-%{version}.tar.gz
URL:            https://containerd.io/
Requires:       runc
Requires(post): systemd
Requires(preun): systemd
Requires(postun): systemd
Provides:       containerd = %{version}-%{release}

%description
containerd is an industry-standard container runtime with an emphasis on
simplicity, robustness, and portability.

%prep
%setup -q -n containerd-%{version}

%build
# No build step is required; upstream static binaries are packaged.

%install
mkdir -p %{buildroot}%{_bindir}
install -m 0755 usr/bin/* %{buildroot}%{_bindir}/
mkdir -p %{buildroot}/usr/lib/systemd/system
install -m 0644 usr/lib/systemd/system/containerd.service \
  %{buildroot}/usr/lib/systemd/system/containerd.service
mkdir -p %{buildroot}/usr/share/licenses/containerd
install -m 0644 usr/share/licenses/containerd/LICENSE \
  %{buildroot}/usr/share/licenses/containerd/LICENSE
install -m 0644 usr/share/licenses/containerd/NOTICE \
  %{buildroot}/usr/share/licenses/containerd/NOTICE

%files
%defattr(-,root,root,-)
%attr(0755,root,root) %{_bindir}/*
/usr/lib/systemd/system/containerd.service
%license /usr/share/licenses/containerd/LICENSE
%license /usr/share/licenses/containerd/NOTICE

%post
if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload || true
fi

%preun
if [ "\$1" -eq 0 ] && command -v systemctl >/dev/null 2>&1; then
  systemctl stop containerd.service || true
  systemctl disable containerd.service || true
fi

%postun
if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload || true
fi
EOF_SPEC

RUNC_SOURCE="$TMPROOT/runc-${RUNC_VERSION}"
mkdir -p "$RUNC_SOURCE/usr/bin" "$RUNC_SOURCE/usr/share/licenses/runc"
install -m 0755 "$RUNC_BIN" "$RUNC_SOURCE/usr/bin/runc"
install -m 0644 "$RUNC_LICENSE" "$RUNC_SOURCE/usr/share/licenses/runc/LICENSE"
install -m 0644 "$RUNC_NOTICE" "$RUNC_SOURCE/usr/share/licenses/runc/NOTICE"
tar -C "$TMPROOT" -czf "$TOPDIR/SOURCES/runc-${RUNC_VERSION}.tar.gz" "runc-${RUNC_VERSION}"

cat > "$TOPDIR/SPECS/runc.spec" <<EOF_SPEC
Name:           runc
Version:        ${RUNC_VERSION}
Release:        1.${DISTRO}
Summary:        Open Container Initiative runtime
License:        Apache-2.0
BuildArch:      x86_64
Source0:        runc-%{version}.tar.gz
URL:            https://github.com/opencontainers/runc
Provides:       runc = %{version}-%{release}

%description
runc is a CLI tool for spawning and running containers according to the OCI
runtime specification.

%prep
%setup -q -n runc-%{version}

%build
# No build step is required; upstream static binary is packaged.

%install
mkdir -p %{buildroot}%{_bindir}
install -m 0755 usr/bin/runc %{buildroot}%{_bindir}/runc
mkdir -p %{buildroot}/usr/share/licenses/runc
install -m 0644 usr/share/licenses/runc/LICENSE \
  %{buildroot}/usr/share/licenses/runc/LICENSE
install -m 0644 usr/share/licenses/runc/NOTICE \
  %{buildroot}/usr/share/licenses/runc/NOTICE

%files
%defattr(-,root,root,-)
%attr(0755,root,root) %{_bindir}/runc
%license /usr/share/licenses/runc/LICENSE
%license /usr/share/licenses/runc/NOTICE
EOF_SPEC

rpmbuild -bb --define "_topdir $TOPDIR" "$TOPDIR/SPECS/runc.spec"
rpmbuild -bb --define "_topdir $TOPDIR" "$TOPDIR/SPECS/containerd.spec"

for package in \
  "$TOPDIR/RPMS/x86_64/runc-${RUNC_VERSION}-1.${DISTRO}.x86_64.rpm" \
  "$TOPDIR/RPMS/x86_64/containerd-${CONTAINERD_VERSION}-1.${DISTRO}.x86_64.rpm"; do
  if [[ ! -f "$package" ]]; then
    echo "Failed to build expected package: $package" >&2
    find "$TOPDIR/RPMS" -maxdepth 3 -type f >&2
    exit 1
  fi
  cp "$package" "$OUT_DIR/"
  echo "$OUT_DIR/$(basename "$package")"
done
