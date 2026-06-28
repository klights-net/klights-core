#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<EOF_USAGE
Usage: $(basename "$0") --binary PATH --version VERSION --distro el9|el10 --out DIR

Create an RPM for klights.

Options:
  --binary PATH     path to klights binary to install
  --version VERSION version string for package metadata
  --distro el9|el10 target EL release label (also used as rpm Release suffix)
  --out DIR         output directory for generated .rpm
EOF_USAGE
  exit 1
}

BINARY=""
VERSION=""
DISTRO=""
OUT_DIR=""

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --binary)
      if [[ "$#" -lt 2 ]]; then
        echo "--binary requires PATH" >&2
        usage
      fi
      BINARY=$2
      shift 2
      ;;
    --version)
      if [[ "$#" -lt 2 ]]; then
        echo "--version requires VERSION" >&2
        usage
      fi
      VERSION=$2
      shift 2
      ;;
    --distro)
      if [[ "$#" -lt 2 ]]; then
        echo "--distro requires el9|el10" >&2
        usage
      fi
      DISTRO=$2
      shift 2
      ;;
    --out)
      if [[ "$#" -lt 2 ]]; then
        echo "--out requires DIR" >&2
        usage
      fi
      OUT_DIR=$2
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

if [[ -z "$BINARY" || -z "$VERSION" || -z "$DISTRO" || -z "$OUT_DIR" ]]; then
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

if [[ ! -f "$BINARY" ]]; then
  echo "Binary not found: $BINARY" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
TMPROOT="$(mktemp -d -t klights-rpm-XXXXXXXX)"
cleanup() {
  rm -rf "$TMPROOT"
}
trap cleanup EXIT

TOPDIR="$TMPROOT/topdir"
SOURCE_ROOT="$TMPROOT/klights-${VERSION}"
mkdir -p "$TOPDIR/SPECS" "$TOPDIR/SOURCES" "$TOPDIR/BUILD" "$TOPDIR/RPMS" "$TOPDIR/SRPMS"
mkdir -p "$SOURCE_ROOT/usr/bin" "$SOURCE_ROOT/usr/lib/systemd/system" "$SOURCE_ROOT/etc/sysconfig"

install -m 0755 "$BINARY" "$SOURCE_ROOT/usr/bin/klights"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SERVICE_SRC="$REPO_ROOT/packaging/systemd/klights.service"
DEFAULT_SRC="$REPO_ROOT/packaging/default/klights"

HAS_SYSTEMD=0
HAS_DEFAULT=0

if [[ -f "$SERVICE_SRC" ]]; then
  install -m 0644 "$SERVICE_SRC" "$SOURCE_ROOT/usr/lib/systemd/system/klights.service"
  HAS_SYSTEMD=1
fi

if [[ -f "$DEFAULT_SRC" ]]; then
  install -m 0644 "$DEFAULT_SRC" "$SOURCE_ROOT/etc/sysconfig/klights"
  HAS_DEFAULT=1
fi

tar -C "$TMPROOT" -czf "$TOPDIR/SOURCES/klights-${VERSION}.tar.gz" "klights-${VERSION}"

cat > "$TOPDIR/SPECS/klights.spec" <<EOF_SPEC
%global has_systemd ${HAS_SYSTEMD}
%global has_default ${HAS_DEFAULT}

Name:           klights
Version:        ${VERSION}
Release:        1.${DISTRO}
Summary:        Lightweight Kubernetes runtime
License:        AGPL-3.0-or-later
BuildArch:      x86_64
Source0:        klights-%{version}.tar.gz
Requires:       containerd
Requires:       libmnl
Requires:       libnftnl

%description
Klights is a compact Rust implementation of core Kubernetes control-plane and
node runtime services for local clusters.

%prep
%setup -q -n klights-%{version}

%build
# No build step is required; binary is prebuilt.

%install
mkdir -p %{buildroot}/usr/bin
install -m 0755 usr/bin/klights %{buildroot}%{_bindir}/klights
%if %{has_systemd}
mkdir -p %{buildroot}/usr/lib/systemd/system
install -m 0644 usr/lib/systemd/system/klights.service \
  %{buildroot}/usr/lib/systemd/system/klights.service
%endif
%if %{has_default}
mkdir -p %{buildroot}/etc/sysconfig
install -m 0644 etc/sysconfig/klights %{buildroot}/etc/sysconfig/klights
%endif

%files
%defattr(-,root,root,-)
%attr(0755,root,root) %{_bindir}/klights
%if %{has_systemd}
/usr/lib/systemd/system/klights.service
%endif
%if %{has_default}
%config(noreplace) /etc/sysconfig/klights
%endif

%post
if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload || true
  systemctl enable klights.service || true
fi

%preun
if [ "$1" -eq 0 ]; then
  if command -v systemctl >/dev/null 2>&1; then
    systemctl disable klights.service || true
  fi
fi
if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload || true
fi
EOF_SPEC

rpmbuild -bb --define "_topdir $TOPDIR" "$TOPDIR/SPECS/klights.spec"

PACKAGE_NAME="klights-${VERSION}-1.${DISTRO}.x86_64.rpm"
BUILT_PACKAGE="$(find "$TOPDIR/RPMS/x86_64" -maxdepth 1 -type f -name "$PACKAGE_NAME" -print | head -n 1)"
if [[ -z "$BUILT_PACKAGE" ]]; then
  echo "Failed to build package: $PACKAGE_NAME" >&2
  find "$TOPDIR/RPMS" -maxdepth 3 -type f >&2
  exit 1
fi

cp "$BUILT_PACKAGE" "$OUT_DIR/$PACKAGE_NAME"
echo "$OUT_DIR/$PACKAGE_NAME"
