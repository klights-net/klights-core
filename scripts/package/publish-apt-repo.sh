#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<EOF_USAGE
Usage: $(basename "$0") \
  --packages DIR --repo DIR --suite SUITE --origin ORIGIN --label LABEL --codename CODENAME

Build a static APT repository for klights Debian packages.
EOF_USAGE
  exit 1
}

PACKAGES_DIR=""
REPO_DIR=""
SUITE=""
ORIGIN=""
LABEL=""
CODENAME=""

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --packages)
      if [[ "$#" -lt 2 ]]; then
        echo "--packages requires DIR" >&2
        usage
      fi
      PACKAGES_DIR=$2
      shift 2
      ;;
    --repo)
      if [[ "$#" -lt 2 ]]; then
        echo "--repo requires DIR" >&2
        usage
      fi
      REPO_DIR=$2
      shift 2
      ;;
    --suite)
      if [[ "$#" -lt 2 ]]; then
        echo "--suite requires SUITE" >&2
        usage
      fi
      SUITE=$2
      shift 2
      ;;
    --origin)
      if [[ "$#" -lt 2 ]]; then
        echo "--origin requires ORIGIN" >&2
        usage
      fi
      ORIGIN=$2
      shift 2
      ;;
    --label)
      if [[ "$#" -lt 2 ]]; then
        echo "--label requires LABEL" >&2
        usage
      fi
      LABEL=$2
      shift 2
      ;;
    --codename)
      if [[ "$#" -lt 2 ]]; then
        echo "--codename requires CODENAME" >&2
        usage
      fi
      CODENAME=$2
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

if [[ -z "$PACKAGES_DIR" || -z "$REPO_DIR" || -z "$SUITE" || -z "$ORIGIN" || -z "$LABEL" || -z "$CODENAME" ]]; then
  echo "Missing required arguments." >&2
  usage
fi

if ! command -v dpkg-scanpackages >/dev/null 2>&1; then
  echo "dpkg-scanpackages not found. Install dpkg-dev on this host." >&2
  exit 1
fi

if ! command -v apt-ftparchive >/dev/null 2>&1; then
  echo "apt-ftparchive not found. Install apt-utils on this host." >&2
  exit 1
fi

mkdir -p "$REPO_DIR"
POOL_DIR="$REPO_DIR/pool/$SUITE/main/k/klights"
DIST_ROOT="$REPO_DIR/dists/$SUITE"
BINARY_DIR="$DIST_ROOT/main/binary-amd64"
mkdir -p "$POOL_DIR" "$BINARY_DIR"

PATTERN="klights_*~${SUITE}_amd64.deb"
shopt -s nullglob
package_count=0
for package_file in "$PACKAGES_DIR"/$PATTERN; do
  install -m 0644 "$package_file" "$POOL_DIR/"
  package_count=$((package_count + 1))
done
shopt -u nullglob

if [[ "$package_count" -eq 0 ]]; then
  echo "No matching packages for suite ${SUITE} in $PACKAGES_DIR" >&2
  exit 1
fi

( cd "$REPO_DIR" && dpkg-scanpackages --arch amd64 "pool/$SUITE/main/k/klights" ) > "$BINARY_DIR/Packages"
gzip -9c "$BINARY_DIR/Packages" > "$BINARY_DIR/Packages.gz"

RELEASE_FILE="$DIST_ROOT/Release"
apt-ftparchive \
  -o "APT::FTPArchive::Release::Origin=$ORIGIN" \
  -o "APT::FTPArchive::Release::Label=$LABEL" \
  -o "APT::FTPArchive::Release::Suite=$SUITE" \
  -o "APT::FTPArchive::Release::Codename=$CODENAME" \
  -o "APT::FTPArchive::Release::Architectures=amd64" \
  -o "APT::FTPArchive::Release::Components=main" \
  release "$DIST_ROOT" > "$RELEASE_FILE"

if [[ -n "${PACKAGE_GPG_KEY_ID:-}" ]]; then
  if ! command -v gpg >/dev/null 2>&1; then
    echo "PACKAGE_GPG_KEY_ID is set but gpg is not available." >&2
    exit 1
  fi

  GPG_ARGS=(--batch --yes --local-user "$PACKAGE_GPG_KEY_ID")
  if [[ -n "${PACKAGE_GPG_PASSPHRASE:-}" ]]; then
    GPG_ARGS+=(--pinentry-mode loopback --passphrase "$PACKAGE_GPG_PASSPHRASE")
  fi

  gpg "${GPG_ARGS[@]}" --detach-sign --armor -o "$RELEASE_FILE.gpg" "$RELEASE_FILE"
  gpg "${GPG_ARGS[@]}" --clearsign -o "$DIST_ROOT/InRelease" "$RELEASE_FILE"
fi

echo "$REPO_DIR"
