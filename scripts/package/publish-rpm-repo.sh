#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<EOF_USAGE
Usage: $(basename "$0") --packages DIR --repo DIR --distro el9|el10

Build an RPM repository for klights.
EOF_USAGE
  exit 1
}

PACKAGES_DIR=""
REPO_DIR=""
DISTRO=""

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
    --distro)
      if [[ "$#" -lt 2 ]]; then
        echo "--distro requires el9|el10" >&2
        usage
      fi
      DISTRO=$2
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

if [[ -z "$PACKAGES_DIR" || -z "$REPO_DIR" || -z "$DISTRO" ]]; then
  echo "Missing required argument." >&2
  usage
fi

if [[ "$DISTRO" != "el9" && "$DISTRO" != "el10" ]]; then
  echo "--distro must be el9 or el10" >&2
  exit 1
fi

if ! command -v createrepo_c >/dev/null 2>&1; then
  echo "createrepo_c not found. Install createrepo_c package." >&2
  exit 1
fi

mkdir -p "$REPO_DIR/$DISTRO/x86_64"
RPM_REPO="$REPO_DIR/$DISTRO/x86_64"

shopt -s nullglob
package_count=0
for package_file in \
  "$PACKAGES_DIR"/klights-*-1.${DISTRO}.x86_64.rpm \
  "$PACKAGES_DIR"/containerd-*-1.${DISTRO}.x86_64.rpm \
  "$PACKAGES_DIR"/runc-*-1.${DISTRO}.x86_64.rpm; do
  install -m 0644 "$package_file" "$RPM_REPO/"
  package_count=$((package_count + 1))
done
shopt -u nullglob

if [[ "$package_count" -eq 0 ]]; then
  echo "No matching RPM packages for distro ${DISTRO} in $PACKAGES_DIR" >&2
  exit 1
fi

createrepo_c "$RPM_REPO"

if [[ -n "${PACKAGE_GPG_KEY_ID:-}" ]]; then
  if ! command -v gpg >/dev/null 2>&1; then
    echo "PACKAGE_GPG_KEY_ID is set but gpg is not available." >&2
    exit 1
  fi

  REPOMD_FILE="$RPM_REPO/repodata/repomd.xml"
  if [[ ! -f "$REPOMD_FILE" ]]; then
    echo "Failed to locate repomd.xml at $REPOMD_FILE" >&2
    exit 1
  fi

  GPG_ARGS=(--batch --yes --local-user "$PACKAGE_GPG_KEY_ID")
  if [[ -n "${PACKAGE_GPG_PASSPHRASE:-}" ]]; then
    GPG_ARGS+=(--pinentry-mode loopback --passphrase "$PACKAGE_GPG_PASSPHRASE")
  fi

  gpg "${GPG_ARGS[@]}" --detach-sign --armor -o "$REPOMD_FILE.asc" "$REPOMD_FILE"
fi

echo "$RPM_REPO"
