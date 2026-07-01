#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<EOF_USAGE
Usage: $(basename "$0") --packages DIR

Sign every RPM package in DIR with PACKAGE_GPG_KEY_ID, then verify the RPM
signatures with rpm --checksig.
EOF_USAGE
  exit 1
}

PACKAGES_DIR=""

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
    --help|-h)
      usage
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      ;;
  esac
done

if [[ -z "$PACKAGES_DIR" ]]; then
  echo "Missing required argument." >&2
  usage
fi

if [[ -z "${PACKAGE_GPG_KEY_ID:-}" ]]; then
  echo "PACKAGE_GPG_KEY_ID is required to sign RPM packages." >&2
  exit 1
fi

if [[ ! -d "$PACKAGES_DIR" ]]; then
  echo "Package directory not found: $PACKAGES_DIR" >&2
  exit 1
fi

if ! command -v rpmsign >/dev/null 2>&1; then
  echo "rpmsign not found. Install rpm signing tooling on this host." >&2
  exit 1
fi

if ! command -v rpm >/dev/null 2>&1; then
  echo "rpm not found. Install rpm tooling on this host." >&2
  exit 1
fi

if ! command -v gpg >/dev/null 2>&1; then
  echo "gpg not found. Install gnupg on this host." >&2
  exit 1
fi

mapfile -t rpm_files < <(find "$PACKAGES_DIR" -maxdepth 1 -type f -name '*.rpm' | sort)
if [[ "${#rpm_files[@]}" -eq 0 ]]; then
  echo "No RPM packages found in $PACKAGES_DIR" >&2
  exit 1
fi

TMPROOT="$(mktemp -d -t klights-rpm-sign-XXXXXXXX)"
cleanup() {
  rm -rf "$TMPROOT"
}
trap cleanup EXIT

gpg --batch --yes --armor --export "$PACKAGE_GPG_KEY_ID" > "$TMPROOT/package-signing-key.asc"
test -s "$TMPROOT/package-signing-key.asc"
grep -q "BEGIN PGP PUBLIC KEY BLOCK" "$TMPROOT/package-signing-key.asc"
mkdir -p "$TMPROOT/rpmdb"
rpm --define "_dbpath $TMPROOT/rpmdb" --import "$TMPROOT/package-signing-key.asc"

sign_defines=(
  --define "_signature gpg"
  --define "_gpg_name $PACKAGE_GPG_KEY_ID"
  --define "__gpg /usr/bin/gpg"
  --define "_gpg_digest_algo sha256"
)

if [[ -n "${PACKAGE_GPG_PASSPHRASE:-}" ]]; then
  install -d -m 0700 "$HOME/.gnupg"
  grep -qxF "allow-loopback-pinentry" "$HOME/.gnupg/gpg-agent.conf" 2>/dev/null \
    || echo "allow-loopback-pinentry" >> "$HOME/.gnupg/gpg-agent.conf"
  gpg-connect-agent reloadagent /bye >/dev/null 2>&1 || true

  PASS_FILE="$TMPROOT/passphrase"
  printf '%s' "$PACKAGE_GPG_PASSPHRASE" > "$PASS_FILE"
  chmod 0600 "$PASS_FILE"
  sign_defines+=(
    --define "_gpg_sign_cmd_extra_args --batch --yes --pinentry-mode loopback --passphrase-file $PASS_FILE"
  )
fi

rpmsign --addsign "${sign_defines[@]}" "${rpm_files[@]}"

for rpm_file in "${rpm_files[@]}"; do
  check_output="$(rpm --define "_dbpath $TMPROOT/rpmdb" --checksig --verbose "$rpm_file")"
  printf '%s\n' "$check_output"
  if ! grep -Eq 'Header .*Signature.*, key ID .*, OK|digests signatures OK' <<<"$check_output"; then
    echo "RPM signature verification failed for $rpm_file" >&2
    exit 1
  fi
done
