#!/bin/sh
# Install Ubuntu packages from the package snapshot that the Dockerfile pins
# (UBUNTU_SNAPSHOT), even while snapshot.ubuntu.com fails part of its requests.
#
# snapshot.ubuntu.com has outages in which many requests answer 500, 502 or 503
# for minutes to hours. apt's retries ride out short bursts, but a cold build
# downloads around a hundred packages, and one file that never succeeds fails
# the build. When the install from the snapshot fails, this script downloads the
# package files apt still lacks from APT_POOL_FALLBACK (archive.ubuntu.com by
# default). The snapshot and the archive use the same pool paths and never
# change a published file. Each fallback file must match the SHA-256 in the
# snapshot's signed index, so the build installs the same packages whichever
# host served them. If the archive no longer has a file (a package superseded
# since the snapshot), the build still fails. The index itself comes only from
# the snapshot, with twice the retries of the package downloads.
#
#   apt-install.sh PACKAGE...
#
# Runs as root in the build stages. APT_CONFIG is honored, so the script can
# also run against a separate apt state directory.
set -eu

FALLBACK="${APT_POOL_FALLBACK:-https://archive.ubuntu.com/ubuntu}"
FALLBACK="${FALLBACK%/}"
export DEBIAN_FRONTEND=noninteractive

apt_install() {
    apt-get -o Acquire::Retries=5 install --yes --no-install-recommends "$@"
}

apt-get -o Acquire::Retries=10 update --error-on=any
if ! apt_install "$@"; then
    echo "WARNING: installing from the snapshot failed; fetching the missing package files from ${FALLBACK}" >&2
    archives=""
    eval "$(apt-config shell archives Dir::Cache::archives/d)"
    uris="$(mktemp)"
    trap 'rm -f "${uris}"' EXIT
    # One line per file apt still needs: 'URI' FILENAME SIZE SHA256:HASH, from
    # the snapshot's index. Without ForceHash, apt prints the index's MD5.
    apt_install --print-uris -qq -o Acquire::ForceHash=SHA256 "$@" >"${uris}"
    while read -r uri file _size hash; do
        uri="${uri#\'}"
        uri="${uri%\'}"
        case "${uri}" in
            */pool/*) ;;
            *)
                echo "ERROR: ${uri} is not a pool file" >&2
                exit 1
                ;;
        esac
        case "${hash}" in
            SHA256:?*) ;;
            *)
                echo "ERROR: the index lists no SHA-256 for ${file}" >&2
                exit 1
                ;;
        esac
        /usr/lib/apt/apt-helper -o Acquire::Retries=5 download-file \
            "${FALLBACK}/pool/${uri#*/pool/}" "${archives}${file}" "${hash}"
    done <"${uris}"
    # apt takes a file already in its archive directory by its size alone, so
    # the SHA-256 check above is the one that counts.
    apt_install "$@"
fi
lists=""
eval "$(apt-config shell lists Dir::State::lists/d)"
rm -rf "${lists:?}"*
