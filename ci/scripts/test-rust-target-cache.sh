#!/usr/bin/env bash
# Regression test for ci/scripts/rust-target-cache.sh: which paths a restored
# Cargo target directory treats as changed, what it deletes, when it is
# discarded, and that the packed transport restores it exactly.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

epoch=946684800
fixture="${tmp_dir}/repo"
parts="${tmp_dir}/parts"
git init -q -b main "${fixture}"
git -C "${fixture}" config user.name test
git -C "${fixture}" config user.email test@example.com
mkdir -p "${fixture}/ci/scripts" "${fixture}/a" "${fixture}/b" "${fixture}/c/sub" "${fixture}/proto/x"
cp "${repo_root}/ci/scripts/rust-target-cache.sh" "${fixture}/ci/scripts/"
echo keep >"${fixture}/a/keep.rs"
echo old >"${fixture}/a/change.rs"
echo gone >"${fixture}/b/delete.rs"
echo stay >"${fixture}/b/stay.rs"
echo x >"${fixture}/c/sub/x.rs"
echo top >"${fixture}/top.txt"
echo imported >"${fixture}/proto/x/imported.proto"
echo importer >"${fixture}/proto/importer.proto"
echo /t/ >"${fixture}/.gitignore"
git -C "${fixture}" add -A
git -C "${fixture}" commit -q -m built

run() {
    (cd "${fixture}" && SPACE_TARGET_CACHE_PARTS=3 SPACE_TARGET_CACHE_ZSTD_LEVEL=3 \
        bash ci/scripts/rust-target-cache.sh "$@")
}

mtime() {
    stat -c %Y "${fixture}/$1"
}

fail() {
    echo "$*" >&2
    exit 1
}

expect_output() {
    local log="$1" line="$2"
    grep -qxF "${line}" "${log}" || { cat "${log}" >&2; fail "expected '${line}'"; }
}

# Cold: nothing restored, every tracked path gets the fixed stamp.
run hydrate t "${parts}" >"${tmp_dir}/cold.log"
expect_output "${tmp_dir}/cold.log" "restored=false"
for path in a/keep.rs a/change.rs b b/delete.rs c/sub c/sub/x.rs top.txt; do
    [ "$(mtime "${path}")" -eq "${epoch}" ] || fail "cold: ${path} was not stamped"
done

# A cold build's outputs, including the uplifted copies, scratch space and
# reports that must not be reused.
t="${fixture}/t"
mkdir -p "${t}/debug/build/a-tool/0123456789abcdef/fingerprint" "${t}/debug/build/a-tool/0123456789abcdef/out" \
    "${t}/debug/examples" "${t}/debug/build/m1_openapi/0123456789abcdef/out" \
    "${t}/debug/build/b/0123456789abcdef/out" "${t}/debug/incremental" "${t}/tmp" "${t}/cargo-timings"
echo out >"${t}/debug/build/a-tool/0123456789abcdef/fingerprint/lib"
head -c 200000 /dev/urandom >"${t}/debug/build/a-tool/0123456789abcdef/out/tool"
ln "${t}/debug/build/a-tool/0123456789abcdef/out/tool" "${t}/debug/tool"
echo ex >"${t}/debug/build/m1_openapi/0123456789abcdef/out/m1_openapi"
ln "${t}/debug/build/m1_openapi/0123456789abcdef/out/m1_openapi" "${t}/debug/examples/m1_openapi"
echo stale >"${t}/debug/examples/removed-0123456789abcdef"
echo script >"${t}/debug/build/b/0123456789abcdef/out/build_script_build"
ln "${t}/debug/build/b/0123456789abcdef/out/build_script_build" \
    "${t}/debug/build/b/0123456789abcdef/out/build-script-build"
echo '{}' >"${t}/.rustc_info.json"
echo scratch >"${t}/tmp/state"
echo report >"${t}/cargo-timings/cargo-timing.html"
echo log >"${t}/run.log"
old="$(($(date +%s) - 600))"
find "${t}" -exec touch -h -d "@${old}" {} +
touch -d "@${old}.123456789" "${t}/debug/build/a-tool/0123456789abcdef/out/tool"
old_ns="$(stat -c %.9Y "${t}/debug/build/a-tool/0123456789abcdef/out/tool")"

# prepare keeps Cargo's layout only, records HEAD's tree and packs.
run prepare t "${parts}" >"${tmp_dir}/prepare.log"
expect_output "${tmp_dir}/prepare.log" "save=true"
for path in t/debug/tool t/debug/examples/m1_openapi t/debug/examples/removed-0123456789abcdef t/tmp t/run.log t/cargo-timings; do
    [ ! -e "${fixture}/${path}" ] || fail "prepare: ${path} was kept"
done
[ "$(find "${parts}" -name 'part-*.tar.zst' | wc -l)" -ge 2 ] || fail "prepare: expected several parts"
[ -z "$(find "${parts}" -name '*.list')" ] || fail "prepare: left part lists behind"
# Only the directory archive, unpacked first, creates directories; parallel
# tars racing to create one can fail.
for part in "${parts}"/part-*.tar.zst; do
    members="$(zstd -q -d --long=31 -c "${part}" | tar -tf -)"
    ! grep -q '/$' <<<"${members}" || fail "prepare: ${part##*/} carries a directory"
done
cp -a "${parts}" "${tmp_dir}/saved-parts"

# The next job checks out a different tree: one modified, one deleted, one
# added, one renamed file, and an imported proto. Checkout stamps everything
# "now". The target directory comes only from the parts.
echo new >"${fixture}/a/change.rs"
git -C "${fixture}" rm -q b/delete.rs
echo new >"${fixture}/c/sub/new.rs"
mkdir -p "${fixture}/d"
git -C "${fixture}" mv top.txt d/top.txt
echo changed >"${fixture}/proto/x/imported.proto"
git -C "${fixture}" add -A
git -C "${fixture}" commit -q -m next
rm -rf "${t}"
find "${fixture}" -path "${fixture}/.git" -prune -o -exec touch -h {} +

run hydrate t "${parts}" >"${tmp_dir}/warm.log"
expect_output "${tmp_dir}/warm.log" "restored=true"
expect_output "${tmp_dir}/warm.log" "changed_paths=6"
[ ! -e "${parts}" ] || fail "warm: the parts were kept after unpacking"
# The transport restores content, sub-second mtimes, empty directories and
# hard-linked build scripts; the uplifted copies stay gone.
[ "$(stat -c %.9Y "${t}/debug/build/a-tool/0123456789abcdef/out/tool")" = "${old_ns}" ] ||
    fail "warm: an output's mtime was not preserved"
[ -d "${t}/debug/incremental" ] || fail "warm: an empty directory was not restored"
[ "$(cat "${t}/debug/build/b/0123456789abcdef/out/build-script-build")" = script ] ||
    fail "warm: a hard-linked build script was not restored"
[ -s "${t}/.space-target-source/tree" ] || fail "warm: the source record was not restored"
for path in t/debug/tool t/debug/examples/m1_openapi t/debug/examples/removed-0123456789abcdef; do
    [ ! -e "${fixture}/${path}" ] || fail "warm: ${path} came back"
done
for path in a/keep.rs b/stay.rs c/sub/x.rs ci/scripts proto/importer.proto; do
    [ "$(mtime "${path}")" -eq "${epoch}" ] || fail "warm: unchanged ${path} was not stamped old"
done
# Changed files and every directory above a changed, added, or deleted path
# must read as newer than the build.
for path in a/change.rs c/sub/new.rs d/top.txt a b c c/sub d proto proto/x proto/x/imported.proto; do
    [ "$(mtime "${path}")" -gt "${old}" ] || fail "warm: ${path} does not read as changed"
done

# A stale uplifted copy that reaches hydrate by any route is deleted.
ln "${t}/debug/build/a-tool/0123456789abcdef/out/tool" "${t}/debug/stale-tool"
run hydrate t >"${tmp_dir}/stale.log"
[ ! -e "${t}/debug/stale-tool" ] || fail "stale: an uplifted copy survived"

# A part that does not unpack discards everything.
rm -rf "${t}"
cp -a "${tmp_dir}/saved-parts" "${parts}"
truncate -s 100 "$(find "${parts}" -name 'part-*.tar.zst' | sort | tail -n 1)"
run hydrate t "${parts}" >"${tmp_dir}/corrupt-part.log" 2>&1
expect_output "${tmp_dir}/corrupt-part.log" "restored=false"
[ ! -e "${t}" ] || fail "corrupt part: the target directory was kept"
[ ! -e "${parts}" ] || fail "corrupt part: the parts were kept"

# A directory without a source record is discarded.
mkdir -p "${t}/debug"
run hydrate t >"${tmp_dir}/unrecorded.log"
expect_output "${tmp_dir}/unrecorded.log" "restored=false"
[ ! -e "${t}" ] || fail "unrecorded: the target directory was kept"

# A record that cannot be read discards the directory.
cp -a "${tmp_dir}/saved-parts" "${parts}"
run hydrate t "${parts}" >/dev/null
rm "${t}/.space-target-source/tree"
mkdir "${t}/.space-target-source/tree"
run hydrate t >"${tmp_dir}/corrupt.log" 2>&1
expect_output "${tmp_dir}/corrupt.log" "restored=false"
[ ! -e "${t}" ] || fail "corrupt: the target directory was kept"

# A part that fails to pack must not be saved, even when zstd, the last
# command in its pipeline, succeeds.
mkdir -p "${t}/debug/build/a-tool/0123456789abcdef/fingerprint" "${tmp_dir}/failing-tar"
printf '#!/bin/sh\nexit 2\n' >"${tmp_dir}/failing-tar/tar"
chmod +x "${tmp_dir}/failing-tar/tar"
if PATH="${tmp_dir}/failing-tar:${PATH}" run prepare t "${parts}" >"${tmp_dir}/failed-pack.log" 2>&1; then
    cat "${tmp_dir}/failed-pack.log" >&2
    fail "failed pack: prepare succeeded"
fi
! grep -qxF save=true "${tmp_dir}/failed-pack.log" || fail "failed pack: prepare reported save=true"

# A build that changed a tracked file must not be saved.
mkdir -p "${t}/debug/build/a-tool/0123456789abcdef/fingerprint"
echo dirty >>"${fixture}/a/keep.rs"
run prepare t "${parts}" >"${tmp_dir}/dirty.log"
expect_output "${tmp_dir}/dirty.log" "save=false"

echo "rust-target-cache tests passed"
