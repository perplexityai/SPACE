#!/usr/bin/env python3
"""Name, prepare, report, trim and prune the package caches CI keeps in GitHub's Actions cache.

A CI job restores Cargo's registry, Go's module download cache, uv's cache or
npm's cache, so it downloads only the packages its lockfile changed. FAMILIES
lists each cache and the files its packages come from. Keys are
<family>-<inputs hash>-<run id>-<run attempt>, restored by the prefix
<family>-<inputs hash>- and then <family>-: the newest copy for these inputs,
else the newest of the family. A uv family also names uv's release, whose cache
layout is its own. Only one job per cache saves it, and only on main. A missing
or unusable copy only makes a job slower: the tools fetch whatever is absent
from upstream as before.

  key       writes a family's `family`, `lock` and `key` step outputs.
  restored  after the restore and before anything reads the cache. When no key
            matched, nothing was restored or the restore failed, possibly after
            unpacking part of an archive, so the directory is emptied. Then
            drops any Cargo or Go archive whose checksum the lockfile does not
            vouch for: both tools take an archive in their cache as downloaded,
            and a damaged one fails every build instead of costing a download.
            If the check itself fails, the directory is emptied too. Finally
            marks the time, which `report` compares against.
  report    prints the cache's size and the files the job added since
            `restored`. With --decide it also decides whether main saves the
            cache: when it was restored for other inputs, or not at all, or when
            this job had to repair or complete the copy for its inputs. Writes
            the `<kind>-save` step output; only main acts on it. Never fails.
  trim      before main saves a copy: deletes the Cargo, Go or npm packages the
            current lockfile no longer lists, so the copy does not grow with
            every lockfile change. uv's copies are pruned by uv itself (see
            common-ci.yml).
  prune     on main: deletes superseded copies. In each ecosystem, across uv
            releases, it keeps the newest copy for the inputs main's newest
            commit has, whichever run saved last, and the newest OTHERS_KEPT
            others. It ignores copies saved after it fetched main, and deletes
            nothing if main moved meanwhile or its tree cannot be read.

<dir> is the Cargo registry (CARGO_HOME/registry), GOMODCACHE, UV_CACHE_DIR or
npm's cache. The time mark sits beside <dir>, outside what is cached.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping

KINDS = ("cargo", "go", "uv", "npm")
UNITS = {
    "cargo": "crates and index entries",
    "go": "module files",
    "uv": "downloaded distributions",
    "npm": "cache entries",
}

KEY_PREFIX = "space-packages-v1-"
# Each cache and the files whose contents decide its packages. The uv families
# are separate because no job installs another's packages: the Python tests'
# project and test environment, the shell tests' migration checkers (PEP 723
# headers and a `--with`), and the Gateway traffic harness's requirements.
FAMILIES = {
    "cargo": ("Cargo.lock",),
    "go": ("shim/go.mod", "shim/go.sum"),
    "npm": ("docs/api/package-lock.json",),
    "uv-python": ("uv.lock", "pyproject.toml", "ci/scripts/run-python-tests.sh"),
    "uv-shell": (
        "uv.lock",
        "pyproject.toml",
        "scripts/check_postgres_partial_indexes.py",
        "scripts/tests/test-check-postgres-migrations-squawk.sh",
    ),
    "uv-gateway": ("scripts/netgtw/traffic/requirements.txt",),
}
UV_RELEASE_FILE = "ci/Dockerfile.ci"
# <ecosystem group>[-uv<release>]-<inputs hash>-<run id>-<run attempt>.
PACKAGE_KEY = re.compile(
    rf"^(?P<group>{re.escape(KEY_PREFIX)}[^-]+-[^-]+-"
    rf"(?:{'|'.join(re.escape(f) for f in sorted(FAMILIES, key=len, reverse=True))}))"
    r"(?:-uv[^-]+)?-[0-9a-f]{64}-[0-9]+-[0-9]+$"
)
OTHERS_KEPT = 1
SETTLE_MARGIN = timedelta(minutes=5)


def tree_reader(tree: str | None) -> Callable[[str], bytes | None]:
    """Read a repository file from the working tree, or from a Git tree-ish."""

    def read(path: str) -> bytes | None:
        if tree is None:
            try:
                return Path(path).read_bytes()
            except OSError:
                return None
        result = subprocess.run(["git", "show", f"{tree}:{path}"], capture_output=True)
        return result.stdout if result.returncode == 0 else None

    return read


def family_prefix(family: str, os_name: str, arch: str, read: Callable[[str], bytes | None]) -> str:
    prefix = f"{KEY_PREFIX}{os_name}-{arch}-{family}"
    if family.startswith("uv-"):
        dockerfile = (read(UV_RELEASE_FILE) or b"").decode("utf-8", "replace")
        release = re.search(r"^ARG UV_VERSION=(\S+)", dockerfile, re.M)
        prefix += f"-uv{release[1] if release else 'unknown'}"
    return prefix + "-"


def inputs_hash(family: str, read: Callable[[str], bytes | None]) -> str:
    digest = hashlib.sha256()
    for path in FAMILIES[family]:
        content = read(path)
        digest.update(path.encode() + b"\0")
        digest.update(hashlib.sha256(content).digest() if content is not None else b"missing")
    return digest.hexdigest()


def lock_prefix(family: str, os_name: str, arch: str, read: Callable[[str], bytes | None]) -> str:
    return f"{family_prefix(family, os_name, arch, read)}{inputs_hash(family, read)}-"


def marker(root: Path) -> Path:
    return root.with_name(root.name + ".restored")


def cached_paths(kind: str, root: Path) -> list[Path]:
    if kind == "cargo":
        return [root / "cache", root / "index"]
    if kind == "go":
        return [root / "cache" / "download"]
    return [root]


def files_under(path: Path, pattern: str = "*") -> Iterable[Path]:
    try:
        yield from (p for p in path.rglob(pattern) if p.is_file())
    except OSError:
        return


def added_files(kind: str, root: Path, since: float) -> list[Path]:
    """The files a fetch writes for each package, newer than `since`.

    Cargo's index entries and Go's .mod and .info files count too: a copy that
    lacks one is incomplete, though the job fetches no archive for it.
    """
    if kind == "cargo":
        candidates = (
            p
            for p in [*files_under(root / "cache", "*.crate"), *files_under(root / "index")]
            if p.suffix == ".crate" or ".cache" in p.parts
        )
    elif kind == "go":
        candidates = (
            p
            for p in files_under(root / "cache" / "download")
            if p.suffix in (".zip", ".mod", ".info") and "sumdb" not in p.parts
        )
    elif kind == "uv":
        # uv records each wheel or source distribution it downloads in a .http
        # entry under its wheel and source-distribution buckets. Wheels it
        # builds locally, and the environments it assembles, do not count.
        candidates = (
            p
            for bucket in sorted(root.glob("wheels-v*")) + sorted(root.glob("sdists-v*"))
            for p in files_under(bucket, "*.http")
        )
    else:
        candidates = files_under(root / "_cacache" / "content-v2")
    added = []
    for path in candidates:
        try:
            if path.stat().st_mtime > since:
                added.append(path)
        except OSError:
            pass
    return added


def size_of(paths: Iterable[Path]) -> int:
    total = 0
    for path in paths:
        for file in files_under(path):
            try:
                total += file.lstat().st_size
            except OSError:
                pass
    return total


def human(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB"):
        if value < 1024:
            return f"{value:.0f} {unit}"
        value /= 1024
    return f"{value:.1f} GiB"


# --- Cargo -----------------------------------------------------------------


def cargo_packages(lockfile: Path) -> dict[str, str]:
    """Map each registry package's .crate file name to its sha256 checksum."""
    lock = tomllib.loads(lockfile.read_text(encoding="utf-8"))
    crates = {}
    for package in lock.get("package", []):
        source = package.get("source", "")
        if source.startswith(("registry+", "sparse+")) and "checksum" in package:
            crates[f"{package['name']}-{package['version']}.crate"] = package["checksum"]
    return crates


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def crate_files(root: Path) -> Iterable[Path]:
    return files_under(root / "cache", "*.crate")


def verify_cargo(root: Path, lockfile: Path) -> list[Path]:
    """Delete the .crate archives whose checksum differs from Cargo.lock's."""
    expected = cargo_packages(lockfile)
    damaged = []
    for path in crate_files(root):
        checksum = expected.get(path.name)
        if checksum is not None and sha256_file(path) != checksum:
            path.unlink()
            damaged.append(path)
    return damaged


def trim_cargo(root: Path, lockfile: Path) -> list[Path]:
    """Delete the .crate archives and index entries Cargo.lock no longer lists."""
    expected = cargo_packages(lockfile)
    names = {
        package["name"].lower()
        for package in tomllib.loads(lockfile.read_text(encoding="utf-8")).get("package", [])
    }
    removed = [p for p in crate_files(root) if p.name not in expected]
    # Each sparse index entry is a file named after its crate, in lower case.
    removed += [
        p
        for index in (root / "index").glob("*/.cache")
        for p in files_under(index)
        if p.name not in names
    ]
    for path in removed:
        path.unlink()
    return removed


# --- Go --------------------------------------------------------------------


def go_escape(text: str) -> str:
    """Go's module cache escapes each upper-case letter as ! and its lower case."""
    return "".join(f"!{c.lower()}" if c.isupper() else c for c in text)


def go_sums(gosum: Path) -> dict[tuple[str, str], dict[str, str]]:
    """Map (escaped module path, escaped version) to its zip and go.mod hashes."""
    sums: dict[tuple[str, str], dict[str, str]] = {}
    for line in gosum.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if len(fields) != 3:
            continue
        module, version, digest = fields
        kind = "mod" if version.endswith("/go.mod") else "zip"
        version = version.removesuffix("/go.mod")
        sums.setdefault((go_escape(module), go_escape(version)), {})[kind] = digest
    return sums


def go_hash1(files: Iterable[tuple[str, bytes]]) -> str:
    """golang.org/x/mod/sumdb/dirhash.Hash1."""
    summary = "".join(
        f"{hashlib.sha256(content).hexdigest()}  {name}\n"
        for name, content in sorted(files, key=lambda item: item[0])
    )
    return "h1:" + base64.b64encode(hashlib.sha256(summary.encode()).digest()).decode()


def go_zip_hash(path: Path) -> str | None:
    """The zip's go.sum hash, or None if any part of it cannot be read."""
    try:
        with zipfile.ZipFile(path) as archive:
            return go_hash1(
                (info.filename, archive.read(info))
                for info in archive.infolist()
                if not info.is_dir()
            )
    except Exception:  # Damaged compressed data raises zlib.error, EOFError, ...
        return None


def go_versions(root: Path) -> Iterable[tuple[str, str, Path]]:
    """Yield (escaped module path, file stem, file) for each downloaded version file."""
    download = root / "cache" / "download"
    for at_v in download.rglob("@v"):
        if not at_v.is_dir():
            continue
        module = at_v.parent.relative_to(download).as_posix()
        for path in at_v.iterdir():
            if path.is_file() and path.name not in ("list", "list.lock"):
                yield module, path.name.rsplit(".", 1)[0], path


def verify_go(root: Path, gosum: Path) -> list[Path]:
    """Delete the module zips and go.mod files whose hash differs from go.sum's.

    Go also trusts a zip's .ziphash sidecar, and fails on one that disagrees
    with go.sum without downloading the module again. A verified zip's sidecar
    is rewritten with its hash when it is missing, unreadable or wrong, and
    returned among the repairs.
    """
    sums = go_sums(gosum)
    damaged = []
    rewritten = []
    for module, version, path in list(go_versions(root)):
        expected = sums.get((module, version), {})
        if path.suffix == ".zip" and "zip" in expected:
            if go_zip_hash(path) != expected["zip"]:
                damaged.append(path)
                damaged.append(path.with_suffix(".ziphash"))
                continue
            sidecar = path.with_suffix(".ziphash")
            try:
                recorded = sidecar.read_text(encoding="utf-8").strip()
            except (OSError, UnicodeDecodeError):
                recorded = None
            if recorded != expected["zip"]:
                sidecar.write_text(expected["zip"], encoding="utf-8")
                rewritten.append(sidecar)
        elif path.suffix == ".mod" and "mod" in expected:
            try:
                content = path.read_bytes()
            except OSError:
                content = None
            if content is None or go_hash1([("go.mod", content)]) != expected["mod"]:
                damaged.append(path)
    for path in damaged:
        path.unlink(missing_ok=True)
    return [p for p in damaged if p.suffix != ".ziphash"] + rewritten


def trim_go(root: Path, gosum: Path) -> list[Path]:
    """Delete the downloaded versions go.sum no longer lists."""
    sums = go_sums(gosum)
    removed = [path for module, version, path in go_versions(root) if (module, version) not in sums]
    for path in removed:
        path.unlink()
    return [p for p in removed if p.suffix == ".zip"]


# --- npm -------------------------------------------------------------------


def npm_urls(lockfile: Path) -> set[str]:
    lock = json.loads(lockfile.read_text(encoding="utf-8"))
    return {
        package["resolved"]
        for package in lock.get("packages", {}).values()
        if isinstance(package, dict) and "resolved" in package
    }


def trim_npm(root: Path, lockfile: Path) -> list[str]:
    """Drop the index entries for URLs the lockfile no longer resolves.

    npm's `npm cache verify` then deletes the content no entry refers to.
    """
    keep = npm_urls(lockfile)
    prefix = "make-fetch-happen:request-cache:"
    dropped = []
    for bucket in files_under(root / "_cacache" / "index-v5"):
        lines = bucket.read_text(encoding="utf-8").splitlines()
        kept = []
        for line in lines:
            try:
                key = json.loads(line.split("\t", 1)[1])["key"]
            except (IndexError, ValueError, KeyError, TypeError):
                continue
            if key.startswith(prefix) and key[len(prefix) :] in keep:
                kept.append(line)
            else:
                dropped.append(key)
        if not kept:
            bucket.unlink()
        elif len(kept) != len(lines):
            bucket.write_text("\n".join(kept) + "\n", encoding="utf-8")
    subprocess.run(["npm", "cache", "verify", "--cache", str(root)], check=True)
    return dropped


# --- Commands --------------------------------------------------------------


def run_restored(args: argparse.Namespace) -> int:
    root = Path(args.dir)
    repaired = 0
    if args.matched_key:
        print(f"Restored the {args.kind} packages from {args.matched_key}.")
        if args.lockfile and args.kind in ("cargo", "go"):
            verify = verify_cargo if args.kind == "cargo" else verify_go
            try:
                damaged = verify(root, Path(args.lockfile))
            except Exception as error:
                print(f"::warning::Could not check the restored {args.kind} packages ({error}); "
                      "this job fetches its packages from upstream.")
                shutil.rmtree(root, ignore_errors=True)
                damaged = []
                repaired = -1
            for path in damaged:
                if path.suffix == ".ziphash":
                    module = path.parent.parent.relative_to(root / "cache" / "download")
                    print(f"::warning::Rewrote {module}/@v/{path.name} in the restored go "
                          "packages with its verified archive's hash, which go.sum lists.")
                else:
                    print(f"::warning::Deleted {path.name} from the restored {args.kind} packages: "
                          "its checksum is not the lockfile's, so the job downloads it again.")
            repaired = repaired or len(damaged)
    else:
        print(f"No {args.kind} package cache was restored; this job fetches its packages from upstream.")
        shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
    # The mark records the repairs, which make main save a corrected copy. A
    # restore keeps the mtimes of the run that saved the cache, so backdating
    # the mark by a second never counts a restored file, and it does count a
    # file the filesystem's coarse clock stamps with the mark's own time.
    mark = marker(root)
    mark.write_text(f"{repaired}\n", encoding="utf-8")
    now = time.time() - 1
    os.utime(mark, (now, now))
    return 0


def decide(
    matched_key: str, key_prefix: str, added: int, repaired: int, unit: str
) -> tuple[bool, str]:
    if not matched_key:
        return True, "nothing was restored"
    if not matched_key.startswith(key_prefix):
        return True, f"it restored {matched_key}, saved for other inputs"
    if repaired < 0:
        return True, "the copy for these inputs could not be checked"
    if repaired:
        return True, f"{repaired} damaged files in the copy for these inputs were replaced"
    if added:
        return True, f"the copy for these inputs lacked {added} {unit}"
    return False, "the copy for these inputs already held every package"


def run_report(args: argparse.Namespace) -> int:
    try:
        return report(args)
    except Exception as error:  # A report never fails the job.
        print(f"::warning::Could not report the {args.kind} package cache: {error}")
        return 0


def report(args: argparse.Namespace) -> int:
    root = Path(args.dir)
    mark = marker(root)
    if not mark.exists():
        print(f"The {args.kind} package cache in {root} was never prepared.")
        return 0
    added = len(added_files(args.kind, root, mark.stat().st_mtime))
    try:
        repaired = int(mark.read_text(encoding="utf-8").strip() or 0)
    except ValueError:
        repaired = 0
    size = human(size_of(cached_paths(args.kind, root)))
    unit = UNITS[args.kind]
    print(f"The {args.kind} package cache holds {size}; this job added {added} {unit} to it.")
    if args.decide is None:
        return 0
    # The key is <family>-<lockfile hash>-<run id>-<run attempt>.
    key_prefix = args.decide.rsplit("-", 2)[0] + "-"
    save, reason = decide(args.matched_key, key_prefix, added, repaired, unit)
    main_push = (
        os.environ.get("GITHUB_EVENT_NAME") == "push"
        and os.environ.get("GITHUB_REF") == "refs/heads/main"
    )
    action = f"save it as {args.decide}" if save else "keep the restored copy"
    lead = "This push to main will" if main_push else "A push to main would"
    print(f"{lead} {action}: {reason}.")
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as output_file:
            output_file.write(f"{args.kind}-save={str(save).lower()}\n")
    return 0


def run_trim(args: argparse.Namespace) -> int:
    root, lockfile = Path(args.dir), Path(args.lockfile)
    before = size_of(cached_paths(args.kind, root))
    trim = {"cargo": trim_cargo, "go": trim_go, "npm": trim_npm}[args.kind]
    removed = trim(root, lockfile)
    after = size_of(cached_paths(args.kind, root))
    print(
        f"Deleted {len(removed)} {args.kind} entries {lockfile} no longer lists; "
        f"the cache shrank from {human(before)} to {human(after)}."
    )
    return 0


def run_key(args: argparse.Namespace) -> int:
    read = tree_reader(None)
    os_name, arch = os.environ.get("RUNNER_OS", "Linux"), os.environ.get("RUNNER_ARCH", "X64")
    family = family_prefix(args.family, os_name, arch, read)
    lock = lock_prefix(args.family, os_name, arch, read)
    run = f"{os.environ.get('GITHUB_RUN_ID', '0')}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    outputs = {"family": family, "lock": lock, "key": f"{lock}{run}"}
    lines = "".join(f"{args.output_prefix}{name}={value}\n" for name, value in outputs.items())
    print(lines, end="")
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a", encoding="utf-8") as output_file:
            output_file.write(lines)
    return 0


def created_at(cache: Mapping) -> datetime:
    return datetime.fromisoformat(cache["created_at"].replace("Z", "+00:00"))


def superseded(caches: Iterable[Mapping], current: Iterable[str]) -> list[Mapping]:
    """Return the package caches to delete.

    In each ecosystem, across uv releases: all but the newest copy for each
    `current` lock prefix and the OTHERS_KEPT newest copies for other inputs.
    The current inputs' copy is kept whichever run saved it last, so main runs
    that overlap and save out of order cannot delete it.
    """
    current = tuple(current)
    kept_current: set[str] = set()
    kept_others: dict[str, int] = {}
    stale = []
    for cache in sorted(caches, key=lambda c: (created_at(c), c["id"]), reverse=True):
        match = PACKAGE_KEY.match(cache["key"])
        if not match:
            continue
        prefix = next((p for p in current if cache["key"].startswith(p)), None)
        if prefix is not None:
            if prefix in kept_current:
                stale.append(cache)
            else:
                kept_current.add(prefix)
            continue
        group = match["group"]
        if kept_others.get(group, 0) >= OTHERS_KEPT:
            stale.append(cache)
        else:
            kept_others[group] = kept_others.get(group, 0) + 1
    return stale


def github(method: str, path: str, token: str) -> dict | None:
    api = os.environ.get("GITHUB_API_URL", "https://api.github.com")
    request = urllib.request.Request(
        f"{api}{path}",
        method=method,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read()
    return json.loads(body) if body else None


def git(*args: str) -> str | None:
    result = subprocess.run(["git", *args], capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else None


def fetch_tip(remote: str, branch: str) -> str | None:
    """Fetch the branch's newest commit and return it."""
    if git("fetch", "--no-tags", "--depth=1", remote, branch) is None:
        return None
    commit = git("rev-parse", "--verify", "FETCH_HEAD^{commit}")
    # Its inputs are read from the tree; a missing input file is hashed as
    # `key` hashes it, but an unreadable tree tells no copies apart.
    return commit if commit and git("rev-parse", "--verify", f"{commit}^{{tree}}") else None


def remote_tip(remote: str, branch: str) -> str | None:
    listing = git("ls-remote", remote, f"refs/heads/{branch}")
    return listing.split()[0] if listing else None


def now() -> datetime:
    return datetime.now(timezone.utc)


def run_prune(args: argparse.Namespace) -> int:
    token = os.environ["GH_TOKEN"]
    fetched_at = now()
    tip = fetch_tip(args.remote, args.branch)
    if tip is None:
        print(f"::warning::Deleting no package caches: could not read {args.branch}'s newest commit.")
        return 0
    read = tree_reader(tip)
    os_name, arch = os.environ.get("RUNNER_OS", "Linux"), os.environ.get("RUNNER_ARCH", "X64")
    current = [lock_prefix(family, os_name, arch, read) for family in FAMILIES]
    caches: list[dict] = []
    for page in range(1, 100):
        query = urllib.parse.urlencode({"key": KEY_PREFIX, "ref": args.ref, "per_page": 100, "page": page})
        batch = github("GET", f"/repos/{args.repository}/actions/caches?{query}", token)["actions_caches"]
        caches.extend(batch)
        if len(batch) < 100:
            break
    # A copy saved after the fetch may be for a newer commit's inputs, which
    # this run cannot see; a margin covers the clocks' skew. Such copies are
    # neither deleted nor counted, and a later run judges them.
    settled = [c for c in caches if created_at(c) < fetched_at - SETTLE_MARGIN]
    stale = superseded(settled, current)
    print(
        f"{len(caches)} package caches on {args.ref}, {len(caches) - len(settled)} too recent to judge; "
        f"{len(stale)} superseded. Kept for {args.branch} at {tip}:"
    )
    for prefix in current:
        print(f"  {prefix}*")
    # The newest commit's inputs decide what is kept, so a commit that landed
    # meanwhile, and may have saved its own copy, leaves the deletions to the
    # next run.
    if (moved := remote_tip(args.remote, args.branch)) != tip:
        print(f"::warning::Deleting no package caches: {args.branch} moved to {moved} during the cleanup.")
        return 0
    for cache in stale:
        print(f"Delete {cache['key']} ({cache.get('size_in_bytes', 0) / 1e6:.0f} MB, created {cache['created_at']})")
        try:
            github("DELETE", f"/repos/{args.repository}/actions/caches/{cache['id']}", token)
        except urllib.error.HTTPError as error:
            # Another main run may have deleted it first.
            if error.code != 404:
                raise
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

    key = commands.add_parser("key")
    key.add_argument("family", choices=sorted(FAMILIES))
    key.add_argument("--output-prefix", default="", help="prefix for the step outputs' names")
    key.set_defaults(run=run_key)

    prune = commands.add_parser("prune")
    prune.add_argument("--repository", required=True)
    prune.add_argument("--ref", required=True)
    prune.add_argument("--remote", default="origin")
    prune.add_argument("--branch", default="main", help="the branch whose newest inputs' copies are kept")
    prune.set_defaults(run=run_prune)

    restored = commands.add_parser("restored")
    restored.add_argument("kind", choices=KINDS)
    restored.add_argument("dir")
    restored.add_argument("matched_key")
    restored.add_argument("--lockfile", help="Cargo.lock or go.sum to check archives against")
    restored.set_defaults(run=run_restored)

    report = commands.add_parser("report")
    report.add_argument("kind", choices=KINDS)
    report.add_argument("dir")
    report.add_argument("--decide", metavar="KEY", help="the key main would save the cache under")
    report.add_argument("--matched-key", default="")
    report.set_defaults(run=run_report)

    trim = commands.add_parser("trim")
    trim.add_argument("kind", choices=("cargo", "go", "npm"))
    trim.add_argument("dir")
    trim.add_argument("lockfile")
    trim.set_defaults(run=run_trim)

    args = parser.parse_args(argv)
    if hasattr(args, "dir") and not args.dir.rstrip("/"):
        parser.error("the cache directory must not be empty or /")
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
