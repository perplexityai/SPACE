#!/usr/bin/env python3
"""Prepare, report and trim a package cache restored from GitHub's Actions cache.

A CI job restores Cargo's registry, Go's module download cache, uv's cache or
npm's cache, so it downloads only the packages its lockfile changed. Keys are
<family>-<lockfile hash>-<run id>-<run attempt>, restored by the prefix
<family>-<lockfile hash>- and then <family>-: the newest copy for this lockfile,
else the newest for any. Only one job per cache saves it, and only on main. A
missing or unusable copy only makes a job slower: the tools fetch whatever is
absent from upstream as before.

  restored  after the restore and before anything reads the cache. When no key
            matched, nothing was restored or the restore failed, possibly after
            unpacking part of an archive, so the directory is emptied. Then
            drops any Cargo or Go archive whose checksum the lockfile does not
            vouch for: both tools take an archive in their cache as downloaded,
            and a damaged one fails every build instead of costing a download.
            Finally marks the time, which `report` compares against.
  report    prints the cache's size and the packages the job added since
            `restored`. With --decide it also decides whether main saves the
            cache: when it was restored for another lockfile, or not at all, or
            when the job had to add packages to this lockfile's copy because
            that copy was incomplete. Writes the `<kind>-save` step output;
            only main acts on it. Never fails.
  trim      before main saves a copy restored for another lockfile: deletes the
            Cargo, Go or npm packages the current lockfile no longer lists, so
            the copy does not grow with every lockfile change. uv's copies are
            pruned by uv itself (see common-ci.yml).

<dir> is the Cargo registry (CARGO_HOME/registry), GOMODCACHE, UV_CACHE_DIR or
npm's cache. The time mark sits beside <dir>, outside what is cached.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import tomllib
import zipfile
from pathlib import Path
from typing import Iterable

KINDS = ("cargo", "go", "uv", "npm")
UNITS = {
    "cargo": "crates",
    "go": "module archives",
    "uv": "downloaded distributions",
    "npm": "cache entries",
}


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
    """The files a fetch writes for each package, newer than `since`."""
    if kind == "cargo":
        candidates = files_under(root / "cache", "*.crate")
    elif kind == "go":
        candidates = files_under(root / "cache" / "download", "*.zip")
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
    try:
        with zipfile.ZipFile(path) as archive:
            return go_hash1(
                (info.filename, archive.read(info))
                for info in archive.infolist()
                if not info.is_dir()
            )
    except (OSError, zipfile.BadZipFile, ValueError):
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
    """Delete the module zips and go.mod files whose hash differs from go.sum's."""
    sums = go_sums(gosum)
    damaged = []
    for module, version, path in list(go_versions(root)):
        expected = sums.get((module, version), {})
        if path.suffix == ".zip" and "zip" in expected:
            if go_zip_hash(path) != expected["zip"]:
                damaged.append(path)
                damaged.append(path.with_suffix(".ziphash"))
        elif path.suffix == ".mod" and "mod" in expected:
            if go_hash1([("go.mod", path.read_bytes())]) != expected["mod"]:
                damaged.append(path)
    for path in damaged:
        path.unlink(missing_ok=True)
    return [p for p in damaged if p.suffix != ".ziphash"]


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
    if args.matched_key:
        print(f"Restored the {args.kind} packages from {args.matched_key}.")
        if args.lockfile and args.kind in ("cargo", "go"):
            verify = verify_cargo if args.kind == "cargo" else verify_go
            damaged = verify(root, Path(args.lockfile))
            for path in damaged:
                print(f"::warning::Deleted {path.name} from the restored {args.kind} packages: "
                      "its checksum is not the lockfile's, so the job downloads it again.")
    else:
        print(f"No {args.kind} package cache was restored; this job fetches its packages from upstream.")
        shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
    # A restore keeps the mtimes of the run that saved the cache, so backdating
    # the mark by a second never counts a restored file, and it does count a
    # file the filesystem's coarse clock stamps with the mark's own time.
    mark = marker(root)
    mark.touch()
    now = time.time() - 1
    os.utime(mark, (now, now))
    return 0


def decide(matched_key: str, key_prefix: str, added: int, unit: str) -> tuple[bool, str]:
    if not matched_key:
        return True, "nothing was restored"
    if not matched_key.startswith(key_prefix):
        return True, f"it restored {matched_key}, saved for another lockfile"
    if added:
        return True, f"the copy for this lockfile lacked {added} {unit}"
    return False, "the copy for this lockfile already held every package"


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
    size = human(size_of(cached_paths(args.kind, root)))
    unit = UNITS[args.kind]
    print(f"The {args.kind} package cache holds {size}; this job added {added} {unit} to it.")
    if args.decide is None:
        return 0
    # The key is <family>-<lockfile hash>-<run id>-<run attempt>.
    key_prefix = args.decide.rsplit("-", 2)[0] + "-"
    save, reason = decide(args.matched_key, key_prefix, added, unit)
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


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)

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
    if not args.dir.rstrip("/"):
        parser.error("the cache directory must not be empty or /")
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
