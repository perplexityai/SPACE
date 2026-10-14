"""Exercise ci/scripts/package-cache.py on fixture caches: what a failed restore,
a damaged archive, a lockfile change, a partial copy and pruning each do."""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
import zipfile
import zlib
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "package_cache_under_test", REPO_ROOT / "ci/scripts/package-cache.py"
)
assert SPEC is not None and SPEC.loader is not None
cache = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = cache
SPEC.loader.exec_module(cache)

OLD = 946684800  # 2000-01-01: an archive the saving run wrote.
KEY = f"space-packages-v1-Linux-X64-cargo-{'a' * 64}-200-1"


def write(path: Path, content: bytes | str = b"x", old: bool = False) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content.encode() if isinstance(content, str) else content)
    if old:
        os.utime(path, (OLD, OLD))
    return path


def run(*argv: str) -> str:
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        assert cache.main(list(argv)) == 0
    return output.getvalue()


def cargo_lock(path: Path, crates: dict[str, bytes]) -> Path:
    blocks = ['version = 4\n\n[[package]]\nname = "workspace-member"\nversion = "0.1.0"\n']
    for crate, content in crates.items():
        name, version = crate.rsplit("-", 1)
        blocks.append(
            f'[[package]]\nname = "{name}"\nversion = "{version}"\n'
            'source = "registry+https://github.com/rust-lang/crates.io-index"\n'
            f'checksum = "{hashlib.sha256(content).hexdigest()}"\n'
        )
    return write(path, "\n".join(blocks))


def go_zip(path: Path, module: str, files: dict[str, bytes]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in files.items():
            archive.writestr(f"{module}/{name}", content)
    os.utime(path, (OLD, OLD))
    return path


class CacheTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp, ignore_errors=True))


class RestoredTest(CacheTest):
    def test_empties_a_cache_whose_restore_matched_nothing(self) -> None:
        # A failed restore can leave part of an archive behind.
        registry = self.tmp / "cargo-home/registry"
        write(registry / "cache/index.example/partial-0.1.0.crate")
        self.assertIn("fetches its packages from upstream", run("restored", "cargo", str(registry), ""))
        self.assertEqual(list(registry.iterdir()), [])
        self.assertTrue((self.tmp / "cargo-home/registry.restored").exists())

    def test_drops_only_the_crates_whose_checksum_is_not_the_lockfiles(self) -> None:
        registry = self.tmp / "registry"
        good, bad = b"good crate", b"truncated"
        lock = cargo_lock(self.tmp / "Cargo.lock", {"good-1.0.0": good, "bad-2.0.0": b"bad crate"})
        write(registry / "cache/index.example/good-1.0.0.crate", good, old=True)
        write(registry / "cache/index.example/bad-2.0.0.crate", bad, old=True)
        write(registry / "cache/index.example/unlisted-3.0.0.crate", b"other", old=True)
        output = run("restored", "cargo", str(registry), KEY, "--lockfile", str(lock))
        self.assertIn("Deleted bad-2.0.0.crate", output)
        remaining = sorted(p.name for p in (registry / "cache/index.example").iterdir())
        # Trimming, not checking, removes crates the lockfile does not list.
        self.assertEqual(remaining, ["good-1.0.0.crate", "unlisted-3.0.0.crate"])

    def test_drops_a_damaged_go_module_archive(self) -> None:
        gomod = self.tmp / "go-mod"
        at_v = gomod / "cache/download/example.com/!upper/@v"
        good = go_zip(at_v / "v1.0.0.zip", "example.com/Upper@v1.0.0", {"a.go": b"package a\n"})
        write(at_v / "v1.0.0.ziphash", cache.go_zip_hash(good) or "", old=True)
        write(at_v / "v2.0.0.zip", b"not a zip", old=True)
        write(at_v / "v2.0.0.ziphash", b"h1:stale", old=True)
        gosum = write(
            self.tmp / "go.sum",
            f"example.com/Upper v1.0.0 {cache.go_zip_hash(good)}\n"
            "example.com/Upper v2.0.0 h1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n",
        )
        run("restored", "go", str(gomod), KEY, "--lockfile", str(gosum))
        self.assertEqual(sorted(p.name for p in at_v.iterdir()), ["v1.0.0.zip", "v1.0.0.ziphash"])

    def test_drops_a_go_zip_whose_compressed_data_is_damaged(self) -> None:
        gomod = self.tmp / "go-mod"
        at_v = gomod / "cache/download/example.com/a/@v"
        content = bytes(range(256)) * 64
        path = at_v / "v1.0.0.zip"
        path.parent.mkdir(parents=True)
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("example.com/a@v1.0.0/a.go", content)
        gosum = write(self.tmp / "go.sum", f"example.com/a v1.0.0 {cache.go_zip_hash(path)}\n")
        # Keep the central directory intact and break the DEFLATE stream.
        data = bytearray(path.read_bytes())
        start = data.index(b"a.go") + len(b"a.go")
        data[start : start + 40] = b"\xff" * 40
        path.write_bytes(bytes(data))
        with self.assertRaises(zlib.error):
            zipfile.ZipFile(path).read("example.com/a@v1.0.0/a.go")
        output = run("restored", "go", str(gomod), KEY, "--lockfile", str(gosum))
        self.assertIn("Deleted v1.0.0.zip", output)
        self.assertFalse(path.exists())

    def test_empties_the_cache_when_checking_it_fails(self) -> None:
        registry = self.tmp / "registry"
        write(registry / "cache/index.example/kept-1.0.0.crate", old=True)
        unreadable = write(self.tmp / "Cargo.lock", "not = [toml")
        output = run("restored", "cargo", str(registry), KEY, "--lockfile", str(unreadable))
        self.assertIn("::warning::Could not check the restored cargo packages", output)
        self.assertEqual(list(registry.iterdir()), [])

    def test_rewrites_a_wrong_or_missing_ziphash_of_a_verified_zip(self) -> None:
        # Go trusts the sidecar and fails on one that disagrees with go.sum.
        gomod = self.tmp / "go-mod"
        download = gomod / "cache/download"
        zips = {}
        for module in ("example.com/wrong", "example.com/missing", "example.com/right"):
            zips[module] = go_zip(download / module / "@v/v1.0.0.zip", f"{module}@v1.0.0", {"a.go": b"package a\n"})
        hashes = {module: cache.go_zip_hash(path) for module, path in zips.items()}
        write(download / "example.com/wrong/@v/v1.0.0.ziphash", "h1:" + "A" * 43 + "=", old=True)
        write(download / "example.com/right/@v/v1.0.0.ziphash", hashes["example.com/right"], old=True)
        gosum = write(self.tmp / "go.sum", "".join(f"{m} v1.0.0 {h}\n" for m, h in hashes.items()))
        output = run("restored", "go", str(gomod), KEY, "--lockfile", str(gosum))
        self.assertIn("Rewrote example.com/wrong/@v/v1.0.0.ziphash", output)
        self.assertIn("Rewrote example.com/missing/@v/v1.0.0.ziphash", output)
        self.assertNotIn("example.com/right", output)
        for module, digest in hashes.items():
            self.assertEqual((download / module / "@v/v1.0.0.ziphash").read_text(), digest)
        self.assertTrue(all(path.exists() for path in zips.values()))
        self.assertEqual((gomod.parent / "go-mod.restored").read_text().strip(), "2")

    def test_go_hash_matches_gos_own(self) -> None:
        # The go.sum entry Go computed for this public module's go.mod.
        self.assertEqual(
            cache.go_hash1([("go.mod", b"module github.com/modern-go/concurrent\n")]),
            "h1:6dJC0mAP4ikYIbvyc7fijjWJddQyLn8Ig3JB5CqoB9Q=",
        )


class ReportTest(CacheTest):
    def report(self, kind: str, root: Path, *extra: str) -> tuple[str, str]:
        github_output = self.tmp / "github-output"
        github_output.write_text("")
        env = {"GITHUB_OUTPUT": str(github_output), "GITHUB_EVENT_NAME": "push", "GITHUB_REF": "refs/heads/main"}
        with mock.patch.dict(os.environ, env):
            return run("report", kind, str(root), *extra), github_output.read_text()

    def test_counts_only_what_a_fetch_added_after_the_restore(self) -> None:
        cases = {
            # An index entry counts: a copy without it is incomplete.
            "cargo": ("cache/index.example/old-1.0.0.crate", ["cache/index.example/new-1.0.0.crate",
                      "index/index.example/.cache/ne/w/new"], ["src/index.example/new-1.0.0/lib.rs"]),
            "go": ("cache/download/example.com/a/@v/v1.0.0.zip", ["cache/download/example.com/b/@v/v1.0.0.zip",
                   "cache/download/example.com/b/@v/v1.0.0.mod", "cache/download/example.com/b/@v/v1.0.0.info"],
                   ["cache/download/example.com/b/@v/v1.0.0.ziphash", "cache/download/sumdb/x/latest",
                    "example.com/b@v1.0.0/b.go"]),
            # A local build or an assembled environment is not a download.
            "uv": ("wheels-v6/pypi/old/1.0-py3-none-any.http", ["wheels-v6/pypi/new/1.0-py3-none-any.http",
                   "sdists-v9/pypi/sdist/1.0/a.http"], ["archive-v0/built/pkg/__init__.py",
                   "simple-v22/pypi/new.rkyv", "environments-v2/env/x"]),
            "npm": ("_cacache/content-v2/sha512/aa/old", ["_cacache/content-v2/sha512/bb/new"],
                    ["_cacache/index-v5/bb/new"]),
        }
        for kind, (restored, fetched, ignored) in cases.items():
            with self.subTest(kind):
                root = self.tmp / kind
                write(root / restored, old=True)
                run("restored", kind, str(root), KEY)
                for name in fetched + ignored:
                    write(root / name)
                output, _ = self.report(kind, root)
                self.assertIn(f"this job added {len(fetched)} {cache.UNITS[kind]}", output)

    def test_saves_a_copy_restored_for_another_lockfile_or_not_at_all(self) -> None:
        root = self.tmp / "cargo"
        run("restored", "cargo", str(root), "")
        _, outputs = self.report("cargo", root, "--decide", KEY, "--matched-key", "")
        self.assertEqual(outputs, "cargo-save=true\n")
        other = f"space-packages-v1-Linux-X64-cargo-{'b' * 64}-100-1"
        output, outputs = self.report("cargo", root, "--decide", KEY, "--matched-key", other)
        self.assertIn("saved for other inputs", output)
        self.assertEqual(outputs, "cargo-save=true\n")

    def test_refreshes_this_lockfiles_copy_only_when_it_was_incomplete(self) -> None:
        root = self.tmp / "cargo"
        this_lockfile = f"space-packages-v1-Linux-X64-cargo-{'a' * 64}-100-1"
        run("restored", "cargo", str(root), this_lockfile)
        output, outputs = self.report("cargo", root, "--decide", KEY, "--matched-key", this_lockfile)
        self.assertIn("This push to main will keep the restored copy", output)
        self.assertEqual(outputs, "cargo-save=false\n")
        write(root / "cache/index.example/evicted-1.0.0.crate")
        output, outputs = self.report("cargo", root, "--decide", KEY, "--matched-key", this_lockfile)
        self.assertIn("lacked 1 crates", output)
        self.assertEqual(outputs, "cargo-save=true\n")

    def test_refreshes_a_copy_whose_metadata_it_repaired(self) -> None:
        # A damaged go.mod is deleted and fetched again, with no new archive.
        gomod = self.tmp / "go-mod"
        at_v = gomod / "cache/download/example.com/a/@v"
        write(at_v / "v1.0.0.mod", b"module damaged\n", old=True)
        good = cache.go_hash1([("go.mod", b"module example.com/a\n")])
        gosum = write(self.tmp / "go.sum", f"example.com/a v1.0.0/go.mod {good}\n")
        this_lockfile = f"space-packages-v1-Linux-X64-cargo-{'a' * 64}-100-1"
        run("restored", "go", str(gomod), this_lockfile, "--lockfile", str(gosum))
        self.assertFalse((at_v / "v1.0.0.mod").exists())
        output, outputs = self.report("go", gomod, "--decide", KEY, "--matched-key", this_lockfile)
        self.assertIn("1 damaged files in the copy for these inputs were replaced", output)
        self.assertEqual(outputs, "go-save=true\n")

    def test_saves_a_copy_it_could_not_check(self) -> None:
        registry = self.tmp / "registry"
        this_lockfile = f"space-packages-v1-Linux-X64-cargo-{'a' * 64}-100-1"
        run("restored", "cargo", str(registry), this_lockfile, "--lockfile", str(write(self.tmp / "Cargo.lock", "[")))
        output, outputs = self.report("cargo", registry, "--decide", KEY, "--matched-key", this_lockfile)
        self.assertIn("could not be checked", output)
        self.assertEqual(outputs, "cargo-save=true\n")

    def test_never_fails_for_a_cache_that_was_never_prepared(self) -> None:
        output, outputs = self.report("go", self.tmp / "absent", "--decide", KEY)
        self.assertIn("never prepared", output)
        self.assertEqual(outputs, "")


class TrimTest(CacheTest):
    def test_cargo_keeps_only_the_lockfiles_crates_and_index_entries(self) -> None:
        registry = self.tmp / "registry"
        lock = cargo_lock(self.tmp / "Cargo.lock", {"kept-1.0.0": b"k", "Mixed_Case-2.0.0": b"m"})
        for name in ("kept-1.0.0", "kept-0.9.0", "Mixed_Case-2.0.0", "dropped-1.0.0"):
            write(registry / f"cache/index.example/{name}.crate")
        for name in ("ke/pt/kept", "mi/xe/mixed_case", "dr/op/dropped"):
            write(registry / f"index/index.example/.cache/{name}")
        write(registry / "index/index.example/config.json")
        run("trim", "cargo", str(registry), str(lock))
        self.assertEqual(
            sorted(p.relative_to(registry).as_posix() for p in registry.rglob("*") if p.is_file()),
            [
                "cache/index.example/Mixed_Case-2.0.0.crate",
                "cache/index.example/kept-1.0.0.crate",
                "index/index.example/.cache/ke/pt/kept",
                "index/index.example/.cache/mi/xe/mixed_case",
                "index/index.example/config.json",
            ],
        )

    def test_go_keeps_only_the_versions_go_sum_lists(self) -> None:
        gomod = self.tmp / "go-mod"
        at_v = gomod / "cache/download/example.com/!upper/@v"
        for name in ("v1.0.0.zip", "v1.0.0.mod", "v1.1.0.mod", "v0.9.0.zip", "v0.9.0.mod", "list"):
            write(at_v / name)
        gosum = write(
            self.tmp / "go.sum",
            "example.com/Upper v1.0.0 h1:x=\nexample.com/Upper v1.0.0/go.mod h1:y=\n"
            "example.com/Upper v1.1.0/go.mod h1:z=\n",
        )
        run("trim", "go", str(gomod), str(gosum))
        self.assertEqual(sorted(p.name for p in at_v.iterdir()), ["list", "v1.0.0.mod", "v1.0.0.zip", "v1.1.0.mod"])

    def test_npm_drops_the_index_entries_the_lockfile_no_longer_resolves(self) -> None:
        npm = self.tmp / "npm"
        kept, dropped = "https://registry.example/kept/-/kept-1.0.0.tgz", "https://registry.example/old.tgz"
        lock = write(
            self.tmp / "package-lock.json",
            json.dumps({"packages": {"": {}, "node_modules/kept": {"resolved": kept}}}),
        )

        def entry(url: str) -> str:
            return "hash\t" + json.dumps({"key": f"make-fetch-happen:request-cache:{url}"})

        write(npm / "_cacache/index-v5/aa/mixed", f"{entry(kept)}\n{entry(dropped)}\n")
        write(npm / "_cacache/index-v5/bb/old", entry(dropped) + "\n")
        with mock.patch.object(cache.subprocess, "run") as npm_run:
            run("trim", "npm", str(npm), str(lock))
        npm_run.assert_called_once_with(["npm", "cache", "verify", "--cache", str(npm)], check=True)
        self.assertEqual((npm / "_cacache/index-v5/aa/mixed").read_text(), entry(kept) + "\n")
        self.assertFalse((npm / "_cacache/index-v5/bb/old").exists())


class KeyTest(CacheTest):
    def test_names_a_family_from_its_inputs_and_uv_release(self) -> None:
        def tree(files: dict[str, str]):
            return lambda path: files[path].encode() if path in files else None

        base = {"uv.lock": "a", "pyproject.toml": "b", "ci/scripts/run-python-tests.sh": "c",
                "ci/Dockerfile.ci": "FROM x\nARG UV_VERSION=0.12.18\n"}
        prefix = cache.lock_prefix("uv-python", "Linux", "X64", tree(base))
        self.assertTrue(prefix.startswith("space-packages-v1-Linux-X64-uv-python-uv0.12.18-"))
        self.assertTrue(cache.PACKAGE_KEY.match(prefix + "1-1"))
        # Another family's input, or the Dockerfile's other lines, leave it alone.
        unrelated = {**base, "scripts/netgtw/traffic/requirements.txt": "x", "ci/Dockerfile.ci": base["ci/Dockerfile.ci"] + "RUN y\n"}
        self.assertEqual(cache.lock_prefix("uv-python", "Linux", "X64", tree(unrelated)), prefix)
        self.assertNotEqual(cache.lock_prefix("uv-python", "Linux", "X64", tree({**base, "uv.lock": "z"})), prefix)
        bumped = {**base, "ci/Dockerfile.ci": "ARG UV_VERSION=0.13.0\n"}
        self.assertIn("-uv-python-uv0.13.0-", cache.lock_prefix("uv-python", "Linux", "X64", tree(bumped)))

    def test_writes_the_step_outputs(self) -> None:
        github_output = self.tmp / "github-output"
        env = {"GITHUB_OUTPUT": str(github_output), "RUNNER_OS": "Linux", "RUNNER_ARCH": "X64",
               "GITHUB_RUN_ID": "300", "GITHUB_RUN_ATTEMPT": "2"}
        with mock.patch.dict(os.environ, env), mock.patch.object(cache, "tree_reader", lambda tree: lambda path: b"x"):
            run("key", "cargo", "--output-prefix", "cargo-")
        outputs = dict(line.split("=", 1) for line in github_output.read_text().splitlines())
        self.assertEqual(outputs["cargo-family"], "space-packages-v1-Linux-X64-cargo-")
        self.assertTrue(outputs["cargo-key"].startswith(outputs["cargo-lock"]))
        self.assertTrue(outputs["cargo-key"].endswith("-300-2"))


class PruneTest(unittest.TestCase):
    CARGO = "space-packages-v1-Linux-X64-cargo"
    UV = "space-packages-v1-Linux-X64-uv-python"

    @staticmethod
    def entry(cache_id: int, key: str, minute: int) -> dict:
        return {"id": cache_id, "key": key, "created_at": f"2026-10-14T19:{minute:02d}:00Z"}

    def stale(self, caches: list[dict], current: list[str]) -> list[int]:
        return sorted(c["id"] for c in cache.superseded(caches, current))

    def test_keeps_the_current_inputs_when_three_overlapping_runs_save_out_of_order(self) -> None:
        # Main lands A, B and C; C's run saves first, then B's, then A's.
        a, b, c = (f"{self.CARGO}-{h * 64}-" for h in "abc")
        caches = [self.entry(1, c + "103-1", 10), self.entry(2, b + "102-1", 11), self.entry(3, a + "101-1", 12)]
        self.assertEqual(self.stale(caches, [c]), [2])

    def test_keeps_the_newest_copy_of_the_current_inputs_and_one_other(self) -> None:
        a, c = f"{self.CARGO}-{'a' * 64}-", f"{self.CARGO}-{'c' * 64}-"
        caches = [
            self.entry(1, c + "100-1", 10),
            self.entry(2, c + "105-1", 15),  # A refresh of the current inputs' copy.
            self.entry(3, a + "90-1", 5),
            self.entry(4, f"{self.CARGO}-{'b' * 64}-95-1", 7),
            self.entry(5, "space-sccache-v1-Linux-X64-tests-" + "d" * 64 + "-1-1", 1),
        ]
        self.assertEqual(self.stale(caches, [c]), [1, 3])

    def test_groups_uv_copies_across_releases(self) -> None:
        current = f"{self.UV}-uv0.12.18-{'c' * 64}-"
        caches = [
            self.entry(1, current + "100-1", 10),
            self.entry(2, f"{self.UV}-uv0.11.28-{'c' * 64}-90-1", 9),
            self.entry(3, f"{self.UV}-uv0.11.27-{'c' * 64}-80-1", 8),
            self.entry(4, f"space-packages-v1-Linux-X64-uv-shell-uv0.11.28-{'c' * 64}-80-1", 8),
        ]
        self.assertEqual(self.stale(caches, [current]), [3])

    # The cleanup fetched main at 19:30; entries are created at 19:<minute>.
    FETCHED_AT = cache.datetime(2026, 10, 14, 19, 30, tzinfo=cache.timezone.utc)

    @staticmethod
    def read(path: str) -> bytes | None:
        return b"ARG UV_VERSION=0.12.18\n" if path == "ci/Dockerfile.ci" else b"inputs"

    def run_prune(self, caches: list[dict], read=None, tip: str | None = "tip", tip_after: str | None = "tip"):
        deleted: list[int] = []

        def github(method: str, path: str, token: str):
            if method == "GET":
                return {"actions_caches": caches}
            cache_id = int(path.rsplit("/", 1)[1])
            if cache_id == 404:  # Another main run deleted it first.
                raise cache.urllib.error.HTTPError(path, 404, "Not Found", {}, None)
            deleted.append(cache_id)
            return None

        env = {"GH_TOKEN": "token", "RUNNER_OS": "Linux", "RUNNER_ARCH": "X64"}
        with mock.patch.dict(os.environ, env), mock.patch.object(cache, "github", github), \
                mock.patch.object(cache, "tree_reader", lambda tree: read or self.read), \
                mock.patch.object(cache, "fetch_tip", lambda remote, branch: tip), \
                mock.patch.object(cache, "remote_tip", lambda remote, branch: tip_after), \
                mock.patch.object(cache, "now", lambda: self.FETCHED_AT):
            output = run("prune", "--repository", "example/repo", "--ref", "refs/heads/main")
        return output, sorted(deleted)

    def test_prune_deletes_only_superseded_copies(self) -> None:
        current = cache.lock_prefix("cargo", "Linux", "X64", self.read)
        other = f"{self.CARGO}-{'a' * 64}-"
        caches = [
            self.entry(1, current + "100-1", 10),
            self.entry(2, other + "99-1", 9),
            self.entry(3, other + "98-1", 8),
            self.entry(404, other + "97-1", 7),
        ]
        output, deleted = self.run_prune(caches)
        self.assertEqual(deleted, [3])
        self.assertIn("4 package caches on refs/heads/main, 0 too recent to judge; 2 superseded", output)

    def test_prune_deletes_nothing_when_mains_tree_cannot_be_read(self) -> None:
        caches = [self.entry(n, f"{self.CARGO}-{'a' * 64}-{n}-1", n) for n in range(1, 5)]
        output, deleted = self.run_prune(caches, tip=None)
        self.assertEqual(deleted, [])
        self.assertIn("could not read main's newest commit", output)

    def test_prune_still_prunes_when_an_input_file_is_missing(self) -> None:
        # A family whose input was renamed hashes it as missing, as `key` does,
        # and the other families are pruned as usual.
        read = lambda path: None if path == "scripts/netgtw/traffic/requirements.txt" else self.read(path)
        github_output = Path(tempfile.mkdtemp()) / "github-output"
        self.addCleanup(lambda: __import__("shutil").rmtree(github_output.parent, ignore_errors=True))
        env = {"GITHUB_OUTPUT": str(github_output), "RUNNER_OS": "Linux", "RUNNER_ARCH": "X64"}
        with mock.patch.dict(os.environ, env), mock.patch.object(cache, "tree_reader", lambda tree: read):
            run("key", "uv-gateway")
        keyed = dict(line.split("=", 1) for line in github_output.read_text().splitlines())["lock"]
        self.assertEqual(keyed, cache.lock_prefix("uv-gateway", "Linux", "X64", read))
        current = cache.lock_prefix("cargo", "Linux", "X64", read)
        other = f"{self.CARGO}-{'a' * 64}-"
        caches = [self.entry(1, current + "100-1", 10), self.entry(2, other + "99-1", 9), self.entry(3, other + "98-1", 8)]
        _, deleted = self.run_prune(caches, read=read)
        self.assertEqual(deleted, [3])

    def test_prune_ignores_copies_saved_after_it_fetched_main(self) -> None:
        # Main was fetched at B. Then C landed and saved, and the older A saved
        # too, both after the fetch: neither is deleted, and B's is kept.
        b = cache.lock_prefix("cargo", "Linux", "X64", self.read)
        a, c = f"{self.CARGO}-{'a' * 64}-", f"{self.CARGO}-{'c' * 64}-"
        caches = [
            self.entry(1, b + "102-1", 10),
            self.entry(2, f"{self.CARGO}-{'d' * 64}-90-1", 5),
            self.entry(3, f"{self.CARGO}-{'e' * 64}-80-1", 4),
            self.entry(4, c + "103-1", 31),
            self.entry(5, a + "101-1", 32),
            self.entry(6, f"{self.CARGO}-{'f' * 64}-104-1", 28),  # Within the clock-skew margin.
        ]
        output, deleted = self.run_prune(caches)
        self.assertEqual(deleted, [3])
        self.assertIn("3 too recent to judge", output)

    def test_fetch_tip_reads_a_real_remote(self) -> None:
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp, ignore_errors=True))
        git = lambda cwd, *args: cache.subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True)
        git(tmp, "init", "-q", "-b", "main", "remote")
        write(tmp / "remote/Cargo.lock", "lock")
        git(tmp / "remote", "add", "Cargo.lock")
        git(tmp / "remote", "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "x")
        git(tmp, "init", "-q", "clone")
        with contextlib.chdir(tmp / "clone"):
            tip = cache.fetch_tip(str(tmp / "remote"), "main")
            self.assertEqual(tip, cache.remote_tip(str(tmp / "remote"), "main"))
            self.assertEqual(cache.tree_reader(tip)("Cargo.lock"), b"lock")
            self.assertIsNone(cache.tree_reader(tip)("go.sum"))
            self.assertIsNone(cache.fetch_tip(str(tmp / "remote"), "absent"))

    def test_prune_deletes_nothing_when_main_moves_during_the_cleanup(self) -> None:
        current = cache.lock_prefix("cargo", "Linux", "X64", self.read)
        other = f"{self.CARGO}-{'a' * 64}-"
        caches = [self.entry(1, current + "100-1", 10), self.entry(2, other + "99-1", 9), self.entry(3, other + "98-1", 8)]
        output, deleted = self.run_prune(caches, tip_after="newer")
        self.assertEqual(deleted, [])
        self.assertIn("main moved to newer during the cleanup", output)

if __name__ == "__main__":
    unittest.main()
