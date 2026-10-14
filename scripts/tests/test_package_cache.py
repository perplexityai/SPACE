"""Exercise ci/scripts/package-cache.py on fixture caches: what a failed restore,
a damaged archive, a lockfile change and a partial copy each do."""

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
            "cargo": ("cache/index.example/old-1.0.0.crate", ["cache/index.example/new-1.0.0.crate"],
                      ["src/index.example/new-1.0.0/lib.rs"]),
            "go": ("cache/download/example.com/a/@v/v1.0.0.zip", ["cache/download/example.com/b/@v/v1.0.0.zip"],
                   ["cache/download/example.com/b/@v/v1.0.0.mod", "example.com/b@v1.0.0/b.go"]),
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
        self.assertIn("saved for another lockfile", output)
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


if __name__ == "__main__":
    unittest.main()
