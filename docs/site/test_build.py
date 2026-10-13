"""Regression checks for the small, fail-closed assembly boundary."""

import json
from pathlib import Path
import shutil
import tempfile
import unittest

from build import copy_bundle, digest


class BundleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source = Path(self.temp.name) / "source"
        self.source.mkdir()
        self.destination = Path(self.temp.name) / "site"
        (self.source / "index.html").write_text("current")
        self.metadata = {
            "commit": "current", "dirty": False,
            "files": {"index.html": digest(self.source / "index.html")},
        }

    def copy(self):
        (self.source / "build.json").write_text(json.dumps(self.metadata))
        return copy_bundle(self.source, self.destination, "current")

    def test_only_manifest_files_are_copied(self):
        (self.source / "old-screenshot.png").write_text("stale")
        (self.source / "unrelated.txt").write_text("not part of this build")
        (self.source / "theme").mkdir()
        for name in ("font.woff2", "style.css", "LICENSE.txt"):
            (self.source / "theme" / name).write_text(f"synthetic {name}")
            self.metadata["files"][f"theme/{name}"] = digest(self.source / "theme" / name)
        (self.source / "theme/unapproved.css").write_text("not part of this build")
        self.copy()
        self.assertEqual(
            {file.relative_to(self.destination).as_posix() for file in self.destination.rglob("*") if file.is_file()},
            set(self.metadata["files"]),
        )
        for name in self.metadata["files"]:
            self.assertEqual(digest(self.destination / name), self.metadata["files"][name])

    def test_stale_revision_is_rejected(self):
        self.metadata["commit"] = "old"
        with self.assertRaisesRegex(ValueError, "stale or uncommitted"):
            self.copy()

    def test_dirty_output_is_rejected(self):
        self.metadata["dirty"] = True
        with self.assertRaisesRegex(ValueError, "stale or uncommitted"):
            self.copy()

    def test_changed_output_is_rejected(self):
        (self.source / "theme").mkdir()
        (self.source / "theme/font.woff2").write_text("synthetic font")
        self.metadata["files"]["theme/font.woff2"] = digest(self.source / "theme/font.woff2")
        for name in ("index.html", "theme/font.woff2"):
            original = (self.source / name).read_bytes()
            with self.subTest(asset=name, failure="changed"):
                (self.source / name).write_text("changed after build")
                with self.assertRaisesRegex(ValueError, "Changed artifact"):
                    self.copy()
                shutil.rmtree(self.destination)
                (self.source / name).write_bytes(original)
            with self.subTest(asset=name, failure="missing"):
                (self.source / name).unlink()
                with self.assertRaises(FileNotFoundError):
                    self.copy()
                shutil.rmtree(self.destination)
                (self.source / name).write_bytes(original)

    def test_traversal_and_absolute_paths_are_rejected(self):
        for name in ("../outside", str(self.source / "index.html")):
            with self.subTest(name=name):
                self.metadata["files"] = {name: "unused"}
                with self.assertRaisesRegex(ValueError, "Unsafe artifact"):
                    self.copy()
                self.destination.rmdir()

    def test_symlink_is_rejected(self):
        (self.source / "alias.html").symlink_to("index.html")
        self.metadata["files"] = {"alias.html": digest(self.source / "index.html")}
        with self.assertRaisesRegex(ValueError, "Unsafe artifact"):
            self.copy()


if __name__ == "__main__":
    unittest.main(verbosity=2)
