"""Boot-file integrity and reproducibility, independent of runtime qualification."""

import copy
import hashlib
import importlib.util
import io
from pathlib import Path
import stat
import tempfile
import unittest
import zipfile

SCRIPT = Path(__file__).resolve().parents[1] / "build/wasi-python-bundle.py"
SPEC = importlib.util.spec_from_file_location("wasi_python_bundle", SCRIPT)
bundle = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bundle)


class WasiPythonBundleTests(unittest.TestCase):
    def source(self, *, reverse=False, extra=None):
        files = [
            ("python.wasm", b"\0asm\x01\0\0\0synthetic-module"),
            ("LICENSE", b"synthetic test license"),
            ("lib/python3.15/encodings/__init__.py", b"encoding_fixture = True\n"),
            ("lib/python3.15/site.py", b"fixture = 17\n"),
        ]
        if extra:
            files.append(extra)
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            for name, data in reversed(files) if reverse else files:
                archive.writestr(name, data)
        return output.getvalue()

    def pins(self, data):
        with zipfile.ZipFile(io.BytesIO(data)) as source:
            library = bundle.library_zip(source, "lib/python3.15/")
            module = source.read("python.wasm")
        pin = lambda kind, payload: {
            "kind": kind,
            "version": "synthetic-v1",
            "sha256": hashlib.sha256(payload).hexdigest(),
            "size": len(payload),
        }
        return {
            "schema": 1,
            "source_url": "https://example.com/synthetic.zip",
            "source_sha256": hashlib.sha256(data).hexdigest(),
            "library_prefix": "lib/python3.15/",
            "module": pin("wasm_module", module),
            "library": pin("guest_tools", library),
        }

    def test_library_reproducibility_preserves_original_files_without_zlib(self):
        archives = [self.source(), self.source(reverse=True)]
        outputs = []
        for data in archives:
            with zipfile.ZipFile(io.BytesIO(data)) as source:
                outputs.append(bundle.library_zip(source, "lib/python3.15/"))
        self.assertEqual(outputs[0], outputs[1])
        with zipfile.ZipFile(io.BytesIO(outputs[0])) as library:
            self.assertEqual(library.namelist(), ["encodings/__init__.py", "site.py"])
            self.assertEqual(library.read("site.py"), b"fixture = 17\n")
            for entry in library.infolist():
                self.assertEqual(entry.compress_type, zipfile.ZIP_STORED)
                self.assertEqual(entry.date_time, (1980, 1, 1, 0, 0, 0))
                self.assertEqual(entry.external_attr >> 16, stat.S_IFREG | 0o444)

    def test_failed_pins_never_publish_a_partial_or_replacement_bundle(self):
        data = self.source()
        pins = self.pins(data)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for key in ("source_sha256", "module", "library"):
                invalid = copy.deepcopy(pins)
                if key == "source_sha256":
                    invalid[key] = "0" * 64
                else:
                    invalid[key]["sha256"] = "0" * 64
                with self.subTest(key=key), self.assertRaises(ValueError):
                    bundle.build_bundle(data, invalid, root / "output")
                self.assertEqual(list(root.iterdir()), [])
            output = root / "output"
            bundle.build_bundle(data, pins, output)
            before = {str(path.relative_to(output)): path.read_bytes()
                      for path in output.rglob("*") if path.is_file()}
            with self.assertRaises(FileExistsError):
                bundle.build_bundle(data, pins, output)
            after = {str(path.relative_to(output)): path.read_bytes()
                     for path in output.rglob("*") if path.is_file()}
            self.assertEqual(before, after)
            self.assertEqual((output / "LICENSE").read_bytes(), b"synthetic test license")
            self.assertEqual(list(root.iterdir()), [output])

    def test_noncanonical_and_duplicate_archive_members_are_rejected(self):
        for name in (
            "lib/python3.15/../escape.py",
            "lib/python3.15//duplicate.py",
            "/absolute.py",
            "lib\\python3.15\\escape.py",
            "lib/python3.15/site.py",
        ):
            with self.subTest(name=name):
                data = self.source(extra=(name, b"malformed"))
                with zipfile.ZipFile(io.BytesIO(data)) as source:
                    with self.assertRaises(ValueError):
                        bundle.library_zip(source, "lib/python3.15/")


if __name__ == "__main__":
    unittest.main()
