#!/usr/bin/env python3
"""Prepare pinned, reproducible Python/WASI boot files, not runtime admission."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import tempfile
from typing import BinaryIO
import urllib.request
import zipfile

MAX_BYTES = 128 * 1024 * 1024


def bounded_read(source: BinaryIO) -> bytes:
    data = source.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ValueError("Python/WASI source exceeds packaging limit")
    return data


def check_pin(data: bytes, pin: dict) -> None:
    if len(data) != pin["size"] or hashlib.sha256(data).hexdigest() != pin["sha256"]:
        raise ValueError("Python/WASI payload differs from its independent pin")


def library_zip(source: zipfile.ZipFile, prefix: str) -> bytes:
    """ZIP_STORED avoids a host zlib version and a guest zlib requirement."""
    entries = source.infolist()
    names = [entry.filename for entry in entries]
    if len(names) != len(set(names)):
        raise ValueError("duplicate Python/WASI archive member")
    if sum(entry.file_size for entry in entries) > MAX_BYTES:
        raise ValueError("unpacked Python/WASI source exceeds packaging limit")
    selected = []
    for entry in entries:
        name = entry.filename.rstrip("/")
        path = PurePosixPath(name)
        if (
            path.is_absolute()
            or "\\" in name
            or path.as_posix() != name
            or ".." in path.parts
            or stat.S_IFMT(entry.external_attr >> 16) not in (0, stat.S_IFREG, stat.S_IFDIR)
        ):
            raise ValueError("unsafe Python/WASI archive member")
        if not entry.is_dir() and entry.filename.startswith(prefix):
            selected.append(entry)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as target:
        for entry in sorted(selected, key=lambda entry: entry.filename):
            name = entry.filename.removeprefix(prefix)
            normalized = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
            normalized.create_system = 3
            normalized.external_attr = (stat.S_IFREG | 0o444) << 16
            target.writestr(normalized, source.read(entry))
    if not selected:
        raise ValueError("Python/WASI source has no standard library")
    return output.getvalue()


def build_bundle(data: bytes, pins: dict, output: Path) -> None:
    if pins["schema"] != 1 or hashlib.sha256(data).hexdigest() != pins["source_sha256"]:
        raise ValueError("Python/WASI source archive differs from its pin")
    prefix = pins["library_prefix"]
    if prefix != "lib/python3.15/":
        raise ValueError("unsupported Python/WASI library layout")
    with zipfile.ZipFile(io.BytesIO(data)) as source:
        library = library_zip(source, prefix)
        module = source.read("python.wasm")
        license_bytes = source.read("LICENSE")
    if module[:8] != b"\0asm\x01\0\0\0":
        raise ValueError("Python/WASI module is not a core WebAssembly module")
    payloads = []
    for key, kind, payload in [
        ("module", "wasm_module", module),
        ("library", "guest_tools", library),
    ]:
        pin = pins[key]
        if (
            pin["kind"] != kind
            or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._+-]*", pin["version"])
            or not re.fullmatch(r"[0-9a-f]{64}", pin["sha256"])
        ):
            raise ValueError("invalid Python/WASI boot-file identity")
        check_pin(payload, pin)
        # Runtime manager's auxiliary feed uses ManagedDirectoryName's compact
        # locator. The installer still authenticates the complete pinned digest.
        directory = f"{kind}-{pin['version']}--{pin['sha256'][:12]}"
        payloads.append((pin, f"boot-artifacts/{directory}/image", payload))
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".wasi-python-", dir=output.parent) as temporary:
        root = Path(temporary) / "bundle"
        root.mkdir()
        for _, path, payload in payloads:
            destination = root / path
            destination.parent.mkdir(parents=True)
            destination.write_bytes(payload)
            destination.chmod(0o444)
        (root / "LICENSE").write_bytes(license_bytes)
        manifest = {
            "schema": 1,
            "source_url": pins["source_url"],
            "source_sha256": pins["source_sha256"],
            "payloads": [{**pin, "path": path} for pin, path, _ in payloads],
        }
        (root / "manifest.json").write_text(json.dumps(manifest, sort_keys=True) + "\n")
        # Never replace an already published bundle or leave partial output.
        if output.exists():
            raise FileExistsError(output)
        os.rename(root, output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pins", required=True, type=Path)
    parser.add_argument("--archive", type=Path, help="offline source instead of HTTPS download")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    pins = json.loads(args.pins.read_text())
    if args.archive:
        with args.archive.open("rb") as source:
            data = bounded_read(source)
    else:
        if not pins["source_url"].startswith("https://"):
            parser.error("Python/WASI downloads require HTTPS")
        with urllib.request.urlopen(pins["source_url"], timeout=60) as source:
            data = bounded_read(source)
    build_bundle(data, pins, args.output)


if __name__ == "__main__":
    main()
