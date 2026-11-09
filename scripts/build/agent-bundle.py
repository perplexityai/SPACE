#!/usr/bin/env python3
"""Write the digest-stable descriptor beside an already-built Linux guest pair.

Run once when publishing a binary bundle, before seed/OCI assembly. Deployment
authorization deliberately lives in a separate document.
The Rust bundle reader is the authoritative schema validator.
"""
import argparse
import hashlib
import json
import re
from pathlib import Path


def describe(path: Path) -> dict:
    digest = hashlib.sha256()
    length = 0
    with path.open("rb") as source:
        while chunk := source.read(65536):
            length += len(chunk)
            digest.update(chunk)
    if not 0 < length <= 256 * 1024 * 1024:
        raise ValueError(f"{path.name} exceeds binary size contract")
    return {"length": length, "digest": "sha256:" + digest.hexdigest()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--built-at", required=True, help="canonical UTC RFC3339 bundle build time")
    parser.add_argument("--version", required=True)
    parser.add_argument("--guest-update-required-since", required=True, help="last published workspace release requiring existing guests to update")
    parser.add_argument("--family", help="defaults to the package compatibility family")
    parser.add_argument("--arch", required=True, choices=("x86_64", "aarch64"))
    args = parser.parse_args()
    stable = r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    match = re.fullmatch(stable, args.version)
    marker = re.fullmatch(stable, args.guest_update_required_since)
    if not match or not marker or not re.fullmatch(r"[0-9a-f]{40,64}", args.revision):
        parser.error("versions must be stable SemVer and revision must be a full lowercase revision hash")
    if tuple(map(int, marker.groups())) > tuple(map(int, match.groups())):
        parser.error("guest-update marker cannot be newer than the bundle version")
    major, minor, patch = match.groups()
    family = args.family or (major if major != "0" else f"0.{minor}" if minor != "0" else f"0.0.{patch}")
    # Declaration order is schema 1's canonical JSON order, shared with
    # pplx-space-spaced-update::BundleDescriptor. No map ordering is implicit.
    descriptor = {
        "schema": 1,
        "source_revision": args.revision,
        "built_at": args.built_at,
        "version": args.version,
        "guest_update_required_since": args.guest_update_required_since,
        "compatibility_family": family,
        "architecture": args.arch,
        "seed_contracts": ["linux-systemd-selector-v1"],
        "service": describe(args.directory / "spaced-svc"),
        "cli": describe(args.directory / "spaced"),
    }
    encoded = json.dumps(descriptor, separators=(",", ":"), ensure_ascii=False).encode()
    with (args.directory / "artifact.json").open("xb") as destination:
        destination.write(encoded)
    print("sha256:" + hashlib.sha256(encoded).hexdigest())


if __name__ == "__main__":
    main()
