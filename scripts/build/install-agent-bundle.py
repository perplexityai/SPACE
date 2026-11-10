#!/usr/bin/env python3
"""Install a verified guest bundle and atomically select it in the node catalog.

Run during trusted node image assembly or as root during node installation.
Qualification names exact published template digests; it is not inferred from
contents reported by a guest. Published recovery-contract metadata qualifies
templates by default; the optional digest list covers older qualified images.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import tempfile
import time
import uuid

MAX_METADATA = 64 * 1024
CONTRACTS = {"linux-systemd-selector-v1": "", "windows-scm-two-slot-v1": ".exe"}


def identity(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"not a regular bundle file: {path.name}")
    size = path.stat().st_size
    if not 0 < size <= 256 * 1024 * 1024:
        raise ValueError(f"invalid payload size: {path.name}")
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(65536):
            digest.update(chunk)
    return {"length": size, "digest": "sha256:" + digest.hexdigest()}


def read_json(path):
    if path.is_symlink():
        raise ValueError(f"refusing symlink: {path}")
    with path.open("rb") as source:
        data = source.read(MAX_METADATA + 1)
    if len(data) > MAX_METADATA:
        raise ValueError("metadata exceeds size limit")
    return json.loads(data), data


def uuid7():
    timestamp = time.time_ns() // 1_000_000
    return str(uuid.UUID(int=(timestamp << 80) | (7 << 76) |
                         (secrets.randbits(12) << 64) | (2 << 62) | secrets.randbits(62)))


def directory(path):
    # Offline image roots are permitted, but existing path components must not
    # redirect publication through symlinks or non-directories.
    for component in reversed((path, *path.parents)):
        if component.is_symlink() or (component.exists() and not component.is_dir()):
            raise ValueError(f"unsafe installation directory: {component}")
    path.mkdir(parents=True, exist_ok=True)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def install(args):
    descriptor, data = read_json(args.bundle / "artifact.json")
    # Schema 1 fixes the serde field order, including nested binary identities.
    # Compact JSON alone does not establish the manager's canonical digest.
    fields = ("schema", "source_revision", "built_at", "version", "guest_update_required_since",
              "compatibility_family", "architecture", "seed_contracts", "service", "cli")
    canonical_shape = isinstance(descriptor, dict) and tuple(descriptor) == fields and all(
        isinstance(descriptor[role], dict) and tuple(descriptor[role]) == ("length", "digest")
        and type(descriptor[role]["length"]) is int for role in ("service", "cli")
    )
    if not canonical_shape or type(descriptor["schema"]) is not int or descriptor["schema"] != 1 or data != json.dumps(descriptor, separators=(",", ":"), ensure_ascii=False).encode():
        raise ValueError("bundle must carry its canonical schema-1 descriptor")
    if re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", args.channel) is None:
        raise ValueError("channel must be a nonempty ASCII release label (at most 128 characters)")
    contracts = descriptor["seed_contracts"]
    if len(contracts) != 1 or contracts[0] not in CONTRACTS:
        raise ValueError("unsupported or ambiguous recovery contract")
    contract = contracts[0]
    arch = descriptor["architecture"]
    if arch not in ("x86_64", "aarch64") or (contract.startswith("windows-") and arch != "x86_64"):
        raise ValueError("unsupported target architecture")
    stable = r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    versions = [descriptor["version"], descriptor["guest_update_required_since"], args.minimum_host or descriptor["version"]]
    if any(re.fullmatch(stable, v) is None for v in versions):
        raise ValueError("installed targets require stable workspace versions")
    if tuple(map(int, versions[1].split('.'))) > tuple(map(int, versions[0].split('.'))):
        raise ValueError("guest marker is newer than target")
    digest = "sha256:" + hashlib.sha256(data).hexdigest()
    names = {"service": "spaced-svc" + CONTRACTS[contract], "cli": "spaced" + CONTRACTS[contract]}
    for role, name in names.items():
        if identity(args.bundle / name) != descriptor[role]:
            raise ValueError(f"payload identity mismatch: {name}")
    for template in args.qualified_template:
        if re.fullmatch(r"blake3:[0-9a-f]{64}", template) is None:
            raise ValueError("qualification requires a published blake3 template digest")
    bundles = args.root / "opt/space/guest-update"
    config = args.root / "etc/space/guest-update"
    directory(bundles)
    directory(config)
    # Serialize catalog changes so independent platform installers cannot lose
    # one another's selected authority. The manager reads only atomic snapshots.
    fd = os.open(config / "install.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        policy_path = config / "policy.json"
        catalog = read_json(policy_path)[0] if policy_path.exists() else {"targets": {}}
        if not isinstance(catalog, dict) or set(catalog) != {"targets"} or not isinstance(catalog["targets"], dict):
            raise ValueError("existing policy.json is not a platform catalog; drain guest updates, remove the invalid file, then reinstall each platform target")
        target = bundles / digest
        if target.exists() or target.is_symlink():
            if target.is_symlink() or read_json(target / "artifact.json")[1] != data:
                raise ValueError("conflicting immutable installed bundle")
            for role, name in names.items():
                if identity(target / name) != descriptor[role]:
                    raise ValueError("existing installed payload is corrupt")
        else:
            with tempfile.TemporaryDirectory(prefix=".install-", dir=bundles) as temporary:
                staged = Path(temporary) / "bundle"
                staged.mkdir(mode=0o755)
                for name in (*names.values(), "artifact.json"):
                    destination = staged / name
                    shutil.copyfile(args.bundle / name, destination)
                    destination.chmod(0o644 if name == "artifact.json" else 0o755)
                    with destination.open("rb") as source:
                        os.fsync(source.fileno())
                # Validate the copied bytes too; a mutable source is not authority.
                if read_json(staged / "artifact.json")[1] != data or any(identity(staged / name) != descriptor[role] for role, name in names.items()):
                    raise ValueError("bundle changed while installing")
                sync_directory(staged)
                os.rename(staged, target)
                sync_directory(bundles)
        targets = catalog["targets"].setdefault(contract, {})
        promotion = {"channel": args.channel, "target": digest, "target_version": versions[0],
                     "guest_update_required_since": versions[1], "intent": "upgrade",
                     "minimum_host": versions[2], "maximum_guest": versions[0]}
        previous = targets.get(arch)
        if previous and previous["promotion"]["target_version"] == versions[0] and previous["promotion"]["target"] != digest:
            raise ValueError("one workspace version cannot select different bytes for the same platform")
        # Reinstalling the same authority must not replenish the attempt budget.
        authorization = previous["authorization"] if previous and previous["promotion"] == promotion else uuid7()
        qualified = sorted(set(args.qualified_template))
        if not qualified and previous and previous["promotion"] == promotion:
            qualified = previous["qualified_templates"]
        targets[arch] = {"authorization": authorization, "promotion": promotion, "qualified_templates": qualified}
        encoded = json.dumps(catalog, separators=(",", ":")).encode()
        if len(encoded) > MAX_METADATA:
            raise ValueError("catalog exceeds metadata limit")
        with tempfile.NamedTemporaryFile(dir=config, prefix=".policy-", delete=False) as output:
            temporary = Path(output.name)
            try:
                output.write(encoded)
                output.flush()
                os.fchmod(output.fileno(), 0o644)
                os.fsync(output.fileno())
                os.replace(temporary, policy_path)
                sync_directory(config)
            finally:
                temporary.unlink(missing_ok=True)
    print(digest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--root", type=Path, default=Path("/"), help="offline image root or /")
    parser.add_argument("--qualified-template", action="append", default=[])
    parser.add_argument("--minimum-host", help="defaults to the installed workspace version")
    parser.add_argument("--channel", default="stable")
    args = parser.parse_args()
    if not args.root.is_absolute():
        parser.error("installation root must be absolute")
    if args.root == Path("/") and os.geteuid() != 0:
        parser.error("node installation requires root")
    try:
        install(args)
    except (ValueError, KeyError, OSError) as error:
        parser.exit(1, f"cannot install guest bundle: {error}\n")


if __name__ == "__main__":
    main()
