#!/usr/bin/env python3
"""Install a verified guest bundle and atomically select it in the node catalog.

Run during trusted node image assembly or as root for directly installed services.
Installing a target selects it immediately for its recovery contract and
architecture; other entries in that catalog are preserved. Container deployments
must bake every supported platform target into the image. Eligibility is checked
by the manager against guest capability and the trusted image profile.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
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


def authorization_for(promotion):
    # UUIDv8 is content-derived authority, not a new chronological operation ID.
    # Independent image rebuilds must preserve the same receipt/retry authority.
    encoded = json.dumps(promotion, sort_keys=True, separators=(",", ":")).encode()
    value = bytearray(hashlib.sha256(b"space.guest-update.authority.v1\0" + encoded).digest()[:16])
    value[6] = (value[6] & 0x0f) | 0x80
    value[8] = (value[8] & 0x3f) | 0x80
    return str(uuid.UUID(bytes=bytes(value)))


def stable_version(value):
    if not isinstance(value, str) or re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", value) is None:
        raise ValueError("catalog requires stable workspace versions")
    parts = tuple(map(int, value.split(".")))
    if any(part > 2**64 - 1 for part in parts):
        raise ValueError("version component exceeds semver range")
    return parts


def validate_catalog(catalog):
    if not isinstance(catalog, dict) or set(catalog) != {"targets"} or not isinstance(catalog["targets"], dict):
        raise ValueError("existing policy.json is not a platform catalog; drain guest updates, remove the invalid file, then reinstall each platform target")
    digests = set()
    fields = {"channel", "target", "target_version", "guest_update_required_since", "intent", "minimum_host", "maximum_guest"}
    for platform, entries in catalog["targets"].items():
        if platform not in CONTRACTS or not isinstance(entries, dict):
            raise ValueError("invalid platform entry")
        for architecture, entry in entries.items():
            if architecture not in ("x86_64", "aarch64") or not isinstance(entry, dict) or set(entry) != {"authorization", "promotion"}:
                raise ValueError("invalid catalog target entry")
            if not isinstance(entry["authorization"], str) or uuid.UUID(entry["authorization"]).int == 0:
                raise ValueError("invalid catalog authorization")
            promotion = entry["promotion"]
            if not isinstance(promotion, dict) or set(promotion) != fields:
                raise ValueError("invalid catalog promotion fields")
            if not isinstance(promotion["channel"], str) or re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", promotion["channel"]) is None:
                raise ValueError("invalid catalog channel")
            digest = promotion["target"]
            if not isinstance(digest, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None or digest in digests:
                raise ValueError("invalid or duplicate catalog target digest")
            digests.add(digest)
            if promotion["intent"] not in ("upgrade", "rollback"):
                raise ValueError("invalid catalog selection intent")
            version = stable_version(promotion["target_version"])
            marker = stable_version(promotion["guest_update_required_since"])
            ceiling = stable_version(promotion["maximum_guest"])
            stable_version(promotion["minimum_host"])
            if not marker <= version <= ceiling:
                raise ValueError("invalid catalog marker or ceiling")


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
    versions = [descriptor["version"], descriptor["guest_update_required_since"], args.minimum_host or descriptor["version"]]
    parsed_versions = [stable_version(value) for value in versions]
    if parsed_versions[1] > parsed_versions[0]:
        raise ValueError("guest marker is newer than target")
    digest = "sha256:" + hashlib.sha256(data).hexdigest()
    names = {"service": "spaced-svc" + CONTRACTS[contract], "cli": "spaced" + CONTRACTS[contract]}
    for role, name in names.items():
        if identity(args.bundle / name) != descriptor[role]:
            raise ValueError(f"payload identity mismatch: {name}")
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
        validate_catalog(catalog)
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
        authorization = args.authorization or (previous["authorization"] if previous and previous["promotion"] == promotion else authorization_for(promotion))
        targets[arch] = {"authorization": authorization, "promotion": promotion}
        validate_catalog(catalog)
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
    parser.add_argument("--minimum-host", help="defaults to the installed workspace version")
    parser.add_argument("--channel", default="stable")
    parser.add_argument("--authorization", help="fresh UUIDv7 to explicitly reauthorize an investigated promotion")
    args = parser.parse_args()
    if args.authorization:
        try:
            value = uuid.UUID(args.authorization)
            if value.version != 7:
                raise ValueError("explicit authorization must be UUIDv7")
            args.authorization = str(value)
        except ValueError as error:
            parser.error(str(error))
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
