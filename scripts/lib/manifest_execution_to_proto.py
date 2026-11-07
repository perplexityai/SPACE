#!/usr/bin/env python3
"""Convert a template manifest's execution metadata to proto JSON.

Reads the `manifest.json` given as argv[1] (the serde shape written by the
seed builder / template-manager) and prints the `space.execution.v1.
ExecutionMetadata` proto-JSON object for embedding in a grpcurl
`RegisterTemplateRelease` request. Registration is the canonical writer of a
release's execution metadata — the node publish path does not backfill it —
so a manifest whose schema does not vouch for its execution payload is an
error, not a silent omission: exits non-zero with a message on stderr.

The schema gate mirrors `TemplateManifest::execution_metadata()` in
`crates/l20_domain/template/manifest/src/manifest.rs`, and the value maps
mirror the enums in `proto/execution/v1/execution.proto`. Used by
`infra/space/kind/scripts/seed-publish-entry.sh` (staged into the
seed-publish Job image) and `scripts/space-upload-template.sh`.
"""

import json
import re
import sys

RUNTIME = {
    "firecracker": "RUNTIME_KIND_FIRECRACKER",
    "cloud-hypervisor": "RUNTIME_KIND_CLOUD_HYPERVISOR",
    "gvisor": "RUNTIME_KIND_GVISOR",
}
OS = {
    "linux": "GUEST_OPERATING_SYSTEM_LINUX",
    "windows": "GUEST_OPERATING_SYSTEM_WINDOWS",
    "macos": "GUEST_OPERATING_SYSTEM_MACOS",
}
ARCH = {
    "x86_64": "CPU_ARCHITECTURE_X86_64",
    "aarch64": "CPU_ARCHITECTURE_AARCH64",
}
COMPONENT = {
    "kernel": "ARTIFACT_COMPONENT_KIND_KERNEL",
    "initrd": "ARTIFACT_COMPONENT_KIND_INITRD",
    "firmware": "ARTIFACT_COMPONENT_KIND_FIRMWARE",
    "rootfs": "ARTIFACT_COMPONENT_KIND_ROOTFS",
    "boot_disk": "ARTIFACT_COMPONENT_KIND_BOOT_DISK",
    "guest_tools": "ARTIFACT_COMPONENT_KIND_GUEST_TOOLS",
}

# Mirrors the legacy execution and environment-bound manifest schemas.
MANIFEST_SCHEMA_VERSIONS = (2, 3)


def main() -> None:
    manifest = json.load(open(sys.argv[1]))
    if manifest.get("schema_version") not in MANIFEST_SCHEMA_VERSIONS:
        sys.exit(
            f"manifest schema_version {manifest.get('schema_version')!r} "
            "does not vouch for execution metadata"
        )
    execution = manifest.get("execution")
    if execution is None:
        sys.exit("manifest carries no execution metadata")

    environment = execution.get("environment")
    if (manifest["schema_version"] == 3) != (environment is not None):
        sys.exit("manifest schema and environment binding disagree")
    if environment is not None and (
        not isinstance(environment, str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", environment)
    ):
        sys.exit("invalid environment reference")

    out = {
        "runtime": {
            "kind": RUNTIME[execution["runtime"]["kind"]],
            "version": execution["runtime"]["version"],
        },
        "guest": {
            "os": OS[execution["guest"]["os"]],
            "architecture": ARCH[execution["guest"]["architecture"]],
        },
    }
    if environment is not None:
        out["environment"] = environment
    state_format = execution.get("state_format")
    if state_format is not None:
        out["state_format"] = {
            "format": state_format["format"],
            "version": state_format["version"],
        }
    components = []
    for component in execution.get("components") or []:
        entry = {"kind": COMPONENT[component["kind"]]}
        for key in ("role", "version", "digest", "format"):
            if component.get(key) is not None:
                entry[key] = component[key]
        components.append(entry)
    if components:
        out["components"] = components
    print(json.dumps(out))


if __name__ == "__main__":
    main()
