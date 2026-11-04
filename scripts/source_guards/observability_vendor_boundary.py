#!/usr/bin/env python3
"""Keep Datadog SDK dependencies inside the Rust observability implementation.

Product crates use the metrics facade; SDKs are optional implementation details
of observability. Check normal, build, dev, and target-specific dependencies,
including renamed packages and workspace inheritance. Go telemetry and vendor
deployment configuration are separate boundaries, outside this Rust guard.
"""

from __future__ import annotations

import pathlib
import sys
import tomllib

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
OWNER = pathlib.Path("crates/l10_foundation/observability/Cargo.toml")


def is_vendor_sdk(package: str) -> bool:
    package = package.replace("_", "-")
    return package.startswith(("datadog-", "libdd-")) or package in {
        "datadog", "opentelemetry-datadog", "dogstatsd", "dogstatsd-client", "cadence", "statsd",
    }


def violations(root: pathlib.Path) -> list[str]:
    workspace = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]
    inherited = workspace.get("dependencies", {})
    failures = []
    for path in sorted(root.glob("crates/**/Cargo.toml")):
        manifest = tomllib.loads(path.read_text())
        relative = path.relative_to(root)
        tables = [manifest, *manifest.get("target", {}).values()]
        for table in tables:
            for section in ("dependencies", "build-dependencies", "dev-dependencies"):
                for alias, value in table.get(section, {}).items():
                    spec = value if isinstance(value, dict) else {}
                    shared = inherited.get(alias, {}) if spec.get("workspace") else {}
                    shared = shared if isinstance(shared, dict) else {}
                    package = spec.get("package", shared.get("package", alias))
                    if not is_vendor_sdk(package):
                        continue
                    if relative != OWNER:
                        failures.append(
                            f"{relative}: {section}.{alias} ({package}) belongs in {OWNER}"
                        )
                    elif section != "dev-dependencies" and not spec.get("optional", False):
                        failures.append(
                            f"{relative}: {alias} must be optional so vendor-free builds remain possible"
                        )
    return failures


def main() -> int:
    failures = violations(REPO_ROOT)
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print("Rust observability vendor dependency boundary: passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
