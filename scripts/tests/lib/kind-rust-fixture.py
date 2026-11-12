"""Stage a small checkout for the Rust artifact key and handoff shell tests.

Use the real Dockerfiles and their context inputs. One file per copied tree
is enough to exercise tree hashing; crate source and manifest mutations also
need representatives. This keeps new COPY inputs visible without copying
and committing the entire workspace for each test process.
"""

import shlex
import shutil
import sys
from pathlib import Path

source, destination = map(Path, sys.argv[1:])
paths = {
    "docker/Dockerfile.rust-build-kind",
    "docker/Dockerfile.rust-build-common",
    "docker/Dockerfile.rust-artifacts-host",
    "infra/space/kind/scripts/build-rust-artifacts.sh",
    "infra/space/kind/lib/build-cache.sh",
    "infra/space/kind/lib/shared-build-inputs.sh",
    "infra/space/kind/lib/rust-build.sh",
    "ci/mirrored-images.txt",
    ".dockerignore",
}
for dockerfile in ("docker/Dockerfile.rust-build-kind", "docker/Dockerfile.rust-build-common"):
    for line in (source / dockerfile).read_text().splitlines():
        if not line.startswith("COPY "):
            continue
        tokens = shlex.split(line, comments=True)
        if tokens and tokens[0] == "COPY" and not any(t.startswith("--from=") for t in tokens):
            paths.update(t for t in tokens[1:-1] if not t.startswith("--"))

for relative in sorted(paths):
    path = source / relative
    if path.is_dir():
        files = sorted(p for p in path.rglob("*") if p.is_file() and not p.is_symlink())
        if not files:
            raise ValueError(f"context input has no fixture file: {path}")
        selected = {files[0]}
        required = {"crates": (".rs", "Cargo.toml"), "proto": (".proto",)}
        for suffix in required.get(relative, ()):
            representative = next((p for p in files if p.name.endswith(suffix)), None)
            if representative is None:
                raise ValueError(f"context input has no {suffix} fixture file: {path}")
            selected.add(representative)
    else:
        selected = {path}
    for item in selected:
        target = destination / item.relative_to(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item, target)

# Inputs the key must ignore need only distinguishable bytes, not their whole
# real implementation. None is executed by these fixture tests.
for relative in (
    "space_smoke/scenarios/fixture.py",
    "infra/space/kind/charts/fixture/values.yaml",
    "Tiltfile",
    ".github/workflows/kind-ci.yml",
    "docker/Dockerfile.api",
    "infra/space/kind/scripts/rust-artifact-handoff.py",
):
    target = destination / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("# unrelated input\n")
