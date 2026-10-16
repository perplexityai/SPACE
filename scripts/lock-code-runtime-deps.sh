#!/usr/bin/env bash
# Re-resolve the code-runtime recipe's floating dependencies as of one UTC date:
# the hash-locked Python set and the Ubuntu archive snapshot used for APT.
#
# Usage: scripts/lock-code-runtime-deps.sh YYYY-MM-DD
#
# Review the resulting recipe diff like any other dependency update. Requires uv.
set -euo pipefail

date="${1:-}"
if [[ ! "$date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "usage: $0 YYYY-MM-DD" >&2
  exit 2
fi

recipe="$(cd "$(dirname "$0")/.." && pwd)/templates/firecracker/recipes/code-runtime.toml"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# requires-python must match the seed's python3; a mismatch fails the build
# because no locked wheel is installable. Builders are x86_64 or aarch64 Linux.
cat >"$work/pyproject.toml" <<EOF
[project]
name = "code-runtime-python"
version = "0"
requires-python = "==3.14.*"
dependencies = [
  "jupyter", "ipython", "jupyterlab",
  "bokeh", "kaleido",
  "nltk", "spacy",
  "numba",
  "numpy", "pandas", "scipy", "pillow",
  "imageio",
  "send2trash", "pygments", "jsonschema",
]

[tool.uv]
package = false
no-build = true
exclude-newer = "${date}T00:00:00Z"
environments = ["sys_platform == 'linux' and (platform_machine == 'x86_64' or platform_machine == 'aarch64')"]
EOF

uv lock --quiet --project "$work"
# The environment markers are always true on a builder; drop them for review.
uv export --quiet --frozen --project "$work" --no-header --no-annotate --no-emit-project |
  sed -E 's/ ; .* \\$/ \\/' >"$work/requirements.txt"

python3 - "$recipe" "$work/requirements.txt" "${date//-/}T000000Z" <<'PY'
import re
import sys
from pathlib import Path

recipe_path, requirements_path, snapshot = sys.argv[1:]
recipe = Path(recipe_path).read_text()
requirements = Path(requirements_path).read_text()

recipe, locks = re.subn(
    r"(<<'REQUIREMENTS'\n)(?:.*?\n)?(REQUIREMENTS\n)",
    lambda m: m.group(1) + requirements.rstrip("\n") + "\n" + m.group(2),
    recipe,
    flags=re.S,
)
recipe, snapshots = re.subn(
    r'APT_SNAPSHOT="[0-9]{8}T[0-9]{6}Z"', f'APT_SNAPSHOT="{snapshot}"', recipe
)
if (locks, snapshots) != (1, 1):
    sys.exit(f"expected one requirements block and one APT_SNAPSHOT in {recipe_path}")
Path(recipe_path).write_text(recipe)
PY
