#!/usr/bin/env bash
# Requires Python 3.11+, Node 22.12+, npm, Git and the pinned mdBook version.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
for command in python3 git mdbook node npm; do
  command -v "$command" >/dev/null || { echo "Missing prerequisite: $command" >&2; exit 1; }
done
python3 -c 'import sys; sys.exit("Python 3.11+ is required to build the SPACE Guide.") if sys.version_info < (3, 11) else None'
version="$(<docs/book/mdbook.version)"
if [[ "$(mdbook --version)" != "mdbook v$version" ]]; then
  echo "Install mdBook $version (see docs/book/mdbook.version)." >&2
  exit 1
fi
node -e 'const [major, minor] = process.versions.node.split(".").map(Number); if (major < 22 || (major === 22 && minor < 12)) throw new Error("Node.js 22.12+ is required");'
if [[ "${SPACE_DOCS_NPM_CI_DONE:-}" != 1 ]]; then
  npm ci --prefix docs/api --ignore-scripts --no-fund --no-audit
fi
node docs/book/bundle-mermaid.mjs
python3 docs/book/build.py
