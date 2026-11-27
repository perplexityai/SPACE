#!/usr/bin/env python3
"""Check and bump SPACE agent plugin manifest versions.

Every plugin under `.agents/plugins/<plugin>/` ships three manifests whose
`name`, `version`, and `description` must stay aligned:

* `plugin.yaml`
* `.claude-plugin/plugin.json`
* `.codex-plugin/plugin.json`

Usage:
    python3 scripts/bump_plugin_version.py --check
    python3 scripts/bump_plugin_version.py --bump pplx-space
    python3 scripts/bump_plugin_version.py --bump pplx-space --part minor
    python3 scripts/bump_plugin_version.py --bump pplx-space --version 0.3.0
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
YAML_FIELD_RE = re.compile(r"(?m)^(name|version|description):[ \t]*(.+?)[ \t]*$")
YAML_VERSION_RE = re.compile(r"(?m)^(version:[ \t]*['\"]?)(\d+\.\d+\.\d+)(['\"]?[ \t]*)$")
JSON_VERSION_RE = re.compile(r'("version":[ \t]*")(\d+\.\d+\.\d+)(")')

JSON_MANIFESTS = (
    ".claude-plugin/plugin.json",
    ".codex-plugin/plugin.json",
)


class PluginError(Exception):
    pass


def unquote_yaml_scalar(raw: str) -> str:
    if len(raw) >= 2 and raw[0] == "'" and raw[-1] == "'":
        return raw[1:-1].replace("''", "'")
    if len(raw) >= 2 and raw[0] == '"' and raw[-1] == '"':
        return json.loads(raw)
    return raw


def parse_plugin_yaml(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8")
    fields: dict[str, str] = {}
    for match in YAML_FIELD_RE.finditer(text):
        key, raw_value = match.group(1), match.group(2)
        if key not in fields:
            fields[key] = unquote_yaml_scalar(raw_value)
    return fields


def parse_plugin_json(path: Path) -> dict[str, str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PluginError(f"{path}: invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise PluginError(f"{path}: expected top-level JSON object")
    fields: dict[str, str] = {}
    for key in ("name", "version", "description"):
        val = data.get(key)
        if isinstance(val, str):
            fields[key] = val
    return fields


def discover_plugins(repo: Path) -> list[str]:
    plugins_dir = repo / ".agents/plugins"
    if not plugins_dir.is_dir():
        return []
    return sorted(
        entry.name
        for entry in plugins_dir.iterdir()
        if entry.is_dir() and (entry / "plugin.yaml").is_file()
    )


def check_plugin(repo: Path, plugin: str) -> tuple[str, list[str]]:
    """Return `(canonical_version, errors)` for a single plugin."""
    base = repo / ".agents/plugins" / plugin
    errors: list[str] = []
    manifests: list[tuple[str, dict[str, str]]] = []

    yaml_path = base / "plugin.yaml"
    if not yaml_path.is_file():
        return "", [f"{yaml_path.relative_to(repo)}: missing manifest"]
    manifests.append(("plugin.yaml", parse_plugin_yaml(yaml_path)))

    for rel in JSON_MANIFESTS:
        json_path = base / rel
        if not json_path.is_file():
            errors.append(f"{json_path.relative_to(repo)}: missing manifest")
            continue
        try:
            manifests.append((rel, parse_plugin_json(json_path)))
        except PluginError as exc:
            errors.append(str(exc))

    canonical = manifests[0][1]
    for field in ("name", "version", "description"):
        if not canonical.get(field):
            errors.append(f".agents/plugins/{plugin}/plugin.yaml: missing `{field}`")

    version = canonical.get("version", "")
    if version and not SEMVER_RE.match(version):
        errors.append(
            f".agents/plugins/{plugin}/plugin.yaml: version {version!r} is not X.Y.Z semver"
        )

    for rel, fields in manifests[1:]:
        for field in ("name", "version", "description"):
            got = fields.get(field)
            want = canonical.get(field)
            if got != want:
                errors.append(
                    f".agents/plugins/{plugin}/{rel}: `{field}` is {got!r}, "
                    f"expected {want!r} (from plugin.yaml)"
                )

    return version, errors


def next_semver(version: str, part: str) -> str:
    match = SEMVER_RE.match(version)
    if not match:
        raise PluginError(f"version {version!r} is not X.Y.Z semver")
    major, minor, patch = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    if part == "patch":
        return f"{major}.{minor}.{patch + 1}"
    if part == "minor":
        return f"{major}.{minor + 1}.0"
    if part == "major":
        return f"{major + 1}.0.0"
    raise PluginError(f"unknown bump part: {part!r}")


def write_version(repo: Path, plugin: str, new_version: str) -> None:
    if not SEMVER_RE.match(new_version):
        raise PluginError(f"target version {new_version!r} is not X.Y.Z semver")

    base = repo / ".agents/plugins" / plugin
    updates: list[tuple[Path, str]] = []

    yaml_path = base / "plugin.yaml"
    yaml_text = yaml_path.read_text(encoding="utf-8")
    updated_yaml, count = YAML_VERSION_RE.subn(rf"\g<1>{new_version}\g<3>", yaml_text, count=1)
    if count != 1:
        raise PluginError(f"{yaml_path.relative_to(repo)}: could not locate version line")
    updates.append((yaml_path, updated_yaml))

    for rel in JSON_MANIFESTS:
        json_path = base / rel
        json_text = json_path.read_text(encoding="utf-8")
        updated_json, count = JSON_VERSION_RE.subn(rf"\g<1>{new_version}\g<3>", json_text, count=1)
        if count != 1:
            raise PluginError(f"{json_path.relative_to(repo)}: could not locate version field")
        # Validate JSON syntax before writing anything.
        json.loads(updated_json)
        updates.append((json_path, updated_json))

    for path, content in updates:
        path.write_text(content, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check or bump SPACE agent plugin versions.")
    parser.add_argument(
        "--repo",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="Repository root (default: parent of scripts/).",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--check",
        action="store_true",
        help="Verify all plugin manifests have aligned name, version, and description.",
    )
    mode.add_argument(
        "--bump",
        metavar="PLUGIN",
        help="Bump the named plugin's version across all three manifests.",
    )
    parser.add_argument(
        "--part",
        choices=("patch", "minor", "major"),
        default="patch",
        help="Semver component to increment with --bump (default: patch).",
    )
    parser.add_argument(
        "--version",
        dest="explicit_version",
        help="Explicit X.Y.Z version to set with --bump (also allows resyncing drifted versions).",
    )
    args = parser.parse_args(argv)

    if args.explicit_version and not args.bump:
        parser.error("--version requires --bump")

    repo = args.repo.resolve()

    if args.bump:
        plugin = args.bump
        current_version, errors = check_plugin(repo, plugin)
        if errors and not args.explicit_version:
            print(
                f"Refusing to bump {plugin}: manifests are out of sync "
                "(pass --version X.Y.Z to resync):",
                file=sys.stderr,
            )
            for err in errors:
                print(f"  {err}", file=sys.stderr)
            return 1
        try:
            target = (
                args.explicit_version
                if args.explicit_version
                else next_semver(current_version, args.part)
            )
            write_version(repo, plugin, target)
        except PluginError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        _, post_errors = check_plugin(repo, plugin)
        if post_errors:
            for err in post_errors:
                print(f"  {err}", file=sys.stderr)
            return 1
        print(f"{plugin}: {current_version or '<drifted>'} -> {target}")
        return 0

    plugins = discover_plugins(repo)
    if not plugins:
        print("error: no plugins found under .agents/plugins/", file=sys.stderr)
        return 1

    all_errors: list[str] = []
    for plugin in plugins:
        _, errors = check_plugin(repo, plugin)
        all_errors.extend(errors)

    if all_errors:
        for err in all_errors:
            print(err, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
