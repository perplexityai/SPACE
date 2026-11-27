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

try:
    import yaml  # type: ignore[import-untyped]
except ImportError:
    yaml = None

SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
YAML_FIELD_RE = re.compile(r"(?m)^(name|version|description):[ \t]*(.+?)[ \t]*$")
YAML_VERSION_RE = re.compile(
    r"(?m)^(version:[ \t]*['\"]?)(\d+\.\d+\.\d+)(['\"]?(?:[ \t]+#.*)?[ \t]*)$"
)

JSON_MANIFESTS = (
    ".claude-plugin/plugin.json",
    ".codex-plugin/plugin.json",
)
ALL_MANIFESTS = ("plugin.yaml", *JSON_MANIFESTS)


class PluginError(Exception):
    pass


def _strip_yaml_inline_comment(raw: str) -> str:
    """Strip an unquoted `# ...` comment from a single-line YAML scalar."""
    if not raw:
        return raw
    quote = raw[0] if raw[0] in ("'", '"') else None
    if quote == "'":
        i = 1
        while i < len(raw):
            if raw[i] == "'":
                if i + 1 < len(raw) and raw[i + 1] == "'":
                    i += 2
                    continue
                return raw[: i + 1]
            i += 1
        return raw
    if quote == '"':
        i = 1
        while i < len(raw):
            if raw[i] == "\\" and i + 1 < len(raw):
                i += 2
                continue
            if raw[i] == '"':
                return raw[: i + 1]
            i += 1
        return raw
    comment_match = re.search(r"[ \t]+#", raw)
    return raw[: comment_match.start()].rstrip() if comment_match else raw


def unquote_yaml_scalar(raw: str) -> str:
    cleaned = _strip_yaml_inline_comment(raw)
    if cleaned in (">", "|", ">-", "|-", ">+", "|+"):
        raise PluginError(
            "PyYAML is required to parse block scalars in plugin.yaml; "
            "install pyyaml or run under `uv run`"
        )
    if len(cleaned) >= 2 and cleaned[0] == "'" and cleaned[-1] == "'":
        return cleaned[1:-1].replace("''", "'")
    if len(cleaned) >= 2 and cleaned[0] == '"' and cleaned[-1] == '"':
        return json.loads(cleaned)
    return cleaned


def parse_plugin_yaml_text(text: str, label: str) -> dict[str, str]:
    if yaml is not None:
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise PluginError(f"{label}: invalid YAML: {exc}") from exc
        if not isinstance(data, dict):
            raise PluginError(f"{label}: expected top-level YAML mapping")
        fields: dict[str, str] = {}
        for key in ("name", "version", "description"):
            val = data.get(key)
            if isinstance(val, str):
                fields[key] = val.strip() if key == "description" else val
        return fields

    fields = {}
    for match in YAML_FIELD_RE.finditer(text):
        key, raw_value = match.group(1), match.group(2)
        if key not in fields:
            fields[key] = unquote_yaml_scalar(raw_value)
    return fields


def parse_plugin_json_text(text: str, label: str) -> dict[str, str]:
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise PluginError(f"{label}: invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise PluginError(f"{label}: expected top-level JSON object")
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
        if entry.is_dir() and any((entry / rel).is_file() for rel in ALL_MANIFESTS)
    )


def validate_manifest_fields(
    plugin: str,
    manifests: list[tuple[str, dict[str, str]]],
) -> tuple[str, list[str]]:
    errors: list[str] = []
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


def check_plugin(repo: Path, plugin: str) -> tuple[str, list[str]]:
    """Return `(canonical_version, errors)` for a single plugin."""
    base = repo / ".agents/plugins" / plugin
    errors: list[str] = []
    manifests: list[tuple[str, dict[str, str]]] = []

    yaml_path = base / "plugin.yaml"
    if not yaml_path.is_file():
        errors.append(f"{yaml_path.relative_to(repo)}: missing manifest")
    else:
        try:
            manifests.append(
                (
                    "plugin.yaml",
                    parse_plugin_yaml_text(
                        yaml_path.read_text(encoding="utf-8"),
                        str(yaml_path.relative_to(repo)),
                    ),
                )
            )
        except PluginError as exc:
            errors.append(str(exc))

    for rel in JSON_MANIFESTS:
        json_path = base / rel
        if not json_path.is_file():
            errors.append(f"{json_path.relative_to(repo)}: missing manifest")
            continue
        try:
            manifests.append(
                (
                    rel,
                    parse_plugin_json_text(
                        json_path.read_text(encoding="utf-8"),
                        str(json_path.relative_to(repo)),
                    ),
                )
            )
        except PluginError as exc:
            errors.append(str(exc))

    if not manifests or manifests[0][0] != "plugin.yaml":
        return "", errors

    version, field_errors = validate_manifest_fields(plugin, manifests)
    return version, errors + field_errors


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


def replace_top_level_json_version(text: str, new_version: str, label: str) -> str:
    """Replace only the root object's `"version"` string value, preserving formatting."""
    # Validate JSON structure first.
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise PluginError(f"{label}: invalid JSON: {exc}") from exc
    if not isinstance(parsed, dict) or not isinstance(parsed.get("version"), str):
        raise PluginError(f"{label}: missing top-level string `version` field")

    n = len(text)
    i = 0
    depth = 0
    in_string = False
    expect_key = False
    matches: list[tuple[int, int]] = []

    def read_json_string(start: int) -> tuple[str, int]:
        j = start + 1
        while j < n:
            ch = text[j]
            if ch == "\\":
                j += 2
                continue
            if ch == '"':
                return json.loads(text[start : j + 1]), j + 1
            j += 1
        raise PluginError(f"{label}: unterminated JSON string")

    while i < n:
        ch = text[i]
        if ch in " \t\r\n":
            i += 1
            continue
        if ch == "{":
            depth += 1
            expect_key = depth == 1
            i += 1
            continue
        if ch == "}":
            depth -= 1
            expect_key = False
            i += 1
            continue
        if ch == "[":
            depth += 1
            expect_key = False
            i += 1
            continue
        if ch == "]":
            depth -= 1
            expect_key = False
            i += 1
            continue
        if ch == ",":
            if depth == 1:
                expect_key = True
            i += 1
            continue
        if ch == '"':
            if depth == 1 and expect_key:
                key, next_i = read_json_string(i)
                expect_key = False
                i = next_i
                while i < n and text[i] in " \t\r\n":
                    i += 1
                if i < n and text[i] == ":":
                    i += 1
                    while i < n and text[i] in " \t\r\n":
                        i += 1
                    if key == "version" and i < n and text[i] == '"':
                        val_start = i
                        _, val_end = read_json_string(i)
                        matches.append((val_start, val_end))
                        i = val_end
                continue
            _, i = read_json_string(i)
            continue
        i += 1

    if len(matches) != 1:
        raise PluginError(
            f"{label}: expected 1 top-level `version` field, found {len(matches)}"
        )

    start, end = matches[0]
    updated = text[:start] + json.dumps(new_version) + text[end:]
    json.loads(updated)
    return updated


def write_version(repo: Path, plugin: str, new_version: str) -> None:
    if not SEMVER_RE.match(new_version):
        raise PluginError(f"target version {new_version!r} is not X.Y.Z semver")

    plugins_root = (repo / ".agents/plugins").resolve()
    base = (plugins_root / plugin).resolve()
    if base.parent != plugins_root or not base.is_dir():
        raise PluginError(f"unknown plugin {plugin!r}")

    updates: list[tuple[Path, str]] = []
    proposed_manifests: list[tuple[str, dict[str, str]]] = []

    yaml_path = (base / "plugin.yaml").resolve()
    if not yaml_path.is_relative_to(base) or not yaml_path.is_file():
        raise PluginError(f".agents/plugins/{plugin}/plugin.yaml: missing manifest")
    yaml_label = f".agents/plugins/{plugin}/plugin.yaml"
    yaml_text = yaml_path.read_text(encoding="utf-8")
    updated_yaml, count = YAML_VERSION_RE.subn(rf"\g<1>{new_version}\g<3>", yaml_text, count=1)
    if count != 1:
        raise PluginError(f"{yaml_label}: could not locate version line")
    proposed_manifests.append(("plugin.yaml", parse_plugin_yaml_text(updated_yaml, yaml_label)))
    updates.append((yaml_path, updated_yaml))

    for rel in JSON_MANIFESTS:
        json_path = (base / rel).resolve()
        if not json_path.is_relative_to(base) or not json_path.is_file():
            raise PluginError(f".agents/plugins/{plugin}/{rel}: missing manifest")
        json_label = f".agents/plugins/{plugin}/{rel}"
        json_text = json_path.read_text(encoding="utf-8")
        updated_json = replace_top_level_json_version(json_text, new_version, json_label)
        proposed_manifests.append((rel, parse_plugin_json_text(updated_json, json_label)))
        updates.append((json_path, updated_json))

    # Validate the complete proposed manifest set before writing the first file.
    _, post_errors = validate_manifest_fields(plugin, proposed_manifests)
    if post_errors:
        raise PluginError("proposed manifests would remain out of sync:\n  " + "\n  ".join(post_errors))

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
