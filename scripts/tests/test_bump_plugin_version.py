"""Tests for scripts/bump_plugin_version.py."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TOOL = REPO_ROOT / "scripts/bump_plugin_version.py"


def run_tool(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(TOOL), "--repo", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def seed_plugin(
    repo: Path,
    plugin: str = "demo",
    *,
    name: str = "demo",
    version: str = "0.2.7",
    description: str = "Demo plugin.",
    claude_version: str | None = None,
    codex_version: str | None = None,
) -> Path:
    base = repo / ".agents/plugins" / plugin
    (base / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (base / ".codex-plugin").mkdir(parents=True, exist_ok=True)

    (base / "plugin.yaml").write_text(
        f"name: {name}\nversion: '{version}'\ndescription: '{description}'\n\ncomponents:\n  skills: ./skills/\n",
        encoding="utf-8",
    )
    (base / ".claude-plugin/plugin.json").write_text(
        json.dumps(
            {
                "name": name,
                "description": description,
                "version": claude_version or version,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (base / ".codex-plugin/plugin.json").write_text(
        json.dumps(
            {
                "name": name,
                "version": codex_version or version,
                "description": description,
                "skills": "./skills/",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return base


def test_live_repository_plugins_are_aligned() -> None:
    result = run_tool(REPO_ROOT, "--check")
    assert result.returncode == 0, result.stderr


def test_check_detects_version_drift(tmp_path: Path) -> None:
    seed_plugin(tmp_path, version="0.2.7", codex_version="0.2.8")
    result = run_tool(tmp_path, "--check")
    assert result.returncode == 1
    assert ".codex-plugin/plugin.json" in result.stderr
    assert "0.2.8" in result.stderr


def test_check_discovers_plugin_missing_plugin_yaml(tmp_path: Path) -> None:
    seed_plugin(tmp_path, plugin="complete")
    incomplete = seed_plugin(tmp_path, plugin="incomplete")
    (incomplete / "plugin.yaml").unlink()

    result = run_tool(tmp_path, "--check")
    assert result.returncode == 1
    assert ".agents/plugins/incomplete/plugin.yaml: missing manifest" in result.stderr


def test_yaml_inline_comments_and_folded_description(tmp_path: Path) -> None:
    base = seed_plugin(tmp_path, version="0.2.7", description="Demo plugin.")
    (base / "plugin.yaml").write_text(
        "name: demo\n"
        "version: '0.2.7'  # release\n"
        "description: >\n"
        "  Demo plugin.\n\n"
        "components:\n"
        "  skills: ./skills/\n",
        encoding="utf-8",
    )

    assert run_tool(tmp_path, "--check").returncode == 0

    bumped = run_tool(tmp_path, "--bump", "demo")
    assert bumped.returncode == 0, bumped.stderr
    assert "version: '0.2.8'  # release" in (base / "plugin.yaml").read_text(encoding="utf-8")
    assert run_tool(tmp_path, "--check").returncode == 0


def test_bump_updates_only_top_level_json_version(tmp_path: Path) -> None:
    base = seed_plugin(tmp_path, version="0.2.7")
    (base / ".codex-plugin/plugin.json").write_text(
        json.dumps(
            {
                "name": "demo",
                "metadata": {"version": "0.1.0"},
                "version": "0.2.7",
                "description": "Demo plugin.",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    bumped = run_tool(tmp_path, "--bump", "demo")
    assert bumped.returncode == 0, bumped.stderr
    codex = json.loads((base / ".codex-plugin/plugin.json").read_text(encoding="utf-8"))
    assert codex["metadata"]["version"] == "0.1.0"
    assert codex["version"] == "0.2.8"


def test_bump_patch_minor_major_and_explicit_version(tmp_path: Path) -> None:
    base = seed_plugin(tmp_path, version="0.2.7")

    patch_res = run_tool(tmp_path, "--bump", "demo")
    assert patch_res.returncode == 0, patch_res.stderr
    assert "demo: 0.2.7 -> 0.2.8" in patch_res.stdout
    assert "version: '0.2.8'" in (base / "plugin.yaml").read_text(encoding="utf-8")
    assert json.loads((base / ".claude-plugin/plugin.json").read_text(encoding="utf-8"))["version"] == "0.2.8"
    assert json.loads((base / ".codex-plugin/plugin.json").read_text(encoding="utf-8"))["version"] == "0.2.8"

    minor_res = run_tool(tmp_path, "--bump", "demo", "--part", "minor")
    assert minor_res.returncode == 0, minor_res.stderr
    assert "demo: 0.2.8 -> 0.3.0" in minor_res.stdout

    major_res = run_tool(tmp_path, "--bump", "demo", "--part", "major")
    assert major_res.returncode == 0, major_res.stderr
    assert "demo: 0.3.0 -> 1.0.0" in major_res.stdout

    explicit_res = run_tool(tmp_path, "--bump", "demo", "--version", "2.4.1")
    assert explicit_res.returncode == 0, explicit_res.stderr
    assert "demo: 1.0.0 -> 2.4.1" in explicit_res.stdout
    assert run_tool(tmp_path, "--check").returncode == 0


def test_bump_refuses_drifted_manifests_unless_explicit_version(tmp_path: Path) -> None:
    base = seed_plugin(tmp_path, version="0.2.7", claude_version="0.2.6")

    refused = run_tool(tmp_path, "--bump", "demo")
    assert refused.returncode == 1
    assert "out of sync" in refused.stderr

    # Explicit version still refuses to write if non-version fields (description) are drifted.
    (base / ".claude-plugin/plugin.json").write_text(
        json.dumps(
            {"name": "demo", "description": "Wrong.", "version": "0.2.6"},
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    still_refused = run_tool(tmp_path, "--bump", "demo", "--version", "0.2.8")
    assert still_refused.returncode == 1
    assert "version: '0.2.7'" in (base / "plugin.yaml").read_text(encoding="utf-8")

    # Fix description drift, then explicit resync succeeds.
    (base / ".claude-plugin/plugin.json").write_text(
        json.dumps(
            {"name": "demo", "description": "Demo plugin.", "version": "0.2.6"},
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    resynced = run_tool(tmp_path, "--bump", "demo", "--version", "0.2.8")
    assert resynced.returncode == 0, resynced.stderr
    assert run_tool(tmp_path, "--check").returncode == 0
