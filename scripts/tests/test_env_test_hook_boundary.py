"""The guard must catch a documented setting read through a test hook."""

from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "env_test_hook_boundary",
    ROOT / "scripts/source_guards/env_test_hook_boundary.py",
)
assert SPEC is not None and SPEC.loader is not None
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)

DISPATCHER = Path("crates/libs/cluster/controller/server/src/public_operation_dispatcher.rs")
DOC_ROW = "| `SPACE_EXAMPLE_LIMIT` | 8 | |\n"


def tree(root: Path, files: dict[str, str]) -> Path:
    (root / guard.DOCS).parent.mkdir(parents=True, exist_ok=True)
    (root / guard.DOCS).write_text(DOC_ROW)
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    return root


def test_repository_reads_documented_settings_outside_test_hooks() -> None:
    assert guard.violations(ROOT) == []


def test_dispatcher_concurrency_moved_back_to_a_test_hook_fails(tmp_path: Path) -> None:
    for relative in (guard.DOCS, DISPATCHER):
        (tmp_path / relative).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, tmp_path / relative)
    source = tmp_path / DISPATCHER
    text = source.read_text()
    anchor = "config.bounded_usize(\n"
    assert anchor in text, "the dispatcher no longer reads its concurrency this way"
    source.write_text(text.replace(anchor, "config.test_bounded_usize(\n", 1))

    failures = guard.violations(tmp_path)
    assert len(failures) == 1
    assert "SPACE_PUBLIC_OPERATION_DISPATCHER_EXECUTION_CONCURRENCY" in failures[0]
    assert "test_bounded_usize" in failures[0]


@pytest.mark.parametrize(
    "source",
    [
        'let n = config.test_positive_u32("SPACE_EXAMPLE_LIMIT", 8);',
        'const LIMIT_ENV: &str = "SPACE_EXAMPLE_LIMIT";\nlet n = test_hook_var(LIMIT_ENV);',
        'impl C { const ENV: &\'static str = "SPACE_EXAMPLE_LIMIT"; }\n'
        "let n = env::test_hook(\n    env,\n    Self::ENV,\n);",
    ],
    ids=["literal", "constant", "associated-constant"],
)
def test_documented_setting_through_a_test_hook_fails(tmp_path: Path, source: str) -> None:
    root = tree(tmp_path, {"crates/libs/example/src/lib.rs": source})
    failures = guard.violations(root)
    assert len(failures) == 1
    assert "crates/libs/example/src/lib.rs" in failures[0]
    assert "SPACE_EXAMPLE_LIMIT" in failures[0]


@pytest.mark.parametrize(
    "files",
    [
        # An undocumented variable is what test hooks are for.
        {"crates/libs/example/src/lib.rs": 'let n = test_hook_var("SPACE_EXAMPLE_TICK_SECS");'},
        # A documented variable read from the environment.
        {"crates/libs/example/src/lib.rs": 'let n = config.bounded_u32("SPACE_EXAMPLE_LIMIT", 8, 64);'},
        # Test sources may read anything.
        {"crates/libs/example/src/tests.rs": 'let n = test_hook_var("SPACE_EXAMPLE_LIMIT");'},
        # A wrapper forwarding its parameter isn't a read of any one variable.
        {"crates/libs/example/src/lib.rs": "fn test_raw(var: &str) { test_hook_var(var) }"},
        # The same constant name means another variable in another file; only
        # the file's own definition applies.
        {
            "crates/libs/example/src/a.rs": 'const ENV: &str = "SPACE_EXAMPLE_LIMIT";\n'
            "let n = std::env::var(ENV);",
            "crates/libs/example/src/b.rs": 'const ENV: &str = "SPACE_EXAMPLE_TICK_SECS";\n'
            "let n = test_hook_var(ENV);",
        },
    ],
    ids=["undocumented", "real-read", "test-source", "wrapper", "per-file-constant"],
)
def test_reads_that_are_not_violations_pass(tmp_path: Path, files: dict[str, str]) -> None:
    assert guard.violations(tree(tmp_path, files)) == []
