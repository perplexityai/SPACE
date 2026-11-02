"""The merge queue's reuse of a Pull CI run that passed the same trees.

Drives ci/scripts/pull-ci-reuse.py as the workflows run it, against a fake
`gh` that answers each API path from a table.
"""

import json
import os
import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci/scripts/pull-ci-reuse.py"
REPOSITORY = "example/space"
BASE, HEAD = "1" * 40, "2" * 40
BASE_TREE, HEAD_TREE = "a" * 40, "b" * 40
RECORD = f"pull-ci-passed-{BASE_TREE}-{HEAD_TREE}"
RUN = 77

FAKE_GH = """#!/usr/bin/env python3
import json, os, sys
answers = json.load(open(os.environ["FAKE_GH_ANSWERS"]))
path = sys.argv[2]
with open(os.environ["FAKE_GH_LOG"], "a") as log:
    log.write(path + "\\n")
if path not in answers:
    print("HTTP 502: Bad Gateway", file=sys.stderr)
    sys.exit(1)
print(json.dumps(answers[path]))
"""


def answers(**changes):
    """A passing Pull CI run of this repository that recorded BASE/HEAD's trees."""

    table = {
        f"repos/{REPOSITORY}/git/commits/{BASE}": {"tree": {"sha": BASE_TREE}},
        f"repos/{REPOSITORY}/git/commits/{HEAD}": {"tree": {"sha": HEAD_TREE}},
        f"repos/{REPOSITORY}/actions/artifacts?name={RECORD}&per_page=100": {
            "artifacts": [
                {"name": RECORD, "expired": False, "workflow_run": {"id": RUN}}
            ]
        },
        f"repos/{REPOSITORY}/actions/runs/{RUN}": {
            "path": ".github/workflows/pull-ci.yml",
            "event": "pull_request",
            "html_url": f"https://github.example/runs/{RUN}",
            "repository": {"full_name": REPOSITORY},
            "head_repository": {"full_name": REPOSITORY},
        },
        f"repos/{REPOSITORY}/actions/runs/{RUN}/jobs?filter=latest&per_page=100": {
            "jobs": [
                {"name": "Select Kind smoke", "conclusion": "success"},
                {"name": "space-pull-ci", "conclusion": "success"},
            ]
        },
    }
    for path, value in changes.items():
        key = {
            "artifacts": f"repos/{REPOSITORY}/actions/artifacts?name={RECORD}&per_page=100",
            "run": f"repos/{REPOSITORY}/actions/runs/{RUN}",
            "jobs": f"repos/{REPOSITORY}/actions/runs/{RUN}/jobs?filter=latest&per_page=100",
        }[path]
        if value is None:
            del table[key]
        else:
            table[key] = value
    return table


@pytest.fixture
def lookup(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "gh").write_text(
        FAKE_GH.replace("#!/usr/bin/env python3", f"#!{sys.executable}")
    )
    (bin_dir / "gh").chmod(0o755)

    def run(table, repository=REPOSITORY):
        (tmp_path / "answers.json").write_text(json.dumps(table))
        environment = {
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "FAKE_GH_ANSWERS": str(tmp_path / "answers.json"),
            "FAKE_GH_LOG": str(tmp_path / "gh.log"),
            "GITHUB_REPOSITORY": repository,
        }
        return subprocess.run(
            [sys.executable, "-I", str(SCRIPT), "lookup", BASE, HEAD],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

    return run


def test_a_passing_pull_ci_run_of_the_same_trees_is_reused(lookup):
    result = lookup(answers())
    assert (result.returncode, result.stdout) == (0, "true\n")
    assert f"https://github.example/runs/{RUN}" in result.stderr


def run_from(**fields):
    run = answers()[f"repos/{REPOSITORY}/actions/runs/{RUN}"]
    return {**run, **fields}


@pytest.mark.parametrize(
    "changes",
    [
        pytest.param({"artifacts": {"artifacts": []}}, id="no record"),
        pytest.param(
            {
                "artifacts": {
                    "artifacts": [
                        {"name": RECORD, "expired": True, "workflow_run": {"id": RUN}}
                    ]
                }
            },
            id="expired record",
        ),
        pytest.param(
            {"run": run_from(head_repository={"full_name": "fork/space"})}, id="fork"
        ),
        pytest.param(
            {"run": run_from(path=".github/workflows/main-ci.yml")}, id="other workflow"
        ),
        pytest.param({"run": run_from(event="merge_group")}, id="other event"),
        pytest.param(
            {"run": run_from(repository={"full_name": "other/space"})},
            id="other repository",
        ),
        pytest.param(
            {
                "artifacts": {
                    "artifacts": [
                        {
                            "name": RECORD + "0",
                            "expired": False,
                            "workflow_run": {"id": RUN},
                        }
                    ]
                }
            },
            id="another record's name",
        ),
        # Another job's success does not stand in for the required one.
        pytest.param(
            {
                "jobs": {
                    "jobs": [
                        {"name": "Select Kind smoke", "conclusion": "success"},
                        {"name": "space-pull-ci", "conclusion": "failure"},
                    ]
                }
            },
            id="latest attempt failed",
        ),
        pytest.param({"jobs": None}, id="jobs unreadable"),
        pytest.param({"artifacts": None}, id="artifacts unreadable"),
        pytest.param({"artifacts": []}, id="artifacts malformed"),
    ],
)
def test_anything_but_a_trusted_pass_runs_the_checks(lookup, changes):
    result = lookup(answers(**changes))
    assert (result.returncode, result.stdout) == (0, "false\n")


def test_an_untrusted_record_does_not_hide_a_trusted_one(lookup):
    fork_run = RUN + 1
    table = answers(
        artifacts={
            "artifacts": [
                {"name": RECORD, "expired": False, "workflow_run": {"id": fork_run}},
                {"name": RECORD, "expired": False, "workflow_run": {"id": RUN}},
            ]
        }
    )
    table[f"repos/{REPOSITORY}/actions/runs/{fork_run}"] = run_from(
        head_repository={"full_name": "fork/space"}
    )
    result = lookup(table)
    assert (result.returncode, result.stdout) == (0, "true\n")
    assert f"https://github.example/runs/{RUN}" in result.stderr


def test_lookup_needs_the_repository(lookup):
    assert lookup(answers(), repository="").returncode == 2


def test_name_is_the_synthetic_merges_base_and_merge_trees(tmp_path):
    def git(*args):
        return subprocess.check_output(
            ["git", "-C", str(tmp_path), *args], text=True
        ).strip()

    git("init", "-q", "-b", "main")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "test")
    git("commit", "-q", "--allow-empty", "-m", "base")
    git("checkout", "-q", "-b", "pr")
    (tmp_path / "change").write_text("pr\n")
    git("add", "change")
    git("commit", "-q", "-m", "pr")
    git("checkout", "-q", "main")
    (tmp_path / "landed").write_text("main\n")
    git("add", "landed")
    git("commit", "-q", "-m", "main moved")
    git("merge", "-q", "--no-ff", "--no-edit", "pr")
    expected = f"pull-ci-passed-{git('rev-parse', 'HEAD^1^{tree}')}-{git('rev-parse', 'HEAD^{tree}')}"
    name = subprocess.check_output(
        [sys.executable, "-I", str(SCRIPT), "name"], cwd=tmp_path, text=True
    ).strip()
    assert name == expected
