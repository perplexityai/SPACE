#!/usr/bin/env python3
"""Let the merge queue reuse a Pull CI run that passed the same trees.

A merge-queue entry's checks are Pull CI's rust_static and common jobs, run
on the merge group's commit against its base. When a Pull CI run already
passed for a synthetic merge with the same tree, against a base with the same
tree, those jobs would rerun on identical inputs. Pull CI records each pass as
an artifact named after both trees (`name`); the merge queue looks for one
(`lookup`) and skips its copies of the jobs when it finds one.

`lookup` trusts a record only from a Pull CI run of this repository's own
branches, never a fork's, whose latest attempt passed the required
space-pull-ci job. Anything else, including an API failure, answers `false`,
and the merge queue runs its checks as before.

Usage:
  pull-ci-reuse.py name             # in Pull CI, from the synthetic merge
  pull-ci-reuse.py lookup BASE HEAD # in the merge queue; prints true or false
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

PREFIX = "pull-ci-passed"
WORKFLOW = ".github/workflows/pull-ci.yml"
REQUIRED_JOB = "space-pull-ci"


def record_name(base_tree: str, head_tree: str) -> str:
    return f"{PREFIX}-{base_tree}-{head_tree}"


def git_tree(revision: str) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "--verify", f"{revision}^{{tree}}"], text=True
    ).strip()


def gh(path: str) -> dict:
    # A short timeout: a slow API only delays the entry's checks, which then run.
    output = subprocess.run(
        ["gh", "api", path], capture_output=True, text=True, timeout=15, check=True
    ).stdout
    return json.loads(output)


def lookup(repository: str, base: str, head: str) -> tuple[bool, str]:
    """Whether a trusted Pull CI pass covers these commits' trees, and why."""

    base_tree = gh(f"repos/{repository}/git/commits/{base}")["tree"]["sha"]
    head_tree = gh(f"repos/{repository}/git/commits/{head}")["tree"]["sha"]
    name = record_name(base_tree, head_tree)
    listed = gh(f"repos/{repository}/actions/artifacts?name={name}&per_page=100")
    for artifact in listed.get("artifacts", []):
        if artifact.get("name") != name or artifact.get("expired"):
            continue
        run_id = (artifact.get("workflow_run") or {}).get("id")
        if not isinstance(run_id, int):
            continue
        run = gh(f"repos/{repository}/actions/runs/{run_id}")
        if (
            run.get("path") != WORKFLOW
            or run.get("event") != "pull_request"
            or (run.get("repository") or {}).get("full_name") != repository
            or (run.get("head_repository") or {}).get("full_name") != repository
        ):
            continue
        jobs = gh(
            f"repos/{repository}/actions/runs/{run_id}/jobs?filter=latest&per_page=100"
        )
        if any(
            job.get("name") == REQUIRED_JOB and job.get("conclusion") == "success"
            for job in jobs.get("jobs", [])
        ):
            return True, f"Pull CI run {run.get('html_url', run_id)} passed these trees"
    return False, f"no passing Pull CI run of this repository recorded {name}"


def main(argv: list[str]) -> int:
    if argv[1:] == ["name"]:
        print(record_name(git_tree("HEAD^1"), git_tree("HEAD")))
        return 0
    if len(argv) == 4 and argv[1] == "lookup":
        repository = os.environ.get("GITHUB_REPOSITORY", "")
        if not repository:
            print("GITHUB_REPOSITORY is required", file=sys.stderr)
            return 2
        try:
            reused, reason = lookup(repository, argv[2], argv[3])
        except (
            subprocess.SubprocessError,
            OSError,
            ValueError,
            AttributeError,
            KeyError,
            TypeError,
        ) as error:
            # Reuse only saves time: any doubt runs the checks.
            reused, reason = (
                False,
                f"could not check for a reusable run ({type(error).__name__})",
            )
        print(reason, file=sys.stderr)
        print("true" if reused else "false")
        return 0
    print("usage:" + __doc__.split("Usage:")[1].rstrip(), file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
