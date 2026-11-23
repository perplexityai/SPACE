"""Postgres helpers for the firewall rules schema tests: a server to create
databases on, and the db/postgres migrations applied as SQLx applies them."""

from __future__ import annotations

import os
from pathlib import Path

import psycopg
from required_test_deps import unavailable_dependency

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "db" / "postgres"


def database_url() -> str:
    # ci/scripts/run-python-tests.sh starts one Postgres for the Python tests
    # and names it with PRUNE_CLUSTER_ARTIFACTS_TEST_DATABASE_URL. The tests
    # create a database of their own on it and drop it afterwards.
    for name in (
        "FIREWALL_RULES_TEST_DATABASE_URL",
        "PRUNE_CLUSTER_ARTIFACTS_TEST_DATABASE_URL",
    ):
        url = os.environ.get(name)
        if url:
            return url
    unavailable_dependency(
        "postgres",
        "set FIREWALL_RULES_TEST_DATABASE_URL to a Postgres where the tests may "
        "create a database, or run them through ci/scripts/run-python-tests.sh",
    )


def migration_files() -> list[Path]:
    files = sorted(
        MIGRATIONS_DIR.glob("*.up.sql"),
        key=lambda path: int(path.name.split("_", 1)[0]),
    )
    if not files:
        raise AssertionError(f"no Postgres migrations under {MIGRATIONS_DIR}")
    return files


def apply_migrations(url: str, files: list[Path] | None = None) -> None:
    # As SQLx does: each file in its own transaction, unless its first line
    # opts out. Every up migration unless `files` names others.
    with psycopg.connect(url, autocommit=True) as conn:
        for path in migration_files() if files is None else files:
            text = path.read_text()
            if text.startswith("-- no-transaction"):
                conn.execute(text)
            else:
                with conn.transaction():
                    conn.execute(text)
