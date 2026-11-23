"""Postgres helpers for the firewall rules schema tests: a server to create
databases on, the db/postgres migrations applied as SQLx applies them, and a
new database migrated to just before one of them."""

from __future__ import annotations

import os
import unittest
import uuid
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
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


def fresh_database(test: unittest.TestCase, *, before: int) -> str:
    """Create a database for `test`, dropped when it ends, with every up
    migration older than version `before` applied, and return its URL."""
    admin_url = database_url()
    database = f"firewall_rules_{uuid.uuid4().hex}"
    try:
        admin = psycopg.connect(admin_url, autocommit=True)
    except psycopg.OperationalError as error:
        unavailable_dependency("postgres", str(error))
    with admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))

    def drop() -> None:
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(database))
            )

    test.addCleanup(drop)
    url = make_conninfo(admin_url, dbname=database)
    apply_migrations(
        url, [path for path in migration_files() if int(path.name.split("_", 1)[0]) < before]
    )
    return url
