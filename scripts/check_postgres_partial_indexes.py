#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "pglast==7.21",
# ]
# ///

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pglast import ast, parse_sql, scan  # type: ignore[import-untyped]
from pglast.parser import ParseError  # type: ignore[import-untyped]


def comment_only_change(before: str, after: str) -> bool:
    # SQLx interprets this leading comment as an execution directive.
    if before.startswith("-- no-transaction") != after.startswith("-- no-transaction"):
        return False

    def executable_tokens(source: str) -> list[tuple[str, str]]:
        return [
            (token.name, source[token.start : token.end + 1])
            for token in scan(source)
            if token.name not in {"SQL_COMMENT", "C_COMMENT"}
        ]

    return executable_tokens(before) == executable_tokens(after)


def partial_index_names(sql: str) -> list[str]:
    return [
        statement.stmt.idxname
        for statement in parse_sql(sql)
        if isinstance(statement.stmt, ast.IndexStmt)
        and statement.stmt.whereClause is not None
    ]


def check_file(path: Path) -> list[str]:
    try:
        return partial_index_names(path.read_text(encoding="utf-8"))
    except ParseError as error:
        raise ValueError(
            f"could not parse PostgreSQL migration {path}: {error}"
        ) from error


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reject partial indexes in PostgreSQL migrations."
    )
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument(
        "--comment-only",
        action="store_true",
        help=("Compare one file with its previous contents on stdin; exit 0 only "
              "if SQL tokens and transaction mode are unchanged."),
    )
    args = parser.parse_args()
    if args.comment_only:
        if len(args.paths) != 1:
            parser.error("--comment-only requires exactly one path")
        return int(not comment_only_change(
            sys.stdin.read(), args.paths[0].read_text(encoding="utf-8")
        ))

    failed = False
    for path in args.paths:
        try:
            index_names = check_file(path)
        except ValueError as error:
            parser.error(str(error))

        for index_name in index_names:
            print(
                f"partial Postgres indexes are not allowed: {path} creates "
                f"{index_name} with a WHERE predicate. Prefer a non-partial index "
                "or redesign the access path. This applies to up and down migrations.",
                file=sys.stderr,
            )
            failed = True

    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
