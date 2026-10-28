#!/usr/bin/env python3
"""Run a command against a disposable, instrumented native PostgreSQL 17."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pg-bin",
        type=Path,
        default=Path("/usr/lib/postgresql/17/bin"),
        help="directory containing PostgreSQL 17 postgres and initdb",
    )
    parser.add_argument(
        "--setup-bin",
        type=Path,
        required=True,
        help="built space-nextest-postgres-setup binary",
    )
    parser.add_argument(
        "--postgres-cpus",
        help="optional taskset CPU list; reserve different CPUs for the child command",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command[:1] == ["--"]:
        args.command = args.command[1:]
    if not args.command:
        parser.error("provide a child command after --")
    args.pg_bin = args.pg_bin.resolve()
    args.setup_bin = args.setup_bin.resolve(strict=True)
    if os.geteuid() == 0:
        parser.error("run as an unprivileged user, not root")
    return args


def unused_port() -> int:
    # PostgreSQL owns the eventual bind. A competing bind fails startup rather
    # than causing this launcher to adopt or mutate an existing server.
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def wait_ready(server: subprocess.Popen[bytes], env: dict[str, str]) -> None:
    deadline = time.monotonic() + 30
    while server.poll() is None:
        result = subprocess.run(
            ["psql", "-X", "-At", "-c", "SELECT 1"],
            env=env,
            capture_output=True,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip() == b"1":
            return
        if time.monotonic() >= deadline:
            raise RuntimeError("PostgreSQL did not become ready within 30 seconds")
        # Readiness polling, not a workload delay.
        time.sleep(0.1)
    raise RuntimeError(f"PostgreSQL exited during startup: {server.returncode}")


def stop(server: subprocess.Popen[bytes]) -> None:
    if server.poll() is not None:
        return
    # This is our unreaped child, never a PID read from an untrusted pidfile.
    server.send_signal(signal.SIGINT)
    try:
        server.wait(timeout=30)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait()
        raise RuntimeError("PostgreSQL required forced shutdown")


def run(args: argparse.Namespace, directory: Path) -> int:
    version = subprocess.check_output(
        [str(args.pg_bin / "postgres"), "--version"], text=True
    ).strip()
    if not version.startswith("postgres (PostgreSQL) 17."):
        raise RuntimeError(f"PostgreSQL 17 is required, found {version}")
    password = secrets.token_hex(32)
    password_file = directory / "password"
    password_file.write_text(password + "\n")
    password_file.chmod(0o600)
    data = directory / "data"
    subprocess.run(
        [
            str(args.pg_bin / "initdb"),
            "-D", str(data),
            "--username=postgres",
            "--auth-local=trust",
            "--auth-host=scram-sha-256",
            "--encoding=UTF8",
            "--locale=C",
            f"--pwfile={password_file}",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    password_file.unlink()
    port = unused_port()
    settings = {
        "listen_addresses": "127.0.0.1",
        "unix_socket_directories": str(directory),
        "max_connections": "200",
        "shared_buffers": "256MB",
        "shared_preload_libraries": "pg_stat_statements",
        "pg_stat_statements.track": "all",
        "pg_stat_statements.track_planning": "on",
        "track_io_timing": "on",
        # Keep durability enabled: disabling it changes the WAL/latency
        # workload. Never share this cluster with another benchmark or test.
        "fsync": "on",
        "synchronous_commit": "on",
        "full_page_writes": "on",
    }
    command = [str(args.pg_bin / "postgres"), "-D", str(data), "-p", str(port)]
    for name, value in settings.items():
        command.extend(["-c", f"{name}={value}"])
    if args.postgres_cpus:
        command = ["taskset", "-c", args.postgres_cpus, *command]
    env = {
        **os.environ,
        "PGHOST": "127.0.0.1",
        "PGPORT": str(port),
        "PGUSER": "postgres",
        "PGPASSWORD": password,
        "PGDATABASE": "postgres",
        "PGCONNECT_TIMEOUT": "2",
        "SPACE_TEST_POSTGRES_ADMIN_URL":
            f"postgres://postgres:{password}@127.0.0.1:{port}/postgres?sslmode=disable",
    }
    env.pop("SPACE_TEST_DEPS_OPTIONAL", None)
    log = directory / "postgres.log"
    with log.open("wb") as output:
        server = subprocess.Popen(command, stdout=output, stderr=output)
        try:
            wait_ready(server, env)
            subprocess.run(
                ["psql", "-X", "-v", "ON_ERROR_STOP=1", "-c",
                 "CREATE EXTENSION pg_stat_statements"],
                env=env,
                check=True,
                stdout=subprocess.DEVNULL,
            )
            subprocess.run(
                [str(args.setup_bin), env["SPACE_TEST_POSTGRES_ADMIN_URL"]],
                env=env,
                check=True,
            )
            env["SPACE_PUBLISH_POSTGRES_PID"] = str(server.pid)
            env["SPACE_PUBLISH_POSTGRES_DATA_DIR"] = str(data)
            print(
                f"{version}: migrated disposable cluster; "
                "SPACE_TEST_POSTGRES_ADMIN_URL exported to child",
                file=sys.stderr,
                flush=True,
            )
            return subprocess.run(args.command, env=env, check=False).returncode
        except (Exception, KeyboardInterrupt):
            # Server settings do not log statement text or bound parameters.
            print(log.read_text(), file=sys.stderr)
            raise
        finally:
            stop(server)


def main() -> int:
    args = arguments()
    with tempfile.TemporaryDirectory(prefix="space-publish-pg17-") as temporary:
        return run(args, Path(temporary))


if __name__ == "__main__":
    sys.exit(main())
