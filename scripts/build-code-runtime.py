#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["e2b==2.21.1"]
# ///
"""Build SPACE's code-runtime template through the ECW-compatible template API.

This mirrors templates/firecracker/recipes/code-runtime.toml using the public
template-build SDK surface. It starts from the SPACE seed template and publishes
the result as code-runtime by default.

The seed recipe itself is not represented here: SPACE's ECW template-build
translator accepts fromTemplate builds, while seed is a SPACE scratch build.
Build seed with space-builder first, then use this script to build the
code-runtime layer on top.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import tomllib
from pathlib import Path

from e2b import Sandbox, Template, default_build_logger
from e2b.api.client.client import AuthenticatedClient

DEFAULT_API_URL = None
DEFAULT_ALIAS = "code-runtime"
DEFAULT_BASE_TEMPLATE = "seed"
DEFAULT_CPU_COUNT = 2
DEFAULT_MEMORY_MB = 2048

VALIDATE_PYTHON = r"""
set -eo pipefail
python3 - <<'PY'
import sys

if sys.version_info < (3, 12):
    raise SystemExit(f"Python {sys.version.split()[0]} is too old")
PY
if ! command -v python >/dev/null 2>&1; then
  ln -sf "$(command -v python3)" /usr/local/bin/python
fi
python3 --version
python --version
"""

CONFIGURE_USER = r"""
set -eo pipefail
id -u user 2>/dev/null || useradd -m -s /bin/bash user
echo 'user ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/user
chmod 440 /etc/sudoers.d/user
mkdir -p /home/user/.ssh /home/user/workspace
chown -R user:user /home/user
chmod 700 /home/user/.ssh
"""

ASSERT_USER_HOME_OWNERSHIP = r"""
set -eo pipefail
owner="$(stat -c '%U:%G' /home/user)"
if [ "$owner" != "user:user" ]; then
  echo "expected /home/user owner user:user, got $owner" >&2
  exit 1
fi
"""

FIX_PING = r"""
if [ -f /usr/bin/ping ]; then
  setcap cap_net_raw+ep /usr/bin/ping 2>/dev/null || chmod u+s /usr/bin/ping
fi
"""

CLEAN_UP = r"""
apt-get clean
rm -rf /tmp/* /var/tmp/*
"""


def log(message: str) -> None:
    print(f"[code-runtime] {message}", flush=True)


def parse_tags(raw: str) -> list[str]:
    tags = [tag.strip() for tag in raw.split(",") if tag.strip()]
    return tags or ["latest"]


def build_template_definition(base_template: str) -> Template:
    recipe_path = (
        Path(__file__).resolve().parents[1]
        / "templates/firecracker/recipes/code-runtime.toml"
    )
    with recipe_path.open("rb") as recipe_file:
        recipe = tomllib.load(recipe_file)
    commands = {
        step["name"]: step["command"]
        for step in recipe["steps"]
        if step["type"] == "run"
    }
    template = (
        Template(file_context_path=recipe_path.parent)
        .from_template(base_template)
        .run_cmd(commands["Install system packages"], user="root")
        .run_cmd(VALIDATE_PYTHON, user="root")
        .run_cmd(commands["Install Node.js v20"], user="root")
        .run_cmd(commands["Install GitHub CLI"], user="root")
        .run_cmd(commands["Install uv package manager"], user="root")
    )
    for step in recipe["steps"]:
        if step["type"] == "copy":
            template = template.copy(step["source"], step["dest"], user="root")
    return (
        template.run_cmd(
            commands["Install code interpreter packages (Jupyter, data science, NLP)"],
            user="root",
        )
        .run_cmd(CONFIGURE_USER, user="root")
        .run_cmd(ASSERT_USER_HOME_OWNERSHIP, user="root")
        .run_cmd(FIX_PING, user="root")
        .run_cmd(CLEAN_UP, user="root")
        # Mirror upstream e2b code-interpreter-v1's tail so that downstream
        # templates inherit `user` as the default identity for run_cmd steps
        # without an explicit
        # `user=` kwarg. Without this, code-runtime defaults to root, which
        # silently diverges from E2B and causes things like ~/.claude and
        # ~/.codex to get created root-owned on first invocation.
        .set_user("user")
        .set_workdir("/home/user")
    )


def install_host_header_override(host_header: str | None) -> None:
    if not host_header:
        return

    original_get_httpx_client = AuthenticatedClient.get_httpx_client

    def get_httpx_client_with_host(self: AuthenticatedClient):
        client = original_get_httpx_client(self)
        client.headers["Host"] = host_header
        return client

    AuthenticatedClient.get_httpx_client = get_httpx_client_with_host


def build_template(
    *,
    api_key: str,
    domain: str,
    api_url: str | None,
    alias: str,
    tags: list[str],
    base_template: str,
    cpu_count: int,
    memory_mb: int,
    skip_cache: bool,
    api_host_header: str | None,
) -> object:
    os.environ["E2B_API_KEY"] = api_key
    os.environ["E2B_DOMAIN"] = domain
    if api_url:
        os.environ["E2B_API_URL"] = api_url
    install_host_header_override(api_host_header)

    log(
        f"building alias={alias} domain={domain} api_url={api_url or '<sdk default>'} "
        f"base={base_template} cpu={cpu_count} memory_mb={memory_mb} tags={tags}"
    )
    started = time.monotonic()
    result = Template.build(
        build_template_definition(base_template),
        alias=alias,
        tags=tags,
        cpu_count=cpu_count,
        memory_mb=memory_mb,
        skip_cache=skip_cache,
        api_key=api_key,
        domain=domain,
        api_url=api_url,
        on_build_logs=default_build_logger(),
    )
    log(f"Template.build succeeded in {time.monotonic() - started:.1f}s: {result!r}")
    return result


def verify_sandbox(api_key: str, domain: str, api_url: str | None, alias: str) -> None:
    log(f"creating sandbox from built template {alias!r}")
    sandbox = Sandbox.create(
        api_key=api_key,
        domain=domain,
        api_url=api_url,
        template=alias,
        timeout=60,
        metadata={"source": "build-code-runtime"},
    )
    try:
        result = sandbox.commands.run(
            "python --version && node --version && gh --version | head -1 && "
            "uv --version && python - <<'PY'\n"
            "import imageio, IPython, jsonschema, jupyterlab, nltk, numpy, pandas, PIL, scipy\n"
            "print('python imports ok')\n"
            "PY"
        )
        if result.exit_code != 0:
            raise SystemExit(f"sandbox verification failed: {result!r}")
        log(f"sandbox verification passed: sandbox_id={sandbox.sandbox_id}")
    finally:
        log(f"killing sandbox {sandbox.sandbox_id}")
        sandbox.kill()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--domain",
        default=os.environ.get("E2B_DOMAIN"),
        required=not os.environ.get("E2B_DOMAIN"),
        help="ECW API domain.",
    )
    parser.add_argument(
        "--api-url",
        default=os.environ.get("E2B_API_URL", DEFAULT_API_URL),
        help="Override the SDK API base URL, useful for localhost.",
    )
    parser.add_argument("--api-key", default=os.environ.get("E2B_API_KEY"))
    parser.add_argument(
        "--api-host-header",
        default=None,
        help="Override HTTP Host for port-forwarded API calls.",
    )
    parser.add_argument("--alias", default=DEFAULT_ALIAS)
    parser.add_argument("--base-template", default=DEFAULT_BASE_TEMPLATE)
    parser.add_argument("--tags", default="latest")
    parser.add_argument("--cpu-count", type=int, default=DEFAULT_CPU_COUNT)
    parser.add_argument("--memory-mb", type=int, default=DEFAULT_MEMORY_MB)
    parser.add_argument("--skip-cache", action="store_true")
    parser.add_argument(
        "--skip-sandbox-check",
        action="store_true",
        help="Only build the template; do not create a sandbox from it.",
    )
    args = parser.parse_args(argv)

    if not args.api_key:
        raise SystemExit(
            "E2B_API_KEY is required by the SDK; pass --api-key or set the env var"
        )

    build_template(
        api_key=args.api_key,
        domain=args.domain,
        api_url=args.api_url,
        alias=args.alias,
        tags=parse_tags(args.tags),
        base_template=args.base_template,
        cpu_count=args.cpu_count,
        memory_mb=args.memory_mb,
        skip_cache=args.skip_cache,
        api_host_header=args.api_host_header,
    )

    if not args.skip_sandbox_check:
        verify_sandbox(args.api_key, args.domain, args.api_url, args.alias)

    log("done")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
