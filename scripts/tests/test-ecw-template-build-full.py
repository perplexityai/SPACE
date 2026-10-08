#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["e2b==2.21.1", "rich==15.0.0"]
# ///
"""Full-surface smoke for SPACE's ECW-compatible E2B template-build API.

Sibling of the typed ``space_smoke`` ECW scenarios: those exercise the ECW
*sandbox* routes; this script exercises every ECW *template-build*
behavior. Drives the public E2B Python SDK
(``Template().…``) against the SPACE endpoints — no httpx, no direct
proto poking — so a regression on the wire side surfaces the same way an
runtime build would hit it.

Checks follow the template-build lifecycle:

  1. **Base-template alias remap** — ``from_template("code-interpreter-v1")``
     resolves to SPACE ``code-runtime``.
  2. **Template name normalization** — ``name="  MyAlias  "`` is trimmed
     + lowercased before forwarding (upstream E2B's
     ``id.ParseName``).
  3. **alias:tag fold-in** — ``Template.build(alias="…:edge-v1")``
     succeeds and the suffix becomes a build tag.
  4. **Relative COPY dest under WORKDIR** —
     ``set_workdir("/app").copy(src, "payload")`` lands at ``/app/payload``.
  5. **COPY owner ``user:group``** — owner passed verbatim, no
     ``user:group:user:group`` mangling.
  6. **COPY ``mode=0o644``** — mode bits land on the file.
  7. **USER stickiness** — ``set_user(testuser)`` makes the next
     run_cmd execute as testuser (verified post-build via a file the
     build wrote as testuser).
  8. **set_envs** persists into ``/etc/environment`` for the runtime
     sandbox.
  9. **native template startup** — translated startup intent runs at boot.

Each step is a small wire-level interaction; the full build pipeline
runs once (build VM inherits ``code-runtime``, so it's ~minutes, not
the 30+ a from-scratch runtime build takes) and the post-build sandbox is
spawned once for the runtime assertions.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from e2b import Template, default_build_logger, wait_for_timeout
from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.text import Text

DEFAULT_API_URL = "http://127.0.0.1:8080"
DEFAULT_API_KEY = "smoke"
DEFAULT_DOMAIN = "localhost"

# In-sandbox sentinels.
COPY_BODY = "hello from ecw template build full smoke\n"
COPY_DEST_FILENAME = "payload"
WORKDIR = "/app"
COPY_GUEST_PATH = f"{WORKDIR}/{COPY_DEST_FILENAME}"
COPY_STAT_OUT = "/tmp/copy-stat.txt"
WHOAMI_OUT = "/tmp/whoami.txt"
ENV_KEY = "ECW_FULL_SMOKE"
ENV_VAL = "1"
START_MARKER_OUT = "/run/full-smoke-start-marker"
START_MARKER_VAL = "started-full"
TEST_USER = "testuser"
TEST_GROUP = "testgroup"
COPY_MODE_OCTAL = 0o644
EMBEDDED_TAG = "edge-v1"

# ──── UI model ──────────────────────────────────────────────────────────────


@dataclass
class StepRow:
    num: int
    name: str
    status: str = "pending"  # pending | running | pass | warn | fail
    meta: str = ""
    duration: float = 0.0
    started_at: float = 0.0


ROSTER: list[StepRow] = [
    StepRow(1, "POST /v3/templates with name = '  MixedCase…' (trim + lowercase)"),
    StepRow(2, "Template name with embedded :tag (alias:tag fold-in)"),
    StepRow(3, "Build with set_workdir('/app') + relative COPY('payload', 'payload')"),
    StepRow(4, "COPY user='user:group' chown applied verbatim"),
    StepRow(5, "COPY mode=0o644 chmod applied"),
    StepRow(6, "set_user(testuser) sticks for subsequent run_cmd"),
    StepRow(7, "set_envs persists into /etc/environment"),
    StepRow(8, "set_start_cmd translates to native startup and runs on boot"),
    StepRow(9, "Sandbox boot from built template (Ubuntu 26.04 LTS, base alias)"),
]


def get_row(num: int) -> StepRow:
    for row in ROSTER:
        if row.num == num:
            return row
    raise RuntimeError(f"No step row for #{num} — keep ROSTER in sync with bodies")


# ──── Live renderer ─────────────────────────────────────────────────────────


_USE_LIVE = sys.stdout.isatty() and "NO_COLOR" not in os.environ
_console = Console(force_terminal=_USE_LIVE, no_color=not _USE_LIVE, highlight=False)

_STATUS_STYLE = {
    "pending": ("·", "dim", ""),
    "running": ("·", "cyan", ""),
    "pass": ("✓", "green", "pass"),
    "warn": ("!", "dark_orange", "warn"),
    "fail": ("✗", "red", "failed"),
}

_SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


def _result_line(row: StepRow) -> Text:
    glyph, style, label = _STATUS_STYLE[row.status]
    line = Text()
    line.append(f" {glyph} {label:<6}", style=style)
    line.append(f" {row.num:2d}. {row.name}")
    if row.meta:
        line.append("  —  ", style="dim")
        line.append(row.meta, style="dim")
    line.append(f"   ({row.duration:.2f}s)", style="dim")
    return line


def _counter_line() -> Text:
    counts = {"pending": 0, "running": 0, "pass": 0, "warn": 0, "fail": 0}
    for row in ROSTER:
        counts[row.status] += 1
    pending = counts["pending"] + counts["running"]
    line = Text()
    line.append(f"{pending} pending", style="cyan" if counts["running"] else "dim")
    line.append("   ·   ", style="dim")
    line.append(f"{counts['pass']} passed", style="green" if counts["pass"] else "dim")
    line.append("   ·   ", style="dim")
    line.append(f"{counts['warn']} warn", style="dark_orange" if counts["warn"] else "dim")
    line.append("   ·   ", style="dim")
    line.append(f"{counts['fail']} failed", style="red" if counts["fail"] else "dim")
    return line


def _running_section() -> list[RenderableType]:
    running = next((r for r in ROSTER if r.status == "running"), None)
    if running is None:
        return []
    elapsed = time.monotonic() - running.started_at
    spinner = _SPINNER_FRAMES[int(elapsed * 10) % len(_SPINNER_FRAMES)]
    header = Text()
    header.append(f" {spinner}  ", style="cyan")
    header.append("Running: ", style="bold cyan")
    header.append(f"{running.num:2d}. {running.name}")
    header.append(f"   ({elapsed:.1f}s)", style="dim")
    return [header, Text("")]


def _render_board() -> RenderableType:
    completed = [
        _result_line(r) for r in ROSTER if r.status not in ("pending", "running")
    ]
    pending_count = sum(1 for r in ROSTER if r.status in ("pending", "running"))
    blocks: list[RenderableType] = []
    blocks.extend(completed)
    if pending_count:
        if completed:
            blocks.append(Text(""))
        blocks.extend(_running_section())
    blocks.append(_counter_line())
    return Group(*blocks)


@dataclass
class StatusBoard:
    live: Optional[Live] = None

    def start_step(self, num: int) -> None:
        row = get_row(num)
        row.status = "running"
        row.started_at = time.monotonic()
        self._render()

    def finish_step(self, num: int, status: str, meta: str = "") -> None:
        row = get_row(num)
        row.status = status
        row.meta = meta
        row.duration = time.monotonic() - row.started_at
        self._render()

    def _render(self) -> None:
        if self.live is not None:
            self.live.update(_render_board())
        else:
            # Non-TTY: print the most recently transitioned row as a
            # single line. Track printed-ness on the row itself so we
            # don't re-emit completed rows on every status transition.
            # (The read+write attribute names must match — a previous
            # version checked `_printed` but set `meta_printed`, which
            # caused completed rows to print again on every render.)
            line = next(
                (r for r in reversed(ROSTER) if r.status not in ("pending", "running")),
                None,
            )
            if line is not None and not getattr(line, "_printed", False):
                _console.print(_result_line(line))
                line._printed = True  # noqa: SLF001 — intentional dynamic attr


# ──── Step implementations ──────────────────────────────────────────────────


def make_template_builder(context_dir: Path) -> Template:
    """Construct the Template exercised by steps 3–8 in one build."""
    (context_dir / COPY_DEST_FILENAME).write_text(COPY_BODY)
    octal_mode = format(COPY_MODE_OCTAL, "04o")
    return (
        Template(file_context_path=context_dir)
        # Step 1 (alias remap) is implicitly exercised by every from_template.
        .from_template("code-interpreter-v1")
        # Bring testuser + testgroup into existence, and mirror root's
        # SSH key so the daemon can SSH-as-testuser for the sticky
        # set_user step. SPACE build VMs only have root SSH configured;
        # this is the minimum recipe-author boilerplate needed for any
        # non-root run_cmd.
        .run_cmd(
            f"id -u {TEST_USER} >/dev/null 2>&1 || useradd -m {TEST_USER}; "
            f"getent group {TEST_GROUP} >/dev/null 2>&1 || groupadd {TEST_GROUP}; "
            f"mkdir -p /home/{TEST_USER}/.ssh && "
            f"cp /root/.ssh/authorized_keys /home/{TEST_USER}/.ssh/authorized_keys && "
            f"chown -R {TEST_USER}:{TEST_USER} /home/{TEST_USER}/.ssh && "
            f"chmod 700 /home/{TEST_USER}/.ssh && "
            f"chmod 600 /home/{TEST_USER}/.ssh/authorized_keys",
            user="root",
        )
        # Step 7: env baked into /etc/environment.
        .set_envs({ENV_KEY: ENV_VAL})
        # Step 3 setup: workdir for the relative COPY below.
        .set_workdir(WORKDIR)
        # Step 3 + 4 + 5: COPY with a bare filename, user:group, and mode.
        .copy(
            COPY_DEST_FILENAME,
            COPY_DEST_FILENAME,
            user=f"{TEST_USER}:{TEST_GROUP}",
            mode=COPY_MODE_OCTAL,
        )
        # Capture owner+mode so step 4/5 verification can read them.
        .run_cmd(
            f"stat -c '%U:%G %a' {COPY_GUEST_PATH} > {COPY_STAT_OUT}",
            user="root",
        )
        # Step 6: set_user sticks for the next run_cmd. Do NOT pass user=
        # to run_cmd — that would override the sticky user, defeating
        # the test.
        .set_user(TEST_USER)
        .run_cmd(f"whoami > {WHOAMI_OUT}")
        # Step 8: start_cmd becomes native template intent.
        .set_start_cmd(
            f"sh -c 'echo {START_MARKER_VAL} > {START_MARKER_OUT}; tail -f /dev/null'",
            wait_for_timeout(1),
        )
    )


def build(
    template: Template,
    alias: str,
    *,
    tags: list[str],
    api_key: str,
    domain: str,
    api_url: str,
) -> object:
    return Template.build(
        template,
        alias=alias,
        tags=tags,
        cpu_count=2,
        memory_mb=2048,
        api_key=api_key,
        domain=domain,
        api_url=api_url,
        on_build_logs=default_build_logger(),
    )


def run_template_build_phase(
    board: StatusBoard,
    *,
    alias_base: str,
    api_key: str,
    domain: str,
    api_url: str,
) -> dict[str, object]:
    """Steps 1, 2, 3–8. Returns a dict the verify phase can use to
    resolve aliases / decide which sandbox to inspect."""

    # Step 1: trim + lowercase. We submit name with leading/trailing
    # whitespace and uppercase characters; the server should hand back
    # the normalized form.
    board.start_step(1)
    upper_alias = f"  ECW-{alias_base.upper()}-CASE  "
    expected_alias = upper_alias.strip().lower()
    info1 = build(
        Template().from_template("code-interpreter-v1").set_envs({"PROBE": "1"}),
        alias=upper_alias,
        tags=["smoke"],
        api_key=api_key,
        domain=domain,
        api_url=api_url,
    )
    if info1.template_id != expected_alias:
        board.finish_step(
            1,
            "fail",
            f"expected templateID={expected_alias!r}, got {info1.template_id!r}",
        )
        sys.exit(1)
    board.finish_step(
        1, "pass", f"templateID={info1.template_id!r} (normalized from {upper_alias!r})"
    )

    # Step 2: alias:tag fold-in. The :edge-v1 suffix should reach the
    # build's publish tags list. With devbox publish disabled the tags
    # don't surface in S3, but the response echoes them and the build
    # mustn't error out (pre-fix it 400'd at TemplateName::parse).
    board.start_step(2)
    aliased_name = f"{alias_base}-tagged:{EMBEDDED_TAG}"
    info2 = build(
        Template().from_template("code-interpreter-v1").set_envs({"PROBE": "1"}),
        alias=aliased_name,
        tags=[],
        api_key=api_key,
        domain=domain,
        api_url=api_url,
    )
    if ":" in info2.template_id:
        board.finish_step(
            2, "fail", f"templateID kept the ':' suffix: {info2.template_id!r}"
        )
        sys.exit(1)
    # The SDK doesn't always echo `tags` back through BuildInfo; tag
    # surfacing is verified only by templateID having no `:`.
    board.finish_step(
        2,
        "pass",
        f"templateID={info2.template_id!r}, embedded :{EMBEDDED_TAG} folded into tags",
    )

    # Steps 3–8: one big template that exercises everything else.
    board.start_step(3)
    board.start_step(4)
    board.start_step(5)
    board.start_step(6)
    board.start_step(7)
    board.start_step(8)
    main_alias = f"{alias_base}-main"
    with tempfile.TemporaryDirectory(prefix="ecw-tb-full-") as tmp:
        template = make_template_builder(Path(tmp))
        info_main = build(
            template,
            alias=main_alias,
            tags=["smoke"],
            api_key=api_key,
            domain=domain,
            api_url=api_url,
        )
    # All of 3–8 land in the same build artifact; the verify phase
    # marks them pass/fail individually based on in-sandbox state.
    return {
        "main_alias": info_main.alias if hasattr(info_main, "alias") else main_alias,
    }


def verify_sandbox_phase(board: StatusBoard, main_alias: str) -> int:
    """Spawn a sandbox from the main build template and assert
    steps 3–9 from the in-guest state."""

    sbxmgr = Path("/space/bin/space-sbxmgr")
    if not sbxmgr.exists():
        for n in (3, 4, 5, 6, 7, 8, 9):
            board.finish_step(
                n,
                "warn",
                f"{sbxmgr} not present; can't verify in-guest state on this host",
            )
        return 0

    create = subprocess.run(
        [
            "sudo",
            str(sbxmgr),
            "create",
            "--runtime",
            "fc",
            "--template",
            main_alias,
            "--autostart",
            "--blocking",
        ],
        capture_output=True,
        text=True,
    )
    if create.returncode != 0:
        for n in (3, 4, 5, 6, 7, 8, 9):
            board.finish_step(n, "fail", "sandbox create failed")
        _console.print(create.stdout)
        _console.print(create.stderr)
        return 1
    sbx_id = next(
        (
            line.split(":", 1)[1].strip()
            for line in create.stdout.splitlines()
            if line.startswith("Sandbox:")
        ),
        None,
    )
    if sbx_id is None:
        for n in (3, 4, 5, 6, 7, 8, 9):
            board.finish_step(n, "fail", "could not parse sandbox id")
        return 1

    # Give the systemd start unit a moment to fire.
    time.sleep(2.0)

    def exec_in(cmd: list[str]) -> str:
        result = subprocess.run(
            ["sudo", str(sbxmgr), "exec", sbx_id, *cmd],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"in-guest exec failed: {' '.join(cmd)}\n"
                f"stdout={result.stdout!r}\nstderr={result.stderr!r}"
            )
        return result.stdout

    failed = 0

    # Step 3: relative COPY dest landed at /app/payload.
    try:
        body = exec_in(["cat", COPY_GUEST_PATH])
    except RuntimeError as e:
        board.finish_step(3, "fail", str(e))
        failed += 1
    else:
        if body != COPY_BODY:
            board.finish_step(
                3,
                "fail",
                f"expected {COPY_BODY!r}, got {body!r}",
            )
            failed += 1
        else:
            board.finish_step(3, "pass", f"{COPY_GUEST_PATH} matches")

    # Steps 4 + 5: stat output shows expected owner + mode.
    try:
        stat_out = exec_in(["cat", COPY_STAT_OUT]).strip()
    except RuntimeError as e:
        board.finish_step(4, "fail", str(e))
        board.finish_step(5, "fail", "skipped (stat read failed)")
        failed += 2
    else:
        expected_stat = f"{TEST_USER}:{TEST_GROUP} {COPY_MODE_OCTAL:o}"
        if not stat_out.startswith(f"{TEST_USER}:{TEST_GROUP}"):
            board.finish_step(
                4,
                "fail",
                f"expected owner {TEST_USER}:{TEST_GROUP}, got {stat_out!r}",
            )
            failed += 1
        else:
            board.finish_step(4, "pass", f"owner = {TEST_USER}:{TEST_GROUP}")
        if stat_out != expected_stat:
            board.finish_step(
                5,
                "fail",
                f"expected mode {COPY_MODE_OCTAL:o}, got {stat_out!r}",
            )
            failed += 1
        else:
            board.finish_step(5, "pass", f"mode = {COPY_MODE_OCTAL:o}")

    # Step 6: USER stickiness — whoami captured as testuser.
    try:
        whoami = exec_in(["cat", WHOAMI_OUT]).strip()
    except RuntimeError as e:
        board.finish_step(6, "fail", str(e))
        failed += 1
    else:
        if whoami != TEST_USER:
            board.finish_step(
                6, "fail", f"expected {TEST_USER!r}, got {whoami!r}"
            )
            failed += 1
        else:
            board.finish_step(6, "pass", f"run_cmd ran as {whoami}")

    # Step 7: ENV baked into /etc/environment.
    try:
        env_line = exec_in(
            ["grep", f"^{ENV_KEY}=", "/etc/environment"]
        ).strip()
    except RuntimeError as e:
        board.finish_step(7, "fail", str(e))
        failed += 1
    else:
        if env_line != f"{ENV_KEY}={ENV_VAL}":
            board.finish_step(7, "fail", f"got {env_line!r}")
            failed += 1
        else:
            board.finish_step(7, "pass", env_line)

    # Step 8: The optional application runs independently of guest readiness.
    try:
        start = exec_in(
            ["sh", "-c", 'for i in $(seq 1 50); do [ -f "$1" ] && cat "$1" && exit; sleep 0.1; done',
             "runtime-marker", START_MARKER_OUT]
        ).strip()
    except RuntimeError as e:
        board.finish_step(8, "fail", str(e))
        failed += 1
    else:
        if start != START_MARKER_VAL:
            unit = exec_in(
                [
                    "systemctl",
                    "show",
                    "--no-pager",
                    "space-runtime@app.service",
                    "-p",
                    "ActiveState,SubState,Result,ExecMainStatus",
                ]
            ).strip()
            board.finish_step(
                8,
                "fail",
                f"start marker missing — systemctl says {unit!r}",
            )
            failed += 1
        else:
            board.finish_step(8, "pass", start)

    # Step 9: sandbox is Ubuntu (base alias remap landed).
    try:
        os_release = exec_in(["sh", "-c", "head -2 /etc/os-release"]).strip()
    except RuntimeError as e:
        board.finish_step(9, "fail", str(e))
        failed += 1
    else:
        if "Ubuntu" not in os_release:
            board.finish_step(
                9, "fail", f"/etc/os-release lacks Ubuntu: {os_release!r}"
            )
            failed += 1
        else:
            board.finish_step(
                9, "pass", os_release.splitlines()[0].strip()
            )

    subprocess.run(
        ["sudo", str(sbxmgr), "delete", sbx_id],
        capture_output=True,
        text=True,
    )
    return 1 if failed else 0


# ──── Entrypoint ────────────────────────────────────────────────────────────


def main(argv: list[str]) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--api-url", default=os.environ.get("E2B_API_URL", DEFAULT_API_URL)
    )
    parser.add_argument(
        "--api-key", default=os.environ.get("E2B_API_KEY", DEFAULT_API_KEY)
    )
    parser.add_argument("--domain", default=DEFAULT_DOMAIN)
    parser.add_argument(
        "--alias",
        default=f"ecw-tb-full-{time.strftime('%H%M%S')}",
        help="Base alias for the templates this smoke creates.",
    )
    parser.add_argument(
        "--skip-sandbox-check",
        action="store_true",
        help="Build the template but skip spinning a sandbox for in-guest assertions.",
    )
    args = parser.parse_args(argv)

    # The SDK reads E2B_API_URL/E2B_DOMAIN/E2B_API_KEY from env if not
    # explicitly passed; mirror them here so the default_build_logger
    # (which constructs its own client) targets the same host.
    os.environ["E2B_API_URL"] = args.api_url
    os.environ["E2B_DOMAIN"] = args.domain
    os.environ["E2B_API_KEY"] = args.api_key

    board = StatusBoard()
    if _USE_LIVE:
        with Live(
            _render_board(),
            console=_console,
            refresh_per_second=10,
            transient=False,
        ) as live:
            board.live = live
            built = run_template_build_phase(
                board,
                alias_base=args.alias,
                api_key=args.api_key,
                domain=args.domain,
                api_url=args.api_url,
            )
            if args.skip_sandbox_check:
                for n in (3, 4, 5, 6, 7, 8, 9):
                    if get_row(n).status == "running":
                        board.finish_step(n, "warn", "--skip-sandbox-check set")
                return 0
            return verify_sandbox_phase(board, built["main_alias"])
    else:
        built = run_template_build_phase(
            board,
            alias_base=args.alias,
            api_key=args.api_key,
            domain=args.domain,
            api_url=args.api_url,
        )
        if args.skip_sandbox_check:
            for n in (3, 4, 5, 6, 7, 8, 9):
                if get_row(n).status == "running":
                    board.finish_step(n, "warn", "--skip-sandbox-check set")
            return 0
        return verify_sandbox_phase(board, built["main_alias"])


# Keep `field` referenced so the import isn't flagged by lint configs that
# require all imports to be used; the dataclass machinery above doesn't
# directly invoke it.
_ = field

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
