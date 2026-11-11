"""Base-memory and pool conformance on a caller-selected standalone worker."""

from __future__ import annotations

import re
import shlex
import textwrap
import time

from space_smoke.direct_node import DirectNodeRecoveryFixture
from space_smoke.adapters import TEMPLATE_CACHE_ROOT
from space_smoke.runtime import ScenarioTimedOut


def field(output: str, name: str) -> str:
    match = re.search(rf"^\s*{re.escape(name)}:\s+(\S+)", output, re.MULTILINE)
    if match is None:
        raise RuntimeError(f"missing {name} in response: {output}")
    return match[1]


def exercise_base_memory(test: DirectNodeRecoveryFixture, *, firmware: bool) -> None:
    """Own the template policy and every clone; never change the input template."""
    test.prepare_node(firmware=firmware)
    test.context.verify(test.command(["space-sbxmgr", "list"]) == "No sandboxes found.",
                        "base-memory qualification requires an idle node")
    name = "space-smoke-memory-" + test.marker
    memory = "8192" if firmware else "1024"
    children: list[DirectNodeRecoveryFixture] = []
    registered = False
    pool_created = False
    # DeletePool stops two donors sequentially; firmware shutdown allows 120s each.
    pool_delete_timeout = 300 if firmware else 180

    def cleanup() -> None:
        nonlocal pool_created, registered
        failures: list[Exception] = []
        deadline = time.monotonic() + 590

        def remaining(limit: float) -> float:
            seconds = deadline - time.monotonic()
            if seconds <= 0:
                raise ScenarioTimedOut("base-memory cleanup budget exhausted")
            return min(limit, seconds)

        if pool_created:
            try:
                test.command(["space-tmplmgr", "delete-pool", "--template", name], timeout=remaining(pool_delete_timeout))
                pool_created = False
            except ScenarioTimedOut:
                raise
            except Exception as error:
                failures.append(error)
        for child in children:
            if child.selector is not None or child.create_directory is not None:
                try:
                    child.cleanup(deadline=deadline)
                    child.selector = None
                    child.create_directory = None
                except ScenarioTimedOut:
                    raise
                except Exception as error:
                    failures.append(error)
        if registered:
            try:
                test.command(["space-tmplmgr", "delete-template", "--template", name], timeout=remaining(180))
                inventory = test.command(["space-sbxmgr", "list"], timeout=remaining(15))
                test.context.verify(name not in inventory, "owned sandboxes remain after template deletion")
                registered = False
            except ScenarioTimedOut:
                raise
            except Exception as error:
                failures.append(error)
        if failures:
            raise failures[0]

    test.context.scope.defer("remove base-memory clones and template", cleanup, critical=True)
    with test.context.phase("prepare an isolated template and base memory"):
        source = test.command(["space-tmplmgr", "get", "--template", test.config.template,
                               "--content-hash", test.config.content_hash])
        runtime = field(source, "runtime")
        stage = f"{TEMPLATE_CACHE_ROOT}/{name}/build.importing.smoke"
        script = textwrap.dedent("""\
            import grp, json, os, pathlib, pwd, subprocess, sys
            source, stage = map(pathlib.Path, sys.argv[1:3])
            name, environment, memory = sys.argv[3:]
            metadata = json.loads((source / 'metadata.json').read_text())
            if metadata['execution']['environment'] != environment:
                raise RuntimeError('input template has a different execution binding')
            stage.parent.mkdir(mode=0o2775)
            stage.mkdir(mode=0o2770)
            uid = pwd.getpwnam('space-tmplmgr').pw_uid
            gid = grp.getgrnam('space-cache-rw').gr_gid
            subprocess.run(['cp', '--reflink=always', '--sparse=auto',
                            str(source / 'rootfs.ext4'), str(stage / 'rootfs.ext4')], check=True)
            metadata.update(id=name, name=name, vcpu=2, memory_mib=int(memory))
            (stage / 'metadata.json').write_text(json.dumps(metadata))
            for path in (stage.parent, stage, *stage.iterdir()):
                os.chown(path, uid, gid)
                os.chmod(path, 0o2770 if path.is_dir() else 0o660)
        """)
        # Own cleanup before staging or registration: a lost reply may still publish.
        registered = True
        test.command(["sudo", "-n", "python3", "-c", script, field(source, "template_dir"),
                      stage, name, test.config.environment, memory])
        result = test.command(["space-tmplmgr", "register", "--template", name,
                               "--staging-dir", "build.importing.smoke"], timeout=180)
        digest = field(result, "content_hash")
        result = test.command(["space-tmplmgr", "base-snapshot", "--template", name,
                               "--state", "true", "--vcpu", "2", "--memory-mib", memory],
                              timeout=240)
        test.context.verify("reconcile_error:" not in result, result)
        result = test.command(["space-tmplmgr", "get", "--template", name,
                               "--content-hash", digest, "--vcpu", "2", "--memory-mib", memory])
        field(result, "snapshot_dir")

    def create(mode: str) -> DirectNodeRecoveryFixture:
        child = DirectNodeRecoveryFixture(test.context, test.config)
        child.marker = test.marker  # Same path makes sibling disk isolation observable.
        children.append(child)
        child.create_directory = test.command(["mktemp", "-d", "/tmp/space-smoke-create.XXXXXXXXXX"])
        response = child.create_directory + "/response"
        arguments = ["space-sbxmgr", "create", "--runtime", runtime, "--template", name,
                     "--content-hash", digest, "--boot-mode", mode,
                     "--user", "deploy@local", "--auto-lifecycle-off"]
        output = test.command(["bash", "-c", f"{shlex.join(arguments)} > {shlex.quote(response)} && cat {shlex.quote(response)}"], timeout=180)
        child.selector = field(output, "Sandbox")
        test.context.verify(f"Boot mode: {mode}" in output, "create fell back from requested memory boot")
        if mode == "pool":
            child.memory_lifecycle("wake")
        else:
            arguments = ["space-sbxmgr", "start", child.selector, "--from-warm", "--blocking",
                         "--memory", memory, "--ready-timeout-secs", "120"]
            child.command(arguments, timeout=240)
        child.observe()
        return child

    with test.context.phase("restore independent siblings from one base"):
        first, second = create("base-snapshot"), create("base-snapshot")
        markers = [child.memory_marker(start=True) for child in (first, second)]
        test.context.verify(markers[0].nonce != markers[1].nonce, "siblings reused an OS-generated nonce")
        first.memory_lifecycle("suspend")
        second.verify_memory(markers[1])
        first.memory_lifecycle("restore")
        first.verify_memory(markers[0])
        second.verify_memory(markers[1])
        if firmware:
            first.verify_windows_agent_operations()
        for child in (first, second):
            child.cleanup()
            child.selector = None
            child.create_directory = None

    for boot_mode in ("snapshot", "cold"):
        with test.context.phase(f"refill and claim a {boot_mode} pool"):
            pool_created = True
            test.command(["space-tmplmgr", "create-pool", "--template", name,
                          "--count", "2", "--boot-mode", boot_mode])
            members = []
            for _ in range(2):
                # Let refill finish before the next claim; a busy donor may
                # legitimately make an explicit pool request fall back to base.
                deadline = time.monotonic() + 240
                while True:
                    status = test.command(["space-tmplmgr", "pool-status", "--template", name])
                    if status == f"{name}: 2/2 ready, 0 pending":
                        break
                    test.context.verify(time.monotonic() < deadline, f"pool did not refill: {status}")
                    test.context.wait(2)
                members.append(create("pool"))
            markers = [member.memory_marker(start=True) for member in members]
            test.context.verify(markers[0].nonce != markers[1].nonce,
                                "claimed pool siblings reused an OS-generated nonce")
            for member, marker in zip(members, markers, strict=True):
                member.memory_lifecycle("pause")
                member.memory_lifecycle("resume")
                member.verify_memory(marker)
                if firmware:
                    member.verify_windows_agent_operations()
            test.command(["space-tmplmgr", "delete-pool", "--template", name], timeout=pool_delete_timeout)
            pool_created = False
            for member in members:
                member.cleanup()
                member.selector = None
                member.create_directory = None
    with test.context.phase("delete owned pool, sandboxes and template"):
        cleanup()
