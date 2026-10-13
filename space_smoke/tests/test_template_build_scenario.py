"""Contracts of the template-build scenario the bench cannot exercise.

A passing bench shows one build complete across the handoff and one guest pass
every check. It cannot show that a build the submit builder never accepts
fails inside the budget, that a build which dies before acceptance surfaces its
own error, that a missing prepared rootfs or a metadata/bundle mismatch or
containerd residue or one failing guest check each fail with the right name,
that the route clear is armed before any routing and released after the build,
that the template delete is armed before the build starts, that the booted
sandbox is killed before the template is deleted, or that the SDK adapter
renders the recipe into the SDK calls with the run's credentials.
"""

from __future__ import annotations

import json
import shutil
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

from space_smoke.adapters import (
    EcwConfig,
    EcwSdk,
    KindConfig,
    KindHarness,
    _endpoint_target_pods,
)
from space_smoke.capabilities import GuestMetadata, TemplateBuildResult, TemplateRecipe
from space_smoke.runtime import CommandResult, InvariantViolation, ResourceScope
from space_smoke.template_build import (
    OCI_IMAGE,
    TemplateBuildAssertions,
    compatibility_family,
    guest_checks,
    metadata_matches_bundle,
    oci_recipe,
)

SID = "01a0b0c9-b8a3-7473-9c12-3821a701bc28"


class Recipe(unittest.TestCase):
    def test_the_dockerfile_carries_every_instruction_family_the_suite_proved(self) -> None:
        recipe = oci_recipe()
        for token in ("FROM docker.io/library/python:3.12-slim-bookworm", "ENV ECW_OCI_DOCKERFILE_ENV",
                      "WORKDIR /opt/ecw-oci-smoke", "COPY copy-marker.txt", "USER user", "USER root"):
            self.assertIn(token, recipe.dockerfile)
        self.assertEqual(set(recipe.files), {"copy-marker.txt"})
        self.assertEqual(recipe.envs, {"ECW_TEMPLATE_BUILD_SMOKE": "1"})
        self.assertTrue(any(cmd.startswith("sleep ") for cmd, _ in recipe.run_cmds))
        self.assertIn("tail -f /dev/null", recipe.start_cmd)

    def test_guest_checks_include_source_image_checks_only_for_the_known_image(self) -> None:
        self.assertIn("source image Python", guest_checks(OCI_IMAGE))
        self.assertNotIn("source image Python", guest_checks("docker.io/library/debian:bookworm-slim"))
        self.assertIn("SPACE guest service", guest_checks())


class Family(unittest.TestCase):
    def test_family_oracle_matches_the_shell_suite(self) -> None:
        self.assertEqual(compatibility_family("0.0.7+g1234"), "0.0.7")
        self.assertEqual(compatibility_family("0.3.1-rc1"), "0.3")
        self.assertEqual(compatibility_family("2.4.0"), "2")

    def test_metadata_verdicts(self) -> None:
        self.assertIsNone(metadata_matches_bundle(GuestMetadata("0.3.1", "0.3", "0.3.1")))
        self.assertIn("no /etc/spaced", metadata_matches_bundle(GuestMetadata("0.3.1", "0.3", "")) or "")
        self.assertIn("spaced_version", metadata_matches_bundle(GuestMetadata("0.3.0", "0.3", "0.3.1")) or "")
        self.assertIn("is not 0.3", metadata_matches_bundle(GuestMetadata("0.3.1", "0.4", "0.3.1")) or "")


class _Fixture:
    def __init__(self) -> None:
        self.sandbox = SimpleNamespace(sandbox_id=SID)
        self.records: dict[str, object] = {}

    def verify(self, condition: bool, message: str, **_evidence: object) -> None:
        if not condition:
            raise InvariantViolation(message)

    def record(self, name: str, value: object) -> None:
        self.records[name] = value

    def wait(self, _seconds: float, *, reason: str | None = None) -> None:
        pass


class _Sandbox:
    def __init__(self, failing: str | None, calls: list[str]) -> None:
        self.sandbox_id = "sbx_boot"
        self.failing = failing
        self.calls = calls

    def run(self, command: str, *, timeout: int = 60):
        code = 1 if self.failing and self.failing in command else 0
        return SimpleNamespace(exit_code=code, stdout="", stderr="boom" if code else "")

    def kill(self) -> None:
        self.calls.append("kill-sandbox")


class _Builder:
    def __init__(self, calls: list[str], *, fail: Exception | None = None, failing_check: str | None = None) -> None:
        self.calls = calls
        self.fail = fail
        self.failing_check = failing_check
        self.built_with: dict[str, object] = {}

    def build_template(self, recipe, *, alias, cpu_count, memory_mb, tags):
        self.calls.append("build")
        if self.fail is not None:
            raise self.fail
        self.built_with = {"alias": alias, "tags": tuple(tags), "cpu": cpu_count, "mem": memory_mb}
        return TemplateBuildResult("tpl-1", "bld-1", alias, 12.5, ("log a", "log b"))

    def create(self, *, template, timeout, metadata):
        self.calls.append(f"create:{template}")
        self.created_with = dict(metadata)
        return _Sandbox(self.failing_check, self.calls)


class _Control:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.nodes = ["space-local-builder-a", "space-local-builder-b"]
        self.accepts_on = {"space-local-builder-a"}
        self.handoff: str | None = None
        self.metadata = GuestMetadata("0.3.1", "0.3", "0.3.1")
        self.residue: list[str] = []
        self.delete_returns: str | None = "op-del"
        self.op_state = "succeeded"

    def builder_nodes(self):
        return list(self.nodes)

    def route_builder_service(self, route, node_name, *, timeout=30):
        self.calls.append(f"route:{route}:{node_name[-1]}")

    def clear_builder_service_route(self):
        self.calls.append("route:clear")

    def builder_logged_alias(self, node_name, alias):
        return node_name in self.accepts_on

    def prepared_rootfs_handoff(self, node_name, *, window_seconds=60):
        return self.handoff

    def published_guest_metadata(self, node_name, alias):
        return self.metadata

    def containerd_residue(self, node_name, image):
        return list(self.residue)

    def delete_template_as_admin(self, alias):
        self.calls.append(f"delete-template:{alias}")
        return self.delete_returns

    def operation_state(self, operation_id):
        return self.op_state


def _assertions(control: _Control, builder: _Builder):
    fixture = _Fixture()
    scope = ResourceScope()
    return fixture, TemplateBuildAssertions(cast(Any, fixture), cast(Any, control), cast(Any, builder), scope), scope


class Handoff(unittest.TestCase):
    def test_route_is_cleared_first_pinned_to_submit_then_moved_to_the_peer_then_cleared(self) -> None:
        calls: list[str] = []
        control, builder = _Control(calls), _Builder(calls)
        fixture, build, _ = _assertions(control, builder)
        build.pin_builders()
        build.build_through_sdk(accept_timeout=5, build_timeout=30)
        self.assertEqual(calls, ["route:clear", "route:submit:a", "build", "route:poll-peer:b", "route:clear"])
        self.assertEqual(fixture.records["service_routed_to"], "space-local-builder-b")
        self.assertEqual(builder.built_with["alias"], build.alias)
        self.assertEqual(builder.built_with["tags"], ("latest", "kind-smoke"))

    def test_a_build_the_peer_accepted_hands_status_to_the_pinned_builder_and_inspects_the_peer(self) -> None:
        calls: list[str] = []
        control, builder = _Control(calls), _Builder(calls)
        control.accepts_on = {"space-local-builder-b"}
        inspected: list[str] = []
        control.published_guest_metadata = lambda node, alias: (inspected.append(node), control.metadata)[1]  # type: ignore[method-assign]
        fixture, build, _ = _assertions(control, builder)
        build.pin_builders()
        build.build_through_sdk(accept_timeout=5, build_timeout=30)
        self.assertEqual(fixture.records["accepted_by"], "space-local-builder-b")
        self.assertEqual(fixture.records["service_routed_to"], "space-local-builder-a")
        self.assertFalse(fixture.records["pinned_submit_honoured"])
        self.assertIn("route:poll-peer:a", calls)
        build.expect_guest_metadata_matches_bundle()
        self.assertEqual(inspected, ["space-local-builder-b"])

    def test_one_builder_is_refused(self) -> None:
        calls: list[str] = []
        control = _Control(calls)
        control.nodes = ["space-local-builder-a"]
        _, build, _ = _assertions(control, _Builder(calls))
        with self.assertRaisesRegex(InvariantViolation, "exactly two builders with a Ready node-api"):
            build.pin_builders()

    def test_a_build_never_accepted_fails_inside_the_budget(self) -> None:
        calls: list[str] = []
        control = _Control(calls)
        control.accepts_on = set()
        _, build, _ = _assertions(control, _Builder(calls))
        build.pin_builders()
        # The fake build returns at once, so `running` is False on the first poll
        # and the join surfaces "no failure"; the acceptance verdict then fires.
        with self.assertRaisesRegex(InvariantViolation, "no builder accepted the build"):
            build.build_through_sdk(accept_timeout=0, build_timeout=5)

    def test_a_build_that_dies_before_acceptance_surfaces_its_own_error(self) -> None:
        calls: list[str] = []
        control = _Control(calls)
        control.accepts_on = set()
        _, build, _ = _assertions(control, _Builder(calls, fail=RuntimeError("build failed: step 3")))
        build.pin_builders()
        with self.assertRaisesRegex(RuntimeError, "step 3"):
            build.build_through_sdk(accept_timeout=5, build_timeout=5)

    def test_a_missing_prepared_rootfs_fails_before_the_handoff(self) -> None:
        calls: list[str] = []
        control = _Control(calls)
        control.handoff = "committed prepared rootfs was not observed"
        _, build, _ = _assertions(control, _Builder(calls))
        build.pin_builders()
        with self.assertRaisesRegex(InvariantViolation, "prepared rootfs"):
            build.build_through_sdk(accept_timeout=5, build_timeout=5)
        self.assertNotIn("route:poll-peer:b", calls)

    def test_template_delete_is_armed_before_the_build_and_runs_on_close(self) -> None:
        calls: list[str] = []
        control = _Control(calls)
        control.handoff = "not committed"
        _, build, scope = _assertions(control, _Builder(calls))
        build.pin_builders()
        with self.assertRaises(InvariantViolation):
            build.build_through_sdk(accept_timeout=5, build_timeout=5)
        scope.close()
        self.assertIn(f"delete-template:{build.alias}", calls)


class _BlockingBuilder(_Builder):
    """A build that does not return until released, so failure paths race it."""

    def __init__(self, calls: list[str]) -> None:
        super().__init__(calls)
        import threading

        self.release = threading.Event()

    def build_template(self, recipe, *, alias, cpu_count, memory_mb, tags):
        self.calls.append("build:start")
        self.release.wait(5)
        self.calls.append("build:end")
        return TemplateBuildResult("tpl-1", "bld-1", alias, 0.1, ())


class SettleBeforeDelete(unittest.TestCase):
    """A failure path must not delete the alias under a live build."""

    def test_an_acceptance_timeout_waits_for_the_build_before_the_template_delete(self) -> None:
        import threading

        calls: list[str] = []
        control = _Control(calls)
        control.accepts_on = set()
        builder = _BlockingBuilder(calls)
        _, build, scope = _assertions(control, builder)
        build.pin_builders()
        with self.assertRaisesRegex(InvariantViolation, "no builder accepted"):
            build.build_through_sdk(accept_timeout=0.2, build_timeout=10)
        threading.Timer(0.3, builder.release.set).start()
        failures = scope.close()
        self.assertEqual([f.name for f in failures], [])
        self.assertLess(calls.index("build:end"), calls.index(f"delete-template:{build.alias}"))

    def test_a_build_still_running_at_the_settle_budget_is_a_critical_failure_and_the_rest_still_runs(self) -> None:
        from unittest import mock

        import space_smoke.template_build as module

        calls: list[str] = []
        control = _Control(calls)
        control.accepts_on = set()
        builder = _BlockingBuilder(calls)
        _, build, scope = _assertions(control, builder)
        build.pin_builders()
        with self.assertRaises(InvariantViolation):
            build.build_through_sdk(accept_timeout=0.2, build_timeout=30)
        with mock.patch.object(module, "SETTLE_BUDGET_SECONDS", 0.6):
            failures = scope.close(timeout_seconds=5)
        builder.release.set()
        settle = [f for f in failures if "settle" in f.name]
        self.assertEqual([(f.critical, "still running" in f.detail) for f in settle], [(True, True)])
        # The stalled build must not eat the cleanup
        # budget — the route clear ran first and the template delete still ran.
        self.assertIn("route:clear", calls)
        self.assertLess(calls.index("route:clear"), calls.index(f"delete-template:{build.alias}"))
        self.assertEqual([f.name for f in failures if "deadline exhausted" in f.detail], [])

    def test_the_settle_step_fits_the_scope_cleanup_budget(self) -> None:
        import space_smoke.template_build as module

        # min(120 s, 10 % of the 1200 s scenario timeout) is the budget every
        # compensation must share; settle + delete leave room for the rest.
        self.assertLessEqual(module.SETTLE_BUDGET_SECONDS + module.TEMPLATE_DELETE_BUDGET_SECONDS, 100)

    def test_the_happy_path_disarms_the_settle_step(self) -> None:
        calls: list[str] = []
        control, builder = _Control(calls), _Builder(calls)
        _, build, scope = _assertions(control, builder)
        build.pin_builders()
        build.build_through_sdk(accept_timeout=5, build_timeout=30)
        failures = scope.close()
        self.assertEqual(failures, ())
        self.assertNotIn("settle", " ".join(calls))


class Artefacts(unittest.TestCase):
    def _built(self):
        calls: list[str] = []
        control, builder = _Control(calls), _Builder(calls)
        fixture, build, scope = _assertions(control, builder)
        build.pin_builders()
        build.build_through_sdk(accept_timeout=5, build_timeout=30)
        return calls, control, builder, fixture, build, scope

    def test_metadata_mismatch_fails_with_the_mismatch(self) -> None:
        _, control, _, _, build, _ = self._built()
        control.metadata = GuestMetadata("0.3.0", "0.3", "0.3.1")
        with self.assertRaisesRegex(InvariantViolation, "spaced_version differs"):
            build.expect_guest_metadata_matches_bundle()

    def test_residue_fails(self) -> None:
        _, control, _, _, build, _ = self._built()
        control.residue = ["temp-path: /space/cache/firecracker/builds/oci/space-oci-rootfs-1"]
        with self.assertRaisesRegex(InvariantViolation, "left containerd records"):
            build.expect_containerd_clean()

    def test_one_failing_guest_check_fails_by_name_and_the_sandbox_is_still_killed(self) -> None:
        calls: list[str] = []
        control, builder = _Control(calls), _Builder(calls, failing_check="space.target")
        _, build, scope = _assertions(control, builder)
        build.pin_builders()
        build.build_through_sdk(accept_timeout=5, build_timeout=30)
        with self.assertRaisesRegex(InvariantViolation, "guest check failed: SPACE guest service"):
            build.boot_and_verify_guest()
        scope.close()
        self.assertIn("kill-sandbox", calls)
        self.assertLess(calls.index("kill-sandbox"), calls.index(f"delete-template:{build.alias}"))

    def test_boot_uses_the_alias_with_a_per_run_tag_and_retire_kills_then_deletes(self) -> None:
        calls, control, builder, fixture, build, scope = self._built()
        build.boot_and_verify_guest()
        self.assertEqual(calls[-1], f"create:{build.alias}")
        self.assertTrue(builder.created_with["space_smoke_run"])
        self.assertEqual(len(fixture.records["guest_checks_passed"]), len(guest_checks()))
        build.retire()
        self.assertEqual(calls[-2:], ["kill-sandbox", f"delete-template:{build.alias}"])
        before = list(calls)
        scope.close()
        self.assertEqual(calls, before)

    def test_a_failed_delete_operation_is_a_cleanup_failure(self) -> None:
        _, control, _, _, build, scope = self._built()
        control.op_state = "failed"
        failures = scope.close()
        self.assertEqual([f.critical for f in failures if "delete the built template" in f.name], [True])


class _ScriptedKubectl:
    def __init__(self, results) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, ...]] = []

    def run(self, command, **_options) -> CommandResult:
        self.calls.append(tuple(command))
        return self.results.pop(0)


ARGV = ("kubectl",)
# The pod lookups on-node operations start with: the operator shell, and the
# builder's node-api pod the Service is routed to.
SHELL = CommandResult(ARGV, 0, "space-node-debug-a", "")
NODE_API = CommandResult(ARGV, 0, "space-builder-node-api-a", "")


def _harness(*results: CommandResult) -> tuple[KindHarness, _ScriptedKubectl]:
    kubectl = _ScriptedKubectl(results)
    return KindHarness(kubectl, KindConfig()), kubectl


def _endpoints(*pods: str) -> str:
    return json.dumps({"subsets": [{"addresses": [{"targetRef": {"name": p}} for p in pods]}]})


class HarnessOps(unittest.TestCase):
    def test_routing_labels_patches_and_waits_for_the_single_endpoint(self) -> None:
        harness, kubectl = _harness(
            NODE_API,
            CommandResult(ARGV, 0, "", ""),
            CommandResult(ARGV, 0, "", ""),
            CommandResult(ARGV, 0, _endpoints("space-builder-node-api-a", "space-builder-node-api-b"), ""),
            CommandResult(ARGV, 0, _endpoints("space-builder-node-api-a"), ""),
        )
        harness.route_builder_service("submit", "space-local-builder-a", timeout=5)
        self.assertIn(
            "app.kubernetes.io/name=space-builder,app.kubernetes.io/component=node-api",
            kubectl.calls[0],
        )
        self.assertIn("space-builder-node-api-a", kubectl.calls[1])
        self.assertIn("space.pplx.net/routing-smoke=submit", kubectl.calls[1])
        self.assertIn("patch", kubectl.calls[2])
        self.assertEqual(len(kubectl.calls), 5)

    def test_routing_refuses_an_unsafe_label_value(self) -> None:
        harness, _ = _harness()
        with self.assertRaises(ValueError):
            harness.route_builder_service("submit; rm -rf /", "space-local-builder-a")

    def test_endpoint_parsing(self) -> None:
        self.assertEqual(_endpoint_target_pods(_endpoints("b", "a")), ["a", "b"])
        self.assertEqual(_endpoint_target_pods("{}"), [])
        self.assertEqual(_endpoint_target_pods("not json"), [])

    def test_guest_metadata_reads_three_lines_and_refuses_a_foreign_alias(self) -> None:
        harness, kubectl = _harness(SHELL, CommandResult(ARGV, 0, "0.3.1+g12\n0.3.1+g12\n0.3\n", ""))
        metadata = harness.published_guest_metadata("space-local-builder-a", "space-smoke-oci-0123abcd")
        self.assertEqual(metadata, GuestMetadata("0.3.1+g12", "0.3", "0.3.1+g12"))
        self.assertIn("/space/cache/firecracker/templates/space-smoke-oci-0123abcd", kubectl.calls[1][-1])
        with self.assertRaises(ValueError):
            harness.published_guest_metadata("space-local-builder-a", "seed")

    def test_containerd_residue_is_the_non_empty_lines(self) -> None:
        harness, _ = _harness(SHELL, CommandResult(ARGV, 0, "\ntemp-path: /x\n\n", ""))
        self.assertEqual(harness.containerd_residue("space-local-builder-a", OCI_IMAGE), ["temp-path: /x"])

    def test_a_failing_ctr_listing_is_an_inspection_failure_not_a_clean_result(self) -> None:
        # `|| true` on the listing would have turned an
        # unavailable containerd into an empty, passing residue list.
        harness, _ = _harness(
            SHELL,
            CommandResult(ARGV, 3, "", "ctr: failed to dial /run/containerd/containerd.sock\nctr images list failed in the space-builds namespace")
        )
        with self.assertRaisesRegex(Exception, "inspect containerd build residue"):
            harness.containerd_residue("space-local-builder-a", OCI_IMAGE)

    @unittest.skipUnless(shutil.which("bash"), "needs bash")
    def test_the_residue_script_exits_non_zero_when_ctr_is_unavailable(self) -> None:
        import subprocess

        from space_smoke.adapters import _CONTAINERD_RESIDUE_SCRIPT

        script = _CONTAINERD_RESIDUE_SCRIPT.replace("__IMAGE__", "'docker.io/library/python:3.12-slim-bookworm'")
        # No `ctr` on PATH: the listing fails and the script must say so, not print nothing and exit 0.
        result = subprocess.run([shutil.which("bash") or "bash", "-c", script], capture_output=True, text=True, timeout=30, env={"PATH": "/nonexistent"})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ctr images list failed", result.stderr)

    def test_handoff_failure_text_comes_from_the_script(self) -> None:
        harness, _ = _harness(
            SHELL,
            CommandResult(ARGV, 0, "", ""),
            SHELL,
            CommandResult(ARGV, 1, "", "committed prepared rootfs was not observed"),
        )
        self.assertIsNone(harness.prepared_rootfs_handoff("space-local-builder-a"))
        self.assertIn("not observed", harness.prepared_rootfs_handoff("space-local-builder-a") or "")


class GuestMetadataScript(unittest.TestCase):
    """Bench round 1 (F2): the sed backreference had been written as a 0x01 byte."""

    @unittest.skipUnless(shutil.which("bash") and shutil.which("sed"), "needs bash and sed")
    def test_the_script_prints_the_two_metadata_fields(self) -> None:
        import subprocess
        import tempfile

        from space_smoke.adapters import _GUEST_METADATA_SCRIPT

        self.assertNotIn("\x01", _GUEST_METADATA_SCRIPT)
        with tempfile.TemporaryDirectory() as tmp:
            digest = Path(tmp) / "sha256:abc"
            digest.mkdir()
            # Pretty-printed like the real file: one field per line, trailing newline.
            (digest / "metadata.json").write_text(
                '{\n  "spaced_version": "0.1.4",\n  "spaced_compatibility_family": "0.1"\n}\n'
            )
            script = _GUEST_METADATA_SCRIPT.replace("__TEMPLATE_DIR__", f"'{tmp}'")
            result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = result.stdout.splitlines()
        # Line 1 is the bundle version from debugfs, empty here (no rootfs).
        self.assertEqual(lines[1:], ["0.1.4", "0.1"])


class SdkBuild(unittest.TestCase):
    def test_the_recipe_is_rendered_into_the_sdk_calls_with_the_run_credentials(self) -> None:
        seen: dict[str, object] = {}
        written: dict[str, str] = {}

        class Builder:
            def __init__(self) -> None:
                self.steps: list[tuple[str, object]] = []

            def set_envs(self, envs):
                self.steps.append(("envs", dict(envs)))
                return self

            def run_cmd(self, command, user=None):
                self.steps.append(("run", (command, user)))
                return self

            def set_start_cmd(self, start_cmd, ready_cmd):
                self.steps.append(("start", (start_cmd, ready_cmd)))
                return self

        class Template:
            def __init__(self, file_context_path=None, **_kw) -> None:
                seen["context"] = Path(file_context_path)

            def from_dockerfile(self, path):
                written["Dockerfile"] = Path(path).read_text()
                written["copy-marker.txt"] = (Path(path).parent / "copy-marker.txt").read_text()
                return Builder()

            @staticmethod
            def build(final, *, alias, tags, cpu_count, memory_mb, on_build_logs, **opts):
                seen["final_steps"] = final.steps
                seen["alias"] = alias
                seen["tags"] = tags
                seen["opts"] = opts
                on_build_logs(SimpleNamespace(timestamp="t", level="info", message="step 1"))
                return SimpleNamespace(template_id="tpl-1", build_id="bld-1")

        fake = types.SimpleNamespace(Template=Template, wait_for_timeout=lambda t: f"wait:{t}")
        with mock.patch.dict(sys.modules, {"e2b": fake}):
            result = EcwSdk(
                EcwConfig(api_key="key", domain="local.e2b.test", api_url="http://127.0.0.1:8080", api_host="api.local.e2b.test")
            ).build_template(oci_recipe(), alias="space-smoke-oci-0123abcd", cpu_count=1, memory_mb=1024, tags=("latest", "kind-smoke"))
        self.assertEqual((result.template_id, result.build_id, result.alias), ("tpl-1", "bld-1", "space-smoke-oci-0123abcd"))
        self.assertEqual(result.log_tail, ("t info step 1",))
        # F1 (bench round 1): the Host is the API URL's own authority, not the
        # DNS-less kind name, so the SDK's upload client can resolve the
        # absolute upload URL the API derives from it.
        self.assertEqual(seen["opts"], {"api_key": "key", "domain": "local.e2b.test", "api_url": "http://127.0.0.1:8080", "headers": {"Host": "127.0.0.1:8080"}})
        self.assertIn("FROM docker.io/library/python:3.12-slim-bookworm", written["Dockerfile"])
        self.assertEqual(written["copy-marker.txt"], "hello from ecw template build smoke\n")
        steps = cast(list, seen["final_steps"])
        self.assertEqual(steps[0], ("envs", {"ECW_TEMPLATE_BUILD_SMOKE": "1"}))
        self.assertEqual(steps[-1][0], "start")
        self.assertEqual(steps[-1][1][1], "wait:2")

    def test_a_context_file_name_with_a_path_is_refused(self) -> None:
        recipe = TemplateRecipe("FROM x", {"../escape": "x"}, {}, (), "true", 1)
        fake = types.SimpleNamespace(Template=lambda **kw: None, wait_for_timeout=lambda t: t)
        with mock.patch.dict(sys.modules, {"e2b": fake}):
            with self.assertRaises(ValueError):
                EcwSdk(EcwConfig(api_key="key")).build_template(recipe, alias="a", cpu_count=1, memory_mb=1, tags=())


if __name__ == "__main__":
    unittest.main()
