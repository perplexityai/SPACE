"""Building bundle images in GitHub Actions and publishing them to ECR from `main`."""

import contextlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "ci/scripts/run-space-bundle-builds.py"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
ACCOUNT = "333333333333"
REGISTRY = f"{ACCOUNT}.dkr.ecr.us-east-1.amazonaws.com"
CACHE_REPOSITORY = "ghcr.io/example/space-bundle-cache"
A, B, C = "a" * 40, "b" * 40, "c" * 40


def load_bundle_builds_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "run_space_bundle_builds_publishing", SCRIPT_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


bundle_builds = load_bundle_builds_module()


def github_main_env(commit: str = A) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(
            ("BUILDKITE", "SPACE_BUNDLE", "SPACE_CI_", "SPACE_MIRRORED", "GITHUB_", "GH_")
        )
    }
    env.update(
        {
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_SHA": commit,
            "GITHUB_REF": "refs/heads/main",
            "GITHUB_REPOSITORY": "perplexityai/SPACE",
            "SPACE_BUNDLE_PUBLISH": "1",
            "SPACE_BUNDLE_CACHE": "enabled",
            "SPACE_BUNDLE_CACHE_REPOSITORY": CACHE_REPOSITORY,
            "SPACE_BUNDLE_TARGETS": "oss",
            "SPACE_BUNDLE_TARGET_ACCOUNTS": json.dumps({"oss": ACCOUNT}),
        }
    )
    return env


def github_dispatch_env() -> dict[str, str]:
    """A dispatched run: it reads main's cache, never writes it, never publishes."""
    env = github_main_env()
    for key in ("SPACE_BUNDLE_TARGETS", "SPACE_BUNDLE_TARGET_ACCOUNTS"):
        del env[key]
    env.update(
        {
            "GITHUB_EVENT_NAME": "workflow_dispatch",
            "GITHUB_REF": "refs/heads/feature",
            "SPACE_BUNDLE_PUBLISH": "0",
            "SPACE_BUNDLE_CACHE": "read-only",
        }
    )
    return env


def option_values(command: list[str], option: str) -> list[str]:
    return [command[index + 1] for index, arg in enumerate(command) if arg == option]


class FakeEcr:
    """One repository's commit tags.

    Each published commit's manifest is `manifest:<commit>` with digest
    `sha256:<commit>`. Any ECR call other than a read fails the test.
    """

    def __init__(self, published: list[str]) -> None:
        self.published = set(published)
        self.failure_code = "ImageNotFound"
        self.calls: list[list[str]] = []

    def run(self, args, **kwargs) -> subprocess.CompletedProcess[str]:
        args = list(args)
        self.calls.append(args)
        if "batch-get-image" in args:
            tags = [arg.removeprefix("imageTag=") for arg in args if arg.startswith("imageTag=")]
            response = {
                "images": [
                    [tag, f"manifest:{tag}", OCI_MANIFEST, f"sha256:{tag}"]
                    for tag in tags
                    if tag in self.published
                ],
                "failures": [
                    [tag, self.failure_code] for tag in tags if tag not in self.published
                ],
            }
            return subprocess.CompletedProcess(args, 0, json.dumps(response), "")
        raise AssertionError(f"unexpected command: {args}")


class ReRunTests(unittest.TestCase):
    def build_feature(self, ecr: FakeEcr, feature: str):
        with mock.patch.dict(os.environ, github_main_env(), clear=True):
            builder = bundle_builds.BundleBuilder([feature])
            with (
                mock.patch.object(builder, "prepare_aws_environment"),
                mock.patch.object(bundle_builds, "run", side_effect=ecr.run),
                mock.patch.object(builder, "build_rust_artifacts") as rust,
                mock.patch.object(
                    builder,
                    "build_space_bundle_image",
                    side_effect=lambda _: ecr.published.add(A),
                ) as image,
            ):
                builder.build_space_bundle_feature(feature)
        return rust, image

    def test_a_published_commit_skips_the_build(self) -> None:
        ecr = FakeEcr(published=[A])

        rust, image = self.build_feature(ecr, "api")

        rust.assert_not_called()
        image.assert_not_called()

    def test_an_unpublished_commit_builds(self) -> None:
        ecr = FakeEcr(published=[])

        _, image = self.build_feature(ecr, "ui")

        image.assert_called_once_with("ui")

    def test_a_build_only_reads_ecr(self) -> None:
        # Images carry only their commit tags; nothing retags after the push.
        for published in ([A], []):
            ecr = FakeEcr(published=published)

            self.build_feature(ecr, "ui")

            self.assertTrue(ecr.calls)
            self.assertTrue(all("batch-get-image" in call for call in ecr.calls))

    def test_an_unexpected_lookup_failure_stops_the_build(self) -> None:
        ecr = FakeEcr(published=[])
        ecr.failure_code = "KmsError"

        with self.assertRaises(SystemExit):
            self.build_feature(ecr, "ui")


class PublishingBuildTests(unittest.TestCase):
    def test_the_build_pushes_only_the_commit_tag(self) -> None:
        with mock.patch.dict(os.environ, github_main_env(), clear=True):
            builder = bundle_builds.BundleBuilder(["ui"])
            builder.buildx_builder_name = "test-builder"
            with (
                mock.patch.object(bundle_builds, "registry_ref_exists", return_value=True),
                mock.patch.object(bundle_builds, "run_buildx") as run_buildx,
            ):
                builder.build_space_bundle_image("ui")

        command = run_buildx.call_args.args[0]
        tags = [command[index + 1] for index, arg in enumerate(command) if arg == "--tag"]
        self.assertEqual(tags, [f"{REGISTRY}/space/ui:{A}"])
        self.assertTrue(run_buildx.call_args.kwargs["push"])

    def test_image_tag_and_runtime_revision_share_the_github_commit(self) -> None:
        with mock.patch.dict(os.environ, github_main_env(), clear=True):
            builder = bundle_builds.BundleBuilder(["api"])
            self.assertEqual(builder.space_bundle_image_tag(), A)
            self.assertEqual(builder.build_git_sha(), A)

    def test_rust_artifacts_stay_out_of_the_registry(self) -> None:
        with mock.patch.dict(os.environ, github_main_env(), clear=True):
            builder = bundle_builds.BundleBuilder(["api"])
            builder.buildx_builder_name = "test-builder"
            with (
                mock.patch.object(builder, "rust_sccache_build_options", return_value=[]),
                mock.patch.object(bundle_builds, "registry_ref_exists", return_value=False),
                mock.patch.object(builder, "image_tag_exists") as image_tag_exists,
                mock.patch.object(bundle_builds, "run_buildx_local_output") as local_output,
                mock.patch.object(bundle_builds, "run_buildx") as run_buildx,
            ):
                builder.build_rust_artifacts("api")
                builder.build_space_bundle_image("api")

            artifact_context = local_output.call_args.args[1]
            final_command = run_buildx.call_args.args[0]
            for temporary_directory in builder._temporary_directories:
                temporary_directory.cleanup()

        image_tag_exists.assert_not_called()
        self.assertEqual(run_buildx.call_count, 1)
        self.assertIn(f"space-rust-artifact-api={artifact_context}", final_command)
        self.assertFalse(any("rust-content-" in arg for arg in final_command))


class OidcTokenTests(unittest.TestCase):
    # Every job on main with `id-token: write` gets the subject the push role
    # trusts, so the grant stays on the jobs that are meant to hold it.
    # reference-deployment.yml runs its apply and addons jobs in an
    # environment, so their tokens carry an environment subject the push role
    # doesn't trust, but only while each job names that environment literally.
    PUBLISHING = {
        ("main-ci.yml", "artifacts"),
        ("artifact-ci.yml", "bundle_images"),
    }
    ENVIRONMENT_SCOPED = {
        ("reference-deployment.yml", "apply"): "reference-deployment",
        ("reference-deployment.yml", "addons"): "reference-deployment",
    }

    @staticmethod
    def grants_id_token(permissions: object) -> bool:
        if permissions == "write-all":
            return True
        return isinstance(permissions, dict) and permissions.get("id-token") == "write"

    def test_only_the_publishing_jobs_grant_an_oidc_token(self) -> None:
        import yaml

        workflows = sorted((REPO_ROOT / ".github/workflows").glob("*.y*ml"))
        self.assertTrue(workflows)
        granting: set[tuple[str, str]] = set()
        loaded: dict[str, dict] = {}
        for path in workflows:
            workflow = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            loaded[path.name] = workflow
            if self.grants_id_token(workflow.get("permissions")):
                granting.add((path.name, "<workflow>"))
            for name, job in (workflow.get("jobs") or {}).items():
                if self.grants_id_token((job or {}).get("permissions")):
                    granting.add((path.name, name))
        self.assertIn(("main-ci.yml", "artifacts"), granting)
        self.assertLessEqual(granting, self.PUBLISHING | set(self.ENVIRONMENT_SCOPED))

        for (workflow_name, job_name), environment in self.ENVIRONMENT_SCOPED.items():
            job = loaded[workflow_name]["jobs"][job_name]
            declared = job.get("environment")
            if isinstance(declared, dict):
                declared = declared.get("name")
            self.assertEqual(declared, environment, f"{workflow_name} {job_name}")

        # A called workflow's jobs inherit the caller job's grant unless the
        # called workflow narrows it, so the granting caller must call
        # artifact-ci, and artifact-ci must narrow it for its other jobs.
        caller = loaded["main-ci.yml"]["jobs"]["artifacts"]
        self.assertEqual(caller.get("uses"), "./.github/workflows/artifact-ci.yml")
        called = loaded["artifact-ci.yml"]
        self.assertIsNotNone(called.get("permissions"))
        self.assertFalse(self.grants_id_token(called.get("permissions")))


class ToolBootstrapTests(unittest.TestCase):
    def test_a_publishing_build_never_runs_an_installer(self) -> None:
        with mock.patch.dict(os.environ, github_main_env(), clear=True):
            builder = bundle_builds.BundleBuilder(["ui"])
            with (
                mock.patch.object(builder, "ensure_uv") as ensure_uv,
                self.assertRaises(SystemExit),
            ):
                builder.run_install_tools("aws-cli")

        ensure_uv.assert_not_called()

    def test_preinstalled_tools_are_used_as_they_are(self) -> None:
        with mock.patch.dict(os.environ, github_main_env(), clear=True):
            builder = bundle_builds.BundleBuilder(["ui"])
            with (
                mock.patch.object(bundle_builds.shutil, "which", return_value="/usr/bin/tool"),
                mock.patch.object(
                    bundle_builds,
                    "run",
                    return_value=subprocess.CompletedProcess([], 0, "", ""),
                ),
                mock.patch.object(builder, "run_install_tools") as install,
            ):
                builder.wait_for_docker()
                builder.ensure_aws_cli()

        install.assert_not_called()

    def test_non_publishing_and_local_builds_may_install_missing_tools(self) -> None:
        not_publishing = github_main_env()
        not_publishing["SPACE_BUNDLE_PUBLISH"] = "0"
        local = github_main_env()
        del local["GITHUB_ACTIONS"]
        for name, env in (("not publishing", not_publishing), ("local", local)):
            with (
                self.subTest(name),
                tempfile.TemporaryDirectory() as tmp_dir,
                mock.patch.dict(os.environ, env, clear=True),
            ):
                builder = bundle_builds.BundleBuilder(["ui"])
                with (
                    mock.patch.object(builder, "ensure_uv", return_value=f"{tmp_dir}/uv"),
                    mock.patch.object(bundle_builds, "run") as run,
                ):
                    builder.run_install_tools("aws-cli")

                self.assertIn("aws-cli", run.call_args.args[0])

    def test_a_publishing_build_does_not_fall_back_to_the_default_builder(self) -> None:
        with mock.patch.dict(os.environ, github_main_env(), clear=True):
            builder = bundle_builds.BundleBuilder(["ui"])
            with (
                mock.patch.object(
                    bundle_builds,
                    "run",
                    return_value=subprocess.CompletedProcess([], 1, "", "pull failed"),
                ),
                self.assertRaises(SystemExit),
            ):
                builder.setup_buildx_builder()

    def test_buildkit_runs_a_pinned_image(self) -> None:
        with mock.patch.dict(os.environ, github_main_env(), clear=True):
            builder = bundle_builds.BundleBuilder(["ui"])
            with mock.patch.object(
                bundle_builds,
                "run",
                return_value=subprocess.CompletedProcess([], 0, "", ""),
            ) as run:
                builder.setup_buildx_builder()

        create = run.call_args_list[0].args[0]
        image = create[create.index("--driver-opt") + 1]
        self.assertRegex(image, r"^image=moby/buildkit:v[0-9.]+@sha256:[0-9a-f]{64}$")



class FakeRegistry:
    """The cache repository's tags; every lookup is recorded."""

    def __init__(self, tags: list[str]) -> None:
        self.refs = {f"{CACHE_REPOSITORY}:{tag}" for tag in tags}
        self.lookups: list[str] = []

    def exists(self, ref: str) -> bool:
        self.lookups.append(ref)
        return ref in self.refs


class RegistryCacheTests(unittest.TestCase):
    CACHE_TO_SUFFIX = ",mode=max,oci-mediatypes=true,image-manifest=true,ignore-error=true"

    def build_image(self, env: dict[str, str], registry: FakeRegistry, feature: str = "ui"):
        with mock.patch.dict(os.environ, env, clear=True):
            builder = bundle_builds.BundleBuilder([feature])
            builder.buildx_builder_name = "test-builder"
            with (
                mock.patch.object(bundle_builds, "registry_ref_exists", side_effect=registry.exists),
                mock.patch.object(builder, "image_tag_exists") as ecr_lookup,
                mock.patch.object(bundle_builds, "run_buildx") as run_buildx,
            ):
                builder.build_space_bundle_image(feature)
        ecr_lookup.assert_not_called()
        return run_buildx.call_args.args[0]

    def test_main_imports_and_refreshes_the_cache_repository(self) -> None:
        ref = f"{CACHE_REPOSITORY}:ui-buildcache-image"

        command = self.build_image(github_main_env(), FakeRegistry(["ui-buildcache-image"]))

        self.assertEqual(option_values(command, "--cache-from"), [f"type=registry,ref={ref}"])
        self.assertEqual(
            option_values(command, "--cache-to"),
            [f"type=registry,ref={ref}{self.CACHE_TO_SUFFIX}"],
        )

    def test_main_writes_a_missing_cache_without_importing_it(self) -> None:
        command = self.build_image(github_main_env(), FakeRegistry([]))

        self.assertEqual(option_values(command, "--cache-from"), [])
        self.assertEqual(len(option_values(command, "--cache-to")), 1)

    def test_a_dispatched_run_only_reads_the_cache(self) -> None:
        registry = FakeRegistry(["api-buildcache-image"])

        command = self.build_image(github_dispatch_env(), registry, feature="api")

        self.assertEqual(
            option_values(command, "--cache-from"),
            [f"type=registry,ref={CACHE_REPOSITORY}:api-buildcache-image"],
        )
        self.assertEqual(option_values(command, "--cache-to"), [])
        self.assertFalse(any("type=gha" in arg for arg in command))

    def test_a_dispatched_run_needs_no_ecr(self) -> None:
        with mock.patch.dict(os.environ, github_dispatch_env(), clear=True):
            builder = bundle_builds.BundleBuilder(["ui"])
            with (
                mock.patch.object(builder, "wait_for_docker"),
                mock.patch.object(builder, "setup_buildx_builder"),
                mock.patch.object(builder, "build_space_bundle_feature") as build,
                mock.patch.object(builder, "ensure_ecr_login") as ecr_login,
            ):
                builder.execute()

        build.assert_called_once_with("ui")
        ecr_login.assert_not_called()

    def test_only_a_main_push_of_the_repository_writes_the_cache(self) -> None:
        for name, change in (
            ("pull request", {"GITHUB_EVENT_NAME": "pull_request"}),
            ("other branch", {"GITHUB_REF": "refs/heads/feature"}),
            ("fork", {"GITHUB_REPOSITORY": "someone/SPACE"}),
            ("outside GitHub Actions", {"GITHUB_ACTIONS": ""}),
        ):
            env = {**github_main_env(), **change}
            with (
                self.subTest(name),
                self.assertRaisesRegex(SystemExit, "Only main push CI in perplexityai/SPACE"),
            ):
                self.build_image(env, FakeRegistry([]))

    def test_the_cache_repository_must_be_untagged(self) -> None:
        for value in (f"{CACHE_REPOSITORY}:latest", f"{CACHE_REPOSITORY}@sha256:{'a' * 64}", "Ghcr.io/x/y", "cache"):
            env = {**github_dispatch_env(), "SPACE_BUNDLE_CACHE_REPOSITORY": value}
            with (
                self.subTest(value),
                self.assertRaisesRegex(SystemExit, "Invalid SPACE_BUNDLE_CACHE_REPOSITORY"),
            ):
                self.build_image(env, FakeRegistry([]))

    def rust_cache_options(self, env: dict[str, str], tags: list[str], digest: str = "a" * 64):
        registry = FakeRegistry(tags)
        with mock.patch.dict(os.environ, env, clear=True):
            builder = bundle_builds.BundleBuilder(["data-plane"])
            command: list[str] = []
            with (
                mock.patch.object(builder, "rust_dependency_digest", return_value=digest),
                mock.patch.object(bundle_builds, "registry_ref_exists", side_effect=registry.exists),
                mock.patch.object(builder, "image_tag_exists") as ecr_lookup,
            ):
                builder.append_rust_dependency_cache(command, "data-plane")
        ecr_lookup.assert_not_called()
        return option_values(command, "--cache-from"), option_values(command, "--cache-to")

    def test_main_refreshes_one_rolling_rust_cache(self) -> None:
        ref = f"{CACHE_REPOSITORY}:data-plane-buildcache-rust"
        for name, tags, cache_from in (
            ("missing", [], []),
            ("present", ["data-plane-buildcache-rust"], [f"type=registry,ref={ref}"]),
        ):
            with self.subTest(name):
                self.assertEqual(
                    self.rust_cache_options(github_main_env(), tags),
                    (cache_from, [f"type=registry,ref={ref}{self.CACHE_TO_SUFFIX}"]),
                )

    def test_a_dependency_change_still_imports_the_previous_rust_layers(self) -> None:
        tags = ["data-plane-buildcache-rust"]
        before = self.rust_cache_options(github_dispatch_env(), tags, digest="a" * 64)
        after = self.rust_cache_options(github_dispatch_env(), tags, digest="b" * 64)

        self.assertEqual(before, after)
        self.assertEqual(
            after, ([f"type=registry,ref={CACHE_REPOSITORY}:data-plane-buildcache-rust"], [])
        )

    def test_a_main_build_warns_when_its_cache_was_not_written(self) -> None:
        command = ["--cache-to", f"type=registry,ref={CACHE_REPOSITORY}:ui-buildcache-image"]
        for name, written, warned in (("written", True, False), ("missing", False, True)):
            with (
                self.subTest(name),
                mock.patch.dict(os.environ, github_main_env(), clear=True),
                mock.patch.object(bundle_builds, "registry_ref_exists", return_value=written),
                mock.patch("builtins.print") as printed,
            ):
                bundle_builds.BundleBuilder(["ui"]).check_cache_written(command, "ui", "image")
            messages = [str(call.args[0]) for call in printed.call_args_list]
            self.assertEqual(any(m.startswith("::warning::") for m in messages), warned)

    def test_an_unreachable_registry_is_a_cache_miss(self) -> None:
        for name, outcome in (
            ("timeout", subprocess.TimeoutExpired(["docker"], 60)),
            ("error", subprocess.CompletedProcess([], 1, "", "dial tcp: i/o timeout")),
        ):
            with (
                self.subTest(name),
                mock.patch.object(bundle_builds, "run", side_effect=[outcome]) as run,
            ):
                self.assertFalse(bundle_builds.registry_ref_exists(f"{CACHE_REPOSITORY}:ui"))
            self.assertEqual(
                run.call_args.kwargs["timeout"], bundle_builds.REGISTRY_LOOKUP_TIMEOUT_SECONDS
            )


class EcrLoginRetryTests(unittest.TestCase):
    TLS_TIMEOUT = 'Error response from daemon: Get "https://registry/v2/": net/http: TLS handshake timeout\n'

    def log_in(self, outcomes: list[tuple[int, str]]):
        calls: list[list[str]] = []

        def fake_run(args, **kwargs):
            calls.append(list(args))
            if "get-login-password" in args:
                return subprocess.CompletedProcess(args, 0, "password", "")
            returncode, stderr = outcomes.pop(0)
            return subprocess.CompletedProcess(args, returncode, "", stderr)

        with mock.patch.dict(os.environ, github_main_env(), clear=True):
            builder = bundle_builds.BundleBuilder(["ui"])
            with (
                mock.patch.object(builder, "prepare_aws_environment"),
                mock.patch.object(bundle_builds, "run", side_effect=fake_run),
                mock.patch.object(bundle_builds.time, "sleep") as sleep,
            ):
                try:
                    builder.ensure_ecr_login()
                finally:
                    logins = [call for call in calls if call[:2] == ["docker", "login"]]
        return builder, logins, sleep

    def test_a_transient_failure_is_retried_with_backoff(self) -> None:
        builder, logins, sleep = self.log_in([(1, self.TLS_TIMEOUT), (1, self.TLS_TIMEOUT), (0, "")])

        self.assertEqual(len(logins), 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [5, 10])
        self.assertEqual(builder.ecr_logged_in_hosts, {REGISTRY})

    def test_an_authentication_failure_is_not_retried(self) -> None:
        refused = "Error response from daemon: login attempt to https://registry/v2/ failed with status: 400 Bad Request\n"
        with self.assertRaises(subprocess.CalledProcessError):
            self.log_in([(1, refused), (0, "")])

    def test_a_hung_call_is_a_failed_attempt(self) -> None:
        hung = subprocess.TimeoutExpired(["docker", "login"], 30)
        outcomes = [hung, subprocess.CompletedProcess([], 0, "", "")]

        def fake_run(args, **kwargs):
            self.assertEqual(kwargs["timeout"], bundle_builds.REGISTRY_COMMAND_TIMEOUT_SECONDS)
            outcome = outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

        with (
            mock.patch.object(bundle_builds, "run", side_effect=fake_run),
            mock.patch.object(bundle_builds.time, "sleep") as sleep,
        ):
            bundle_builds.run_registry_command(["docker", "login"], description="Logging in")

        self.assertEqual(outcomes, [])
        sleep.assert_called_once_with(bundle_builds.ECR_LOGIN_INITIAL_DELAY_SECONDS)

    def test_retries_are_bounded(self) -> None:
        attempts = bundle_builds.ECR_LOGIN_MAX_ATTEMPTS
        with self.assertRaises(SystemExit) as raised:
            self.log_in([(1, self.TLS_TIMEOUT)] * attempts)

        self.assertEqual(raised.exception.code, bundle_builds.ECR_TRANSIENT_ERROR_EXIT_STATUS)


class BuildxPushRetryTests(unittest.TestCase):
    """Runs a stand-in for `docker buildx build` that fails `failures` times,
    or for as long as its arguments still carry a cache import or a mirror
    build context, with `source_error`, and records each attempt's arguments."""

    def run_buildx(
        self,
        error: str,
        failures: int,
        *,
        source_error: str = "",
        success_log: str = "",
        args: tuple = (),
        published=None,
    ):
        with tempfile.TemporaryDirectory() as tmp_dir:
            calls = Path(tmp_dir) / "calls"
            script = (
                "import json, pathlib, sys\n"
                f"calls = pathlib.Path({str(calls)!r})\n"
                "with calls.open('a') as f: f.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                "attempt = len(calls.read_text().splitlines())\n"
                "sources = any(a == '--cache-from' or 'docker-image://' in a for a in sys.argv)\n"
                f"if {source_error!r} and sources:\n"
                f"    sys.stderr.write({source_error!r} + '\\n')\n"
                "    sys.exit(1)\n"
                f"if attempt <= {failures}:\n"
                f"    sys.stderr.write({error!r} + '\\n')\n"
                "    sys.exit(1)\n"
                f"sys.stderr.write({success_log!r})\n"
            )
            self.stdout = io.StringIO()
            with (
                mock.patch.object(bundle_builds.time, "sleep"),
                mock.patch.object(sys, "stderr", new=tempfile.TemporaryFile("w+")),
                contextlib.redirect_stdout(self.stdout),
            ):
                try:
                    bundle_builds.run_buildx_command(
                        [sys.executable, "-c", script, *args], published=published
                    )
                finally:
                    self.calls = [json.loads(line) for line in calls.read_text().splitlines()]
        return len(self.calls)

    def test_a_dropped_push_is_retried(self) -> None:
        error = (
            f"ERROR: failed to solve: failed to push {REGISTRY}/space/ui:{A}: "
            'failed to do request: Head "https://registry/v2/": net/http: TLS handshake timeout'
        )

        self.assertEqual(self.run_buildx(error, failures=1), 2)

    def test_a_refused_push_is_not_retried(self) -> None:
        error = (
            f"ERROR: failed to solve: failed to push {REGISTRY}/space/ui:{A}: "
            "unexpected status from PUT request: 403 Forbidden"
        )
        with self.assertRaises(subprocess.CalledProcessError):
            self.run_buildx(error, failures=1)

    def test_a_push_that_reached_the_registry_is_not_repeated(self) -> None:
        error = (
            f"ERROR: failed to solve: failed to push {REGISTRY}/space/ui:{A}: "
            "unexpected status: 502 Bad Gateway"
        )

        self.assertEqual(self.run_buildx(error, failures=5, published=lambda: True), 1)

    def test_paths_in_the_command_are_accepted(self) -> None:
        self.assertEqual(self.run_buildx("", failures=0, args=(Path("/context"),)), 1)

    READ_ERROR = (
        "ERROR: failed to build: failed to solve: failed to copy: httpReadSeeker: "
        f"failed open: could not fetch content descriptor sha256:{'c' * 64} "
        "(application/vnd.oci.image.layer.v1.tar+gzip) from remote: not found"
    )
    # As BuildKit reports a cache layer it could not read while pushing.
    PUSH_READ_ERROR = (
        f"ERROR: failed to build: failed to solve: failed to push {REGISTRY}/space/ui:{A}: "
        "failed to copy: httpReadSeeker: failed open: could not fetch content "
        f"descriptor sha256:{'c' * 64} (application/vnd.oci.image.layer.v1.tar+gzip) "
        "from remote: not found"
    )
    CACHE_TO = f"type=registry,ref={CACHE_REPOSITORY}:ui-buildcache-image,mode=max"
    MIRROR_CONTEXT = (
        "--build-context=public.ecr.aws/docker/library/debian:bookworm-slim="
        f"docker-image://ghcr.io/example/mirror@sha256:{'d' * 64}"
    )
    LOCAL_CONTEXT = ("--build-context", "space-rust-artifact-api=/tmp/artifacts")
    SOURCES = (
        "--cache-from", f"type=registry,ref={CACHE_REPOSITORY}:ui-buildcache-image",
        "--cache-to", CACHE_TO, MIRROR_CONTEXT, *LOCAL_CONTEXT,
    )

    def test_a_damaged_cache_or_mirror_falls_back_to_the_sources(self) -> None:
        attempts = self.run_buildx("", failures=0, source_error=self.READ_ERROR, args=self.SOURCES)

        self.assertEqual(attempts, 2)
        retry = self.calls[1]
        self.assertNotIn("--cache-from", retry)
        self.assertFalse(any("docker-image://" in arg for arg in retry))
        # The export stays, so a rebuild on main replaces the damaged cache, and
        # local contexts such as the Rust artifacts stay too.
        self.assertEqual(option_values(retry, "--cache-to"), [self.CACHE_TO])
        self.assertEqual(option_values(retry, "--build-context"), [self.LOCAL_CONTEXT[1]])
        self.assertIn("::warning::", self.stdout.getvalue())

    def test_the_fallback_drops_registry_sources_in_either_spelling(self) -> None:
        mirror = f"base=docker-image://ghcr.io/example/mirror@sha256:{'d' * 64}"
        command = [
            "build", "--cache-from", "type=registry,ref=a", "--cache-from=type=registry,ref=b",
            "--build-context", mirror, f"--build-context={mirror}",
            *self.LOCAL_CONTEXT, "--cache-to", self.CACHE_TO, ".",
        ]

        self.assertEqual(
            bundle_builds.without_external_sources(command),
            ["build", *self.LOCAL_CONTEXT, "--cache-to", self.CACHE_TO, "."],
        )

    MIRROR_METADATA_FAILURE = (
        "ERROR: failed to build: failed to solve: public.ecr.aws/docker/library/debian:bookworm-slim: "
        f"failed to resolve source metadata for ghcr.io/example/mirror@sha256:{'d' * 64}: {{}}"
    )

    def test_a_mirror_metadata_failure_falls_back_to_the_sources(self) -> None:
        manifest = f"https://ghcr.io/v2/example/mirror/manifests/sha256:{'d' * 64}"
        for name, cause in (
            ("TLS timeout", f'failed to do request: Head "{manifest}": net/http: TLS handshake timeout'),
            ("rate limit", f"unexpected status from HEAD request to {manifest}: 429 Too Many Requests"),
        ):
            with self.subTest(name):
                attempts = self.run_buildx(
                    "", failures=0,
                    source_error=self.MIRROR_METADATA_FAILURE.format(cause),
                    args=(self.MIRROR_CONTEXT,),
                )
                # Once with the mirror, once without: not five times with it.
                self.assertEqual(attempts, 2)
                self.assertFalse(any("docker-image://" in arg for arg in self.calls[1]))

    def test_errors_name_external_sources_by_reference_and_url(self) -> None:
        command = [
            "--cache-from", f"type=registry,ref={CACHE_REPOSITORY}:ui-buildcache-image",
            self.MIRROR_CONTEXT, *self.LOCAL_CONTEXT, "--cache-to", self.CACHE_TO,
        ]

        self.assertEqual(
            bundle_builds.external_source_names(command),
            [
                CACHE_REPOSITORY, "ghcr.io/v2/example/space-bundle-cache/",
                "ghcr.io/example/mirror", "ghcr.io/v2/example/mirror/",
            ],
        )

    def test_a_damaged_mirror_alone_falls_back_to_the_sources(self) -> None:
        attempts = self.run_buildx(
            "", failures=0, source_error=self.READ_ERROR, args=(self.MIRROR_CONTEXT,)
        )

        self.assertEqual(attempts, 2)

    def test_a_cache_layer_unreadable_at_push_falls_back(self) -> None:
        attempts = self.run_buildx(
            "", failures=0, source_error=self.PUSH_READ_ERROR, args=self.SOURCES
        )

        self.assertEqual(attempts, 2)

    def test_a_push_unreadable_but_stored_is_not_rebuilt(self) -> None:
        attempts = self.run_buildx(
            "", failures=0, source_error=self.PUSH_READ_ERROR, args=self.SOURCES,
            published=lambda: True,
        )

        self.assertEqual(attempts, 1)

    def test_a_read_failure_without_external_sources_still_fails(self) -> None:
        with self.assertRaises(subprocess.CalledProcessError):
            self.run_buildx(self.READ_ERROR, failures=1)
        self.assertEqual(len(self.calls), 1)

    def test_a_failed_build_step_with_external_sources_still_fails(self) -> None:
        error = 'ERROR: failed to build: failed to solve: process "/bin/sh -c false" did not complete successfully: exit code: 1'
        with self.assertRaises(subprocess.CalledProcessError):
            self.run_buildx(error, failures=1, args=self.SOURCES)
        self.assertEqual(len(self.calls), 1)

    def test_a_failed_cache_export_is_reported(self) -> None:
        log = (
            "#13 exporting cache to registry\n"
            "#13 preparing build cache for export 0.2s done\n"
            "#13 ERROR: error writing layer blob: failed to do request: connection refused\n"
        )
        self.assertEqual(self.run_buildx("", failures=0, success_log=log), 1)
        self.assertIn("::warning::Exporting the layer cache failed", self.stdout.getvalue())
        self.assertIn("error writing layer blob", self.stdout.getvalue())

    def test_a_successful_export_is_quiet(self) -> None:
        log = "#13 exporting cache to registry\n#13 DONE 1.0s\n#14 ERROR: unrelated\n"
        self.run_buildx("", failures=0, success_log=log)
        self.assertNotIn("::warning::", self.stdout.getvalue())

    def test_a_failed_build_step_is_not_retried(self) -> None:
        error = "ERROR: failed to solve: process \"/bin/sh -c apt-get update\" did not complete successfully: exit code: 100"
        with self.assertRaises(subprocess.CalledProcessError):
            self.run_buildx(error, failures=1)


class BuildkitMirrorTests(unittest.TestCase):
    MIRROR_DIGEST = "sha256:" + "e" * 64
    MIRRORED = f"ghcr.io/example/mirror@{MIRROR_DIGEST}"

    def setup_builder(self, env: dict[str, str], failing_images: set[str]) -> list[str]:
        images: list[str] = []
        current: list[str] = []

        def fake_run(args, **kwargs):
            args = list(args)
            if args[:3] == ["docker", "buildx", "create"]:
                current[:] = [args[args.index("--driver-opt") + 1].removeprefix("image=")]
                images.append(current[0])
            if args[:3] == ["docker", "buildx", "inspect"] and current[0] in failing_images:
                return subprocess.CompletedProcess(args, 1, "", "manifest unknown")
            return subprocess.CompletedProcess(args, 0, "", "")

        with tempfile.TemporaryDirectory() as tmp_dir:
            listing = Path(tmp_dir) / "mirrored-images.txt"
            listing.write_text(
                "# comment\n"
                f"{bundle_builds.BUILDKIT_IMAGE} {self.MIRROR_DIGEST} ghcr.io/example/mirror:buildkit\n"
            )
            env = {**env, "SPACE_MIRRORED_IMAGES": str(listing)}
            with mock.patch.dict(os.environ, env, clear=True):
                builder = bundle_builds.BundleBuilder(["ui"])
                with mock.patch.object(bundle_builds, "run", side_effect=fake_run):
                    builder.setup_buildx_builder()
                self.builder = builder
        return images

    def test_github_ci_takes_buildkit_from_the_mirror(self) -> None:
        env = {**github_main_env(), "SPACE_CI_IMAGE_MIRROR": "ghcr"}

        self.assertEqual(self.setup_builder(env, failing_images=set()), [self.MIRRORED])
        self.assertTrue(self.builder.buildx_builder_name)

    def test_a_missing_mirror_falls_back_to_the_source_digest(self) -> None:
        env = {**github_main_env(), "SPACE_CI_IMAGE_MIRROR": "ghcr"}

        images = self.setup_builder(env, failing_images={self.MIRRORED})

        self.assertEqual(images, [self.MIRRORED, bundle_builds.BUILDKIT_IMAGE])
        self.assertTrue(self.builder.buildx_builder_name)
        self.assertFalse(self.builder.disable_external_cache)

    def test_a_publishing_build_fails_when_neither_starts(self) -> None:
        env = {**github_main_env(), "SPACE_CI_IMAGE_MIRROR": "ghcr"}
        with self.assertRaises(SystemExit):
            self.setup_builder(env, failing_images={self.MIRRORED, bundle_builds.BUILDKIT_IMAGE})

    def test_a_non_publishing_build_falls_back_to_the_default_builder(self) -> None:
        env = {**github_dispatch_env(), "SPACE_CI_IMAGE_MIRROR": "ghcr"}

        self.setup_builder(env, failing_images={self.MIRRORED, bundle_builds.BUILDKIT_IMAGE})

        self.assertEqual(self.builder.buildx_builder_name, "")
        self.assertTrue(self.builder.disable_external_cache)

    def test_a_stalled_mirror_pull_moves_on_to_the_source(self) -> None:
        env = {**github_main_env(), "SPACE_CI_IMAGE_MIRROR": "ghcr"}
        images: list[str] = []

        def fake_run(args, **kwargs):
            args = list(args)
            self.assertIsNotNone(kwargs.get("timeout"))
            if args[:3] == ["docker", "buildx", "create"]:
                images.append(args[args.index("--driver-opt") + 1].removeprefix("image="))
            if args[:3] == ["docker", "buildx", "inspect"] and images[-1] == self.MIRRORED:
                raise subprocess.TimeoutExpired(args, kwargs["timeout"])
            return subprocess.CompletedProcess(args, 0, "", "")

        with tempfile.TemporaryDirectory() as tmp_dir:
            listing = Path(tmp_dir) / "mirrored-images.txt"
            listing.write_text(
                f"{bundle_builds.BUILDKIT_IMAGE} {self.MIRROR_DIGEST} ghcr.io/example/mirror:buildkit\n"
            )
            with mock.patch.dict(os.environ, {**env, "SPACE_MIRRORED_IMAGES": str(listing)}, clear=True):
                builder = bundle_builds.BundleBuilder(["ui"])
                with mock.patch.object(bundle_builds, "run", side_effect=fake_run):
                    builder.setup_buildx_builder()

        self.assertEqual(images, [self.MIRRORED, bundle_builds.BUILDKIT_IMAGE])
        self.assertTrue(builder.buildx_builder_name)

    def test_each_image_gets_a_builder_name_of_its_own(self) -> None:
        # Docker as it behaves when a create times out after registering the
        # builder and its removal fails too: that name stays taken.
        env = {**github_main_env(), "SPACE_CI_IMAGE_MIRROR": "ghcr"}
        builders: set[str] = set()
        removed: list[str] = []

        def fake_run(args, **kwargs):
            args = list(args)
            if args[:3] == ["docker", "buildx", "create"]:
                name = args[args.index("--name") + 1]
                if name in builders:
                    return subprocess.CompletedProcess(args, 1, "", "existing instance")
                builders.add(name)
                if self.MIRRORED in args[args.index("--driver-opt") + 1]:
                    raise subprocess.TimeoutExpired(args, kwargs["timeout"])
            if args[:3] == ["docker", "buildx", "rm"]:
                removed.append(args[-1])
                if len(removed) == 1:
                    return subprocess.CompletedProcess(args, 1, "", "daemon busy")
                builders.discard(args[-1])
            return subprocess.CompletedProcess(args, 0, "", "")

        with tempfile.TemporaryDirectory() as tmp_dir:
            listing = Path(tmp_dir) / "mirrored-images.txt"
            listing.write_text(
                f"{bundle_builds.BUILDKIT_IMAGE} {self.MIRROR_DIGEST} ghcr.io/example/mirror:buildkit\n"
            )
            with (
                mock.patch.dict(os.environ, {**env, "SPACE_MIRRORED_IMAGES": str(listing)}, clear=True),
                mock.patch.object(bundle_builds, "run", side_effect=fake_run),
            ):
                builder = bundle_builds.BundleBuilder(["ui"])
                builder.setup_buildx_builder()
                started = builder.buildx_builder_name
                builder.cleanup_buildx_builder()

        self.assertTrue(started)
        self.assertNotEqual(removed[0], started)
        self.assertEqual(builders, set())

    def test_a_publishing_build_removes_its_failed_builders_before_exiting(self) -> None:
        env = {**github_main_env(), "SPACE_CI_IMAGE_MIRROR": "ghcr"}
        builders: set[str] = set()
        rm_calls: list[str] = []

        def fake_run(args, **kwargs):
            args = list(args)
            if args[:3] == ["docker", "buildx", "create"]:
                builders.add(args[args.index("--name") + 1])
            if args[:3] == ["docker", "buildx", "inspect"]:
                return subprocess.CompletedProcess(args, 1, "", "pull failed")
            if args[:3] == ["docker", "buildx", "rm"]:
                rm_calls.append(args[-1])
                # Each builder's first removal fails, as when the daemon is busy.
                if rm_calls.count(args[-1]) == 1:
                    return subprocess.CompletedProcess(args, 1, "", "daemon busy")
                builders.discard(args[-1])
            return subprocess.CompletedProcess(args, 0, "", "")

        with tempfile.TemporaryDirectory() as tmp_dir:
            listing = Path(tmp_dir) / "mirrored-images.txt"
            listing.write_text(
                f"{bundle_builds.BUILDKIT_IMAGE} {self.MIRROR_DIGEST} ghcr.io/example/mirror:buildkit\n"
            )
            with (
                mock.patch.dict(os.environ, {**env, "SPACE_MIRRORED_IMAGES": str(listing)}, clear=True),
                mock.patch.object(bundle_builds, "run", side_effect=fake_run),
            ):
                builder = bundle_builds.BundleBuilder(["ui"])
                with (
                    mock.patch.object(builder, "wait_for_docker"),
                    mock.patch.object(builder, "build_space_bundle_feature") as build,
                    self.assertRaisesRegex(SystemExit, "Couldn't start the pinned BuildKit builder"),
                ):
                    builder.execute()

        build.assert_not_called()
        self.assertEqual(len(set(rm_calls)), 2)
        self.assertEqual(builders, set())

    def test_builds_outside_github_ci_use_the_source(self) -> None:
        self.assertEqual(
            self.setup_builder(github_main_env(), failing_images=set()),
            [bundle_builds.BUILDKIT_IMAGE],
        )


class MirrorLookupTests(unittest.TestCase):
    def test_a_hung_mirror_lookup_builds_from_the_from_images(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            hanging = Path(tmp_dir) / "mirror-images.sh"
            hanging.write_text("exec sleep 30\n")
            stdout = io.StringIO()
            with (
                mock.patch.dict(os.environ, {"SPACE_CI_IMAGE_MIRROR": "ghcr"}),
                mock.patch.object(bundle_builds, "MIRROR_IMAGES_SCRIPT", hanging),
                mock.patch.object(bundle_builds, "MIRROR_LOOKUP_TIMEOUT_SECONDS", 1),
                contextlib.redirect_stdout(stdout),
            ):
                contexts = bundle_builds.BundleBuilder.mirrored_base_image_contexts(
                    ["docker", "buildx", "build", "--file", "Dockerfile"]
                )

        self.assertEqual(contexts, [])
        self.assertIn("::warning::Looking up the mirrored bases of Dockerfile timed out", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
