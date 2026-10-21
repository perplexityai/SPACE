"""Exercise the artifact build's cache policy at its Docker CLI interface."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "infra/space/kind/scripts/build-rust-artifacts.sh"
REPOSITORY = "ghcr.io/perplexityai/space-kind-build-cache"
BUILDS = (
    (
        "space-rust-chef-base:local",
        "docker/Dockerfile.rust-build-common",
        "chef-base",
        "kind-rust-chef-base",
    ),
    (
        "space-grpc-health-probe:local",
        "docker/Dockerfile.rust-build-common",
        "grpc-health-probe-downloader",
        "kind-grpc-health-probe",
    ),
    (
        "space-rust-build-kind:local",
        "docker/Dockerfile.rust-build-kind",
        None,
        "kind-rust-build",
    ),
)


class RegistryCacheTests(unittest.TestCase):
    def run_build(self, overrides):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            calls_file = directory / "calls.jsonl"
            docker = directory / "docker"
            docker.write_text(
                f"#!{sys.executable}\n"
                "import json, os, sys\n"
                "with open(os.environ['DOCKER_CALLS'], 'a') as calls:\n"
                "    calls.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                "sys.exit(7 if os.environ.get('FAIL_BUILD') in sys.argv else 0)\n"
            )
            docker.chmod(0o755)
            environment = {
                key: value
                for key, value in os.environ.items()
                if not key.startswith(
                    ("GITHUB_", "ACTIONS_", "SPACE_KIND_BUILD_CACHE_")
                )
            }
            environment.update(
                PATH=f"{directory}:{os.environ['PATH']}",
                SPACE_KIND_RUST_ARTIFACTS="docker",
                SPACE_TILT_STATE_DIR=str(directory / "state"),
                DOCKER_CALLS=str(calls_file),
            )
            environment.update(overrides)
            result = subprocess.run(
                ["bash", str(SCRIPT)],
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
            calls = (
                [json.loads(line) for line in calls_file.read_text().splitlines()]
                if calls_file.exists()
                else []
            )
            return result, calls

    def test_local_pr_and_main_builds_have_exact_cache_arguments(self):
        for mode in ("local", "pr", "pr-registry-only", "main"):
            with self.subTest(mode=mode):
                environment = {}
                if mode != "local":
                    environment = {
                        "SPACE_KIND_BUILD_CACHE_REPOSITORY": REPOSITORY,
                        "GITHUB_ACTIONS": "true",
                        "GITHUB_REPOSITORY": "perplexityai/SPACE",
                        "GITHUB_EVENT_NAME": "push"
                        if mode == "main"
                        else "pull_request",
                        "GITHUB_REF": "refs/heads/main"
                        if mode == "main"
                        else "refs/pull/73/merge",
                    }
                if mode == "main":
                    environment["SPACE_KIND_BUILD_CACHE_WRITE"] = "1"
                if mode in ("pr", "main"):
                    environment["ACTIONS_RUNTIME_TOKEN"] = "test-runtime-token"
                result, calls = self.run_build(environment)
                self.assertEqual(result.returncode, 0, result.stderr)
                expected = []
                for image, dockerfile, target, scope in BUILDS:
                    command = ["build", "-t", image, "-f", dockerfile]
                    if target:
                        command += ["--target", target]
                    if mode != "local":
                        command += [
                            "--cache-from",
                            f"type=registry,ref={REPOSITORY}:{scope}",
                        ]
                    if mode in ("pr", "main"):
                        command += [
                            "--cache-from",
                            f"type=gha,scope={scope},timeout=60s",
                        ]
                    if mode == "main":
                        exports = [
                            "--cache-to",
                            f"type=registry,ref={REPOSITORY}:{scope},mode=max,image-manifest=true,oci-mediatypes=true,ignore-error=true",
                        ]
                        if scope == "kind-rust-build":
                            # Only dependency layers may be published. The
                            # final artifact keeps cache imports but no export.
                            expected.append([
                                "build", "-f", dockerfile, "--target", "cook",
                                "--output", "type=cacheonly",
                                *command[5:], *exports, ".",
                            ])
                        else:
                            command += exports
                    if scope == "kind-rust-build":
                        # One manifest, and no input key label: these builds
                        # take no base from the mirror.
                        command[5:5] = ["--provenance=false"]
                    expected.append([*command, "."])
                self.assertEqual(calls, expected)

    def test_invalid_configuration_and_untrusted_writes_never_reach_docker(self):
        main = {
            "SPACE_KIND_BUILD_CACHE_REPOSITORY": REPOSITORY,
            "SPACE_KIND_BUILD_CACHE_WRITE": "1",
            "GITHUB_ACTIONS": "true",
            "GITHUB_REPOSITORY": "perplexityai/SPACE",
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_REF": "refs/heads/main",
        }
        cases = (
            {"GITHUB_EVENT_NAME": "pull_request"},
            {"GITHUB_EVENT_NAME": "workflow_dispatch"},
            {"GITHUB_REF": "refs/heads/feature"},
            {"GITHUB_REPOSITORY": "contributor/SPACE"},
            {"GITHUB_ACTIONS": "false"},
            {"SPACE_KIND_BUILD_CACHE_WRITE": "yes"},
            {"SPACE_KIND_BUILD_CACHE_REPOSITORY": ""},
            {"SPACE_KIND_BUILD_CACHE_REPOSITORY": REPOSITORY + ",mode=max"},
        )
        for override in cases:
            with self.subTest(override=override):
                result, calls = self.run_build(main | override)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("[rust-artifacts] ERROR:", result.stderr)
                self.assertEqual(calls, [])

    def test_optional_cache_does_not_mask_a_real_build_failure(self):
        result, calls = self.run_build(
            {
                "SPACE_KIND_BUILD_CACHE_REPOSITORY": REPOSITORY,
                "FAIL_BUILD": "space-grpc-health-probe:local",
            }
        )
        self.assertEqual(result.returncode, 7)
        self.assertEqual(len(calls), 2)
        self.assertNotIn("ready:", result.stdout)


if __name__ == "__main__":
    unittest.main()
