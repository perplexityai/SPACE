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

    def test_local_pr_and_main_builds_follow_cache_authorization(self):
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
                builds = [call for call in calls if call[0] == "build"]
                self.assertTrue(builds)
                final = next(call for call in builds if "space-rust-build-kind:local" in call)
                # Artifacts depend on the whole source revision; only reusable
                # dependency layers may be exported, even in trusted main CI.
                self.assertNotIn("--cache-to", final)
                exports = [call for call in builds if "--cache-to" in call]
                self.assertEqual(bool(exports), mode == "main")
                for call in builds:
                    imports = [call[i + 1] for i, arg in enumerate(call) if arg == "--cache-from"]
                    self.assertEqual(any(value.startswith(f"type=registry,ref={REPOSITORY}:") for value in imports), mode != "local")
                    self.assertEqual(any(value.startswith("type=gha,") for value in imports), mode in ("pr", "main"))
                if mode == "main":
                    dependency = next(call for call in exports if "type=cacheonly" in call)
                    self.assertEqual(dependency[dependency.index("--target") + 1], "cook")


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
