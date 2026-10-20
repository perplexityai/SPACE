#!/usr/bin/env python3
"""Build the canonical seed chain through the authenticated Admin API."""

from __future__ import annotations

import base64
import json
import os
import re
import socket
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias

JsonValue: TypeAlias = (
    None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
)

MIB = 1024 * 1024
TERMINAL_BUILD_STATES = {"succeeded", "failed", "publish_failed"}
TERMINAL_OPERATION_STATES = {"succeeded", "failed", "canceled", "expired"}
FULL_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
TEMPLATE_NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,62}\Z")
PACKAGE_MANAGERS = {
    "apt-install": "apt",
    "pip-install": "pip",
    "npm-install": "npm",
    "npm-install-global": "npm_global",
}


class SmokeFailure(RuntimeError):
    def __init__(self, phase: str, message: str, request_id: str = "") -> None:
        super().__init__(message)
        self.phase = phase
        self.request_id = request_id


class TransientReadFailure(SmokeFailure):
    pass


@dataclass(frozen=True)
class Response:
    body: JsonValue
    request_id: str
    status: int


class JsonClient:
    def __init__(self, root: str, token: str = "", timeout: float = 30) -> None:
        self.root = root.rstrip("/")
        self.token = token
        self.timeout = timeout

    def request(
        self,
        method: str,
        path: str,
        *,
        body: JsonValue = None,
        headers: dict[str, str] | None = None,
        expected: tuple[int, ...] = (200,),
        phase: str,
        transient_read: bool = False,
    ) -> Response:
        data = None
        request_headers = {"Accept": "application/json"}
        if self.token:
            request_headers["Authorization"] = f"Bearer {self.token}"
        if body is not None:
            data = json.dumps(body, separators=(",", ":")).encode()
            request_headers["Content-Type"] = "application/json"
        if headers:
            request_headers.update(headers)
        request = urllib.request.Request(
            f"{self.root}{path}", data=data, headers=request_headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
                parsed = json.loads(raw) if raw else None
                result = Response(
                    parsed,
                    response.headers.get("x-request-id", ""),
                    response.status,
                )
        except urllib.error.HTTPError as error:
            request_id = error.headers.get("x-request-id", "")
            raw = error.read()
            try:
                payload = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                payload = {}
            if error.code in expected:
                return Response(payload, request_id, error.code)
            message = (
                payload.get("message") or payload.get("error") or "HTTP request failed"
            )
            failure = SmokeFailure(phase, f"HTTP {error.code}: {message}", request_id)
            if transient_read and (error.code == 429 or error.code >= 500):
                raise TransientReadFailure(phase, str(failure), request_id) from error
            raise failure from error
        except (TimeoutError, urllib.error.URLError, socket.timeout) as error:
            failure_type = TransientReadFailure if transient_read else SmokeFailure
            raise failure_type(
                phase, f"transport failure: {type(error).__name__}"
            ) from error
        except json.JSONDecodeError as error:
            raise SmokeFailure(phase, "response was not valid JSON") from error

        if result.status not in expected:
            raise SmokeFailure(
                phase,
                f"unexpected HTTP {result.status}",
                result.request_id,
            )
        return result


def require_object(value: JsonValue, phase: str) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise SmokeFailure(phase, "response was not a JSON object")
    return value


def require_list(value: JsonValue, phase: str) -> list[JsonValue]:
    if not isinstance(value, list):
        raise SmokeFailure(phase, "response was not a JSON array")
    return value


def resolve_env_refs(value: str) -> str:
    """Match the canonical TOML adapter's ASCII `$NAME` substitution."""
    result: list[str] = []
    index = 0
    while index < len(value):
        if value[index] != "$":
            result.append(value[index])
            index += 1
            continue
        end = index + 1
        while (
            end < len(value)
            and value[end].isascii()
            and (value[end].isalnum() or value[end] == "_")
        ):
            end += 1
        if end == index + 1:
            result.append("$")
            index += 1
            continue
        result.append(os.environ.get(value[index + 1 : end], ""))
        index = end
    return "".join(result)


def read_copy_source(recipe_path: Path, source: str) -> bytes:
    source_path = Path(source)
    if not source_path.is_absolute():
        source_path = recipe_path.parent / source_path
    source_path = source_path.resolve(strict=True)
    # The builder writes inline bytes to dest as one file; only an uploaded
    # blob can carry a directory.
    if not source_path.is_file():
        raise SmokeFailure("render_recipe", f"copy source {source} is not a file")
    return source_path.read_bytes()


def render_step(
    recipe_path: Path, step: dict[str, JsonValue], index: int
) -> dict[str, JsonValue]:
    kind = step["type"]
    rendered: dict[str, JsonValue] = {
        "display_name": step.get("name"),
        "env_refs": step.get("env_refs", []),
    }
    if kind in PACKAGE_MANAGERS:
        rendered["package_install"] = {
            "manager": PACKAGE_MANAGERS[kind],
            "packages": step["packages"],
            "extra_args": step.get("extra_args", []),
        }
    elif kind == "run":
        rendered["run"] = {
            "command": step["command"],
            "user": step.get("user", "root"),
        }
    elif kind == "env":
        rendered["env"] = {
            "vars": {
                key: resolve_env_refs(value) for key, value in step["vars"].items()
            }
        }
    elif kind == "copy":
        rendered["copy"] = {
            "inline_bytes_base64": base64.b64encode(
                read_copy_source(recipe_path, step["source"])
            ).decode("ascii"),
            "dest": step["dest"],
        }
    elif kind == "playwright-install":
        rendered["playwright_install"] = {
            "browsers": step.get("browsers", ["chromium"]),
        }
    else:
        raise SmokeFailure(
            "render_recipe", f"step {index} has unsupported type {kind!r}"
        )
    return rendered


def render_build_request(
    recipe_path: Path, *, base_override: str | None = None
) -> dict[str, JsonValue]:
    try:
        with recipe_path.open("rb") as recipe_file:
            recipe = tomllib.load(recipe_file)
        metadata = recipe["metadata"]
        build_from = metadata.get("from", "template")
        source = recipe.get("source")
        if build_from == "scratch":
            if base_override:
                raise SmokeFailure(
                    "render_recipe", "scratch recipe cannot have a base override"
                )
            if not source or source.get("type") != "ubuntu-minimal-cloud-squashfs":
                raise SmokeFailure(
                    "render_recipe", "scratch recipe has an unsupported source"
                )
            origin = {
                "from_scratch": {
                    "source": {
                        "ubuntu_minimal_cloud_squashfs": {
                            "release": source["release"],
                            "version": source["version"],
                            "date": source["date"],
                        }
                    }
                }
            }
        elif build_from == "template":
            if source is not None:
                raise SmokeFailure(
                    "render_recipe", "template recipe cannot declare a source"
                )
            base = base_override or metadata.get("base")
            if not base:
                raise SmokeFailure("render_recipe", "template recipe is missing a base")
            origin = {"from_template": {"base": base}}
        else:
            raise SmokeFailure(
                "render_recipe", f"unsupported metadata.from value {build_from!r}"
            )
        config_values = {
            "vcpu": metadata.get("vcpu"),
            "memory_bytes": metadata.get("memory_mib", 0) * MIB
            if "memory_mib" in metadata
            else None,
            "disk_bytes": metadata.get("disk_size_mb", 0) * MIB
            if "disk_size_mb" in metadata
            else None,
        }
        build_config = {
            key: value for key, value in config_values.items() if value is not None
        }
        return {
            "recipe": {
                "metadata": {"description": metadata.get("description")},
                "build_config": build_config,
                "origin": origin,
                "steps": [
                    render_step(recipe_path, step, index)
                    for index, step in enumerate(recipe.get("steps", []), start=1)
                ],
            },
            "force": True,
            "publish_tags": [],
            "client": {
                "source_path": recipe_path.as_posix(),
                "cli_version": "buildkite-smoke",
                "hostname": os.environ.get("HOSTNAME", ""),
            },
        }
    except (KeyError, OSError, tomllib.TOMLDecodeError) as error:
        raise SmokeFailure("render_recipe", f"invalid recipe: {error}") from error


def load_credentials() -> tuple[str, str, str]:
    raw = os.environ.get("SPACE_ADMIN_CREDENTIALS", "")
    if raw:
        try:
            secret = json.loads(raw)
        except json.JSONDecodeError:
            secret = {"token": raw}
        if isinstance(secret, str):
            secret = {"token": secret}
        if not isinstance(secret, dict):
            raise SmokeFailure(
                "authentication", "admin credential has an invalid shape"
            )
    else:
        secret = {}
    token = os.environ.get("SPACE_API_TOKEN", "") or str(
        secret.get("token") or secret.get("SPACE_API_TOKEN") or ""
    )
    email = os.environ.get("SPACE_ADMIN_EMAIL", "") or str(
        secret.get("email") or secret.get("SPACE_ADMIN_EMAIL") or ""
    )
    password = os.environ.get("SPACE_ADMIN_PASSWORD", "") or str(
        secret.get("password") or secret.get("SPACE_ADMIN_PASSWORD") or ""
    )
    if not token and not (email and password):
        raise SmokeFailure(
            "authentication",
            "set SPACE_API_TOKEN, SPACE_ADMIN_SECRET_ID, or SPACE_ADMIN_EMAIL/PASSWORD",
        )
    return token, email, password


class TemplateRecipeSmoke:
    def __init__(self, repo_root: Path) -> None:
        self.repo_root = repo_root
        self.admin_root = os.environ["SPACE_ADMIN_API_URL"].rstrip("/")
        self.auth_root = os.environ.get("SPACE_AUTH_URL", "").rstrip("/")
        self.poll_seconds = float(
            os.environ.get("SPACE_TEMPLATE_RECIPE_BUILD_POLL_SECONDS", "10")
        )
        self.build_timeout = float(
            os.environ.get("SPACE_TEMPLATE_RECIPE_BUILD_TIMEOUT_SECONDS", "7200")
        )
        self.cleanup_timeout = float(
            os.environ.get("SPACE_TEMPLATE_RECIPE_CLEANUP_TIMEOUT_SECONDS", "900")
        )
        self.max_transient_reads = int(
            os.environ.get("SPACE_TEMPLATE_RECIPE_MAX_TRANSIENT_READS", "12")
        )
        self.evidence_path = Path(
            os.environ.get(
                "SPACE_TEMPLATE_RECIPE_EVIDENCE_PATH",
                "template-recipe-build-evidence.json",
            )
        )
        stamp = (
            os.environ.get("SPACE_TEMPLATE_RECIPE_BUILD_RUN_ID")
            or os.environ.get("BUILDKITE_JOB_ID")
            or str(uuid.uuid4())
        )
        self.seed_name = os.environ.get(
            "SPACE_TEMPLATE_RECIPE_BUILD_SEED_ID", f"seed-bk-smoke-{stamp}"
        )
        self.derived_name = os.environ.get(
            "SPACE_TEMPLATE_RECIPE_BUILD_CODE_RUNTIME_ID",
            f"code-runtime-bk-smoke-{stamp}",
        )
        self._validate_name(self.seed_name, "seed")
        self._validate_name(self.derived_name, "code-runtime")
        self.client: JsonClient | None = None
        self.auth_client: JsonClient | None = None
        self.owns_session = False
        self.created: list[str] = []
        self.cleanup_errors: list[dict[str, str]] = []
        self.evidence: dict[str, JsonValue] = {
            "target": os.environ.get("SPACE_TEMPLATE_RECIPE_BUILD_TARGET", "testing"),
            "source_sha": os.environ.get("BUILDKITE_COMMIT", "unknown"),
            "deployed_images": json.loads(
                os.environ.get("SPACE_TEMPLATE_RECIPE_DEPLOYED_IMAGES_JSON", "{}")
            ),
            "templates": {
                "seed": {"name": self.seed_name},
                "code_runtime": {"name": self.derived_name},
            },
            "request_ids": [],
            "first_failure": None,
            "cleanup": [],
            "cold_retrieval": {
                "status": "not_proven",
                "reason": "admin recipe-build API exposes no builder placement control",
            },
        }

    @staticmethod
    def _validate_name(name: str, prefix: str) -> None:
        if not TEMPLATE_NAME.fullmatch(name) or not name.startswith(
            f"{prefix}-bk-smoke-"
        ):
            raise SmokeFailure("configuration", f"unsafe smoke template name {name!r}")

    def record_response(self, phase: str, response: Response) -> None:
        if response.request_id:
            self.evidence["request_ids"].append(
                {"phase": phase, "request_id": response.request_id}
            )

    def write_evidence(self) -> None:
        self.evidence_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.evidence_path.with_suffix(self.evidence_path.suffix + ".tmp")
        temporary.write_text(json.dumps(self.evidence, indent=2, sort_keys=True) + "\n")
        temporary.replace(self.evidence_path)

    def authenticate(self) -> None:
        token, email, password = load_credentials()
        if token:
            self.client = JsonClient(self.admin_root, token)
            return
        if not self.auth_root:
            raise SmokeFailure(
                "authentication", "SPACE_AUTH_URL is required for password login"
            )
        self.auth_client = JsonClient(self.auth_root)
        response = self.auth_client.request(
            "POST",
            "/sessions",
            body={"email": email, "password": password},
            expected=(200,),
            phase="authentication",
        )
        payload = require_object(response.body, "authentication")
        token = payload.get("token")
        user = payload.get("user")
        if not isinstance(token, str) or not token or not isinstance(user, dict):
            raise SmokeFailure(
                "authentication", "login response was missing token or user"
            )
        self.record_response("authentication", response)
        self.auth_client.token = token
        self.client = JsonClient(self.admin_root, token)
        self.owns_session = True

    def create_template(self, name: str, description: str) -> dict[str, JsonValue]:
        assert self.client is not None
        response = self.client.request(
            "POST",
            "/templates",
            body={"name": name, "description": description},
            expected=(201,),
            phase=f"create_{name}",
        )
        self.record_response(f"create_{name}", response)
        payload = require_object(response.body, f"create_{name}")
        self.created.append(name)
        if payload.get("name") != name:
            raise SmokeFailure(
                f"create_{name}", "create response named a different template"
            )
        try:
            uuid.UUID(str(payload.get("org_id")))
        except (TypeError, ValueError, AttributeError) as error:
            raise SmokeFailure(
                f"create_{name}",
                "template was not assigned to the managing organization",
            ) from error
        return payload

    def start_build(
        self,
        name: str,
        recipe_path: Path,
        evidence_key: str,
        *,
        base_override: str | None = None,
    ) -> tuple[str, str]:
        assert self.client is not None
        request = render_build_request(recipe_path, base_override=base_override)
        response = self.client.request(
            "POST",
            f"/templates/{urllib.parse.quote(name)}/builds",
            body=request,
            expected=(202,),
            phase=f"submit_{name}",
        )
        self.record_response(f"submit_{name}", response)
        payload = require_object(response.body, f"submit_{name}")
        job_id = payload.get("job_id")
        if (
            not isinstance(job_id, str)
            or not job_id
            or payload.get("template_name") != name
        ):
            raise SmokeFailure(
                f"submit_{name}", "build response had an invalid job identity"
            )
        self.evidence["templates"][evidence_key]["job_id"] = job_id
        self.write_evidence()
        content_hash = self.wait_for_build(name, job_id)
        self.evidence["templates"][evidence_key]["content_hash"] = content_hash
        self.write_evidence()
        return job_id, content_hash

    def wait_for_build(self, name: str, job_id: str) -> str:
        assert self.client is not None
        deadline = time.monotonic() + self.build_timeout
        transient_failures = 0
        last_state = ""
        while time.monotonic() <= deadline:
            phase = f"status_{name}"
            try:
                response = self.client.request(
                    "GET",
                    f"/templates/{urllib.parse.quote(name)}/builds/{urllib.parse.quote(job_id)}",
                    phase=phase,
                    transient_read=True,
                )
            except TransientReadFailure as error:
                transient_failures += 1
                if transient_failures > self.max_transient_reads:
                    raise SmokeFailure(
                        phase, "too many transient build status failures"
                    ) from error
                time.sleep(self.poll_seconds)
                continue
            self.record_response(phase, response)
            payload = require_object(response.body, phase)
            if payload.get("job_id") != job_id or payload.get("template_name") != name:
                raise SmokeFailure(
                    phase, "build status identity mismatch", response.request_id
                )
            state = payload.get("state")
            if not isinstance(state, str):
                raise SmokeFailure(
                    phase, "build status omitted state", response.request_id
                )
            if state != last_state:
                print(f"{name} job={job_id} state={state}", flush=True)
                last_state = state
            if state not in TERMINAL_BUILD_STATES:
                time.sleep(self.poll_seconds)
                continue
            if state != "succeeded":
                raise SmokeFailure(
                    phase,
                    f"build reached {state}: {payload.get('error') or 'no error detail'}",
                    response.request_id,
                )
            content_hash = payload.get("content_hash")
            if not isinstance(content_hash, str) or not FULL_DIGEST.fullmatch(
                content_hash
            ):
                raise SmokeFailure(
                    phase, "successful build omitted a full content digest"
                )
            return content_hash
        raise SmokeFailure(
            f"status_{name}", "build did not reach a terminal state before timeout"
        )

    def authoritative_release(
        self, name: str, content_hash: str
    ) -> dict[str, JsonValue]:
        assert self.client is not None
        phase = f"release_{name}"
        response = self.client.request("GET", "/template-releases", phase=phase)
        self.record_response(phase, response)
        releases = require_list(response.body, phase)
        matches = [
            require_object(release, phase)
            for release in releases
            if isinstance(release, dict)
            and release.get("template_name") == name
            and release.get("content_hash") == content_hash
        ]
        if len(matches) != 1:
            raise SmokeFailure(
                phase, "authoritative release digest was absent or ambiguous"
            )
        release = matches[0]
        tags = release.get("tags")
        if tags != [content_hash]:
            raise SmokeFailure(
                phase,
                "digest-only publication unexpectedly created or omitted release aliases",
            )
        try:
            uuid.UUID(str(release.get("id")))
        except (TypeError, ValueError, AttributeError) as error:
            raise SmokeFailure(phase, "release response had an invalid id") from error
        return release

    def delete_template(self, name: str) -> None:
        assert self.client is not None
        phase = f"cleanup_delete_{name}"
        response = self.client.request(
            "DELETE",
            f"/templates/{urllib.parse.quote(name)}",
            headers={"Idempotency-Key": f"template-smoke-delete-{name}"},
            expected=(202, 404),
            phase=phase,
        )
        self.record_response(phase, response)
        if response.status == 404:
            absence = self.client.request(
                "GET",
                f"/templates/{urllib.parse.quote(name)}",
                expected=(404,),
                phase=f"cleanup_absence_{name}",
            )
            self.record_response(f"cleanup_absence_{name}", absence)
            self.evidence["cleanup"].append(
                {"template": name, "operation_id": None, "state": "already_absent"}
            )
            return
        payload = require_object(response.body, phase)
        operation_id = payload.get("operation_id")
        if not isinstance(operation_id, str) or not operation_id.startswith("op_"):
            raise SmokeFailure(phase, "delete response omitted operation id")
        if payload.get("template") != name:
            raise SmokeFailure(phase, "delete response named a different template")
        self.wait_for_operation(name, operation_id)
        absence = self.client.request(
            "GET",
            f"/templates/{urllib.parse.quote(name)}",
            expected=(404,),
            phase=f"cleanup_absence_{name}",
        )
        self.record_response(f"cleanup_absence_{name}", absence)
        self.evidence["cleanup"].append(
            {"template": name, "operation_id": operation_id, "state": "succeeded"}
        )

    def wait_for_operation(self, name: str, operation_id: str) -> None:
        assert self.client is not None
        deadline = time.monotonic() + self.cleanup_timeout
        transient_failures = 0
        while time.monotonic() <= deadline:
            phase = f"cleanup_status_{name}"
            try:
                response = self.client.request(
                    "GET",
                    f"/operations/{urllib.parse.quote(operation_id)}",
                    phase=phase,
                    transient_read=True,
                )
            except TransientReadFailure as error:
                transient_failures += 1
                if transient_failures > self.max_transient_reads:
                    raise SmokeFailure(
                        phase, "too many transient cleanup status failures"
                    ) from error
                time.sleep(self.poll_seconds)
                continue
            self.record_response(phase, response)
            payload = require_object(response.body, phase)
            if payload.get("id") != operation_id:
                raise SmokeFailure(phase, "cleanup operation identity mismatch")
            state = payload.get("state")
            if state not in TERMINAL_OPERATION_STATES:
                time.sleep(self.poll_seconds)
                continue
            if state != "succeeded":
                raise SmokeFailure(
                    phase,
                    f"cleanup reached {state}: {payload.get('progress_message') or 'no detail'}",
                    response.request_id,
                )
            return
        raise SmokeFailure(
            f"cleanup_status_{name}", "cleanup did not finish before timeout"
        )

    def cleanup(self) -> None:
        for name in (self.derived_name, self.seed_name):
            if name not in self.created:
                continue
            try:
                self.delete_template(name)
            except SmokeFailure as error:
                self.cleanup_errors.append(
                    {"template": name, "phase": error.phase, "error": str(error)}
                )
        if self.owns_session and self.auth_client is not None:
            try:
                self.auth_client.request(
                    "DELETE", "/session", expected=(204,), phase="session_cleanup"
                )
            except SmokeFailure as error:
                self.cleanup_errors.append(
                    {"template": "session", "phase": error.phase, "error": str(error)}
                )
        self.evidence["cleanup_errors"] = self.cleanup_errors

    def execute(self) -> int:
        primary_failure: SmokeFailure | None = None
        try:
            self.authenticate()
            seed_template = self.create_template(
                self.seed_name, "Buildkite recipe smoke seed"
            )
            derived_template = self.create_template(
                self.derived_name, "Buildkite recipe smoke code runtime"
            )
            if seed_template["org_id"] != derived_template["org_id"]:
                raise SmokeFailure(
                    "create_templates", "smoke templates have different owners"
                )
            seed_recipe = self.repo_root / "templates/firecracker/recipes/seed.toml"
            derived_recipe = (
                self.repo_root / "templates/firecracker/recipes/code-runtime.toml"
            )
            seed_job, seed_hash = self.start_build(self.seed_name, seed_recipe, "seed")
            seed_release = self.authoritative_release(self.seed_name, seed_hash)
            seed_ref = f"{self.seed_name}@{seed_hash}"
            self.evidence["templates"]["seed"].update(
                {
                    "job_id": seed_job,
                    "content_hash": seed_hash,
                    "release_id": seed_release["id"],
                    "release_tags": seed_release["tags"],
                    "runtime": seed_release.get("runtime"),
                }
            )
            self.evidence["templates"]["code_runtime"]["requested_base"] = seed_ref
            self.evidence["templates"]["code_runtime"].update(
                {
                    "requested_base_digest": seed_hash,
                    "resolved_base_digest": None,
                    "resolution_note": (
                        "the status API reports no resolved base; populated only after "
                        "successful materialization of the exact digest reference"
                    ),
                }
            )
            self.write_evidence()
            derived_job, derived_hash = self.start_build(
                self.derived_name,
                derived_recipe,
                "code_runtime",
                base_override=seed_ref,
            )
            derived_release = self.authoritative_release(
                self.derived_name, derived_hash
            )
            self.evidence["templates"]["code_runtime"].update(
                {
                    "job_id": derived_job,
                    "content_hash": derived_hash,
                    "release_id": derived_release["id"],
                    "release_tags": derived_release["tags"],
                    "runtime": derived_release.get("runtime"),
                    "resolved_base_digest": seed_hash,
                }
            )
            self.evidence["outcome"] = "passed"
        except SmokeFailure as error:
            primary_failure = error
            self.evidence["outcome"] = "failed"
            self.evidence["first_failure"] = {
                "phase": error.phase,
                "error": str(error),
                "request_id": error.request_id or None,
            }
            print(f"ERROR [{error.phase}]: {error}", file=sys.stderr, flush=True)
        finally:
            self.write_evidence()
            self.cleanup()
            self.write_evidence()

        if primary_failure is not None:
            if self.cleanup_errors:
                print(f"Cleanup also failed: {self.cleanup_errors}", file=sys.stderr)
            return 1
        if self.cleanup_errors:
            print(f"ERROR: cleanup failed: {self.cleanup_errors}", file=sys.stderr)
            return 1
        print("ALL CHECKS PASSED")
        return 0


def main() -> int:
    repo_root = Path(
        os.environ.get("BUILDKITE_BUILD_CHECKOUT_PATH", Path.cwd())
    ).resolve()
    try:
        return TemplateRecipeSmoke(repo_root).execute()
    except (KeyError, ValueError, json.JSONDecodeError) as error:
        print(f"ERROR [configuration]: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
