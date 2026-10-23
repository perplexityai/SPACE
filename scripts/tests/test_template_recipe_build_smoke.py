"""Guards for the template recipe build smoke's derived-recipe profiles.

The per-deploy smoke defaults to an offline derived recipe so an upstream
package-archive outage (snapshot.ubuntu.com 5xx) cannot fail the deploy
signal. These tests hold that default, the offline invariant, and that the
offline recipe renders into a request the Admin API accepts.

    python -m pytest scripts/tests/test_template_recipe_build_smoke.py -q
"""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import sys
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
RECIPES = REPO_ROOT / "templates/firecracker/recipes"
OFFLINE_RECIPE = RECIPES / "build-pipeline-smoke.toml"

_spec = importlib.util.spec_from_file_location(
    "template_recipe_build_smoke",
    REPO_ROOT / "scripts/template_recipe_build_smoke.py",
)
assert _spec and _spec.loader
smoke = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = smoke
_spec.loader.exec_module(smoke)


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPACE_ADMIN_API_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("SPACE_TEMPLATE_RECIPE_BUILD_RUN_ID", "test")
    for name in (
        "SPACE_TEMPLATE_RECIPE_BUILD_PROFILE",
        "SPACE_TEMPLATE_RECIPE_BUILD_CODE_RUNTIME_ID",
        "SPACE_TEMPLATE_RECIPE_BUILD_SEED_ID",
    ):
        monkeypatch.delenv(name, raising=False)


def test_default_profile_is_offline() -> None:
    run = smoke.TemplateRecipeSmoke(REPO_ROOT)
    assert run.profile == "offline"
    assert run.derived_recipe == OFFLINE_RECIPE
    assert run.derived_name == "build-pipeline-bk-smoke-test"
    assert run.evidence["profile"] == "offline"
    assert run.evidence["templates"]["code_runtime"]["recipe"] == (
        "templates/firecracker/recipes/build-pipeline-smoke.toml"
    )


def test_empty_profile_is_the_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPACE_TEMPLATE_RECIPE_BUILD_PROFILE", "")
    assert smoke.TemplateRecipeSmoke(REPO_ROOT).profile == "offline"


def test_code_runtime_profile_keeps_existing_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPACE_TEMPLATE_RECIPE_BUILD_PROFILE", "code-runtime")
    run = smoke.TemplateRecipeSmoke(REPO_ROOT)
    assert run.derived_recipe == RECIPES / "code-runtime.toml"
    assert run.derived_name == "code-runtime-bk-smoke-test"


def test_unknown_profile_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPACE_TEMPLATE_RECIPE_BUILD_PROFILE", "full")
    with pytest.raises(smoke.SmokeFailure, match="unknown"):
        smoke.TemplateRecipeSmoke(REPO_ROOT)


def test_derived_name_override_must_match_profile_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "SPACE_TEMPLATE_RECIPE_BUILD_CODE_RUNTIME_ID", "code-runtime-bk-smoke-x"
    )
    with pytest.raises(smoke.SmokeFailure, match="unsafe"):
        smoke.TemplateRecipeSmoke(REPO_ROOT)


def test_offline_guard_rejects_network_steps() -> None:
    with pytest.raises(smoke.SmokeFailure, match="network steps"):
        smoke.TemplateRecipeSmoke._require_offline(RECIPES / "code-runtime.toml")


def test_offline_recipe_has_no_network_steps() -> None:
    recipe = tomllib.loads(OFFLINE_RECIPE.read_text())
    kinds = {step["type"] for step in recipe["steps"]}
    assert not kinds & smoke.NETWORK_STEP_TYPES
    # Cover every step kind the build VM runs offline.
    assert {"env", "copy", "run"} <= kinds
    assert {
        step.get("user", "root") for step in recipe["steps"] if step["type"] == "run"
    } == {
        "root",
        "user",
    }


def test_offline_recipe_renders_with_seed_override() -> None:
    request = smoke.render_build_request(
        OFFLINE_RECIPE, base_override="seed-bk-smoke-test@sha256:" + "0" * 64
    )
    steps = request["recipe"]["steps"]
    copy = next(step["copy"] for step in steps if "copy" in step)
    payload = base64.b64decode(copy["inline_bytes_base64"])
    # The recipe's root step checks this digest inside the build VM.
    digest = hashlib.sha256(payload).hexdigest()
    assert digest in OFFLINE_RECIPE.read_text()
    assert copy["dest"] == "/opt/space/build-smoke/payload.txt"
