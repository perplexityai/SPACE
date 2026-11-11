"""The cross-origin Native files scenarios refuse a single origin."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from space_smoke.model import Verdict
from space_smoke.runtime import execute_scenario
from space_smoke.scenarios.native_contract_file_idempotency_across_origins import (
    file_idempotency_across_origins,
)
from space_smoke.scenarios.native_contract_file_operations_match_on_the_data_plane import (
    file_operations_match_on_the_data_plane,
)

CROSS_ORIGIN = (file_operations_match_on_the_data_plane, file_idempotency_across_origins)


class _Started(Exception):
    """The scenario went on to create its sandbox."""


def _fixture(api_url: str, data_plane_url: str) -> SimpleNamespace:
    def create_ready() -> str:
        raise _Started

    return SimpleNamespace(
        config=SimpleNamespace(api_url=api_url, data_plane_url=data_plane_url),
        create_ready=create_ready,
    )


@pytest.mark.parametrize("scenario", CROSS_ORIGIN, ids=lambda scenario: scenario.id)
@pytest.mark.parametrize(
    ("api_url", "data_plane_url"),
    [
        ("http://127.0.0.1:8080", "http://127.0.0.1:8080"),
        ("https://space.example", "https://space.example:443/v1"),
    ],
)
def test_one_origin_is_inconclusive_before_any_sandbox(scenario, api_url, data_plane_url):
    result = execute_scenario(
        title=scenario.title,
        body=lambda _: scenario.verify(_fixture(api_url, data_plane_url)),
    )
    assert result.verdict == Verdict.INCONCLUSIVE, result
    assert "SPACE_DATA_PLANE_URL" in result.summary


@pytest.mark.parametrize("scenario", CROSS_ORIGIN, ids=lambda scenario: scenario.id)
def test_distinct_origins_go_on_to_compare(scenario):
    with pytest.raises(_Started):
        scenario.verify(_fixture("http://127.0.0.1:8080", "http://127.0.0.1:8090"))
