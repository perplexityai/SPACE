"""The reconcile-queue GC scenario: the tick it waits out, and its seed check."""

import pytest

from space_smoke.reconcile_queue_gc import GC_TICK_ENV, GC_TICK_SECONDS, gc_tick_seconds


def test_the_deployments_tick_hook_sets_the_wait():
    assert gc_tick_seconds({GC_TICK_ENV: "10"}) == 10


@pytest.mark.parametrize("value", [None, "", "0", "-5", "abc", " 10"])
def test_anything_the_binary_ignores_waits_its_default_tick(value):
    assert gc_tick_seconds({GC_TICK_ENV: value}) == GC_TICK_SECONDS


def test_seed_rows_are_read_from_its_own_transaction_output():
    from space_smoke.adapters import _reconcile_queue_refs

    out = "BEGIN\nINSERT 0 2\nINSERT 0 2\nINSERT 0 4\nlive|node-status|f\nlive|sandbox-inventory|t\nCOMMIT\n"
    refs = _reconcile_queue_refs(out)
    assert [(r.node_id, r.facet, r.dead) for r in refs] == [
        ("live", "node-status", False),
        ("live", "sandbox-inventory", True),
    ]
