"""An exchanged storage session reaches only the object it was vended for."""

import space_smoke as smoke


# Two sandboxes, each suspended to the store: one whose snapshot the session
# is vended for, one whose snapshot it must not reach.
@smoke.scenario(
    "local.storage_exchange_scoping",
    sandbox=smoke.SandboxConfig(timeout=900),
    sandboxes=2,
    required_tools=("aws",),
    storage_credentials="exchange",
)
def storage_exchange_scoping(test: smoke.KindEcw) -> None:
    """A read-scoped session that a sandbox worker's credential-manager agent
    vends on its volume socket, as volume-manager asks, for one object of a
    sandbox's suspend snapshot, reads that object; the object store refuses it
    for another sandbox's snapshot object and listing, for the templates
    bucket, and for a small write to its own object, which stays unchanged;
    and the authority refuses to vend that sandbox's subject for the other
    sandbox's object at all. Afterwards the session, and a fresh one, still
    read the object, so no refusal was a session that stopped working."""

    exchange = test.storage_exchange

    with test.phase("the cluster exchanges storage credentials"):
        exchange.require_exchange_mode()

    with test.phase("suspend both sandboxes to the object store"):
        exchange.suspend_both()

    with test.phase("exchange a read session for one snapshot object through the agent"):
        exchange.exchange_read_session()

    with test.phase("control: the session reads its own object"):
        exchange.expect_own_object_readable()

    with test.phase("the store refuses the session for another sandbox's snapshot"):
        exchange.expect_other_sandbox_refused()

    with test.phase("the store refuses the session for another bucket"):
        exchange.expect_other_bucket_refused()

    with test.phase("the store refuses a write under read scope, and the object is unchanged"):
        exchange.expect_write_refused_and_object_unchanged()

    with test.phase("the authority refuses the subject for another sandbox's object"):
        exchange.expect_authority_refuses_another_sandboxs_object()

    with test.phase("control: the session and a fresh exchange still read the object"):
        exchange.expect_controls_hold_after_refusals()
