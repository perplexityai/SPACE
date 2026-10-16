"""Tests for the termination-fence writer guard.

The guard's own failure mode is the one worth testing hardest: a scanner that
stops finding anything passes, silently, and stops protecting the thing it exists
for. An earlier revision did exactly that — a per-line quote counter that skipped
its own state update found zero writer sites on a tree with six, and reported
success. So the first test below is not "does it pass" but "does it still see the
writers", and the guard carries the same assertion at runtime.
"""

from __future__ import annotations

import importlib.util
import pathlib
import subprocess
import sys

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/source_guards/placement_fence_writers.py"
SPEC = importlib.util.spec_from_file_location("placement_fence_writers_guard", SCRIPT)
assert SPEC is not None
guard = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = guard
assert SPEC.loader is not None
SPEC.loader.exec_module(guard)


def test_current_repository_passes() -> None:
    guard.main()


def test_guard_finds_every_known_writer() -> None:
    """Non-vacuity, against the real tree.

    Pinned by count *and* by path, because the two go blind differently: a
    scanner that finds nothing fails the count, and one that finds six of the
    wrong things fails the paths.
    """
    sites = guard.writer_sites()
    assert len(sites) == guard.EXPECTED_FENCED_SITES
    assert all(fenced for _, _, fenced in sites), [
        f"{path}:{line}" for path, line, fenced in sites if not fenced
    ]
    files = {path.as_posix() for path, _, _ in sites}
    assert files == {
        "crates/libs/api/src/controllers/sandbox/queries.rs",
        "crates/libs/api/src/controllers/sandbox/responses.rs",
        "crates/libs/api/src/controllers/substrate/direct.rs",
        "crates/libs/api/src/controllers/substrate/kubernetes/store/pod_placement.rs",
        "crates/libs/sandbox/store/src/create.rs",
    }


def write_rust(root: pathlib.Path, relative: str, body: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    subprocess.run(("git", "init", "-q"), cwd=root, check=True)
    subprocess.run(("git", "add", "-A"), cwd=root, check=True)


UNFENCED_UPDATE = '''
async fn attach(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2,
                placement = 'on_node'
          WHERE id = $1"
    ))
    .execute(pool)
    .await?;
    Ok(())
}
'''

UNFENCED_INSERT = '''
async fn insert(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, lifecycle, placement, active_node_id, network_policy_generation
         ) VALUES ($1, 'paused', 'on_node', $2, 1)"
    ))
    .execute(pool)
    .await?;
    Ok(())
}
'''


@pytest.mark.parametrize("body", (UNFENCED_UPDATE, UNFENCED_INSERT), ids=("update", "insert"))
def test_guard_rejects_an_unfenced_writer(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, body: str
) -> None:
    write_rust(tmp_path, "crates/libs/api/src/new_writer.rs", body)
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    with pytest.raises(SystemExit, match="no termination-fence check"):
        guard.main()


@pytest.mark.parametrize("body", (UNFENCED_UPDATE, UNFENCED_INSERT), ids=("update", "insert"))
def test_guard_accepts_the_same_writer_once_fenced(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, body: str
) -> None:
    fenced = body.replace(
        "    sqlx::query(sql!(",
        "    let fence = check_placement_fence(&mut tx, node, id, writer::X).await?;\n"
        "    if fence.refuses() {\n"
        "        return Ok(());\n"
        "    }\n"
        "    sqlx::query(sql!(",
        1,
    )
    write_rust(tmp_path, "crates/libs/api/src/new_writer.rs", fenced)
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(guard, "EXPECTED_FENCED_SITES", 1)
    guard.main()


# Shapes that ARE writes and were each missed by an earlier revision of the
# scanner. A table because it is the guard's own regression suite: a scanner that
# stops seeing a shape fails open, and failing open is indistinguishable from "no
# violations" in the output.
UPSERT_ONE_LINE = (
    "\nasync fn upsert(pool: &PgPool) -> Result<(), sqlx::Error> {\n"
    "    sqlx::query(sql!(\n"
    '        "INSERT INTO sandboxes (id) VALUES ($1) '
    'ON CONFLICT (id) DO UPDATE SET active_node_id = $2"\n'
    "    ))\n    .execute(pool)\n    .await?;\n    Ok(())\n}\n"
)

UPSERT_WRAPPED = (
    "\nasync fn upsert(pool: &PgPool) -> Result<(), sqlx::Error> {\n"
    "    sqlx::query(sql!(\n"
    '        "INSERT INTO sandboxes (id) VALUES ($1)\n'
    '          ON CONFLICT (id) DO UPDATE SET active_node_id = $2"\n'
    "    ))\n    .execute(pool)\n    .await?;\n    Ok(())\n}\n"
)

SET_WITH_CALL_FIRST = (
    "\nasync fn attach(pool: &PgPool) -> Result<(), sqlx::Error> {\n"
    "    sqlx::query(sql!(\n"
    '        "UPDATE sandboxes\n'
    '            SET placement_changed_at = NOW(),\n'
    '                active_node_id = $1\n'
    '          WHERE id = $2"\n'
    "    ))\n    .execute(pool)\n    .await?;\n    Ok(())\n}\n"
)

SET_WITH_SUBQUERY = (
    "\nasync fn attach(pool: &PgPool) -> Result<(), sqlx::Error> {\n"
    "    sqlx::query(sql!(\n"
    '        "UPDATE sandboxes\n'
    '            SET target_node_id = (SELECT id FROM nodes WHERE address = $3),\n'
    '                active_node_id = $1\n'
    '          WHERE id = $2"\n'
    "    ))\n    .execute(pool)\n    .await?;\n    Ok(())\n}\n"
)

CTE_WRAPPED_UPDATE = (
    "\nasync fn attach(pool: &PgPool) -> Result<(), sqlx::Error> {\n"
    "    sqlx::query(sql!(\n"
    '        "WITH updated AS (\n'
    '             UPDATE sandboxes\n'
    '             SET lifecycle = \'started\',\n'
    '                 active_node_id = $1\n'
    '             WHERE id = $2\n'
    '             RETURNING *\n'
    '         )\n'
    '         SELECT * FROM updated"\n'
    "    ))\n    .execute(pool)\n    .await?;\n    Ok(())\n}\n"
)

MISSED_SHAPES = {
    # `NULLIF($1, $3)` is a placement. Testing whether the value merely *starts*
    # with NULL read it as a detachment and skipped it.
    "nullif": UNFENCED_UPDATE.replace("active_node_id = $2", "active_node_id = NULLIF($2, $3)"),
    # The assigned value on the following line. A `\S+` capture needed one on the
    # same line.
    "value_on_next_line": UNFENCED_UPDATE.replace(
        "SET active_node_id = $2,", "SET active_node_id =\n                $2,"
    ),
    # `ON CONFLICT (id) DO UPDATE SET active_node_id = $3` on one line. A clause
    # *terminator* sits to the left of the `SET` that opens the assignment list,
    # so comparing a single `SET` against a single terminator closed the clause
    # and never tested the assignment. Worse than a plain miss: no site was found,
    # so the pinned-count tripwire stayed quiet and the guard reported success.
    # The same statement wrapped across lines was caught, making the blind spot
    # formatting-dependent.
    # `INSERT … VALUES (…) ON CONFLICT … DO UPDATE SET active_node_id = $n` with
    # everything on ONE physical line. An earlier "fix" claimed to cover this
    # shape while its fixture put `ON CONFLICT` on line 2 — so it exercised the
    # form that already worked, and the blind spot survived a green suite. The
    # single-line form is the dangerous one: the scanner opened the INSERT column
    # list, the same line's `VALUES` closed it, the assignment was never
    # evaluated, no site was found, and the pinned count therefore still matched.
    "on_conflict_do_update_one_physical_line": UPSERT_ONE_LINE,
    # The same statement wrapped. Its own case so a regression in either form is
    # attributable to that form.
    "on_conflict_do_update_wrapped": UPSERT_WRAPPED,
    # A function call *earlier* in the same `SET` list. This was the widest miss
    # the guard has had: a bare `)` was treated as a `SET` terminator, so
    # `SET placement_changed_at = NOW(), active_node_id = $1` lost the clause at
    # `NOW()` and never saw the assignment on the following line. The six real
    # writers all happen to put `active_node_id` first, so the guard was passing
    # on luck of column ordering — a new writer with the columns the other way
    # round would have been invisible, with the pinned count still green.
    "call_before_the_assignment": SET_WITH_CALL_FIRST,
    # A scalar subquery inside the `SET` list. Its `FROM`/`WHERE` sit one paren
    # deeper than the `SET`, and treating clause keywords as terminators wherever
    # they appear let the subquery end the clause from the inside — losing the
    # assignment on the next line. Fixed by comparing paren depth against the
    # depth the `SET` opened at, which closes the class rather than one token.
    "subquery_inside_the_set_list": SET_WITH_SUBQUERY,
    # A CTE-wrapped `UPDATE` — the pooled post-wake writer's real shape. The whole
    # statement sits at paren depth 1, so an absolute "boundaries only count at
    # depth 0" rule (the first attempt at the fix above) silently *lost* that
    # writer. The pinned site count is what caught it; this pins the shape.
    "cte_wrapped_update": CTE_WRAPPED_UPDATE,
    # Every pattern below is case-insensitive; the cheap early-out was not.
    "uppercase": UNFENCED_UPDATE.replace("active_node_id", "ACTIVE_NODE_ID").replace(
        "UPDATE sandboxes", "UPDATE SANDBOXES"
    ),
}


@pytest.mark.parametrize("body", MISSED_SHAPES.values(), ids=list(MISSED_SHAPES))
def test_guard_sees_every_writer_shape(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, body: str
) -> None:
    write_rust(tmp_path, "crates/libs/api/src/new_writer.rs", body)
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    with pytest.raises(SystemExit, match="no termination-fence check"):
        guard.main()


NOT_WRITES = {
    # A detachment. Clearing the column cannot land a sandbox anywhere.
    "set_null": '''
async fn detach(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET placement = 'unplaced',
                active_node_id = NULL,
                target_node_id = NULL
          WHERE id = $1"
    ))
    .execute(pool)
    .await?;
    Ok(())
}
''',
    # A predicate read. This class is why the guard tracks clause state at all:
    # the first revision reported forty of these as writers.
    "where_predicate": '''
async fn count(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandboxes
          WHERE active_node_id = $1
            AND lifecycle = 'started'"
    ))
    .fetch_one(pool)
    .await
}
''',
    # A `WHERE` that closes a `SET` on its own line. Same statement shape as the
    # internal API's `prune_absent_node_sandboxes`, which a position-blind
    # version of the clause machine misread.
    "where_closes_set_on_one_line": '''
async fn prune(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes s
            SET lifecycle = 'deleted', active_node_id = NULL
          WHERE s.active_node_id = $1"
    ))
    .execute(pool)
    .await?;
    Ok(())
}
''',
    # A Rust binding that happens to share the column's name, with an unrelated
    # SQL literal earlier in the same function.
    "rust_let_binding": '''
async fn resolve(pool: &PgPool) -> Result<(), sqlx::Error> {
    let _ = sqlx::query(sql!("UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"))
        .execute(pool)
        .await?;
    let active_node_id = state.controller.authoritative_active_node_id().await?;
    Ok(())
}
''',
    # A doc comment quoting the assignment. `sandbox_residency`'s docs do this.
    "doc_comment": '''
/// The predicate's hard-coded `active_node_id = {node_expr}` is why the seam is
/// here: `SET active_node_id = $1` is not what this renders.
fn predicate(alias: &str) -> String {
    format!("{alias}.lifecycle = ANY($2)")
}
''',
}


@pytest.mark.parametrize("body", NOT_WRITES.values(), ids=list(NOT_WRITES))
def test_guard_ignores_things_that_are_not_placements(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, body: str
) -> None:
    write_rust(tmp_path, "crates/libs/api/src/reader.rs", body)
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(guard, "EXPECTED_FENCED_SITES", 0)
    guard.main()


def test_guard_skips_test_sources(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """Tests attach sandboxes to fenced nodes deliberately, to assert the refusal."""
    write_rust(tmp_path, "crates/libs/api/src/thing/tests.rs", UNFENCED_UPDATE)
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(guard, "EXPECTED_FENCED_SITES", 0)
    guard.main()


def test_allowlisted_writer_is_exempt_but_reported(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """An allowlist entry silences the failure and nothing else.

    The success line has to name it: an exemption that leaves no trace in the
    output is an exemption nobody re-examines.
    """
    relative = "crates/libs/placement/machine/src/postgres.rs"
    (path, name), _ = next(iter(guard.KNOWN_UNFENCED.items()))
    assert path.as_posix() == relative
    write_rust(tmp_path, relative, UNFENCED_UPDATE.replace("attach", name))
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(guard, "EXPECTED_FENCED_SITES", 0)
    guard.main()
    output = capsys.readouterr().out
    assert "knowingly unfenced" in output
    assert relative in output


def test_the_exemption_does_not_cover_a_neighbouring_writer(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """The allowlist is keyed on the function, not the file.

    A file-wide skip would hide a *future* writer added beside the known one — from
    the violation check and from the pinned count both, so neither would notice. So
    the same allowlisted file, carrying a second unfenced writer in a differently
    named function, must still fail.
    """
    (path, name), _ = next(iter(guard.KNOWN_UNFENCED.items()))
    body = UNFENCED_UPDATE.replace("attach", name) + UNFENCED_UPDATE.replace(
        "attach", "some_later_writer"
    )
    write_rust(tmp_path, path.as_posix(), body)
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(guard, "EXPECTED_FENCED_SITES", 0)
    with pytest.raises(SystemExit, match="some_later_writer"):
        guard.main()


def test_a_mention_is_not_a_fence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """Naming the helper or the verdict type in a comment must not count.

    This feature's doc comments quote both names constantly, so a substring search
    over the function marked unfenced statements as fenced. The guard requires a
    call — the parenthesis is what distinguishes prose from code.
    """
    mentioned = UNFENCED_UPDATE.replace(
        "    sqlx::query(sql!(",
        "    // See check_placement_fence and PlacementFence for the contract.\n"
        "    sqlx::query(sql!(",
        1,
    )
    write_rust(tmp_path, "crates/libs/api/src/new_writer.rs", mentioned)
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    with pytest.raises(SystemExit, match="no termination-fence check"):
        guard.main()


def test_a_blind_scanner_fails_rather_than_passes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """The vacuity guard itself.

    A scanner that finds nothing must fail, not pass. This is the regression that
    already happened once, and the only assertion in this file that would have
    caught it.
    """
    write_rust(tmp_path, "crates/libs/api/src/nothing.rs", "fn noop() {}\n")
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    with pytest.raises(SystemExit, match="expected 6 fenced"):
        guard.main()


# --- Fail-open paths the guard used to have. ---------------------------------
#
# Both let an unfenced writer pass CI, which is the only failure that matters
# here: a guard that reports success while the invariant is broken is worse than
# no guard, because it is the thing standing in for the reviewer's attention.


@pytest.mark.parametrize(
    "mention",
    (
        "    // Ordering is handled upstream; see fence_node_for_placement(…).\n",
        "    /* fence_node_for_placement( is called by the caller. */\n",
        '    tracing::debug!("skipping fence_node_for_placement(…) here");\n',
    ),
    ids=("line-comment", "block-comment", "string"),
)
def test_a_mention_of_the_fence_does_not_certify_a_writer(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, mention: str
) -> None:
    """A comment or string naming the fence is not a call to it.

    The guard searched raw source lines, so any of these three marked the
    enclosing function fenced and the unfenced `active_node_id` write passed. It
    fails open in the one direction that readmits the whole bug class, and it is
    reachable by accident: a comment explaining *why* a writer is safe is the
    natural thing to write next to one that is not.
    """
    body = UNFENCED_UPDATE.replace(
        "    sqlx::query(sql!(", mention + "    sqlx::query(sql!("
    )
    write_rust(tmp_path, "crates/libs/api/src/new_writer.rs", body)
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    with pytest.raises(SystemExit, match="no termination-fence check"):
        guard.main()


def test_the_allowlist_does_not_cover_a_longer_name_containing_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """`completion_sql_v2` is not `completion_sql`.

    `KNOWN_UNFENCED` is keyed per function precisely so an exemption cannot
    spread to writers added beside the known one. A substring test gave that
    property away: the new function inherited the exemption and disappeared from
    the violation list *and* the pinned count, so neither check noticed it.
    """
    allowlisted = next(iter(guard.KNOWN_UNFENCED))[0]
    body = UNFENCED_UPDATE.replace("async fn attach(", "async fn completion_sql_v2(")
    write_rust(tmp_path, allowlisted.as_posix(), body)
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    with pytest.raises(SystemExit, match="no termination-fence check"):
        guard.main()


def test_the_allowlist_still_covers_its_exact_function(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """The other half of the pair — the exemption must still work.

    Tightening a match is the easy way to turn a fail-open into a fail-closed,
    which would break CI on the one writer that is deliberately unfenced. Pinned
    here so the narrowing above cannot overshoot.
    """
    path, name = next(iter(guard.KNOWN_UNFENCED))
    body = UNFENCED_UPDATE.replace("async fn attach(", f"async fn {name}(")
    write_rust(tmp_path, path.as_posix(), body)
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(guard, "EXPECTED_FENCED_SITES", 0)
    guard.main()


RAW_STRING_UNFENCED = '''
async fn attach(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(r#"UPDATE sandboxes SET "placement" = 'on_node', active_node_id = $2 WHERE id = $1"#)
        .execute(pool)
        .await?;
    Ok(())
}
'''


def test_a_raw_string_with_an_embedded_quote_still_reports_the_writer(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """`r#"… "placement" … active_node_id = $2 …"#` is one literal, not three.

    Treating the inner quote as the terminator split the statement before
    `active_node_id`, so the `SET` context was lost, `assigning_lines` found
    nothing, and the site vanished from the violation list *and* the pinned
    count — the guard reporting success on a tree with an unfenced writer. A
    quoted SQL identifier is the ordinary way to reach this, not a contrivance.
    """
    write_rust(tmp_path, "crates/libs/api/src/new_writer.rs", RAW_STRING_UNFENCED)
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    with pytest.raises(SystemExit, match="no termination-fence check"):
        guard.main()


FENCED_WITH_LIFETIMES = '''
async fn attach(tx: &mut Transaction<'_, Postgres>, node: Uuid) -> Result<(), sqlx::Error> {
    let fence = check_placement_fence(tx, node, id, Writer::X).await?;
    if fence.refuses() {
        return Ok(());
    }
    let _guard: Option<&'static str> = None;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .execute(&mut **tx)
    .await?;
    Ok(())
}
'''


def test_a_lifetime_is_not_a_char_literal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """`'_` and `'static` must not open a literal that swallows the fence call.

    Read as quotes, the pair blanks every line between them, the fence call with
    them, and a fenced writer reports as unfenced.
    """
    write_rust(tmp_path, "crates/libs/api/src/new_writer.rs", FENCED_WITH_LIFETIMES)
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(guard, "EXPECTED_FENCED_SITES", 1)
    guard.main()


def test_a_line_continuation_keeps_the_blanked_lines_aligned() -> None:
    text = 'let a = "one \\\n    two";\nlet b = \'x\';\ncheck_placement_fence(tx);\n'
    code = guard.code_only_lines(text)
    assert len(code) == len(text.splitlines())
    assert guard.FENCE_CALL.search(code[3])


CALLEE = '''
pub async fn insert_row(executor: impl PgExecutor<'_>, node: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, active_node_id) VALUES ($1, $2)"
    ))
    .execute(executor)
    .await?;
    Ok(())
}
'''

FENCING_CALLER = '''
async fn create(tx: &mut Transaction<'_, Postgres>, node: Uuid) -> Result<(), Error> {
    let fence = check_placement_fence(tx, node, id, Writer::X).await?;
    if fence.refuses() {
        return Ok(());
    }
    store::insert_row(&mut **tx, node).await?;
    Ok(())
}
'''


def caller_fenced_tree(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, caller: str, extra: str = ""
) -> None:
    write_rust(tmp_path, "crates/libs/store/src/create.rs", CALLEE)
    write_rust(tmp_path, "crates/libs/api/src/create.rs", caller + extra)
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(guard, "EXPECTED_FENCED_SITES", 1)
    monkeypatch.setattr(
        guard,
        "FENCED_BY_CALLER",
        {
            (pathlib.Path("crates/libs/store/src/create.rs"), "insert_row"): (
                pathlib.Path("crates/libs/api/src/create.rs"),
                "create",
            )
        },
    )


def test_a_writer_fenced_by_its_only_caller_passes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    caller_fenced_tree(monkeypatch, tmp_path, FENCING_CALLER)
    guard.main()


def test_a_writer_whose_caller_drops_the_fence_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    unfenced = FENCING_CALLER.replace(
        "    let fence = check_placement_fence(tx, node, id, Writer::X).await?;\n"
        "    if fence.refuses() {\n        return Ok(());\n    }\n",
        "",
    )
    assert unfenced != FENCING_CALLER
    caller_fenced_tree(monkeypatch, tmp_path, unfenced)
    with pytest.raises(SystemExit, match="no termination-fence check"):
        guard.main()


def test_a_second_caller_unfences_the_writer(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """A new caller inherits the write without the fence the named caller takes."""
    second = '''
async fn backfill(pool: &PgPool, node: Uuid) -> Result<(), Error> {
    store::insert_row(pool, node).await?;
    Ok(())
}
'''
    caller_fenced_tree(monkeypatch, tmp_path, FENCING_CALLER, second)
    with pytest.raises(SystemExit, match="no termination-fence check"):
        guard.main()

