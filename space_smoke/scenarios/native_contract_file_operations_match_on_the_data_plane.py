"""Every Native file operation answers the same from the data plane as from the API."""

from space_smoke.native_files import FileCalls, comparable, data, require_distinct_origins
from space_smoke.native_hardening import NativeHardeningFixture
from space_smoke.registry import scenario


def _is_modified_entry(entry: object) -> bool:
    """`entry` has the fields and types of a `files:modified` entry
    (`FileModifiedEntry`, crates/l20_domain/public/api/src/lib.rs). `root`
    may be absent, as an older guest omits it."""
    return (
        isinstance(entry, dict)
        and isinstance(entry.get("path"), str)
        and ("root" not in entry or isinstance(entry["root"], str))
        and "last_modified_at" in entry
        and isinstance(entry["last_modified_at"], str | None)
        and isinstance(entry.get("kinds"), list)
        and all(isinstance(kind, str) for kind in entry["kinds"])
    )


def _exercise(calls: FileCalls, directory: str) -> list[tuple[str, int, object]]:
    """All 11 operations, and the errors their checks answer, in `directory`."""
    key = calls.test.new_key
    steps = [
        ("mkdir", {"path": directory, "parents": True}, key("mkdir")),
        ("write", {"path": f"{directory}/a.txt", "data_base64": data(b"hello")}, key("write")),
        (
            "write_batch",
            {"entries": [
                {"path": f"{directory}/b.txt", "data_base64": data(b"b")},
                {"path": f"{directory}/c.txt", "data_base64": data(b"cc")},
            ]},
            key("write-batch"),
        ),
        ("stat", {"path": f"{directory}/a.txt"}, None),
        ("list", {"path": directory, "depth": 1}, None),
        ("rename", {"old_path": f"{directory}/b.txt", "new_path": f"{directory}/b2.txt"}, key("rename")),
        ("remove", {"path": f"{directory}/c.txt"}, key("remove")),
        ("changes_enable", {"roots": [directory]}, key("changes-enable")),
        ("write", {"path": f"{directory}/d.txt", "data_base64": data(b"d")}, key("write-d")),
        ("modified", {}, None),
        ("changes_disable", {}, key("changes-disable")),
        # What the routes refuse, each with its own answer.
        ("stat", {"path": f"{directory}/missing.txt"}, None),
        ("mkdir", {"path": f"{directory}/unkeyed"}, None),
        ("read", {"path": f"{directory}/a.txt", "length": 0}, None),
        ("list", {"path": directory, "depth": 0}, None),
    ]
    answers = []
    for action, body, idempotency_key in steps:
        response = calls.call(action, body, key=idempotency_key)
        answer = comparable(response.body, directory)
        if action == "modified" and isinstance(answer, dict) and isinstance(answer.get("files"), list):
            # The guest records changes as they happen, so which have landed
            # when the call is answered is not the contract; the shape is.
            files = response.body["files"]
            calls.test.context.verify(
                all(_is_modified_entry(entry) for entry in files),
                f"files:modified through {calls.origin} answered an entry out of its contract's shape",
                files=files,
            )
            answer["files"] = "<list>"
        answers.append((action, response.status, answer))
    read = calls.call("read", {"path": f"{directory}/a.txt", "length": 5})
    answers.append(("read", read.status, read.text))
    wrong_method = calls.call("stat", None, method="GET")
    answers.append(("stat by GET", wrong_method.status, comparable(wrong_method.body, directory)))
    return answers


@scenario("native_contract.file_operations_match_on_the_data_plane", api_surfaces=("native",))
def file_operations_match_on_the_data_plane(test: NativeHardeningFixture) -> None:
    """The 11 `files:*` operations, and the refusals of their field checks,
    answer the data plane's origin with the statuses and bodies the API's
    origin answers, on the same sandbox. Inconclusive when the two origins
    are one."""
    require_distinct_origins(test)
    sandbox_id = test.create_ready()
    run = test.new_key("files")
    with test.context.phase("run every file operation through the API"):
        api_directory = f"/tmp/{run}-api"
        through_api = _exercise(FileCalls(test, sandbox_id, test.config.api_url), api_directory)
    with test.context.phase("run every file operation through the data plane"):
        dp_directory = f"/tmp/{run}-dp"
        through_dp = _exercise(FileCalls(test, sandbox_id, test.config.data_plane_url), dp_directory)
    test.context.record("api_answers", through_api)
    test.context.record("data_plane_answers", through_dp)
    # The comparison only shows parity; the API's answers show it is parity
    # with a working route.
    statuses = [status for _, status, _ in through_api]
    test.context.verify(
        statuses == [200] * 11 + [404, 400, 400, 400, 200, 405],
        "the API's file routes did not answer as expected",
        statuses=statuses,
    )
    test.context.verify(
        through_api[-2][2] == "hello", "the API read the wrong bytes", read=through_api[-2][2]
    )
    for (action, api_status, api_body), (_, dp_status, dp_body) in zip(
        through_api, through_dp, strict=True
    ):
        test.context.verify(
            (api_status, api_body) == (dp_status, dp_body),
            f"files:{action} answered the data plane differently from the API",
            api_status=api_status,
            data_plane_status=dp_status,
            api_body=api_body,
            data_plane_body=dp_body,
        )
