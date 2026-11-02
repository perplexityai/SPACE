"""The Kind CI Rust artifact handoff's reuse and record/load decisions.

Never touches the Docker daemon: a fake `docker` on PATH answers from the
environment, and the checkout's input key is fixed (the shell test
test-kind-rust-input-key.sh owns how it is computed).
"""

import importlib.util
import json
import pathlib
import subprocess

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "infra/space/kind/scripts/rust-artifact-handoff.py"
spec = importlib.util.spec_from_file_location("kind_rust_artifact_handoff", SCRIPT)
handoff = importlib.util.module_from_spec(spec)
spec.loader.exec_module(handoff)

KEY = "a" * 64
MAIN_SHA = "b" * 40
IMAGE_ID = "sha256:" + "c" * 64
REPOSITORY = "registry.example/kind-rust-artifacts"
REFERENCE = f"{REPOSITORY}:{KEY}"

FAKE_DOCKER = """#!/usr/bin/env bash
printf '%s\\n' "$*" >>"$FAKE_DOCKER_LOG"
case "$1 $2" in
  "pull --quiet")
    [[ -z "${FAKE_PULL_SECONDS:-}" ]] || exec sleep "$FAKE_PULL_SECONDS"
    exit "${FAKE_PULL_STATUS:-0}" ;;
  "image inspect")
    case "$4" in
      "{{json .Config.Labels}}") printf '%s\\n' "$FAKE_LABELS" ;;
      "{{.Id}}") printf '%s\\n' "$FAKE_IMAGE_ID" ;;
    esac ;;
esac
"""


@pytest.fixture
def run(tmp_path, monkeypatch):
    """A GitHub run of this checkout, with a fake docker and fixed input key."""

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "docker").write_text(FAKE_DOCKER)
    (bin_dir / "docker").chmod(0o755)
    head = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    for name, value in {
        "PATH": f"{bin_dir}:{handoff.os.environ['PATH']}",
        "FAKE_DOCKER_LOG": str(tmp_path / "docker.log"),
        "FAKE_IMAGE_ID": IMAGE_ID,
        "FAKE_LABELS": "null",
        "GITHUB_REPOSITORY": "example/space",
        "GITHUB_SHA": head,
        "GITHUB_RUN_ID": "7",
        "GITHUB_RUN_ATTEMPT": "1",
        "GITHUB_OUTPUT": str(tmp_path / "output"),
        "GITHUB_ENV": str(tmp_path / "env"),
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(handoff, "input_key", lambda: KEY)
    (tmp_path / "docker.log").touch()
    (tmp_path / "output").touch()
    (tmp_path / "env").touch()
    return tmp_path


def labels(key=KEY, revision=MAIN_SHA) -> str:
    return json.dumps(
        {
            handoff.INPUT_KEY_LABEL: key,
            handoff.REVISION_LABEL: revision,
            "org.opencontainers.image.title": "space-rust-build-kind",
        }
    )


def test_reuse_takes_the_published_image_for_this_key(run, monkeypatch):
    monkeypatch.setenv("FAKE_LABELS", labels())
    handoff.reuse(REPOSITORY)
    assert (run / "output").read_text() == "reused=true\n"
    assert f"tag {REFERENCE} {handoff.IMAGE}" in (run / "docker.log").read_text()


def no_key():
    raise subprocess.CalledProcessError(1, ["bash", "--input-key"])


@pytest.mark.parametrize(
    "environment, input_key",
    [
        pytest.param({"FAKE_PULL_STATUS": "1"}, None, id="pull fails"),
        pytest.param({"FAKE_PULL_SECONDS": "5"}, None, id="pull stalls"),
        pytest.param({"FAKE_LABELS": labels(key="d" * 64)}, None, id="other key"),
        pytest.param({"FAKE_LABELS": labels(revision="")}, None, id="no revision"),
        pytest.param({"FAKE_LABELS": "null"}, None, id="no labels"),
        pytest.param({"FAKE_LABELS": labels()}, no_key, id="uncommitted inputs"),
    ],
)
def test_reuse_falls_back_to_the_build(run, monkeypatch, environment, input_key):
    monkeypatch.setenv("FAKE_LABELS", labels())
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    if input_key:
        monkeypatch.setattr(handoff, "input_key", input_key)
    monkeypatch.setattr(handoff, "PULL_TIMEOUT_SECONDS", 0.5)
    handoff.reuse(REPOSITORY)
    assert (run / "output").read_text() == ""
    log = (run / "docker.log").read_text().splitlines()
    assert not any(line.startswith("tag ") for line in log)


def stub_build(run, monkeypatch) -> pathlib.Path:
    """Replace prebuilt mode with a stub recording the handoff it is given."""

    seen = run / "prebuilt-env"
    stub = run / "build-rust-artifacts.sh"
    stub.write_text(
        f"env | grep '^SPACE_KIND_' | sort >{seen}\n",
    )
    monkeypatch.setattr(handoff, "BUILD", stub)
    return seen


def handed_off(run, monkeypatch) -> dict:
    archive = run / "handoff"
    archive.mkdir()
    (archive / "kind-rust.tar.gz").write_bytes(b"archive")
    handoff.record(archive)
    return json.loads((archive / "manifest.json").read_text())


def test_record_and_load_hand_off_a_reused_image(run, monkeypatch):
    monkeypatch.setenv("FAKE_LABELS", labels())
    manifest = handed_off(run, monkeypatch)
    assert manifest["built_from"] == MAIN_SHA
    assert manifest["input_key"] == KEY
    seen = stub_build(run, monkeypatch)
    handoff.load(run / "handoff")
    expected = {
        "SPACE_KIND_PREBUILT_RUST_IMAGE_ID": IMAGE_ID,
        "SPACE_KIND_PREBUILT_RUST_SHA": MAIN_SHA,
        "SPACE_KIND_PREBUILT_RUST_KEY": KEY,
    }
    prebuilt = dict(line.split("=", 1) for line in seen.read_text().splitlines())
    assert {name: prebuilt[name] for name in expected} == expected
    assert prebuilt["SPACE_KIND_RUST_ARTIFACTS"] == "prebuilt"
    assert (run / "env").read_text().splitlines() == [
        f"{name}={value}" for name, value in expected.items()
    ]


def test_record_and_load_hand_off_an_image_built_here(run, monkeypatch):
    head = handoff.os.environ["GITHUB_SHA"]
    for image_labels in ("null", labels(revision=head)):
        monkeypatch.setenv("FAKE_LABELS", image_labels)
        manifest = handed_off(run, monkeypatch)
        assert manifest["built_from"] == head
        assert manifest["input_key"] == ("" if image_labels == "null" else KEY)
        seen = stub_build(run, monkeypatch)
        handoff.load(run / "handoff")
        assert f"SPACE_KIND_PREBUILT_RUST_SHA={head}" in seen.read_text()
        (run / "handoff" / "manifest.json").unlink()
        (run / "handoff" / "kind-rust.tar.gz").unlink()
        (run / "handoff").rmdir()


@pytest.mark.parametrize(
    "field, value",
    [
        ("built_from", "main"),
        ("built_from", None),
        ("input_key", "not-a-key"),
        ("input_key", None),
    ],
)
def test_load_rejects_an_invalid_image_source(run, monkeypatch, field, value):
    monkeypatch.setenv("FAKE_LABELS", labels())
    manifest = handed_off(run, monkeypatch)
    manifest[field] = value
    (run / "handoff" / "manifest.json").write_text(json.dumps(manifest))
    seen = stub_build(run, monkeypatch)
    with pytest.raises(ValueError, match="invalid image source"):
        handoff.load(run / "handoff")
    assert not seen.exists()


def test_record_rejects_a_key_label_without_a_valid_revision(run, monkeypatch):
    monkeypatch.setenv("FAKE_LABELS", labels(revision="main"))
    with pytest.raises(ValueError, match="invalid input key or revision"):
        handed_off(run, monkeypatch)


class FakeClock:
    """Stands in for the handoff's `time`: sleeping advances it, nothing waits."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []
        # The timeout each `gh api` call was given.
        self.timeouts = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def gh(run, monkeypatch):
    """Answers each `gh api` call from a script; returns (script, clock)."""

    script = []
    clock = FakeClock()
    real_run = subprocess.run

    def fake_run(args, **kwargs):
        if list(args[:2]) != ["gh", "api"]:
            return real_run(args, **kwargs)
        clock.timeouts.append(kwargs.get("timeout"))
        answer = script.pop(0)
        if answer == "timeout":
            raise subprocess.TimeoutExpired(args, kwargs.get("timeout"))
        code, stderr, stdout = answer
        return subprocess.CompletedProcess(args, code, stdout, stderr)

    monkeypatch.setattr(handoff.subprocess, "run", fake_run)
    monkeypatch.setattr(handoff, "time", clock)
    return script, clock


def producer_jobs(conclusion="success"):
    job = {"name": handoff.PRODUCER, "status": "completed", "conclusion": conclusion}
    return (0, "", json.dumps([{"jobs": [job]}]))


def producer_upload(attempt=1):
    name = f"kind-rust-{handoff.os.environ['GITHUB_SHA']}-{attempt}"
    return (0, "", json.dumps([{"artifacts": [{"name": name, "expired": False}]}]))


def test_wait_rides_out_transient_api_failures(run, gh, capsys):
    script, clock = gh
    script.extend(
        [
            (1, "gh: Server Error (HTTP 502)", ""),
            "timeout",
            (1, "gh: API rate limit exceeded (HTTP 429)", ""),
            (1, "error connecting to api.github.com", ""),
            producer_jobs(),
            (1, "gh: Service Unavailable (HTTP 503)", ""),
            producer_upload(),
        ]
    )

    handoff.wait(3600)

    assert "SPACE_KIND_RUST_PRODUCER_ATTEMPT=1" in (run / "env").read_text()
    assert clock.sleeps == [5, 10, 20, 30, 5]
    out = capsys.readouterr().out
    assert "Retrying the Rust producer status lookup in 5 s after HTTP 502" in out
    assert f"after no answer within {handoff.API_CALL_SECONDS} s" in out
    assert "after a transport failure" in out
    assert "Retrying the Rust producer artifacts lookup in 5 s after HTTP 503" in out
    # Only the status is printed, never the remote error text.
    assert "Server Error" not in out and "rate limit exceeded" not in out


def test_wait_fails_at_once_when_actions_read_is_refused(gh):
    script, clock = gh
    script.append((1, "gh: Resource not accessible by integration (HTTP 403)", ""))

    with pytest.raises(RuntimeError, match=r"\(HTTP 403\); Actions read access is required"):
        handoff.wait(3600)
    assert clock.sleeps == []


def test_wait_gives_up_at_its_deadline_during_a_persistent_outage(gh):
    script, clock = gh
    script.extend([(1, "gh: Bad Gateway (HTTP 502)", "")] * 10)

    with pytest.raises(RuntimeError, match="before the wait's deadline: HTTP 502"):
        handoff.wait(40)
    # 5 + 10 + 20 s leaves less than the next 30 s pause before the deadline,
    # and the last call's timeout is capped at the 5 s left.
    assert clock.sleeps == [5, 10, 20]
    assert clock.timeouts == [30, 30, 25, 5]


def test_wait_refuses_a_failed_producer(gh):
    script, _ = gh
    script.append(producer_jobs(conclusion="failure"))

    with pytest.raises(RuntimeError, match="refusing local compilation fallback"):
        handoff.wait(3600)


def test_wait_rides_out_a_rate_limit_reported_as_403_and_a_brief_404(run, gh, capsys):
    script, clock = gh
    script.extend(
        [
            (1, "gh: API rate limit exceeded for installation ID 1 (HTTP 403)", ""),
            (1, "gh: Not Found (HTTP 404)", ""),
            producer_jobs(),
            producer_upload(),
        ]
    )

    handoff.wait(3600)

    assert clock.sleeps == [5, 10]
    out = capsys.readouterr().out
    assert "after HTTP 403 (rate limit)" in out and "after HTTP 404" in out
    assert "installation ID" not in out


@pytest.mark.parametrize(
    ("answer", "message"),
    [
        ((4, "To get started with GitHub CLI, please run:  gh auth login", ""), "gh has no authentication"),
        ((1, "gh: Validation Failed (HTTP 422)", ""), r"\(HTTP 422\)$"),
    ],
)
def test_wait_fails_at_once_on_a_failure_that_cannot_heal(gh, answer, message):
    script, clock = gh
    script.append(answer)

    with pytest.raises(RuntimeError, match=message):
        handoff.wait(3600)
    assert clock.sleeps == []
