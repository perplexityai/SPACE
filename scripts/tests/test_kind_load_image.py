"""Which build the Kind image loader tags and loads, against a fake Docker store.

Never touches Docker or a cluster: `docker`, `kind`, `kubectl` and `crictl`
answers come from an in-memory store of tags and images.
"""

import importlib.util
import json
import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "infra/space/kind/scripts/kind-load-image.py"
spec = importlib.util.spec_from_file_location("kind_load_image", SCRIPT)
loader = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = loader
spec.loader.exec_module(loader)

CLUSTER = "space-local"
NODE = "space-local-worker3"
BUILD_REF = "space-api:tilt-build-1"


def image(n: int) -> dict:
    """An image as the containerd store inspects it: ID is the manifest digest."""
    return {
        "Id": f"sha256:{str(n) * 64}",
        "Descriptor": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "annotations": {"config.digest": f"sha256:{'abcdef'[n] * 64}"},
        },
    }


class FakeDocker:
    """Images by ID and the tags naming them.

    `keeps_untagged`: the graph-driver store keeps an image whose last tag
    moved; the containerd image store deletes it. `after_inspect` runs once,
    after the first inspect.
    """

    def __init__(self, keeps_untagged: bool, after_inspect=None):
        self.images: dict[str, dict] = {}
        self.tags: dict[str, str] = {}
        self.keeps_untagged = keeps_untagged
        self.after_inspect = after_inspect
        self.loaded: list[str] = []

    def tag(self, ref: str, built: dict) -> None:
        old = self.tags.get(ref)
        self.images[built["Id"]] = built
        self.tags[ref] = built["Id"]
        if old and old not in self.tags.values() and not self.keeps_untagged:
            del self.images[old]

    def resolve(self, ref: str) -> dict:
        image_id = self.tags.get(ref, ref)
        if image_id not in self.images:
            raise subprocess.CalledProcessError(1, ["docker"], stderr=f"No such image: {ref}")
        return self.images[image_id]

    def run(self, args, timeout=60):
        if args[:3] == ["docker", "image", "inspect"]:
            found = self.resolve(args[-1])
            if self.after_inspect:
                hook, self.after_inspect = self.after_inspect, None
                hook(self)
            return json.dumps([found])
        if args[:2] == ["docker", "tag"]:
            self.tag(args[3], self.resolve(args[2]))
            return ""
        if args[:3] == ["kind", "get", "nodes"]:
            return NODE + "\n"
        if args[0] == "kubectl":
            return json.dumps({"items": [{"metadata": {"name": NODE, "labels": {}}}]})
        if args[:2] == ["docker", "exec"]:
            raise subprocess.CalledProcessError(1, args, stderr="no such image")
        raise AssertionError(f"unexpected command {args}")

    def kind_load(self, args, **_):
        self.loaded.append(args[-1])
        return subprocess.CompletedProcess(args, 0)


def load(monkeypatch, tmp_path, fake: FakeDocker, n: int) -> tuple[int, str | None]:
    monkeypatch.setattr(loader, "run", fake.run)
    monkeypatch.setattr(loader.subprocess, "run", fake.kind_load)
    monkeypatch.delenv(loader.PLACEMENT_ENV, raising=False)
    ref_file = tmp_path / f"space-api-{n}"
    status = loader.main(["--cluster", CLUSTER, "--image", "space-api",
                          "--build-ref", BUILD_REF, "--ref-file", str(ref_file)])
    return status, ref_file.read_text().strip() if ref_file.exists() else None


def own_ref(built: dict) -> str:
    return loader.content_ref("space-api", built["Descriptor"]["annotations"]["config.digest"])


@pytest.mark.parametrize("keeps_untagged", [True, False], ids=["graph-driver", "containerd"])
def test_a_build_tag_replaced_mid_load_does_not_cross_tag(monkeypatch, tmp_path, keeps_untagged):
    # Two invocations share a build tag; the second build retags it between the
    # first's inspect and its tag. Whatever reference each deploys must name its
    # own build, and the first build's reference must never name the second's.
    first, second = image(1), image(2)
    fake = FakeDocker(keeps_untagged, after_inspect=lambda fake: fake.tag(BUILD_REF, second))
    fake.tag(BUILD_REF, first)

    first_status, first_ref = load(monkeypatch, tmp_path, fake, 1)
    second_status, second_ref = load(monkeypatch, tmp_path, fake, 2)

    assert fake.images.get(fake.tags.get(own_ref(first))) in (None, first)
    assert (second_status, second_ref) == (0, own_ref(second))
    assert fake.resolve(second_ref) == second
    if keeps_untagged:
        # The first build still exists, untagged: it is the one tagged and loaded.
        assert (first_status, first_ref) == (0, own_ref(first))
        assert fake.resolve(first_ref) == first
        assert fake.loaded == [first_ref, second_ref]
    else:
        # The first build is gone with its tag: its load fails, deploying nothing.
        assert (first_status, first_ref) == (2, None)
        assert fake.loaded == [second_ref]
