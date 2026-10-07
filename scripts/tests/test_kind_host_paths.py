#!/usr/bin/env python3
"""Guards for the kind harness's host-path resolution.

The defect these cover: the cluster config hardcoded `/home/dev`, while the
script that stages the Firecracker vendor tree defaulted to `$HOME`. Under any
other username the mount and the tree were different directories, and Docker
answers a missing bind-mount source by creating an empty directory instead of
failing — so the cluster came up Ready and the first sandbox died looking like
a Firecracker fault. Nothing about a healthy cluster caught it, so these tests
assert where the paths land rather than that the harness runs.
"""

from __future__ import annotations

import os
import pathlib
import re
import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
KIND_DIR = REPO_ROOT / "infra/space/kind"
TEMPLATE = KIND_DIR / "kind-cluster.yaml.tmpl"
LIB = KIND_DIR / "scripts/lib/kind-host-paths.sh"
BOOTSTRAP = KIND_DIR / "scripts/bootstrap.sh"
INSTALL_FC_VENDOR = KIND_DIR / "scripts/install-fc-vendor.sh"
SMOKE_BOOTSTRAP = REPO_ROOT / "scripts/tests/lib/bootstrap-kind-stack.sh"

FC_VERSION = "v1.15.1"

# The escalation branch in install-fc-vendor.sh is gated on the vendor root
# being unwritable *by us*. Root writes through mode 0555, so as root that
# branch is unreachable and any assertion about what it does would be
# asserting something false. CI runs as root — `scripts/tests/test-credmgr-dek.py`
# requires it — so those cases are skipped there rather than inverted.
ROOT_CANNOT_TEST_ESCALATION = (
    "the escalation branch needs a root the running user cannot write; "
    "root writes through mode 0555, so the branch is unreachable"
)

# What the config bind-mounted before the paths were parameterised. A devbox
# renders exactly this set, which is the whole no-regression claim.
DEVBOX_HOST_PATHS = {
    "/home/dev/.kind-data/control-plane/containerd",
    "/home/dev/.kind-data/worker1/containerd",
    "/home/dev/.kind-data/worker2/containerd",
    "/home/dev/.kind-data/worker3/containerd",
    "/home/dev/.kind-data/builder/containerd",
    "/home/dev/.kind-data/builder2/containerd",
    "/home/dev/.space-devbox/space/vendor",
    "/dev/kvm",
}


def run_bash(
    script: str, *, env: dict[str, str], check: bool = True, cwd: str | None = None
):
    """Run `script` with the host-path library already sourced.

    `cwd` matters whenever the script can act on a relative path: the default
    is this repository, so a guard that stops creating one would otherwise
    write into the working tree. Tests that assert nothing was created pass a
    directory they own.
    """
    return subprocess.run(
        ["bash", "-euo", "pipefail", "-c", f'source "$1"\n{script}', "_", str(LIB)],
        env=env,
        text=True,
        capture_output=True,
        check=check,
        cwd=cwd,
    )


def base_env(home: str, **overrides: str) -> dict[str, str]:
    """A PATH-preserving environment with every SPACE_* host-path knob unset."""
    env = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            "HOME",
            "SPACE_VENDOR_ROOT",
            "SPACE_KIND_DATA_ROOT",
            "SPACE_KIND_CONFIG",
            # Rendered into the node images; the default is the tree's own.
            "SPACE_KIND_NODE_IMAGE",
            "SPACE_KIND_BASE_NODE_IMAGE",
            # The vendor default chains through these, so an ambient value
            # would silently move what the tests assert about.
            "SPACE_DEVBOX_PERSIST_ROOT",
            "SPACE_DEVBOX_SPACE_TARGET",
        }
    }
    env["HOME"] = home
    env.update(overrides)
    return env


def shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"


def render(env: dict[str, str], dest: Path, template: Path = TEMPLATE) -> dict:
    run_bash(
        "kind_render_cluster_config "
        f"{shell_quote(str(template))} {shell_quote(str(dest))}",
        env=env,
    )
    return yaml.safe_load(dest.read_text())


def host_paths_from_yaml(config: dict) -> set[str]:
    return {
        mount["hostPath"]
        for node in config["nodes"]
        for mount in node.get("extraMounts", [])
    }


def stage_fake_image_vendor(root: Path) -> None:
    """A stand-in for the devbox-space image's /opt/space/vendor tree.

    install-fc-vendor.sh prefers copying from it over downloading, which is
    what lets this run offline. The firecracker stub answers `--version` the
    way the real binary does, because the script re-checks the version on a
    second run.
    """
    binaries = root / f"firecracker/{FC_VERSION}/bin"
    binaries.mkdir(parents=True)
    for name in ("firecracker", "jailer"):
        stub = binaries / name
        stub.write_text(
            textwrap.dedent(
                f"""\
                #!/usr/bin/env bash
                printf 'Firecracker {FC_VERSION}\\n'
                """
            )
        )
        stub.chmod(0o755)
    kernel = root / "kernels/vmlinux-6.1.0"
    kernel.mkdir(parents=True)
    (kernel / "vmlinux.bin").write_text("not-a-real-kernel\n")


class RenderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(
            os.environ.get("TMPDIR", "/tmp"),
            f"kind-host-paths-{os.getpid()}-{self.id().rsplit('.', 1)[-1]}",
        )
        shutil.rmtree(self.tmp, ignore_errors=True)
        self.tmp.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def refuse_render_with(self, **overrides: str):
        """Render with the given root overrides; return the failed result.

        The rejections fire when a root is about to be used, not when the
        library is sourced — sourcing must stay usable for a caller that
        supplies only the one root it reads.
        """
        return run_bash(
            "kind_render_cluster_config "
            f"{shell_quote(str(TEMPLATE))} {shell_quote(str(self.tmp / 'out.yaml'))}",
            env=base_env("/home/test-user", **overrides),
            check=False,
        )

    def test_home_dev_renders_the_paths_the_config_used_to_hardcode(self) -> None:
        dest = self.tmp / "rendered.yaml"
        config = render(base_env("/home/dev"), dest)
        self.assertEqual(host_paths_from_yaml(config), DEVBOX_HOST_PATHS)

    def test_another_username_moves_every_mount_into_that_home(self) -> None:
        dest = self.tmp / "rendered.yaml"
        config = render(base_env("/home/test-user"), dest)
        self.assertEqual(
            host_paths_from_yaml(config),
            {
                path.replace("/home/dev/", "/home/test-user/")
                for path in DEVBOX_HOST_PATHS
            },
        )
        # The mount the workers read Firecracker through is the one that
        # silently emptied, so name it rather than leaning on the set compare.
        self.assertIn(
            "/home/test-user/.space-devbox/space/vendor", host_paths_from_yaml(config)
        )

    def test_the_vendor_default_follows_the_devbox_persist_root(self) -> None:
        """Persisted-state overrides must move the vendor staging path and mount.

        The shared path resolver derives the tree from
        `${SPACE_DEVBOX_SPACE_TARGET:-${SPACE_DEVBOX_PERSIST_ROOT:-$HOME/.space-devbox}/space}/vendor`
        and stages there. Re-deriving `~/.space-devbox` here instead left an
        operator who moved their devbox state with the writer on the moved
        path and the mount on the original one — the same writer/reader split,
        through a knob this library did not consult.
        """
        for env_overrides, expected in (
            ({"SPACE_DEVBOX_PERSIST_ROOT": "/mnt/x"}, "/mnt/x/space/vendor"),
            ({"SPACE_DEVBOX_SPACE_TARGET": "/mnt/y/space"}, "/mnt/y/space/vendor"),
            (
                {
                    "SPACE_DEVBOX_PERSIST_ROOT": "/mnt/x",
                    "SPACE_DEVBOX_SPACE_TARGET": "/mnt/y/space",
                },
                "/mnt/y/space/vendor",
            ),
        ):
            with self.subTest(**env_overrides):
                dest = self.tmp / "rendered.yaml"
                config = render(base_env("/home/dev", **env_overrides), dest)
                self.assertIn(expected, host_paths_from_yaml(config))

    def test_an_explicit_vendor_root_outranks_the_devbox_knobs(self) -> None:
        dest = self.tmp / "rendered.yaml"
        config = render(
            base_env(
                "/home/dev",
                SPACE_DEVBOX_PERSIST_ROOT="/mnt/x",
                SPACE_VENDOR_ROOT="/mnt/explicit/vendor",
            ),
            dest,
        )
        paths = host_paths_from_yaml(config)
        self.assertIn("/mnt/explicit/vendor", paths)
        self.assertNotIn("/mnt/x/space/vendor", paths)

    def test_explicit_roots_win_over_home(self) -> None:
        dest = self.tmp / "rendered.yaml"
        config = render(
            base_env(
                "/home/test-user",
                SPACE_VENDOR_ROOT="/mnt/space/vendor",
                SPACE_KIND_DATA_ROOT="/mnt/kind-data",
            ),
            dest,
        )
        paths = host_paths_from_yaml(config)
        self.assertIn("/mnt/space/vendor", paths)
        self.assertIn("/mnt/kind-data/worker1/containerd", paths)
        self.assertEqual([path for path in paths if "/home/" in path], [])

    def test_a_placeholder_the_renderer_does_not_know_is_refused(self) -> None:
        # Non-vacuity for the leftover-placeholder guard: without it this
        # template would reach `kind create` with a literal `${...}` hostPath
        # for Docker to bind-mount as a directory of that name.
        template = self.tmp / "kind-cluster.yaml.tmpl"
        template.write_text(
            TEMPLATE.read_text().replace(
                "- hostPath: ${SPACE_VENDOR_ROOT}",
                "- hostPath: ${SPACE_SOMETHING_NEW}",
                1,
            )
        )
        dest = self.tmp / "rendered.yaml"
        result = run_bash(
            f"kind_render_cluster_config {shell_quote(str(template))} {shell_quote(str(dest))}",
            env=base_env("/home/test-user"),
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("SPACE_SOMETHING_NEW", result.stderr)
        self.assertFalse(dest.exists(), "a refused render must leave no config")

    def test_a_root_containing_a_placeholder_like_string_renders_literally(self) -> None:
        """`${...}` in a root is data, not a placeholder.

        The leftover-placeholder guard used to scan the *rendered output*, so a
        root such as `/mnt/${USER}/vendor` — which a single-quoted knob in a
        Makefile or CI YAML produces — was reported as a template defect that
        did not exist. That is the same misdiagnosis `__sed_escape` was added
        to avoid, so the guard now checks the template instead.
        """
        dest = self.tmp / "braced.yaml"
        config = render(
            base_env("/home/dev", SPACE_VENDOR_ROOT="/mnt/${weird}/vendor"), dest
        )
        self.assertIn("/mnt/${weird}/vendor", host_paths_from_yaml(config))

    def test_a_root_containing_a_placeholder_the_renderer_knows_is_refused(
        self,
    ) -> None:
        """Literal `${...}` is fine; a placeholder the renderer expands is not.

        The substitutions are two `sed` expressions over one stream, so the
        second rescans what the first inserted. A data root spelling
        `${SPACE_VENDOR_ROOT}` therefore renders the vendor root inside it,
        `kind_prepare_host_mounts` creates that directory, and
        `kind_assert_live_cluster_mounts` compares the live source against the
        unexpanded spelling forever — drift on a cluster the harness just
        built correctly, with a recreate remedy that can never clear it.

        Only the data-then-vendor ordering is exposed today. Both spellings
        are refused in both roots, so reordering those two expressions cannot
        reopen it.
        """
        for name, value in (
            ("SPACE_KIND_DATA_ROOT", "/mnt/${SPACE_VENDOR_ROOT}/data"),
            ("SPACE_VENDOR_ROOT", "/mnt/${SPACE_KIND_DATA_ROOT}/vendor"),
            ("SPACE_VENDOR_ROOT", "/mnt/${SPACE_VENDOR_ROOT}/vendor"),
        ):
            with self.subTest(root=name, value=value):
                result = self.refuse_render_with(**{name: value})
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("placeholder the renderer substitutes", result.stderr)

    def test_a_root_containing_sed_metacharacters_renders_literally(self) -> None:
        # `&` in sed replacement text re-inserts the whole match, so an
        # unescaped root rendered `hostPath: /mnt/a${SPACE_VENDOR_ROOT}b/...`
        # and then died from the leftover-placeholder guard — fail-closed with
        # a diagnosis blaming the template for the caller's path.
        dest = self.tmp / "rendered.yaml"
        config = render(
            base_env("/home/test-user", SPACE_VENDOR_ROOT="/mnt/a&b/vendor"),
            dest,
        )
        self.assertIn("/mnt/a&b/vendor", host_paths_from_yaml(config))

    def test_a_root_containing_the_sed_delimiter_is_refused(self) -> None:
        result = self.refuse_render_with(SPACE_VENDOR_ROOT="/mnt/a|b/vendor")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must not contain '|'", result.stderr)

    def test_a_root_with_a_yaml_comment_character_is_refused(self) -> None:
        # Verified against a real YAML parse: `hostPath: /mnt/vendor #1` loads
        # as `/mnt/vendor`, so the mount would use a path the vendor tree was
        # never staged at — the writer/reader split, re-entered through the
        # knob that was meant to close it.
        result = self.refuse_render_with(SPACE_VENDOR_ROOT="/mnt/vendor #1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must not contain '#'", result.stderr)

    def test_a_root_with_trailing_whitespace_is_refused(self) -> None:
        # Verified against PyYAML: `hostPath: /a/b ` loads as `/a/b`. So the
        # node would mount the trimmed path while the installer staged the
        # tree at the space-bearing one — the split, one invisible character
        # wide.
        result = self.refuse_render_with(SPACE_VENDOR_ROOT="/mnt/vendor ")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must not end with whitespace", result.stderr)

    def test_a_root_with_an_interior_space_still_renders(self) -> None:
        # Non-vacuity for the test above: only leading and trailing
        # whitespace is hostile. A path with a space in the middle is
        # legitimate and YAML keeps it, so rejecting it would be a
        # regression, not a guard.
        dest = self.tmp / "spaced.yaml"
        config = render(
            base_env("/home/dev", SPACE_VENDOR_ROOT="/mnt/my data/vendor"), dest
        )
        self.assertIn("/mnt/my data/vendor", host_paths_from_yaml(config))

    def test_a_root_with_a_tab_is_refused(self) -> None:
        # A tab is a hard YAML ScannerError, and it is the separator
        # kind_config_mounts emits, so it would also split a parsed pair.
        result = self.refuse_render_with(SPACE_VENDOR_ROOT="/mnt/a\tb/vendor")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must not contain a tab", result.stderr)

    def test_a_root_with_a_newline_is_refused(self) -> None:
        """One line in, one line out.

        The substitution is a line-oriented `sed`, so an embedded newline
        splits one `hostPath:` into a key and a stray document line. YAML
        then reads a mount this harness never staged, or fails to parse at a
        line the operator did not write.
        """
        result = self.refuse_render_with(SPACE_VENDOR_ROOT="/mnt/a\nb/vendor")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must not contain a newline", result.stderr)

    def test_a_root_with_a_quote_is_refused(self) -> None:
        """Quotes change what a YAML plain scalar means.

        The root lands in a plain scalar, and `kind_config_mounts` strips
        quotes when reading the config back, so a quoted character would be
        present for `kind create` and absent for every comparison this
        library makes against the live cluster.
        """
        result = self.refuse_render_with(SPACE_VENDOR_ROOT='/mnt/a"b/vendor')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must not contain a quote", result.stderr)

    def test_a_root_with_a_carriage_return_is_refused(self) -> None:
        """A CRLF-sourced env file would otherwise render a trailing \r.

        Docker reports a mount source back verbatim, so the `\r` survives into
        `.Source` and the staged path compares unequal to it forever — the
        recreate-a-healthy-cluster outcome, triggered by an invisible
        character.
        """
        result = self.refuse_render_with(SPACE_VENDOR_ROOT="/mnt/ab/vendor\r")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must not contain a carriage return", result.stderr)

    def test_a_root_with_a_colon_is_refused(self) -> None:
        result = self.refuse_render_with(SPACE_KIND_DATA_ROOT="/mnt/a:b")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must not contain ':'", result.stderr)

    def test_a_trailing_slash_resolves_to_the_same_paths(self) -> None:
        # Both spellings are legitimate input. Before normalizing, bootstrapping
        # once with the slash and again without it compared unequal against the
        # live cluster's recorded mount and demanded a recreate for a cluster
        # that was correct.
        plain = self.tmp / "plain.yaml"
        slashed = self.tmp / "slashed.yaml"
        render(
            base_env(
                "/home/test-user",
                SPACE_VENDOR_ROOT="/mnt/vendor",
                SPACE_KIND_DATA_ROOT="/mnt/data",
            ),
            plain,
        )
        render(
            base_env(
                "/home/test-user",
                SPACE_VENDOR_ROOT="/mnt/vendor///",
                SPACE_KIND_DATA_ROOT="/mnt/data/",
            ),
            slashed,
        )
        self.assertEqual(slashed.read_text(), plain.read_text())
        self.assertNotIn("//", "\n".join(host_paths_from_yaml(yaml.safe_load(slashed.read_text()))))

    def test_a_dot_dot_component_is_refused(self) -> None:
        """`/tmp/..` lands on `/` while passing a literal `/` comparison.

        Verified with a sudo stub before the fix: it produced
        `chown -R <user> /tmp/..`, a recursive rewrite of the whole
        filesystem, and the installer's own `created_root != /` guard compares
        literally so it did not catch this spelling either.
        """
        for root in ("/tmp/..", "/mnt/x/../..", "/../"):
            with self.subTest(root=root):
                result = self.refuse_render_with(SPACE_VENDOR_ROOT=root)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("must not contain a '..' component", result.stderr)

    def test_a_symlink_to_the_root_is_refused(self) -> None:
        # No `..`, not literally `/`, absolute — and it still lands there.
        link = self.tmp / "rootlink"
        link.symlink_to("/")
        result = self.refuse_render_with(SPACE_VENDOR_ROOT=str(link))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("resolves to the filesystem root", result.stderr)

    def test_a_normal_relative_looking_root_still_works(self) -> None:
        # Non-vacuity for the two above: rejecting `..` must not reject a
        # perfectly ordinary absolute path that merely contains dots.
        dest = self.tmp / "dots.yaml"
        config = render(
            base_env("/home/dev", SPACE_VENDOR_ROOT="/mnt/my.dir/vendor..old"), dest
        )
        self.assertIn("/mnt/my.dir/vendor..old", host_paths_from_yaml(config))

    def test_internal_duplicate_separators_are_canonicalised(self) -> None:
        """`/a//b` and `/a/./b` name `/a/b`, so they must resolve to it.

        Only trailing slashes were stripped, while the comment promised the
        spellings were interchangeable against a live cluster's recorded
        mount. They were not: bootstrapping with one and re-running with
        another compared unequal and demanded a recreate for a healthy
        cluster — the outcome normalization exists to prevent.
        """
        for spelling in ("/mnt/v//vendor", "/mnt/v/./vendor", "/mnt///v//./vendor/"):
            with self.subTest(root=spelling):
                dest = self.tmp / "norm.yaml"
                config = render(
                    base_env("/home/dev", SPACE_VENDOR_ROOT=spelling), dest
                )
                self.assertIn("/mnt/v/vendor", host_paths_from_yaml(config))

    def test_the_filesystem_root_is_refused(self) -> None:
        """`/` must not be usable as a root.

        `$UNSET_VAR/` produces it, and so does the trailing-slash
        normalization from `//`. It is absolute and contains no rejected
        character, so it used to reach `install-fc-vendor.sh`, where
        `[[ ! -w / ]]` holds for any non-root user and the escalation became
        `sudo chown -R <user> /` — verified with a sudo stub, for `/`, `//`
        and `///`. That recursively rewrites the root filesystem and strips
        setuid from sudo itself, so there is no second escalation to undo it.

        Refusing it by name also settles what the old normalization test was
        worried about: `/` does not reach the absolute-path check and fail
        there with a message about something the operator did not type.
        """
        for root in ("/", "//", "///"):
            with self.subTest(root=root):
                result = self.refuse_render_with(SPACE_VENDOR_ROOT=root)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("not the filesystem root itself", result.stderr)

    def test_a_relative_root_is_refused_before_it_is_rendered(self) -> None:
        result = self.refuse_render_with(SPACE_VENDOR_ROOT="relative/vendor")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("absolute path", result.stderr)


class RenderedCommentTests(unittest.TestCase):
    """The rendered config's own documentation has to survive the render.

    `sed` substitutes inside comments too. A knob named as `${SPACE_...}` in
    prose therefore renders as its value, and the template's knob table came
    out as "/home/you/.kind-data default ~/.kind-data" — documentation that
    actively misleads whoever opens the generated config. The template names
    its knobs bare so every comment line renders byte-identical.
    """

    def test_comment_lines_render_unchanged(self) -> None:
        tmp = Path(os.environ.get("TMPDIR", "/tmp"), f"kind-comments-{os.getpid()}")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, tmp, True)
        dest = tmp / "rendered.yaml"
        render(base_env("/home/test-user"), dest)

        def comments(text: str) -> list[str]:
            return [line for line in text.splitlines() if line.lstrip().startswith("#")]

        template_comments = comments(TEMPLATE.read_text())
        self.assertTrue(template_comments, "the template is expected to be commented")
        self.assertEqual(comments(dest.read_text()), template_comments)


class LiveClusterMountTests(unittest.TestCase):
    """A cluster that already exists keeps the mounts it was created with.

    Docker fixes bind mounts at container-create time and `kind create` is the
    only reader of the config, so staging the tree and passing the preflight
    still leaves a pre-existing cluster mounting the old path. Without this
    check the repair reports success and the first sandbox fails anyway.
    """

    def setUp(self) -> None:
        self.tmp = Path(
            os.environ.get("TMPDIR", "/tmp"),
            f"kind-live-{os.getpid()}-{self.id().rsplit('.', 1)[-1]}",
        )
        shutil.rmtree(self.tmp, ignore_errors=True)
        self.tmp.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.tmp, True)
        # The check counts the mounts the config declares, so it needs one.
        self.rendered = self.tmp / "rendered.yaml"
        render(base_env("/home/test-user"), self.rendered)

    def env_with_docker_stub(
        self,
        vendor_source: str,
        data_source_root: str = "/home/test-user/.kind-data",
        *,
        mountless: bool = False,
        kvm: bool = True,
        kvm_source: str = "/dev/kvm",
        extra_mount: str | None = None,
    ) -> dict[str, str]:
        """PATH-shadow `docker` with a stub standing in for a real cluster.

        The rendered config declares six `/var/lib/containerd` mounts, four
        `/space/vendor` and four `/dev/kvm`, and the check counts every
        destination the config names, so the stub reports that shape rather
        than a single node. `mountless=True` reports the six node containers
        with no mounts at all — a cluster created without this config.
        `kvm=False` reports the vendor and containerd mounts but no device,
        which is the shape a hardcoded pair of destinations used to exempt.
        """
        stub_dir = self.tmp / "bin"
        stub_dir.mkdir(exist_ok=True)
        nodes = [
            ("space-local-control-plane", False),
            ("space-local-worker", True),
            ("space-local-worker2", True),
            ("space-local-worker3", False),
            ("space-local-worker4", True),
            ("space-local-worker5", True),
        ]
        arms = []
        for index, (name, has_vendor) in enumerate(nodes, start=1):
            if mountless:
                body = "            :"
            else:
                lines = [
                    f"            printf '/var/lib/containerd {data_source_root}/n{index}/containerd\\n'"
                ]
                if has_vendor:
                    lines.append(f"            printf '/space/vendor {vendor_source}\\n'")
                    if kvm:
                        lines.append(
                            f"            printf '/dev/kvm {kvm_source}\\n'"
                        )
                if extra_mount:
                    lines.append(f"            printf '{extra_mount}\\n'")
                body = "\n".join(lines)
            arms.append(f"          {name})\n{body}\n            ;;")
        stub = stub_dir / "docker"
        stub.write_text(
            "#!/usr/bin/env bash\n"
            'case "$1" in\n'
            "  ps)\n"
            + "".join(f"    printf '{name}\\n'\n" for name, _ in nodes)
            + "    ;;\n"
            "  inspect)\n"
            '    case "$2" in\n'
            + "\n".join(arms)
            + "\n    esac\n"
            "    ;;\n"
            "esac\n"
        )
        stub.chmod(0o755)
        env = base_env("/home/test-user")
        env["PATH"] = f"{stub_dir}:{env['PATH']}"
        env["SPACE_KIND_CONFIG"] = str(self.rendered)
        return env

    def test_an_undeclared_live_mount_is_not_judged(self) -> None:
        """The provider's own binds are not this config's business.

        The per-mount switch keyed on the *live* destination, so any `/dev/*`
        mount a node happened to carry was asserted to have `src == dest` —
        with a message claiming the config renders it, and a printed remedy
        (recreate the cluster) that cannot work, because the provider adds it
        again on the next create. kind ships `/dev/mapper` and `/lib/modules`;
        a rootless provider adding `/dev/null:/dev/kmsg` would have wedged
        every bring-up.
        """
        env = self.env_with_docker_stub(
            "/home/test-user/.space-devbox/space/vendor", extra_mount="/dev/kmsg /dev/null"
        )
        run_bash("kind_assert_live_cluster_mounts space-local", env=env)

    def test_a_cluster_without_the_device_mounts_is_refused(self) -> None:
        """A cluster missing /dev/kvm cannot run a sandbox.

        The vendor and containerd mounts agree, so a check that counted only
        those two destinations reported success; bootstrap then skipped
        creation and the node-stack pods could not start, because the chart
        requires /dev/kvm as a CharDevice. The count now covers every
        destination the config declares.
        """
        result = run_bash(
            "kind_assert_live_cluster_mounts space-local",
            env=self.env_with_docker_stub(
                "/home/test-user/.space-devbox/space/vendor", kvm=False
            ),
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("0 of 4 expected /dev/kvm mounts", result.stderr)
        self.assertIn("kind delete cluster --name space-local", result.stderr)

    def test_a_device_mount_backed_by_something_else_is_refused(self) -> None:
        """/dev/kvm has to be the device, not a file standing where it goes.

        The count is satisfied by any mount at that destination, so a node
        binding a regular file — or a rootless provider's `/dev/null` — onto
        `/dev/kvm` passed with the right count and the wrong device. The pods
        then start, because the chart's `CharDevice` check is on the node
        path that now exists, and Firecracker fails on ioctl instead. The
        `src == dest` arm is what distinguishes them, and the missing-device
        case is caught by the count rather than by this arm, so it needed its
        own assertion.
        """
        result = run_bash(
            "kind_assert_live_cluster_mounts space-local",
            env=self.env_with_docker_stub(
                "/home/test-user/.space-devbox/space/vendor",
                kvm_source="/home/test-user/not-a-device",
            ),
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("/dev/kvm", result.stderr)
        self.assertIn("/home/test-user/not-a-device", result.stderr)
        self.assertIn("kind delete cluster --name space-local", result.stderr)

    def test_a_cluster_on_the_old_vendor_literal_is_refused(self) -> None:
        """The check for the defect this branch exists to fix.

        A cluster created before the paths were parameterised still mounts
        `/home/dev/.space-devbox/space/vendor` while the config now renders a
        different root. Docker fixes bind mounts at container-create time, so
        that cluster can never pick the new path up — and every other guard
        passes, because the mount *counts* agree and only the source differs.

        Every stub call site used to pass the agreeing source, so this arm
        could be deleted outright and the suite stayed green: verified by
        neutering it in place, 50 passed.
        """
        result = run_bash(
            "kind_assert_live_cluster_mounts space-local",
            env=self.env_with_docker_stub("/home/dev/.space-devbox/space/vendor"),
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("/space/vendor <- /home/dev/.space-devbox/space/vendor", result.stderr)
        self.assertIn("renders /home/test-user/.space-devbox/space/vendor", result.stderr)
        self.assertIn("kind delete cluster --name space-local", result.stderr)
        # Four nodes declare it, and each is named so the operator can see the
        # scope rather than inferring it.
        self.assertEqual(result.stderr.count("/space/vendor <-"), 4)

    def test_a_cluster_on_a_stale_data_root_is_refused(self) -> None:
        # The vendor root agrees, so a vendor-only check reports success while
        # the nodes keep the old containerd storage — SPACE_KIND_DATA_ROOT
        # silently doing nothing, which is the failure this library exists to
        # remove.
        result = run_bash(
            "kind_assert_live_cluster_mounts space-local",
            env=self.env_with_docker_stub(
                "/home/test-user/.space-devbox/space/vendor",
                data_source_root="/home/dev/.kind-data",
            ),
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("/var/lib/containerd", result.stderr)
        self.assertIn("/home/dev/.kind-data/", result.stderr)
        self.assertIn("renders under /home/test-user/.kind-data", result.stderr)
        self.assertIn("kind delete cluster --name space-local", result.stderr)
        # The vendor root agrees, so it must not be reported as drifted.
        self.assertNotIn("/space/vendor <-", result.stderr)

    def test_a_cluster_mounting_the_rendered_root_passes(self) -> None:
        # Non-vacuity for the test above: the same stub with the agreeing
        # source must not trip the check, or it would fail every devbox whose
        # cluster is fine.
        run_bash(
            'kind_assert_live_cluster_mounts space-local',
            env=self.env_with_docker_stub("/home/test-user/.space-devbox/space/vendor"),
        )

    def test_a_cluster_created_without_this_config_is_refused(self) -> None:
        """A reused cluster with no extraMounts at all must not be accepted.

        The source comparison can only disagree with a mount that exists, so
        this cluster passed it untouched and `bootstrap.sh` skipped creation —
        failing two minutes later in `apply_node_labels`, blaming labels for a
        cluster that simply was not built from this config.
        """
        result = run_bash(
            "kind_assert_live_cluster_mounts space-local",
            env=self.env_with_docker_stub(
                "/home/test-user/.space-devbox/space/vendor", mountless=True
            ),
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("0 of 6 expected /var/lib/containerd mounts", result.stderr)
        self.assertIn("0 of 4 expected /space/vendor mounts", result.stderr)
        self.assertIn("kind delete cluster --name space-local", result.stderr)

    def test_no_docker_on_path_is_not_a_failure(self) -> None:
        # A box without docker must not fail here: the check has nothing to
        # inspect, and `bootstrap.sh` already refuses an unreachable daemon
        # with a message about that. PATH carries bash alone, since with
        # docker absent the function runs only shell builtins.
        bash = shutil.which("bash")
        assert bash is not None
        only_bash = self.tmp / "only-bash"
        only_bash.mkdir(exist_ok=True)
        (only_bash / "bash").symlink_to(bash)
        env = base_env("/home/test-user")
        env["PATH"] = str(only_bash)
        run_bash("kind_assert_live_cluster_mounts space-local", env=env)


class HostPathExtractionTests(unittest.TestCase):
    """The library hand-parses hostPaths; hold that against a real parser.

    `kind_prepare_host_mounts` acts on this extraction, so a config shape the
    hand parser misses is a mount that is silently never created or verified —
    the same class of failure as the bug itself.
    """

    def test_a_trailing_comment_is_not_part_of_the_mount_source(self) -> None:
        """` # note` after a hostPath is a YAML comment, not path characters.

        A real parser drops it, so without the strip the library's extraction
        and YAML disagree: the source gains ` # note`, which matches no live
        mount and no staged directory. The template carries no such line
        today, which is exactly why nothing else notices if the strip goes.
        """
        config = Path(
            os.environ.get("TMPDIR", "/tmp"), f"kind-comment-{os.getpid()}.yaml"
        )
        self.addCleanup(config.unlink, True)
        config.write_text(
            "kind: Cluster\n"
            "nodes:\n"
            "  - role: worker\n"
            "    extraMounts:\n"
            "      - hostPath: /mnt/vendor # staged by install-fc-vendor.sh\n"
            "        containerPath: /space/vendor\n"
        )

        pairs = run_bash(
            f"kind_config_mounts {shell_quote(str(config))}",
            env=base_env("/home/test-user"),
        ).stdout.splitlines()

        self.assertEqual(pairs, ["/space/vendor\t/mnt/vendor"])
        parsed = yaml.safe_load(config.read_text())
        self.assertEqual(
            parsed["nodes"][0]["extraMounts"][0]["hostPath"],
            "/mnt/vendor",
            "and that is what a real YAML parser reads, which is the point",
        )

    def test_the_template_keeps_to_the_shapes_the_parser_handles(self) -> None:
        """Enforce what the context-free extraction assumes.

        It cannot tell a node extraMount from a `hostPath:` inside a
        `kubeadmConfigPatches` block — that one would be demanded to exist on
        the host, which a node-internal path never will — and it does not see
        flow-style sequences at all, which is the original silently-skipped
        mount. Both are ordinary kind idioms, so adding either has to fail
        here rather than at someone's bring-up.
        """
        text = TEMPLATE.read_text()
        # Anchored to a key, not a substring: a comment explaining why the
        # template cannot use one must not itself trip the guard.
        self.assertIsNone(
            re.search(r"^\s*kubeadmConfigPatches:", text, re.MULTILINE)
        )
        for number, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith("extraMounts:"):
                with self.subTest(line=number, shape="flow sequence"):
                    self.assertEqual(
                        stripped,
                        "extraMounts:",
                        "flow-style extraMounts are invisible to "
                        "kind_config_mounts",
                    )
            if stripped.startswith("- {") or stripped.startswith("-{"):
                with self.subTest(line=number, shape="flow mapping"):
                    self.fail(
                        "a flow mapping in a block sequence is invisible to "
                        "kind_config_mounts; write one key per line"
                    )

    def test_either_key_order_parses(self) -> None:
        """`- containerPath:` first is ordinary YAML and must not be dropped.

        The two awk rules were asymmetric — only the hostPath rule allowed the
        list dash — so a mount written containerPath-first produced no pair at
        all. A dropped `/space/vendor` is the dangerous direction: the preflight
        never checks it and the drift check expects zero of it, so any cluster
        passes, which is the original defect.
        """
        tmp = Path(os.environ.get("TMPDIR", "/tmp"), f"kind-order-{os.getpid()}")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, tmp, True)
        config = tmp / "reversed.yaml"
        config.write_text(
            "nodes:\n"
            "  - role: worker\n"
            "    extraMounts:\n"
            "      - containerPath: /space/vendor\n"
            "        hostPath: /host/vendor\n"
            "      - hostPath: /host/cd\n"
            "        containerPath: /var/lib/containerd\n"
        )
        pairs = run_bash(
            f"kind_config_mounts {shell_quote(str(config))}",
            env=base_env("/home/test-user"),
        ).stdout.splitlines()
        self.assertEqual(
            sorted(pairs),
            ["/space/vendor\t/host/vendor", "/var/lib/containerd\t/host/cd"],
        )

    def test_a_quoted_host_path_loses_its_quotes(self) -> None:
        # A quoted scalar is valid YAML the parser would otherwise hand on
        # with its quotes attached, tripping the absolute-path assertion with
        # a message about something the operator did not write.
        tmp = Path(os.environ.get("TMPDIR", "/tmp"), f"kind-quoted-{os.getpid()}")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, tmp, True)
        config = tmp / "quoted.yaml"
        config.write_text(
            "nodes:\n"
            "  - role: worker\n"
            "    extraMounts:\n"
            '      - hostPath: "/quoted/path"\n'
            "        containerPath: /c\n"
        )
        extracted = run_bash(
            f"kind_config_mounts {shell_quote(str(config))} | cut -f2 | sort -u",
            env=base_env("/home/test-user"),
        ).stdout.splitlines()
        self.assertEqual(extracted, ["/quoted/path"])

    def test_hand_parsed_host_paths_match_a_yaml_parse(self) -> None:
        tmp = Path(os.environ.get("TMPDIR", "/tmp"), f"kind-hostpaths-{os.getpid()}")
        shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, tmp, True)
        dest = tmp / "rendered.yaml"
        config = render(base_env("/home/test-user"), dest)

        extracted = run_bash(
            f"kind_config_mounts {shell_quote(str(dest))} | cut -f2 | sort -u",
            env=base_env("/home/test-user"),
        ).stdout.splitlines()  # not .split(): a path may contain a space

        self.assertEqual(set(extracted), host_paths_from_yaml(config))

        # The pair parser is what classification keys on, so hold it against
        # the same real walk — including multiplicity, which the mount-count
        # check depends on.
        pairs = run_bash(
            f"kind_config_mounts {shell_quote(str(dest))}",
            env=base_env("/home/test-user"),
        ).stdout.splitlines()
        expected_pairs = sorted(
            f"{mount['containerPath']}\t{mount['hostPath']}"
            for node in config["nodes"]
            for mount in node.get("extraMounts", [])
        )
        self.assertEqual(sorted(pairs), expected_pairs)

    def test_no_template_host_path_is_an_absolute_home_literal(self) -> None:
        # A hostPath written out by hand is the single edit that re-splits the
        # harness, and it would render identically on the devbox reviewing it.
        # /dev/kvm is the one legitimate absolute: it is a host device, not a
        # per-user tree.
        for path in host_paths_from_yaml(yaml.safe_load(TEMPLATE.read_text())):
            with self.subTest(host_path=path):
                self.assertTrue(
                    path.startswith("${") or path == "/dev/kvm",
                    f"{path} is a fixed host path; root it at a placeholder",
                )


class PrepareHostMountsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(
            os.environ.get("TMPDIR", "/tmp"),
            f"kind-prepare-{os.getpid()}-{self.id().rsplit('.', 1)[-1]}",
        )
        shutil.rmtree(self.tmp, ignore_errors=True)
        self.tmp.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.home = self.tmp / "home/notdev"
        self.home.mkdir(parents=True)
        self.dest = self.tmp / "rendered.yaml"
        self.env = base_env(str(self.home))

    def stage_vendor(self) -> pathlib.Path:
        """Stage the vendor tree the way a correct box has it.

        The preflight checks the binary a worker executes, not the directory,
        so a fixture that only mkdir's the root is the rejected case rather
        than the working one.
        """
        root = self.home / ".space-devbox/space/vendor"
        binary = root / "firecracker/default/bin/firecracker"
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_text("#!/bin/sh\nexit 0\n")
        binary.chmod(0o755)
        return root

    def prepare(self, check: bool = True):
        render(self.env, self.dest)
        return run_bash(
            f"kind_prepare_host_mounts {shell_quote(str(self.dest))}",
            env=self.env,
            check=check,
        )

    def test_an_unmountable_hostpath_in_the_config_is_refused(self) -> None:
        """The roots are validated on the way in; the config is validated too.

        Rendering can only produce hostPaths built from the two roots, so this
        arm looks redundant — but `kind_prepare_host_mounts` takes a config
        *path*, and the documented flows include a hand-edited config and a
        future template with a mount neither root drives. Without it, such a
        path goes straight to `mkdir -p`/`kind create` having skipped every
        check the knobs get, which is how the roots came to need checking.
        """
        render(self.env, self.dest)
        # A relative hostPath: absolute is the first thing assert_mountable
        # demands, and it is the one un-mountable spelling whose side effect
        # is observable — `mkdir -p` would resolve it against the process's
        # working directory. Run from a directory this test owns, so a guard
        # that stopped refusing writes into the fixture and not the repo.
        text = self.dest.read_text().splitlines()
        for index, line in enumerate(text):
            if "hostPath:" in line and "/dev/" not in line:
                indent = line[: len(line) - len(line.lstrip())]
                text[index] = f"{indent}hostPath: relative/not/absolute"
                break
        else:
            self.fail("the rendered config declares no non-device hostPath")
        self.dest.write_text("\n".join(text) + "\n")

        result = run_bash(
            f"kind_prepare_host_mounts {shell_quote(str(self.dest))}",
            env=self.env,
            check=False,
            cwd=str(self.tmp),
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("hostPath in", result.stderr)
        self.assertIn("absolute", result.stderr)
        self.assertFalse(
            (self.tmp / "relative/not/absolute").exists(),
            "a refused hostPath must not be created",
        )

    def test_a_missing_vendor_tree_is_refused_instead_of_bind_mounted_empty(
        self,
    ) -> None:
        """Refuse, do not create — read as a pair with the empty-dir test.

        This pins "refuse, and do not create the source", which is what its
        name claims: with the `/space/vendor` arm deleted the path falls
        through to the generic existence check, which still refuses and still
        creates nothing. `test_an_empty_vendor_directory_is_refused` is what
        pins the arm itself.

        Off Linux the `returncode != 0` assertion is carried by the absent
        `/dev/kvm` rather than by the vendor root, so the vendor-specific
        assertions below are what make this meaningful on a macOS host.
        """
        vendor = self.home / ".space-devbox/space/vendor"
        result = self.prepare(check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(vendor), result.stderr)
        self.assertFalse(
            vendor.exists(),
            "the preflight must not create the source Docker would have emptied",
        )

    def test_an_empty_vendor_directory_is_refused(self) -> None:
        """A directory is not a staged tree.

        The missing-path error names the vendor root, and the obvious response
        to "does not exist" is `mkdir -p` on it. An existence check accepts
        that, `kind create` succeeds, and four nodes mount an empty
        /space/vendor — a Ready cluster whose first sandbox fails as if
        Firecracker were broken, which is the whole defect. So the check is
        the binary the node will execute.
        """
        vendor = self.home / ".space-devbox/space/vendor"
        vendor.mkdir(parents=True)  # exactly what the previous error invites
        render(self.env, self.dest)
        self.dest.write_text(
            self.dest.read_text().replace(
                "- hostPath: /dev/kvm", "- hostPath: /dev/null"
            )
        )

        result = run_bash(
            f"kind_prepare_host_mounts {shell_quote(str(self.dest))}",
            env=self.env,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no Firecracker binary", result.stderr)
        self.assertIn("Creating the directory is not enough", result.stderr)

    def test_a_staged_vendor_tree_passes(self) -> None:
        # Non-vacuity for the test above: a tree with the binary in place must
        # pass, or the check would refuse every correct box.
        self.stage_vendor()
        render(self.env, self.dest)
        self.dest.write_text(
            self.dest.read_text().replace(
                "- hostPath: /dev/kvm", "- hostPath: /dev/null"
            )
        )
        run_bash(
            f"kind_prepare_host_mounts {shell_quote(str(self.dest))}", env=self.env
        )

    def test_a_directory_at_a_device_path_is_refused(self) -> None:
        """Existence is not enough for a device mount.

        Docker answers a missing bind source by creating a directory — the
        behaviour this library exists to defend against — so a bring-up on a
        host without KVM is itself how `/dev/kvm` becomes a directory. An
        existence test then blesses it forever, while the node-stack chart's
        `type: CharDevice` makes kubelet refuse the volume and the pod never
        starts.
        """
        self.stage_vendor()
        fake_device = self.tmp / "fakekvm"
        fake_device.mkdir()
        render(self.env, self.dest)
        self.dest.write_text(
            self.dest.read_text().replace(
                "- hostPath: /dev/kvm", f"- hostPath: {fake_device}"
            )
        )

        result = run_bash(
            f"kind_prepare_host_mounts {shell_quote(str(self.dest))}",
            env=self.env,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(f"{fake_device} is a directory", result.stderr)
        # Named once, not once per declaring node.
        self.assertEqual(result.stderr.count(f"{fake_device} is a"), 1)

    def test_a_character_device_at_a_device_path_passes(self) -> None:
        # Non-vacuity for the test above: a real device must not be rejected.
        # /dev/null is a character device on every platform this runs on.
        self.stage_vendor()
        self.assertTrue(pathlib.Path("/dev/null").is_char_device())
        render(self.env, self.dest)
        self.dest.write_text(
            self.dest.read_text().replace(
                "- hostPath: /dev/kvm", "- hostPath: /dev/null"
            )
        )
        run_bash(
            f"kind_prepare_host_mounts {shell_quote(str(self.dest))}", env=self.env
        )

    def test_the_refusal_names_only_the_remedy_that_applies(self) -> None:
        # Printing both told a host with no KVM to stage a vendor tree, and a
        # host with an unstaged tree that it cannot run Firecracker. Here the
        # device stand-in exists and the vendor tree does not, so only the
        # staging remedy is correct.
        render(self.env, self.dest)
        self.dest.write_text(
            self.dest.read_text().replace(
                "- hostPath: /dev/kvm", "- hostPath: /dev/null"
            )
        )
        result = run_bash(
            f"kind_prepare_host_mounts {shell_quote(str(self.dest))}",
            env=self.env,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("install-fc-vendor.sh", result.stderr)
        self.assertNotIn("cannot run Firecracker", result.stderr)

    def test_a_vendor_root_nested_under_the_data_root_is_still_refused(self) -> None:
        """Classification must key on the mount's destination, not its source.

        `SPACE_KIND_DATA_ROOT=/mnt/kind` with `SPACE_VENDOR_ROOT=/mnt/kind/vendor`
        is a reasonable "everything on the big disk" layout, and the README
        advertises both knobs with no disjointness rule. A source-prefix test
        put the vendor root in the create-it arm, so the preflight created it
        empty and the workers mounted it — this library's own defect, reached
        through its own two knobs.
        """
        data_root = self.tmp / "mnt"
        vendor = data_root / "vendor"
        kvm = self.tmp / "kvm"
        kvm.write_text("")
        env = base_env(
            str(self.home),
            SPACE_KIND_DATA_ROOT=str(data_root),
            SPACE_VENDOR_ROOT=str(vendor),
        )
        render(env, self.dest)
        self.dest.write_text(
            self.dest.read_text().replace("- hostPath: /dev/kvm", f"- hostPath: {kvm}")
        )

        result = run_bash(
            f"kind_prepare_host_mounts {shell_quote(str(self.dest))}",
            env=env,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(
            vendor.exists(), "the nested vendor root must not be created empty"
        )
        # The containerd dirs under the same root are still created: they are
        # the mounts that legitimately start empty.
        self.assertTrue((data_root / "worker1/containerd").is_dir())
        # Reported once, not once per node that mounts it.
        self.assertEqual(result.stderr.count(str(vendor) + "\n"), 1)

    def test_containerd_dirs_are_created_and_a_staged_vendor_tree_passes(self) -> None:
        self.stage_vendor()
        # /dev/kvm exists on a Linux/KVM host and not on this one, so point
        # the config at /dev/null: a real character device, which is what the
        # preflight requires of a device mount. A regular file would be
        # rejected, and rightly — that is the stale-directory case.
        render(self.env, self.dest)
        # Only the hostPath — a blanket replace would rewrite the containerPath
        # too and the fixture would stop standing in for a device mount.
        self.dest.write_text(
            self.dest.read_text().replace(
                "- hostPath: /dev/kvm", "- hostPath: /dev/null"
            )
        )
        run_bash(
            f"kind_prepare_host_mounts {shell_quote(str(self.dest))}", env=self.env
        )
        for role in (
            "control-plane",
            "worker1",
            "worker2",
            "worker3",
            "builder",
            "builder2",
        ):
            self.assertTrue(
                (self.home / ".kind-data" / role / "containerd").is_dir(),
                f"{role} containerd storage dir was not created",
            )


class InstallerAgreementTests(unittest.TestCase):
    """The writer and the mount have to resolve to one directory.

    This is the acceptance criterion the issue names, checked without a
    cluster: stage the vendor tree the way `bootstrap.sh` does under a
    non-`dev` home, then read the rendered config and confirm the tree landed
    at the path the workers will mount.
    """

    def setUp(self) -> None:
        self.tmp = Path(
            os.environ.get("TMPDIR", "/tmp"),
            f"kind-installer-{os.getpid()}-{self.id().rsplit('.', 1)[-1]}",
        )
        shutil.rmtree(self.tmp, ignore_errors=True)
        self.tmp.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.home = self.tmp / "home/test-user"
        self.home.mkdir(parents=True)
        self.image_vendor = self.tmp / "opt/space/vendor"
        stage_fake_image_vendor(self.image_vendor)
        self.env = base_env(
            str(self.home), SPACE_IMAGE_VENDOR_ROOT=str(self.image_vendor)
        )

    def install(self):
        return subprocess.run(
            ["bash", str(INSTALL_FC_VENDOR)],
            env=self.env,
            text=True,
            capture_output=True,
            check=True,
        )

    def test_the_installed_tree_is_the_tree_the_config_mounts(self) -> None:
        self.install()
        config = yaml.safe_load(
            subprocess.run(
                [
                    "bash",
                    "-euo",
                    "pipefail",
                    "-c",
                    'source "$1"; kind_render_cluster_config "$2" "$3"; cat "$3"',
                    "_",
                    str(LIB),
                    str(TEMPLATE),
                    str(self.tmp / "rendered.yaml"),
                ],
                env=self.env,
                text=True,
                capture_output=True,
                check=True,
            ).stdout
        )
        vendor_mounts = {
            mount["hostPath"]
            for node in config["nodes"]
            for mount in node.get("extraMounts", [])
            if mount["containerPath"] == "/space/vendor"
        }
        self.assertEqual(len(vendor_mounts), 1, "one vendor root drives every node")
        mounted = Path(vendor_mounts.pop())

        # The exact predicate bootstrap.sh gates staging on, applied to the
        # path the workers read. Before the fix it was false here and true
        # nowhere, so the installer re-ran on every invocation and the workers
        # mounted an empty directory.
        self.assertTrue(
            os.access(mounted / "firecracker/default/bin/firecracker", os.X_OK)
        )
        self.assertTrue((mounted / "firecracker/default/kernel/vmlinux.bin").is_file())
        self.assertEqual(mounted, self.home / ".space-devbox/space/vendor")

    def test_a_fresh_vendor_root_is_staged_without_escalating(self) -> None:
        """A directory inside the caller's own home needs no sudo.

        Not skipped as root, because the assertion stays true there — but it
        carries force only as a non-root user, since root would not have
        escalated regardless.

        `-w` is false for a root that merely doesn't exist yet, which is every
        box that isn't a provisioned devbox, so the unprivileged `mkdir -p`
        has to come first. The stub makes the difference observable: a real
        sudo on a passwordless CI runner would let an escalating script pass
        this test silently.
        """
        stub_dir = self.tmp / "bin"
        stub_dir.mkdir()
        marker = self.tmp / "sudo-was-called"
        stub = stub_dir / "sudo"
        stub.write_text(
            textwrap.dedent(
                f"""\
                #!/usr/bin/env bash
                printf '%s\\n' "$*" >>{shell_quote(str(marker))}
                exit 1
                """
            )
        )
        stub.chmod(0o755)
        self.env["PATH"] = f"{stub_dir}:{self.env['PATH']}"

        self.install()

        self.assertFalse(
            marker.exists(),
            f"install-fc-vendor.sh escalated: {marker.read_text() if marker.exists() else ''}",
        )

    def test_an_explicit_root_needs_no_home(self) -> None:
        """Sourcing the library must not fail over a root nobody reads.

        Before this library existed the installer read
        `${SPACE_VENDOR_ROOT:-$HOME/...}`, so an explicit root meant `$HOME`
        was never evaluated and the script ran with HOME unset. Resolving all
        three roots eagerly broke that: the run died on
        `SPACE_KIND_DATA_ROOT`, which the installer never touches. No in-repo
        caller runs without HOME today — this pins the property so absorbing
        scripts into the library cannot quietly narrow where they work.
        """
        env = {k: v for k, v in self.env.items() if k != "HOME"}
        env["SPACE_VENDOR_ROOT"] = str(self.home / "explicit-vendor")

        subprocess.run(
            ["bash", str(INSTALL_FC_VENDOR)],
            env=env,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertTrue(
            os.access(
                self.home / "explicit-vendor/firecracker/default/bin/firecracker",
                os.X_OK,
            )
        )

    def test_an_underivable_root_fails_naming_the_knob(self) -> None:
        # Non-vacuity for the test above: with HOME unset AND no explicit
        # root there is nothing to derive, and the failure has to name which
        # knob is missing rather than report an empty path.
        env = {k: v for k, v in self.env.items() if k != "HOME"}
        result = subprocess.run(
            ["bash", str(INSTALL_FC_VENDOR)],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("SPACE_VENDOR_ROOT is unset", result.stderr)

    def recording_sudo(self) -> tuple[pathlib.Path, pathlib.Path]:
        """A `sudo` on PATH that records its argv instead of escalating."""
        stub_dir = self.tmp / "sudobin"
        stub_dir.mkdir(exist_ok=True)
        log = self.tmp / "sudo.log"
        stub = stub_dir / "sudo"
        stub.write_text(
            textwrap.dedent(
                f"""\
                #!/usr/bin/env bash
                printf '%s\\n' "$*" >>{shell_quote(str(log))}
                exit 0
                """
            )
        )
        stub.chmod(0o755)
        return stub_dir, log

    @unittest.skipIf(os.geteuid() == 0, ROOT_CANNOT_TEST_ESCALATION)
    def test_an_unwritable_root_outside_home_is_not_escalated(self) -> None:
        """The escalation is bounded; the root is not.

        The `created_root` walk leaves the target at `$VENDOR_ROOT` whenever
        that root already exists, so a pre-existing unwritable root was
        chowned recursively whatever it was — `SPACE_VENDOR_ROOT=/usr/local`
        rewrites ownership of `/usr/local/bin`, which is
        privilege-escalation-shaped rather than merely destructive.

        Refusing such a root outright would break the "everything on the big
        disk" layout this harness documents, so the *escalation* is what gets
        refused, with the command handed to the operator.
        """
        outside = self.tmp / "usrlocal"
        outside.mkdir()
        outside.chmod(0o555)
        self.addCleanup(outside.chmod, 0o755)
        stub_dir, log = self.recording_sudo()
        env = dict(self.env)
        env["PATH"] = f"{stub_dir}:{env['PATH']}"
        env["SPACE_VENDOR_ROOT"] = str(outside)

        result = subprocess.run(
            ["bash", str(INSTALL_FC_VENDOR)],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Refusing to escalate", result.stderr)
        self.assertIn(f"sudo chown -R", result.stderr)
        self.assertFalse(log.exists(), f"sudo was invoked: {log.read_text() if log.exists() else ''}")

    @unittest.skipIf(os.geteuid() == 0, ROOT_CANNOT_TEST_ESCALATION)
    def test_the_chown_targets_the_created_ancestor_not_the_leaf(self) -> None:
        """What gets chowned is what this run created, all of it.

        The property, in operator terms: after a run that had to escalate, no
        directory it created is left owned by root. Chowning only
        `$VENDOR_ROOT` satisfies the escalation and still leaves the
        intermediate `.../space` root-owned — which is the state the runbook
        tells operators to repair by hand, reintroduced one level up. The
        walk's whole purpose is this difference, so it needs its own
        assertion: deleting the walk is caught elsewhere, but changing the
        chown argument alone was not.
        """
        persist = self.home / ".space-devbox"
        persist.mkdir()
        persist.chmod(0o555)
        self.addCleanup(persist.chmod, 0o755)
        vendor_root = persist / "space/vendor"
        stub_dir, log = self.recording_sudo()
        env = dict(self.env)
        env["PATH"] = f"{stub_dir}:{env['PATH']}"
        env["SPACE_VENDOR_ROOT"] = str(vendor_root)

        subprocess.run(
            ["bash", str(INSTALL_FC_VENDOR)],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertTrue(log.exists(), "this case is the documented escalation")
        chowns = [
            line.split()
            for line in log.read_text().splitlines()
            if line.startswith("chown")
        ]
        self.assertEqual(len(chowns), 1, f"one chown, not a sweep: {chowns}")
        chowned = chowns[0][-1]
        self.assertEqual(
            chowned,
            str(persist / "space"),
            "the topmost directory this run creates, so nothing it made stays root-owned",
        )
        self.assertNotEqual(
            chowned, str(vendor_root), "chowning only the leaf is the bug the walk exists for"
        )

    @unittest.skipIf(os.geteuid() == 0, ROOT_CANNOT_TEST_ESCALATION)
    def test_a_symlink_out_of_home_is_not_escalated(self) -> None:
        """The bound is on the resolved path, not the spelling.

        A symlinked component makes a path look like it is inside `$HOME`
        while physically pointing elsewhere, and `chown -R` follows the
        physical path. Verified before the fix: `$HOME/mount/usrlocal` with
        `$HOME/mount` -> an outside tree passed the string-prefix check and
        reached sudo.
        """
        outside = self.tmp / "real/usrlocal"
        outside.mkdir(parents=True)
        outside.chmod(0o555)
        self.addCleanup(outside.chmod, 0o755)
        (self.home / "mount").symlink_to(self.tmp / "real")
        stub_dir, log = self.recording_sudo()
        env = dict(self.env)
        env["PATH"] = f"{stub_dir}:{env['PATH']}"
        env["SPACE_VENDOR_ROOT"] = str(self.home / "mount/usrlocal")

        result = subprocess.run(
            ["bash", str(INSTALL_FC_VENDOR)],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Refusing to escalate", result.stderr)
        # The resolved target, so the operator sees what they would chown.
        self.assertIn("resolves to", result.stderr)
        self.assertFalse(
            log.exists(),
            f"sudo was invoked: {log.read_text() if log.exists() else ''}",
        )

    @unittest.skipIf(os.geteuid() == 0, ROOT_CANNOT_TEST_ESCALATION)
    def test_the_home_directory_itself_is_not_escalated(self) -> None:
        """Equality with the owned prefix is not a licence to chown it.

        The bound admits `target == owned` so that a `SPACE_DEVBOX_PERSIST_ROOT`
        which does not exist yet can be created. `SPACE_VENDOR_ROOT=$HOME`
        collapses onto the same equality while *already existing*, and
        admitting it unconditionally made the escalation
        `sudo chown -R <user> $HOME` — the whole home tree, `.ssh` included.
        The existing in-home test uses a strict descendant, so it passes
        either way and does not separate these.
        """
        # Named `.ssh` because that is the tree the docstring is about: it
        # makes the fixture carry the thing the refused escalation would have
        # reached. It also has to not be a name the repository export gate
        # treats as the repo's private `docs/` — a public test joining a path
        # into a denylisted directory reads as a boundary violation, and this
        # fixture is exported.
        (self.home / ".ssh").mkdir()
        self.home.chmod(0o555)
        self.addCleanup(self.home.chmod, 0o755)
        stub_dir, log = self.recording_sudo()
        env = dict(self.env)
        env["PATH"] = f"{stub_dir}:{env['PATH']}"
        env["SPACE_VENDOR_ROOT"] = str(self.home)

        result = subprocess.run(
            ["bash", str(INSTALL_FC_VENDOR)],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(
            log.exists(),
            f"sudo was invoked on $HOME: {log.read_text() if log.exists() else ''}",
        )

    @unittest.skipIf(os.geteuid() == 0, ROOT_CANNOT_TEST_ESCALATION)
    def test_a_persist_root_that_does_not_exist_yet_is_created(self) -> None:
        # The other half of the equality arm, and the case it was added for:
        # SPACE_DEVBOX_PERSIST_ROOT under an unwritable parent. The walk stops
        # at the persist root because its parent exists and it does not, so
        # target == owned — and here that must be allowed.
        parent = self.tmp / "mnt"
        parent.mkdir()
        parent.chmod(0o555)
        self.addCleanup(parent.chmod, 0o755)
        stub_dir, log = self.recording_sudo()
        env = dict(self.env)
        env["PATH"] = f"{stub_dir}:{env['PATH']}"
        env.pop("SPACE_VENDOR_ROOT", None)
        env["SPACE_DEVBOX_PERSIST_ROOT"] = str(parent / "devbox-state")

        subprocess.run(
            ["bash", str(INSTALL_FC_VENDOR)],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertTrue(log.exists(), "the documented persist-root case must escalate")
        self.assertIn(str(parent / "devbox-state"), log.read_text())

    @unittest.skipIf(os.geteuid() == 0, ROOT_CANNOT_TEST_ESCALATION)
    def test_an_unwritable_root_inside_home_still_escalates(self) -> None:
        # Non-vacuity for the bound: the devbox platform pre-provisions a
        # root-owned ~/.space-devbox/space/vendor, and that case must still be
        # handed over rather than refused.
        parent = self.home / ".space-devbox/space"
        parent.mkdir(parents=True)
        parent.chmod(0o555)
        self.addCleanup(parent.chmod, 0o755)
        stub_dir, log = self.recording_sudo()
        env = dict(self.env)
        env["PATH"] = f"{stub_dir}:{env['PATH']}"

        subprocess.run(
            ["bash", str(INSTALL_FC_VENDOR)],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertTrue(log.exists(), "the in-home case must still escalate")
        self.assertIn("chown -R", log.read_text())

    def test_a_second_install_restages_nothing(self) -> None:
        self.install()
        binary = (
            self.home
            / f".space-devbox/space/vendor/firecracker/{FC_VERSION}/bin/firecracker"
        )
        kernel = (
            self.home
            / f".space-devbox/space/vendor/firecracker/{FC_VERSION}/kernel/vmlinux.bin"
        )
        before = (binary.stat().st_mtime_ns, kernel.stat().st_mtime_ns)

        second = self.install()

        self.assertEqual(
            (binary.stat().st_mtime_ns, kernel.stat().st_mtime_ns),
            before,
            "re-running the installer rewrote an already-staged tree",
        )
        self.assertIn("already installed", second.stdout)
        self.assertIn("vmlinux already present", second.stdout)


class RenderWrapperTests(unittest.TestCase):
    """`render-cluster-config.sh` prints a path for an operator to paste."""

    def test_the_config_it_renders_is_not_the_path_it_publishes(self) -> None:
        """A second run must not be able to swap what this caller creates from.

        The caller runs `kind create` after this script has exited, so what
        the script hands back has to be a file no other run writes.
        Publishing atomically does not cover this: atomicity rules out a
        half-written file, not a fully-written *other* file appearing at a
        shared path between the print and the read.

        Scope: this asserts the render *target* is private, which is the half
        that is observable without `/dev/kvm` — the preflight refuses here and
        names the config it was given. That the printed path is that same
        target is only reachable on a host that can pass the preflight, so it
        is not covered here.
        """
        tmp = Path(
            os.environ.get("TMPDIR", "/tmp"), f"kind-wrapper-priv-{os.getpid()}"
        )
        shutil.rmtree(tmp, ignore_errors=True)
        published = tmp / "home/.space-devbox/kind/kind-cluster.yaml"
        published.parent.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, tmp, True)

        result = subprocess.run(
            ["bash", str(KIND_DIR / "scripts/render-cluster-config.sh")],
            env=base_env(str(tmp / "home")),
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0, "no vendor tree exists here")
        match = re.search(r"(\S*kind-cluster-run\.\S+)", result.stderr)
        self.assertIsNotNone(
            match,
            "the preflight named no rendered config, so this test proves "
            f"nothing about where the render went.\n{result.stderr}",
        )
        rendered = Path(match.group(1))
        self.assertNotEqual(rendered, published)
        self.assertNotEqual(
            rendered.parent,
            published.parent,
            "a run file beside the published config is one an operator can "
            "paste by mistake",
        )
        self.assertFalse(
            rendered.exists(), f"the failed run left its render behind: {rendered}"
        )

    def test_a_failed_run_publishes_nothing_and_preserves_what_is_there(self) -> None:
        """$SPACE_KIND_CONFIG is per-user, so a failure must not touch it.

        Two properties in one, both about that path being shared. Nothing
        unverified may be published there — an operator who fixes the missing
        tree and reuses the path from their scrollback would otherwise build
        from the render that preceded the fix. And nothing already there may be
        removed: deleting it on failure, which is what the earlier EXIT trap
        did, could take out the config a `bootstrap.sh` in a sibling worktree
        had just rendered and not yet handed to `kind create`.
        """
        tmp = Path(os.environ.get("TMPDIR", "/tmp"), f"kind-wrapper-{os.getpid()}")
        shutil.rmtree(tmp, ignore_errors=True)
        published = tmp / "home/.space-devbox/kind/kind-cluster.yaml"
        published.parent.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, tmp, True)
        sentinel = "# standing in for a sibling worktree's rendered config\n"
        published.write_text(sentinel)
        env = base_env(str(tmp / "home"))

        result = subprocess.run(
            ["bash", str(KIND_DIR / "scripts/render-cluster-config.sh")],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0, "no vendor tree exists here")
        self.assertEqual(result.stdout, "", "a failed run must print no path")
        self.assertEqual(
            published.read_text(),
            sentinel,
            "a failed run must neither overwrite nor delete the shared config",
        )


class PhysicalResolverTests(unittest.TestCase):
    """`kind_host_paths__physical` bounds every privileged side effect.

    The library designates it as the one resolver for comparisons that gate a
    side effect, so a wrong answer here is a wrong bound on `sudo chown -R`.
    These pin the two answers that were wrong: an empty input, and a target
    whose parent is missing too.
    """

    def setUp(self) -> None:
        self.tmp = Path(
            os.environ.get("TMPDIR", "/tmp"),
            f"kind-physical-{os.getpid()}-{self.id().rsplit('.', 1)[-1]}",
        )
        shutil.rmtree(self.tmp, ignore_errors=True)
        self.tmp.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def physical(self, target: str) -> str:
        return subprocess.run(
            [
                "bash",
                "-euo",
                "pipefail",
                "-c",
                'source "$1"; kind_host_paths__physical "$2"',
                "_",
                str(LIB),
                target,
            ],
            env=base_env(str(self.tmp / "home")),
            cwd=str(self.tmp),
            text=True,
            capture_output=True,
            check=True,
        ).stdout.strip()

    def test_an_empty_target_resolves_to_nothing_not_the_working_directory(
        self,
    ) -> None:
        """Empty in, empty out — so the caller's `-z` check can fire.

        `dirname ""` is `.`, so re-appending the leaf to the resolved parent
        answered "$PWD/". A caller bounding `chown -R` by that value bounds it
        by wherever the process happens to be running, while reporting the
        bound as `$HOME`.
        """
        self.assertEqual(self.physical(""), "")

    def test_a_target_whose_parent_is_also_missing_keeps_its_whole_path(self) -> None:
        """Walk up to the deepest existing ancestor, not just one level.

        A single `dirname` left the leaf rooted at `/` when the parent was
        missing too, so the resolver returned a string that no longer named
        the target at all — and `/leaf` is under no home, which turns the
        documented one-time mkdir into a refusal.
        """
        anchor = self.tmp / "anchor"
        anchor.mkdir()
        deep = anchor / "missing/deeper/leaf"

        resolved = self.physical(str(deep))

        self.assertTrue(
            resolved.endswith("/anchor/missing/deeper/leaf"),
            f"every component below the existing ancestor survives: {resolved}",
        )
        self.assertEqual(
            resolved,
            self.physical(str(anchor)) + "/missing/deeper/leaf",
            "the existing ancestor resolves exactly as it does on its own",
        )

    def test_a_missing_path_directly_under_root_gains_no_double_slash(self) -> None:
        """The ancestor walk can land on `/`, which already ends in a slash.

        Appending to it unnormalized yields `//name`, and Docker reports a
        mount source back verbatim, so a `//` that reaches a comparison can
        never match.
        """
        self.assertEqual(
            self.physical("/kind-host-paths-no-such-top/x"),
            "/kind-host-paths-no-such-top/x",
        )

    def test_an_existing_directory_still_resolves_through_its_symlinks(self) -> None:
        """The fast path stays physical, not lexical.

        Seeded with a real symlink rather than asserting on a path that is
        already its own physical form, so the resolution is observable.
        """
        real = self.tmp / "real/tree"
        real.mkdir(parents=True)
        (self.tmp / "link").symlink_to(self.tmp / "real")

        through_link = self.physical(str(self.tmp / "link/tree"))

        self.assertEqual(through_link, self.physical(str(real)))
        self.assertNotIn("/link/", through_link)


class BringUpConfigPrivacyTests(unittest.TestCase):
    """A bring-up must create from a config no other process can replace.

    The operator-visible property: two bring-ups on one box must not be able
    to hand each other's host roots to `kind create`. The window is real
    because the vendor staging between the render and the create takes about a
    minute, and the outcome is silent — the cluster comes up with every mount
    it declares present, just rooted somewhere this run never staged.
    """

    def setUp(self) -> None:
        self.tmp = Path(
            os.environ.get("TMPDIR", "/tmp"),
            f"kind-bringup-{os.getpid()}-{self.id().rsplit('.', 1)[-1]}",
        )
        shutil.rmtree(self.tmp, ignore_errors=True)
        self.tmp.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.home = self.tmp / "home/notdev"
        self.home.mkdir(parents=True)
        # Staged, so the bring-up skips install-fc-vendor.sh and gets as far
        # as the preflight.
        probe = (
            self.home
            / ".space-devbox/space/vendor/firecracker/default/bin/firecracker"
        )
        probe.parent.mkdir(parents=True)
        probe.write_text("#!/bin/sh\nexit 0\n")
        probe.chmod(0o755)
        self.shared = self.home / ".space-devbox/kind/kind-cluster.yaml"

    def drive_bring_up(self):
        """Run the real `bootstrap_kind_stack__bring_up` against stubs.

        The stubs are the commands, not the library: `bring_up` sources
        `kind-host-paths.sh` *inside* itself, so any shell function with a
        library name defined here is replaced by the real one before it runs.
        That is deliberate — the render and the preflight are what this
        exercises, and both are real. Host module preparation, `kind`, the port guard and the
        polling commands are stubbed.

        Where it stops depends on the host: with no `/dev/kvm` the real
        preflight refuses and names the config it was given; with one, it
        proceeds and the `kind` stub records the `--config`. Both readings
        identify the same file, so the assertions below hold either way.
        """
        recorded = self.tmp / "recorded"
        script = f"""
        REPO_ROOT={shell_quote(str(REPO_ROOT))}
        KIND_CLUSTER=privacy-test
        source "$REPO_ROOT/scripts/tests/lib/bootstrap-kind-stack.sh"
        bootstrap_kind_stack__assert_port_owner() {{ :; }}
        curl() {{ :; }}; kubectl() {{ :; }}; tilt() {{ :; }}
        kind() {{
            if [[ "$1" == get ]]; then return 0; fi
            if [[ "$1" == create ]]; then
                while [[ $# -gt 0 ]]; do
                    if [[ "$1" == --config ]]; then
                        printf '%s\n' "$2" >>{shell_quote(str(recorded))}
                    fi
                    shift
                done
                return 9
            fi
        }}
        bootstrap_kind_stack__bring_up
        """
        result = subprocess.run(
            ["bash", "-uo", "pipefail", "-c", script],
            env=base_env(str(self.home)),
            text=True,
            capture_output=True,
            check=False,
        )
        if recorded.exists():
            return recorded.read_text().split()[0], result
        # The preflight's refusal leads with the config path it was handed.
        match = re.search(r"(\S*kind-cluster-bringup\.\S+)", result.stderr)
        self.assertIsNotNone(
            match,
            "the bring-up named no rendered config; it stopped before the "
            f"render and this test would prove nothing.\n{result.stderr}",
        )
        return match.group(1), result

    def test_the_config_handed_onward_is_never_the_shared_path(self) -> None:
        """Both the preflight and `kind create` get this run's own file.

        `$SPACE_KIND_CONFIG` is per-user, not per-run, so a sibling bring-up
        rendering its roots into it between this run's render and its create
        is the whole failure. `bootstrap.sh` renders to
        `$SPACE_KIND_CONFIG.$$` for the same reason, and this path is the one
        the smoke suites take.

        Non-vacuous by construction: `drive_bring_up` fails the test if no
        rendered config was named at all, so "not the shared path" cannot pass
        by the bring-up never having rendered one.
        """
        config, _ = self.drive_bring_up()

        self.assertNotEqual(Path(config), self.shared)
        self.assertFalse(
            self.shared.exists(),
            "the bring-up wrote the shared config; a sibling run can replace "
            "it in the window before `kind create` reads it",
        )

    def test_a_surviving_render_cannot_be_mistaken_for_the_published_config(
        self,
    ) -> None:
        """Where the render lives matters, because it can outlive the run.

        The bring-up reaps it on both branches of `kind create`, but a
        preflight refusal calls `kind_host_paths__die`, which exits — and no
        EXIT trap can be installed here, because every smoke script that
        sources this library has its own `trap cleanup EXIT` that deletes
        sandboxes and clusters. So the guarantee is not "always removed", it
        is "never left anywhere that reads as published": in `$TMPDIR`, and
        not in the directory `$SPACE_KIND_CONFIG` names, where a stale file
        would be picked up as a rendered config by the documented manual
        `kind create`.
        """
        config, _ = self.drive_bring_up()
        published_dir = self.shared.parent

        self.assertNotEqual(Path(config).parent, published_dir)
        self.assertFalse(
            any(published_dir.glob("*")) if published_dir.exists() else False,
            f"the bring-up left files in the published config's directory: "
            f"{list(published_dir.glob('*'))}",
        )


if __name__ == "__main__":
    unittest.main()
