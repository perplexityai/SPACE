"""Check the real mdBook output without a running SPACE deployment."""

from html.parser import HTMLParser
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch
from urllib.parse import unquote, urljoin, urlsplit

import build


class Page(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.ids = set()
        self.links = []
        self.anchors = []
        self.bases = []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "base":
            self.bases.append(attrs["href"])
            return
        if "id" in attrs:
            self.ids.add(attrs["id"])
        for key in ("href", "src"):
            if key in attrs:
                self.links.append(attrs[key])
        if tag == "a" and "href" in attrs:
            self.anchors.append(attrs["href"])


class GuideTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.output = build.OUTPUT
        cls.commit = build.git("rev-parse", "HEAD")
        cls.chapters = [Path(name).with_suffix(".html").name for name in build.CHAPTERS]
        cls.pages = {
            page.name: Page(page.read_text())
            for page in cls.output.glob("*.html")
        }

    def test_explicit_staging_boundary(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "repo"
            (root / "docs/book").mkdir(parents=True)
            for name in build.CHAPTERS:
                source = build.chapter_source(name)
                shutil.copyfile(source, root / source.relative_to(build.ROOT))
            for name in ("book.toml", "SUMMARY.md", *build.ASSETS):
                shutil.copyfile(build.ROOT / "docs/book" / name, root / "docs/book" / name)
            for name in build.LIBRARY_ASSETS.values():
                source = Path(name)
                (root / source).parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(build.ROOT / source, root / source)
            (root / "docs/operator-secret.md").write_text("DO_NOT_PUBLISH")
            stage = Path(temp) / "stage"
            with patch.object(build, "ROOT", root):
                build.stage_sources(stage)
            self.assertEqual(
                {file.relative_to(stage).as_posix() for file in stage.rglob("*") if file.is_file()},
                {"book.toml", "src/SUMMARY.md", *build.ASSETS, *build.LIBRARY_ASSETS,
                 *(f"src/{name}" for name in build.CHAPTERS)},
            )
            with patch.object(build, "ROOT", root):
                for name in build.CHAPTERS:
                    self.assertEqual((stage / "src" / name).read_text(), build.chapter_markdown(name))
        config = tomllib.loads((build.ROOT / "docs/book/book.toml").read_text())
        self.assertFalse(config["build"]["use-default-preprocessors"])
        self.assertFalse(config["build"]["create-missing"])
        self.assertFalse(list(self.output.rglob("*.md")))

    def test_every_top_level_page_is_classified(self):
        tracked = build.git("ls-files", "docs/*.md").splitlines()
        pages = {Path(name).name for name in tracked if Path(name).parent == Path("docs")}
        published = {name for name in build.CHAPTERS if build.chapter_source(name).parent == build.ROOT / "docs"}
        self.assertEqual(len(build.CHAPTERS), len(set(build.CHAPTERS)))
        self.assertFalse(published & set(build.UNPUBLISHED))
        self.assertTrue(all(build.UNPUBLISHED.values()))
        self.assertEqual(
            pages - published - set(build.UNPUBLISHED), set(),
            "Add new docs pages to docs/book/SUMMARY.md, or list them in "
            "UNPUBLISHED in docs/book/build.py with a reason.",
        )
        self.assertEqual(set(build.UNPUBLISHED) - pages, set())

    def test_canonical_development_and_contributing(self):
        readme = (build.ROOT / "README.md").read_text()
        development = readme.split("## Development\n", 1)[1].split("\n## ", 1)[0]
        self.assertEqual(build.chapter_markdown("development.md"), "# Development workflow\n" + development + "\n")
        self.assertEqual(
            build.chapter_markdown("contributing.md"),
            (build.ROOT / "CONTRIBUTING.md").read_text(),
        )
        metadata = json.loads((self.output / "build.json").read_text())
        for chapter in build.CHAPTERS:
            self.assertEqual(metadata["chapterSources"][chapter], {
                "path": build.chapter_source(chapter).relative_to(build.ROOT).as_posix(),
                "sha256": sha256(build.chapter_markdown(chapter).encode()).hexdigest(),
            })
        self.assertIn("installation.html", self.pages["development.html"].anchors)
        self.assertIn("development.html", self.pages["introduction.html"].anchors)
        self.assertIn("contributing.html", self.pages["introduction.html"].anchors)
        base = f"https://github.com/perplexityai/SPACE/blob/{self.commit}/"
        self.assertIn(base + "README.md#development", self.pages["development.html"].anchors)
        self.assertIn(base + "DEPENDENCIES.md", self.pages["development.html"].anchors)
        self.assertIn(base + "SECURITY.md", self.pages["contributing.html"].anchors)
        self.assertIn(base + "CONTRIBUTING.md", self.pages["contributing.html"].anchors)
        self.assertIn("propose-a-change", self.pages["contributing.html"].ids)

    def test_chapters_and_navigation(self):
        summary = (build.ROOT / "docs/book/SUMMARY.md").read_text()
        self.assertEqual(re.findall(r"\]\(([^)]+)\)", summary), list(build.CHAPTERS))
        self.assertEqual(
            set(self.pages),
            {*self.chapters, "index.html", "print.html", "toc.html", "404.html"},
        )
        self.assertEqual(
            [href for href in self.pages["toc.html"].anchors if not urlsplit(href).scheme],
            ["index.html", *self.chapters],
        )
        self.assertEqual(
            re.findall(r"^# (.+)$", summary, re.MULTILINE),
            ["Summary"],
        )
        self.assertEqual(self.chapters[0], "introduction.html")
        for _, href in build.SECTION_LINKS:
            pinned = build.repository_link(href, self.output, self.commit)
            self.assertIn(pinned, self.pages["toc.html"].anchors)
            self.assertIn(pinned, (self.output / "toc.js").read_text())
        toc = (self.output / "toc.html").read_text()
        self.assertLess(toc.index('class="section-nav"'), toc.index('<ol class="chapter">'))
        self.assertNotIn("API Reference", toc.split('<ol class="chapter">', 1)[1])
        self.assertEqual(
            (self.output / "index.html").read_bytes(),
            (self.output / self.chapters[0]).read_bytes(),
        )
        for index, chapter in enumerate(self.chapters):
            anchors = self.pages[chapter].anchors
            if index:
                self.assertIn(self.chapters[index - 1], anchors)
            if index + 1 < len(self.chapters):
                self.assertIn(self.chapters[index + 1], anchors)

    def test_section_landing_links(self):
        for page, targets in {
            "using-api.html": ["first-sandbox.html", "installation.html"],
            "running-space.html": [
                "installation.html",
                "observability.html",
                "troubleshooting.html",
                "warm-pools.html",
                "base-snapshots.html",
            ],
            "development-overview.html": ["architecture.html", "development.html", "contributing.html"],
        }.items():
            for target in targets:
                self.assertIn(target, self.pages[page].anchors)
        self.assertIn(
            build.repository_link("api/index.md", self.output, self.commit),
            self.pages["using-api.html"].anchors,
        )

    def test_old_heading_fragments_target_the_renamed_sections(self):
        for page, aliases in {
            "first-sandbox.html": {
                "before-you-start": "prepare-your-environment",
                "command-output": "read-command-output",
                "if-a-step-fails": "troubleshoot-failures",
            },
            "installation.html": {
                "before-you-begin": "prepare-your-environment",
                "configuration": "configure-your-environment",
                "shim-telemetry-selectors": "select-shim-telemetry",
                "if-setup-fails": "troubleshoot-setup",
            },
        }.items():
            for output in (page, "print.html"):
                html = (self.output / output).read_text()
                for old, new in aliases.items():
                    # mdBook disambiguates repeated headings in print.html.
                    suffix = r"(?:-\d+)?" if output == "print.html" else ""
                    self.assertRegex(html, rf'<a id="{old}"></a></p>\s*<h2 id="{new}{suffix}">')

    def test_authored_mermaid_and_pinned_offline_renderer(self):
        source = (build.ROOT / "docs/architecture.md").read_text()
        diagram = re.search(r"```mermaid\n(.*?)```", source, re.DOTALL)[1]
        self.assertNotIn("```text", source)
        self.assertEqual(re.findall(r"^\s*(\w+) (?:-->|<-->)", diagram, re.MULTILINE), [
            "application", "api", "postgres", "controller", "pod", "shim",
            "workers", "workers", "vm", "application", "data",
        ])
        self.assertIn("accTitle:", diagram)
        self.assertIn("accDescr:", diagram)
        installed = build.ROOT / "docs/api/node_modules/mermaid"
        version = json.loads((installed / "package.json").read_text())["version"]
        pinned = json.loads((build.ROOT / "docs/api/package.json").read_text())["devDependencies"]["mermaid"]
        self.assertEqual(version, pinned)
        for asset, original in build.LIBRARY_ASSETS.items():
            self.assertEqual((self.output / asset).read_bytes(), (build.ROOT / original).read_bytes())
        self.assertIn("spaceDiagramsReady", (self.output / "print.html").read_text())

    def assert_local_target(self, page, link, prefix):
        url = urlsplit(urljoin(f"https://guide.example{prefix}{page}", link))
        self.assertEqual(url.netloc, "guide.example")
        self.assertTrue(url.path.startswith(prefix), link)
        target = unquote(url.path.removeprefix(prefix))
        self.assertTrue((self.output / target).is_file(), f"{page}: {link}")
        if url.fragment:
            self.assertIn(unquote(url.fragment), self.pages[target].ids, f"{page}: {link}")

    def test_links_fragments_assets_and_prefixes(self):
        for page, parsed in self.pages.items():
            self.assertTrue(all(base == "./" for base in parsed.bases))
            for link in parsed.links:
                url = urlsplit(link)
                if url.scheme or url.netloc:
                    # No remote scripts, stylesheets, images, or fonts in HTML.
                    self.assertIn(link, parsed.anchors)
                    continue
                self.assertFalse(url.path.startswith("/"), f"{page}: {link}")
                for prefix in ("/", "/space/", "/preview/space/guide/"):
                    self.assert_local_target(page, link, prefix)

    def test_source_links_match_revision(self):
        base = f"https://github.com/perplexityai/SPACE/blob/{self.commit}/"
        found = set()
        for chapter in self.chapters:
            anchors = self.pages[chapter].anchors
            if chapter not in ("development.html", "contributing.html"):
                self.assertIn(base + "README.md", anchors)
            for link in anchors:
                if "/perplexityai/SPACE/blob/" not in link:
                    continue
                self.assertTrue(link.startswith(base), link)
                path = unquote(urlsplit(link.removeprefix(base)).path)
                build.git("ls-files", "--error-unmatch", path)
                self.assertTrue((build.ROOT / path).is_file(), link)
                found.add(path)
        self.assertIn("examples/first_sandbox.py", found)
        self.assertIn("infra/space/kind/scripts/bootstrap.sh", found)
        self.assertIn("crates/libs/api/src/controllers/substrate/direct.rs", found)

    def test_search_index_targets(self):
        # mdBook 0.4.52 embeds JSON in a JS string. Only parse its URL list.
        script = (self.output / "searchindex.js").read_text()
        urls = script.split('"doc_urls":[', 1)[1].split("]", 1)[0]
        urls = json.loads("[" + urls + "]")
        self.assertEqual({urlsplit(url).path for url in urls}, set(self.chapters))
        self.assertGreater(len(urls), len(self.chapters))
        for url in urls:
            self.assert_local_target("index.html", url, "/preview/space/guide/")
        self.assertTrue((self.output / "searcher.js").is_file())
        for chapter in self.chapters:
            self.assertIn("searchbar", self.pages[chapter].ids)

    def test_link_rewriter_rejects_unsafe_or_missing_files(self):
        for href in ("/installation.html", "../../../etc/passwd", "missing.py", "../.git/config"):
            with self.assertRaises((ValueError, subprocess.CalledProcessError)):
                build.repository_link(href, self.output, self.commit)
        self.assertEqual(
            build.repository_link("installation.html#authenticate", self.output, self.commit),
            "installation.html#authenticate",
        )
        self.assertEqual(
            build.repository_link("https://example.com/path", self.output, self.commit),
            "https://example.com/path",
        )

    def test_old_python_gets_actionable_failure(self):
        # Simulate 3.10's version report using the real entrypoint. No old
        # interpreter is required, and the build must stop before mdBook runs.
        with tempfile.TemporaryDirectory() as temp:
            tools = Path(temp)
            python = tools / "python3"
            python.write_text(
                "#!/bin/sh\nexec " + shlex.quote(sys.executable)
                + """ -c 'import sys; sys.version_info = (3, 10, 0); exec(sys.argv[1])' "$2"\n"""
            )
            python.chmod(0o755)
            mdbook = tools / "mdbook"
            mdbook.write_text("#!/bin/sh\necho 'mdbook should not run' >&2\nexit 99\n")
            mdbook.chmod(0o755)
            result = subprocess.run(
                ["bash", str(build.ROOT / "scripts/build-guide-docs.sh")],
                env={**os.environ, "PATH": f"{tools}:{os.environ['PATH']}"},
                capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("Python 3.11+ is required", result.stderr)
            self.assertNotIn("mdbook should not run", result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
