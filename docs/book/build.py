"""Stage only the public chapters, build mdBook, and pin repository links."""

from html import escape
from html.parser import HTMLParser
from hashlib import sha256
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from urllib.parse import unquote, urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "target/docs/guide"
# Explicit publication boundary. No recursive copies from docs/.
CHAPTERS = (
    "introduction.md",
    "using-api.md",
    "first-sandbox.md",
    "running-space.md",
    "installation.md",
    "observability.md",
    "troubleshooting.md",
    "warm-pools.md",
    "base-snapshots.md",
    "development-overview.md",
    "architecture.md",
    "versioning.md",
    "development.md",
    "contributing.md",
)
ASSETS = ("navigation.css", "page-toc.js", "mermaid-init.js")
LIBRARY_ASSETS = {
    "mermaid.min.js": "target/docs/diagram/mermaid.min.js",
    "MERMAID-LICENSES.txt": "target/docs/diagram/MERMAID-LICENSES.txt",
}
# API reference falls back to its pinned source page in a standalone guide.
SECTION_LINKS = (("SPACE Guide", "index.html"), ("API Reference", "api/index.md"))


def chapter_source(chapter):
    if chapter == "development.md":
        return ROOT / "README.md"
    if chapter == "contributing.md":
        return ROOT / "CONTRIBUTING.md"
    return ROOT / "docs" / chapter


def chapter_markdown(chapter):
    source = chapter_source(chapter).read_text()
    if chapter != "development.md":
        return source
    sections = re.findall(r"^## Development\n(.*?)(?=^## |\Z)", source, re.MULTILINE | re.DOTALL)
    if len(sections) != 1:
        raise ValueError("README must contain exactly one Development section")
    return "# Development workflow\n" + sections[0]


def git(*args):
    return subprocess.check_output(
        ["git", *args], cwd=ROOT, text=True
    ).strip()


def stage_sources(stage):
    (stage / "src").mkdir(parents=True)
    shutil.copyfile(ROOT / "docs/book/book.toml", stage / "book.toml")
    shutil.copyfile(ROOT / "docs/book/SUMMARY.md", stage / "src/SUMMARY.md")
    for asset in ASSETS:
        shutil.copyfile(ROOT / "docs/book" / asset, stage / asset)
    for asset, source in LIBRARY_ASSETS.items():
        shutil.copyfile(ROOT / source, stage / asset)
    for chapter in CHAPTERS:
        (stage / "src" / chapter).write_text(chapter_markdown(chapter))


def repository_link(href, output, commit, source_page=None):
    """Leave book links relative; send other file links to this source revision."""
    url = urlsplit(href)
    if url.scheme or url.netloc or not url.path:
        return href
    path = Path(unquote(url.path))
    if path.is_absolute():
        raise ValueError(f"Root-relative link is not prefix-safe: {href}")
    if (output / path).resolve().is_relative_to(output.resolve()):
        if (output / path).is_file():
            return href
    origin = source_page.parent if source_page else ROOT / "docs"
    source = (origin / path).resolve()
    # mdBook converts Markdown destinations to .html before this pass.
    if source.suffix == ".html":
        source = source.with_suffix(".md")
    for chapter in CHAPTERS:
        if source != chapter_source(chapter):
            continue
        fragment = url.fragment
        if chapter == "development.md":
            if fragment != "development":
                continue
            fragment = ""
        return urlunsplit(("", "", Path(chapter).with_suffix(".html").name, url.query, fragment))
    relative = source.relative_to(ROOT)
    git("ls-files", "--error-unmatch", relative.as_posix())
    if not source.is_file():
        raise ValueError(f"Missing source link: {href}")
    return urlunsplit((
        "https", "github.com",
        f"/perplexityai/SPACE/blob/{commit}/{relative.as_posix()}",
        url.query, url.fragment,
    ))


class RepositoryLinks(HTMLParser):
    def __init__(self, output, commit, source_page=None):
        super().__init__(convert_charrefs=False)
        self.output = output
        self.commit = commit
        self.source_page = source_page
        self.replacements = {}

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if href is None:
            return
        target = repository_link(href, self.output, self.commit, self.source_page)
        if target == href:
            return
        original = self.get_starttag_text()
        rewritten, count = re.subn(
            r"""(\bhref\s*=\s*)(["']).*?\2""",
            lambda match: f'{match[1]}"{escape(target, quote=True)}"',
            original, count=1,
        )
        if count != 1:
            raise ValueError(f"Cannot rewrite link: {original}")
        self.replacements[original] = rewritten


def sidebar_links(html, output, commit, iframe=False):
    target = ' target="_parent"' if iframe else ""
    links = "".join(
        '<a href="' + escape(repository_link(href, output, commit), quote=True)
        + '"' + target + (' aria-current="true"' if label == "SPACE Guide" else "")
        + ">" + escape(label) + "</a>"
        for label, href in SECTION_LINKS
    )
    # Sibling sections are deliberately outside mdBook's chapter list.
    marker = '<ol class="chapter">'
    if html.count(marker) != 1:
        raise ValueError("Review mdBook sidebar markup for documentation sections")
    return html.replace(marker, '<nav class="section-nav" aria-label="Documentation sections">'
                        + links + "</nav>" + marker)


def build():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    commit = git("rev-parse", "HEAD")
    with tempfile.TemporaryDirectory(prefix="guide-", dir=OUTPUT.parent) as temp:
        stage = Path(temp)
        stage_sources(stage)
        subprocess.run(["mdbook", "build", str(stage)], check=True)
        rendered = stage / "book"
        for asset in LIBRARY_ASSETS:
            shutil.copyfile(stage / asset, rendered / asset)
        for name in ("toc.html", "toc.js"):
            page = rendered / name
            html = sidebar_links(page.read_text(), rendered, commit, name == "toc.html")
            if name == "toc.js":
                # mdBook aliases index.html to the first chapter anchor. The
                # sibling section switcher must not take that first position.
                selector = 'this.querySelectorAll("a")'
                if html.count(selector) != 1:
                    raise ValueError("Review mdBook active-chapter selection after an upgrade")
                html = html.replace(selector, 'this.querySelectorAll(".chapter a")')
            page.write_text(html)
        # Rewrite each chapter in its canonical source directory. Apply the
        # same unambiguous link rewrites to mdBook's concatenated print page.
        print_links = {}
        pages = sorted(rendered.glob("*.html"), key=lambda page: page.name == "print.html")
        for page in pages:
            chapter = "introduction.md" if page.name == "index.html" else page.with_suffix(".md").name
            source_page = chapter_source(chapter) if chapter in CHAPTERS else None
            html = page.read_text()
            if page.name == "print.html":
                # mdBook normally prints 100 ms after load, before an async
                # diagram may finish. The initializer resolves even on failure.
                print_call = "window.setTimeout(window.print, 100);"
                if html.count(print_call) != 1:
                    raise ValueError("Review mdBook print readiness after an upgrade")
                html = html.replace(print_call, (
                    "(window.spaceDiagramsReady || Promise.resolve()).then(() => "
                    "window.setTimeout(window.print, 100));"
                ))
                for original, rewritten in print_links.items():
                    html = html.replace(original, rewritten)
            links = RepositoryLinks(rendered, commit, source_page)
            links.feed(html)
            for original, rewritten in links.replacements.items():
                if original in print_links and print_links[original] != rewritten:
                    raise ValueError(f"Ambiguous source link in print output: {original}")
                print_links[original] = rewritten
                html = html.replace(original, rewritten)
            source_url = f"https://github.com/perplexityai/SPACE/tree/{commit}"
            if source_page:
                source_url = f"https://github.com/perplexityai/SPACE/blob/{commit}/{source_page.relative_to(ROOT).as_posix()}"
                if chapter == "development.md":
                    source_url += "#development"
            html = html.replace("</main>", (
                '<footer class="docs-build">'
                f'<a href="{source_url}">Source {commit[:8]}</a>'
                ' · <a href="build.json">Build details</a></footer></main>'
            ))
            page.write_text(html)
        metadata = {
            "commit": commit,
            "dirty": bool(git("status", "--porcelain", "--untracked-files=normal")),
            "chapters": list(CHAPTERS),
            "diagramRenderer": "mermaid " + json.loads(
                (ROOT / "docs/api/node_modules/mermaid/package.json").read_text()
            )["version"],
            "chapterSources": {
                chapter: {
                    "path": chapter_source(chapter).relative_to(ROOT).as_posix(),
                    "sha256": sha256(chapter_markdown(chapter).encode()).hexdigest(),
                }
                for chapter in CHAPTERS
            },
            "files": {
                file.relative_to(rendered).as_posix(): sha256(file.read_bytes()).hexdigest()
                for file in sorted(rendered.rglob("*")) if file.is_file()
            },
        }
        (rendered / "build.json").write_text(json.dumps(metadata, indent=2) + "\n")
        if OUTPUT.is_symlink():
            raise ValueError(f"Refusing symlink output directory: {OUTPUT}")
        if OUTPUT.exists():
            shutil.rmtree(OUTPUT)
        shutil.move(rendered, OUTPUT)
    print(f"Built SPACE Guide from {commit} in target/docs/guide")


if __name__ == "__main__":
    build()
