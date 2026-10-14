# Documentation theme assets

This directory owns shared documentation styles and local Inter 4.1 variable
fonts. The Guide and API Reference builders explicitly stage the listed files
into `theme/` in each standalone output, using `docs/site/theme_assets.py`
for the common allowlist and hash checks. Standalone API builds also require
Python 3 for this staging step. The Guide loads the stylesheet through mdBook's
additional CSS configuration; the API Reference loads it from its HTML page.

`curiosity.css` provides font faces, shared layout dimensions, and semantic
Curiosity Light and Dark colors. Each mode uses one page background for the
header, toolbar, sidebar and content, with separate raised and code surfaces.
Text, borders, status indicators and API response tabs use the shared palette;
dark values live under `--docs-dark-*` in that stylesheet. Code keeps a monospace
font, and the Guide preserves native highlighting with readable print colors.

The shared stylesheet also owns the SPACE Docs header and section tabs. Desktop
tabs align with the reading column; narrow screens use a compact header. Each
renderer keeps its native reading controls in the utility row without a repeated
section title. Both use the shared heading weight and tracking, and a soft
palette background marks the selected navigation entry.

The Guide sidebar separates chapter groups from their indented child pages.
Only the active chapter gains smaller H2 links for navigating within the page;
these share the chapter links' hover background and keyboard focus treatment.
The sidebar remains scrollable, and mdBook owns its chapter links and controls.

Both sections load `system-theme.js` before their content. It selects the system
color preference once per page load and ignores legacy mdBook theme choices.
There is no theme switcher. When the system preference changes, the shared script
reloads the page so the API renderer and shell initialize with the same palette.
The stylesheet also follows the system when scripts are disabled.

The Guide's `docs/book/guide.css` adapts the shell while preserving mdBook's
toolbar, highlighting, and reading controls. Only the named `index.hbs`,
`header.hbs`, and `toc.js.hbs` templates are staged from `docs/book/theme/`.
The index override is based on mdBook 0.4.52. It loads the shared theme before
content, uses the selected mode for both native defaults, and leaves hidden
controller hooks for native highlighting with no selectable themes. This keeps
legacy saved choices from overriding the selected mode.
Review these changes against the public upstream template when upgrading.
The TOC override is based on mdBook 0.4.52
and only guards optional sidebar-scroll storage; review it against the native
template when upgrading mdBook. Chapter navigation uses the native footer links.
`docs/book/LICENSE-mdBook.txt` preserves its MPL 2.0 license in the Guide output.

The API Reference uses Redoc's supported `redoc-options.json` theme and
scroll-offset settings, with its shell in `docs/api/reference.css`. Its theme
reads resolved palette colors from the shared stylesheet through
`docs/api/reference-theme.js`, because Redoc computes derived colors during
initialization. Native API
navigation and inline samples stay with Redoc; the builder preserves the raw
OpenAPI download.

The fonts and `LICENSE-Inter.txt` are unmodified files from the
[official Inter 4.1 release](https://github.com/rsms/inter/releases/tag/v4.1).
`LICENSE-Inter.txt` contains their copyright notice and full OFL 1.1 license.
`assets.json` records the release source and archive hash, and pins the
stylesheet, system preference script, fonts and license by SHA-256. Both output
manifests also hash this manifest. Builders reject a missing, changed or
symlinked asset and an unexpected manifest entry. Builds use only
local assets; they do not download a replacement font or stylesheet.

To update an asset intentionally, update its hash in `assets.json` and review
both standalone outputs and the combined documentation site. A font release
update also needs its official source, archive hash and license reviewed.
