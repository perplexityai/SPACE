# Documentation theme assets

This directory owns shared documentation styles and local Inter 4.1 variable
fonts. The Guide and API Reference builders explicitly stage the listed files
into `theme/` in each standalone output, using `docs/site/theme_assets.py`
for the common allowlist and hash checks. Standalone API builds also require
Python 3 for this staging step. The Guide loads the stylesheet through mdBook's
additional CSS configuration; the API Reference loads it from its HTML page.

`curiosity.css` provides font faces, semantic light-palette variables and shared
layout dimensions. Consumers choose where to apply them; code keeps a monospace
font and Guide colors stay scoped to its Light theme.

The Guide's `docs/book/guide.css` adapts the shell while preserving mdBook's
toolbar and theme choices. Only the named `header.hbs` and `toc.js.hbs` templates
are staged from `docs/book/theme/`. The TOC override is based on mdBook 0.4.52
and only guards optional sidebar-scroll storage; review it against the native
template when upgrading mdBook. Chapter navigation uses the native footer links.
`docs/book/LICENSE-mdBook.txt` preserves its MPL 2.0 license in the Guide output.

The API Reference uses Redoc's supported `redoc-options.json` theme and
scroll-offset settings, with its shell in `docs/api/reference.css`. Its theme
uses literal palette colors because Redoc computes derived colors during
initialization; keep those values aligned with `curiosity.css`. Native API
navigation and inline samples stay with Redoc; the builder preserves the raw
OpenAPI download.

The fonts and `LICENSE-Inter.txt` are unmodified files from the
[official Inter 4.1 release](https://github.com/rsms/inter/releases/tag/v4.1).
`LICENSE-Inter.txt` contains their copyright notice and full OFL 1.1 license.
`assets.json` records the release source and archive hash, and pins the
stylesheet, fonts and license by SHA-256. Both output
manifests also hash this manifest. Builders reject a missing, changed or
symlinked asset and an unexpected manifest entry. Builds use only
local assets; they do not download a replacement font or stylesheet.

To update an asset intentionally, update its hash in `assets.json` and review
both standalone outputs and the combined documentation site. A font release
update also needs its official source, archive hash and license reviewed.
