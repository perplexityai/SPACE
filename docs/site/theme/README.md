# Documentation theme assets

This directory owns shared documentation styles and local Inter 4.1 variable
fonts. The Guide and API Reference builders explicitly stage the listed files
into `theme/` in each standalone output, using `docs/site/theme_assets.py`
for the common allowlist and hash checks. Standalone API builds also require
Python 3 for this staging step. No renderer loads the stylesheet yet;
adoption belongs to its own supported configuration and templates.

`curiosity.css` provides font faces, semantic light-palette variables and shared
layout dimensions. Consumers choose where to apply them; code keeps a monospace
font and Guide colors stay scoped to its Light theme.

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
