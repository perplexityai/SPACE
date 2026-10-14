"""Stage the named, hash-pinned documentation theme payloads."""

from hashlib import sha256
import json
from pathlib import Path
import sys


ASSETS = (
    "curiosity.css",
    "system-theme.js",
    "mobile-menu.js",
    "InterVariable.woff2",
    "InterVariable-Italic.woff2",
    "LICENSE-Inter.txt",
    "assets.json",
)


def stage_theme_assets(root, destination):
    source = Path(root).resolve() / "docs/site/theme"
    if source.resolve() != source:
        raise ValueError("Refusing symlinked theme source")
    payloads = {}
    for name in ASSETS:
        file = source / name
        if file.is_symlink() or not file.is_file():
            raise ValueError(f"Missing or unsafe theme asset: {name}")
        payloads[name] = file.read_bytes()
    manifest = json.loads(payloads["assets.json"])
    if set(manifest["files"]) != set(ASSETS) - {"assets.json"}:
        raise ValueError("Unexpected theme asset manifest entries")
    hashes = {name: sha256(data).hexdigest() for name, data in payloads.items()}
    for name, expected in manifest["files"].items():
        if hashes[name] != expected:
            raise ValueError(f"Changed theme asset: {name}")
    destination = Path(destination).absolute()
    if destination.resolve() != destination:
        raise ValueError("Refusing unsafe theme destination")
    destination.mkdir(parents=True, exist_ok=True)
    for name, data in payloads.items():
        target = destination / name
        if target.is_symlink():
            raise ValueError(f"Refusing symlinked theme destination: {name}")
        target.write_bytes(data)
    return hashes


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    print(json.dumps(stage_theme_assets(root, sys.argv[1])))
