#!/usr/bin/env python3
"""Split one rustfmt-formatted Rust module file into child modules, mechanically.

The split is a pure move. Every top-level item named in the spec moves verbatim
(with its doc comments and attributes, in original order) into a new child
module. The parent keeps `mod child;` plus a glob re-export, so every existing
path keeps resolving. The only edits to moved text are the ones that keep
name resolution and effective visibility exactly as they were:

* Visibility, so each item and each inherent-impl member and struct field keeps
  the same reach it had in the parent:
    private / pub(self)   -> pub(super)
    pub(super)            -> pub(in <grandparent path>)   (pub(crate) if that is the crate root)
    pub(crate), pub, pub(in crate::..) unchanged
* `super::` paths in moved code gain one `super::` (the code is one module deeper).
Each new module starts with `use super::*;`, so the parent's imports, traits and
sibling items resolve in moved code as before.

The spec is item names, not line numbers, so it can be re-run on a newer main
instead of rebasing a split by hand.

Usage:
  split_rust_module.py list FILE                       # items, impl members and their selectors
  split_rust_module.py apply SPEC.toml [--repo ROOT] [--report OUT.md] [--dry-run]
  split_rust_module.py verify [BASE] [--head REV] [--print-spec]

`verify` proves that BASE...HEAD's Rust changes are exactly what `apply` generates: it
derives the spec from the diff (which items each new child module holds), regenerates
from the merge base, and compares every parent and child file byte for byte after
rustfmt. Any other Rust change, a missing or altered `mod`/`use` line, a dropped item, an
edit inside a moved item or a comment, or a symlink fails it. It checks the generator,
not the generator's semantics: those rest on the edits listed above, the refusals below,
and compiling and testing the result.

Spec (TOML):
  file = "crates/libs/x/src/foo.rs"
  [[module]]
  name = "bar"
  doc = "What bar holds."            # one `//!` line at the top of the new file
  items = ["fn parse_*", "struct Config", "impl Config", "impl Display for Config"]
  # optional: glob_import = false   (omit `use super::*;` when the module needs nothing
  #           from its parent, which `-D warnings` would otherwise reject as unused)

Selectors are fnmatch patterns over `<kind> <name>`; impl blocks are
`impl <header>` with whitespace collapsed (e.g. `impl<T> From<T> for Foo`).
`impl <header> :: <kind> <name>` selects members of an inherent impl: they move into
an impl block with the same attributes and header in the child, and the parent's
block keeps the rest (or disappears when none are left). Trait impls move whole.
Every pattern must match at least one item and every item at most one module.

* Relative `include!`/`include_str!`/`include_bytes!` paths gain `../` when the
  children live one directory deeper than the parent file (parent is `foo.rs`).

Refused, to be done by hand (the tool stops rather than guess):
* moving `use`, `mod x;`, `extern` blocks or `macro_rules!` items (macros stay in the
  parent, above the new `mod` lines, so children still see them);
* a parent file loaded through `#[path]`, or a `#[path]` inside moved code;
* moved code that invokes a parent macro emitting `pub(super)`/`pub(self)` (directly or
  through another macro), or a macro name defined twice;
* relative visibility the edits cannot remap: `pub(in super::..)`, a `pub(super)`
  nested inside a moved item, an inline module naming `super`, `self::super`;
* a `super::` path inside a string literal (e.g. an attribute argument), and include
  arguments other than a plain string literal;
* a module name that is a keyword or collides with an item or import in the parent;
* an attribute on the same line as its item (so items are never mis-split).
"""
from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass, field

# --------------------------------------------------------------------- masking

CHAR_RE = re.compile(r"'(?:[^'\\\n]|\\(?:[nrt\\0'\"]|x[0-9a-fA-F]{2}|u\{[0-9a-fA-F_]{1,6}\}))'")


def mask(src: str, comment: str = " ") -> str:
    """Same length as src; comments -> `comment`, string/char literal bodies -> 'x'.

    Newlines are kept, so line structure and character offsets line up with src.
    """
    out = list(src)
    n = len(src)
    i = 0

    def blank(a: int, b: int, ch: str) -> None:
        for k in range(a, b):
            if out[k] != "\n":
                out[k] = ch

    while i < n:
        c = src[i]
        if c == "/" and i + 1 < n and src[i + 1] == "/":
            j = src.find("\n", i)
            if j < 0:
                j = n
            blank(i, j, comment)
            i = j
            continue
        if c == "/" and i + 1 < n and src[i + 1] == "*":
            depth = 1
            j = i + 2
            while j < n and depth:
                if src.startswith("/*", j):
                    depth += 1
                    j += 2
                elif src.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            blank(i, j, comment)
            i = j
            continue
        if c in "rbc" and (i == 0 or not (src[i - 1].isalnum() or src[i - 1] == "_")):
            m = re.match(r"(?:br|cr|r)(#*)\"", src[i:i + 300])
            if m:
                term = '"' + m.group(1)
                j = src.find(term, i + m.end())
                j = n if j < 0 else j + len(term)
                blank(i, j, "x")
                i = j
                continue
            if re.match(r"(?:b|c)\"", src[i:i + 2]):
                out[i] = "x"
                i += 1
                c = '"'
            elif src.startswith("b'", i):
                m = CHAR_RE.match(src, i + 1)
                if m:
                    blank(i, m.end(), "x")
                    i = m.end()
                    continue
        if c == '"':
            j = i + 1
            while j < n:
                if src[j] == "\\":
                    j += 2
                    continue
                if src[j] == '"':
                    j += 1
                    break
                j += 1
            blank(i, j, "x")
            i = j
            continue
        if c == "'":
            m = CHAR_RE.match(src, i)
            if m:
                blank(i, m.end(), "x")
                i = m.end()
                continue
        i += 1
    return "".join(out)


OPEN = "([{"
CLOSE = ")]}"

# ------------------------------------------------------------------- items

VIS = r"(?:pub(?:\s*\(\s*(?:crate|self|super|in\s+[A-Za-z0-9_:\s]+)\s*\))?\s+)?"
QUAL = r"(?:(?:const|async|unsafe|default|extern(?:\s+(?:\"[^\"]*\"|x+))?)\s+)*"
ITEM_RE = re.compile(
    r"^(?P<vis>" + VIS + r")" + QUAL +
    r"(?P<kind>fn|struct|enum|union|trait|impl|type|const|static|mod|use|extern\s+crate|auto\s+trait)\b|(?P<mr>macro_rules!)"
)
NAME_RE = re.compile(r"\s*(?:r#)?([A-Za-z_][A-Za-z0-9_]*)")
MACRO_CALL_RE = re.compile(r"^(?P<path>[A-Za-z_][A-Za-z0-9_:]*)\s*!")
UNMOVABLE = {"use", "extern crate", "extern-block"}
KEYWORDS = set("""as async await break const continue crate dyn else enum extern false fn for if impl in let
loop match mod move mut pub ref return self static struct super trait true type union unsafe use where while
abstract become box do final gen macro override priv try typeof unsized virtual yield""".split())


@dataclass
class Item:
    kind: str
    name: str
    start: int  # first line of the item's code (0-based)
    end: int  # last line (inclusive)
    lead: int  # first line of its leading trivia (attributes, comments)
    vis: str
    cfg: list[str] = field(default_factory=list)

    @property
    def selector(self) -> str:
        if self.kind == "impl":
            return "impl" + (self.name if self.name.startswith("<") else " " + self.name)
        return f"{self.kind} {self.name}"


def line_depth_delta(mline: str) -> int:
    d = 0
    for ch in mline:
        if ch in OPEN:
            d += 1
        elif ch in CLOSE:
            d -= 1
    return d


def collapse(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def parse_items(src: str) -> tuple[list[Item], int]:
    """Return (items, header_end) where header_end is the first line after the header.

    Relies on rustfmt layout: top-level items start in column 0 at depth 0.
    """
    lines = src.split("\n")
    mlines = mask(src).split("\n")
    n = len(lines)
    # Header: leading //! docs, #![...] inner attributes, and blank lines.
    i = 0
    header_end = 0
    depth = 0
    while i < n:
        s = lines[i].strip()
        if s.startswith("//!") or s == "":
            i += 1
            if s:
                header_end = i
            continue
        if s.startswith("#!["):
            d = line_depth_delta(mlines[i])
            i += 1
            while d > 0 and i < n:
                d += line_depth_delta(mlines[i])
                i += 1
            header_end = i
            continue
        break
    items: list[Item] = []
    i = header_end
    lead = None
    while i < n:
        raw, m = lines[i], mlines[i]
        s = raw.strip()
        if depth != 0:
            raise SystemExit(f"internal: depth {depth} at line {i + 1}")
        if s == "":
            i += 1
            continue
        if m.strip() == "":  # comment-only line
            if lead is None:
                lead = i
            i += 1
            continue
        if raw[0].isspace():
            raise SystemExit(f"line {i + 1}: top-level code not in column 0 (run rustfmt first)")
        if s.startswith("#[") and not attr_then_code(m):
            if lead is None:
                lead = i
            d = line_depth_delta(m)
            i += 1
            while d > 0 and i < n:
                d += line_depth_delta(mlines[i])
                i += 1
            continue
        if s.startswith("#["):
            raise SystemExit(f"line {i + 1}: an attribute and an item share a line; put the item on its own line")
        # item start
        start = i
        d = 0
        while True:
            d += line_depth_delta(mlines[i])
            tail = mlines[i].rstrip()
            if d == 0 and (tail.endswith("}") or tail.endswith(";")):
                break
            i += 1
            if i >= n:
                raise SystemExit(f"line {start + 1}: item never ends")
        end = i
        i += 1
        text_m = "\n".join(mlines[start:end + 1])
        it = classify(lines, mlines, start, end, text_m)
        it.lead = lead if lead is not None else start
        it.cfg = [x for x in attrs_of(lines, it.lead, start) if re.match(r"#\[\s*cfg\s*\(", x)]
        items.append(it)
        lead = None
    return items, header_end


def attr_then_code(mline: str) -> bool:
    """Whether a masked line holds a complete `#[...]` followed by more code."""
    s = mline.lstrip()
    d = 0
    for k, ch in enumerate(s):
        if ch == "[":
            d += 1
        elif ch == "]":
            d -= 1
            if d == 0:
                return bool(s[k + 1:].strip())
    return False


def attrs_of(lines: list[str], lead: int, start: int) -> list[str]:
    """Attributes in an item's leading trivia, each with its exact original text."""
    out, k = [], lead
    mlines = mask("\n".join(lines[lead:start])).split("\n")
    while k < start:
        if lines[k].strip().startswith("#["):
            d = line_depth_delta(mlines[k - lead])
            group = [lines[k]]
            k += 1
            while d > 0 and k < start:
                d += line_depth_delta(mlines[k - lead])
                group.append(lines[k])
                k += 1
            out.append("\n".join(group))
            continue
        k += 1
    return out


def classify(lines, mlines, start, end, text_m) -> Item:
    first = lines[start]
    mm = ITEM_RE.match(first)
    if mm and mm.group("mr"):
        nm = NAME_RE.match(first[mm.end():])
        return Item("macro_rules!", nm.group(1) if nm else "?", start, end, start, "")
    if mm:
        kind = collapse(mm.group("kind"))
        vis = collapse(mm.group("vis"))
        rest = first[mm.end():]
        if kind == "impl":
            mtext = "\n".join(mlines[start:end + 1])
            cut = find_brace(mtext, 0)
            header = collapse("\n".join(lines[start:end + 1])[: cut if cut >= 0 else None])
            name = re.sub(r"^(?:unsafe\s+)?impl\s*", "", header).strip()
            return Item("impl", name, start, end, start, vis)
        if kind in ("use", "extern crate"):
            return Item(kind, collapse(" ".join(lines[start:end + 1]))[:120], start, end, start, vis)
        nm = NAME_RE.match(re.sub(r"^\s*mut\s+", " ", rest) if kind == "static" else rest)
        name = nm.group(1) if nm else "?"
        if kind == "mod":
            kind = "mod" if text_m.rstrip().endswith("}") else "mod-decl"
        return Item(kind, name, start, end, start, vis)
    if re.match(r"^(?:unsafe\s+)?extern\s+(?:\"[^\"]*\"\s*)?\{", first):
        return Item("extern-block", collapse(first.rstrip("{ ")), start, end, start, "")
    mc = MACRO_CALL_RE.match(first.strip())
    if mc:
        return Item("macro", mc.group("path"), start, end, start, "")
    raise SystemExit(f"line {start + 1}: cannot classify top-level item: {lines[start][:80]!r}")


# ----------------------------------------------------------- visibility map

def file_layout(rel_file: str) -> tuple[list[str], bool]:
    """(module path inside its crate, whether children live beside the file).

    Children of a crate root (`src/lib.rs`, `src/main.rs`, `src/bin/x.rs`,
    `src/bin/x/main.rs`) and of a `mod.rs` sit in the same directory; children of
    `foo.rs` sit in `foo/`.
    """
    p = rel_file.replace(os.sep, "/")
    if "/src/" not in p or not p.endswith(".rs"):
        raise SystemExit(f"{rel_file}: expected a .rs file under <crate>/src/")
    parts = p.split("/src/", 1)[1][:-3].split("/")
    if parts in (["lib"], ["main"]):
        return [], True
    if parts[0] == "bin":
        if len(parts) == 2 or (len(parts) == 3 and parts[2] == "main"):
            return [], True
        parts = parts[2:]
    if parts[-1] == "mod":
        return parts[:-1], True
    return parts, False


def module_path_of(rel_file: str) -> list[str]:
    return file_layout(rel_file)[0]


def map_vis(vis: str, parent_path: list[str]) -> str | None:
    """New visibility for an item moved from module P (path parent_path) into P::child.

    Returns None when the visibility is unchanged.
    """
    v = collapse(vis)
    if v in ("", "pub(self)"):
        return "pub(super) "
    if v == "pub(super)":
        if not parent_path:
            raise SystemExit("pub(super) in a crate root")
        gp = parent_path[:-1]
        return "pub(crate) " if not gp else f"pub(in crate::{'::'.join(gp)}) "
    if v.startswith("pub(in ") and not v.startswith("pub(in crate"):
        raise SystemExit(f"relative visibility {v!r} not supported; edit by hand")
    return None


VIS_AT = re.compile(r"pub(?:\s*\(\s*(?:crate|self|super|in\s+[A-Za-z0-9_:\s]+)\s*\))?\s+")


def skip_ws_attrs(mt: str, p: int) -> int:
    """Skip whitespace (comments are already blank in masked text) and #[...] attributes."""
    n = len(mt)
    while p < n:
        if mt[p].isspace():
            p += 1
        elif mt.startswith("#[", p) or mt.startswith("#![", p):
            d = 0
            while p < n:
                if mt[p] == "[":
                    d += 1
                elif mt[p] == "]":
                    d -= 1
                    if d == 0:
                        p += 1
                        break
                p += 1
        else:
            break
    return p


def find_brace(mt: str, p: int) -> int:
    """Offset of the first `{` at nesting depth 0 from p, skipping (), [] and generic <>
    (the `>` of `->`/`=>` excepted); -1 if a depth-0 `;` comes first."""
    depth = angle = 0
    n = len(mt)
    while p < n:
        ch = mt[p]
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif ch == "<" and depth == 0:
            angle += 1
        elif ch == ">" and depth == 0 and angle > 0 and mt[p - 1] not in "-=":
            angle -= 1
        elif ch == "{" and depth == 0 and angle == 0:
            return p
        elif ch == "{":
            depth += 1  # a const-generic block expression
        elif ch == "}" and depth > 0:
            depth -= 1
        elif ch == ";" and depth == 0 and angle == 0:
            return -1
        p += 1
    return -1


def find_body_open(mt: str, p: int) -> int:
    """From after a struct/union name, skip generics and where clauses; return the offset of
    the body delimiter `{` or `(`, or -1 for a unit item ending in `;`."""
    n = len(mt)
    if p < n and mt[p] == "<":
        d = 0
        while p < n:
            ch = mt[p]
            if ch == "<":
                d += 1
            elif ch == ">" and mt[p - 1] not in "-=":
                d -= 1
                if d == 0:
                    p += 1
                    break
            p += 1
    while p < n and mt[p].isspace():
        p += 1
    if p < n and mt[p] == "(":
        return p
    return find_brace(mt, p)


def split_top(mt: str, open_pos: int) -> list[tuple[int, int]]:
    """Segments [a, b) separated by top-level commas inside the delimiter at open_pos.

    Generic angle brackets count as nesting only directly inside the delimiter, so
    `HashMap<K, V>` stays one segment while `[u8; 1 << 3]` (inside brackets) is ignored.
    """
    segs, depth, angle, a = [], 0, 0, open_pos + 1
    for p in range(open_pos, len(mt)):
        ch = mt[p]
        if ch in OPEN:
            depth += 1
        elif ch in CLOSE:
            depth -= 1
            if depth == 0:
                segs.append((a, p))
                return segs
        elif ch == "<" and depth == 1:
            angle += 1
        elif ch == ">" and depth == 1 and mt[p - 1] not in "-=" and angle > 0:
            angle -= 1
        elif ch == "," and depth == 1 and angle == 0:
            segs.append((a, p))
            a = p + 1
    return segs


MEMBER_RE = re.compile(r"(" + VIS + r")" + QUAL + r"(fn|const|type)\b")


REL_VIS = re.compile(r"\bpub\s*\(\s*(?:super|self|in\s+(?:super|self)\b)")
INCLUDE = re.compile(r"\binclude(?:_str|_bytes)?\s*!\s*\(")
PLAIN_INCLUDE = re.compile(r"(\binclude(?:_str|_bytes)?\s*!\s*\(\s*)\"([^\"\\]*)\"\s*\)")


def transform_item(text: str, kind: str, parent_path: list[str], deeper_dir: bool) -> tuple[str, dict]:
    """Apply the visibility/path edits to one moved item's text (lead trivia included)."""
    counts: dict = {}
    mt = mask(text)
    edits: list[tuple[int, int, str]] = []  # (pos, len_old, new) applied right-to-left
    edited_rel: set[int] = set()

    def vis_edit(pos: int) -> None:
        m = VIS_AT.match(text, pos)
        old = m.group(0) if m else ""
        new = map_vis(old, parent_path)
        if new is not None:
            edits.append((pos, len(old), new))
            counts["vis"] = counts.get("vis", 0) + 1
            if old:
                edited_rel.add(pos)

    # 1. the item's own visibility: first code offset after attributes
    p0 = skip_ws_attrs(mt, 0)
    if kind not in ("impl", "macro") and not re.compile(VIS + r"const\s+_\s*:").match(mt, p0):
        vis_edit(p0)  # `const _` takes no visibility
    # 2. members
    if kind in ("struct", "union"):
        km = re.compile(VIS + QUAL + r"(struct|union)\s+").match(mt, p0)
        nm = NAME_RE.match(mt, km.end()) if km else None
        if nm:
            ob = find_body_open(mt, nm.end())
            if ob >= 0:
                for a, b in split_top(mt, ob):
                    q = skip_ws_attrs(mt, a)
                    if q < b:
                        vis_edit(q)
    elif kind == "impl":
        header_end = find_brace(mt, p0)
        if header_end >= 0 and is_inherent_header(mt[p0:header_end]):
            # members start after the body brace or after a depth-1 `}`/`;`
            depth, p, starts = 0, header_end, []
            while p < len(mt):
                ch = mt[p]
                if ch in OPEN:
                    depth += 1
                    if depth == 1 and ch == "{":
                        starts.append(p + 1)
                elif ch in CLOSE:
                    depth -= 1
                    if depth == 1 and ch == "}":
                        starts.append(p + 1)
                    if depth == 0:
                        break
                elif ch == ";" and depth == 1:
                    starts.append(p + 1)
                p += 1
            for s in starts:
                q = skip_ws_attrs(mt, s)
                if MEMBER_RE.match(mt, q):
                    vis_edit(q)
    elif kind == "mod" and re.search(r"\bsuper\b", mt):
        raise SystemExit("an inline module that names `super` moves one level deeper; edit by hand")
    # Any relative visibility the edits above did not rewrite (nested items, macro bodies)
    # would change reach one module deeper.
    for m in REL_VIS.finditer(mt):
        if m.start() not in edited_rel:
            raise SystemExit("moved code has a nested pub(super)/pub(self) the tool cannot remap; edit by hand")
    if re.search(r"\b(?:stringify|concat_idents)\s*!\s*\([^()]*\bsuper\b", mt):
        raise SystemExit("moved code stringifies a `super` path; its text would change. Edit by hand.")
    if re.search(r"\bself\s*::\s*super\b", mt):
        raise SystemExit("`self::super` in moved code changes meaning one module deeper; edit by hand")
    for m in re.finditer(r"\bsuper\s*::", text):
        if mt[m.start()] == "x":  # inside a string literal, e.g. #[serde(default = "super::f")]
            raise SystemExit("a string literal in moved code names a `super::` path (an attribute argument?); "
                             "the tool cannot tell whether it resolves relative to the module. Edit by hand.")
    # 3. super:: paths (code only)
    for m in re.finditer(r"(?<![\w:])super(?=\s*::)", mt):
        edits.append((m.start(), 0, "super::"))
        counts["super"] = counts.get("super", 0) + 1
    if re.search(r"\bpub\s*\(\s*in\s+(?!crate\b)", mt):
        raise SystemExit("relative pub(in ...) inside a moved item; edit by hand")
    # 4. include paths, relative to the file
    if deeper_dir:
        for m in INCLUDE.finditer(mt):
            pm = PLAIN_INCLUDE.match(text, m.start())
            if pm and pm.group(2).startswith("/"):
                continue
            if pm:
                edits.append((pm.end(1) + 1, 0, "../"))
                counts["include"] = counts.get("include", 0) + 1
                continue
            arg = text[m.end():m.end() + 80]
            if re.match(r"\s*concat!\s*\(\s*env!\s*\(\s*\"CARGO_MANIFEST_DIR\"", arg):
                continue
            raise SystemExit("an include!/include_str!/include_bytes! argument this tool cannot rebase "
                             "moves one directory deeper; edit by hand")
    if re.search(r"#\s*\[\s*path\s*=", mt):
        raise SystemExit("#[path] attribute inside a moved item; edit by hand")
    for pos, ln, new in sorted(edits, key=lambda e: e[0], reverse=True):
        text = text[:pos] + new + text[pos + ln:]
    return text, counts


# ------------------------------------------------------------ observables

OBS = {
    "tracing_events_without_target": re.compile(
        r"\b(?:tracing::)?(?:trace|debug|info|warn|error|event)!\s*\((?!\s*target\s*:)"),
    "spans_without_target": re.compile(
        r"\b(?:tracing::)?(?:trace_span|debug_span|info_span|warn_span|error_span|span)!\s*\((?!\s*target\s*:)"),
    "instrument_attrs": re.compile(r"#\[\s*(?:tracing::)?instrument\b"),
    "sql_locators": re.compile(r"\bsql!\s*\("),
    "module_path": re.compile(r"\bmodule_path!\s*\("),
    "file_line": re.compile(r"\b(?:file|line|column)!\s*\("),
    "type_name": re.compile(r"\btype_name(?:_of_val)?\b"),
}


def observables(text: str) -> dict:
    mt = mask(text)
    return {k: len(r.findall(mt if k != "instrument_attrs" else text)) for k, r in OBS.items()}


# ------------------------------------------------------------------ apply

@dataclass
class Member:
    kind: str
    name: str
    lead: int  # absolute line indices
    start: int
    end: int

    @property
    def selector(self) -> str:
        return f"{self.kind} {self.name}"


@dataclass
class ImplLayout:
    body_open: int  # line holding the impl's opening `{`
    members: list[Member]
    close: int  # line holding the closing `}`


MEMBER_LINE_RE = re.compile(r"^\s+(?P<vis>" + VIS + r")" + QUAL + r"(?P<kind>fn|const|type)\b")
MEMBER_MACRO_RE = re.compile(r"^\s+(?P<path>[A-Za-z_][A-Za-z0-9_:]*)\s*!")


def is_inherent_header(mheader: str) -> bool:
    header = re.sub(r"\bfor\s*<[^<>]*>", "", mheader)
    while re.search(r"<[^<>]*>", header):
        header = re.sub(r"<[^<>]*>", "", header)
    return not re.search(r"\bfor\s+(?!<)", header)


def impl_layout(lines: list[str], mlines: list[str], it: Item) -> ImplLayout | None:
    """Members of an inherent impl item; None for a trait impl or a one-line impl."""
    joined = "\n".join(mlines[it.start:it.end + 1])
    cut = find_brace(joined, 0)
    if cut < 0:
        return None
    header = joined[:cut]
    body_open = it.start + header.count("\n")
    if not is_inherent_header(header):
        return None
    if body_open == it.end or not mlines[body_open].rstrip().endswith("{"):
        return None
    members: list[Member] = []
    i, lead = body_open + 1, None
    while i < it.end:
        raw, m = lines[i], mlines[i]
        s = raw.strip()
        if s == "":
            i += 1
            continue
        if m.strip() == "":
            lead = i if lead is None else lead
            i += 1
            continue
        if s.startswith("#["):
            lead = i if lead is None else lead
            d = line_depth_delta(m)
            i += 1
            while d > 0 and i < it.end:
                d += line_depth_delta(mlines[i])
                i += 1
            continue
        start, d = i, 0
        while True:
            d += line_depth_delta(mlines[i])
            tail = mlines[i].rstrip()
            if d == 0 and (tail.endswith("}") or tail.endswith(";")):
                break
            i += 1
            if i >= it.end:
                raise SystemExit(f"line {start + 1}: impl member never ends")
        mm = MEMBER_LINE_RE.match(raw)
        if mm:
            nm = NAME_RE.match(raw[mm.end():])
            kind, name = mm.group("kind"), (nm.group(1) if nm else "?")
        else:
            mc = MEMBER_MACRO_RE.match(raw)
            if not mc:
                raise SystemExit(f"line {start + 1}: cannot classify impl member: {raw.strip()[:80]!r}")
            kind, name = "macro", mc.group("path")
        members.append(Member(kind, name, lead if lead is not None else start, start, i))
        lead = None
        i += 1
    return ImplLayout(body_open, members, it.end)


def rewrite_super(text: str) -> str:
    mt = mask(text)
    for m in reversed(list(re.finditer(r"(?<![\w:])super(?=\s*::)", mt))):
        text = text[:m.start()] + "super::" + text[m.start():]
    return text


def attr_lines(lines: list[str], lead: int, start: int) -> list[str]:
    """The attribute lines (whole multi-line attributes) in an item's leading trivia."""
    out, k = [], lead
    mlines = mask("\n".join(lines[lead:start])).split("\n")
    while k < start:
        s = lines[k].strip()
        if s.startswith("#["):
            d = line_depth_delta(mlines[k - lead])
            out.append(lines[k])
            k += 1
            while d > 0 and k < start:
                d += line_depth_delta(mlines[k - lead])
                out.append(lines[k])
                k += 1
            continue
        k += 1
    return out


RANK = {"none": -1, "private": 0, "in": 1, "crate": 2, "pub": 3}
TYPE_NS = {"struct", "enum", "union", "trait", "type", "mod", "mod-decl"}
DEFS = {"struct", "enum", "union", "trait", "impl", "type", "const", "static", "macro"}


def new_reach(orig_vis: str, parent_path: list[str]) -> str:
    """An item's reach after the move: "pub", "crate", "private" (the parent module only),
    or the crate-relative module path of a `pub(in crate::..)`."""
    v = collapse(orig_vis)
    mapped = map_vis(v, parent_path)
    v = collapse(mapped) if mapped is not None else v
    if v == "pub":
        return "pub"
    if v == "pub(crate)":
        return "crate"
    m = re.fullmatch(r"pub\(\s*in\s+crate((?:\s*::\s*\w+)*)\s*\)", v)
    if m:
        return re.sub(r"\s+", "", m.group(1)).lstrip(":") or "crate"
    return "private"


def reexport_vis(reach: set[str]) -> str:
    """The narrowest glob re-export that narrows no item: a glob re-exports each item at the
    smaller of its own visibility and the glob's, so the glob must reach as far as the widest
    item."""
    if "pub" in reach:
        return "pub "
    if "crate" in reach:
        return "pub(crate) "
    paths = [r.split("::") for r in reach if r not in ("private",)]
    if not paths:
        return ""
    common = paths[0]
    for q in paths[1:]:
        n = 0
        while n < min(len(common), len(q)) and common[n] == q[n]:
            n += 1
        common = common[:n]
    return "pub(crate) " if not common else f"pub(in crate::{'::'.join(common)}) "


def new_vis_rank(orig_vis: str, parent_path: list[str]) -> tuple[int, str]:
    """Rank of an item's visibility after the move, for choosing the re-export."""
    v = collapse(orig_vis)
    mapped = map_vis(v, parent_path)
    v = collapse(mapped) if mapped is not None else v
    if v == "pub":
        return RANK["pub"], ""
    if v == "pub(crate)":
        return RANK["crate"], ""
    if v.startswith("pub(in "):
        return RANK["in"], v
    return RANK["private"], ""  # pub(super): reaches the parent only


def code_lines(text: str) -> int:
    return sum(1 for ln in mask(text).split("\n") if ln.strip())


def strip_super(sel: str) -> str:
    return re.sub(r"\bsuper::", "", collapse(sel))


def impl_block(lines: list[str], it: Item, lay: ImplLayout, mids: list[int], lead_lines: list[str],
               child: bool, trailing: bool, parent_path: list[str], deeper: bool, counts: dict) -> str:
    """An impl block holding the members `mids` (indices into lay.members), in order."""
    header = "\n".join(lines[it.start:lay.body_open + 1])
    lead = "\n".join(lead_lines)
    out = (rewrite_super(lead) if child else lead).split("\n") if lead_lines else []
    out.extend((rewrite_super(header) if child else header).split("\n"))
    prev = None
    for mi in mids:
        m = lay.members[mi]
        if prev is not None:
            if prev == mi - 1:
                out.extend(lines[lay.members[prev].end + 1:m.lead])
            else:
                out.append("")
        text = "\n".join(lines[m.lead:m.end + 1])
        if child:
            text, c = transform_item(text, "macro" if m.kind == "macro" else "fn", parent_path, deeper)
            for key, v in c.items():
                counts[key] = counts.get(key, 0) + v
        out.extend(text.split("\n"))
        prev = mi
    if trailing:
        out.extend(lines[lay.members[-1].end + 1:lay.close])  # trailing comments in the body
    out.append(lines[lay.close])
    return "\n".join(out)


def apply(spec_path: str, repo: str, dry: bool, report: str | None) -> int:
    spec = tomllib.load(open(spec_path, "rb"))
    rel = spec["file"]
    path = os.path.join(repo, rel)
    src = open(path, encoding="utf-8").read()
    items, header_end = parse_items(src)
    lines = src.split("\n")
    mlines = mask(src).split("\n")
    parent_path, beside = file_layout(rel)
    deeper = not beside
    child_dir = os.path.join(os.path.dirname(path), os.path.basename(rel)[:-3]) if deeper else os.path.dirname(path)
    loaded_by_path = path_attribute_users(repo, rel)
    if loaded_by_path:
        raise SystemExit(f"{rel} is loaded through #[path] ({loaded_by_path}); its child modules would "
                         "not resolve under the directory this tool writes. Remove the attribute first.")
    mods = spec["module"]
    names = [m["name"] for m in mods]
    if len(set(names)) != len(names):
        raise SystemExit("duplicate module names in spec")
    layouts: dict[int, ImplLayout | None] = {}

    def layout(k: int) -> ImplLayout | None:
        if k not in layouts:
            layouts[k] = impl_layout(lines, mlines, items[k]) if items[k].kind == "impl" else None
        return layouts[k]

    assign: dict[int, str] = {}
    massign: dict[tuple[int, int], str] = {}
    for mod in mods:
        name = mod["name"]
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name) or name in KEYWORDS:
            raise SystemExit(f"bad module name {name!r}")
        for pat in mod["items"]:
            if " :: " in pat:
                ipat, mpat = pat.split(" :: ", 1)
                hits = []
                for k, it in enumerate(items):
                    if it.kind == "impl" and fnmatch.fnmatchcase(it.selector, ipat):
                        lay = layout(k)
                        if lay is None:
                            raise SystemExit(f"{it.selector}: only inherent impls can be split by member")
                        hits += [(k, mi) for mi, m in enumerate(lay.members) if fnmatch.fnmatchcase(m.selector, mpat)]
                if not hits:
                    raise SystemExit(f"module {name}: pattern {pat!r} matches no impl member")
                for km in hits:
                    if km in massign and massign[km] != name:
                        raise SystemExit(f"{pat}: member matched by {massign[km]} and {name}")
                    massign[km] = name
                continue
            hits = [k for k, it in enumerate(items) if fnmatch.fnmatchcase(it.selector, pat)]
            if not hits:
                raise SystemExit(f"module {name}: pattern {pat!r} matches no item")
            for k in hits:
                it = items[k]
                if it.kind in UNMOVABLE or it.kind == "mod-decl":
                    raise SystemExit(f"{it.selector}: use/mod declarations stay in the parent")
                if it.kind == "macro_rules!":
                    raise SystemExit(f"{it.selector}: macro_rules! stays in the parent (textual scope)")
                if k in assign and assign[k] != name:
                    raise SystemExit(f"{it.selector} matched by {assign[k]} and {name}")
                assign[k] = name
    # A macro_rules! that stays in the parent and emits a relative visibility (`pub(super)`),
    # directly or through another parent macro, would mean something else when expanded one
    # module deeper. A macro name defined twice binds by textual position, which the new
    # `mod` lines (placed after every macro) would change.
    macro_defs = [it for it in items if it.kind == "macro_rules!"]
    macro_names = [it.name for it in macro_defs]
    moved_text = "\n".join("\n".join(mlines[items[k].start:items[k].end + 1]) for k in assign)
    moved_text += "\n" + "\n".join("\n".join(mlines[layout(k).members[mi].start:layout(k).members[mi].end + 1])
                                   for k, mi in massign)

    def invokes(text: str, name: str) -> bool:
        return re.search(r"(?<![\w:])" + re.escape(name) + r"\s*!", text) is not None

    for name in {n for n in macro_names if macro_names.count(n) > 1}:
        if invokes(moved_text, name):
            raise SystemExit(f"macro_rules! {name} is defined more than once in the parent and moved code "
                             "invokes it; which definition it binds to would change. Edit by hand.")
    bodies = {it.name: "\n".join(mlines[it.start:it.end + 1]) for it in macro_defs}
    relative = {n for n, b in bodies.items() if REL_VIS.search(b)}
    while True:
        more = {n for n, b in bodies.items() if n not in relative and any(invokes(b, r) for r in relative)}
        if not more:
            break
        relative |= more
    for name in sorted(relative):
        if invokes(moved_text, name):
            raise SystemExit(f"moved code invokes macro_rules! {name}, which emits a relative visibility "
                             "(pub(super)/pub(self)), directly or through another macro; its expansion would "
                             "change reach. Keep its callers in the parent or edit the macro by hand.")
    split_impls = sorted({k for k, _ in massign})
    for k in split_impls:
        if k in assign:
            raise SystemExit(f"{items[k].selector}: selected whole and by member")
    imported: set[str] = set()
    for it in items:
        if it.kind in ("use", "extern crate"):
            body = mask("\n".join(lines[it.start:it.end + 1]))
            imported |= set(re.findall(r"\b(\w+)\s*(?=[,;}])", body))
            imported |= set(re.findall(r"\bas\s+(\w+)", body))
            imported |= set(re.findall(r"\b(\w+)\s*::\s*\{\s*self\b", body))
    for it in items:
        if it.kind in TYPE_NS and it.name in names:
            raise SystemExit(f"module name {it.name!r} collides with {it.selector}")
    for n in names:
        if n in imported:
            raise SystemExit(f"module name {n!r} collides with a name the parent imports")

    # Where a split impl's doc comments go: with the members the parent keeps, else with the
    # first module (in spec order) that receives members.
    lead_owner: dict[int, str | None] = {}
    for k in split_impls:
        lay = layout(k)
        kept = [mi for mi in range(len(lay.members)) if (k, mi) not in massign]
        first = min(mi for kk, mi in massign if kk == k)
        lead_owner[k] = None if kept else massign[(k, first)]

    results = {}
    for mod in mods:
        name = mod["name"]
        chunks, counts, obs, sels = [], {}, {k: 0 for k in OBS}, []
        reach: set[str] = set()  # "pub", "crate", "private", or a crate-relative path for pub(in ..)
        for k, it in enumerate(items):
            if assign.get(k) == name:
                text = "\n".join(lines[it.lead:it.end + 1]).lstrip("\n")
                new, c = transform_item(text, it.kind, parent_path, deeper)
                for key, v in c.items():
                    counts[key] = counts.get(key, 0) + v
                for key, v in observables(text).items():
                    obs[key] += v
                chunks.append(new)
                sels.append(it.selector)
                if it.kind == "macro":
                    # What a macro invocation emits is unknown: re-export at most `pub`, which a
                    # glob clamps to each emitted item's own visibility, so nothing widens.
                    counts["macro_items"] = counts.get("macro_items", 0) + 1
                    reach.add("pub")
                elif it.kind != "impl":
                    reach.add(new_reach(it.vis, parent_path))
            elif k in split_impls:
                lay = layout(k)
                mids = [mi for mi in range(len(lay.members)) if massign.get((k, mi)) == name]
                if not mids:
                    continue
                lead_lines = (lines[it.lead:it.start] if lead_owner[k] == name
                              else attr_lines(lines, it.lead, it.start))
                while lead_lines and not lead_lines[0].strip():
                    lead_lines = lead_lines[1:]
                block = impl_block(lines, it, lay, mids, lead_lines, True, lead_owner[k] == name,
                                   parent_path, deeper, counts)
                for mi in mids:
                    m = lay.members[mi]
                    for key, v in observables("\n".join(lines[m.lead:m.end + 1])).items():
                        obs[key] += v
                counts["members"] = counts.get("members", 0) + len(mids)
                chunks.append(block)
                sels.append(it.selector)
        if not chunks:
            raise SystemExit(f"module {name}: no items")
        doc = mod.get("doc", "").strip()
        body = ""
        if doc:
            body += "".join(f"//! {d}\n" if d.strip() else "//!\n" for d in doc.split("\n")) + "\n"
        glob = mod.get("glob_import", True)
        if not isinstance(glob, bool):
            raise SystemExit(f"module {name}: glob_import must be true or false")
        body += ("use super::*;\n\n" if glob else "") + "\n\n".join(chunks) + "\n"
        ks = [k for k in range(len(items)) if assign.get(k) == name or
              (k in split_impls and any(massign.get((k, mi)) == name for mi in range(len(layout(k).members))))]
        cfgs = [set(items[k].cfg) for k in ks]
        common_cfg = sorted(set.intersection(*cfgs)) if cfgs else []
        reexport = "auto" if reach else "none"  # only impl blocks: nothing to name
        use_vis = reexport_vis(reach)
        results[name] = dict(items=sels, text=body, counts=counts, obs=obs,
                             cfg=common_cfg, use_vis=use_vis, reexport=reexport,
                             code=code_lines(body), path=os.path.join(child_dir, name + ".rs"))

    # declarations for the parent
    decls = []
    for mod in mods:
        r = results[mod["name"]]
        decls.append("".join(c + "\n" for c in r["cfg"]) + f"mod {mod['name']};")
    uses = []
    for mod in mods:
        r = results[mod["name"]]
        if r["reexport"] == "none":
            continue
        gate = "".join(c + "\n" for c in r["cfg"])
        uses.append(f"{gate}{r['use_vis']}use {mod['name']}::*;")
    decl_text = "\n".join(decls) + ("\n\n" + "\n".join(uses) if uses else "")

    # parent remainder: kept items in order; keep original spacing between items that were
    # adjacent, one blank line where moved items were removed.
    keep = []
    for k in range(len(items)):
        if k in assign:
            continue
        if k in split_impls and lead_owner[k] is not None:
            continue  # every member moved
        keep.append(k)
    first_def = next((k for k, it in enumerate(items) if it.kind in DEFS), len(items))
    decl_after = -1
    for idx, k in enumerate(keep):
        if k < first_def and items[k].kind in ("use", "extern crate", "mod-decl"):
            decl_after = idx
        if items[k].kind == "macro_rules!":
            decl_after = max(decl_after, idx)
    out = lines[:header_end]
    while out and not out[-1].strip():
        out.pop()

    def sep() -> None:
        if out and out[-1].strip():
            out.append("")

    if decl_after == -1:
        sep()
        out.extend(decl_text.split("\n"))
    prev = None
    for idx, k in enumerate(keep):
        it = items[k]
        if prev is not None and prev == k - 1:
            out.extend(lines[items[prev].end + 1:it.lead])  # original gap (blank lines)
        else:
            sep()
        if k in split_impls:
            lay = layout(k)
            kept_m = [mi for mi in range(len(lay.members)) if (k, mi) not in massign]
            out.extend(impl_block(lines, it, lay, kept_m, lines[it.lead:it.start], False, True,
                                  parent_path, deeper, {}).split("\n"))
        else:
            out.extend(lines[it.lead:it.end + 1])
        if idx == decl_after:
            sep()
            out.extend(decl_text.split("\n"))
            prev = None
        else:
            prev = k
    trailing = lines[items[-1].end + 1:] if items else []
    while trailing and not trailing[-1].strip():
        trailing.pop()
    if trailing:
        sep()
        out.extend(trailing)
    parent_text = "\n".join(out).rstrip("\n") + "\n"

    # self-check: re-parse everything and compare item sequences
    for name, r in results.items():
        got = [strip_super(it.selector) for it in parse_items(r["text"])[0] if it.kind != "use"]
        want = [strip_super(x) for x in r["items"]]
        if got != want:
            raise SystemExit(f"self-check failed for {name}: {got} != {want}")
    new_decl = {f"mod-decl {n}" for n in names}
    got = [it.selector for it in parse_items(parent_text)[0]
           if it.selector not in new_decl and not re.fullmatch(
               r"use (?:pub(?:\([^)]*\))? )?use (" + "|".join(names) + r")::\*;", "use " + it.name)]
    want = [items[k].selector for k in keep]
    if got != want:
        missing = set(want) - set(got)
        extra = set(got) - set(want)
        raise SystemExit(f"self-check failed for parent: missing {missing}, extra {extra}")

    ratchets = ratchet_hits(repo, results)
    summary = {
        "file": rel,
        "parent_code_before": code_lines(src),
        "parent_code_after": code_lines(parent_text),
        "modules": {n: {k: v for k, v in r.items() if k != "text"} for n, r in results.items()},
        "pattern_ratchet_lines_moved": ratchets,
        "kept": [items[k].selector for k in keep if items[k].kind not in ("use", "mod-decl")],
    }
    if not dry:
        for name, r in results.items():
            if os.path.exists(r["path"]):
                raise SystemExit(f"{r['path']} already exists")
        os.makedirs(child_dir, exist_ok=True)
        for name, r in results.items():
            with open(r["path"], "w", encoding="utf-8") as f:
                f.write(r["text"])
        with open(path, "w", encoding="utf-8") as f:
            f.write(parent_text)
    print(json.dumps(summary, indent=1, default=str))
    if report:
        write_report(report, summary, repo)
    return 0


def path_attribute_users(repo: str, rel: str) -> str:
    """A `#[path = "..."]` in the crate that names this file, if any."""
    crate_src = os.path.join(repo, rel.replace(os.sep, "/").split("/src/", 1)[0], "src")
    target = os.path.realpath(os.path.join(repo, rel))
    for d, _, files in os.walk(crate_src):
        for f in files:
            if not f.endswith(".rs"):
                continue
            fp = os.path.join(d, f)
            text = open(fp, encoding="utf-8", errors="replace").read()
            masked = mask(text)
            for m in re.finditer(r'#\s*\[\s*path\s*=\s*r?(#*)"([^"]+)"\1\s*\]', text):
                if masked[m.start()] != "#":
                    continue  # inside a comment
                if os.path.realpath(os.path.join(d, m.group(2))) == target:
                    return os.path.relpath(fp, crate_src)
    return ""


def ratchet_hits(repo: str, results: dict) -> dict:
    """Lines matching an archcheck pattern_ratchet that a module would now hold. A ratchet
    counts lines per file, so a moved match needs its baseline entry moved too."""
    arch = os.path.join(repo, "ARCHITECTURE.toml")
    if not os.path.exists(arch):
        return {}
    rules = tomllib.load(open(arch, "rb")).get("rules", {}).get("pattern_ratchet", {})
    hits: dict = {}
    for inst, cfg in rules.items():
        pats = [re.compile(p) for p in cfg.get("patterns", [])]
        for name, r in results.items():
            n = sum(1 for ln in r["text"].split("\n") if any(p.search(ln) for p in pats))
            if n:
                hits.setdefault(inst, {})[name] = n
    return hits


# ------------------------------------------------------------------ verify

def git(repo: str, *args: str) -> str:
    return subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True, check=True).stdout


def git_show(repo: str, rev: str, path: str) -> str | None:
    r = subprocess.run(["git", "-C", repo, "show", f"{rev}:{path}"], capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


def parent_candidates(child: str) -> list[str]:
    d = os.path.dirname(child)
    if os.path.basename(d) == "src":
        return [os.path.join(d, "lib.rs"), os.path.join(d, "main.rs")]
    return [d + ".rs", os.path.join(d, "mod.rs")]


def crate_edition(repo: str, rev: str, rel: str) -> str:
    d = os.path.dirname(rel)
    while d:
        manifest = git_show(repo, rev, f"{d}/Cargo.toml")
        if manifest is not None:
            pkg = tomllib.loads(manifest).get("package", {})
            ed = pkg.get("edition")
            if isinstance(ed, str):
                return ed
            break
        d = os.path.dirname(d)
    root = tomllib.loads(git_show(repo, rev, "Cargo.toml") or "")
    return root.get("workspace", {}).get("package", {}).get("edition", "2024")


def rustfmt(repo: str, text: str, edition: str) -> str:
    r = subprocess.run(["rustfmt", "--edition", edition, "--emit", "stdout", "--quiet"], cwd=repo,
                       input=text, capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"rustfmt failed: {r.stderr.strip()[:400]}")
    return r.stdout


def header_key(sel: str) -> str:
    """An impl selector without `super::`, whitespace or trailing commas, so a header matches
    its base however rustfmt wrapped it."""
    return re.sub(r",(?=[>)\]}])", "", re.sub(r"\s+", "", strip_super(sel)))


def escape_glob(sel: str) -> str:
    return re.sub(r"([*?\[])", r"[\1]", sel)


def derive_spec(repo: str, base: str, head: str, parent: str, children: list[str]) -> str:
    """The spec `apply` would need to turn the base parent into the head files."""
    base_items = parse_items(git_show(repo, base, parent) or "")[0]
    head_parent = git_show(repo, head, parent) or ""
    hp_items = parse_items(head_parent)[0]
    hp_lines = head_parent.split("\n")
    order = [it.name for it in hp_items if it.kind == "mod-decl"]
    names = sorted(children, key=lambda n: order.index(n) if n in order else len(order))
    out = [f"file = {json.dumps(parent)}"]
    for name in names:
        child = os.path.join(os.path.dirname(parent), os.path.basename(parent)[:-3], f"{name}.rs") \
            if not file_layout(parent)[1] else os.path.join(os.path.dirname(parent), f"{name}.rs")
        src = git_show(repo, head, child) or ""
        items, hdr = parse_items(src)
        lines, mlines = src.split("\n"), mask(src).split("\n")
        doc = "\n".join(re.sub(r"^//! ?", "", ln.strip()) for ln in lines[:hdr] if ln.strip().startswith("//!"))
        sels: list[str] = []
        for it in items:
            if it.kind == "use" and collapse("\n".join(lines[it.start:it.end + 1])) == "use super::*;":
                continue
            lay = impl_layout(lines, mlines, it) if it.kind == "impl" else None
            if lay and lay.members:
                base_sel = next((b.selector for b in base_items if b.kind == "impl"
                                 and header_key(b.selector) == header_key(it.selector)), it.selector)
                sels += [f"{escape_glob(base_sel)} :: {escape_glob(m.selector)}" for m in lay.members]
            else:
                sels.append(escape_glob(it.selector))
        has_glob = any(it.kind == "use" and collapse("\n".join(lines[it.start:it.end + 1])) == "use super::*;"
                       for it in items)
        out.append(f"\n[[module]]\nname = {json.dumps(name)}")
        if doc:
            out.append(f"doc = {json.dumps(doc)}")
        if not has_glob:
            out.append("glob_import = false")
        out.append("items = [\n" + "".join(f"  {json.dumps(s)},\n" for s in dict.fromkeys(sels)) + "]")
    return "\n".join(out) + "\n"


def verify(repo: str, base: str, head: str, print_spec: bool = False) -> int:
    """Prove BASE...HEAD's Rust changes are exactly what `apply` generates: derive the spec from
    the diff, regenerate from the merge base, and compare byte for byte after rustfmt."""
    import contextlib
    import difflib
    import io
    import shutil
    import tempfile

    merge_base = git(repo, "merge-base", base, head).strip()
    raw = git(repo, "diff", "--raw", "-z", "--no-renames", "--no-abbrev", f"{merge_base}..{head}", "--", "*.rs")
    fields = raw.split("\0")
    status, modes = [], {}
    for k in range(0, len(fields) - 1, 2):
        meta, path = fields[k].split(), fields[k + 1]
        status.append((meta[4], path))
        modes[path] = (meta[0].lstrip(":"), meta[1])
    irregular = {p for p, (old, new) in modes.items()
                 if new not in ("100644", "100755", "000000") or old not in ("100644", "100755", "000000")}
    added = sorted(p for st, p in status if st == "A" and p not in irregular)
    modified = {p for st, p in status if st == "M" and p not in irregular}
    problems: list[str] = [f"{p}: deleted" for st, p in status if st == "D"]
    problems += [f"{p}: {st}" for st, p in status if st not in ("A", "M", "D")]
    problems += [f"{p}: not a regular file (mode {modes[p][0]} -> {modes[p][1]})" for p in sorted(irregular)]
    groups: dict[str, list[str]] = {}
    for p in added:
        parent = next((c for c in parent_candidates(p) if c in modified), None)
        if parent is None:
            problems.append(f"{p}: new file whose parent module is not part of the change")
            continue
        groups.setdefault(parent, []).append(os.path.splitext(os.path.basename(p))[0])
    for p in sorted(modified - set(groups)):
        problems.append(f"{p}: changed, but no child module was split out of it")
    checked = 0
    for parent, children in groups.items():
        spec = derive_spec(repo, merge_base, head, parent, children)
        if print_spec:
            print(f"# derived spec for {parent}\n{spec}")
        crate_src = parent.split("/src/", 1)[0] + "/src"
        tmp = tempfile.mkdtemp(prefix="split-verify-")
        try:
            paths = [crate_src] + (["ARCHITECTURE.toml"] if git_show(repo, merge_base, "ARCHITECTURE.toml") else [])
            archive = subprocess.run(["git", "-C", repo, "archive", merge_base, *paths],
                                     capture_output=True, check=True).stdout
            subprocess.run(["tar", "-x", "-C", tmp], input=archive, check=True)
            spec_path = os.path.join(tmp, "spec.toml")
            with open(spec_path, "w") as f:
                f.write(spec)
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    apply(spec_path, tmp, False, None)
            except SystemExit as e:
                problems.append(f"{parent}: regenerating from the derived spec failed: {e}")
                continue
            edition = crate_edition(repo, merge_base, parent)
            beside = file_layout(parent)[1]
            cdir = os.path.dirname(parent) if beside else parent[:-3]
            for rel in [parent] + [f"{cdir}/{c}.rs" for c in children]:
                want = rustfmt(repo, open(os.path.join(tmp, rel), encoding="utf-8").read(), edition)
                got = rustfmt(repo, git_show(repo, head, rel) or "", edition)
                checked += 1
                if want != got:
                    diff = list(difflib.unified_diff(want.split("\n"), got.split("\n"), "regenerated/" + rel,
                                                     "head/" + rel, lineterm="", n=2))
                    problems.append(f"{rel}: differs from what apply generates\n      " + "\n      ".join(diff[:40]))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    if not groups and not problems:
        problems.append("no split found: no new child module next to a changed parent")
    print(f"regenerated and compared {checked} files from {len(groups)} split parents: "
          f"{'identical' if not problems else str(len(problems)) + ' problems'}")
    for p in problems:
        print("  " + p)
    return 1 if problems else 0


def write_report(out: str, s: dict, repo: str) -> None:
    rows = []
    for name, m in s["modules"].items():
        rows.append(f"| `{name}` | {m['code']} | {len(m['items'])} | "
                    f"{m['counts'].get('vis', 0)} | {m['counts'].get('super', 0)} | "
                    f"{m['counts'].get('include', 0)} | {m['obs']['tracing_events_without_target']} | "
                    f"{m['obs']['spans_without_target'] + m['obs']['instrument_attrs']} | "
                    f"{m['obs']['sql_locators']} | {m['obs']['type_name']} |")
    with open(out, "w") as f:
        f.write(f"Split of `{s['file']}`: parent {s['parent_code_before']} -> "
                f"{s['parent_code_after']} code lines.\n\n")
        f.write("| module | code lines | items | vis edits | super edits | include edits | "
                "events w/o target | spans/instrument | sql! | type_name |\n|--|--:|--:|--:|--:|--:|--:|--:|--:|--:|\n")
        f.write("\n".join(rows) + "\n")


def list_items(path: str) -> int:
    src = open(path, encoding="utf-8").read()
    items, _ = parse_items(src)
    lines = src.split("\n")
    mlines = mask(src).split("\n")
    for it in items:
        text = "\n".join(lines[it.lead:it.end + 1])
        cfg = (" " + " ".join(collapse(c) for c in it.cfg)) if it.cfg else ""
        print(f"{it.start + 1:6}-{it.end + 1:<6} {code_lines(text):5}  {it.selector}{cfg}")
        lay = impl_layout(lines, mlines, it) if it.kind == "impl" else None
        for m in (lay.members if lay else []):
            mtext = "\n".join(lines[m.lead:m.end + 1])
            print(f"{m.start + 1:6}-{m.end + 1:<6} {code_lines(mtext):5}      {it.selector} :: {m.selector}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("list")
    a.add_argument("file")
    b = sub.add_parser("apply")
    b.add_argument("spec")
    b.add_argument("--repo", default=".")
    b.add_argument("--dry-run", action="store_true")
    b.add_argument("--report")
    c = sub.add_parser("verify", help="prove BASE...HEAD is a move made the way `apply` makes it")
    c.add_argument("base", nargs="?", default="origin/main")
    c.add_argument("--head", default="HEAD")
    c.add_argument("--repo", default=".")
    c.add_argument("--print-spec", action="store_true", help="print the spec derived from the diff")
    args = ap.parse_args()
    if args.cmd == "list":
        return list_items(args.file)
    if args.cmd == "verify":
        return verify(args.repo, args.base, args.head, args.print_spec)
    return apply(args.spec, args.repo, args.dry_run, args.report)


if __name__ == "__main__":
    sys.exit(main())
