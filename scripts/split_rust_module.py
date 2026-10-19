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
  split_rust_module.py verify [BASE] [--head REV] [--print-spec] [--allow-non-rust]

`verify` proves that BASE...HEAD's Rust changes are exactly what `apply` generates: it
derives the spec from the diff (which items each new child module holds), regenerates
from the merge base, and compares every parent and child file byte for byte after
rustfmt. Any other Rust change, a missing or altered `mod`/`use` line, a dropped item, an
edit inside a moved item or a comment, or a symlink fails it, and so does a non-Rust change
inside the split crate (a manifest, data or build input) unless `--allow-non-rust` is
given; non-Rust changes elsewhere are listed, not checked. It checks the generator,
not the generator's semantics: those rest on the edits listed above, the refusals below,
and compiling and testing the result.

Spec (TOML):
  file = "crates/libs/x/src/foo.rs"
  [[module]]
  name = "bar"
  doc = "What bar holds."            # one `//!` line at the top of the new file
  items = ["fn parse_*", "struct Config", "impl Config", "impl Display for Config"]
  # optional: allow_unused_glob = true   (the module needs nothing from its parent: keep
  #           `use super::*;` under #[allow(unused_imports)], which -D warnings would reject)
  # optional: allow_unused_reexport = true   (the parent's `use child::*;` is kept, under
  #           #[allow(unused_imports)], when nothing outside the module names its items)
  # optional: allow_macro_items = true   (move top-level macro invocations; their emitted
  #           items keep the visibility the macro gives them)

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
  parent, above the new `mod` lines, so children still see them), and any split of a
  parent with a `#[macro_use]` item;
* a parent file loaded through `#[path]`, or a `#[path]` inside moved code;
* moved code reaching a macro whose expansion names `super`/`self` paths or emits
  `pub(super)`/`pub(self)`: any macro_rules! in the crate, directly, through other macros,
  path-qualified or through a `use .. as` alias; a macro invoked before the parent defines
  it, or defined twice; `super` passed in a user macro's arguments, path-qualified or not
  (format/tracing-style macros are fine unless the crate defines a macro of the same
  name); `stringify!` of `super`; `include!`;
* top-level or impl-member macro invocations, unless the module sets allow_macro_items;
* a doc comment in moved code naming a super::/self:: path (an intra-doc link would point
  one module off);
* relative visibility the edits cannot remap: `pub(in super::..)`, a `pub(super)` nested
  inside a moved item, an inline module naming `super`, `self::super`, a `super::` path
  inside a string literal, include arguments other than a plain string literal;
* a module name that is a keyword, collides with an item or import of the parent, or is
  already used as a path in the parent or its child modules;
* an attribute sharing a line with its item or impl member; a carriage return; a
  `src/bin/<name>.rs` crate root; a code block in a module doc.

Not analyzed: macros defined outside the crate being split (other workspace crates,
dependencies, proc and attribute macros). One that emits `pub(super)` or a `super::` path
expands one module deeper without a refusal; compiling and testing the result is the check.
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
    # Header: everything up to the last inner doc (`//!`, `/*! */`) or inner attribute
    # (`#![...]`) that precedes the first item, including plain comments and blank lines
    # before it (a license line above the module docs stays with the parent).
    i = 0
    header_end = 0
    depth = 0
    in_inner_block = False
    while i < n:
        raw = lines[i]
        s = raw.strip()
        if in_inner_block:
            if "*/" in s:
                in_inner_block = False
            i += 1
            header_end = i
            continue
        if s.startswith("//!"):
            i += 1
            header_end = i
            continue
        if s.startswith("/*!"):
            in_inner_block = "*/" not in s[3:]
            i += 1
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
        if s == "" or mlines[i].strip() == "":
            i += 1  # blank or plain comment: header only if an inner doc/attribute follows
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
            if d == 0 and i - 1 > lead and re.search(r"\][^\]]*\S", mlines[i - 1]) and \
                    not mlines[i - 1].rstrip().endswith("]"):
                raise SystemExit(f"line {i}: an attribute and an item share a line; put the item on its own line")
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
        if mlines[k - lead].strip().startswith("#["):
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


def macro_group(mt: str, pos: int) -> tuple[int, int] | None:
    """The span of the delimited token group starting at the first (, [ or { at or after pos."""
    m = re.compile(r"\s*([(\[{])").match(mt, pos)
    if not m:
        return None
    depth, p = 0, m.start(1)
    while p < len(mt):
        if mt[p] in OPEN:
            depth += 1
        elif mt[p] in CLOSE:
            depth -= 1
            if depth == 0:
                return m.start(1), p + 1
        p += 1
    return None


# `super` that starts a path or a use tree (`super::x`, `use super as y`, `use super;`), not
# the `super` of a `pub(super)` / `pub(in super..)` visibility.
SUPER_PATH = re.compile(r"(?<![\w:])super\b")


def super_path_positions(mt: str) -> list[int]:
    out = []
    for m in SUPER_PATH.finditer(mt):
        before = mt[max(0, m.start() - 12):m.start()]
        if re.search(r"pub\s*\(\s*(?:in\s+)?$", before):
            continue
        out.append(m.start())
    return out


# Macros whose arguments are ordinary expressions, paths or format arguments: a deeper
# `super::` path inside them means what it meant before.
KNOWN_MACROS = set("""format format_args print println eprint eprintln write writeln vec assert assert_eq
assert_ne debug_assert debug_assert_eq debug_assert_ne matches panic unreachable todo unimplemented dbg
trace debug info warn error event trace_span debug_span info_span warn_span error_span span anyhow bail
ensure sql json""".split())

# macro_rules! names defined in the crate being split (set by apply): a crate macro that
# shares a name with a KNOWN_MACROS entry gets no exemption.
CRATE_MACROS: set[str] = set()

REL_VIS = re.compile(r"\bpub\s*\(\s*(?:super|self|in\s+(?:super|self)\b)")
INCLUDE = re.compile(r"\binclude_(?:str|bytes)\s*!\s*[(\[{]")
PLAIN_INCLUDE = re.compile(r"(\binclude_(?:str|bytes)\s*!\s*[(\[{]\s*)\"([^\"\\]*)\"\s*[)\]}]")


def doc_texts(text: str, mt: str) -> list[str]:
    """The doc comments (`///`, `//!`, `/** */`, `/*! */`) and `#[doc ..]` attributes in text."""
    mc = mask(text, "\0")
    out = []
    for m in re.finditer(r"/[/*]", text):
        p = m.start()
        if mc[p] != "\0" or (p and mc[p - 1] == "\0"):
            continue  # not a comment, or inside one
        if text[p + 1] == "/":
            e = text.find("\n", p)
            body = text[p:len(text) if e < 0 else e]
            doc = re.match(r"///(?!/)|//!", body)
        else:
            depth, e = 1, p + 2
            while e < len(text) and depth:
                depth += 1 if text.startswith("/*", e) else -1 if text.startswith("*/", e) else 0
                e += 2 if text.startswith(("/*", "*/"), e) else 1
            body = text[p:e]
            doc = re.match(r"/\*\*(?![*/])|/\*!", body)
        if doc:
            out.append(body)
    for m in re.finditer(r"#\s*!?\s*\[\s*doc\b", mt):
        g = macro_group(mt, m.start() + mt[m.start():].index("["))
        if g:
            out.append(text[g[0]:g[1]])
    return out


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
            kind_key = "vis_super" if collapse(old) == "pub(super)" else "vis_private"
            counts[kind_key] = counts.get(kind_key, 0) + 1
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
    for m in re.finditer(r"\b(?:stringify|concat_idents)\s*!", mt):
        g = macro_group(mt, m.end())
        if g and re.search(r"\bsuper\b", mt[g[0]:g[1]]):
            raise SystemExit("moved code stringifies a `super` path; its text would change. Edit by hand.")
    if re.search(r"\bmod\s+(?:r#)?\w+\s*\{", mt) and re.search(r"\bsuper\b", mt):
        raise SystemExit("moved code has an inline module and names `super`; which module each `super` "
                         "means depends on nesting the tool does not track. Edit by hand.")
    for m in re.finditer(r"(?<![\w$:])(?:\$?\w+\s*::\s*)*(\w+)\s*!\s*(?=[(\[{])", mt):
        if m.group(1) in KNOWN_MACROS and m.group(1) not in CRATE_MACROS:
            continue
        g = macro_group(mt, m.end())
        if g and re.search(r"\bsuper\b", mt[g[0]:g[1]]):
            raise SystemExit(f"moved code passes `super` to {m.group(1)}!; a macro can match or stringify those "
                             "tokens, so deepening them may change what it expands to. Edit by hand.")
    if any(re.search(r"\b(?:super|self)\s*::", d) for d in doc_texts(text, mt)):
        raise SystemExit("a doc comment in moved code names a super::/self:: path (an intra-doc link?); "
                             "one module deeper it would point elsewhere. Edit by hand.")
    if re.search(r"\binclude\s*!\s*[(\[{]", mt):
        raise SystemExit("moved code uses include!; the included Rust code resolves its paths from the module "
                         "it is expanded in, which the tool cannot rewrite. Edit by hand.")
    if re.search(r"\bself\s*::\s*super\b", mt):
        raise SystemExit("`self::super` in moved code changes meaning one module deeper; edit by hand")
    for m in re.finditer(r"\bsuper\s*::", text):
        if mt[m.start()] == "x":  # inside a string literal, e.g. #[serde(default = "super::f")]
            raise SystemExit("a string literal in moved code names a `super::` path (an attribute argument?); "
                             "the tool cannot tell whether it resolves relative to the module. Edit by hand.")
    # 3. super:: paths (code only)
    for pos in super_path_positions(mt):
        edits.append((pos, 0, "super::"))
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
        if s.startswith("#[") and attr_then_code(m):
            raise SystemExit(f"line {i + 1}: an attribute and an impl member share a line; put the member on its own line")
        if s.startswith("#["):
            lead = i if lead is None else lead
            first_attr = i
            d = line_depth_delta(m)
            i += 1
            while d > 0 and i < it.end:
                d += line_depth_delta(mlines[i])
                i += 1
            if d == 0 and i - 1 > first_attr and not mlines[i - 1].rstrip().endswith("]"):
                raise SystemExit(f"line {i}: an attribute and an impl member share a line; put the member on its own line")
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
    for pos in reversed(super_path_positions(mt)):
        text = text[:pos] + "super::" + text[pos:]
    return text


def attr_lines(lines: list[str], lead: int, start: int) -> list[str]:
    """The attribute lines (whole multi-line attributes) in an item's leading trivia; an
    attribute written inside a comment is not one."""
    out, k = [], lead
    mlines = mask("\n".join(lines[lead:start])).split("\n")
    while k < start:
        s = mlines[k - lead].strip()
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


TYPE_NS = {"struct", "enum", "union", "trait", "type", "mod", "mod-decl"}
DEFS = {"struct", "enum", "union", "trait", "impl", "type", "const", "static", "macro"}


def doc_comment(doc: str) -> str:
    """`//!` lines for a module doc, paragraphs separated by blank lines, wrapped at 100 columns."""
    import textwrap

    if "```" in doc or "~~~" in doc:
        raise SystemExit("a module doc may not contain a code block (rustdoc would run it as a doctest)")

    paras = [" ".join(p.split()) for p in doc.strip().split("\n\n") if p.strip()]
    if not paras:
        return ""
    out = []
    for k, para in enumerate(paras):
        if k:
            out.append("//!")
        out += textwrap.wrap(para, width=100, initial_indent="//! ", subsequent_indent="//! ",
                             break_long_words=False, break_on_hyphens=False)
    return "\n".join(out) + "\n\n"


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


def code_lines(text: str) -> int:
    return sum(1 for ln in mask(text).split("\n") if ln.strip())


def strip_super(sel: str) -> str:
    return re.sub(r"\bsuper::", "", collapse(sel))


def impl_block(lines: list[str], it: Item, lay: ImplLayout, mids: list[int], lead_lines: list[str],
               child: bool, trailing: bool, parent_path: list[str], deeper: bool, counts: dict) -> str:
    """An impl block holding the members `mids` (indices into lay.members), in order."""
    header = "\n".join(lines[it.start:lay.body_open + 1])
    lead = "\n".join(lead_lines)
    if child and deeper and INCLUDE.search(mask(lead + "\n" + header)):
        raise SystemExit(f"{it.selector}: an include in a split impl's header or attributes would resolve "
                         "from a different directory. Edit by hand.")
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
    with open(path, encoding="utf-8", newline="") as fh:
        src = fh.read()
    if "\r" in src:
        raise SystemExit(f"{rel} contains a carriage return; rustc treats a lone CR differently from a newline. "
                         "Normalize line endings first.")
    items, header_end = parse_items(src)
    lines = src.split("\n")
    mlines = mask(src).split("\n")
    parent_path, beside = file_layout(rel)
    if beside and re.search(r"/src/bin/[^/]+\.rs$", rel.replace(os.sep, "/")):
        raise SystemExit(f"{rel}: children of a src/bin/*.rs crate root would sit in src/bin/, where Cargo builds "
                         "every file as its own binary. Move it to src/bin/<name>/main.rs first.")
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
        return re.search(r"(?<!\w)" + re.escape(name) + r"\s*!", text) is not None

    if (assign or massign) and any(re.match(r"#\[\s*macro_use\b", a) for it in items
                                   for a in attrs_of(lines, it.lead, it.start)):
        raise SystemExit("the parent has a #[macro_use] item; the new mod lines would change which macros moved "
                         "code sees by textual order. Edit by hand.")
    for name in {n for n in macro_names if macro_names.count(n) > 1}:
        if invokes(moved_text, name):
            raise SystemExit(f"macro_rules! {name} is defined more than once in the parent and moved code "
                             "invokes it; which definition it binds to would change. Edit by hand.")
    bodies = {it.name: "\n".join(mlines[it.start:it.end + 1]) for it in macro_defs}
    # A macro whose expansion names `super`/`self` (a path or a visibility) resolves from the
    # module it is expanded in, so its meaning changes one module deeper.
    relative = {n for n, b in bodies.items()
                if REL_VIS.search(b) or super_path_positions(b) or re.search(r"\bself\s*::", b)}
    while True:
        more = {n for n, b in bodies.items() if n not in relative and any(invokes(b, r) for r in relative)}
        if not more:
            break
        relative |= more
    # Macros defined anywhere in the crate expand in the moved code's module too. A macro is
    # relative if its body names a super/self path or a relative visibility, or invokes a
    # relative macro (transitively); a `use x as y` of one, anywhere in the crate, is one too.
    crate_src = os.path.join(repo, rel.replace(os.sep, "/").split("/src/", 1)[0], "src")
    all_bodies: dict[str, str] = dict(bodies)
    aliases: list[tuple[str, str]] = []
    for d, _, files in os.walk(crate_src):
        for fn in files:
            if not fn.endswith(".rs"):
                continue
            fp = os.path.join(d, fn)
            other = mask(open(fp, encoding="utf-8", errors="replace").read())
            for m in re.finditer(r"\bmacro_rules!\s*(?:r#)?(\w+)", other):
                g = macro_group(other, m.end())
                all_bodies[m.group(1)] = all_bodies.get(m.group(1), "") + "\n" + (other[g[0]:g[1]] if g else "")
            for m in re.finditer(r"\buse\b[^;]*;", other):
                aliases += re.findall(r"\b(\w+)\s+as\s+(\w+)", m.group(0))
    CRATE_MACROS.clear()
    CRATE_MACROS.update(all_bodies)
    relative |= {n for n, b in all_bodies.items()
                 if REL_VIS.search(b) or super_path_positions(b) or re.search(r"\bself\s*::", b)}
    while True:
        more = {n for n, b in all_bodies.items() if n not in relative and any(invokes(b, r) for r in relative)}
        more |= {alias for src_name, alias in aliases if src_name in relative and alias not in relative}
        if not more:
            break
        relative |= more
    for name in sorted(relative):
        if invokes(moved_text, name):
            raise SystemExit(f"moved code invokes macro_rules! {name}, whose expansion names `super`/`self` "
                             "(a path or pub(super)/pub(self)), directly or through another macro; it would "
                             "resolve differently one module deeper. Keep its callers in the parent or edit by hand.")
    # Children are declared after every parent macro. A moved item that invokes a macro the
    # parent defines only later bound to an outer macro of that name; in the child it would
    # bind to the parent's definition instead.
    first_def = {}
    for it in macro_defs:
        first_def.setdefault(it.name, it.start)
    moved_units = [(items[k].start, "\n".join(mlines[items[k].start:items[k].end + 1])) for k in assign]
    moved_units += [(layout(k).members[mi].start,
                     "\n".join(mlines[layout(k).members[mi].start:layout(k).members[mi].end + 1]))
                    for k, mi in massign]
    def reaches(text: str) -> set[str]:
        """Parent macros a moved unit invokes, directly or through other parent macros."""
        seen: set[str] = set()
        todo = [n for n in first_def if invokes(text, n)]
        while todo:
            n = todo.pop()
            if n in seen:
                continue
            seen.add(n)
            todo += [m for m in first_def if m not in seen and invokes(bodies.get(n, ""), m)]
        return seen

    dup = {n for n in macro_names if macro_names.count(n) > 1}
    for start, text in moved_units:
        used = reaches(text)
        for name in sorted(used & dup):
            raise SystemExit(f"moved code at line {start + 1} reaches macro_rules! {name}, which the parent defines "
                             "more than once; which definition it binds to would change. Edit by hand.")
        for name, line in first_def.items():
            if line > start and name in used:
                raise SystemExit(f"moved code at line {start + 1} invokes {name}! before the parent defines it "
                                 f"(line {line + 1}); its binding would change. Edit by hand.")
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
    masked_src = mask(src)
    for n in names:
        if n in imported:
            raise SystemExit(f"module name {n!r} collides with a name the parent imports")
        siblings = ""
        if os.path.isdir(child_dir):
            for d, _, files in os.walk(child_dir):
                for fn in sorted(files):
                    if fn.endswith(".rs"):
                        siblings += mask(open(os.path.join(d, fn), encoding="utf-8", errors="replace").read())
        if re.search(r"(?<![\w:])" + re.escape(n) + r"\s*::", masked_src + "\n" + siblings):
            raise SystemExit(f"module name {n!r} is already used as a path in the parent (a crate or a glob-imported "
                             "module?); a new module of that name would shadow it")

    # Where a split impl's doc comments go: with the members the parent keeps, else with the
    # module that receives the impl's first (lowest-indexed) moved member.
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
                    # What a macro invocation emits is unknown: its items keep the visibility the
                    # macro gives them, which the tool cannot remap. Moving one needs an explicit
                    # opt-in; re-export at most `pub`, which a glob clamps, so nothing widens.
                    if not mod.get("allow_macro_items", False):
                        raise SystemExit(f"{it.selector}: a top-level macro invocation emits items whose visibility "
                                         "the tool cannot remap; set allow_macro_items = true on the module if "
                                         "every item it emits is pub or used only inside the module")
                    counts["macro_items"] = counts.get("macro_items", 0) + 1
                    reach.add("pub")
                elif it.kind != "impl" and not (it.kind == "const" and it.name == "_"):
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
                if any(lay.members[mi].kind == "macro" for mi in mids) and not mod.get("allow_macro_items", False):
                    raise SystemExit(f"{it.selector}: a macro invocation inside the impl emits members whose visibility "
                                     "the tool cannot remap; set allow_macro_items = true on the module to move it")
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
        body = doc_comment(mod.get("doc", ""))
        allow_unused = mod.get("allow_unused_glob", False)
        if "glob_import" in mod or not isinstance(allow_unused, bool):
            raise SystemExit(f"module {name}: the parent glob import is always kept; use "
                             "allow_unused_glob = true for a module that needs nothing from its parent")
        glob = "#[allow(unused_imports)]\nuse super::*;\n\n" if allow_unused else "use super::*;\n\n"
        body += glob + "\n\n".join(chunks) + "\n"
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
        allow = mod.get("allow_unused_reexport", False)
        if not isinstance(allow, bool):
            raise SystemExit(f"module {mod['name']}: allow_unused_reexport must be true or false")
        if allow:
            gate += "#[allow(unused_imports)]\n"
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
    first_def = next((k for k, it in enumerate(items) if it.kind in DEFS),
                     next((k for k, it in enumerate(items) if it.kind in ("fn", "mod")), len(items)))
    decl_after = -1
    for idx, k in enumerate(keep):
        if k < first_def and items[k].kind in ("use", "extern crate", "mod-decl"):
            decl_after = idx
        if items[k].kind == "macro_rules!" or any(re.match(r"#\[\s*macro_use\b", a) for a in attrs_of(
                lines, items[k].lead, items[k].start)):
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
    """A file's bytes at a revision, decoded without newline translation (a lone CR stays a CR)."""
    r = subprocess.run(["git", "-C", repo, "show", f"{rev}:{path}"], capture_output=True)
    return r.stdout.decode("utf-8") if r.returncode == 0 else None


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
    base_src = git_show(repo, base, parent) or ""
    base_items = parse_items(base_src)[0]
    base_lines, base_mlines = base_src.split("\n"), mask(base_src).split("\n")
    head_parent = git_show(repo, head, parent) or ""
    hp_items = parse_items(head_parent)[0]
    order = [it.name for it in hp_items if it.kind == "mod-decl"]
    names = sorted(children, key=lambda n: order.index(n) if n in order else len(order))
    out = [f"file = {json.dumps(parent, ensure_ascii=False)}"]
    for name in names:
        child = os.path.join(os.path.dirname(parent), os.path.basename(parent)[:-3], f"{name}.rs") \
            if not file_layout(parent)[1] else os.path.join(os.path.dirname(parent), f"{name}.rs")
        src = git_show(repo, head, child) or ""
        items, hdr = parse_items(src)
        lines, mlines = src.split("\n"), mask(src).split("\n")
        doc_lines = [re.sub(r"^//! ?", "", ln.strip()) for ln in lines[:hdr] if ln.strip().startswith("//!")]
        doc = "\n".join(doc_lines).replace("\n\n", "\0").replace("\n", " ").replace("\0", "\n\n")
        sels: list[str] = []
        for it in items:
            if it.kind == "use" and collapse("\n".join(lines[it.start:it.end + 1])) == "use super::*;":
                continue
            if it.kind != "impl":
                sels.append(escape_glob(it.selector))
                continue
            base = [b for b in base_items if b.kind == "impl" and header_key(b.selector) == header_key(it.selector)]
            base_sel = base[0].selector if base else it.selector
            lay = impl_layout(lines, mlines, it)
            if not (lay and lay.members):
                sels.append(escape_glob(base_sel))
                continue
            moved = [m.selector for m in lay.members]
            whole = any(sorted(m.selector for m in (impl_layout(base_lines, base_mlines, b) or ImplLayout(0, [], 0)).members)
                        == sorted(moved) for b in base)
            if whole and len(base) == 1:
                sels.append(escape_glob(base_sel))
            else:
                sels += [f"{escape_glob(base_sel)} :: {escape_glob(m)}" for m in moved]
        glob_lead = next(("\n".join(lines[it.lead:it.start]) for it in items if it.kind == "use"
                          and collapse("\n".join(lines[it.start:it.end + 1])) == "use super::*;"), "")
        out.append(f"\n[[module]]\nname = {json.dumps(name)}")
        if doc:
            out.append(f"doc = {json.dumps(doc, ensure_ascii=False)}")
        if "allow(unused_imports)" in glob_lead:
            out.append("allow_unused_glob = true")
        for it in hp_items:
            if it.kind == "use" and re.search(r"\buse\s+" + re.escape(name) + r"::\*;", it.name) and \
                    "allow(unused_imports)" in "\n".join(head_parent.split("\n")[it.lead:it.start]):
                out.append("allow_unused_reexport = true")
        if any(it.kind == "macro" for it in items):
            out.append("allow_macro_items = true")
        out.append("items = [\n" + "".join(f"  {json.dumps(x, ensure_ascii=False)},\n" for x in dict.fromkeys(sels)) + "]")
    return "\n".join(out) + "\n"


def verify(repo: str, base: str, head: str, print_spec: bool = False, allow_non_rust: bool = False) -> int:
    """Prove BASE...HEAD's Rust changes are exactly what `apply` generates: derive the spec from
    the diff, regenerate from the merge base, and compare byte for byte after rustfmt."""
    import contextlib
    import difflib
    import io
    import shutil
    import tempfile

    repo = git(repo, "rev-parse", "--show-toplevel").strip()
    merge_base = git(repo, "merge-base", base, head).strip()
    raw = git(repo, "diff", "--raw", "-z", "--no-renames", "--no-abbrev", f"{merge_base}..{head}", "--", "*.rs")
    other = [p for p in git(repo, "diff", "--name-only", "--no-renames", f"{merge_base}..{head}").splitlines()
             if not p.endswith(".rs")]
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
    def children_dir(parent: str) -> str:
        return os.path.dirname(parent) if file_layout(parent)[1] else parent[:-3]

    groups: dict[str, list[str]] = {}
    for p in added:
        owners = [m for m in sorted(modified) if "/src/" in m and children_dir(m) == os.path.dirname(p)]
        if len(owners) != 1:
            problems.append(f"{p}: new file whose parent module is not part of the change" if not owners
                            else f"{p}: more than one changed file could own it: {owners}")
            continue
        groups.setdefault(owners[0], []).append(os.path.splitext(os.path.basename(p))[0])
    for p in sorted(modified - set(groups)):
        problems.append(f"{p}: changed, but no child module was split out of it")
    for p in sorted(set(added) | modified):
        if "\r" in (git_show(repo, head, p) or ""):
            problems.append(f"{p}: contains a carriage return")
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
                # Format only the regenerated side: head must be exactly `apply` + rustfmt, so an
                # edit rustfmt would erase (a trailing comma inside a macro call) still differs.
                want = rustfmt(repo, open(os.path.join(tmp, rel), encoding="utf-8").read(), edition)
                got = git_show(repo, head, rel) or ""
                checked += 1
                if want != got:
                    diff = list(difflib.unified_diff(want.split("\n"), got.split("\n"), "regenerated/" + rel,
                                                     "head/" + rel, lineterm="", n=2))
                    problems.append(f"{rel}: differs from what apply generates\n      " + "\n      ".join(diff[:40]))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    if not groups and not problems:
        problems.append("no split found: no new child module next to a changed parent")
    crates = {g.split("/src/", 1)[0] + "/" for g in groups}
    inside = [p for p in other if any(p.startswith(c) for c in crates)]
    if inside and not allow_non_rust:
        problems.append("non-Rust files changed inside the split crate (manifest, data, build inputs); "
                        "a pure move changes none. Pass --allow-non-rust if they are intended: " + ", ".join(inside))
    print(f"regenerated and compared {checked} files from {len(groups)} split parents: "
          f"{'identical' if not problems else str(len(problems)) + ' problems'}")
    for p in problems:
        print("  " + p)
    if other:
        print("not checked by verify (not Rust): " + ", ".join(other))
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
    c.add_argument("--allow-non-rust", action="store_true",
                   help="accept non-Rust changes inside the split crate (they are listed, not checked)")
    args = ap.parse_args()
    if args.cmd == "list":
        return list_items(args.file)
    if args.cmd == "verify":
        return verify(args.repo, args.base, args.head, args.print_spec, args.allow_non_rust)
    return apply(args.spec, args.repo, args.dry_run, args.report)


if __name__ == "__main__":
    sys.exit(main())
