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
* Relative `include_str!`/`include_bytes!` paths gain `../` when the children
  live one directory deeper than the parent file (parent is `foo.rs`).

Each new module starts with `use super::*;`, so the parent's imports, traits and
sibling items resolve in moved code as before.

The spec is item names, not line numbers, so it can be re-run on a newer main
instead of rebasing a split by hand.

Usage:
  split_rust_module.py list FILE                   # items and selectors, for writing a spec
  split_rust_module.py apply SPEC.toml [--repo ROOT] [--report OUT.md] [--dry-run]
  split_rust_module.py verify [BASE] [--head REV]   # prove BASE...HEAD is such a move

Spec (TOML):
  file = "crates/libs/x/src/foo.rs"
  [[module]]
  name = "bar"
  doc = "What bar holds."            # one `//!` line at the top of the new file
  items = ["fn parse_*", "struct Config", "impl Config", "impl Display for Config"]
  # optional: reexport = "auto" | "none" | "test"   (default auto)

Selectors are fnmatch patterns over `<kind> <name>`; impl blocks are
`impl <header>` with whitespace collapsed (e.g. `impl<T> From<T> for Foo`).
`impl <header> :: <kind> <name>` selects members of an inherent impl: they move into
an impl block with the same attributes and header in the child, and the parent's
block keeps the rest (or disappears when none are left). Trait impls move whole.
Every pattern must match at least one item and every item at most one module.

Refused, to be done by hand: moving `use`, `mod x;` or `macro_rules!` items (they
stay in the parent, above the new `mod` lines, so children see the macros), relative
`pub(in super::..)` visibility, `#[path]` inside moved items, and a parent file that
is itself loaded through `#[path]`.
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
QUAL = r"(?:(?:const|async|unsafe|default|extern(?:\s+\"[^\"]*\")?)\s+)*"
ITEM_RE = re.compile(
    r"^(?P<vis>" + VIS + r")" + QUAL +
    r"(?P<kind>fn|struct|enum|union|trait|impl|type|const|static|mod|use|extern\s+crate|auto\s+trait)\b|(?P<mr>macro_rules!)"
)
NAME_RE = re.compile(r"\s*(?:r#)?([A-Za-z_][A-Za-z0-9_]*)")
MACRO_CALL_RE = re.compile(r"^(?P<path>[A-Za-z_][A-Za-z0-9_:]*)\s*!")
UNMOVABLE = {"use", "extern crate"}


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
        if s.startswith("#["):
            if lead is None:
                lead = i
            d = line_depth_delta(m)
            i += 1
            while d > 0 and i < n:
                d += line_depth_delta(mlines[i])
                i += 1
            continue
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
        it.cfg = [collapse(x) for x in attrs_of(lines, it.lead, start) if collapse(x).startswith("#[cfg(")]
        items.append(it)
        lead = None
    return items, header_end


def attrs_of(lines: list[str], lead: int, start: int) -> list[str]:
    out, cur = [], None
    for k in range(lead, start):
        s = lines[k].strip()
        if s.startswith("#["):
            cur = s
            if cur.count("[") <= cur.count("]"):
                out.append(cur)
                cur = None
        elif cur is not None:
            cur += " " + s
            if cur.count("[") <= cur.count("]"):
                out.append(cur)
                cur = None
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
            cut = mtext.find("{")
            header = collapse("\n".join(lines[start:end + 1])[: cut if cut >= 0 else None])
            name = re.sub(r"^(?:unsafe\s+)?impl\s*", "", header).strip()
            return Item("impl", name, start, end, start, vis)
        if kind in ("use", "extern crate"):
            return Item(kind, collapse(" ".join(lines[start:end + 1]))[:120], start, end, start, vis)
        nm = NAME_RE.match(rest)
        name = nm.group(1) if nm else "?"
        if kind == "mod":
            kind = "mod" if text_m.rstrip().endswith("}") else "mod-decl"
        return Item(kind, name, start, end, start, vis)
    mc = MACRO_CALL_RE.match(first.strip())
    if mc:
        return Item("macro", mc.group("path"), start, end, start, "")
    raise SystemExit(f"line {start + 1}: cannot classify top-level item: {lines[start][:80]!r}")


# ----------------------------------------------------------- visibility map

def module_path_of(rel_file: str) -> list[str]:
    """Module path segments of a file inside a crate's src/ (crate root -> [])."""
    p = rel_file.replace(os.sep, "/")
    if "/src/" not in p:
        raise SystemExit(f"{rel_file}: expected a path under <crate>/src/")
    tail = p.split("/src/", 1)[1]
    parts = tail[:-3].split("/") if tail.endswith(".rs") else tail.split("/")
    if parts[-1] in ("lib", "main", "mod"):
        parts = parts[:-1]
    if parts and parts[0] == "bin":  # src/bin/x.rs is its own crate root
        parts = parts[2:]
    return parts


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


def find_body_open(mt: str, p: int) -> int:
    """From after the item name, skip generics and where clauses; return offset of the body
    delimiter `{` or `(`, or -1 for a unit item ending in `;`."""
    n = len(mt)
    if p < n and mt[p] == "<":
        d = 0
        while p < n:
            ch = mt[p]
            if ch == "<":
                d += 1
            elif ch == ">" and mt[p - 1] != "-":
                d -= 1
                if d == 0:
                    p += 1
                    break
            p += 1
    while p < n and mt[p].isspace():
        p += 1
    if p < n and mt[p] == "(":
        return p
    depth = 0
    while p < n:
        ch = mt[p]
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif ch == "{" and depth == 0:
            return p
        elif ch == ";" and depth == 0:
            return -1
        p += 1
    return -1


def split_top(mt: str, open_pos: int) -> list[tuple[int, int]]:
    """Segments [a, b) separated by depth-1 commas inside the delimiter at open_pos.

    Angle brackets count as nesting (`HashMap<K, V>`), except the `>` of `->` and `=>`.
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
        elif ch == "<" and depth >= 1:
            angle += 1
        elif ch == ">" and depth >= 1 and mt[p - 1] not in "-=" and angle > 0:
            angle -= 1
        elif ch == "," and depth == 1 and angle == 0:
            segs.append((a, p))
            a = p + 1
    return segs


MEMBER_RE = re.compile(r"(" + VIS + r")" + QUAL + r"(fn|const|type)\b")


def transform_item(text: str, kind: str, parent_path: list[str], deeper_dir: bool) -> tuple[str, dict]:
    """Apply the visibility/path edits to one moved item's text (lead trivia included)."""
    counts: dict = {}
    mt = mask(text)
    edits: list[tuple[int, int, str]] = []  # (pos, len_old, new) applied right-to-left

    def vis_edit(pos: int) -> None:
        m = VIS_AT.match(text, pos)
        old = m.group(0) if m else ""
        new = map_vis(old, parent_path)
        if new is not None:
            edits.append((pos, len(old), new))
            counts["vis"] = counts.get("vis", 0) + 1

    # 1. the item's own visibility: first code offset after attributes
    p0 = skip_ws_attrs(mt, 0)
    if kind not in ("impl", "macro"):
        vis_edit(p0)
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
        header_end = mt.find("{", p0)
        header = re.sub(r"\bfor\s*<[^<>]*>", "", mt[p0:header_end])
        header = re.sub(r"<[^<>]*>", "", header)
        while re.search(r"<[^<>]*>", header):
            header = re.sub(r"<[^<>]*>", "", header)
        inherent = not re.search(r"\bfor\s+(?!<)", header)
        if inherent:
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
    # 3. super:: paths (code only)
    for m in re.finditer(r"(?<![\w:])super(?=\s*::)", mt):
        edits.append((m.start(), 0, "super::"))
        counts["super"] = counts.get("super", 0) + 1
    if re.search(r"pub\s*\(\s*in\s+super", mt) or re.search(r"\bpub\s*\(\s*in\s+self", mt):
        raise SystemExit("relative pub(in super/self) inside a moved item; edit by hand")
    if re.search(r"(?m)^\s*(?:pub\S*\s+)?mod\s+\w+\s*\{", mt) and kind != "mod":
        counts["inline_mod_warning"] = 1
    # 4. include paths
    if deeper_dir:
        for m in re.finditer(r"include_(?:str|bytes)!\s*\(\s*\"(?!/)", text):
            if mt[m.start():m.start() + 8] == "include_":
                edits.append((m.end(), 0, "../"))
                counts["include"] = counts.get("include", 0) + 1
    if re.search(r"#\s*\[\s*path\s*=", text):
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
    body_open, header = None, ""
    for i in range(it.start, it.end + 1):
        pos = mlines[i].find("{")
        if pos >= 0:
            header += mlines[i][:pos]
            body_open = i
            break
        header += mlines[i] + "\n"
    if body_open is None or not is_inherent_header(header):
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
    out = list(lead_lines)
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
    parent_path = module_path_of(rel)
    base = os.path.basename(rel)
    deeper = base not in ("lib.rs", "main.rs", "mod.rs")
    child_dir = os.path.join(os.path.dirname(path), base[:-3]) if deeper else os.path.dirname(path)
    loaded_by_path = path_attribute_users(path)
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
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
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
    split_impls = sorted({k for k, _ in massign})
    for k in split_impls:
        if k in assign:
            raise SystemExit(f"{items[k].selector}: selected whole and by member")
    for it in items:
        if it.kind in TYPE_NS and it.name in names:
            raise SystemExit(f"module name {it.name!r} collides with {it.selector}")

    # Where a split impl's doc comments go: with the members the parent keeps, else with the
    # first module (in spec order) that receives members.
    lead_owner: dict[int, str | None] = {}
    for k in split_impls:
        lay = layout(k)
        kept = [mi for mi in range(len(lay.members)) if (k, mi) not in massign]
        lead_owner[k] = None if kept else next(n for n in names if any(massign.get((k, mi)) == n
                                                                         for mi in range(len(lay.members))))

    results = {}
    for mod in mods:
        name = mod["name"]
        chunks, counts, obs, best, sels = [], {}, {k: 0 for k in OBS}, (RANK["none"], ""), []
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
                    counts["macro_items"] = counts.get("macro_items", 0) + 1
                    best = max(best, (RANK["private"], ""))
                elif it.kind != "impl":
                    best = max(best, new_vis_rank(it.vis, parent_path))
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
        body += "use super::*;\n\n" + "\n\n".join(chunks) + "\n"
        ks = [k for k in range(len(items)) if assign.get(k) == name or
              (k in split_impls and any(massign.get((k, mi)) == name for mi in range(len(layout(k).members))))]
        cfgs = [set(items[k].cfg) for k in ks]
        common_cfg = sorted(set.intersection(*cfgs)) if cfgs else []
        reexport = mod.get("reexport", "auto")
        if best[0] == RANK["none"]:
            reexport = "none"  # only impl blocks: nothing to name
        use_vis = {RANK["pub"]: "pub ", RANK["crate"]: "pub(crate) ", RANK["in"]: best[1] + " "}.get(best[0], "")
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
        gate = "".join(c + "\n" for c in r["cfg"]) + ("#[cfg(test)]\n" if r["reexport"] == "test" else "")
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


def path_attribute_users(path: str) -> str:
    """A `#[path = "..."]` in the crate that names this file, if any."""
    crate_src = path.split(os.sep + "src" + os.sep, 1)[0] + os.sep + "src"
    target = os.path.realpath(path)
    for d, _, files in os.walk(crate_src):
        for f in files:
            if not f.endswith(".rs"):
                continue
            fp = os.path.join(d, f)
            for m in re.finditer(r'#\s*\[\s*path\s*=\s*"([^"]+)"\s*\]', open(fp, encoding="utf-8").read()):
                if os.path.realpath(os.path.join(d, m.group(1))) == target:
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

def normalize(text: str) -> str:
    """Whitespace- and trailing-comma-insensitive form of code, for comparing a move across a
    rustfmt run. String and char literals are kept byte for byte."""
    m = mask(text, comment="c")
    out: list[str] = []
    for ch, mk in zip(text, m):
        if mk == "x":
            out.append(ch)
        elif ch.isspace():
            continue
        else:
            if mk != "c" and ch in CLOSE and out and out[-1] == ",":
                out.pop()
            out.append(ch)
    return "".join(out)


def git(repo: str, *args: str) -> str:
    return subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True, check=True).stdout


def git_show(repo: str, rev: str, path: str) -> str | None:
    r = subprocess.run(["git", "-C", repo, "show", f"{rev}:{path}"], capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


@dataclass
class Record:
    """One unit a move must conserve: a whole item, one member of an inherent impl (keyed by
    the impl's attributes and header), or the non-attribute trivia of an inherent impl."""
    kind: str  # "item" | "member" | "impl-trivia"
    item_kind: str
    selector: str
    key: str  # normalized impl attrs + header for members, else ""
    text: str
    line: int


def records(src: str) -> list[Record]:
    items, _ = parse_items(src)
    lines = src.split("\n")
    mlines = mask(src).split("\n")
    out: list[Record] = []
    for it in items:
        lay = impl_layout(lines, mlines, it) if it.kind == "impl" else None
        if lay is None or not lay.members:
            text = "\n".join(lines[it.lead:it.end + 1]).lstrip("\n")
            out.append(Record("item", it.kind, it.selector, "", text, it.start))
            continue
        attrs = attr_lines(lines, it.lead, it.start)
        header = "\n".join(attrs + lines[it.start:lay.body_open + 1])
        trivia = [ln for ln in lines[it.lead:it.start] if ln.strip() and ln not in attrs]
        trivia += lines[lay.members[-1].end + 1:lay.close]
        if any(t.strip() for t in trivia):
            out.append(Record("impl-trivia", "impl", it.selector, "", "\n".join(trivia), it.start))
        for m in lay.members:
            out.append(Record("member", m.kind, f"{it.selector} :: {m.selector}", header,
                              "\n".join(lines[m.lead:m.end + 1]), m.start))
    return out


def parent_candidates(child: str) -> list[str]:
    d = os.path.dirname(child)
    out = [d + ".rs", os.path.join(d, "mod.rs")]
    if os.path.basename(d) == "src":
        out = [os.path.join(d, "lib.rs"), os.path.join(d, "main.rs")]
    return out


def verify(repo: str, base: str, head: str) -> int:
    """Prove BASE...HEAD only moves items (or inherent-impl members) into child modules the
    way `apply` does, plus the `mod`/`use` declarations `apply` writes."""
    merge_base = git(repo, "merge-base", base, head).strip()
    status = [ln.split("\t") for ln in git(repo, "diff", "--name-status", "--no-renames",
                                              f"{merge_base}..{head}", "--", "*.rs").splitlines()]
    added = {p for st, p in status if st == "A"}
    modified = {p for st, p in status if st == "M"}
    deleted = {p for st, p in status if st == "D"}
    pool = {p: records(git_show(repo, merge_base, p) or "") for p in modified | deleted}
    new_mods = {os.path.splitext(os.path.basename(p))[0] for p in added}
    residual_added: list[str] = []
    moved = 0

    def take(p: str, rec: Record, want: str, key: str) -> bool:
        for idx, b in enumerate(pool.get(p, [])):
            if b.kind == rec.kind and normalize(b.key) == key and normalize(b.text) == want:
                pool[p].pop(idx)
                return True
        return False

    trivia_head: list[tuple[str, Record]] = []
    for p in sorted(modified):
        for rec in records(git_show(repo, head, p) or ""):
            if rec.kind == "impl-trivia":
                trivia_head.append((p, rec))
                continue
            if take(p, rec, normalize(rec.text), normalize(rec.key)):
                continue
            if rec.item_kind == "mod-decl" and rec.selector.split(" ", 1)[1] in new_mods:
                continue
            if rec.item_kind == "use" and new_mods and re.search(
                    r"\buse\s+(" + "|".join(map(re.escape, new_mods)) + r")::\*;", rec.text):
                continue
            residual_added.append(f"{p}: {rec.selector} (line {rec.line + 1})")
    for p in sorted(added):
        parent = next((c for c in parent_candidates(p) if c in pool), None)
        for rec in records(git_show(repo, head, p) or ""):
            if rec.kind == "item" and rec.item_kind == "use" and collapse(rec.text) == "use super::*;":
                continue
            if rec.kind == "impl-trivia":
                trivia_head.append((p, rec))
                continue
            if parent is None:
                residual_added.append(f"{p}: {rec.selector} (new file with no changed parent module)")
                continue
            ppath = module_path_of(parent)
            deeper = os.path.basename(parent) not in ("lib.rs", "main.rs", "mod.rs")
            want, key = normalize(rec.text), normalize(rec.key)
            hit = False
            for idx, b in enumerate(pool[parent]):
                if b.kind != rec.kind or b.item_kind != rec.item_kind:
                    continue
                if b.kind == "member" and normalize(rewrite_super(b.key)) != key:
                    continue
                try:
                    tkind = b.item_kind if b.kind == "item" else ("macro" if b.item_kind == "macro" else "fn")
                    exp, _ = transform_item(b.text, tkind, ppath, deeper)
                except SystemExit:
                    continue
                if normalize(exp) == want:
                    pool[parent].pop(idx)
                    hit = True
                    moved += 1
                    break
            if not hit:
                residual_added.append(f"{p}: {rec.selector} (line {rec.line + 1})")
    for p, rec in trivia_head:  # an impl's doc comments may land with any of its blocks
        src_p = next((c for c in [p] + parent_candidates(p) if c in pool), None)
        cands = [c for c in ([src_p] if src_p else []) + list(pool) if c]
        if not any(take(c, rec, normalize(rec.text), "") for c in dict.fromkeys(cands)):
            residual_added.append(f"{p}: doc/comments of {rec.selector} (line {rec.line + 1})")
    residual_removed = [f"{p}: {b.selector} (base line {b.line + 1})" for p, v in pool.items() for b in v]
    print(f"moved: {moved}   unmatched at base: {len(residual_removed)}   unmatched at head: {len(residual_added)}")
    for title, rows in (("Base items not found at head (removed or edited):", residual_removed),
                        ("Head items that are not a move (added or edited):", residual_added)):
        if rows:
            print("\n" + title)
            for r in rows:
                print("  " + r)
    return 1 if residual_removed or residual_added else 0


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
    for it in items:
        text = "\n".join(lines[it.lead:it.end + 1])
        cfg = (" " + " ".join(it.cfg)) if it.cfg else ""
        print(f"{it.start + 1:6}-{it.end + 1:<6} {code_lines(text):5}  {it.selector}{cfg}")
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
    args = ap.parse_args()
    if args.cmd == "list":
        return list_items(args.file)
    if args.cmd == "verify":
        return verify(args.repo, args.base, args.head)
    return apply(args.spec, args.repo, args.dry_run, args.report)


if __name__ == "__main__":
    sys.exit(main())
