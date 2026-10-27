#!/usr/bin/env python3
"""Render the Artifacts table of README.md and docs/site/index.md from the module POMs and matrix.json.

Without arguments the files are rewritten; with --check nothing is written and the exit code is 1 when a file differs.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FILES = ("README.md", "docs/site/index.md")
START, END = "<!-- artifacts:start -->", "<!-- artifacts:end -->"
HEADER = ["| Artifact | Spark | Scala | Java bytecode | JDKs tested | Delta tested |", "|---|---|---|---|---|---|"]
PROPS = {"spark": "spark.version", "minor": "spark.minor", "delta": "delta.version",
         "scala": "scala.binary.version", "java": "java.release", "scala_version": "scala.version",
         "hadoop": "hadoop.aws.version"}


def vkey(v):
    return [int(p) for p in re.findall(r"\d+", v)]


def read_modules(root):
    """Modules in the order of the parent POM, each with the properties its pom.xml pins.

    delta_artifact is the module's delta.artifact, or the parent's default when the module sets none."""
    parent = (root / "pom.xml").read_text()
    default = re.search(r"<delta\.artifact>([^<]+)</delta\.artifact>", parent).group(1)
    out = []
    for path in re.findall(r"<module>([^<]+)</module>", parent):
        pom = (root / path / "pom.xml").read_text()
        mod = {}
        for k, p in PROPS.items():
            found = re.search(rf"<{re.escape(p)}>([^<]+)</{re.escape(p)}>", pom)
            if not found:
                raise ValueError(f"{root / path / 'pom.xml'}: missing <{p}>")
            mod[k] = found.group(1)
        art = re.search(r"<delta\.artifact>([^<]+)</delta\.artifact>", pom)
        mod["delta_artifact"] = (art.group(1) if art else default).replace("${scala.binary.version}", mod["scala"])
        out.append(mod)
    return out


def read_rows(root):
    return json.loads((root / "matrix.json").read_text())


def join_and(items):
    return items[0] if len(items) == 1 else ", ".join(items[:-1]) + " and " + items[-1]


def spark_cell(m, rows):
    lowest = min((r["spark"] for r in rows), key=vkey)
    deltas_top = {r["delta"] for r in rows if r["spark"] == m["spark"]}
    # A lower Spark patch whose Delta versions the top patch also runs is a floor ("and later").
    if vkey(lowest) < vkey(m["spark"]) and {r["delta"] for r in rows if r["spark"] == lowest} <= deltas_top:
        return f"{lowest} and later {m['minor']}"
    return f"{m['minor']}.x"


def delta_cell(m, rows):
    on_top = {r["delta"] for r in rows if r["spark"] == m["spark"]}
    pinned = {}  # Delta version -> the lower Spark patch that alone runs it
    for r in rows:
        if r["delta"] not in on_top:
            pinned[r["delta"]] = r["spark"]
    out, done = [], set()
    for d in sorted({r["delta"] for r in rows}, key=vkey):
        if d in pinned:
            spark = pinned[d]
            if spark not in done:
                done.add(spark)
                group = [x for x in sorted(pinned, key=vkey) if pinned[x] == spark]
                out.append(f"{join_and(group)} (on Spark {spark})")
        else:
            out.append(d)
    return ", ".join(out)


def table(modules, rows):
    lines = list(HEADER)
    for m in modules:
        name = f"spark-{m['minor']}_{m['scala']}"
        mine = [r for r in rows if r["module"] == name]
        jdks = ", ".join(sorted({r["jdk"] for r in mine}, key=vkey))
        lines.append(f"| `gluecat-{name}` | {spark_cell(m, mine)} | {m['scala']} | {m['java']} | {jdks} | "
                     f"{delta_cell(m, mine)} |")
    return "\n".join(lines)


def replace_block(text, rendered):
    i, j = text.find(START), text.find(END)
    if i < 0 or j < i:
        raise ValueError("artifacts:start / artifacts:end markers not found")
    return text[:i + len(START)] + "\n" + rendered + "\n" + text[j:]


def main(argv=None):
    check = "--check" in (sys.argv[1:] if argv is None else argv)
    rendered = table(read_modules(ROOT), read_rows(ROOT))
    stale = []
    for f in FILES:
        path = ROOT / f
        old = path.read_text()
        new = replace_block(old, rendered)
        if new != old:
            stale.append(f)
            if not check:
                path.write_text(new)
    if check and stale:
        print(f"stale: {', '.join(stale)}; run python scripts/render_tables.py", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
