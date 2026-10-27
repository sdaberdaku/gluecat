#!/usr/bin/env python3
"""Render the generated blocks of the documentation from the module POMs, matrix.json and CHANGELOG.md.

The Artifacts table (README.md, docs/site/index.md) and the Spark 4.1 example command (README.md,
docs/site/quickstart.md) sit between marker comments. The gluecat version in the example is the newest released
heading of CHANGELOG.md: the parent POM carries the next SNAPSHOT version, and a release moves the heading, not
the other way round.

The module/row invariant is checked first: every module has at least one row on its own spark.version and
delta.version, and every row names an existing module.

Without arguments the files are rewritten; with --check nothing is written and the exit code is 1 when a file differs.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TABLE_FILES = ("README.md", "docs/site/index.md")
EXAMPLE_FILES = ("README.md", "docs/site/quickstart.md")
EXAMPLE_MINOR = "4.1"
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


def validate(modules, rows):
    """Raise ValueError naming the module when the rows and the module POMs disagree."""
    names = {f"spark-{m['minor']}_{m['scala']}": m for m in modules}
    for r in rows:
        if r["module"] not in names:
            raise ValueError(f"matrix.json: row {r} names the module {r['module']}, which does not exist")
    for n, m in names.items():
        if not any(r["module"] == n and r["spark"] == m["spark"] and r["delta"] == m["delta"] for r in rows):
            raise ValueError(f"{n}: no matrix.json row runs its own spark.version {m['spark']} with its "
                             f"delta.version {m['delta']}")


def released_version(changelog):
    found = re.search(r"^## (\d+\.\d+\.\d+) - ", changelog, flags=re.M)
    if not found:
        raise ValueError("CHANGELOG.md: no released version heading")
    return found.group(1)


def example(modules, version):
    """The quickstart command for the Spark EXAMPLE_MINOR module with Delta and gluecat versions filled in."""
    m = next((x for x in modules if x["minor"] == EXAMPLE_MINOR and x["scala"] == "2.13"), None)
    if m is None:
        raise ValueError(f"no Spark {EXAMPLE_MINOR} Scala 2.13 module for the example command")
    return (
        f"Example for Spark {EXAMPLE_MINOR} with Delta {m['delta']} and S3A: `hadoop-aws` (the same version as the Hadoop "
        "jars in your Spark distribution, `<hadoop-version>` below) brings the SDK bundle, and mapping `s3://` to S3A "
        "keeps the table locations in Glue as `s3://`, the form Athena reads:\n\n```\n"
        f"spark-sql --packages io.github.sdaberdaku:gluecat-spark-{m['minor']}_{m['scala']}:{version},"
        f"io.delta:{m['delta_artifact']}:{m['delta']},org.apache.hadoop:hadoop-aws:<hadoop-version> \\\n"
        "  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem\n```")


def replace_block(text, rendered, tag="artifacts"):
    start, end = f"<!-- {tag}:start -->", f"<!-- {tag}:end -->"
    i, j = text.find(start), text.find(end)
    if i < 0 or j < i:
        raise ValueError(f"{tag}:start / {tag}:end markers not found")
    return text[:i + len(start)] + "\n" + rendered + "\n" + text[j:]


def main(argv=None):
    check = "--check" in (sys.argv[1:] if argv is None else argv)
    modules, rows = read_modules(ROOT), read_rows(ROOT)
    try:
        validate(modules, rows)
    except ValueError as e:
        print(f"invalid: {e}", file=sys.stderr)
        return 1
    blocks = (("artifacts", table(modules, rows), TABLE_FILES),
              ("example", example(modules, released_version((ROOT / "CHANGELOG.md").read_text())), EXAMPLE_FILES))
    stale = []
    for tag, rendered, files in blocks:
        for f in files:
            path = ROOT / f
            old = path.read_text()
            new = replace_block(old, rendered, tag)
            if new != old:
                stale.append(f)
                if not check:
                    path.write_text(new)
    stale = sorted(set(stale))
    if check and stale:
        print(f"stale: {', '.join(stale)}; run python scripts/render_tables.py", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
