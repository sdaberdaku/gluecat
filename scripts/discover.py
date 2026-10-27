#!/usr/bin/env python3
"""Discover new Spark and Delta releases on Maven Central and update the module POMs and matrix.json.

Rules (docs/superpowers/specs/2026-10-27-version-automation-design.md), applied in this order:
  1. a new Spark patch in a module's line moves the module and its current-Spark matrix rows;

  3. a new Delta minor (same major, artifact available for the module) adds rows and shrinks the old ones;
  4. a new Spark line for Scala 2.13 becomes a module once delta-spark_<minor>_2.13 exists.
Discovery never changes the Spark version of a row pinned to another Spark patch.
One line per addition is printed; nothing is printed when nothing changes. Stable releases only.
"""
import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

import render_tables
from render_tables import vkey

ROOT = Path(__file__).resolve().parent.parent
CENTRAL = "https://repo1.maven.org/maven2"
RETRY_SECONDS = 5
STABLE = re.compile(r"^\d+\.\d+\.\d+$")
EPILOG = """\
Test-only: when DISCOVER_FAKE_MAVEN is set the network is never used. It is a JSON object mapping
"group:artifact" to a list of versions, e.g. "io.delta:delta-spark_2.13": ["4.4.1"], and
"spark-parent:<scala binary>:<spark version>" to {"hadoop": "...", "scala": "..."}.
"""


# ---- pure rules -------------------------------------------------------------------------------------------------

def name(m):
    return f"spark-{m['minor']}_{m['scala']}"


def stable(versions):
    return sorted({v for v in versions if STABLE.match(v)}, key=vkey)


def line(v):
    return tuple(vkey(v)[:2])


def newest_patch(versions, base):
    """Newest stable version on base's major.minor line that is newer than base, else None."""
    same = [v for v in stable(versions) if line(v) == line(base) and vkey(v) > vkey(base)]
    return same[-1] if same else None


def higher(current, candidate):
    return candidate if vkey(candidate) > vkey(current) else current


def per_spark(m):
    return f"delta-spark_{m['minor']}_{m['scala']}"


def patch_from(m, delta_versions, base, fallback):
    """(newer patch of base, artifact) from the module's per-Spark artifact first, then from fallback."""
    for art in (per_spark(m), fallback):
        new = newest_patch(delta_versions.get(art, []), base)
        if new:
            return new, art
    return None


def with_artifact(r, art, m):
    """Row r on artifact art: no deltaArtifact key when art is the module default, else the key after delta."""
    if art == m["delta_artifact"]:
        return {k: v for k, v in r.items() if k != "deltaArtifact"}
    if "deltaArtifact" in r:
        return dict(r, deltaArtifact=art)
    out = {}
    for k, v in r.items():
        out[k] = v
        if k == "delta":
            out["deltaArtifact"] = art
    return out


def switch_default(m, rows, new, delta_versions):
    """Make new the module's default artifact. Rows naming it drop the key; rows that inherited the old default
    but whose Delta the new one lacks name the old artifact."""
    old, m["delta_artifact"] = m["delta_artifact"], new
    have = set(delta_versions.get(new, []))
    for i, r in enumerate(rows):
        if r["module"] == name(m):
            if r.get("deltaArtifact") == new:
                rows[i] = {k: v for k, v in r.items() if k != "deltaArtifact"}
            elif "deltaArtifact" not in r and r["delta"] not in have:
                rows[i] = with_artifact(r, old, m)


def plan_updates(modules, rows, spark_versions, delta_versions, spark_parent):
    """Apply rules 2-6. spark_versions: {scala binary: [versions]}; delta_versions: {artifact: [versions]};
    spark_parent(scala binary, spark version) -> {"hadoop", "scala"}. Returns (modules, rows, additions)."""
    modules, rows = [dict(m) for m in modules], [dict(r) for r in rows]
    adds = []

    def report(kind, found):
        for v in sorted(found, key=vkey):
            adds.append(f"{kind} {v} ({', '.join(found[v])})")

    found = {}  # rule 2: Spark patches
    for m in modules:
        new = newest_patch(spark_versions.get(m["scala"], []), m["spark"])
        if not new:
            continue
        parent = spark_parent(m["scala"], new)
        for r in rows:
            if r["module"] == name(m) and r["spark"] == m["spark"]:
                r["spark"] = new
        # the Spark release may carry an older Scala or Hadoop than the module deliberately uses
        m["scala_version"] = higher(m["scala_version"], parent["scala"])
        m["hadoop"] = higher(m["hadoop"], parent["hadoop"])
        m["spark"] = new
        found.setdefault(new, []).append(name(m))
    report("spark", found)

    found = {}  # rule 3: Delta patches; every row on the old patch follows, pinned Spark patches stay as they are
    for m in modules:
        for i, r in enumerate(rows):
            if r["module"] != name(m):
                continue
            hit = patch_from(m, delta_versions, r["delta"], r.get("deltaArtifact", m["delta_artifact"]))
            if hit:
                rows[i] = with_artifact(dict(r, delta=hit[0]), hit[1], m)
                found.setdefault(hit[0], [])
                if name(m) not in found[hit[0]]:
                    found[hit[0]].append(name(m))
        hit = patch_from(m, delta_versions, m["delta"], m["delta_artifact"])
        if hit:
            m["delta"] = hit[0]
            if hit[0] not in delta_versions.get(m["delta_artifact"], []):
                switch_default(m, rows, hit[1], delta_versions)
            found.setdefault(hit[0], [])
            if name(m) not in found[hit[0]]:
                found[hit[0]].append(name(m))
    report("delta", found)

    found = {}  # rule 4: Delta minors (current rows only)
    for m in modules:
        per = per_spark(m)
        by_art = {a: stable(delta_versions.get(a, [])) for a in (per, m["delta_artifact"])}
        major = vkey(m["delta"])[0]
        lines = {line(v) for vs in by_art.values() for v in vs if line(v)[0] == major and line(v) > line(m["delta"])}
        for ln in sorted(lines):
            art = next(a for a in (per, m["delta_artifact"]) if any(line(v) == ln for v in by_art[a]))
            new = [v for v in by_art[art] if line(v) == ln][-1]
            cur = [r for r in rows if r["module"] == name(m) and r["spark"] == m["spark"] and r["delta"] == m["delta"]]
            if not cur:
                continue
            copies = [with_artifact(dict(r, delta=new), art, m) for r in cur]
            keep = min(cur, key=lambda r: vkey(r["jdk"]))
            rows = [r for r in rows if r is keep or not any(r is c for c in cur)]
            keep["smoke"] = False
            last = max(i for i, r in enumerate(rows) if r["module"] == name(m))
            rows[last + 1:last + 1] = copies
            m["delta"] = new
            if new not in delta_versions.get(m["delta_artifact"], []):
                switch_default(m, rows, art, delta_versions)
            found.setdefault(new, []).append(name(m))
    report("delta", found)

    spark213 = stable(spark_versions.get("2.13", []))  # rule 5: new Spark lines
    while True:
        template = [m for m in modules if m["scala"] == "2.13"][-1]
        candidates = sorted({line(v) for v in spark213 if line(v) > line(template["spark"])})
        for ln in candidates:
            minor = f"{ln[0]}.{ln[1]}"
            artifact = f"delta-spark_{minor}_2.13"
            delta = stable(delta_versions.get(artifact, []))
            if delta:
                break
        else:
            break
        spark = [v for v in spark213 if line(v) == ln][-1]
        parent = spark_parent("2.13", spark)
        new = dict(template, minor=minor, spark=spark, delta=delta[-1], delta_artifact=artifact,
                   scala_version=higher(template["scala_version"], parent["scala"]),
                   hadoop=higher(template["hadoop"], parent["hadoop"]))
        for r in [r for r in rows if r["module"] == name(template) and r["spark"] == template["spark"]
                  and r["delta"] == template["delta"]]:
            c = dict(r, module=name(new), spark=spark, delta=delta[-1])
            c.pop("deltaArtifact", None)
            rows.append(c)
        modules.append(new)
        adds.append(f"module {name(new)} (spark {spark}, delta {delta[-1]})")
        spark213 = [v for v in spark213 if line(v) != ln or v == spark]
    return modules, rows, adds


def new_lines(modules, spark_versions):
    """Delta artifacts worth fetching for Spark lines newer than the newest Scala 2.13 module."""
    top = [m for m in modules if m["scala"] == "2.13"][-1]["spark"]
    return sorted({f"delta-spark_{line(v)[0]}.{line(v)[1]}_2.13" for v in stable(spark_versions.get("2.13", []))
                   if line(v) > line(top)})


# ---- writers ----------------------------------------------------------------------------------------------------

def write_matrix(rows):
    body = ",\n".join("  {" + ", ".join(f'"{k}": {json.dumps(v)}' for k, v in r.items()) + "}" for r in rows)
    return "[\n" + body + "\n]\n"


def edit_pom(text, prop, value):
    out, n = re.subn(rf"(<{re.escape(prop)}>)[^<]*(</{re.escape(prop)}>)", rf"\g<1>{value}\g<2>", text, count=1)
    if not n:
        raise ValueError(f"missing <{prop}>")
    return out


def new_module_pom(newest_pom_text, minor, spark, delta, scala, hadoop):
    old = re.search(r"<spark\.minor>([^<]+)</spark\.minor>", newest_pom_text).group(1)
    text = re.sub(r"^.*<(artifactId>gluecat-spark-|name>|description>).*$",
                  lambda mo: mo.group(0).replace(old, minor), newest_pom_text, flags=re.M)
    for prop, value in (("spark.version", spark), ("spark.minor", minor), ("delta.version", delta),
                        ("scala.version", scala), ("hadoop.aws.version", hadoop)):
        text = edit_pom(text, prop, value)
    scala_bin = re.search(r"<scala\.binary\.version>([^<]+)<", text).group(1)
    return set_delta_artifact(text, f"delta-spark_{minor}_{scala_bin}")


def set_delta_artifact(text, artifact):
    """Set the module's delta.artifact, adding the line after delta.version when the module inherits the default."""
    if "<delta.artifact>" in text:
        return edit_pom(text, "delta.artifact", artifact)
    return re.sub(r"^( *)(<delta\.version>.*)$", rf"\1\2\n\1<delta.artifact>{artifact}</delta.artifact>", text,
                  count=1, flags=re.M)


def add_module(parent_pom_text, module_dir):
    lines = list(re.finditer(r"^( *)<module>[^<]*</module>\n", parent_pom_text, flags=re.M))
    last = lines[-1]
    return (parent_pom_text[:last.end()] + f"{last.group(1)}<module>{module_dir}</module>\n"
            + parent_pom_text[last.end():])


def changelog_unreleased(text, bullet):
    if "## Unreleased" in text:
        head, _, rest = text.partition("## Unreleased\n")
        body, sep, tail = rest.partition("\n## ")
        return head + "## Unreleased\n" + body.rstrip("\n") + f"\n- {bullet}\n" + ("\n## " + tail if sep else "")
    i = text.find("\n## ")
    return text[:i + 1] + f"## Unreleased\n\n- {bullet}\n\n" + text[i + 1:]


# ---- network ----------------------------------------------------------------------------------------------------

def parse_versions(xml_text):
    return [v.text for v in ET.fromstring(xml_text).findall("./versioning/versions/version")]


def parse_spark_parent(pom_text):
    root = ET.fromstring(pom_text)
    props = root.find("{http://maven.apache.org/POM/4.0.0}properties")
    get = lambda tag: props.find("{http://maven.apache.org/POM/4.0.0}" + tag).text.strip()  # noqa: E731
    return {"hadoop": get("hadoop.version"), "scala": get("scala.version")}


def get(url, missing_ok=False):
    for attempt in (1, 2):
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                return resp.read().decode()
        except urllib.error.HTTPError as e:
            if e.code == 404 and missing_ok:
                return None
            err = e
        except OSError as e:
            err = e
        if attempt == 1:
            time.sleep(RETRY_SECONDS)
    sys.exit(f"discover: {url}: {err}")


def fake():
    raw = os.environ.get("DISCOVER_FAKE_MAVEN")
    return json.loads(raw) if raw is not None else None


def fetch_versions(group, artifact):
    if (f := fake()) is not None:
        return f.get(f"{group}:{artifact}", [])
    text = get(f"{CENTRAL}/{group.replace('.', '/')}/{artifact}/maven-metadata.xml", missing_ok=True)
    return [] if text is None else parse_versions(text)


def fetch_spark_parent(scala, version):
    if (f := fake()) is not None:
        try:
            return f[f"spark-parent:{scala}:{version}"]
        except KeyError:
            sys.exit(f"discover: no fake spark-parent:{scala}:{version}")
    a = f"spark-parent_{scala}"
    return parse_spark_parent(get(f"{CENTRAL}/org/apache/spark/{a}/{version}/{a}-{version}.pom"))


# ---- driver -----------------------------------------------------------------------------------------------------

POM_PROPS = {"spark": "spark.version", "delta": "delta.version", "scala_version": "scala.version",
             "hadoop": "hadoop.aws.version"}


def apply(root, old_modules, modules, rows, old_rows):
    texts = {}
    for i, m in enumerate(modules):
        path = root / "modules" / name(m) / "pom.xml"
        if i < len(old_modules):
            text = path.read_text()
            for key, prop in POM_PROPS.items():
                if m[key] != old_modules[i][key]:
                    text = edit_pom(text, prop, m[key])
            if m["delta_artifact"] != old_modules[i]["delta_artifact"]:
                text = set_delta_artifact(text, m["delta_artifact"])
        else:
            prev = modules[i - 1]
            text = new_module_pom((root / "modules" / name(prev) / "pom.xml").read_text(), m["minor"], m["spark"],
                                  m["delta"], m["scala_version"], m["hadoop"])
            path.parent.mkdir(parents=True)
            parent = root / "pom.xml"
            parent.write_text(add_module(parent.read_text(), f"modules/{name(m)}"))
            log = root / "CHANGELOG.md"
            log.write_text(changelog_unreleased(
                log.read_text(), f"Add `gluecat-{name(m)}` for Spark {m['spark']} with Delta {m['delta']}."))
        texts[path] = text
    for path, text in texts.items():
        if not path.exists() or path.read_text() != text:
            path.write_text(text)
    if rows != old_rows:
        (root / "matrix.json").write_text(write_matrix(rows))


def main(argv=None, root=ROOT):
    ap = argparse.ArgumentParser(description=__doc__, epilog=EPILOG,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="print the additions, write nothing")
    args = ap.parse_args(argv)
    modules, rows = render_tables.read_modules(root), render_tables.read_rows(root)
    spark = {s: fetch_versions("org.apache.spark", f"spark-sql_{s}") for s in dict.fromkeys(m["scala"] for m in modules)}
    artifacts = ({m["delta_artifact"] for m in modules} | {per_spark(m) for m in modules} | {r["deltaArtifact"] for r in rows if "deltaArtifact" in r}
                 | set(new_lines(modules, spark)))
    delta = {a: fetch_versions("io.delta", a) for a in sorted(artifacts)}
    new_modules, new_rows, adds = plan_updates(modules, rows, spark, delta, fetch_spark_parent)
    if adds:
        print("\n".join(adds))
    if adds and not args.dry_run:
        apply(root, modules, new_modules, new_rows, rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
